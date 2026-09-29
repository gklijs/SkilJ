//! Round-trip tests for `skilj_core::db` - the Postgres persistence layer
//! added propagating `skilj-rest`'s five wired routes (docs/
//! architecture.md §7.2/§7.4). Unlike every other test file in this
//! crate (pure functions, no I/O), these need a real Postgres to run
//! against:
//!
//! - Set `DATABASE_URL` (e.g.
//!   `postgres://postgres:postgres@localhost/skilj_test`) to point at
//!   one you already have running - fastest, and what CI should do.
//! - Otherwise, `test_pool()` falls back to `postgresql_embedded`
//!   (a dev-dependency only - never a real one): it downloads and runs a
//!   real PostgreSQL binary as the current user, no Docker/root needed.
//!   One instance is started per test *binary*, by
//!   `skilj_test_support::database_url` (shared across test functions -
//!   `setup()`/`start()` cost real wall-clock time), whose watchdog stops
//!   it and deletes its data dir once the binary exits, however it exits.
//! - If neither works (no `DATABASE_URL` *and* the embedded download/
//!   start fails - e.g. a sandboxed environment with no egress to fetch
//!   the archive, or missing a system library like `libxml2` the
//!   downloaded binary links against), every test skips itself with a
//!   note on stderr rather than failing, so `cargo test --workspace`
//!   still stays green with no database reachable at all.
//!
//! Every test picks its own random bounded-context name
//! (`generate_token_id()` doubles as a convenient random-string source)
//! rather than truncating shared tables between tests, so tests stay
//! safe to run concurrently against the same database - the same reason
//! `skilj-core`'s own pure-function tests never share mutable state.

use chrono::{SubsecRound, Utc};
use skilj_core::access_control::{
    AccessLevel, CommandToken, DirectCreationToken, EventReadStartPosition, EventReadToken,
    ExternalEventToken, Role, RoleAccessMapping, RoleStatus, TokenStatus,
};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, AccessTokenKind, Pool};
use skilj_core::encryption::EncryptionMasterKey;
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    AckMode, BoundedContext, BoundedContextStatus, CommandType, CursorUpdate, EncryptionKeyStatus,
    Event, EventOrigin, EventType, ReadCursor,
};
use skilj_core::projections::{Projection, ProjectionRebuild, ProjectionRebuildStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret, Metadata, Tag};

/// One `tokio::runtime::Runtime`, shared by every test in this binary via
/// `#[test] fn ... { runtime().block_on(async { ... }) }` rather than
/// `#[tokio::test]`'s own per-function runtime. Required, not a style
/// preference: `sqlx::Pool`'s internal connection-maintenance task is
/// spawned onto whichever runtime is active when the pool is created, and
/// a pool built once (in `provision`, on whichever test happens to run
/// first) and then reused by every other test - see `TEST_DB` below -
/// would otherwise be reused from a *different* runtime than the one it
/// was spawned on every time after the first, which manifests as
/// `sqlx::Error::PoolTimedOut` once enough per-test runtimes have come
/// and gone. Sharing one runtime for the whole binary is the standard fix.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for persistence tests")
    })
}

/// The shared connection pool. Never read directly outside
/// `provision`/`test_pool`.
struct TestDb {
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

/// `None` (with a stderr note explaining why) when no database could be
/// reached at all - every test below opens with
/// `let Some(pool) = test_pool().await else { return };`. See this
/// module's own doc comment for the `DATABASE_URL`-then-embedded
/// fallback and why provisioning happens once per test binary, not once
/// per test function.
async fn test_pool() -> Option<Pool> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.pool.clone())
}

async fn provision() -> Option<TestDb> {
    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        let pool = match db::connect(&database_url).await {
            Ok(pool) => pool,
            Err(e) => {
                eprintln!("skipping: DATABASE_URL is set but connecting failed: {e}");
                return None;
            }
        };
        if let Err(e) = db::migrate(&pool).await {
            eprintln!("skipping: DATABASE_URL migration failed: {e}");
            return None;
        }
        return Some(TestDb { pool });
    }

    let url = skilj_test_support::database_url("skilj_test").await?;
    let pool = match db::connect(&url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to embedded PostgreSQL failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating embedded PostgreSQL failed: {e}");
        return None;
    }
    Some(TestDb { pool })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

/// `Utc::now()` truncated to microsecond precision - `TIMESTAMPTZ` is
/// microsecond-precision in Postgres, `chrono::Utc::now()` is nanosecond-
/// precision in Rust, so a value built with the latter and compared
/// against what a round-trip through the former hands back never
/// `assert_eq!`s equal otherwise. Not a persistence bug - `db::insert_*`/
/// `db::get_*` round-trip every value exactly as far as Postgres's own
/// column precision allows; this only matters for test fixtures that
/// construct a value in memory and then compare it byte-for-byte against
/// the same value freshly loaded back.
fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn seed_bounded_context(pool: &Pool) -> BoundedContext {
    let bc = BoundedContext {
        name: unique_name("bc"),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    bc
}

async fn seed_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: unique_name("event_type"),
        schema: r#"{"properties":{"amount":{"type":"number"}}}"#.to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: true,
        direct_creation_allowed: true,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: true,
    };
    db::upsert_event_type(pool, &et).await.unwrap();
    et
}

async fn seed_command_type(pool: &Pool, bc: &BoundedContext) -> CommandType {
    let ct = CommandType {
        bounded_context: bc.clone(),
        name: unique_name("command_type"),
        schema: r#"{"properties":{"amount":{"type":"number"}}}"#.to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        rest_trigger_allowed: true,
    };
    db::upsert_command_type(pool, &ct).await.unwrap();
    ct
}

async fn seed_role(pool: &Pool, superadmin: bool) -> Role {
    let role = Role {
        id: generate_token_id(),
        external_subject: unique_name("subject"),
        name: "Test Role".to_string(),
        superadmin,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();
    role
}

async fn seed_active_role_access_mapping(
    pool: &Pool,
    role: &Role,
    bc: &BoundedContext,
    level: AccessLevel,
) -> RoleAccessMapping {
    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
        level,
        can_read_sensitive: false,
        scope: None,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(pool, &mapping)
        .await
        .unwrap();
    mapping
}

#[test]
fn round_trips_a_system_created_bounded_context() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;

        let loaded = db::get_bounded_context(&pool, &bc.name).await.unwrap();
        assert_eq!(loaded, Some(bc));
    });
}

#[test]
fn round_trips_a_superadmin_created_bounded_context() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = Role {
            id: generate_token_id(),
            external_subject: unique_name("subject"),
            name: "Superadmin".to_string(),
            superadmin: true,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        let bc = BoundedContext {
            name: unique_name("bc"),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SuperadminCreator { role: role.clone() },
            template: None,
        };
        db::insert_role(&pool, &role).await.unwrap();
        db::insert_bounded_context(&pool, &bc).await.unwrap();

        let loaded = db::get_bounded_context(&pool, &bc.name).await.unwrap();
        assert_eq!(loaded, Some(bc));
    });
}

#[test]
fn get_bounded_context_is_none_for_an_unknown_name() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let loaded = db::get_bounded_context(&pool, &unique_name("missing"))
            .await
            .unwrap();
        assert_eq!(loaded, None);
    });
}

/// `list_bounded_contexts` - added propagating `skilj-graphql`'s Phase 1
/// admin console (`BoundedContextDirectory`'s `boundedContexts` query,
/// [docs/architecture.md §8](../../docs/architecture.md#open-for-a-future-pass) item 5's own plan). Every context this engine
/// knows of, unfiltered - so two freshly-seeded contexts are both in the
/// returned set, not just the one most recently inserted.
#[test]
fn list_bounded_contexts_includes_every_inserted_context() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc_a = seed_bounded_context(&pool).await;
        let bc_b = seed_bounded_context(&pool).await;

        let listed = db::list_bounded_contexts(&pool).await.unwrap();

        assert!(listed.contains(&bc_a));
        assert!(listed.contains(&bc_b));
    });
}

#[test]
fn round_trips_an_event_type() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;

        let loaded = db::get_event_type(&pool, &bc.name, &et.name).await.unwrap();
        assert_eq!(loaded, Some(et));
    });
}

#[test]
fn upsert_event_type_updates_in_place() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let mut et = seed_event_type(&pool, &bc).await;

        et.schema_version = 2;
        et.event_read_allowed = false;
        db::upsert_event_type(&pool, &et).await.unwrap();

        let loaded = db::get_event_type(&pool, &bc.name, &et.name).await.unwrap();
        assert_eq!(loaded, Some(et));
    });
}

#[test]
fn next_sequence_starts_at_zero_and_increments() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;

        assert_eq!(db::next_sequence(&pool, &bc.name).await.unwrap(), 0);
        assert_eq!(db::next_sequence(&pool, &bc.name).await.unwrap(), 1);
        assert_eq!(db::next_sequence(&pool, &bc.name).await.unwrap(), 2);
    });
}

#[test]
fn next_sequence_is_independent_per_bounded_context() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let a = seed_bounded_context(&pool).await;
        let b = seed_bounded_context(&pool).await;

        assert_eq!(db::next_sequence(&pool, &a.name).await.unwrap(), 0);
        assert_eq!(db::next_sequence(&pool, &b.name).await.unwrap(), 0);
        assert_eq!(db::next_sequence(&pool, &a.name).await.unwrap(), 1);
    });
}

fn sample_event(bc: &BoundedContext, et: &EventType, sequence: i64, client_id: &str) -> Event {
    Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: r#"{"amount":5}"#.to_string(),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: client_id.to_string(),
            created_at: test_now(),
            correlation_id: None,
            causation_id: None,
        },
        sequence,
        tags: vec![Tag {
            key: "region".to_string(),
            value: Some("eu".to_string()),
        }],
        encryption_keys: Vec::new(),
        origin: EventOrigin::ExternalTriggered {
            source_content: "raw source".to_string(),
            source_context: Some("adapter-1".to_string()),
        },
    }
}

#[test]
fn round_trips_an_externally_triggered_event() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let event = sample_event(&bc, &et, 0, "adapter-token-id");
        db::insert_event(&pool, &event, None).await.unwrap();

        let loaded = db::list_events(&pool, &bc.name, &et.name).await.unwrap();
        assert_eq!(loaded, vec![event]);
    });
}

#[test]
fn round_trips_a_directly_created_event() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let event = Event {
            origin: EventOrigin::DirectlyCreated,
            ..sample_event(&bc, &et, 0, "adapter-token-id")
        };
        db::insert_event(&pool, &event, None).await.unwrap();

        let loaded = db::list_events(&pool, &bc.name, &et.name).await.unwrap();
        assert_eq!(loaded, vec![event]);
    });
}

#[test]
fn list_events_is_ordered_by_sequence() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        for seq in [2_i64, 0, 1] {
            db::insert_event(
                &pool,
                &sample_event(&bc, &et, seq, "adapter-token-id"),
                None,
            )
            .await
            .unwrap();
        }

        let loaded = db::list_events(&pool, &bc.name, &et.name).await.unwrap();
        assert_eq!(
            loaded.iter().map(|e| e.sequence).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    });
}

#[test]
fn round_trips_an_external_event_token_and_its_kind() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let token = ExternalEventToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Active,
            created_at: test_now(),
            revoked_at: None,
            event_type: et,
            // A real value, not just None - cross-tenant write fix
            // (docs/architecture.md's own write-up of these passes):
            // proves the shared access_tokens.scope column actually
            // round-trips for this kind now too, not only `event_read`.
            scope: Some("acme".into()),
        };
        db::insert_external_event_token(&pool, &token)
            .await
            .unwrap();

        assert_eq!(
            db::access_token_kind(&pool, &token.id).await.unwrap(),
            Some(AccessTokenKind::ExternalEvent)
        );
        let loaded = db::get_external_event_token(&pool, &token.id)
            .await
            .unwrap()
            .unwrap();
        // `secret` is stored hashed (`hash_secret`), never round-tripped as
        // the plaintext it was inserted with - see AccessToken.secret.
        assert_eq!(
            loaded.secret,
            skilj_core::shared::hash_secret(&token.secret)
        );
        assert_eq!(
            loaded,
            ExternalEventToken {
                secret: loaded.secret.clone(),
                ..token
            }
        );
    });
}

#[test]
fn round_trips_a_direct_creation_token() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let token = DirectCreationToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Active,
            created_at: test_now(),
            revoked_at: None,
            event_type: et,
            scope: None,
        };
        db::insert_direct_creation_token(&pool, &token)
            .await
            .unwrap();

        assert_eq!(
            db::access_token_kind(&pool, &token.id).await.unwrap(),
            Some(AccessTokenKind::DirectCreation)
        );
        let loaded = db::get_direct_creation_token(&pool, &token.id)
            .await
            .unwrap()
            .unwrap();
        // See `round_trips_an_external_event_token_and_its_kind` - `secret`
        // is stored hashed, not round-tripped as the plaintext it was
        // inserted with.
        assert_eq!(
            loaded.secret,
            skilj_core::shared::hash_secret(&token.secret)
        );
        assert_eq!(
            loaded,
            DirectCreationToken {
                secret: loaded.secret.clone(),
                ..token
            }
        );
    });
}

/// No prior round-trip test existed for `CommandToken` at all - this is
/// new coverage, not just a scope addition, for `insert_command_token`/
/// `get_command_token` (the write-side pass changed both: `scope` joined
/// `insert_command_token`'s own hand-written `INSERT`, which previously
/// wrote no `scope` column at all, unlike the three `insert_access_token_row`-
/// backed kinds - see `insert_command_token`'s own doc comment for why
/// it stays hand-written).
#[test]
fn round_trips_a_command_token() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let ct = seed_command_type(&pool, &bc).await;
        let token = CommandToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Active,
            created_at: test_now(),
            revoked_at: None,
            command_type: ct,
            scope: Some("acme".into()),
        };
        db::insert_command_token(&pool, &token).await.unwrap();

        assert_eq!(
            db::access_token_kind(&pool, &token.id).await.unwrap(),
            Some(AccessTokenKind::Command)
        );
        let loaded = db::get_command_token(&pool, &token.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            loaded.secret,
            skilj_core::shared::hash_secret(&token.secret)
        );
        assert_eq!(
            loaded,
            CommandToken {
                secret: loaded.secret.clone(),
                ..token
            }
        );
    });
}

#[test]
fn round_trips_a_revoked_event_read_token() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let now = test_now();
        let token = EventReadToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Revoked,
            created_at: now,
            revoked_at: Some(now),
            event_type: et,
            scope: None,
            start_from: EventReadStartPosition::Beginning,
            start_at_sequence: None,
            start_at_time: None,
        };
        db::insert_event_read_token(&pool, &token).await.unwrap();

        assert_eq!(
            db::access_token_kind(&pool, &token.id).await.unwrap(),
            Some(AccessTokenKind::EventRead)
        );
        let loaded = db::get_event_read_token(&pool, &token.id)
            .await
            .unwrap()
            .unwrap();
        // See `round_trips_an_external_event_token_and_its_kind` - `secret`
        // is stored hashed, not round-tripped as the plaintext it was
        // inserted with.
        assert_eq!(
            loaded.secret,
            skilj_core::shared::hash_secret(&token.secret)
        );
        assert_eq!(
            loaded,
            EventReadToken {
                secret: loaded.secret.clone(),
                ..token
            }
        );
    });
}

/// The 401-vs-403 distinction `skilj-rest`'s auth layer relies on
/// (`AccessTokenKind`'s own doc comment): a lookup under the wrong
/// getter returns `None`, not the row - `get_direct_creation_token`
/// must not hand back a token that's actually kind `external_event`.
#[test]
fn getting_a_token_by_the_wrong_kind_returns_none() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let token = ExternalEventToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Active,
            created_at: test_now(),
            revoked_at: None,
            event_type: et,
            scope: None,
        };
        db::insert_external_event_token(&pool, &token)
            .await
            .unwrap();

        assert_eq!(
            db::get_direct_creation_token(&pool, &token.id)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            db::get_event_read_token(&pool, &token.id).await.unwrap(),
            None
        );
    });
}

#[test]
fn access_token_kind_is_none_for_an_unknown_id() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        assert_eq!(
            db::access_token_kind(&pool, &generate_token_id())
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn read_cursor_is_none_before_the_first_consume() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let token = EventReadToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Active,
            created_at: test_now(),
            revoked_at: None,
            event_type: et,
            scope: None,
            start_from: EventReadStartPosition::Beginning,
            start_at_sequence: None,
            start_at_time: None,
        };
        db::insert_event_read_token(&pool, &token).await.unwrap();

        assert_eq!(db::get_read_cursor(&pool, &token).await.unwrap(), None);
    });
}

#[test]
fn apply_cursor_update_created_then_advanced_round_trips() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let token = EventReadToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Active,
            created_at: test_now(),
            revoked_at: None,
            event_type: et,
            scope: None,
            start_from: EventReadStartPosition::Beginning,
            start_at_sequence: None,
            start_at_time: None,
        };
        db::insert_event_read_token(&pool, &token).await.unwrap();

        let created_at = test_now();
        let created = CursorUpdate::Created(Box::new(ReadCursor {
            token: token.clone(),
            ack_mode: AckMode::AutoAdvance,
            sequence: 0,
            updated_at: created_at,
            checked_out_at: None,
        }));
        db::apply_cursor_update(&pool, &token, &created)
            .await
            .unwrap();
        let loaded = db::get_read_cursor(&pool, &token).await.unwrap().unwrap();
        assert_eq!(loaded.sequence, 0);
        assert_eq!(loaded.ack_mode, AckMode::AutoAdvance);
        assert_eq!(loaded.checked_out_at, None);

        let advanced_at = test_now();
        let advanced = CursorUpdate::Advanced {
            sequence: 5,
            updated_at: advanced_at,
        };
        db::apply_cursor_update(&pool, &token, &advanced)
            .await
            .unwrap();
        let loaded = db::get_read_cursor(&pool, &token).await.unwrap().unwrap();
        assert_eq!(loaded.sequence, 5);
        // ack_mode isn't part of an Advanced update - it stays whatever
        // Created it with, per ReadCursor's own 1:1-with-a-token shape.
        assert_eq!(loaded.ack_mode, AckMode::AutoAdvance);
    });
}

/// Codeberg issue #25's investigation (docs/architecture.md §53) -
/// `CursorUpdate::Claimed` round-trips through `apply_cursor_update`
/// without touching `sequence`/`updated_at`, the same "a claim alone
/// never moves the cursor's own position" guarantee
/// `event_store::consume_events`' own doc comment describes.
#[test]
fn apply_cursor_update_claimed_round_trips_without_moving_sequence() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let token = EventReadToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Active,
            created_at: test_now(),
            revoked_at: None,
            event_type: et,
            scope: None,
            start_from: EventReadStartPosition::Beginning,
            start_at_sequence: None,
            start_at_time: None,
        };
        db::insert_event_read_token(&pool, &token).await.unwrap();
        db::apply_cursor_update(
            &pool,
            &token,
            &CursorUpdate::Created(Box::new(ReadCursor {
                token: token.clone(),
                ack_mode: AckMode::ManualAck,
                sequence: 3,
                updated_at: test_now(),
                checked_out_at: None,
            })),
        )
        .await
        .unwrap();

        let claimed_at = test_now();
        db::apply_cursor_update(
            &pool,
            &token,
            &CursorUpdate::Claimed {
                checked_out_at: claimed_at,
            },
        )
        .await
        .unwrap();

        let loaded = db::get_read_cursor(&pool, &token).await.unwrap().unwrap();
        assert_eq!(loaded.sequence, 3, "a claim alone must not move sequence");
        assert_eq!(loaded.checked_out_at, Some(claimed_at));
    });
}

#[test]
fn record_acknowledgement_moves_the_cursor() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let token = EventReadToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Active,
            created_at: test_now(),
            revoked_at: None,
            event_type: et,
            scope: None,
            start_from: EventReadStartPosition::Beginning,
            start_at_sequence: None,
            start_at_time: None,
        };
        db::insert_event_read_token(&pool, &token).await.unwrap();
        db::apply_cursor_update(
            &pool,
            &token,
            &CursorUpdate::Created(Box::new(ReadCursor {
                token: token.clone(),
                ack_mode: AckMode::ManualAck,
                sequence: -1,
                updated_at: test_now(),
                checked_out_at: None,
            })),
        )
        .await
        .unwrap();
        // Codeberg issue #25's investigation (docs/architecture.md §53) -
        // a live claim, so the assertion below actually proves
        // `record_acknowledgement` clears it rather than it having never
        // been set in the first place.
        db::apply_cursor_update(
            &pool,
            &token,
            &CursorUpdate::Claimed {
                checked_out_at: test_now(),
            },
        )
        .await
        .unwrap();

        let ack_at = test_now();
        db::record_acknowledgement(&pool, &token, 3, ack_at)
            .await
            .unwrap();

        let loaded = db::get_read_cursor(&pool, &token).await.unwrap().unwrap();
        assert_eq!(loaded.sequence, 3);
        assert_eq!(loaded.ack_mode, AckMode::ManualAck);
        assert_eq!(
            loaded.checked_out_at, None,
            "AcknowledgeEvents must clear any live claim unconditionally"
        );
    });
}

#[test]
fn round_trips_a_role() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool, false).await;

        let loaded = db::get_role(&pool, &role.id).await.unwrap();
        assert_eq!(loaded, Some(role));
    });
}

#[test]
fn get_role_is_none_for_an_unknown_id() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let loaded = db::get_role(&pool, &generate_token_id()).await.unwrap();
        assert_eq!(loaded, None);
    });
}

#[test]
fn list_roles_includes_every_inserted_role() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool, true).await;

        let loaded = db::list_roles(&pool).await.unwrap();
        // A `contains` check, not exact equality - other tests running
        // concurrently against the same database insert their own roles
        // too (see this file's own doc comment on why every test uses a
        // random name rather than assuming exclusive access).
        assert!(loaded.contains(&role));
    });
}

#[test]
fn update_role_persists_a_revocation() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool, false).await;

        let revoked_at = test_now();
        let revoked = Role {
            status: RoleStatus::Revoked,
            revoked_at: Some(revoked_at),
            ..role
        };
        db::update_role(&pool, &revoked).await.unwrap();

        let loaded = db::get_role(&pool, &revoked.id).await.unwrap();
        assert_eq!(loaded, Some(revoked));
    });
}

/// `db::revoke_role_and_mappings` - the drift audit's P4 batch fix:
/// `RevokeRole`'s own role-update-plus-mapping-cascade used to run as
/// separate autocommit statements; this proves the real, single-call
/// replacement leaves every active mapping revoked alongside the role
/// itself, in the one call `access_management.rs::revoke_role_field` now
/// makes instead of its own hand-rolled loop.
#[test]
fn revoke_role_and_mappings_revokes_the_role_and_every_active_mapping_together() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool, false).await;
        let bc_a = seed_bounded_context(&pool).await;
        let bc_b = seed_bounded_context(&pool).await;
        let mapping_a =
            seed_active_role_access_mapping(&pool, &role, &bc_a, AccessLevel::Admin).await;
        let mapping_b =
            seed_active_role_access_mapping(&pool, &role, &bc_b, AccessLevel::Write).await;

        let revoked_at = test_now();
        let revoked_role = Role {
            status: RoleStatus::Revoked,
            revoked_at: Some(revoked_at),
            ..role.clone()
        };
        let revoked_mappings: Vec<_> = [mapping_a, mapping_b]
            .into_iter()
            .map(|m| RoleAccessMapping {
                status: RoleStatus::Revoked,
                revoked_at: Some(revoked_at),
                ..m
            })
            .collect();

        db::revoke_role_and_mappings(&pool, &revoked_role, &revoked_mappings)
            .await
            .unwrap();

        let loaded_role = db::get_role(&pool, &role.id).await.unwrap();
        assert_eq!(loaded_role, Some(revoked_role));
        for bc in [&bc_a, &bc_b] {
            let loaded_mapping = db::get_active_role_access_mapping(&pool, &role.id, &bc.name)
                .await
                .unwrap();
            assert_eq!(
                loaded_mapping, None,
                "mapping for {:?} must no longer be active",
                bc.name
            );
        }
    });
}

#[test]
fn round_trips_an_active_role_access_mapping() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool, false).await;
        let bc = seed_bounded_context(&pool).await;
        let mapping = seed_active_role_access_mapping(&pool, &role, &bc, AccessLevel::Admin).await;

        let loaded = db::get_active_role_access_mapping(&pool, &role.id, &bc.name)
            .await
            .unwrap();
        assert_eq!(loaded, Some(mapping));
    });
}

#[test]
fn get_active_role_access_mapping_is_none_when_no_mapping_exists() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool, false).await;
        let bc = seed_bounded_context(&pool).await;

        let loaded = db::get_active_role_access_mapping(&pool, &role.id, &bc.name)
            .await
            .unwrap();
        assert_eq!(loaded, None);
    });
}

#[test]
fn get_active_role_access_mapping_is_none_after_revocation() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool, false).await;
        let bc = seed_bounded_context(&pool).await;
        seed_active_role_access_mapping(&pool, &role, &bc, AccessLevel::Write).await;

        db::revoke_active_role_access_mapping(&pool, &role.id, &bc.name, test_now())
            .await
            .unwrap();

        let loaded = db::get_active_role_access_mapping(&pool, &role.id, &bc.name)
            .await
            .unwrap();
        assert_eq!(loaded, None);
    });
}

#[test]
fn revoke_active_role_access_mapping_is_a_noop_when_none_is_active() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool, false).await;
        let bc = seed_bounded_context(&pool).await;

        // No mapping exists at all yet - revoking should affect zero rows,
        // not error.
        db::revoke_active_role_access_mapping(&pool, &role.id, &bc.name, test_now())
            .await
            .unwrap();

        let loaded = db::get_active_role_access_mapping(&pool, &role.id, &bc.name)
            .await
            .unwrap();
        assert_eq!(loaded, None);
    });
}

#[test]
fn list_role_access_mappings_includes_active_and_revoked() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool, false).await;
        let bc_active = seed_bounded_context(&pool).await;
        let bc_revoked = seed_bounded_context(&pool).await;
        let active =
            seed_active_role_access_mapping(&pool, &role, &bc_active, AccessLevel::Read).await;
        seed_active_role_access_mapping(&pool, &role, &bc_revoked, AccessLevel::Write).await;
        let revoked_at = test_now();
        db::revoke_active_role_access_mapping(&pool, &role.id, &bc_revoked.name, revoked_at)
            .await
            .unwrap();

        let loaded = db::list_role_access_mappings(&pool).await.unwrap();
        assert!(loaded.contains(&active));
        assert!(loaded.iter().any(|m| {
            m.role == role
                && m.bounded_context == bc_revoked
                && m.status == RoleStatus::Revoked
                && m.revoked_at == Some(revoked_at)
        }));
    });
}

#[test]
fn list_active_role_access_mappings_for_role_excludes_other_roles_and_revoked() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role_a = seed_role(&pool, false).await;
        let role_b = seed_role(&pool, false).await;
        let bc_a = seed_bounded_context(&pool).await;
        let bc_b = seed_bounded_context(&pool).await;
        let active =
            seed_active_role_access_mapping(&pool, &role_a, &bc_a, AccessLevel::Admin).await;
        seed_active_role_access_mapping(&pool, &role_a, &bc_b, AccessLevel::Read).await;
        db::revoke_active_role_access_mapping(&pool, &role_a.id, &bc_b.name, test_now())
            .await
            .unwrap();
        seed_active_role_access_mapping(&pool, &role_b, &bc_a, AccessLevel::Write).await;

        let loaded = db::list_active_role_access_mappings_for_role(&pool, &role_a.id)
            .await
            .unwrap();
        assert_eq!(loaded, vec![active]);
    });
}

#[test]
fn round_trips_a_command_type() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let ct = seed_command_type(&pool, &bc).await;

        let loaded = db::get_command_type(&pool, &bc.name, &ct.name)
            .await
            .unwrap();
        assert_eq!(loaded, Some(ct));
    });
}

#[test]
fn get_command_type_is_none_for_an_unknown_name() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;

        let loaded = db::get_command_type(&pool, &bc.name, &unique_name("missing"))
            .await
            .unwrap();
        assert_eq!(loaded, None);
    });
}

#[test]
fn upsert_command_type_updates_in_place() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let mut ct = seed_command_type(&pool, &bc).await;

        ct.schema_version = 2;
        ct.rest_trigger_allowed = false;
        db::upsert_command_type(&pool, &ct).await.unwrap();

        let loaded = db::get_command_type(&pool, &bc.name, &ct.name)
            .await
            .unwrap();
        assert_eq!(loaded, Some(ct));
    });
}

#[test]
fn round_trips_a_projection_with_no_consumed_event_types() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: unique_name("projection"),
            schema: r#"{"properties":{"balance":{"type":"number"}}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();

        let loaded = db::get_projection(&pool, &bc.name, &projection.name)
            .await
            .unwrap();
        assert_eq!(loaded, Some(projection));
    });
}

#[test]
fn round_trips_a_projection_with_consumed_event_types() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et_a = seed_event_type(&pool, &bc).await;
        let et_b = seed_event_type(&pool, &bc).await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: unique_name("projection"),
            schema: r#"{"properties":{"balance":{"type":"number"}}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: vec![et_a.clone(), et_b.clone()],
            sync: true,
            caught_up_to: Some(41),
        };
        db::upsert_projection(&pool, &projection).await.unwrap();

        let mut loaded = db::get_projection(&pool, &bc.name, &projection.name)
            .await
            .unwrap()
            .unwrap();
        loaded
            .consumed_event_types
            .sort_by(|a, b| a.name.cmp(&b.name));
        let mut expected = vec![et_a, et_b];
        expected.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(loaded.consumed_event_types, expected);
        assert_eq!(loaded.caught_up_to, Some(41));
        assert!(loaded.sync);
    });
}

#[test]
fn upsert_projection_replaces_consumed_event_types() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et_a = seed_event_type(&pool, &bc).await;
        let et_b = seed_event_type(&pool, &bc).await;
        let mut projection = Projection {
            bounded_context: bc.clone(),
            name: unique_name("projection"),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: vec![et_a.clone()],
            sync: false,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();

        // Replace et_a with et_b entirely - the old join row must be gone,
        // not just the new one added.
        projection.consumed_event_types = vec![et_b.clone()];
        db::upsert_projection(&pool, &projection).await.unwrap();

        let loaded = db::get_projection(&pool, &bc.name, &projection.name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(loaded.consumed_event_types, vec![et_b]);
    });
}

#[test]
fn get_projection_is_none_for_an_unknown_name() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;

        let loaded = db::get_projection(&pool, &bc.name, &unique_name("missing"))
            .await
            .unwrap();
        assert_eq!(loaded, None);
    });
}

#[test]
fn round_trips_a_projection_rebuild() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: unique_name("projection"),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: Some(10),
        };
        db::upsert_projection(&pool, &projection).await.unwrap();

        let rebuild = ProjectionRebuild {
            projection: projection.clone(),
            schema: r#"{"properties":{"total":{"type":"number"}}}"#.to_string(),
            schema_version: 2,
            consumed_event_types: vec![et.clone()],
            sync: true,
            caught_up_to: None,
            status: ProjectionRebuildStatus::Pending,
        };
        db::upsert_projection_rebuild(&pool, &rebuild)
            .await
            .unwrap();

        let loaded = db::get_projection_rebuild(
            &pool,
            &bc.name,
            &projection.name,
            ProjectionRebuildStatus::Pending,
        )
        .await
        .unwrap();
        assert_eq!(loaded, Some(rebuild));
    });
}

#[test]
fn get_projection_rebuild_is_none_when_none_staged() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: unique_name("projection"),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();

        let loaded = db::get_projection_rebuild(
            &pool,
            &bc.name,
            &projection.name,
            ProjectionRebuildStatus::Pending,
        )
        .await
        .unwrap();
        assert_eq!(loaded, None);
    });
}

#[test]
fn upsert_projection_rebuild_restages_in_place_when_status_is_unchanged() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: unique_name("projection"),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();
        let first = ProjectionRebuild {
            projection: projection.clone(),
            schema: "{}".to_string(),
            schema_version: 2,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: None,
            status: ProjectionRebuildStatus::Pending,
        };
        db::upsert_projection_rebuild(&pool, &first).await.unwrap();

        // A real restage, `RegisterProjection`'s own repeat-while-still-pending
        // case - same status, new schema_version.
        let restaged = ProjectionRebuild {
            schema_version: 3,
            ..first
        };
        db::upsert_projection_rebuild(&pool, &restaged)
            .await
            .unwrap();

        let loaded = db::get_projection_rebuild(
            &pool,
            &bc.name,
            &projection.name,
            ProjectionRebuildStatus::Pending,
        )
        .await
        .unwrap();
        // One row, not two - restaging at the same status replaces it in place.
        assert_eq!(loaded, Some(restaged));
    });
}

#[test]
fn a_pending_and_a_building_rebuild_coexist_for_the_same_projection() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: unique_name("projection"),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();

        // The deliberate case UniqueRebuildPerProjectionAndStatus names:
        // a build already replaying under an older staged definition...
        let building = ProjectionRebuild {
            projection: projection.clone(),
            schema: r#"{"properties":{"v2":{"type":"number"}}}"#.to_string(),
            schema_version: 2,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: Some(5),
            status: ProjectionRebuildStatus::Building,
        };
        db::upsert_projection_rebuild(&pool, &building)
            .await
            .unwrap();
        // ...and a newer registration arriving mid-build, staged alongside
        // it rather than disturbing it.
        let pending = ProjectionRebuild {
            projection: projection.clone(),
            schema: r#"{"properties":{"v3":{"type":"number"}}}"#.to_string(),
            schema_version: 3,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: None,
            status: ProjectionRebuildStatus::Pending,
        };
        db::upsert_projection_rebuild(&pool, &pending)
            .await
            .unwrap();

        // Both are independently retrievable by their own status - the
        // whole point of the fix (`projection_rebuilds`' own PRIMARY KEY
        // is now `(projection_name, status)`, not `projection_name` alone).
        assert_eq!(
            db::get_projection_rebuild(
                &pool,
                &bc.name,
                &projection.name,
                ProjectionRebuildStatus::Building
            )
            .await
            .unwrap(),
            Some(building)
        );
        assert_eq!(
            db::get_projection_rebuild(
                &pool,
                &bc.name,
                &projection.name,
                ProjectionRebuildStatus::Pending
            )
            .await
            .unwrap(),
            Some(pending)
        );

        // Discarding the pending one leaves the building one untouched.
        db::delete_projection_rebuild(
            &pool,
            &bc.name,
            &projection.name,
            ProjectionRebuildStatus::Pending,
        )
        .await
        .unwrap();
        assert_eq!(
            db::get_projection_rebuild(
                &pool,
                &bc.name,
                &projection.name,
                ProjectionRebuildStatus::Pending
            )
            .await
            .unwrap(),
            None
        );
        assert!(db::get_projection_rebuild(
            &pool,
            &bc.name,
            &projection.name,
            ProjectionRebuildStatus::Building
        )
        .await
        .unwrap()
        .is_some());
    });
}

#[test]
fn transition_projection_rebuild_to_building_replaces_the_pending_row() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: unique_name("projection"),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();
        let pending = ProjectionRebuild {
            projection: projection.clone(),
            schema: "{}".to_string(),
            schema_version: 2,
            consumed_event_types: vec![et],
            sync: false,
            caught_up_to: None,
            status: ProjectionRebuildStatus::Pending,
        };
        db::upsert_projection_rebuild(&pool, &pending)
            .await
            .unwrap();

        let building = ProjectionRebuild {
            status: ProjectionRebuildStatus::Building,
            ..pending
        };
        db::transition_projection_rebuild_to_building(&pool, &building)
            .await
            .unwrap();

        // The pending row is gone - not a second row alongside the new
        // building one.
        assert_eq!(
            db::get_projection_rebuild(
                &pool,
                &bc.name,
                &projection.name,
                ProjectionRebuildStatus::Pending
            )
            .await
            .unwrap(),
            None
        );
        assert_eq!(
            db::get_projection_rebuild(
                &pool,
                &bc.name,
                &projection.name,
                ProjectionRebuildStatus::Building
            )
            .await
            .unwrap(),
            Some(building)
        );
    });
}

#[test]
fn delete_projection_rebuild_removes_it() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let projection = Projection {
            bounded_context: bc.clone(),
            name: unique_name("projection"),
            schema: r#"{"properties":{}}"#.to_string(),
            schema_version: 1,
            consumed_event_types: Vec::new(),
            sync: false,
            caught_up_to: None,
        };
        db::upsert_projection(&pool, &projection).await.unwrap();
        let rebuild = ProjectionRebuild {
            projection: projection.clone(),
            schema: "{}".to_string(),
            schema_version: 2,
            consumed_event_types: vec![et],
            sync: false,
            caught_up_to: None,
            status: ProjectionRebuildStatus::Pending,
        };
        db::upsert_projection_rebuild(&pool, &rebuild)
            .await
            .unwrap();

        db::delete_projection_rebuild(
            &pool,
            &bc.name,
            &projection.name,
            ProjectionRebuildStatus::Pending,
        )
        .await
        .unwrap();

        let loaded = db::get_projection_rebuild(
            &pool,
            &bc.name,
            &projection.name,
            ProjectionRebuildStatus::Pending,
        )
        .await
        .unwrap();
        assert_eq!(loaded, None);
    });
}

// ---------------------------------------------------------------------
// Schema-per-bounded-context provisioning / hard deletion
// (docs/architecture.md §2.2.2, `DeleteBoundedContext` in
// specs/skilj.allium)
// ---------------------------------------------------------------------

/// `insert_bounded_context` provisions a real, independently-usable
/// `bc_<name>` schema, not just the `bounded_contexts` registry row -
/// every per-context table is there and empty, ready for
/// `seed_event_type`/etc. to write into without any further setup. Two
/// contexts get two entirely separate schemas: an `EventType` registered
/// in one is invisible from the other's, the isolation the whole design
/// exists for (docs/architecture.md §2.2.2).
#[test]
fn provisioning_creates_an_independently_queryable_schema_per_context() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc_a = seed_bounded_context(&pool).await;
        let bc_b = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc_a).await;

        // Visible from the context it was registered in...
        assert_eq!(
            db::get_event_type(&pool, &bc_a.name, &et.name)
                .await
                .unwrap(),
            Some(et.clone())
        );
        // ...but not from a different context's own, separate schema.
        assert_eq!(
            db::get_event_type(&pool, &bc_b.name, &et.name)
                .await
                .unwrap(),
            None
        );
    });
}

/// `hard_delete_bounded_context` removes the schema (a query against it
/// afterward finds nothing - the same `None` a never-provisioned context
/// would give) and the `bounded_contexts` registry row together, and its
/// `role_access_mappings`/`access_token_index` rows are gone too, via the
/// `ON DELETE CASCADE` FK - no separate cleanup query needed. Afterward,
/// the same name is free to be reused by a fresh `insert_bounded_context`
/// call - a genuinely different context, not the old one reappearing.
#[test]
fn hard_delete_drops_the_schema_and_cascades_the_registry_row() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let role = seed_role(&pool, false).await;
        let mapping = seed_active_role_access_mapping(&pool, &role, &bc, AccessLevel::Admin).await;
        let token = ExternalEventToken {
            id: generate_token_id(),
            secret: generate_token_secret(),
            status: TokenStatus::Active,
            created_at: test_now(),
            revoked_at: None,
            event_type: et.clone(),
            scope: None,
        };
        db::insert_external_event_token(&pool, &token)
            .await
            .unwrap();

        db::hard_delete_bounded_context(&pool, &bc.name)
            .await
            .unwrap();

        assert_eq!(
            db::get_bounded_context(&pool, &bc.name).await.unwrap(),
            None
        );
        assert_eq!(
            db::get_active_role_access_mapping(&pool, &mapping.role.id, &bc.name)
                .await
                .unwrap(),
            None
        );
        assert_eq!(db::access_token_kind(&pool, &token.id).await.unwrap(), None);

        // The name is free again - a fresh context, not the old one back.
        let reused = BoundedContext {
            name: bc.name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &reused).await.unwrap();
        assert_eq!(
            db::get_bounded_context(&pool, &bc.name).await.unwrap(),
            Some(reused)
        );
        // The reused schema starts genuinely empty - the old EventType
        // doesn't resurface under the recycled name.
        assert_eq!(
            db::get_event_type(&pool, &bc.name, &et.name).await.unwrap(),
            None
        );
    });
}

/// A read against a bounded context that has been hard-deleted (or never
/// existed) must come back as an `Err`, not a panic: the caller's own
/// access check can pass just before a concurrent `DeleteBoundedContext`
/// lands, and a panic here takes the whole request task down with it
/// instead of surfacing as an ordinary error response.
#[test]
fn reading_a_hard_deleted_bounded_context_errors_instead_of_panicking() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        db::hard_delete_bounded_context(&pool, &bc.name)
            .await
            .unwrap();

        assert!(db::list_events_for_bounded_context(&pool, &bc.name)
            .await
            .is_err());
    });
}

/// `list_events`/`list_events_from` take an `event_type_name` the caller
/// already resolved from somewhere else - a `hard_delete_bounded_context`
/// race can make it stale for the same reason a stale `bounded_context`
/// name can (`get_event_type` internally rechecks the registry row and
/// comes back `None` once it's gone) - but a plain unregistered name on a
/// bounded context that's still very much alive reaches the exact same
/// `require_event_type` call and used to panic identically. Cheaper to
/// trigger deterministically than the true race, and just as real a way
/// to reach it: any caller passing a name that was never registered for
/// this bounded context, not only a raced-out one.
#[test]
fn listing_events_for_an_unregistered_event_type_errors_instead_of_panicking() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;

        assert!(db::list_events(&pool, &bc.name, "NoSuchEventType")
            .await
            .is_err());
        assert!(db::list_events_from(&pool, &bc.name, "NoSuchEventType", 0)
            .await
            .is_err());
    });
}

// --- EncryptionKey (§SubjectErasure) ---

#[test]
fn get_or_create_encryption_key_provisions_once_then_reuses_the_same_row() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let master = EncryptionMasterKey::from_bytes([1u8; 32]);

        let (first, first_id, _data_key) =
            db::get_or_create_encryption_key(&pool, &bc.name, "user", "42", &master)
                .await
                .unwrap();
        assert_eq!(first.subject_key, "user");
        assert_eq!(first.subject_value, "42");
        assert_eq!(first.status, EncryptionKeyStatus::Active);
        assert_eq!(first.destroyed_at, None);

        let (second, second_id, _data_key) =
            db::get_or_create_encryption_key(&pool, &bc.name, "user", "42", &master)
                .await
                .unwrap();
        assert_eq!(first_id, second_id);
        assert_eq!(first, second);
    });
}

#[test]
fn get_active_encryption_key_is_none_for_an_unknown_subject() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        assert_eq!(
            db::get_active_encryption_key(&pool, &bc.name, "user", "nope")
                .await
                .unwrap(),
            None
        );
    });
}

/// Real crypto-shredding, not just a status flag: destroying nulls the
/// wrapped key material too (see `db::destroy_encryption_key`'s own doc
/// comment), and "a later event for the same subject provisions a new
/// active key; it does not revive this one" (`entity EncryptionKey`'s own
/// doc comment) - a genuinely different row, not the destroyed one back.
#[test]
fn destroy_encryption_key_is_irreversible_and_a_later_provision_is_a_new_key() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let master = EncryptionMasterKey::from_bytes([2u8; 32]);

        let (_key, original_id, _data_key) =
            db::get_or_create_encryption_key(&pool, &bc.name, "user", "7", &master)
                .await
                .unwrap();
        db::destroy_encryption_key(&pool, &bc.name, "user", "7", test_now())
            .await
            .unwrap();

        assert_eq!(
            db::get_active_encryption_key(&pool, &bc.name, "user", "7")
                .await
                .unwrap(),
            None
        );

        let (revived, revived_id, _data_key) =
            db::get_or_create_encryption_key(&pool, &bc.name, "user", "7", &master)
                .await
                .unwrap();
        assert_eq!(revived.status, EncryptionKeyStatus::Active);
        assert_ne!(revived_id, original_id);
    });
}

// --- get_active_data_key (real decrypt-on-read) ---

/// `get_active_data_key` round-trips a provisioned key back to the
/// identical `DataKey` - proven indirectly (`DataKey` has no
/// `PartialEq`/`Debug` by design) by encrypting under the freshly
/// provisioned key and decrypting under the one this function returns.
#[test]
fn get_active_data_key_round_trips_a_provisioned_key() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let master = EncryptionMasterKey::from_bytes([3u8; 32]);

        let (_key, _id, provisioned) =
            db::get_or_create_encryption_key(&pool, &bc.name, "user", "42", &master)
                .await
                .unwrap();
        let ciphertext = skilj_core::encryption::encrypt_leaf(&provisioned, "hello");

        let fetched = db::get_active_data_key(&pool, &bc.name, "user", "42", &master)
            .await
            .unwrap()
            .expect("the key just provisioned is active");
        assert_eq!(
            skilj_core::encryption::decrypt_leaf(&fetched, &ciphertext).unwrap(),
            "hello"
        );
    });
}

#[test]
fn get_active_data_key_is_none_for_an_unknown_subject() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let master = EncryptionMasterKey::from_bytes([4u8; 32]);
        assert!(
            db::get_active_data_key(&pool, &bc.name, "user", "nope", &master)
                .await
                .unwrap()
                .is_none()
        );
    });
}

/// A destroyed key's own `wrapped_key`/`wrap_nonce` are nulled (real
/// crypto-shredding, see `destroy_encryption_key`'s own doc comment) -
/// `get_active_data_key` returns `None`, the identical treatment an
/// unknown subject gets, not an error.
#[test]
fn get_active_data_key_is_none_for_a_destroyed_key() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let master = EncryptionMasterKey::from_bytes([5u8; 32]);

        db::get_or_create_encryption_key(&pool, &bc.name, "user", "42", &master)
            .await
            .unwrap();
        db::destroy_encryption_key(&pool, &bc.name, "user", "42", test_now())
            .await
            .unwrap();

        assert!(
            db::get_active_data_key(&pool, &bc.name, "user", "42", &master)
                .await
                .unwrap()
                .is_none()
        );
    });
}

// --- list_active_data_keys_for_subject_value (real decrypt-on-read for
// Projections - no `subject_key` known ahead of time) ---

#[test]
fn list_active_data_keys_for_subject_value_finds_every_namespace_for_a_subject() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let master = EncryptionMasterKey::from_bytes([6u8; 32]);

        // Two different subject_key namespaces, the same subject_value -
        // both are real candidates for a projection instance keyed "42".
        db::get_or_create_encryption_key(&pool, &bc.name, "user", "42", &master)
            .await
            .unwrap();
        db::get_or_create_encryption_key(&pool, &bc.name, "employee", "42", &master)
            .await
            .unwrap();
        // An unrelated subject_value never matches.
        db::get_or_create_encryption_key(&pool, &bc.name, "user", "99", &master)
            .await
            .unwrap();

        let data_keys =
            db::list_active_data_keys_for_subject_value(&pool, &bc.name, "42", Some(&master))
                .await
                .unwrap();
        assert_eq!(data_keys.len(), 2);

        // Each resolved key genuinely decrypts something sealed under its
        // own real DataKey - proven indirectly (`DataKey` has no
        // `PartialEq`/`Debug` by design).
        for data_key in &data_keys {
            let ciphertext = skilj_core::encryption::encrypt_leaf(data_key, "hello");
            assert_eq!(
                skilj_core::encryption::decrypt_leaf(data_key, &ciphertext).unwrap(),
                "hello"
            );
        }
    });
}

#[test]
fn list_active_data_keys_for_subject_value_excludes_a_destroyed_key() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let master = EncryptionMasterKey::from_bytes([7u8; 32]);

        db::get_or_create_encryption_key(&pool, &bc.name, "user", "42", &master)
            .await
            .unwrap();
        db::destroy_encryption_key(&pool, &bc.name, "user", "42", test_now())
            .await
            .unwrap();

        let data_keys =
            db::list_active_data_keys_for_subject_value(&pool, &bc.name, "42", Some(&master))
                .await
                .unwrap();
        assert!(data_keys.is_empty());
    });
}

#[test]
fn list_active_data_keys_for_subject_value_is_empty_for_an_unknown_subject_with_no_master_key_needed(
) {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;

        let data_keys = db::list_active_data_keys_for_subject_value(&pool, &bc.name, "nope", None)
            .await
            .unwrap();
        assert!(data_keys.is_empty());
    });
}

/// A subject that *does* have an active key, queried with no master key
/// configured at all, is a real, actionable configuration error - not a
/// silent empty result - the identical precedent
/// `resolve_encryption_keys`/`resolve_data_keys_for_reading` already set.
#[test]
fn list_active_data_keys_for_subject_value_errors_when_master_key_missing_but_a_key_exists() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let master = EncryptionMasterKey::from_bytes([8u8; 32]);

        db::get_or_create_encryption_key(&pool, &bc.name, "user", "42", &master)
            .await
            .unwrap();

        // `Vec<DataKey>` has no `Debug` (`DataKey` deliberately doesn't),
        // so `unwrap_err()` can't be used here - matched by hand instead.
        match db::list_active_data_keys_for_subject_value(&pool, &bc.name, "42", None).await {
            Err(err) => assert_eq!(
                err.code(),
                skilj_core::encryption::Error::MasterKeyNotConfigured.code()
            ),
            Ok(_) => panic!("expected MasterKeyNotConfigured"),
        }
    });
}

/// `schedule_position`/`last_fired_at` belong to the scheduler once a row
/// exists. Re-registration (every instance re-registers every type at
/// startup) builds its `EventType` from an unlocked read; if the scheduler
/// fires between that read and the write, writing the stale copies back
/// would rewind the position and re-fire the occurrence. The one moment
/// registration does set the position - scheduling newly enabled - still
/// works.
#[test]
fn re_registration_never_rewinds_what_the_scheduler_advanced() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let et = seed_event_type(&pool, &bc).await;
        let enabled_at = test_now() - chrono::Duration::hours(1);
        let enabled = EventType {
            system_triggered_allowed: true,
            system_triggered_schedule: Some("0 0 * * * * *".to_string()),
            missed_occurrence_policy: Some(
                skilj_core::event_store::MissedOccurrencePolicy::ReplayBacklog,
            ),
            schedule_position: Some(enabled_at),
            ..et.clone()
        };
        db::upsert_event_type(&pool, &enabled).await.unwrap();
        let stale = db::get_event_type(&pool, &bc.name, &et.name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stale.schedule_position,
            Some(enabled_at),
            "opting in sets it"
        );

        // The scheduler fires, after registration's read.
        let fired_at = test_now();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{}\".event_types SET schedule_position = $1, last_fired_at = $1 \
             WHERE name = $2",
            bc.name
        )))
        .bind(fired_at)
        .bind(&et.name)
        .execute(&pool)
        .await
        .unwrap();

        // Registration writes what it read.
        db::upsert_event_type(&pool, &stale).await.unwrap();
        let after = db::get_event_type(&pool, &bc.name, &et.name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.schedule_position, Some(fired_at));
        assert_eq!(after.last_fired_at, Some(fired_at));
    });
}

/// docs/architecture.md §87: `delete_expired_idempotency_keys` removes
/// only keys recorded before the cutoff, at most `batch` per call.
#[test]
fn expired_idempotency_keys_are_deleted_in_bounded_batches() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let now = test_now();
        for (key, age_minutes) in [("a", 120), ("b", 90), ("c", 61), ("fresh", 5)] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO \"bc_{}\".idempotency_keys \
                 (command_type_name, client_id, idempotency_key, triggered_event_sequences, created_at) \
                 VALUES ('Withdraw', 'client', $1, '{{1}}', $2)",
                bc.name
            )))
            .bind(key)
            .bind(now - chrono::Duration::minutes(age_minutes))
            .execute(&pool)
            .await
            .unwrap();
        }

        let cutoff = now - chrono::Duration::hours(1);
        let mut deleted = Vec::new();
        for _ in 0..3 {
            deleted.push(
                db::delete_expired_idempotency_keys(&pool, &bc.name, cutoff, 2)
                    .await
                    .unwrap(),
            );
        }
        assert_eq!(deleted, vec![2, 1, 0]);

        let remaining: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT idempotency_key FROM \"bc_{}\".idempotency_keys",
            bc.name
        )))
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(remaining, vec!["fresh".to_string()]);
    });
}

/// docs/architecture.md §107: `get_or_create_encryption_key`'s insert can
/// lose to a concurrent provisioner whose key a concurrent `forgetSubject`
/// then destroys before the loser re-reads it - no active key at all. That
/// used to `expect` and panic; it provisions again instead. Reproduced with
/// a trigger that skips the first insert (so it neither inserts nor
/// conflicts) while no active key exists.
#[test]
fn get_or_create_encryption_key_survives_a_key_vanishing_mid_provision() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let schema = format!("\"bc_{}\"", bc.name);
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE SEQUENCE {schema}.skip_once;
             CREATE FUNCTION {schema}.skip_first_insert() RETURNS trigger AS $$
             BEGIN
                 IF nextval('{schema}.skip_once') = 1 THEN RETURN NULL; END IF;
                 RETURN NEW;
             END $$ LANGUAGE plpgsql;
             CREATE TRIGGER skip_first_insert BEFORE INSERT ON {schema}.encryption_keys
                 FOR EACH ROW EXECUTE FUNCTION {schema}.skip_first_insert();"
        )))
        .execute(&pool)
        .await
        .unwrap();

        let master = EncryptionMasterKey::from_bytes([4u8; 32]);
        let (key, _id, _data_key) =
            db::get_or_create_encryption_key(&pool, &bc.name, "user", "7", &master)
                .await
                .expect("provisioning must not fail (or panic) when the key vanished");
        assert_eq!(key.status, EncryptionKeyStatus::Active);
        assert!(db::get_active_encryption_key(&pool, &bc.name, "user", "7")
            .await
            .unwrap()
            .is_some());
    });
}
