//! End-to-end regression test for the cross-tenant projection read fix
//! (docs/architecture.md's own write-up of this pass): before
//! `RoleAccessMapping.scope`/`Projection::OWNER_TAG_KEY` existed,
//! `ProjectionQuery`'s only access check was "does the caller hold any
//! active `RoleAccessMapping` on this bounded context" -
//! `require_read_mapping` (`skilj-graphql`) - with no check that the
//! queried instance actually belonged to that caller. In a bounded
//! context shared by several tenants (skilj-helpdesk's own motivating
//! case: every company's tickets live in one `helpdesk` bounded context,
//! `company_id` a payload field rather than a tenancy boundary), any
//! caller with read access could query any instance by key.
//!
//! Two layers, exercised together against real Postgres: the DB layer
//! (`db::insert_event_and_update_sync_projections`/
//! `db::get_projection_state_and_owner`, via `apply_projection_fold_update`)
//! that derives and persists each instance's own `owner` from its
//! folding events' tags, and the pure enforcement
//! (`projections::query_projection`) that rejects a `scope`-restricted
//! grant querying an instance whose derived owner doesn't match. Same
//! "hand-rolled `ProjectionDispatcher` test double, no `skilj-graphql`/
//! `skilj-rest` involved" shape `sync_projections.rs` already uses - see
//! its own doc comment for the shared provisioning harness this file
//! duplicates rather than extracts (the same call every prior pass in
//! this project already made).

use chrono::{SubsecRound, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::ProjectionDispatcher;
use skilj_core::projections::{self, Projection, ProjectionAccessScope};
use skilj_core::shared::{generate_token_id, Metadata, Tag, TagMapping};

/// Two owner-scoped projections, both keyed by the payload's own
/// `ticket_id` and folding `"TicketOpened"` (tagged `company`, the
/// declared owner dimension) and `"TicketCommented"` (untagged, proving
/// an owner already established survives a fold from an event that
/// carries none): `"TicketSummary"` declares no required team - the
/// pre-issue-#17 baseline every test above this line exercises unchanged
/// - and `"StaffTicketSummary"` additionally declares `TEAM_ONLY =
/// Some("staff")`, for the composability tests below.
struct TestDispatcher;

impl ProjectionDispatcher for TestDispatcher {
    fn keys(
        &self,
        _bounded_context: &str,
        projection_name: &str,
        event: &Event,
    ) -> Option<Vec<String>> {
        match projection_name {
            "TicketSummary" | "StaffTicketSummary" => {
                let payload: serde_json::Value =
                    serde_json::from_str(&event.payload).expect("test payload is always JSON");
                Some(vec![payload["ticket_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string()])
            }
            _ => None,
        }
    }

    fn project(
        &self,
        _bounded_context: &str,
        projection_name: &str,
        state_json: &str,
        _event: &Event,
        _key: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        match projection_name {
            "TicketSummary" | "StaffTicketSummary" => {
                let current: i64 = state_json.parse().unwrap_or(0);
                Some(Ok((current + 1).to_string()))
            }
            _ => None,
        }
    }

    fn default_state(&self, _bounded_context: &str, projection_name: &str) -> Option<String> {
        match projection_name {
            "TicketSummary" | "StaffTicketSummary" => Some("0".to_string()),
            _ => None,
        }
    }

    fn owner_tag_key(
        &self,
        _bounded_context: &str,
        projection_name: &str,
    ) -> Option<Option<&'static str>> {
        match projection_name {
            "TicketSummary" | "StaffTicketSummary" => Some(Some("company")),
            _ => None,
        }
    }

    fn team_only(
        &self,
        _bounded_context: &str,
        projection_name: &str,
    ) -> Option<Option<&'static str>> {
        match projection_name {
            "TicketSummary" => Some(None),
            "StaffTicketSummary" => Some(Some("staff")),
            _ => None,
        }
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---
// Identical harness to `sync_projections.rs` - see its own doc comment.

struct TestDb {
    pool: Pool,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for projection_owner_scoping tests")
    })
}

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
        return Some(TestDb {
            pool,
            _embedded: None,
        });
    }

    let mut server = postgresql_embedded::PostgreSQL::default();
    if let Err(e) = server.setup().await {
        eprintln!(
            "skipping: DATABASE_URL not set and embedded PostgreSQL setup failed \
             (no network egress to fetch the binary, or a missing system library \
             like libxml2 it links against): {e}"
        );
        return None;
    }
    if let Err(e) = server.start().await {
        eprintln!("skipping: embedded PostgreSQL failed to start: {e}");
        return None;
    }
    let database_name = "skilj_projection_owner_scoping_test";
    if let Err(e) = server.create_database(database_name).await {
        eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
        return None;
    }
    let pool = match db::connect(&server.settings().url(database_name)).await {
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
    Some(TestDb {
        pool,
        _embedded: Some(server),
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

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

/// `"TicketOpened"`, tagged `company` from its own `company_id` field -
/// the source of every `"TicketSummary"` instance's own derived owner.
async fn seed_ticket_opened(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "TicketOpened".to_string(),
        schema: r#"{"properties":{"ticket_id":{"type":"string"},"company_id":{"type":"string"}}}"#
            .to_string(),
        schema_version: 1,
        tag_mappings: vec![TagMapping {
            key: "company".to_string(),
            field: "company_id".to_string(),
        }],
        owner_tag_key: Some("company".to_string()),
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

/// `"TicketCommented"` - deliberately untagged, so folding one proves an
/// already-established `owner` survives an event that carries none.
async fn seed_ticket_commented(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "TicketCommented".to_string(),
        schema: r#"{"properties":{"ticket_id":{"type":"string"}}}"#.to_string(),
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

/// Parametrized by `name` so the same helper seeds either
/// `"TicketSummary"` (no required team) or `"StaffTicketSummary"`
/// (`TEAM_ONLY = Some("staff")`, resolved via `TestDispatcher::team_only`).
/// There's no field on `Projection` itself to set, exactly as
/// `owner_tag_key` already isn't.
async fn seed_projection(
    pool: &Pool,
    bc: &BoundedContext,
    name: &str,
    consumed: Vec<EventType>,
) -> Projection {
    let projection = Projection {
        bounded_context: bc.clone(),
        name: name.to_string(),
        schema: r#"{"properties":{}}"#.to_string(),
        schema_version: 1,
        consumed_event_types: consumed,
        sync: true,
        caught_up_to: None,
    };
    db::upsert_projection(pool, &projection).await.unwrap();
    projection
}

async fn insert_ticket_opened(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    ticket_id: &str,
    company_id: &str,
) {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let e = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: serde_json::json!({ "ticket_id": ticket_id, "company_id": company_id })
            .to_string(),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: "test".to_string(),
            created_at: test_now(),
        },
        sequence: seq,
        tags: vec![Tag {
            key: "company".to_string(),
            value: Some(company_id.to_string()),
        }],
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event_and_update_sync_projections(
        pool,
        &e,
        None,
        &TestDispatcher,
        &[],
        &skilj_core::event_store::EventBroadcaster::new(16),
        &skilj_core::event_cache::EventCache::new(1000),
    )
    .await
    .unwrap();
}

async fn insert_ticket_commented(
    pool: &Pool,
    bc: &BoundedContext,
    et: &EventType,
    ticket_id: &str,
) {
    let seq = db::next_sequence(pool, &bc.name).await.unwrap();
    let e = Event {
        bounded_context: bc.clone(),
        event_type: et.clone(),
        payload: serde_json::json!({ "ticket_id": ticket_id }).to_string(),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: "test".to_string(),
            created_at: test_now(),
        },
        sequence: seq,
        tags: Vec::new(), // untagged - TagMapping-derived, and this type declares none
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event_and_update_sync_projections(
        pool,
        &e,
        None,
        &TestDispatcher,
        &[],
        &skilj_core::event_store::EventBroadcaster::new(16),
        &skilj_core::event_cache::EventCache::new(1000),
    )
    .await
    .unwrap();
}

fn scoped_mapping(bc: &BoundedContext, scope: Option<&str>) -> RoleAccessMapping {
    named_mapping(bc, "Reader", scope)
}

/// `scoped_mapping`'s own general form - a `Role.name` lever, for the
/// team-gate tests below (`scoped_mapping` keeps its own signature, still
/// always naming its Role `"Reader"`, since no test before this pass
/// cared what it was called).
fn named_mapping(bc: &BoundedContext, role_name: &str, scope: Option<&str>) -> RoleAccessMapping {
    RoleAccessMapping {
        role: Role {
            id: unique_name("role"),
            external_subject: unique_name("subject"),
            name: role_name.to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        },
        bounded_context: bc.clone(),
        level: AccessLevel::Read,
        can_read_sensitive: false,
        scope: scope.map(str::to_string),
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    }
}

/// DB layer: a `TicketOpened` event tagged `company` gives its own
/// instance a real, persisted `owner` - `apply_projection_fold_update`/
/// `get_projection_state_and_owner`, exercised through the real fold
/// path (`db::insert_event_and_update_sync_projections`), not called
/// directly.
#[test]
fn folding_a_tagged_event_derives_and_persists_the_instance_owner() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let opened = seed_ticket_opened(&pool, &bc).await;
        seed_projection(&pool, &bc, "TicketSummary", vec![opened.clone()]).await;

        insert_ticket_opened(&pool, &bc, &opened, "t1", "company-a").await;
        insert_ticket_opened(&pool, &bc, &opened, "t2", "company-b").await;

        let (state, owner) =
            db::get_projection_state_and_owner(&pool, &bc.name, "TicketSummary", "t1")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(state, "1");
        assert_eq!(owner.as_deref(), Some("company-a"));

        let (_, owner) = db::get_projection_state_and_owner(&pool, &bc.name, "TicketSummary", "t2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(owner.as_deref(), Some("company-b"));

        // A key nothing has touched: no row at all, not a proven owner.
        assert!(
            db::get_projection_state_and_owner(&pool, &bc.name, "TicketSummary", "t3")
                .await
                .unwrap()
                .is_none()
        );
    });
}

/// A later fold from an untagged event type leaves an already-established
/// owner untouched - `apply_projection_fold_update`'s own "an event
/// lacking the tag never clears an owner" contract.
#[test]
fn folding_an_untagged_event_leaves_an_established_owner_untouched() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let opened = seed_ticket_opened(&pool, &bc).await;
        let commented = seed_ticket_commented(&pool, &bc).await;
        seed_projection(
            &pool,
            &bc,
            "TicketSummary",
            vec![opened.clone(), commented.clone()],
        )
        .await;

        insert_ticket_opened(&pool, &bc, &opened, "t1", "company-a").await;
        insert_ticket_commented(&pool, &bc, &commented, "t1").await;

        let (state, owner) =
            db::get_projection_state_and_owner(&pool, &bc.name, "TicketSummary", "t1")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(state, "2"); // both events folded
        assert_eq!(owner.as_deref(), Some("company-a")); // owner survived
    });
}

/// The vulnerability this whole pass fixes, end to end: real derived
/// owners from real Postgres rows, fed into `query_projection`'s real
/// enforcement. Before `scope`/`OWNER_TAG_KEY` existed, any active
/// `RoleAccessMapping` on this bounded context - `company-a`'s own
/// included - could read `company-b`'s ticket by key, and an unscoped
/// staff grant still can, deliberately.
#[test]
fn a_scoped_grant_can_only_read_its_own_companys_ticket() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let opened = seed_ticket_opened(&pool, &bc).await;
        let projection = seed_projection(&pool, &bc, "TicketSummary", vec![opened.clone()]).await;

        insert_ticket_opened(&pool, &bc, &opened, "t1", "company-a").await;
        insert_ticket_opened(&pool, &bc, &opened, "t2", "company-b").await;

        let (_, owner_t1) =
            db::get_projection_state_and_owner(&pool, &bc.name, "TicketSummary", "t1")
                .await
                .unwrap()
                .unwrap();
        let (_, owner_t2) =
            db::get_projection_state_and_owner(&pool, &bc.name, "TicketSummary", "t2")
                .await
                .unwrap()
                .unwrap();

        let company_a = scoped_mapping(&bc, Some("company-a"));

        // Its own ticket: allowed.
        projections::query_projection(
            &company_a,
            &projection,
            "t1",
            None,
            false,
            ProjectionAccessScope {
                declares_owner: true,
                instance_owner: owner_t1.as_deref(),
                team_only: None,
            },
            "ok".into(),
        )
        .unwrap();

        // Company B's ticket: rejected - the concrete cross-tenant read
        // this pass closes.
        let err = projections::query_projection(
            &company_a,
            &projection,
            "t2",
            None,
            false,
            ProjectionAccessScope {
                declares_owner: true,
                instance_owner: owner_t2.as_deref(),
                team_only: None,
            },
            "leaked".into(),
        )
        .unwrap_err();
        assert_eq!(err.code(), access_control::Error::GrantScopeMismatch.code());

        // A never-touched key: fail-closed, same rejection as a proven
        // mismatch - real DB-confirmed `None`, not a hand-constructed one.
        let (_, owner_t3): (String, Option<String>) =
            db::get_projection_state_and_owner(&pool, &bc.name, "TicketSummary", "t3")
                .await
                .unwrap()
                .unwrap_or_default();
        let err = projections::query_projection(
            &company_a,
            &projection,
            "t3",
            None,
            false,
            ProjectionAccessScope {
                declares_owner: true,
                instance_owner: owner_t3.as_deref(),
                team_only: None,
            },
            "leaked".into(),
        )
        .unwrap_err();
        assert_eq!(err.code(), access_control::Error::GrantScopeMismatch.code());

        // An unscoped (staff) grant is unrestricted, deliberately -
        // `RoleAccessMapping.scope`'s own doc comment.
        let staff = scoped_mapping(&bc, None);
        for (key, owner) in [("t1", owner_t1.as_deref()), ("t2", owner_t2.as_deref())] {
            projections::query_projection(
                &staff,
                &projection,
                key,
                None,
                false,
                ProjectionAccessScope {
                    declares_owner: true,
                    instance_owner: owner,
                    team_only: None,
                },
                "ok".into(),
            )
            .unwrap();
        }
    });
}

/// Codeberg issue #17's own gap and fix, end to end: `private_fields`
/// (0.0.3) protects `queryEvents`/`fetchCommands` - surfaces no
/// `Write`-level Role can reach anyway - while leaving `ProjectionQuery`,
/// the one surface such a Role *can* reach, exactly as open as before
/// issue #16 was filed. `StaffTicketSummary` declares both a required
/// team (`"staff"`) and an owner dimension (`"company"`) - independent
/// checks, both of which must hold.
#[test]
fn a_team_only_projection_rejects_a_role_not_on_the_team() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let opened = seed_ticket_opened(&pool, &bc).await;
        let projection =
            seed_projection(&pool, &bc, "StaffTicketSummary", vec![opened.clone()]).await;

        insert_ticket_opened(&pool, &bc, &opened, "t1", "company-a").await;

        let (_, owner_t1) =
            db::get_projection_state_and_owner(&pool, &bc.name, "StaffTicketSummary", "t1")
                .await
                .unwrap()
                .unwrap();

        // A staff Role, unscoped: reads fine - on the team, and no owner
        // restriction narrows it further.
        let staff = named_mapping(&bc, "staff", None);
        projections::query_projection(
            &staff,
            &projection,
            "t1",
            None,
            false,
            ProjectionAccessScope {
                declares_owner: true,
                instance_owner: owner_t1.as_deref(),
                team_only: Some("staff"),
            },
            "ok".into(),
        )
        .unwrap();

        // A Role with any other name: rejected outright, regardless of
        // scope - team membership, not ownership, is what failed.
        let customer = named_mapping(&bc, "Reader", Some("company-a"));
        let err = projections::query_projection(
            &customer,
            &projection,
            "t1",
            None,
            false,
            ProjectionAccessScope {
                declares_owner: true,
                instance_owner: owner_t1.as_deref(),
                team_only: Some("staff"),
            },
            "leaked".into(),
        )
        .unwrap_err();
        assert_eq!(err.code(), access_control::Error::NotOnRequiredTeam.code());
    });
}

/// Composability: `TEAM_ONLY` and `OWNER_TAG_KEY` are independent checks
/// on the same projection, and both must hold - neither substitutes for
/// the other (`docs/architecture.md`'s own write-up of this pass, and
/// `Projection::TEAM_ONLY`'s own doc comment).
#[test]
fn a_team_only_projection_also_enforces_its_own_owner_scope_independently() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let opened = seed_ticket_opened(&pool, &bc).await;
        let projection =
            seed_projection(&pool, &bc, "StaffTicketSummary", vec![opened.clone()]).await;

        insert_ticket_opened(&pool, &bc, &opened, "t1", "company-a").await;
        insert_ticket_opened(&pool, &bc, &opened, "t2", "company-b").await;

        let (_, owner_t1) =
            db::get_projection_state_and_owner(&pool, &bc.name, "StaffTicketSummary", "t1")
                .await
                .unwrap()
                .unwrap();
        let (_, owner_t2) =
            db::get_projection_state_and_owner(&pool, &bc.name, "StaffTicketSummary", "t2")
                .await
                .unwrap()
                .unwrap();

        // A staff Role scoped to company-a: on the team AND owns t1 -
        // both checks hold, reads fine.
        let staff_company_a = named_mapping(&bc, "staff", Some("company-a"));
        projections::query_projection(
            &staff_company_a,
            &projection,
            "t1",
            None,
            false,
            ProjectionAccessScope {
                declares_owner: true,
                instance_owner: owner_t1.as_deref(),
                team_only: Some("staff"),
            },
            "ok".into(),
        )
        .unwrap();

        // The same staff Role, company-b's ticket: on the team, but the
        // owner check alone rejects it - team membership was never in
        // question here.
        let err = projections::query_projection(
            &staff_company_a,
            &projection,
            "t2",
            None,
            false,
            ProjectionAccessScope {
                declares_owner: true,
                instance_owner: owner_t2.as_deref(),
                team_only: Some("staff"),
            },
            "leaked".into(),
        )
        .unwrap_err();
        assert_eq!(err.code(), access_control::Error::GrantScopeMismatch.code());

        // A non-staff Role scoped to company-a, querying its own
        // company's ticket: owns t1, but the team check alone rejects it
        // - ownership was never in question here either.
        let customer_company_a = named_mapping(&bc, "Reader", Some("company-a"));
        let err = projections::query_projection(
            &customer_company_a,
            &projection,
            "t1",
            None,
            false,
            ProjectionAccessScope {
                declares_owner: true,
                instance_owner: owner_t1.as_deref(),
                team_only: Some("staff"),
            },
            "leaked".into(),
        )
        .unwrap_err();
        assert_eq!(err.code(), access_control::Error::NotOnRequiredTeam.code());
    });
}
