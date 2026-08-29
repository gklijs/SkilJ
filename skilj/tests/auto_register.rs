//! End-to-end tests for `SkiljBuilder::auto_register()` +
//! `#[skilj::auto_register]` (docs/architecture.md §1.3.3) - a real
//! `.build()` against a real Postgres, proving a macro-tagged
//! `EventType`/`CommandType`/`Projection` impl reaches the same
//! `event_types`/`command_types`/`projections` tables an equivalent
//! manual `.bounded_context(name).event_type::<T>()` chain would, with no
//! explicit per-type call at all - plus a fourth macro-tagged `Snapshot`
//! fixture, proven a different way (`snapshot_dispatcher().snapshot_names(...)`,
//! not the reconciliation report) since `Snapshot` has no metadata table/
//! reconciliation surface of its own (see `Snapshot`'s own doc comment,
//! `skilj-core::plugin`) - and that the defaulted `SkiljBuilder`
//! (part 1 of the same pass) lands manual registration under
//! `plugin::DEFAULT_BOUNDED_CONTEXT` too, when `.bounded_context(...)` is
//! never called. Same `DATABASE_URL`-then-embedded-Postgres-then-skip
//! harness as `skilj/tests/command_trigger.rs` - see that file's own doc
//! comment for the details, not repeated a third time here.
//!
//! Every bounded context name used below is a compile-time literal, not
//! `unique_name(...)` like most other `skilj/tests/*.rs` fixtures use for
//! their own bounded context - `EventType`/`CommandType`/`Projection::
//! BOUNDED_CONTEXT` is a `const`, fixed at compile time by definition, so
//! there's no way to randomize it per test run. `ensure_bounded_context`
//! below is the idempotent get-or-insert `skilj-demo/src/bin/server.rs`
//! already uses for exactly this reason, rather than the unconditional
//! insert (`.unwrap()`-on-conflict) most other `skilj/tests/*.rs` files
//! use - a second run against a persistent `DATABASE_URL` reuses the same
//! row instead of failing on it.

use chrono::{SubsecRound, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{auto_register, CommandType, EventType, Projection, Skilj, Snapshot};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::{BoundedContextEvent, DEFAULT_BOUNDED_CONTEXT};
use skilj_core::shared::{generate_token_id, CommandDecision};

// --- fixtures ---

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct FixturePayload {}

/// One custom, fixed bounded context name (see this file's own doc
/// comment for why it's a literal, not `unique_name(...)`) - what every
/// `Custom*` fixture below overrides `BOUNDED_CONTEXT` to.
const CUSTOM_BOUNDED_CONTEXT: &str = "skilj_auto_register_test_custom";

struct DefaultScopedAutoEvent;

#[auto_register]
impl EventType for DefaultScopedAutoEvent {
    type Payload = FixturePayload;
    const NAME: &'static str = "AutoRegisterDefaultScopedEvent";
    fn direct_creation_allowed() -> bool {
        true
    }
}

struct CustomScopedAutoEvent;

// `#[auto_register(CUSTOM_BOUNDED_CONTEXT)]` here, rather than a
// hand-written `const BOUNDED_CONTEXT = ...` override in the impl body,
// is the shorthand form (skilj-macros::auto_register's own doc comment) -
// what skilj-demo's own banking/courses modules use to scope every type
// to a bounded context declared once, at the top of the file.
#[auto_register(CUSTOM_BOUNDED_CONTEXT)]
impl EventType for CustomScopedAutoEvent {
    type Payload = FixturePayload;
    const NAME: &'static str = "AutoRegisterCustomScopedEvent";
    fn direct_creation_allowed() -> bool {
        true
    }
}

/// Bridges `DefaultScopedAutoEvent` alone - just enough of a generated
/// per-bounded-context enum to satisfy `CommandType::Event`/
/// `Projection::Event`'s own `BoundedContextEvent` bound, the same
/// minimal-fixture treatment `command_trigger.rs`'s own `BankingEvent`
/// uses.
enum AutoRegisterEvent {
    DefaultScoped(FixturePayload),
}

impl BoundedContextEvent for AutoRegisterEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "AutoRegisterDefaultScopedEvent" => {
                Some(serde_json::from_str(&event.payload).map(AutoRegisterEvent::DefaultScoped))
            }
            _ => None,
        }
    }
}

struct DefaultScopedAutoCommand;

#[auto_register]
impl CommandType for DefaultScopedAutoCommand {
    type Payload = FixturePayload;
    type Event = AutoRegisterEvent;
    const NAME: &'static str = "AutoRegisterDefaultScopedCommand";
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

struct DefaultScopedAutoProjection;

#[auto_register]
impl Projection for DefaultScopedAutoProjection {
    type State = FixturePayload;
    type Event = AutoRegisterEvent;
    const NAME: &'static str = "AutoRegisterDefaultScopedProjection";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["AutoRegisterDefaultScopedEvent"]
    }
    fn project(_state: &mut Self::State, _event: &Self::Event, _key: &str) {}
}

struct DefaultScopedAutoSnapshot;

#[auto_register]
impl Snapshot for DefaultScopedAutoSnapshot {
    type State = FixturePayload;
    type Event = AutoRegisterEvent;
    const NAME: &'static str = "AutoRegisterDefaultScopedSnapshot";
    const TAG_KEY: &'static str = "fixture";
    const VERSION: u64 = 1;
    fn fold(_state: &mut Self::State, _event: &Self::Event) {}
}

/// A plain, un-tagged `EventType` - registered the ordinary manual way,
/// with no leading `.bounded_context(...)` call at all, to prove the
/// defaulted `SkiljBuilder` (this pass's other half) independently of
/// `#[auto_register]`.
struct ManuallyChainedEvent;

impl EventType for ManuallyChainedEvent {
    type Payload = FixturePayload;
    const NAME: &'static str = "ManuallyChainedEvent";
    fn direct_creation_allowed() -> bool {
        true
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---
// (identical to skilj/tests/command_trigger.rs's own harness)

struct TestDb {
    database_url: String,
    pool: Pool,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for auto_register tests")
    })
}

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| (db.database_url.clone(), db.pool.clone()))
}

async fn connect_and_migrate(database_url: &str, label: &str) -> Option<Pool> {
    let pool = match db::connect(database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to {label} failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating {label} failed: {e}");
        return None;
    }
    Some(pool)
}

async fn provision() -> Option<TestDb> {
    let database_url = if let Ok(database_url) = std::env::var("DATABASE_URL") {
        database_url
    } else {
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
        let database_name = "skilj_auto_register_test";
        if let Err(e) = server.create_database(database_name).await {
            eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
            return None;
        }
        let url = server.settings().url(database_name);
        let pool = connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            pool,
            _embedded: Some(server),
        });
    };

    let pool = connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb {
        database_url,
        pool,
        _embedded: None,
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

/// Idempotent get-or-insert - see this file's own doc comment for why
/// this (rather than the unconditional insert most `skilj/tests/*.rs`
/// fixtures use) is needed here. Both of this file's `#[test]` functions
/// race to create the same `DEFAULT_BOUNDED_CONTEXT` row (Rust runs
/// `#[test]` functions in parallel by default), so a failed insert is
/// re-checked for "someone else already won this race" rather than
/// treated as a real error outright - the same tolerance
/// `SkiljBuilder::build()`'s own admin-context stamping already extends
/// to the identical race (`skilj/src/lib.rs`).
async fn ensure_bounded_context(pool: &Pool, name: &str) -> BoundedContext {
    if let Some(bc) = db::get_bounded_context(pool, name).await.unwrap() {
        return bc;
    }
    let bc = BoundedContext {
        name: name.to_string(),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
    };
    if let Err(e) = db::insert_bounded_context(pool, &bc).await {
        return db::get_bounded_context(pool, name)
            .await
            .unwrap()
            .unwrap_or_else(|| {
                panic!("insert_bounded_context({name:?}) failed and no row exists: {e}")
            });
    }
    bc
}

/// Fresh `Role` + admin `RoleAccessMapping` on `bc`, unique to this call -
/// unlike the bounded context name, nothing about a `Role` needs to be
/// compile-time fixed, so this keeps the usual `unique_name(...)`
/// treatment.
async fn admin_role_on(pool: &Pool, bc: &BoundedContext) -> String {
    let external_subject = unique_name("subject");
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.clone(),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();
    let mapping = RoleAccessMapping {
        role,
        bounded_context: bc.clone(),
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(pool, &mapping)
        .await
        .unwrap();
    external_subject
}

#[test]
fn auto_register_registers_every_plugin_trait_under_its_own_bounded_context() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };

        let default_bc = ensure_bounded_context(&pool, DEFAULT_BOUNDED_CONTEXT).await;
        let custom_bc = ensure_bounded_context(&pool, CUSTOM_BOUNDED_CONTEXT).await;
        let default_subject = admin_role_on(&pool, &default_bc).await;
        let custom_subject = admin_role_on(&pool, &custom_bc).await;

        // Two reconciliation passes, one per role - `auto_register()`
        // itself needs no `.bounded_context(...)` call at all; every
        // `#[auto_register]`-tagged fixture above carries its own scope.
        let (_skilj, report) = Skilj::builder(database_url.clone())
            .auto_register()
            .reconciliation_role(default_subject)
            .build()
            .await
            .unwrap();
        assert!(
            report.registered.contains(&format!(
                "{DEFAULT_BOUNDED_CONTEXT}/AutoRegisterDefaultScopedEvent"
            )),
            "registered: {:?}",
            report.registered
        );
        assert!(report.registered.contains(&format!(
            "{DEFAULT_BOUNDED_CONTEXT}/AutoRegisterDefaultScopedCommand"
        )));
        assert!(report.registered.contains(&format!(
            "{DEFAULT_BOUNDED_CONTEXT}/AutoRegisterDefaultScopedProjection"
        )));
        // Snapshot has no metadata table/reconciliation surface of its own
        // (see Snapshot's own doc comment) - so unlike the three above,
        // there's no report.registered entry to check. What #[auto_register]
        // reaching SkiljBuilder::snapshot::<T>() actually looks like is the
        // in-process dispatcher now knowing about it.
        assert!(
            _skilj
                .snapshot_dispatcher()
                .snapshot_names(DEFAULT_BOUNDED_CONTEXT)
                .contains(&"AutoRegisterDefaultScopedSnapshot"),
            "the Snapshot arm of #[auto_register] did not reach SkiljBuilder"
        );
        // The custom-scoped fixtures aren't skipped as "unregistered" -
        // they're skipped for lack of *this* role's access to
        // CUSTOM_BOUNDED_CONTEXT, the ordinary reconciliation outcome
        // (docs §1.5), not a sign auto-registration missed them.
        assert!(report.skipped_no_access.contains(&format!(
            "{CUSTOM_BOUNDED_CONTEXT}/AutoRegisterCustomScopedEvent"
        )));

        let (_skilj2, report2) = Skilj::builder(database_url)
            .auto_register()
            .reconciliation_role(custom_subject)
            .build()
            .await
            .unwrap();
        assert!(report2.registered.contains(&format!(
            "{CUSTOM_BOUNDED_CONTEXT}/AutoRegisterCustomScopedEvent"
        )));

        let default_event = db::get_event_type(
            &pool,
            DEFAULT_BOUNDED_CONTEXT,
            "AutoRegisterDefaultScopedEvent",
        )
        .await
        .unwrap();
        assert!(default_event.is_some());

        let custom_event = db::get_event_type(
            &pool,
            CUSTOM_BOUNDED_CONTEXT,
            "AutoRegisterCustomScopedEvent",
        )
        .await
        .unwrap();
        assert!(custom_event.is_some());
    });
}

#[test]
fn manual_registration_without_bounded_context_defaults_to_default() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };

        let default_bc = ensure_bounded_context(&pool, DEFAULT_BOUNDED_CONTEXT).await;
        let external_subject = admin_role_on(&pool, &default_bc).await;

        // No `.bounded_context(...)` call anywhere in this chain.
        let (_skilj, report) = Skilj::builder(database_url)
            .event_type::<ManuallyChainedEvent>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(
            report.registered,
            vec![format!("{DEFAULT_BOUNDED_CONTEXT}/ManuallyChainedEvent")]
        );

        let event = db::get_event_type(&pool, DEFAULT_BOUNDED_CONTEXT, "ManuallyChainedEvent")
            .await
            .unwrap();
        assert!(event.is_some());
    });
}
