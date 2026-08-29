//! End-to-end tests for `SkiljBuilder`'s reconciliation loop (docs/
//! architecture.md §1.5/§1.7) - the biggest, least-obviously-correct
//! piece of code this crate owns (many chained async DB calls, three-way
//! branching in `register_projection`), so it gets the same real-Postgres
//! treatment `skilj-core/tests/persistence.rs` already established
//! rather than trusting compilation alone. See that file's own doc
//! comment for the full `DATABASE_URL`-then-embedded-Postgres-then-skip
//! fallback and why - this one is a near-identical copy of the same
//! harness, not yet worth extracting into a shared test-support crate
//! for two files.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Projection, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, generate_token_secret, CommandDecision};
use std::collections::HashSet;
use tower::ServiceExt;

// --- test fixture types: one small bounded context's worth of plugin impls ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct MoneyDepositedPayload {
    amount: i64,
}

struct MoneyDeposited;

impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn event_read_allowed() -> bool {
        true
    }
}

enum BankingEvent {
    MoneyDeposited(MoneyDepositedPayload),
}

impl BoundedContextEvent for BankingEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "MoneyDeposited" => {
                Some(serde_json::from_str(&event.payload).map(BankingEvent::MoneyDeposited))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct WithdrawPayload {
    amount: i64,
}

struct WithdrawMoney;

impl CommandType for WithdrawMoney {
    type Payload = WithdrawPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "WithdrawMoney";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: Vec::new() }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct AccountBalanceState {
    balance: i64,
}

struct AccountBalance;

impl Projection for AccountBalance {
    type State = AccountBalanceState;
    type Event = BankingEvent;
    const NAME: &'static str = "AccountBalance";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["MoneyDeposited"]
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            BankingEvent::MoneyDeposited(p) => state.balance += p.amount,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct SyncAccountBalanceState {
    balance: i64,
}

/// `AccountBalance`'s sync twin - identical fold, `sync() = true` -
/// dedicated to `reconciliation_folds_pre_existing_history_into_a_first_time_sync_projection`
/// below, so that test doesn't depend on `AccountBalance`'s own async
/// default staying what it is.
struct SyncAccountBalance;

impl Projection for SyncAccountBalance {
    type State = SyncAccountBalanceState;
    type Event = BankingEvent;
    const NAME: &'static str = "SyncAccountBalance";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["MoneyDeposited"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            BankingEvent::MoneyDeposited(p) => state.balance += p.amount,
        }
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip - see this file's own doc comment ---

/// Unlike `skilj-core/tests/persistence.rs`'s `TestDb`, this one keeps
/// the raw connection URL string too - `Skilj::builder(database_url)`
/// connects for itself, so tests need to hand it a URL, not a
/// ready-made `Pool`.
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
            .expect("failed to build a tokio runtime for reconciliation tests")
    })
}

/// `(database_url, pool)` - the URL for `Skilj::builder(...)` itself, the
/// pool for seeding fixtures directly via `skilj_core::db`. `None` (with
/// a stderr note) when no database could be reached at all.
async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| (db.database_url.clone(), db.pool.clone()))
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
        let database_name = "skilj_reconciliation_test";
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

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

/// Seeds a system-created bounded context plus an active `Admin`
/// `RoleAccessMapping` for a fresh `Role` - the "reconciliation Role has
/// admin access" happy path every test below except the skip test needs.
/// Returns `(bounded_context_name, external_subject)` - what
/// `Skilj::builder(...)`'s own `.bounded_context(...)`/
/// `.reconciliation_role(...)` calls need.
async fn seed_admin_context(pool: &Pool) -> (String, String) {
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

    let bc_name = unique_name("banking");
    let bc = BoundedContext {
        name: bc_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();

    let mapping = RoleAccessMapping {
        role,
        bounded_context: bc,
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(pool, &mapping)
        .await
        .unwrap();

    (bc_name, external_subject)
}

#[test]
fn reconciliation_registers_every_type_with_admin_access() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc_name, external_subject) = seed_admin_context(&pool).await;

        let (_skilj, report) = Skilj::builder(database_url)
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<WithdrawMoney>()
            .projection::<AccountBalance>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();

        assert_eq!(report.skipped_no_access, Vec::<String>::new());
        assert_eq!(
            report.registered.into_iter().collect::<HashSet<_>>(),
            HashSet::from([
                format!("{bc_name}/MoneyDeposited"),
                format!("{bc_name}/WithdrawMoney"),
                format!("{bc_name}/AccountBalance"),
            ])
        );

        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        assert!(event_type.direct_creation_allowed);
        assert!(event_type.event_read_allowed);
        assert!(event_type.schema.contains("amount"));

        let command_type = db::get_command_type(&pool, &bc_name, "WithdrawMoney")
            .await
            .unwrap()
            .unwrap();
        assert!(command_type.rest_trigger_allowed);

        let projection = db::get_projection(&pool, &bc_name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            projection
                .consumed_event_types
                .iter()
                .map(|et| et.name.clone())
                .collect::<Vec<_>>(),
            vec!["MoneyDeposited".to_string()]
        );
        assert!(!projection.sync);
    });
}

#[test]
fn reconciliation_skips_bounded_contexts_with_no_admin_access() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        // A Role that exists, but has no RoleAccessMapping for the
        // bounded context registered below at all.
        let external_subject = unique_name("subject");
        let role = Role {
            id: generate_token_id(),
            external_subject: external_subject.clone(),
            name: "No Access Role".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &role).await.unwrap();
        let bc_name = unique_name("banking");
        let bc = BoundedContext {
            name: bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &bc).await.unwrap();

        let (_skilj, report) = Skilj::builder(database_url)
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();

        assert_eq!(report.registered, Vec::<String>::new());
        assert_eq!(
            report.skipped_no_access,
            vec![format!("{bc_name}/MoneyDeposited")]
        );
        assert_eq!(
            db::get_event_type(&pool, &bc_name, "MoneyDeposited")
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn omitting_reconciliation_role_skips_reconciliation_entirely() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc_name, _external_subject) = seed_admin_context(&pool).await;

        let (_skilj, report) = Skilj::builder(database_url)
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .build()
            .await
            .unwrap();

        assert_eq!(report.registered, Vec::<String>::new());
        assert_eq!(report.skipped_no_access, Vec::<String>::new());
        assert_eq!(
            db::get_event_type(&pool, &bc_name, "MoneyDeposited")
                .await
                .unwrap(),
            None
        );
    });
}

#[test]
fn reconciliation_is_idempotent_across_repeated_builds() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc_name, external_subject) = seed_admin_context(&pool).await;

        for _ in 0..2 {
            let (_skilj, report) = Skilj::builder(database_url.clone())
                .bounded_context(bc_name.clone())
                .event_type::<MoneyDeposited>()
                .reconciliation_role(external_subject.clone())
                .build()
                .await
                .unwrap();
            assert_eq!(report.registered, vec![format!("{bc_name}/MoneyDeposited")]);
        }

        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        // Re-registering an unchanged schema doesn't bump schema_version -
        // event_store::register_event_type's own "schema_changed" check.
        assert_eq!(event_type.schema_version, 1);
    });
}

/// Drift audit finding #3 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): a first-time *sync* projection
/// registered into a bounded context that already has matching committed
/// history must fold that history in immediately, right at registration -
/// unlike an async projection, a sync one has no periodic catch-up of its
/// own to eventually pick it up (`db::catch_up_bounded_context` filters
/// to `!p.sync`), so without this the history would be missed forever.
/// Two real `.build()` calls against the same database, one deploy apart -
/// first with only the event type, so a real event can be committed
/// before the projection exists at all; second adding the sync
/// projection, proving reconciliation itself folds the pre-existing
/// history in, with no further event needed to trigger it.
#[test]
fn reconciliation_folds_pre_existing_history_into_a_first_time_sync_projection() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };

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
        db::insert_role(&pool, &role).await.unwrap();

        let bc_name = unique_name("banking");
        let bc = BoundedContext {
            name: bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &bc).await.unwrap();

        let mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping)
            .await
            .unwrap();

        // First deploy: only the event type exists yet, no projection at
        // all - so nothing here folds anything, but a real event is
        // committed for it below regardless.
        let (skilj, report) = Skilj::builder(database_url.clone())
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .reconciliation_role(external_subject.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &token)
            .await
            .unwrap();
        let credential = format!("{}.{}", token.id, token.secret);

        let rest_router = skilj.rest_router();
        for amount in [20, 5] {
            let request = Request::builder()
                .method("POST")
                .uri("/v1/events/direct")
                .header("authorization", format!("Bearer {credential}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "payload": { "amount": amount } }).to_string(),
                ))
                .unwrap();
            let response = rest_router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        // Second deploy: the sync projection is added now, into a
        // bounded context that already has both events committed above.
        let (_skilj, report) = Skilj::builder(database_url)
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .projection::<SyncAccountBalance>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(
            report.registered.into_iter().collect::<HashSet<_>>(),
            HashSet::from([
                format!("{bc_name}/MoneyDeposited"),
                format!("{bc_name}/SyncAccountBalance"),
            ])
        );

        // Folded in immediately by reconciliation itself - no additional
        // event or command was needed to reach this state.
        let state = db::get_projection_state(&pool, &bc_name, "SyncAccountBalance", "")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state, r#"{"balance":25}"#);

        let projection = db::get_projection(&pool, &bc_name, "SyncAccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert!(projection.sync);
        assert!(projection.caught_up_to.is_some());
    });
}
