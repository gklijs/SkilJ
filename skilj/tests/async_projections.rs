//! End-to-end test for the background consumer [§8](../../docs/architecture.md#open-for-a-future-pass) item 6's async case
//! spawns - a real HTTP request through `Skilj::rest_router()`, a short
//! wait for the spawned poll task to actually run, then back out through
//! `db::get_projection_state`. Proof the task `SkiljBuilder::build()`
//! spawns is real and running, not just the persistence layer in
//! isolation (see `skilj-core/tests/async_projections.rs` for that half,
//! including the `ProjectionRebuild` replay/promotion case this file
//! doesn't repeat). Same `DATABASE_URL`-then-embedded-Postgres-then-skip
//! harness as `skilj/tests/sync_projections.rs` - see its own doc comment
//! for the details, not repeated a third time here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Projection, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, generate_token_secret, CommandDecision, EventSpec};
use tower::ServiceExt;

// --- fixtures ---

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
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "MoneyDeposited".to_string(),
                payload: serde_json::json!({ "amount": payload.amount }),
            }],
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct AccountBalanceState {
    total: i64,
}

/// Deliberately does *not* override `sync()` - the default (`false`) is
/// exactly the case under test.
struct AccountBalance;

impl Projection for AccountBalance {
    type State = AccountBalanceState;
    type Event = BankingEvent;
    const NAME: &'static str = "AccountBalance";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["MoneyDeposited"]
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        let BankingEvent::MoneyDeposited(payload) = event;
        state.total += payload.amount;
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

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
            .expect("failed to build a tokio runtime for async_projections tests")
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
        let database_name = "skilj_async_projections_e2e_test";
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

#[test]
fn triggering_a_command_eventually_updates_a_real_async_projection_through_rest() {
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
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping)
            .await
            .unwrap();

        let (skilj, report) = Skilj::builder(database_url)
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<WithdrawMoney>()
            .projection::<AccountBalance>()
            .reconciliation_role(external_subject)
            .async_projection_poll_interval(std::time::Duration::from_millis(50))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let command_type = db::get_command_type(&pool, &bc_name, "WithdrawMoney")
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_command_token(
            &mapping,
            &command_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_command_token(&pool, &token).await.unwrap();
        let credential = format!("{}.{}", token.id, token.secret);

        let router = skilj.rest_router();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"amount":20}}"#))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["accepted"], true);

        // Unlike the sync case, the response above says nothing about
        // whether the projection has caught up yet - the whole point of
        // `sync: false` (the default `AccountBalance` doesn't override).
        // The spawned background task's own 50ms poll interval is what
        // eventually reflects it; give it a few ticks' worth of margin
        // rather than relying on exactly one.
        let mut state = None;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            state = db::get_projection_state(&pool, &bc_name, "AccountBalance", "")
                .await
                .unwrap();
            if state.as_deref() == Some(r#"{"total":20}"#) {
                break;
            }
        }
        assert_eq!(state, Some(r#"{"total":20}"#.to_string()));

        let projection = db::get_projection(&pool, &bc_name, "AccountBalance")
            .await
            .unwrap()
            .unwrap();
        assert!(projection.caught_up_to.is_some());
    });
}
