//! A panic in application plugin code run by one of `SkiljBuilder::build`'s
//! background tasks must not take the task down for every other bounded
//! context (docs/architecture.md §80). Same provisioning harness as
//! `skilj/tests/async_projections.rs`.

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

/// Stands in for application code with a bug: a `project` that panics
/// (an `unwrap()` on a bad assumption, an index out of range).
struct ExplodingBalance;

impl Projection for ExplodingBalance {
    type State = AccountBalanceState;
    type Event = BankingEvent;
    const NAME: &'static str = "ExplodingBalance";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["MoneyDeposited"]
    }
    fn project(_state: &mut Self::State, _event: &Self::Event, _key: &str) {
        panic!("a bug in application projection code");
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for background_task_panics tests")
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
        let url = skilj_test_support::database_url("skilj_background_task_panics_test").await?;
        let pool = connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            pool,
        });
    };

    let pool = connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb { database_url, pool })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn admin_bounded_context(pool: &Pool, role: &Role, prefix: &str) -> RoleAccessMapping {
    let bc = BoundedContext {
        name: unique_name(prefix),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc,
        level: AccessLevel::Admin,
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

async fn deposit(pool: &Pool, skilj: &Skilj, mapping: &RoleAccessMapping, amount: i64) {
    let command_type = db::get_command_type(pool, &mapping.bounded_context.name, "WithdrawMoney")
        .await
        .unwrap()
        .unwrap();
    let token = access_control::create_command_token(
        mapping,
        &command_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        test_now(),
    )
    .unwrap();
    db::insert_command_token(pool, &token).await.unwrap();
    let request = Request::builder()
        .method("POST")
        .uri("/v1/commands/trigger")
        .header(
            "authorization",
            format!("Bearer {}.{}", token.id, token.secret),
        )
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"payload":{{"amount":{amount}}}}}"#
        )))
        .unwrap();
    let response = skilj.rest_router().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[test]
fn a_panicking_projection_does_not_stop_catch_up_for_other_bounded_contexts() {
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
        let broken = admin_bounded_context(&pool, &role, "broken").await;
        let healthy = admin_bounded_context(&pool, &role, "healthy").await;

        let (skilj, report) = Skilj::builder(database_url)
            .bounded_context(broken.bounded_context.name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<WithdrawMoney>()
            .projection::<ExplodingBalance>()
            .bounded_context(healthy.bounded_context.name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<WithdrawMoney>()
            .projection::<AccountBalance>()
            .reconciliation_role(external_subject)
            .async_projection_poll_interval(std::time::Duration::from_millis(50))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        // The broken projection panics on the next tick, and on every tick
        // after it (the event is still there to fold).
        deposit(&pool, &skilj, &broken, 5).await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;

        // Only now does the healthy bounded context get an event, so it is
        // folded by a tick that runs *after* the panics began.
        deposit(&pool, &skilj, &healthy, 20).await;
        let healthy_name = &healthy.bounded_context.name;
        let mut state = None;
        for _ in 0..80 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            state = db::get_projection_state(&pool, healthy_name, "AccountBalance", "")
                .await
                .unwrap();
            if state.is_some() {
                break;
            }
        }
        assert_eq!(state, Some(r#"{"total":20}"#.to_string()));

        // The broken projection folded nothing: its transaction rolled back.
        let broken_state =
            db::get_projection_state(&pool, &broken.bounded_context.name, "ExplodingBalance", "")
                .await
                .unwrap();
        assert_eq!(broken_state, None);
    });
}
