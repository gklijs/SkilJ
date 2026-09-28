//! docs/architecture.md §87: a recorded idempotency key deduplicates for
//! `SkiljBuilder::idempotency_key_retention` and is then deleted, after
//! which a submission bearing it is a new command. Its own test binary:
//! the retention task sweeps every bounded context in the database, so a
//! short retention here would expire other tests' keys.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj};
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
    #[allow(dead_code)]
    MoneyDeposited(MoneyDepositedPayload),
}

impl BoundedContextEvent for BankingEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "MoneyDeposited" => {
                Some(serde_json::from_str(&event.payload).map(Self::MoneyDeposited))
            }
            _ => None,
        }
    }
}

/// Always accepts - the point of this file is the idempotency-key
/// derivation, not `decide()` logic, so kept as simple as
/// `command_trigger.rs`'s own trivial commands.
struct WithdrawMoney;

impl CommandType for WithdrawMoney {
    type Payload = MoneyDepositedPayload;
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

// --- harness (same shape as command_trigger.rs) ---

struct TestDb {
    database_url: String,
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for idempotency_key_retention tests")
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
        let url = skilj_test_support::database_url("skilj_idempotency_key_retention_test").await?;
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

async fn setup(retention: std::time::Duration) -> (Router, String, Pool, String) {
    let (database_url, pool) = test_db()
        .await
        .expect("test_db() must be Some - caller already checked");

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
        .reconciliation_role(external_subject)
        .idempotency_key_retention(retention)
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
    (skilj.rest_router(), credential, pool, bc_name)
}

async fn trigger(router: Router, credential: &str, key: &str) -> serde_json::Value {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/commands/trigger")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .header("Idempotency-Key", key)
        .body(Body::from(r#"{"payload":{"amount":5}}"#))
        .unwrap();
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

#[test]
fn a_key_deduplicates_within_its_retention_and_is_new_after_it() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (router, credential, pool, bc_name) = setup(std::time::Duration::from_secs(1)).await;

        let first = trigger(router.clone(), &credential, "order-17").await;
        assert_eq!(first["deduplicated"], false, "{first}");
        let retried = trigger(router.clone(), &credential, "order-17").await;
        assert_eq!(retried["deduplicated"], true, "{retried}");
        assert_eq!(
            retried["triggeredEventSequences"],
            first["triggeredEventSequences"]
        );

        // The retention task runs once a second here; wait for the key to go.
        let count_keys = || {
            let pool = pool.clone();
            let bc_name = bc_name.clone();
            async move {
                let (count,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT count(*) FROM \"bc_{bc_name}\".idempotency_keys"
                )))
                .fetch_one(&pool)
                .await
                .unwrap();
                count
            }
        };
        let mut remaining = count_keys().await;
        for _ in 0..50 {
            if remaining == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            remaining = count_keys().await;
        }
        assert_eq!(remaining, 0, "the expired key was never deleted");

        let after = trigger(router.clone(), &credential, "order-17").await;
        assert_eq!(after["deduplicated"], false, "{after}");
        assert_ne!(
            after["triggeredEventSequences"],
            first["triggeredEventSequences"]
        );
    });
}
