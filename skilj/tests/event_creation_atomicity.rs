//! Real end-to-end regression guard for `db::create_and_insert_external_event`/
//! `db::create_and_insert_direct_event` (the drift audit's #2 fix - see
//! project memory `skilj-drift-audit-2026-08-18`): `next_sequence`'s own
//! row lock, the event's construction, and its insert now share one
//! transaction, so a rejection - here, a token revoked between minting
//! and use - rolls the sequence allocation back too, instead of burning
//! a number nothing ever gets written under. Same `DATABASE_URL`-then-
//! embedded-Postgres-then-skip harness as `skilj/tests/event_fetch_rest.rs`,
//! see its own doc comment for the details, not repeated a third time
//! here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{EventType, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};
use tower::ServiceExt;

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

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for event_creation_atomicity tests")
    })
}

async fn test_db() -> Option<String> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.database_url.clone())
}

async fn connect_and_migrate(database_url: &str, label: &str) -> Option<()> {
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
    Some(())
}

async fn provision() -> Option<TestDb> {
    let database_url =
        skilj_test_support::database_url("skilj_event_creation_atomicity_test").await?;

    connect_and_migrate(&database_url, "the test database").await?;
    Some(TestDb { database_url })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn deposit(
    router: &axum::Router,
    credential: &str,
    amount: i64,
) -> axum::http::Response<Body> {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events/direct")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"payload":{{"amount":{amount}}}}}"#
        )))
        .unwrap();
    router.clone().oneshot(request).await.unwrap()
}

/// A revoked `DirectCreationToken`'s own presented secret is still
/// well-formed, so `create_and_insert_direct_event` reaches all the way
/// to `create_direct_event`'s own `TokenNotActive` rejection - after
/// `next_sequence` has already run, inside the same transaction. Before
/// the #2 fix, that allocation ran unlocked on the bare pool, well
/// before this rejection was even reachable, and stayed burned
/// regardless. Here, it must not: the very next successful deposit still
/// lands on sequence 0.
#[test]
fn a_rejected_direct_event_creation_leaves_no_sequence_gap() {
    runtime().block_on(async {
        let Some(database_url) = test_db().await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();

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
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());
        let router = skilj.rest_router();

        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();

        // A token that gets revoked before it's ever used.
        let doomed_token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &doomed_token)
            .await
            .unwrap();
        let doomed_credential = format!("{}.{}", doomed_token.id, doomed_token.secret);
        db::revoke_access_token(&pool, &doomed_token.id, test_now())
            .await
            .unwrap();

        let response = deposit(&router, &doomed_credential, 20).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);

        // A second, active token - its own deposit must still land on
        // sequence 0, proving the rejected attempt above left no gap.
        let live_token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &live_token)
            .await
            .unwrap();
        let live_credential = format!("{}.{}", live_token.id, live_token.secret);

        let response = deposit(&router, &live_credential, 20).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["sequence"], 0);
    });
}
