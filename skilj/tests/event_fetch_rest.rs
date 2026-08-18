//! End-to-end test for `GET /v1/events`'s real `filter=field:op:value`
//! wire shape (docs/architecture.md §7.3) - a real HTTP request, through
//! `Skilj::rest_router()`, exercising `valid_filters`/`matches_filters`
//! for real over the actual REST surface, not just the pure-function
//! layer (`skilj-core/tests/event_filtering.rs` covers that). Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj/tests/command_trigger.rs` - see its own doc comment for the
//! details, not repeated a third time here.

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
    fn event_read_allowed() -> bool {
        true
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for event_fetch_rest tests")
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
        let database_name = "skilj_event_fetch_rest_test";
        if let Err(e) = server.create_database(database_name).await {
            eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
            return None;
        }
        let url = server.settings().url(database_name);
        connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            _embedded: Some(server),
        });
    };

    connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb {
        database_url,
        _embedded: None,
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

/// Builds a fully reconciled `Skilj` plus real, minted
/// `DirectCreationToken`/`EventReadToken` credentials for `MoneyDeposited`.
async fn setup() -> (Skilj, String, String) {
    let database_url = test_db()
        .await
        .expect("test_db() must be Some - caller already checked");
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

    let (skilj, report) = Skilj::builder(database_url)
        .bounded_context(bc_name.clone())
        .event_type::<MoneyDeposited>()
        .reconciliation_role(external_subject)
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
        .await
        .unwrap()
        .unwrap();

    let direct_token = access_control::create_direct_creation_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        test_now(),
    )
    .unwrap();
    db::insert_direct_creation_token(&pool, &direct_token)
        .await
        .unwrap();
    let direct_credential = format!("{}.{}", direct_token.id, direct_token.secret);

    let read_token = access_control::create_event_read_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        test_now(),
    )
    .unwrap();
    db::insert_event_read_token(&pool, &read_token)
        .await
        .unwrap();
    let read_credential = format!("{}.{}", read_token.id, read_token.secret);

    (skilj, direct_credential, read_credential)
}

async fn deposit(router: &axum::Router, credential: &str, amount: i64) {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events/direct")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"payload":{{"amount":{amount}}}}}"#
        )))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[test]
fn get_events_filter_param_narrows_results_for_real_over_rest() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        deposit(&router, &direct_credential, 5).await;
        deposit(&router, &direct_credential, 20).await;

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events?filter=amount:greater_than:10")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let events = json["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["payload"]["amount"], 20);
    });
}

#[test]
fn get_events_rejects_a_malformed_filter_param_with_400() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events?filter=amount-only-no-colons")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    });
}

#[test]
fn get_events_rejects_a_filter_naming_an_undeclared_field_with_400() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events?filter=bogus_field:equals:x")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    });
}
