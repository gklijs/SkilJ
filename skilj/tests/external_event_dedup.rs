//! End-to-end tests for `POST /v1/events/external`'s own `dedupe` field
//! ([docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event), specs/skilj.allium's own `rule
//! CreateExternalEvent`) - a real HTTP request, through
//! `Skilj::rest_router()`, into `db::create_and_insert_external_event`,
//! and back out through the real response body. `skilj-core/tests/external_event_dedup.rs`
//! already covers the mechanism itself against `db::create_and_insert_external_event`
//! directly; this file is the wire-level counterpart, proving the
//! `dedupe: { partitionKey, sequence }` JSON shape and the
//! `sequence`/`redelivered` response fields actually work over the real
//! REST surface, the same "layer in isolation, then the real wire" split
//! `skilj/tests/command_trigger.rs` already uses for `CommandTrigger`.
//! Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as that
//! file - see its own doc comment for the details, not repeated a third
//! time here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{EventType, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};
use tower::ServiceExt;

// --- fixtures ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OrderPlacedPayload {
    order_id: String,
}

struct OrderPlaced;

impl EventType for OrderPlaced {
    type Payload = OrderPlacedPayload;
    const NAME: &'static str = "OrderPlaced";
    fn external_creation_allowed() -> bool {
        true
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
            .expect("failed to build a tokio runtime for external_event_dedup tests")
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
    let database_url =
        skilj_test_support::database_url("skilj_external_event_dedup_rest_test").await?;

    let pool = connect_and_migrate(&database_url, "the test database").await?;
    Some(TestDb { database_url, pool })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn setup() -> (Skilj, String, Pool, String) {
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

    let bc_name = unique_name("orders");
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
        .event_type::<OrderPlaced>()
        .reconciliation_role(external_subject)
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let event_type = db::get_event_type(&pool, &bc_name, "OrderPlaced")
        .await
        .unwrap()
        .unwrap();
    let token = access_control::create_external_event_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        test_now(),
    )
    .unwrap();
    db::insert_external_event_token(&pool, &token)
        .await
        .unwrap();

    let credential = format!("{}.{}", token.id, token.secret);
    (skilj, credential, pool, bc_name)
}

async fn submit(
    router: &axum::Router,
    credential: &str,
    order_id: &str,
    dedupe: Option<(&str, i64)>,
) -> (StatusCode, serde_json::Value) {
    let body = match dedupe {
        Some((partition_key, sequence)) => serde_json::json!({
            "payload": { "order_id": order_id },
            "sourceContent": "kafka",
            "dedupe": { "partitionKey": partition_key, "sequence": sequence },
        }),
        None => serde_json::json!({
            "payload": { "order_id": order_id },
            "sourceContent": "kafka",
        }),
    };
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events/external")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

/// [docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event): omitting `dedupe` entirely creates an event
/// every time, unchanged from before this feature existed -
/// `response.sequence` present, `response.redelivered` false.
#[test]
fn omitting_dedupe_creates_an_event_and_reports_no_redelivery() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, ..) = setup().await;
        let router = skilj.rest_router();

        let (status, body) = submit(&router, &credential, "A", None).await;
        assert_eq!(status, StatusCode::CREATED);
        assert!(body["sequence"].is_i64());
        assert_eq!(body["redelivered"], false);
    });
}

/// The central case, over the real wire: a redelivered message (the
/// exact same `dedupe.partitionKey`/`dedupe.sequence` pair as an earlier,
/// already-created submission) still returns 201 - it's accepted, not an
/// error - but `sequence` is `null` and `redelivered` is `true`, and no
/// second event exists.
#[test]
fn a_redelivered_message_is_accepted_but_creates_nothing() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name) = setup().await;
        let router = skilj.rest_router();

        let (first_status, first_body) =
            submit(&router, &credential, "A", Some(("orders-topic:0", 10))).await;
        assert_eq!(first_status, StatusCode::CREATED);
        assert!(first_body["sequence"].is_i64());
        assert_eq!(first_body["redelivered"], false);

        let (redelivered_status, redelivered_body) =
            submit(&router, &credential, "A", Some(("orders-topic:0", 10))).await;
        assert_eq!(
            redelivered_status,
            StatusCode::CREATED,
            "a redelivery is accepted, not an error"
        );
        assert!(redelivered_body["sequence"].is_null());
        assert_eq!(redelivered_body["redelivered"], true);

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "the redelivery must not have created a second event"
        );
    });
}
