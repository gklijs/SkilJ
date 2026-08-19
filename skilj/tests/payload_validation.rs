//! Real end-to-end proof of the drift audit's #10 fix (`valid_payload` -
//! see project memory `skilj-drift-audit-2026-08-18`) over the event
//! side's own REST surface. `skilj-core/tests/event_creation_surfaces.rs`
//! already covers the pure-function rejection at the `create_direct_event`
//! layer; this proves the same rejection is real end to end, through the
//! actual `POST /v1/events/direct` route and its `{"code": ...}` wire
//! shape - `command_trigger.rs`'s own
//! `command_trigger_rejects_a_payload_that_does_not_match_the_schema_with_400`
//! is this file's sibling on the command side. Same `DATABASE_URL`-then-
//! embedded-Postgres-then-skip harness as `skilj/tests/
//! event_creation_atomicity.rs` - see its own doc comment for the
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
            .expect("failed to build a tokio runtime for payload_validation tests")
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
        let database_name = "skilj_payload_validation_test";
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

/// The request body is decoded as generic JSON at the wire layer
/// (`DirectEventRequest.payload: serde_json::Value`), so a payload
/// missing the schema's own required `amount` field reaches
/// `create_direct_event`'s `valid_payload` guard rather than failing to
/// deserialize earlier - proving the rejection is real over the actual
/// REST surface, not just at the pure-function layer
/// `skilj-core/tests/event_creation_surfaces.rs` already covers.
#[test]
fn direct_event_creation_rejects_a_payload_that_does_not_match_the_schema_with_400() {
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
        let router = skilj.rest_router();

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

        // No "amount" at all - MoneyDepositedPayload's own schemars-derived
        // schema declares it required.
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{}}"#))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "payload_does_not_match_schema");

        // Nothing was persisted - the rejection is a real no-op, not a
        // partial write.
        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert!(events.is_empty());
    });
}
