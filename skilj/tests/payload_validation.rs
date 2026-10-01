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
use skilj_core::db::{self, Pool};
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
    /// Cross-tenant write fix (docs/architecture.md's own write-up of
    /// these passes) - a real declared owner dimension so
    /// `direct_event_creation_rejects_an_event_whose_owner_does_not_match_the_tokens_scope`
    /// below has something to test a scoped `DirectCreationToken` against
    /// over the actual REST surface. `direct_event_creation_rejects_a_
    /// payload_that_does_not_match_the_schema_with_400` above mints its
    /// own token with `scope: None`, so this is vacuously true there -
    /// no behaviour change to the existing test.
    fn tag_mappings() -> Vec<skilj_core::shared::TagMapping> {
        vec![skilj_core::shared::TagMapping {
            key: "amount".into(),
            field: "amount".into(),
        }]
    }
    fn owner_tag_key() -> Option<&'static str> {
        Some("amount")
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
    let database_url = skilj_test_support::database_url("skilj_payload_validation_test").await?;

    connect_and_migrate(&database_url, "the test database").await?;
    Some(TestDb { database_url })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

/// Builds a fully reconciled `Skilj` (`MoneyDeposited` registered) plus a
/// real, minted `DirectCreationToken` for it - everything a
/// `POST /v1/events/direct` test needs. The trailing `RoleAccessMapping`/
/// `EventType` are for callers that need to mint a second, differently-
/// scoped token of their own (cross-tenant write fix, docs/architecture.md's
/// own write-up of these passes).
async fn setup() -> (
    Skilj,
    Pool,
    String,
    String,
    RoleAccessMapping,
    skilj_core::event_store::EventType,
) {
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

    let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
        .await
        .unwrap()
        .unwrap();
    let token = access_control::create_direct_creation_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        test_now(),
    )
    .unwrap();
    db::insert_direct_creation_token(&pool, &token)
        .await
        .unwrap();
    let credential = format!("{}.{}", token.id, token.secret);

    (skilj, pool, bc_name, credential, mapping, event_type)
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
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, bc_name, credential, _mapping, _event_type) = setup().await;
        let router = skilj.rest_router();

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

/// Cross-tenant write fix (docs/architecture.md's own write-up of these
/// passes), the real end-to-end proof over `POST /v1/events/direct` -
/// `setup()`'s own token has `scope: None`, so this mints a *second*,
/// real `DirectCreationToken` scoped to `"20"` and proves it can no
/// longer create an event for a *different* owner. Also the regression
/// test for the same real bug `command_trigger.rs`'s own identical test
/// found and fixed: `Error::GrantScopeMismatch` had no entry in
/// `skilj-rest`'s `status_for` table, so it fell through to 500 instead
/// of 403 - see that table's own comment.
#[test]
fn direct_event_creation_rejects_an_event_whose_owner_does_not_match_the_tokens_scope() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, bc_name, _credential, mapping, event_type) = setup().await;
        let scoped_token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            Some("20".into()),
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &scoped_token)
            .await
            .unwrap();
        let scoped_credential = format!("{}.{}", scoped_token.id, scoped_token.secret);
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {scoped_credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"amount":999}}"#))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "grant_scope_mismatch");

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert!(events.is_empty());
    });
}
