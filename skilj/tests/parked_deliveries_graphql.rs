//! End-to-end tests for Codeberg issue #21's admin surface -
//! `parkedDeliveries`/`retryParkedDelivery`/`discardParkedDelivery`. Same
//! real-HTTP-through-`Skilj::graphql_router()`, real-JWKS-server harness
//! as `skilj/tests/graphql_type_registration.rs` - see its own doc
//! comment for the details, duplicated here rather than extracted into
//! shared test-support (the same call this project's earlier passes
//! already made).
//!
//! Deliberately doesn't try to reproduce a real bridge/route failure to
//! get a parked row - `ParkedDelivery.request_json` is seeded directly
//! via `skilj_core::db::insert_parked_delivery` (the exact mechanism
//! `catch_up_cross_context_route` and `skilj-kafka`'s own `run_inbound`
//! each already have their own dedicated tests proving for real - see
//! `skilj-core/tests/cross_context_route_parking.rs` and
//! `skilj-kafka/tests/kafka_bridge.rs`). This file's own job is narrower
//! and complementary: given a parked delivery already exists, does the
//! GraphQL admin surface list/retry/discard it correctly, end to end
//! over real HTTP with real auth.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use serde_json::json;
use skilj::{IdpConfig, SigningAlgorithm, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};
use tower::ServiceExt;

const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDPHVFsUHiWXSbG
/TCig1cTQHNT6FnoYoZtMEjvDiQArsOL/dFoM9pmGRM9CfEtQGNum4TsimPtgJec
awfdPnW0uJCRlIF9wGmYdh2mYNBKw8jqxwp664Gd5uqH5L6A4pN8bfGO7+2niD6p
8t0cNeyYOd0PusbAEDcpzCUZmr6KQyM5i8/wk5oO98gntp+ZpMjUZabAD6R8DyhM
IZmV645jo5NPJG7zuSz+3dmKkNY0/GXz8YwvZ2swqmmOANRZHHfN1vgP2ycK02WZ
4yihx6EiuQCDseddBw+xit9KSvSq6GwmwnV1qVpMVNlSGGOeVX7v7JQ3z/BNbQ85
5p6s/FjhAgMBAAECggEAFu8fKghLIhNUjOpSbVxv0vDrFFqBQitOyV50ZQxCzlSL
0L+dZZWAVJfoOnUUYLdli0TrVioI4K7Bmw97AnO9IvLhB03TfPJGfxxtMhQ8XFsL
r3u03GGhq7N7OusIcUslm7ys5/AHd+qtTbJX65zJAx49LVW4VmI1SYqSfSBWgway
8uGYaXyCfwuxQ+xB4fQd6llm/+9dqS+U36LVSMWgEmVjceorYFhPVLfuX4A1wHjF
mDl40AwPBqzVbOIzFDMDikk4heFi6wlt6N3LGDtyBUUuzEg5TBhyiirvNvTjW+4V
Z4MZs3tez+IqM0+F4EsgAEQUU12YQxa4lobm8/zgZQKBgQD81FMzymNR6xWhUSwY
4RtkVntfMBOMp1rVGcVyBxOLKxEXF6ctk2rV38krfUI50h/lWzrbpl+zJvEe8D1H
vZjYj28sL3wf0CSnPYUeGANTxrW1dTiz1HVzzChfbAEWj3fsVrlghNcnHBkDDhqz
L/rPEfp//fB0SyLAEAJt87cgFwKBgQDRtjtH1gIkGn5GCS3u0FAbxV+qrUlTvu4t
Di1GcEw32jootQQSMZN1PxEvLuehaBlaASEL2OZzZlQ4q60LV1Jisvd7wqv5EYnG
o+sKtrCS5iXKfkxqTmg+JS7OZazggyvgBnv4GXT0US6/G4nw7C9JaS2jyOvPGIPS
K8dsWDIxxwKBgQCgr4FBxTticPqKUECqf0cdeilm0fNazXJZRcvLMNwm8vQlrQ6/
VJXt4BDG5xEUFovXBShfOVpRTkqo0x7fXYyq9l49wuAsh+kDsYHNIo3azMvny9yB
zmHnerWeD9KROBWLy4J96W+kl6L94hTuFWxd9psyhX4xKx+m2YXxw5d7eQKBgFB2
I86PHOkvRQ2oDfiX8nSFSQxaSk0Yb5fX3aUuBwBS+YeO1E4KuXH9zaEV1QeHwlpX
Ho/GG71hIKVRsSYtzc1Sr0PL0GHSydLuJ4tHxv3F0fAcf0M2bCaT656DQk4t5dKh
ikUJt2baEx59+XH3nLkE4t75gwhFdqZX5775I+EXAoGAfnpHlLZdGW48rl9Cl887
hRDjXDm/gP/ljCrvxxiWselEgaLj2o4NiT28QAfq7KgtOIpAeLAGzIBP6vkE7KFp
nAF+t4gRpooXXSI5oXCBcGI9a26q68UV3iDEmQGiP8kVHOsdzcOKY0qk1ulNAIV4
fU919gnTKorSq3FdV6zGZ8s=
-----END PRIVATE KEY-----
";

const TEST_MODULUS_N: &str = "zx1RbFB4ll0mxv0wooNXE0BzU-hZ6GKGbTBI7w4kAK7Di_3RaDPaZhkTPQnxLUBjbpuE7Ipj7YCXnGsH3T51tLiQkZSBfcBpmHYdpmDQSsPI6scKeuuBnebqh-S-gOKTfG3xju_tp4g-qfLdHDXsmDndD7rGwBA3KcwlGZq-ikMjOYvP8JOaDvfIJ7afmaTI1GWmwA-kfA8oTCGZleuOY6OTTyRu87ks_t3ZipDWNPxl8_GML2drMKppjgDUWRx3zdb4D9snCtNlmeMoocehIrkAg7HnXQcPsYrfSkr0quhsJsJ1dalaTFTZUhhjnlV-7-yUN8_wTW0POeaerPxY4Q";
const TEST_EXPONENT_E: &str = "AQAB";
const TEST_KID: &str = "test-key-1";
const TEST_ISSUER: &str = "https://idp.example.test/";

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
            .expect("failed to build a tokio runtime for parked_deliveries_graphql tests")
    })
}

async fn test_database_url() -> Option<String> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.database_url.clone())
}

async fn check_reachable(database_url: &str, label: &str) -> bool {
    match skilj_core::db::connect(database_url).await {
        Ok(pool) => {
            skilj_core::db::migrate(&pool).await.is_ok() || {
                eprintln!("skipping: migrating {label} failed");
                false
            }
        }
        Err(e) => {
            eprintln!("skipping: connecting to {label} failed: {e}");
            false
        }
    }
}

async fn provision() -> Option<TestDb> {
    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        return check_reachable(&database_url, "DATABASE_URL")
            .await
            .then_some(TestDb {
                database_url,
                _embedded: None,
            });
    }

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
    let database_name = "skilj_parked_deliveries_graphql_test";
    if let Err(e) = server.create_database(database_name).await {
        eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
        return None;
    }
    let url = server.settings().url(database_name);
    if !check_reachable(&url, "embedded PostgreSQL").await {
        return None;
    }
    Some(TestDb {
        database_url: url,
        _embedded: Some(server),
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<chrono::Utc> {
    use chrono::SubsecRound;
    chrono::Utc::now().trunc_subsecs(6)
}

// --- JWKS test server + JWT signing (same fixture as graphql_type_registration.rs) ---

async fn serve_jwks() -> String {
    let jwks = json!({
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": TEST_KID,
            "n": TEST_MODULUS_N,
            "e": TEST_EXPONENT_E,
        }]
    });
    let app = axum::Router::new().route(
        "/jwks.json",
        axum::routing::get(move || {
            let jwks = jwks.clone();
            async move { axum::Json(jwks) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind an ephemeral port for the JWKS test server");
    let addr = listener
        .local_addr()
        .expect("a bound listener always has a local address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("the JWKS test server stopped unexpectedly");
    });
    format!("http://{addr}/jwks.json")
}

fn sign_jwt(subject: &str) -> String {
    let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(TEST_KID.to_string());
    let claims = json!({
        "sub": subject,
        "iss": TEST_ISSUER,
        "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
    });
    let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes())
        .expect("the test private key PEM is well-formed");
    jsonwebtoken::encode(&header, &claims, &key).expect("signing a well-formed JWT never fails")
}

/// Builds a `Skilj` (real `identity_provider`), a real admin `Role` with
/// an active admin-level grant on a fresh bounded context, and that
/// role's own signed JWT - everything every test below needs to call
/// straight into a gated mutation. Also registers a real `MoneyDeposited`
/// `EventType` (`external_creation_allowed: true`) and mints a real
/// `ExternalEventToken` for it - what the `ExternalEvent`-kind retry test
/// needs to redrive through.
async fn setup() -> (Skilj, Pool, String, String, String) {
    let database_url = test_database_url()
        .await
        .expect("test_database_url() must be Some - caller already checked");
    let jwks_url = serve_jwks().await;

    let (skilj, _report) = Skilj::builder(database_url.clone())
        .identity_provider(IdpConfig::new(
            jwks_url.parse().unwrap(),
            TEST_ISSUER,
            SigningAlgorithm::Rs256,
        ))
        .build()
        .await
        .unwrap();

    let pool = skilj_core::db::connect(&database_url).await.unwrap();

    let admin_subject = unique_name("admin");
    let role = Role {
        id: generate_token_id(),
        external_subject: admin_subject.clone(),
        name: "Admin".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    skilj_core::db::insert_role(&pool, &role).await.unwrap();

    let bc_name = unique_name("bank");
    let bc = BoundedContext {
        name: bc_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    skilj_core::db::insert_bounded_context(&pool, &bc)
        .await
        .unwrap();

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
    skilj_core::db::insert_role_access_mapping(&pool, &mapping)
        .await
        .unwrap();

    let event_type = skilj_core::event_store::EventType {
        bounded_context: bc.clone(),
        name: "MoneyDeposited".to_string(),
        schema: r#"{"properties":{"amount":{"type":"number"}}}"#.to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: true,
        direct_creation_allowed: false,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: true,
    };
    db::upsert_event_type(&pool, &event_type).await.unwrap();
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

    let jwt = sign_jwt(&admin_subject);
    (skilj, pool, bc_name, jwt, token.id)
}

async fn graphql_request(
    router: &axum::Router,
    jwt: Option<&str>,
    query: &str,
    variables: serde_json::Value,
) -> serde_json::Value {
    let mut request = Request::builder().method("POST").uri("/graphql");
    if let Some(jwt) = jwt {
        request = request.header("authorization", format!("Bearer {jwt}"));
    }
    let request = request
        .header("content-type", "application/json")
        .body(Body::from(
            json!({ "query": query, "variables": variables }).to_string(),
        ))
        .unwrap();

    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

/// `parkedDeliveries` lists a directly-seeded row with every field
/// rendered correctly, `retryParkedDelivery` redrives its own stored
/// `ExternalEvent`-kind `request_json` for real (a genuine event lands),
/// and deletes the row on success; a second, separately-seeded row is
/// then removed by `discardParkedDelivery` alone, without ever being
/// redriven - proof the two mutations are independent paths, not one
/// implemented in terms of the other.
#[test]
fn parked_deliveries_are_listed_retried_and_discarded_over_real_graphql() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, pool, bc_name, jwt, access_token_id) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        let first_failed_at = test_now() - chrono::Duration::seconds(30);
        let last_failed_at = test_now();
        let seeded = db::insert_parked_delivery(
            &pool,
            &bc_name,
            "kafka-inbound",
            db::ParkedDeliveryKind::ExternalEvent,
            "orders:0:42",
            Some(&access_token_id),
            None,
            None,
            &json!({
                "payload": { "amount": 5 },
                "sourceContent": "kafka:orders:0:42",
                "correlationId": null,
                "causationId": null,
            }),
            "connection refused",
            3,
            first_failed_at,
            last_failed_at,
        )
        .await
        .unwrap();

        // Gating: no caller at all is rejected before anything runs.
        let response = graphql_request(
            &router,
            None,
            "query($bc: String!) { parkedDeliveries(boundedContext: $bc) { id } }",
            json!({ "bc": bc_name }),
        )
        .await;
        assert_eq!(response["errors"][0]["extensions"]["code"], "unauthenticated");

        // parkedDeliveries - every field renders correctly off the
        // directly-seeded row.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { parkedDeliveries(boundedContext: $bc) { \
                id source kind identifier accessTokenId targetBoundedContext \
                targetCommandType requestJson error attemptCount firstFailedAt lastFailedAt \
            } }",
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let listed = response["data"]["parkedDeliveries"].as_array().unwrap();
        assert_eq!(listed.len(), 1);
        let row = &listed[0];
        assert_eq!(row["id"], json!(seeded.id));
        assert_eq!(row["source"], json!("kafka-inbound"));
        assert_eq!(row["kind"], json!("EXTERNAL_EVENT"));
        assert_eq!(row["identifier"], json!("orders:0:42"));
        assert_eq!(row["accessTokenId"], json!(access_token_id));
        assert!(row["targetBoundedContext"].is_null());
        assert!(row["targetCommandType"].is_null());
        assert_eq!(row["error"], json!("connection refused"));
        assert_eq!(row["attemptCount"], json!(3));
        let request_json: serde_json::Value =
            serde_json::from_str(row["requestJson"].as_str().unwrap()).unwrap();
        assert_eq!(request_json["payload"], json!({ "amount": 5 }));

        // retryParkedDelivery redrives the stored request for real - a
        // genuine MoneyDeposited event lands - and deletes the row.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $id: String!) { retryParkedDelivery(boundedContext: $bc, id: $id) { id } }",
            json!({ "bc": bc_name, "id": seeded.id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["retryParkedDelivery"]["id"], json!(seeded.id));

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1, "the retry must have created a real event");
        assert_eq!(events[0].event_type.name, "MoneyDeposited");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&events[0].payload).unwrap(),
            json!({ "amount": 5 })
        );

        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { parkedDeliveries(boundedContext: $bc) { id } }",
            json!({ "bc": bc_name }),
        )
        .await;
        assert_eq!(
            response["data"]["parkedDeliveries"].as_array().unwrap().len(),
            0,
            "a successfully retried delivery must no longer be parked"
        );

        // A second row, discarded without ever being redriven.
        let discardable = db::insert_parked_delivery(
            &pool,
            &bc_name,
            "kafka-inbound",
            db::ParkedDeliveryKind::ExternalEvent,
            "orders:0:99",
            Some(&access_token_id),
            None,
            None,
            &json!({
                "payload": { "amount": 999 },
                "sourceContent": "kafka:orders:0:99",
                "correlationId": null,
                "causationId": null,
            }),
            "connection refused",
            5,
            test_now(),
            test_now(),
        )
        .await
        .unwrap();

        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $id: String!) { discardParkedDelivery(boundedContext: $bc, id: $id) { id } }",
            json!({ "bc": bc_name, "id": discardable.id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(
            response["data"]["discardParkedDelivery"]["id"],
            json!(discardable.id)
        );

        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { parkedDeliveries(boundedContext: $bc) { id } }",
            json!({ "bc": bc_name }),
        )
        .await;
        assert_eq!(
            response["data"]["parkedDeliveries"].as_array().unwrap().len(),
            0
        );
        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "the discarded delivery must never have been redriven - still just the one \
             event from the successful retry above"
        );
    });
}
