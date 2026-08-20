//! End-to-end tests for `skilj-graphql`'s Phase 2 - the four
//! `AdminAccess`-gated static surfaces: `TypeRegistration`,
//! `EventTypeAdminOperations`, `CommandTypeAdminOperations`,
//! `TokenRevocation`. Same real-HTTP-through-`Skilj::graphql_router()`,
//! real-JWKS-server harness as `skilj/tests/graphql_admin_console.rs` -
//! see its own doc comment for the details, duplicated here rather than
//! extracted into shared test-support (not yet worth it for two files,
//! the same call this project's earlier passes already made).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use serde_json::json;
use skilj::{IdpConfig, SigningAlgorithm, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::Pool;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::generate_token_id;
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
            .expect("failed to build a tokio runtime for graphql_type_registration tests")
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
    let database_name = "skilj_graphql_type_registration_test";
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

// --- JWKS test server + JWT signing (same fixture as graphql_admin_console.rs) ---

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

/// Builds a `Skilj` (real `identity_provider`, no compiled-in event/
/// command types - Phase 2's mutations register types dynamically over
/// GraphQL, not through the builder), a real admin Role with an active
/// admin-level grant on a fresh bounded context, and that Role's own
/// signed JWT - everything every test below needs to call straight into
/// a gated mutation.
async fn setup() -> (Skilj, Pool, String, String) {
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

    let bc_name = unique_name("banking");
    let bc = BoundedContext {
        name: bc_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
    };
    skilj_core::db::insert_bounded_context(&pool, &bc)
        .await
        .unwrap();

    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    skilj_core::db::insert_role_access_mapping(&pool, &mapping)
        .await
        .unwrap();

    let jwt = sign_jwt(&admin_subject);
    (skilj, pool, bc_name, jwt)
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

const REGISTER_EVENT_TYPE_MUTATION: &str = "\
    mutation($bc: String!, $name: String!) { \
        registerEventType(boundedContext: $bc, name: $name, schema: \"{\\\"properties\\\":{\\\"amount\\\":{\\\"type\\\":\\\"number\\\"},\\\"account_id\\\":{\\\"type\\\":\\\"string\\\"}}}\", \
            tagMappings: [{key: \"account\", field: \"account_id\"}], sensitiveFields: [], \
            externalCreationAllowed: true, directCreationAllowed: true, systemTriggeredAllowed: false, \
            eventReadAllowed: true) { \
            name schemaVersion tagMappings { key field } externalCreationAllowed \
        } \
    }";

#[test]
fn full_type_registration_lifecycle_end_to_end() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        // Gating: no caller at all is rejected before anything runs.
        let response = graphql_request(
            &router,
            None,
            REGISTER_EVENT_TYPE_MUTATION,
            json!({ "bc": bc_name, "name": "MoneyDeposited" }),
        )
        .await;
        assert_eq!(response["errors"][0]["extensions"]["code"], "unauthenticated");

        // registerEventType.
        let response = graphql_request(
            &router,
            Some(&jwt),
            REGISTER_EVENT_TYPE_MUTATION,
            json!({ "bc": bc_name, "name": "MoneyDeposited" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["registerEventType"]["name"], "MoneyDeposited");
        assert_eq!(response["data"]["registerEventType"]["schemaVersion"], 1);
        assert_eq!(
            response["data"]["registerEventType"]["tagMappings"][0]["key"],
            "account"
        );
        assert_eq!(
            response["data"]["registerEventType"]["externalCreationAllowed"],
            true
        );

        // registerCommandType.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                registerCommandType(boundedContext: $bc, name: $name, schema: \"{}\", \
                    tagMappings: [], sensitiveFields: [], restTriggerAllowed: true) { \
                    name schemaVersion restTriggerAllowed \
                } \
            }",
            json!({ "bc": bc_name, "name": "WithdrawMoney" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["registerCommandType"]["name"], "WithdrawMoney");
        assert_eq!(response["data"]["registerCommandType"]["restTriggerAllowed"], true);

        // registerProjection - first call creates it outright.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                registerProjection(boundedContext: $bc, name: $name, schema: \"{\\\"properties\\\":{}}\", \
                    consumedEventTypes: [\"MoneyDeposited\"], sync: false) { \
                    outcome projection { name schemaVersion sync pendingRebuild { status } buildingRebuild { status } } \
                } \
            }",
            json!({ "bc": bc_name, "name": "AccountBalance" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["registerProjection"]["outcome"], "CREATED");
        assert_eq!(
            response["data"]["registerProjection"]["projection"]["name"],
            "AccountBalance"
        );
        assert!(response["data"]["registerProjection"]["projection"]["pendingRebuild"].is_null());
        assert!(response["data"]["registerProjection"]["projection"]["buildingRebuild"].is_null());

        // Re-registering with a changed schema stages a rebuild instead of
        // updating in place.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                registerProjection(boundedContext: $bc, name: $name, schema: \"{\\\"properties\\\":{\\\"total\\\":{\\\"type\\\":\\\"number\\\"}}}\", \
                    consumedEventTypes: [\"MoneyDeposited\"], sync: false) { \
                    outcome rebuild { status schemaVersion } \
                } \
            }",
            json!({ "bc": bc_name, "name": "AccountBalance" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["registerProjection"]["outcome"], "REBUILD_STAGED");
        assert_eq!(response["data"]["registerProjection"]["rebuild"]["status"], "PENDING");
        assert_eq!(response["data"]["registerProjection"]["rebuild"]["schemaVersion"], 2);

        // The projections() query reflects the staged rebuild.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { projections(boundedContext: $bc) { name pendingRebuild { status } buildingRebuild { status } } }",
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let projections = response["data"]["projections"].as_array().unwrap();
        let listed = projections
            .iter()
            .find(|p| p["name"] == "AccountBalance")
            .unwrap();
        assert_eq!(listed["pendingRebuild"]["status"], "PENDING");
        assert!(listed["buildingRebuild"].is_null());

        // rebuildProjection moves it to building.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { rebuildProjection(boundedContext: $bc, name: $name) { status } }",
            json!({ "bc": bc_name, "name": "AccountBalance" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["rebuildProjection"]["status"], "BUILDING");

        // The pending row is gone - rebuildProjection moved it to building,
        // not left a stale duplicate behind under its old status (see
        // `db::transition_projection_rebuild_to_building`'s own doc
        // comment).
        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { projections(boundedContext: $bc) { name pendingRebuild { status } buildingRebuild { status } } }",
            json!({ "bc": bc_name }),
        )
        .await;
        let projections = response["data"]["projections"].as_array().unwrap();
        let listed = projections
            .iter()
            .find(|p| p["name"] == "AccountBalance")
            .unwrap();
        assert!(listed["pendingRebuild"].is_null());
        assert_eq!(listed["buildingRebuild"]["status"], "BUILDING");

        // The deliberate coexisting case (UniqueRebuildPerProjectionAndStatus's
        // own "a non-trivial registration arriving mid-build stages
        // alongside the build rather than disturbing it"): registering a
        // further schema change while AccountBalance's rebuild is still
        // BUILDING must stage a brand new PENDING row, not touch the
        // building one.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                registerProjection(boundedContext: $bc, name: $name, schema: \"{\\\"properties\\\":{\\\"count\\\":{\\\"type\\\":\\\"number\\\"}}}\", \
                    consumedEventTypes: [\"MoneyDeposited\"], sync: false) { \
                    outcome rebuild { status schemaVersion } \
                } \
            }",
            json!({ "bc": bc_name, "name": "AccountBalance" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["registerProjection"]["outcome"], "REBUILD_STAGED");
        assert_eq!(response["data"]["registerProjection"]["rebuild"]["status"], "PENDING");
        // 2, not 3 - `new_schema_version` is always `existing.schema_version
        // + 1` off the *live* Projection (still version 1; nothing has
        // promoted yet in this test), not off the building rebuild's own
        // staged version.
        assert_eq!(response["data"]["registerProjection"]["rebuild"]["schemaVersion"], 2);

        // Both rows are now live at once, each independently visible.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { projections(boundedContext: $bc) { name pendingRebuild { status schemaVersion } buildingRebuild { status } } }",
            json!({ "bc": bc_name }),
        )
        .await;
        let projections = response["data"]["projections"].as_array().unwrap();
        let listed = projections
            .iter()
            .find(|p| p["name"] == "AccountBalance")
            .unwrap();
        assert_eq!(listed["pendingRebuild"]["status"], "PENDING");
        assert_eq!(listed["pendingRebuild"]["schemaVersion"], 2);
        assert_eq!(listed["buildingRebuild"]["status"], "BUILDING");

        // createExternalEventToken / createDirectCreationToken / createEventReadToken.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                createExternalEventToken(boundedContext: $bc, eventTypeName: $name) { \
                    id secret status eventType { name } \
                } \
            }",
            json!({ "bc": bc_name, "name": "MoneyDeposited" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["createExternalEventToken"]["status"], "ACTIVE");
        assert_eq!(
            response["data"]["createExternalEventToken"]["eventType"]["name"],
            "MoneyDeposited"
        );
        assert!(!response["data"]["createExternalEventToken"]["secret"]
            .as_str()
            .unwrap()
            .is_empty());
        let external_event_token_id = response["data"]["createExternalEventToken"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        // createCommandToken.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                createCommandToken(boundedContext: $bc, commandTypeName: $name) { \
                    id status commandType { name } \
                } \
            }",
            json!({ "bc": bc_name, "name": "WithdrawMoney" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(
            response["data"]["createCommandToken"]["commandType"]["name"],
            "WithdrawMoney"
        );

        // revokeToken - the union return type, queried via an inline fragment.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($id: ID!) { \
                revokeToken(tokenId: $id) { \
                    ... on ExternalEventToken { id status } \
                } \
            }",
            json!({ "id": external_event_token_id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["revokeToken"]["status"], "REVOKED");

        // discardProjectionRebuild - a fresh, separate projection, its own
        // clean pending-then-discard scenario.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                registerProjection(boundedContext: $bc, name: $name, schema: \"{\\\"properties\\\":{}}\", \
                    consumedEventTypes: [], sync: false) { outcome } \
            }",
            json!({ "bc": bc_name, "name": "MonthlyReport" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["registerProjection"]["outcome"], "CREATED");

        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                registerProjection(boundedContext: $bc, name: $name, schema: \"{\\\"properties\\\":{\\\"x\\\":{\\\"type\\\":\\\"number\\\"}}}\", \
                    consumedEventTypes: [], sync: false) { outcome rebuild { status } } \
            }",
            json!({ "bc": bc_name, "name": "MonthlyReport" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["registerProjection"]["outcome"], "REBUILD_STAGED");
        assert_eq!(response["data"]["registerProjection"]["rebuild"]["status"], "PENDING");

        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { discardProjectionRebuild(boundedContext: $bc, name: $name) { status } }",
            json!({ "bc": bc_name, "name": "MonthlyReport" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["discardProjectionRebuild"]["status"], "PENDING");

        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { projections(boundedContext: $bc) { name pendingRebuild { status } } }",
            json!({ "bc": bc_name }),
        )
        .await;
        let projections = response["data"]["projections"].as_array().unwrap();
        let listed = projections
            .iter()
            .find(|p| p["name"] == "MonthlyReport")
            .unwrap();
        assert!(listed["pendingRebuild"].is_null());
    });
}

/// Real end-to-end proof of the 2026-08-20 drift audit's #2 fix
/// (`valid_sensitive_fields` used to accept a non-string leaf, and
/// `protect_sensitive_fields` would then silently replace it with a
/// ciphertext *string* on every write, corrupting the payload's own
/// declared type - see project memory `skilj-drift-audit-2026-08-20`).
/// `amount` is declared `"type":"number"` in this schema - a
/// `sensitiveFields` entry naming it must now be rejected at
/// registration, before any write ever has the chance to corrupt
/// anything.
#[test]
fn register_event_type_rejects_a_sensitive_field_on_a_non_string_leaf() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                registerEventType(boundedContext: $bc, name: $name, \
                    schema: \"{\\\"properties\\\":{\\\"amount\\\":{\\\"type\\\":\\\"number\\\"},\\\"account_id\\\":{\\\"type\\\":\\\"string\\\"}}}\", \
                    tagMappings: [], \
                    sensitiveFields: [{field: \"amount\", subjectKey: \"account\", subjectField: \"account_id\"}], \
                    externalCreationAllowed: true, directCreationAllowed: true, systemTriggeredAllowed: false, \
                    eventReadAllowed: true) { name } \
            }",
            json!({ "bc": bc_name, "name": "MoneyDeposited" }),
        )
        .await;

        assert!(response.get("data").is_none() || response["data"]["registerEventType"].is_null());
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "invalid_sensitive_field"
        );
    });
}

/// Drift audit finding #16 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): before `valid_schema`, this exact
/// registration - a malformed schema, no `tagMappings`, no
/// `sensitiveFields` - would have silently succeeded, since neither
/// existing validator ever inspects the schema when both lists are
/// empty.
#[test]
fn register_event_type_rejects_a_malformed_schema_with_no_tag_mappings_or_sensitive_fields() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                registerEventType(boundedContext: $bc, name: $name, \
                    schema: \"not json at all\", \
                    tagMappings: [], \
                    sensitiveFields: [], \
                    externalCreationAllowed: true, directCreationAllowed: true, systemTriggeredAllowed: false, \
                    eventReadAllowed: true) { name } \
            }",
            json!({ "bc": bc_name, "name": "MoneyDeposited" }),
        )
        .await;

        assert!(response.get("data").is_none() || response["data"]["registerEventType"].is_null());
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "invalid_schema"
        );
    });
}

/// Drift audit finding #16's own follow-up (2026-08-20, see project
/// memory `skilj-drift-audit-2026-08-20`): `registerProjection` had no
/// schema check at all before this - worse than `registerEventType`/
/// `registerCommandType`'s own previously-vacuous pair, since a
/// projection's schema has no `valid_payload`-style backstop later
/// either.
#[test]
fn register_projection_rejects_a_malformed_schema() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!) { \
                registerProjection(boundedContext: $bc, name: $name, \
                    schema: \"not json at all\", \
                    consumedEventTypes: [], sync: false) { outcome } \
            }",
            json!({ "bc": bc_name, "name": "AccountBalance" }),
        )
        .await;

        assert!(response.get("data").is_none() || response["data"]["registerProjection"].is_null());
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "invalid_schema"
        );
    });
}
