//! End-to-end tests for `skilj-graphql`'s Phase 1 - the superadmin admin
//! console (`/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`):
//! real HTTP GraphQL requests through `Skilj::graphql_router()`, real
//! JWT verification against a real local JWKS server, into the real
//! pure functions, and back out through Postgres persistence. Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj/tests/command_trigger.rs` - see its own doc comment for the
//! details, not repeated a third time here. The JWKS-serving harness is
//! the same pattern `skilj-core/tests/jwt_verification.rs` uses, with the
//! identical test RSA keypair - see that file's own doc comment for
//! where it came from.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use serde_json::json;
use skilj::{IdpConfig, SigningAlgorithm, Skilj};
use skilj_core::db::Pool;
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
            .expect("failed to build a tokio runtime for graphql_admin_console tests")
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
    let database_name = "skilj_graphql_admin_console_test";
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

// --- JWKS test server + JWT signing (same fixture as skilj-core/tests/jwt_verification.rs) ---

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

/// Builds a `Skilj` with a real `identity_provider` pointed at a fresh
/// local JWKS server - no bounded contexts/event/command types
/// registered, since Phase 1's admin console doesn't need any (it's the
/// cross-context, superadmin-facing half of the schema).
async fn setup() -> (Skilj, Pool) {
    let database_url = test_database_url()
        .await
        .expect("test_database_url() must be Some - caller already checked");
    let jwks_url = serve_jwks().await;

    let (skilj, report) = Skilj::builder(database_url.clone())
        .identity_provider(IdpConfig::new(
            jwks_url.parse().unwrap(),
            TEST_ISSUER,
            SigningAlgorithm::Rs256,
        ))
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let pool = skilj_core::db::connect(&database_url).await.unwrap();
    (skilj, pool)
}

/// Sends one GraphQL request over `router`, optionally bearer-authenticated
/// as `jwt`, and returns the parsed JSON response body.
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

#[test]
fn full_admin_console_lifecycle_end_to_end() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool) = setup().await;
        let router = skilj.graphql_router();
        let bootstrap_secret = skilj
            .bootstrap_secret()
            .expect("no active superadmin exists yet on a freshly built Skilj")
            .to_string();
        let root_subject = unique_name("root");

        // 1. createSuperadmin - no caller identity needed at all.
        let response = graphql_request(
            &router,
            None,
            "mutation($secret: String!, $subject: String!) { \
                createSuperadmin(bootstrapSecret: $secret, name: \"Root\", externalSubject: $subject) { \
                    id name externalSubject \
                } \
            }",
            json!({ "secret": bootstrap_secret, "subject": root_subject }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["createSuperadmin"]["name"], "Root");
        assert_eq!(
            response["data"]["createSuperadmin"]["externalSubject"],
            root_subject
        );
        let root_role_id = response["data"]["createSuperadmin"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        // ClosesPermanentlyOnFirstClaim - a second attempt, even with the
        // same secret, fails now that an active superadmin exists.
        let response = graphql_request(
            &router,
            None,
            "mutation($secret: String!) { \
                createSuperadmin(bootstrapSecret: $secret, name: \"Root2\", externalSubject: \"someone-else\") { id } \
            }",
            json!({ "secret": bootstrap_secret }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "superadmin_already_exists"
        );

        // 2. Every gated mutation needs a caller - no Authorization header
        // at all is a clean "unauthenticated" rejection, not a panic or a
        // silent no-op.
        let response = graphql_request(
            &router,
            None,
            "mutation { addBoundedContext(name: \"billing\") { name } }",
            json!({}),
        )
        .await;
        assert_eq!(response["errors"][0]["extensions"]["code"], "unauthenticated");

        // A JWT that verifies but resolves to no Role at all - also
        // rejected, distinctly.
        let unrecognised_jwt = sign_jwt("nobody@example.com");
        let response = graphql_request(
            &router,
            Some(&unrecognised_jwt),
            "mutation { addBoundedContext(name: \"billing\") { name } }",
            json!({}),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "unrecognised_subject"
        );

        // 3. The real, authenticated flow, as the newly-created superadmin.
        let root_jwt = sign_jwt(&root_subject);

        let bc_name = unique_name("billing");
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($name: String!) { addBoundedContext(name: $name) { name status accessMappings { role { name } } } }",
            json!({ "name": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["addBoundedContext"]["name"], bc_name);
        assert_eq!(response["data"]["addBoundedContext"]["status"], "ACTIVE");
        assert_eq!(
            response["data"]["addBoundedContext"]["accessMappings"],
            json!([])
        );

        // createRole - a second, non-superadmin Role for the new context.
        let worker_subject = unique_name("worker");
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($subject: String!) { \
                createRole(name: \"Worker\", superadmin: false, externalSubject: $subject) { id name } \
            }",
            json!({ "subject": worker_subject }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let worker_role_id = response["data"]["createRole"]["id"].as_str().unwrap().to_string();

        // grantRoleAccessMapping - admin-level, on the new context.
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($roleId: ID!, $bc: String!) { \
                grantRoleAccessMapping(roleId: $roleId, boundedContext: $bc, level: ADMIN, canReadSensitive: false) { \
                    level canReadSensitive status role { id } \
                } \
            }",
            json!({ "roleId": worker_role_id, "bc": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["grantRoleAccessMapping"]["level"], "ADMIN");
        assert_eq!(response["data"]["grantRoleAccessMapping"]["status"], "ACTIVE");

        // boundedContexts - the directory now shows the grant.
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "query { boundedContexts { name accessMappings { role { name } level } } }",
            json!({}),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let contexts = response["data"]["boundedContexts"].as_array().unwrap();
        let listed = contexts
            .iter()
            .find(|c| c["name"] == bc_name)
            .expect("the just-created bounded context must be in the directory");
        assert_eq!(listed["accessMappings"][0]["role"]["name"], "Worker");
        assert_eq!(listed["accessMappings"][0]["level"], "ADMIN");

        // revokeRoleAccessMapping.
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($roleId: ID!, $bc: String!) { \
                revokeRoleAccessMapping(roleId: $roleId, boundedContext: $bc) { status } \
            }",
            json!({ "roleId": worker_role_id, "bc": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["revokeRoleAccessMapping"]["status"], "REVOKED");

        // revokeRole.
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($roleId: ID!) { revokeRole(roleId: $roleId) { status } }",
            json!({ "roleId": worker_role_id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["revokeRole"]["status"], "REVOKED");

        // archiveBoundedContext - the superadmin itself has no grant on
        // this context, so this must fail (GrantScopedToBoundedContext /
        // archival is admin-grant-gated, not superadmin-gated).
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($name: String!) { archiveBoundedContext(name: $name) { status } }",
            json!({ "name": bc_name }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "grant_not_active"
        );

        // deleteBoundedContext requires archived first - still active, so
        // this must fail too.
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($name: String!) { deleteBoundedContext(name: $name) { name } }",
            json!({ "name": bc_name }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "bounded_context_not_archived"
        );

        // Grant the superadmin's own Role admin access on this context,
        // then archive + delete for real - the full, legitimate lifecycle
        // end. `Role.superadmin` grants no bounded-context access of its
        // own (`AddBoundedContext`'s own note: "creation grants no
        // RoleAccessMapping, not even to the caller") - archiving still
        // needs a real grant, superadmin or not.
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($roleId: ID!, $bc: String!) { \
                grantRoleAccessMapping(roleId: $roleId, boundedContext: $bc, level: ADMIN, canReadSensitive: false) { level } \
            }",
            json!({ "roleId": root_role_id, "bc": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");

        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($name: String!) { archiveBoundedContext(name: $name) { status } }",
            json!({ "name": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["archiveBoundedContext"]["status"], "ARCHIVED");

        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "mutation($name: String!) { deleteBoundedContext(name: $name) { name status } }",
            json!({ "name": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["deleteBoundedContext"]["name"], bc_name);

        // Gone for real - the directory no longer lists it.
        let response = graphql_request(
            &router,
            Some(&root_jwt),
            "query { boundedContexts { name } }",
            json!({}),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let contexts = response["data"]["boundedContexts"].as_array().unwrap();
        assert!(!contexts.iter().any(|c| c["name"] == bc_name));
    });
}
