//! Verifies `server.rs`'s own local JWKS/IdP shortcut actually
//! authenticates a real GraphQL request end to end - a real HTTP POST
//! through `Skilj::graphql_router()`, a JWT signed with the exact
//! mechanism `sign_jwt`/`serve_local_jwks` use, real JWT verification
//! against a real local JWKS server, into a real `AdminAccess`-gated
//! resolver. Same fixed test RSA keypair as `skilj/tests/graphql_admin_console.rs`
//! (that file's own doc comment explains why reusing a fixed, publicly-
//! known key is fine here) - duplicated rather than imported, since
//! `server.rs`'s own copy lives in a binary, not something a test file
//! can pull in, and the crate boundary this repository otherwise keeps
//! test fixtures within already discourages a new shared crate for this
//! alone.

mod support;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use serde_json::json;
use skilj::{IdpConfig, Skilj, SigningAlgorithm};
use support::{runtime, test_db};
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
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
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
    let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes()).unwrap();
    jsonwebtoken::encode(&header, &claims, &key).unwrap()
}

async fn graphql_request(router: &axum::Router, jwt: &str, query: &str) -> serde_json::Value {
    let request = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("authorization", format!("Bearer {jwt}"))
        .header("content-type", "application/json")
        .body(Body::from(json!({ "query": query }).to_string()))
        .unwrap();

    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

#[test]
fn a_jwt_signed_against_the_local_jwks_server_authenticates_a_real_query() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let mappings = support::seed_admin(&pool).await;
        let external_subject = mappings[0].role.external_subject.clone();
        let jwks_url = serve_jwks().await;

        let (skilj, report) = skilj_demo::register(Skilj::builder(database_url))
            .reconciliation_role(external_subject.clone())
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let router = skilj.graphql_router().await.unwrap();
        let jwt = sign_jwt(&external_subject);

        // `queryEvents` is `AdminAccess`-gated - the seeded Role's own
        // `Admin`-level mapping (from `support::seed_admin`) is exactly
        // what it needs, the same level `server.rs`'s own seeded Role
        // gets. Not asserting an empty result: `skilj_demo::banking::BOUNDED_CONTEXT`
        // is a fixed name every test file in this crate shares (see
        // `support`'s own doc comment on why), so `banking.rs`'s own
        // tests may well have already appended real `MoneyDeposited`
        // events here by the time this one runs. What's under test is
        // that this request authenticates at all and the resolver runs
        // for real - not what it happens to return.
        let query = format!(
            "query {{ queryEvents(boundedContext: {:?}, eventTypes: [\"MoneyDeposited\"]) {{ sequence }} }}",
            skilj_demo::banking::BOUNDED_CONTEXT,
        );
        let response = graphql_request(&router, &jwt, &query).await;

        assert!(
            response.get("errors").is_none(),
            "expected no GraphQL errors, got {response:?}"
        );
        assert!(
            response["data"]["queryEvents"].is_array(),
            "expected a real (possibly non-empty) queryEvents array, got {response:?}"
        );
    });
}
