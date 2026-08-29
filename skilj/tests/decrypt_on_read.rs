//! End-to-end tests for real decrypt-on-read - `render_event`'s own
//! two-grant test, exercised through `queryEvents` over GraphQL, not just
//! the pure-function coverage in `skilj-core/tests/encryption.rs` - see
//! the plan at `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`
//! and `docs/architecture.md`'s own write-up of this pass. Same
//! real-Postgres + real-local-JWKS-server harness as every other
//! `skilj/tests/graphql_*.rs`/`subject_erasure.rs` - see those files' own
//! doc comments for the details, duplicated here rather than extracted
//! into shared test-support (the same call every prior pass in this
//! project already made). Direct event creation over REST provisions the
//! real ciphertext, the same shape `subject_erasure.rs` already
//! establishes - this file's own job is exercising the *read* side that
//! file never touched.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::SubsecRound;
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use skilj::{EncryptionMasterKey, EventType, IdpConfig, SigningAlgorithm, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret, SensitiveField};
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

// --- fixtures ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct AccountOpenedPayload {
    email: String,
    user_id: String,
}

struct AccountOpened;

impl EventType for AccountOpened {
    type Payload = AccountOpenedPayload;
    const NAME: &'static str = "AccountOpened";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn sensitive_fields() -> Vec<SensitiveField> {
        vec![SensitiveField {
            field: "email".to_string(),
            subject_key: "user".to_string(),
            subject_field: "user_id".to_string(),
        }]
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct PlainPayload {
    note: String,
}

struct PlainEvent;

impl EventType for PlainEvent {
    type Payload = PlainPayload;
    const NAME: &'static str = "PlainEvent";
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
            .expect("failed to build a tokio runtime for decrypt_on_read tests")
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
    let database_name = "skilj_decrypt_on_read_test";
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
    chrono::Utc::now().trunc_subsecs(6)
}

// --- JWKS test server + JWT signing ---

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

async fn graphql_request(
    router: &axum::Router,
    jwt: &str,
    query: &str,
    variables: serde_json::Value,
) -> serde_json::Value {
    let request = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("authorization", format!("Bearer {jwt}"))
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

const QUERY_EVENTS: &str = "\
    query($bc: String!) { \
        queryEvents(boundedContext: $bc, eventTypes: [\"AccountOpened\"]) { \
            sequence payload \
        } \
    }";

const FORGET_SUBJECT_MUTATION: &str = "\
    mutation($bc: String!, $subjectKey: String!, $subjectValue: String!) { \
        forgetSubject(boundedContext: $bc, subjectKey: $subjectKey, subjectValue: $subjectValue) { \
            status \
        } \
    }";

async fn insert_role(pool: &skilj_core::db::Pool, external_subject: &str) -> Role {
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.to_string(),
        name: "Reader".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    skilj_core::db::insert_role(pool, &role).await.unwrap();
    role
}

async fn grant_admin(
    pool: &skilj_core::db::Pool,
    role: &Role,
    bc: &BoundedContext,
    can_read_sensitive: bool,
) {
    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
        level: AccessLevel::Admin,
        can_read_sensitive,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    skilj_core::db::insert_role_access_mapping(pool, &mapping)
        .await
        .unwrap();
}

/// The core two-grant test, exercised end-to-end over `queryEvents`, plus
/// crypto-shredding verified through the real read path (not just a raw
/// DB check, unlike `subject_erasure.rs`'s own coverage) - `forgetSubject`
/// destroying the key makes even a previously-granted caller see
/// ciphertext again.
#[test]
fn decrypt_on_read_end_to_end() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let database_url = test_database_url().await.unwrap();
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();

        // The admin whose own access reconciliation authenticates as -
        // Admin-level, but no can_read_sensitive and an unrelated
        // external_subject, so neither grant applies to it either.
        let admin_subject = unique_name("admin");
        let admin_role = insert_role(&pool, &admin_subject).await;

        let bc_name = unique_name("banking");
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
        grant_admin(&pool, &admin_role, &bc, false).await;

        // can_read_sensitive = true - grant (a).
        let sensitive_subject = unique_name("sensitive-reader");
        let sensitive_role = insert_role(&pool, &sensitive_subject).await;
        grant_admin(&pool, &sensitive_role, &bc, true).await;

        // external_subject matching the event's own subject_value ("42")
        // - grant (b), with no can_read_sensitive at all.
        let self_role = insert_role(&pool, "42").await;
        grant_admin(&pool, &self_role, &bc, false).await;

        let (skilj, report) = Skilj::builder(database_url.clone())
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<AccountOpened>()
            .reconciliation_role(admin_subject)
            .encryption_master_key(EncryptionMasterKey::from_bytes([42u8; 32]))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        // A real direct-creation token, minted the same way the admin
        // console already would - constructed directly here since
        // minting one isn't itself under test.
        let admin_mapping =
            skilj_core::db::get_active_role_access_mapping(&pool, &admin_role.id, &bc_name)
                .await
                .unwrap()
                .unwrap();
        let event_type = skilj_core::db::get_event_type(&pool, &bc_name, "AccountOpened")
            .await
            .unwrap()
            .unwrap();
        let token = skilj_core::access_control::create_direct_creation_token(
            &admin_mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            test_now(),
        )
        .unwrap();
        skilj_core::db::insert_direct_creation_token(&pool, &token)
            .await
            .unwrap();
        let credential = format!("{}.{}", token.id, token.secret);

        let rest_router = skilj.rest_router();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "payload": { "email": "person@example.com", "user_id": "42" } })
                    .to_string(),
            ))
            .unwrap();
        let response = rest_router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let graphql_router = skilj.graphql_router().await.unwrap();

        // Neither grant - ciphertext.
        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let response = graphql_request(
            &graphql_router,
            &admin_jwt,
            QUERY_EVENTS,
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let payload: serde_json::Value = serde_json::from_str(
            response["data"]["queryEvents"][0]["payload"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_ne!(payload["email"], "person@example.com");
        let ciphertext = payload["email"].as_str().unwrap().to_string();

        // Grant (a): can_read_sensitive.
        let sensitive_jwt = sign_jwt(&sensitive_role.external_subject);
        let response = graphql_request(
            &graphql_router,
            &sensitive_jwt,
            QUERY_EVENTS,
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let payload: serde_json::Value = serde_json::from_str(
            response["data"]["queryEvents"][0]["payload"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(payload["email"], "person@example.com");
        assert_eq!(payload["user_id"], "42");

        // Grant (b): the caller's own external_subject matches the
        // field's subject - "a caller reading their own data needs no
        // separate grant."
        let self_jwt = sign_jwt(&self_role.external_subject);
        let response = graphql_request(
            &graphql_router,
            &self_jwt,
            QUERY_EVENTS,
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let payload: serde_json::Value = serde_json::from_str(
            response["data"]["queryEvents"][0]["payload"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(payload["email"], "person@example.com");

        // forgetSubject destroys the key for real - even the previously
        // can_read_sensitive-granted caller now sees ciphertext again,
        // the crypto-shredding guarantee verified through the real read
        // path, not just a raw DB check.
        let response = graphql_request(
            &graphql_router,
            &admin_jwt,
            FORGET_SUBJECT_MUTATION,
            json!({ "bc": bc_name, "subjectKey": "user", "subjectValue": "42" }),
        )
        .await;
        assert_eq!(response["data"]["forgetSubject"]["status"], "DESTROYED");

        let response = graphql_request(
            &graphql_router,
            &sensitive_jwt,
            QUERY_EVENTS,
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let payload: serde_json::Value = serde_json::from_str(
            response["data"]["queryEvents"][0]["payload"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(payload["email"], ciphertext);
    });
}

/// The "unauthorised/no-subject-to-resolve caller never needs a master
/// key" property - a whole `Skilj` built with no `encryption_master_key`
/// at all still answers `queryEvents` successfully as long as nothing it
/// returns actually needs decrypting (no sensitive fields declared at
/// all here).
#[test]
fn queries_succeed_with_no_master_key_configured_when_nothing_needs_decrypting() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let database_url = test_database_url().await.unwrap();
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();

        let admin_subject = unique_name("admin");
        let admin_role = insert_role(&pool, &admin_subject).await;

        let bc_name = unique_name("logging");
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
        grant_admin(&pool, &admin_role, &bc, false).await;

        let (skilj, report) = Skilj::builder(database_url.clone())
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<PlainEvent>()
            .reconciliation_role(admin_subject)
            // Deliberately no .encryption_master_key(...) - nothing in
            // this bounded context ever declares a sensitive field.
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_mapping =
            skilj_core::db::get_active_role_access_mapping(&pool, &admin_role.id, &bc_name)
                .await
                .unwrap()
                .unwrap();
        let event_type = skilj_core::db::get_event_type(&pool, &bc_name, "PlainEvent")
            .await
            .unwrap()
            .unwrap();
        let token = skilj_core::access_control::create_direct_creation_token(
            &admin_mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            test_now(),
        )
        .unwrap();
        skilj_core::db::insert_direct_creation_token(&pool, &token)
            .await
            .unwrap();
        let credential = format!("{}.{}", token.id, token.secret);

        let rest_router = skilj.rest_router();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "payload": { "note": "hello" } }).to_string(),
            ))
            .unwrap();
        let response = rest_router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let graphql_router = skilj.graphql_router().await.unwrap();
        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let query = "query($bc: String!) { \
            queryEvents(boundedContext: $bc, eventTypes: [\"PlainEvent\"]) { \
                sequence payload \
            } \
        }";
        let response =
            graphql_request(&graphql_router, &admin_jwt, query, json!({ "bc": bc_name })).await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let payload: serde_json::Value = serde_json::from_str(
            response["data"]["queryEvents"][0]["payload"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(payload["note"], "hello");
    });
}
