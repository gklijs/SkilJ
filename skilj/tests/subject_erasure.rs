//! End-to-end tests for `SubjectErasure` (`forgetSubject`) and the real,
//! non-empty `protect_sensitive_fields` path it depends on - see the plan
//! at `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md` and
//! `docs/architecture.md`'s own write-up of this pass. Direct event
//! creation over REST provisions the real `EncryptionKey`/ciphertext (no
//! command/`decide()` machinery needed to exercise `protect_sensitive_fields`
//! itself), `forgetSubject` is then exercised over GraphQL - same
//! real-Postgres + real-local-JWKS-server harness as every other
//! `skilj/tests/graphql_*.rs`, see `graphql_business_surfaces.rs`'s own
//! doc comment for the details, duplicated here rather than extracted
//! into shared test-support (the same call every prior pass in this
//! project already made).

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
/// The `aud` this deployment's tokens carry - `IdpConfig` requires one.
const TEST_AUDIENCE: &str = "skilj-test-client";

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
    // For the parked-delivery half of forgetSubject (§91): an
    // ExternalEvent-kind parked row carries this kind of token.
    fn external_creation_allowed() -> bool {
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

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for subject_erasure tests")
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
            .then_some(TestDb { database_url });
    }

    let url = skilj_test_support::database_url("skilj_subject_erasure_test").await?;
    if !check_reachable(&url, "embedded PostgreSQL").await {
        return None;
    }
    Some(TestDb { database_url: url })
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
        "aud": TEST_AUDIENCE,
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

const FORGET_SUBJECT_MUTATION: &str = "\
    mutation($bc: String!, $subjectKey: String!, $subjectValue: String!) { \
        forgetSubject(boundedContext: $bc, subjectKey: $subjectKey, subjectValue: $subjectValue) { \
            status subjectKey subjectValue \
        } \
    }";

#[test]
fn subject_erasure_end_to_end() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let database_url = test_database_url().await.unwrap();
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();

        let admin_subject = unique_name("admin");
        let admin_role = Role {
            id: generate_token_id(),
            external_subject: admin_subject.clone(),
            name: "Admin".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &admin_role)
            .await
            .unwrap();

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

        let admin_mapping = RoleAccessMapping {
            role: admin_role.clone(),
            bounded_context: bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role_access_mapping(&pool, &admin_mapping)
            .await
            .unwrap();

        let (skilj, report) = Skilj::builder(database_url.clone())
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                TEST_AUDIENCE,
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
        // console already would (via GraphQL) - constructed directly here
        // since minting one isn't itself under test.
        let event_type = skilj_core::db::get_event_type(&pool, &bc_name, "AccountOpened")
            .await
            .unwrap()
            .unwrap();
        let token = skilj_core::access_control::create_direct_creation_token(
            &admin_mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
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

        // The stored payload is genuinely encrypted, not plaintext - a
        // real assertion against the raw stored row, not just "the API
        // accepted it".
        let events = skilj_core::db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let stored: serde_json::Value = serde_json::from_str(&events[0].payload).unwrap();
        assert_eq!(stored["user_id"], "42"); // subject_field itself is read, never encrypted
        let ciphertext = stored["email"].as_str().unwrap();
        assert_ne!(ciphertext, "person@example.com");

        // A real, active EncryptionKey now exists for this subject.
        let key = skilj_core::db::get_active_encryption_key(&pool, &bc_name, "user", "42")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            key.status,
            skilj_core::event_store::EncryptionKeyStatus::Active
        );

        // docs/architecture.md §91: parked deliveries hold the plaintext
        // request. Three, parked by a bridge: one naming subject 42 through
        // the event type's own sensitive field, one naming 43, and one whose
        // token is gone (so its type can't be resolved) but whose request
        // mentions 42.
        let external_token = skilj_core::access_control::create_external_event_token(
            &admin_mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        skilj_core::db::insert_external_event_token(&pool, &external_token)
            .await
            .unwrap();
        let park = |identifier: &'static str, token_id: String, user_id: &'static str| {
            let pool = pool.clone();
            let bc_name = bc_name.clone();
            async move {
                skilj_core::db::insert_parked_delivery(
                    &pool,
                    &bc_name,
                    "kafka-inbound",
                    skilj_core::db::ParkedDeliveryKind::ExternalEvent,
                    identifier,
                    Some(&token_id),
                    None,
                    None,
                    &json!({ "payload": { "email": "person@example.com", "user_id": user_id } }),
                    "connection refused",
                    1,
                    test_now(),
                    test_now(),
                )
                .await
                .unwrap()
                .id
            }
        };
        park("users:0:1", external_token.id.clone(), "42").await;
        let kept = park("users:0:2", external_token.id.clone(), "43").await;
        park(
            "users:0:3",
            "a-token-that-no-longer-exists".to_string(),
            "42",
        )
        .await;

        let graphql_router = skilj.graphql_router().await.unwrap();
        let admin_jwt = sign_jwt(&admin_role.external_subject);

        // forgetSubject destroys it for real.
        let response = graphql_request(
            &graphql_router,
            &admin_jwt,
            FORGET_SUBJECT_MUTATION,
            json!({ "bc": bc_name, "subjectKey": "user", "subjectValue": "42" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["forgetSubject"]["status"], "DESTROYED");
        assert_eq!(
            skilj_core::db::get_active_encryption_key(&pool, &bc_name, "user", "42")
                .await
                .unwrap(),
            None
        );
        // ...and takes the parked plaintext naming subject 42 with it, both
        // the resolvable row and the unresolvable one; 43's row stays.
        let parked: Vec<String> = skilj_core::db::list_parked_deliveries(&pool, &bc_name)
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.id)
            .collect();
        assert_eq!(parked, vec![kept]);

        // A second forgetSubject on the same, now-destroyed subject is
        // rejected - there is nothing active left to find.
        let response = graphql_request(
            &graphql_router,
            &admin_jwt,
            FORGET_SUBJECT_MUTATION,
            json!({ "bc": bc_name, "subjectKey": "user", "subjectValue": "42" }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "EncryptionKey_not_found"
        );

        // A caller with no grant on this bounded context at all is
        // rejected before ever reaching the lookup.
        let stranger_subject = unique_name("stranger");
        let stranger_role = Role {
            id: generate_token_id(),
            external_subject: stranger_subject.clone(),
            name: "Stranger".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &stranger_role)
            .await
            .unwrap();
        let stranger_jwt = sign_jwt(&stranger_subject);
        let response = graphql_request(
            &graphql_router,
            &stranger_jwt,
            FORGET_SUBJECT_MUTATION,
            json!({ "bc": bc_name, "subjectKey": "user", "subjectValue": "42" }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "grant_not_active"
        );
    });
}
