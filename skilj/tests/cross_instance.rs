//! End-to-end tests for cross-instance push completeness (Codeberg issue
//! #2; `@guarantee DeliverySpansInstances`/`RegistrationReachesEveryInstance`
//! in specs/skilj.allium) - two real `Skilj` instances, each its own
//! `axum::serve` listener with its own background tasks (including its
//! own `skilj_core::cross_instance::Listener`), sharing one Postgres
//! database. Same real-Postgres/real-JWKS-server/real-websocket harness
//! as `skilj/tests/event_subscription.rs` - see that file's own doc
//! comment for the details, duplicated here rather than extracted into
//! shared test-support (the same call every prior pass in this project
//! already made).
//!
//! Every assertion here is specifically about instance B observing
//! something that happened *only* on instance A - each instance's own
//! same-process push already has its own coverage in
//! `event_subscription.rs`; this file's whole point would be trivially
//! satisfied by same-instance delivery, so care is taken that instance A
//! and instance B never share more than the database connection string.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::SubsecRound;
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use skilj::{CommandType, EventType, IdpConfig, SigningAlgorithm, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, CommandDecision, EventSpec};
use std::time::Duration;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

// --- fixtures - identical shape to event_subscription.rs's own ---

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

#[allow(dead_code)]
enum BankingEvent {
    MoneyDeposited(MoneyDepositedPayload),
}

impl BoundedContextEvent for BankingEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "MoneyDeposited" => {
                Some(serde_json::from_str(&event.payload).map(BankingEvent::MoneyDeposited))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct DepositPayload {
    amount: i64,
}

struct DepositMoney;

impl CommandType for DepositMoney {
    type Payload = DepositPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "DepositMoney";
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "MoneyDeposited".to_string(),
                payload: serde_json::json!({ "amount": payload.amount }),
            }],
        }
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
            .expect("failed to build a tokio runtime for cross_instance tests")
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
    let database_name = "skilj_cross_instance_test";
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

// --- JWKS test server + JWT signing - identical to event_subscription.rs ---

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

const DEPOSIT_MONEY_MUTATION: &str = "\
    mutation($bc: String!, $payload: String!) { \
        submitCommand(boundedContext: $bc, commandTypeName: \"DepositMoney\", payload: $payload) { \
            accepted triggeredEventSequences rejectionReason \
        } \
    }";

const REVOKE_ROLE_ACCESS_MAPPING_MUTATION: &str = "\
    mutation($roleId: ID!, $bc: String!) { \
        revokeRoleAccessMapping(roleId: $roleId, boundedContext: $bc) { status } \
    }";

const REGISTER_PROJECTION_MUTATION: &str = "\
    mutation($bc: String!, $schema: String!) { \
        registerProjection(boundedContext: $bc, name: \"Balance\", schema: $schema, \
            sync: true, consumedEventTypes: [\"MoneyDeposited\"]) { outcome } \
    }";

const QUERY_FIELD_INTROSPECTION: &str = "{ __type(name: \"Query\") { fields { name } } }";

// --- graphql-transport-ws client helpers - identical to event_subscription.rs ---

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(url: &str) -> WsStream {
    let mut request = url
        .into_client_request()
        .expect("a ws:// URL is always a well-formed client request");
    request.headers_mut().insert(
        "sec-websocket-protocol",
        "graphql-transport-ws".parse().unwrap(),
    );
    let (stream, _response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("connecting to the test's own graphql_router() websocket listener never fails");
    stream
}

async fn ws_send_json(ws: &mut WsStream, value: serde_json::Value) {
    ws.send(Message::text(value.to_string()))
        .await
        .expect("sending on a freshly connected websocket never fails");
}

async fn ws_try_recv_json(ws: &mut WsStream, timeout: Duration) -> Option<serde_json::Value> {
    match tokio::time::timeout(timeout, ws.next()).await {
        Err(_) => None,
        Ok(None) => panic!("websocket stream ended unexpectedly"),
        Ok(Some(Err(e))) => panic!("websocket error: {e}"),
        Ok(Some(Ok(Message::Text(text)))) => {
            Some(serde_json::from_str(&text).expect("server sent well-formed JSON"))
        }
        Ok(Some(Ok(other))) => panic!("unexpected websocket message: {other:?}"),
    }
}

async fn ws_recv_json(ws: &mut WsStream) -> serde_json::Value {
    ws_try_recv_json(ws, Duration::from_secs(10))
        .await
        .expect("timed out waiting for a websocket message")
}

/// Mounts `skilj.graphql_router()` on a real, freshly-bound TCP listener -
/// every instance in this file needs its own, since `axum::Router::
/// oneshot` can't drive a websocket subscription (see this file's own
/// module doc comment).
async fn serve(skilj: &Skilj) -> (axum::Router, String) {
    let router = skilj.graphql_router().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind an ephemeral port for a graphql_router() test listener");
    let addr = listener.local_addr().unwrap();
    let serve_router = router.clone();
    tokio::spawn(async move {
        axum::serve(listener, serve_router)
            .await
            .expect("a graphql_router() test listener stopped unexpectedly");
    });
    (router, format!("ws://{addr}/graphql"))
}

#[test]
fn cross_instance_push_reaches_a_second_instance_sharing_one_database() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
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

        // `revokeRoleAccessMapping` is `require_active_superadmin`-gated,
        // distinct from every `RoleAccessMapping`-level actor this file's
        // other fixtures use - see `event_subscription.rs`'s own
        // identical fixture for the same mutation.
        let superadmin_subject = unique_name("superadmin");
        let superadmin_role = Role {
            id: generate_token_id(),
            external_subject: superadmin_subject.clone(),
            name: "Superadmin".to_string(),
            superadmin: true,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &superadmin_role)
            .await
            .unwrap();

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
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: admin_role.clone(),
                bounded_context: bc.clone(),
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();

        // A second, quiet-subscription role - revoked partway through,
        // never touched by any event, so its subscription can only close
        // via the push path (matching `event_subscription.rs`'s own
        // "quiet" revocation test), and here specifically via the
        // *cross-instance* push, since the revocation happens on
        // instance A while this role's subscription lives on instance B.
        let reader_subject = unique_name("reader");
        let reader_role = Role {
            id: generate_token_id(),
            external_subject: reader_subject.clone(),
            name: "Reader".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &reader_role)
            .await
            .unwrap();
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: reader_role.clone(),
                bounded_context: bc.clone(),
                level: AccessLevel::Read,
                can_read_sensitive: false,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();

        // --- two independent Skilj instances, one shared database ---

        let (skilj_a, report_a) = Skilj::builder(database_url.clone())
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .reconciliation_role(admin_subject.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(report_a.skipped_no_access, Vec::<String>::new());

        let (skilj_b, report_b) = Skilj::builder(database_url.clone())
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .reconciliation_role(admin_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report_b.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let reader_jwt = sign_jwt(&reader_role.external_subject);
        let superadmin_jwt = sign_jwt(&superadmin_role.external_subject);

        let (router_a, _ws_url_a) = serve(&skilj_a).await;
        let (router_b, ws_url_b) = serve(&skilj_b).await;

        // === 1. an event committed on instance A reaches a live
        //        subscription on instance B ===

        let mut ws = ws_connect(&ws_url_b).await;
        ws_send_json(
            &mut ws,
            json!({
                "type": "connection_init",
                "payload": { "Authorization": format!("Bearer {reader_jwt}") },
            }),
        )
        .await;
        assert_eq!(ws_recv_json(&mut ws).await["type"], "connection_ack");
        ws_send_json(
            &mut ws,
            json!({
                "id": "1",
                "type": "subscribe",
                "payload": {
                    "query": "subscription($bc: String!) { \
                        allEvents(boundedContext: $bc) { sequence payload } \
                    }",
                    "variables": { "bc": bc_name },
                },
            }),
        )
        .await;

        let response = graphql_request(
            &router_a,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":20}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);
        let sequence = response["data"]["submitCommand"]["triggeredEventSequences"][0]
            .as_i64()
            .unwrap();

        let delivered = ws_recv_json(&mut ws).await;
        assert_eq!(delivered["id"], "1");
        assert_eq!(delivered["type"], "next");
        assert_eq!(
            delivered["payload"]["data"]["allEvents"]["sequence"], sequence,
            "instance B's own live subscription must receive an event committed on instance A"
        );

        // === 2. a revocation on instance A closes a quiet subscription
        //        on instance B, with no event ever following it ===

        let response = graphql_request(
            &router_a,
            Some(&superadmin_jwt),
            REVOKE_ROLE_ACCESS_MAPPING_MUTATION,
            json!({ "roleId": reader_role.id, "bc": bc_name }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );

        let closing = ws_recv_json(&mut ws).await;
        assert_eq!(closing["id"], "1");
        assert_eq!(closing["type"], "next");
        assert_eq!(
            closing["payload"]["errors"][0]["extensions"]["code"], "grant_not_active",
            "a revocation on instance A must close instance B's own quiet subscription, \
             distinguishably, without any event ever having to arrive first"
        );
        let complete = ws_recv_json(&mut ws).await;
        assert_eq!(complete["id"], "1");
        assert_eq!(complete["type"], "complete");

        // === 3. a type registered on instance A becomes queryable on
        //        instance B's own GraphQL schema, without a restart ===

        let before = graphql_request(
            &router_b,
            Some(&admin_jwt),
            QUERY_FIELD_INTROSPECTION,
            json!({}),
        )
        .await;
        let fields_before: Vec<&str> = before["data"]["__type"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect();
        assert!(
            !fields_before.contains(&"projection"),
            "instance B's schema must not expose `projection` before anything is registered: \
             {fields_before:?}"
        );

        let schema = json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": { "balance": { "type": "integer" } },
        })
        .to_string();
        let response = graphql_request(
            &router_a,
            Some(&admin_jwt),
            REGISTER_PROJECTION_MUTATION,
            json!({ "bc": bc_name, "schema": schema }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );

        // The cross-instance schema rebuild is asynchronous (a `NOTIFY`
        // round trip through instance B's own background listener task) -
        // poll rather than assume it has already landed by the time this
        // continues.
        let mut fields_after = Vec::new();
        for _ in 0..50 {
            let after = graphql_request(
                &router_b,
                Some(&admin_jwt),
                QUERY_FIELD_INTROSPECTION,
                json!({}),
            )
            .await;
            fields_after = after["data"]["__type"]["fields"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| f["name"].as_str().unwrap().to_string())
                .collect();
            if fields_after.iter().any(|f| f == "projection") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            fields_after.iter().any(|f| f == "projection"),
            "instance B's schema must expose `projection` after a Projection was registered \
             on instance A, without instance B ever restarting: {fields_after:?}"
        );
    });
}
