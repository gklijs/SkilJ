//! End-to-end tests for `skilj-graphql`'s `EventSubscription` - `allEvents`
//! (the last surface out of [§8](../../docs/architecture.md#open-for-a-future-pass)/[§9](../../docs/architecture.md#next-steps)'s backlog). Unlike every other
//! `skilj/tests/graphql_*.rs` file, `tower::ServiceExt::oneshot` can't
//! drive this: a subscription is a long-lived streaming connection, not
//! a single request/response, so this one drives a real
//! `graphql-transport-ws` websocket client (`tokio-tungstenite`) against
//! `Skilj::graphql_router()` mounted on a real `axum::serve` listener.
//! Ordinary request/response calls (`submitCommand`, to trigger real
//! events) still go through `tower::ServiceExt::oneshot` on a cloned
//! `axum::Router` - `axum::Router` is cheap to clone, and a `Skilj`'s
//! `EventBroadcaster` is shared regardless of which router clone a
//! request went through (see `Skilj::rest_router`'s/`graphql_router`'s
//! own doc comments). Same real-Postgres/real-JWKS-server harness as
//! `skilj/tests/projection_query.rs` - see its own doc comment for the
//! details, duplicated here rather than extracted into shared
//! test-support (the same call every prior pass in this project already
//! made).

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

// `MoneyDeposited`'s own payload is never read here - no `Projection` is
// registered in this test at all (`EventSubscription` is the surface
// under test, not §8 item 6), unlike `skilj/tests/projection_query.rs`'s
// own `BankingEvent`, which an `AccountBalance` projection does read.
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
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for event_subscription tests")
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
    let url = skilj_test_support::database_url("skilj_event_subscription_test").await?;
    if !check_reachable(&url, "the test database").await {
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
    sign_jwt_expiring(
        subject,
        (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
    )
}

fn sign_jwt_expiring(subject: &str, exp: i64) -> String {
    let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(TEST_KID.to_string());
    let claims = json!({
        "sub": subject,
        "iss": TEST_ISSUER,
        "aud": TEST_AUDIENCE,
        "exp": exp,
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

// --- graphql-transport-ws client helpers ---
//
// `async_graphql_axum::GraphQLWebSocket` speaks both legacy
// `subscriptions-transport-ws` ("graphql-ws") and the modern
// `graphql-ws` library's protocol ("graphql-transport-ws" - see
// `async_graphql::http::websocket`'s own `Protocols` enum, confusingly
// named the other way around from its own wire string). This drives the
// modern one.

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

/// `None` when nothing arrived within `timeout` - the "an event in a
/// different bounded context never arrives" negative assertion's own
/// mechanism, since there's no positive signal to wait for instead.
async fn ws_try_recv_json(ws: &mut WsStream, timeout: Duration) -> Option<serde_json::Value> {
    // Server pings (docs/architecture.md §136) are answered by
    // tungstenite itself; skip past them.
    let next = async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                other => return other,
            }
        }
    };
    match tokio::time::timeout(timeout, next).await {
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
    ws_try_recv_json(ws, Duration::from_secs(5))
        .await
        .expect("timed out waiting for a websocket message")
}

#[test]
fn event_subscription_end_to_end() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let database_url = test_database_url().await.unwrap();
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();

        // The admin role reconciliation authenticates as, and that
        // triggers every real event in this test via submitCommand -
        // kept active throughout, distinct from the subscriber role
        // below (whose own mapping gets revoked mid-stream).
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
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: admin_role.clone(),
                bounded_context: bc.clone(),
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();

        // A second, unrelated bounded context - proves an event there
        // never arrives on a subscription scoped to `bc_name`.
        let other_bc_name = unique_name("other_banking");
        let other_bc = BoundedContext {
            name: other_bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        skilj_core::db::insert_bounded_context(&pool, &other_bc)
            .await
            .unwrap();
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: admin_role.clone(),
                bounded_context: other_bc.clone(),
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();

        // The subscriber - Read-level only (ReadAccess, not AdminAccess -
        // require_read_mapping), scoped to `bc_name` alone.
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
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
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
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .bounded_context(other_bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .reconciliation_role(admin_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let reader_jwt = sign_jwt(&reader_role.external_subject);

        let router = skilj.graphql_router().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind an ephemeral port for the GraphQL test listener");
        let addr = listener.local_addr().unwrap();
        let serve_router = router.clone();
        tokio::spawn(async move {
            axum::serve(listener, serve_router)
                .await
                .expect("the GraphQL test listener stopped unexpectedly");
        });

        // --- connect + connection_init + subscribe ---

        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        ws_send_json(
            &mut ws,
            json!({
                "type": "connection_init",
                "payload": { "Authorization": format!("Bearer {reader_jwt}") },
            }),
        )
        .await;
        let ack = ws_recv_json(&mut ws).await;
        assert_eq!(ack["type"], "connection_ack");

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

        // --- an event in a different bounded context never arrives ---

        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": other_bc_name, "payload": r#"{"amount":999}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);

        assert!(
            ws_try_recv_json(&mut ws, Duration::from_millis(500))
                .await
                .is_none(),
            "an event in a different bounded context must never be delivered"
        );

        // --- a real, matching event arrives with the right shape ---

        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":20}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let first_sequence = response["data"]["submitCommand"]["triggeredEventSequences"][0]
            .as_i64()
            .unwrap();

        let delivered = ws_recv_json(&mut ws).await;
        assert_eq!(delivered["id"], "1");
        assert_eq!(delivered["type"], "next");
        assert_eq!(
            delivered["payload"]["data"]["allEvents"]["sequence"],
            first_sequence
        );
        let delivered_payload: serde_json::Value = serde_json::from_str(
            delivered["payload"]["data"]["allEvents"]["payload"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(delivered_payload["amount"], 20);

        // --- revoking the subscriber mid-stream closes the connection,
        //     distinguishably, not silently ---

        skilj_core::db::revoke_active_role_access_mapping(
            &pool,
            &reader_role.id,
            &bc_name,
            test_now(),
        )
        .await
        .unwrap();

        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":5}"# }),
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
            closing["payload"]["errors"][0]["extensions"]["code"],
            "grant_not_active"
        );

        let complete = ws_recv_json(&mut ws).await;
        assert_eq!(complete["id"], "1");
        assert_eq!(complete["type"], "complete");
    });
}

const REVOKE_ROLE_ACCESS_MAPPING_MUTATION: &str = "\
    mutation($roleId: ID!, $bc: String!) { \
        revokeRoleAccessMapping(roleId: $roleId, boundedContext: $bc) { status } \
    }";

/// Drift audit finding #4 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): before the push half of
/// `RevocationClosesTheConnection` existed, a revoked subscription in a
/// bounded context that stayed quiet - no event ever arriving - would
/// have stayed open indefinitely, since the only re-check ran per
/// delivered event. This test's whole point is that no event is ever
/// submitted after the revocation - the connection has to close on its
/// own, promptly, or this fails on timeout.
#[test]
fn revoking_via_graphql_closes_a_quiet_subscription_without_waiting_for_an_event() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let database_url = test_database_url().await.unwrap();
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();

        // A real Superadmin, direct-inserted the same way
        // `admin_context_bootstrap.rs` does - `revokeRoleAccessMapping`
        // is `require_active_superadmin`-gated, distinct from every
        // `RoleAccessMapping`-level actor the rest of this file's own
        // fixtures use.
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
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: admin_role.clone(),
                bounded_context: bc.clone(),
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();

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
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
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
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .reconciliation_role(admin_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let superadmin_jwt = sign_jwt(&superadmin_role.external_subject);
        let reader_jwt = sign_jwt(&reader_role.external_subject);

        let router = skilj.graphql_router().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind an ephemeral port for the GraphQL test listener");
        let addr = listener.local_addr().unwrap();
        let serve_router = router.clone();
        tokio::spawn(async move {
            axum::serve(listener, serve_router)
                .await
                .expect("the GraphQL test listener stopped unexpectedly");
        });

        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        ws_send_json(
            &mut ws,
            json!({
                "type": "connection_init",
                "payload": { "Authorization": format!("Bearer {reader_jwt}") },
            }),
        )
        .await;
        let ack = ws_recv_json(&mut ws).await;
        assert_eq!(ack["type"], "connection_ack");

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

        // Nothing arrives yet - the subscription is genuinely quiet, not
        // merely "hasn't been given a chance to receive anything."
        assert!(
            ws_try_recv_json(&mut ws, Duration::from_millis(300))
                .await
                .is_none(),
            "no event has been submitted yet - nothing should arrive"
        );

        // Revoke through the real GraphQL mutation - not
        // `db::revoke_active_role_access_mapping` directly, the way the
        // end-to-end test above does - specifically so the push
        // notification this resolver now publishes actually fires. No
        // event is submitted after this, ever: the connection has to
        // close on its own.
        let response = graphql_request(
            &router,
            Some(&superadmin_jwt),
            REVOKE_ROLE_ACCESS_MAPPING_MUTATION,
            json!({ "roleId": reader_role.id, "bc": bc_name }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(
            response["data"]["revokeRoleAccessMapping"]["status"],
            "REVOKED"
        );

        let closing = ws_try_recv_json(&mut ws, Duration::from_secs(2))
            .await
            .expect(
                "the connection must close on its own, promptly, with no event ever \
                 submitted after the revocation",
            );
        assert_eq!(closing["id"], "1");
        assert_eq!(closing["type"], "next");
        assert_eq!(
            closing["payload"]["errors"][0]["extensions"]["code"],
            "grant_not_active"
        );

        let complete = ws_recv_json(&mut ws).await;
        assert_eq!(complete["id"], "1");
        assert_eq!(complete["type"], "complete");
    });
}

/// `eventsByType(filters: [...])` actually narrows a live push - real
/// `valid_filters`/`matches_filters` (this pass), not the eager
/// `filters_not_supported_error()` rejection every `filters` argument hit
/// before it. Reuses the exact harness/fixtures above rather than
/// duplicating the JWKS/JWT boilerplate a second time.
#[test]
fn events_by_type_subscription_narrows_by_a_real_filter() {
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
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: admin_role.clone(),
                bounded_context: bc.clone(),
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();

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
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
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
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .reconciliation_role(admin_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let reader_jwt = sign_jwt(&reader_role.external_subject);

        let router = skilj.graphql_router().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind an ephemeral port for the GraphQL test listener");
        let addr = listener.local_addr().unwrap();
        let serve_router = router.clone();
        tokio::spawn(async move {
            axum::serve(listener, serve_router)
                .await
                .expect("the GraphQL test listener stopped unexpectedly");
        });

        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        ws_send_json(
            &mut ws,
            json!({
                "type": "connection_init",
                "payload": { "Authorization": format!("Bearer {reader_jwt}") },
            }),
        )
        .await;
        let ack = ws_recv_json(&mut ws).await;
        assert_eq!(ack["type"], "connection_ack");

        ws_send_json(
            &mut ws,
            json!({
                "id": "1",
                "type": "subscribe",
                "payload": {
                    "query": "subscription($bc: String!) { \
                        eventsByType(boundedContext: $bc, eventType: \"MoneyDeposited\", \
                            filters: [{ field: \"amount\", operator: GREATER_THAN, value: \"10\" }]) \
                        { sequence payload } \
                    }",
                    "variables": { "bc": bc_name },
                },
            }),
        )
        .await;

        // A non-matching event (amount=5) - never delivered.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":5}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert!(
            ws_try_recv_json(&mut ws, Duration::from_millis(500))
                .await
                .is_none(),
            "a filtered-out event must never be delivered"
        );

        // A matching event (amount=20) - delivered.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":20}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let matching_sequence = response["data"]["submitCommand"]["triggeredEventSequences"][0]
            .as_i64()
            .unwrap();

        let delivered = ws_recv_json(&mut ws).await;
        assert_eq!(delivered["id"], "1");
        assert_eq!(delivered["type"], "next");
        assert_eq!(
            delivered["payload"]["data"]["eventsByType"]["sequence"],
            matching_sequence
        );
    });
}

/// `FilterOperator::In` (docs/architecture.md's filterable-scalar-types
/// pass) through the real GraphQL wire, not just the pure-function layer
/// (`skilj-core/tests/event_filtering.rs` covers the full matrix,
/// including the format-gated `Near`/`SimilarColor`/`InSubnet`) - proves
/// `gql_types::filter_operator_enum`'s new `IN` item and
/// `resolvers::parse_filters`' new match arm actually reach
/// `valid_filters`/`matches_filters` end to end over a live subscription.
/// Otherwise an exact copy of
/// `events_by_type_subscription_narrows_by_a_real_filter`'s own harness -
/// see that test's doc comment for why it isn't deduplicated further.
#[test]
fn events_by_type_subscription_narrows_by_a_real_in_filter() {
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
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: admin_role.clone(),
                bounded_context: bc.clone(),
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();

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
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
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
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .reconciliation_role(admin_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let reader_jwt = sign_jwt(&reader_role.external_subject);

        let router = skilj.graphql_router().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("failed to bind an ephemeral port for the GraphQL test listener");
        let addr = listener.local_addr().unwrap();
        let serve_router = router.clone();
        tokio::spawn(async move {
            axum::serve(listener, serve_router)
                .await
                .expect("the GraphQL test listener stopped unexpectedly");
        });

        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        ws_send_json(
            &mut ws,
            json!({
                "type": "connection_init",
                "payload": { "Authorization": format!("Bearer {reader_jwt}") },
            }),
        )
        .await;
        let ack = ws_recv_json(&mut ws).await;
        assert_eq!(ack["type"], "connection_ack");

        ws_send_json(
            &mut ws,
            json!({
                "id": "1",
                "type": "subscribe",
                "payload": {
                    "query": "subscription($bc: String!) { \
                        eventsByType(boundedContext: $bc, eventType: \"MoneyDeposited\", \
                            filters: [{ field: \"amount\", operator: IN, value: \"20,30\" }]) \
                        { sequence payload } \
                    }",
                    "variables": { "bc": bc_name },
                },
            }),
        )
        .await;

        // A non-matching event (amount=5, not in "20,30") - never delivered.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":5}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert!(
            ws_try_recv_json(&mut ws, Duration::from_millis(500))
                .await
                .is_none(),
            "a filtered-out event must never be delivered"
        );

        // A matching event (amount=20, in "20,30") - delivered.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":20}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let matching_sequence = response["data"]["submitCommand"]["triggeredEventSequences"][0]
            .as_i64()
            .unwrap();

        let delivered = ws_recv_json(&mut ws).await;
        assert_eq!(delivered["id"], "1");
        assert_eq!(delivered["type"], "next");
        assert_eq!(
            delivered["payload"]["data"]["eventsByType"]["sequence"],
            matching_sequence
        );
    });
}

/// The websocket endpoint shares `/graphql`'s body cap: a message larger
/// than `GraphqlLimits::max_request_body_bytes` closes the connection
/// instead of being read and parsed. Unauthenticated connections are
/// accepted (a subscription may need no caller), so without the cap
/// anyone could make the server buffer axum's default 64 MiB per message.
#[test]
fn an_oversized_websocket_message_closes_the_connection() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let (skilj, _) = Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        ws_send_json(&mut ws, json!({ "type": "connection_init", "payload": {} })).await;
        assert_eq!(ws_recv_json(&mut ws).await["type"], "connection_ack");

        let huge = json!({
            "id": "1",
            "type": "subscribe",
            "payload": {
                "query": "{ __typename }",
                "variables": { "padding": "x".repeat(3 * 1024 * 1024) },
            },
        });
        // The send itself may fail once the server drops the connection.
        let _ = ws.send(Message::text(huge.to_string())).await;
        let outcome = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("the server neither answered nor closed within 5s");
        match outcome {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => {}
            Some(Ok(other)) => panic!("an oversized message was processed: {other:?}"),
        }
    });
}

/// `GraphqlLimits::max_subscriptions_per_connection`: once a connection
/// has that many subscriptions running, another is refused with
/// `too_many_subscriptions`; completing one frees its slot for the next.
#[test]
fn a_connection_holds_at_most_max_subscriptions_at_once() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();
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
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: reader_role,
                bounded_context: bc,
                level: AccessLevel::Read,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();
        let (skilj, _) = Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                TEST_AUDIENCE,
                SigningAlgorithm::Rs256,
            ))
            .graphql_limits(skilj::GraphqlLimits {
                max_subscriptions_per_connection: 2,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        ws_send_json(
            &mut ws,
            json!({
                "type": "connection_init",
                "payload": { "Authorization": format!("Bearer {}", sign_jwt(&reader_subject)) },
            }),
        )
        .await;
        assert_eq!(ws_recv_json(&mut ws).await["type"], "connection_ack");

        let subscribe = |id: &str| {
            json!({
                "id": id,
                "type": "subscribe",
                "payload": {
                    "query": "subscription($bc: String!) { allEvents(boundedContext: $bc) { sequence } }",
                    "variables": { "bc": bc_name },
                },
            })
        };
        for id in ["1", "2"] {
            ws_send_json(&mut ws, subscribe(id)).await;
        }
        assert_eq!(
            ws_try_recv_json(&mut ws, Duration::from_millis(500)).await,
            None,
            "two subscriptions fit"
        );

        // A resolver error arrives as `next` carrying `errors`, then the
        // server's own `complete` for that id.
        ws_send_json(&mut ws, subscribe("3")).await;
        let refused = ws_recv_json(&mut ws).await;
        assert_eq!(refused["id"], "3", "{refused}");
        assert_eq!(
            refused["payload"]["errors"][0]["extensions"]["code"], "too_many_subscriptions",
            "{refused}"
        );
        let completed = ws_recv_json(&mut ws).await;
        assert_eq!(
            (completed["id"].as_str(), completed["type"].as_str()),
            (Some("3"), Some("complete")),
            "{completed}"
        );

        // The server acknowledges a client `complete` with its own, once
        // it has dropped that subscription's stream (and so its slot).
        ws_send_json(&mut ws, json!({ "id": "1", "type": "complete" })).await;
        let completed = ws_recv_json(&mut ws).await;
        assert_eq!(
            (completed["id"].as_str(), completed["type"].as_str()),
            (Some("1"), Some("complete")),
            "{completed}"
        );
        ws_send_json(&mut ws, subscribe("4")).await;
        assert_eq!(
            ws_try_recv_json(&mut ws, Duration::from_millis(500)).await,
            None,
            "completing one freed its slot"
        );
    });
}

/// A reader Role with Read access to a fresh bounded context, and a
/// `graphql_router()` serving on an ephemeral port under `limits`.
/// Returns (address, the reader's JWT subject, the bounded context name).
async fn serve_reader_graphql(
    database_url: String,
    limits: skilj::GraphqlLimits,
) -> (std::net::SocketAddr, String, String) {
    let jwks_url = serve_jwks().await;
    let pool = skilj_core::db::connect(&database_url).await.unwrap();
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
    skilj_core::db::insert_role_access_mapping(
        &pool,
        &RoleAccessMapping {
            role: reader_role,
            bounded_context: bc,
            level: AccessLevel::Read,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        },
    )
    .await
    .unwrap();
    let (skilj, _) = Skilj::builder(database_url)
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
        .identity_provider(IdpConfig::new(
            jwks_url.parse().unwrap(),
            TEST_ISSUER,
            TEST_AUDIENCE,
            SigningAlgorithm::Rs256,
        ))
        .graphql_limits(limits)
        .build()
        .await
        .unwrap();
    let router = skilj.graphql_router().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (addr, reader_subject, bc_name)
}

/// `connection_init` with `jwt`, then one `allEvents` subscription.
async fn init_and_subscribe(ws: &mut WsStream, jwt: &str, bc_name: &str) {
    ws_send_json(
        ws,
        json!({
            "type": "connection_init",
            "payload": { "Authorization": format!("Bearer {jwt}") },
        }),
    )
    .await;
    assert_eq!(ws_recv_json(ws).await["type"], "connection_ack");
    ws_send_json(
        ws,
        json!({
            "id": "1",
            "type": "subscribe",
            "payload": {
                "query": "subscription($bc: String!) { allEvents(boundedContext: $bc) { sequence } }",
                "variables": { "bc": bc_name },
            },
        }),
    )
    .await;
}

/// The next frame that isn't a ping or pong, within `timeout`.
async fn next_non_ping(
    ws: &mut WsStream,
    timeout: Duration,
) -> Option<Result<Message, tokio_tungstenite::tungstenite::Error>> {
    tokio::time::timeout(timeout, async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                other => return other,
            }
        }
    })
    .await
    .expect("timed out waiting for a websocket frame")
}

fn assert_closed_with(
    frame: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    code: u16,
    reason: &str,
) {
    match frame {
        Some(Ok(Message::Close(Some(frame)))) => {
            assert_eq!(u16::from(frame.code), code, "{frame:?}");
            assert_eq!(frame.reason.as_str(), reason);
        }
        other => panic!("expected a {code} close frame, got {other:?}"),
    }
}

/// docs/architecture.md §135: the JWT from `connection_init` is only
/// checked then, so the connection - and a subscription running on it -
/// used to outlive it indefinitely. Now the server closes it with 4403
/// once the token's `exp` (plus the 60s verification leeway) passes.
#[test]
fn a_websocket_is_closed_when_its_credential_expires() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let (addr, reader_subject, bc_name) =
            serve_reader_graphql(database_url, Default::default()).await;

        // Past `exp`, but still inside the leeway for another ~3s.
        let exp = chrono::Utc::now().timestamp() - 57;
        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        init_and_subscribe(&mut ws, &sign_jwt_expiring(&reader_subject, exp), &bc_name).await;
        assert_eq!(
            ws_try_recv_json(&mut ws, Duration::from_millis(500)).await,
            None,
            "the subscription runs while the credential is valid"
        );

        assert_closed_with(
            next_non_ping(&mut ws, Duration::from_secs(10)).await,
            4403,
            "credential expired",
        );
    });
}

/// docs/architecture.md §136: a websocket that never sends
/// `connection_init` used to be held open indefinitely.
#[test]
fn a_websocket_that_never_initialises_is_closed() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let (addr, _, _) = serve_reader_graphql(
            database_url,
            skilj::GraphqlLimits {
                websocket_init_timeout: Duration::from_millis(300),
                ..Default::default()
            },
        )
        .await;

        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        assert_closed_with(
            next_non_ping(&mut ws, Duration::from_secs(5)).await,
            4408,
            "connection initialisation timeout",
        );
    });
}

/// docs/architecture.md §136: the server pings, a client that answers
/// (tungstenite does, whenever it reads) stays connected well past the
/// dead-peer window, and one that has stopped answering is dropped -
/// before, a vanished peer held its connection and subscriptions forever.
#[test]
fn a_peer_answering_pings_is_kept_and_a_silent_one_dropped() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let (addr, reader_subject, bc_name) = serve_reader_graphql(
            database_url,
            skilj::GraphqlLimits {
                websocket_ping_interval: Some(Duration::from_millis(200)),
                ..Default::default()
            },
        )
        .await;
        let jwt = sign_jwt(&reader_subject);

        let mut live = ws_connect(&format!("ws://{addr}/graphql")).await;
        init_and_subscribe(&mut live, &jwt, &bc_name).await;
        let mut silent = ws_connect(&format!("ws://{addr}/graphql")).await;
        init_and_subscribe(&mut silent, &jwt, &bc_name).await;

        // Reading answers pings; five dead-peer windows pass. Not reading
        // `silent` meanwhile leaves its pings unanswered.
        assert_eq!(
            ws_try_recv_json(&mut live, Duration::from_secs(2)).await,
            None,
            "a peer answering pings stays connected"
        );

        let ended = next_non_ping(&mut silent, Duration::from_secs(5)).await;
        assert!(
            matches!(ended, None | Some(Err(_))),
            "a silent peer's connection is dropped, got {ended:?}"
        );
    });
}

/// docs/architecture.md §84: resuming from `fromSequence` delivers the
/// events committed since then - before this subscription existed - in
/// order, then the live feed with no duplicate; a span over
/// `max_events_per_read` is refused rather than loaded.
#[test]
fn resuming_from_a_sequence_replays_the_missed_span_then_goes_live() {
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
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: admin_role,
                bounded_context: bc,
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();
        let (skilj, _) = Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                TEST_AUDIENCE,
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .reconciliation_role(admin_subject.clone())
            .max_events_per_read(3)
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();
        let serve_router = router.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, serve_router).await.unwrap();
        });
        let jwt = sign_jwt(&admin_subject);

        let mut sequences = Vec::new();
        for amount in [1, 2, 3, 4] {
            let response = graphql_request(
                &router,
                Some(&jwt),
                DEPOSIT_MONEY_MUTATION,
                json!({ "bc": bc_name, "payload": format!(r#"{{"amount":{amount}}}"#) }),
            )
            .await;
            assert!(response.get("errors").is_none(), "{response}");
            sequences.push(
                response["data"]["submitCommand"]["triggeredEventSequences"][0]
                    .as_i64()
                    .unwrap(),
            );
        }

        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        ws_send_json(
            &mut ws,
            json!({
                "type": "connection_init",
                "payload": { "Authorization": format!("Bearer {jwt}") },
            }),
        )
        .await;
        assert_eq!(ws_recv_json(&mut ws).await["type"], "connection_ack");
        let subscribe = |id: &str, from: i64| {
            json!({
                "id": id,
                "type": "subscribe",
                "payload": {
                    "query": "subscription($bc: String!, $from: Int) { \
                        allEvents(boundedContext: $bc, fromSequence: $from) { sequence } }",
                    "variables": { "bc": bc_name, "from": from },
                },
            })
        };

        // Read up to the first event, "away" for the other three: they
        // were committed before this subscription existed.
        ws_send_json(&mut ws, subscribe("1", sequences[0])).await;
        let mut received = Vec::new();
        for _ in 0..3 {
            let message = ws_recv_json(&mut ws).await;
            assert_eq!(message["type"], "next", "{message}");
            received.push(
                message["payload"]["data"]["allEvents"]["sequence"]
                    .as_i64()
                    .unwrap(),
            );
        }
        assert_eq!(received, sequences[1..].to_vec());

        // Then live, with nothing replayed twice.
        let response = graphql_request(
            &router,
            Some(&jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":5}"# }),
        )
        .await;
        let live = response["data"]["submitCommand"]["triggeredEventSequences"][0]
            .as_i64()
            .unwrap();
        let message = ws_recv_json(&mut ws).await;
        assert_eq!(
            message["payload"]["data"]["allEvents"]["sequence"], live,
            "{message}"
        );
        assert_eq!(
            ws_try_recv_json(&mut ws, Duration::from_millis(300)).await,
            None
        );

        // eventsByType resumes the same way, over its own event type.
        ws_send_json(
            &mut ws,
            json!({
                "id": "3",
                "type": "subscribe",
                "payload": {
                    "query": "subscription($bc: String!, $from: Int) { \
                        eventsByType(boundedContext: $bc, eventType: \"MoneyDeposited\", \
                        fromSequence: $from) { sequence } }",
                    "variables": { "bc": bc_name, "from": sequences[2] },
                },
            }),
        )
        .await;
        let mut received = Vec::new();
        for _ in 0..2 {
            let message = ws_recv_json(&mut ws).await;
            assert_eq!(message["id"], "3", "{message}");
            received.push(
                message["payload"]["data"]["eventsByType"]["sequence"]
                    .as_i64()
                    .unwrap(),
            );
        }
        assert_eq!(received, vec![sequences[3], live]);

        // Five committed events from the start is over the cap of 3.
        ws_send_json(&mut ws, subscribe("2", -1)).await;
        let refused = ws_recv_json(&mut ws).await;
        assert_eq!(refused["id"], "2", "{refused}");
        assert_eq!(
            refused["payload"]["errors"][0]["extensions"]["code"], "resume_span_too_large",
            "{refused}"
        );

        // docs/architecture.md §148: past the latest committed sequence is
        // refused - started from there, the subscription would drop every
        // live event up to it - while the latest itself is fine.
        ws_send_json(&mut ws, subscribe("4", live + 1000)).await;
        // Past the `complete` that followed subscription "2"'s error.
        let refused = loop {
            let message = ws_recv_json(&mut ws).await;
            if message["type"] != "complete" {
                break message;
            }
        };
        assert_eq!(refused["id"], "4", "{refused}");
        assert_eq!(
            refused["payload"]["errors"][0]["extensions"]["code"], "from_sequence_not_committed",
            "{refused}"
        );
        assert_eq!(ws_recv_json(&mut ws).await["type"], "complete");
        ws_send_json(&mut ws, subscribe("5", live)).await;
        assert_eq!(
            ws_try_recv_json(&mut ws, Duration::from_millis(300)).await,
            None,
            "subscribing from the latest sequence is accepted, with nothing to replay"
        );

        // docs/architecture.md §176: `fromSequence` with the epoch it was
        // read in. Another epoch's is refused - after a failover it may
        // name a different event - and the current one is accepted.
        let epoch = graphql_request(&router, Some(&jwt), "{ epoch }", json!({})).await;
        let epoch = epoch["data"]["epoch"].as_str().unwrap().to_string();
        let subscribe_in = |id: &str, from: i64, epoch: &str| {
            json!({
                "id": id,
                "type": "subscribe",
                "payload": {
                    "query": "subscription($bc: String!, $from: Int, $epoch: String) { \
                        allEvents(boundedContext: $bc, fromSequence: $from, epoch: $epoch) \
                        { sequence } }",
                    "variables": { "bc": bc_name, "from": from, "epoch": epoch },
                },
            })
        };
        ws_send_json(&mut ws, subscribe_in("6", live, "elsewhere-1")).await;
        let refused = ws_recv_json(&mut ws).await;
        assert_eq!(refused["id"], "6", "{refused}");
        assert_eq!(
            refused["payload"]["errors"][0]["extensions"]["code"], "epoch_changed",
            "{refused}"
        );
        assert_eq!(ws_recv_json(&mut ws).await["type"], "complete");
        ws_send_json(&mut ws, subscribe_in("7", live, &epoch)).await;
        assert_eq!(
            ws_try_recv_json(&mut ws, Duration::from_millis(300)).await,
            None,
            "the current epoch is accepted"
        );
    });
}

/// docs/architecture.md §90: the broadcaster sees events in publish order,
/// not commit order - here an event committed but never published to this
/// instance (standing in for another instance's event whose `NOTIFY` hasn't
/// arrived, or a concurrent commit that hasn't published yet), followed by
/// one committed and published normally. The subscriber must get both, in
/// sequence order, not just the later one.
#[test]
fn a_live_subscription_delivers_in_sequence_order_across_an_unpublished_event() {
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
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: admin_role,
                bounded_context: bc,
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();
        let (skilj, _) = Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                TEST_AUDIENCE,
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .reconciliation_role(admin_subject.clone())
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();
        let serve_router = router.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, serve_router).await.unwrap();
        });
        let jwt = sign_jwt(&admin_subject);

        let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
        ws_send_json(
            &mut ws,
            json!({
                "type": "connection_init",
                "payload": { "Authorization": format!("Bearer {jwt}") },
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
                    "query": "subscription($bc: String!) { allEvents(boundedContext: $bc) { sequence } }",
                    "variables": { "bc": bc_name },
                },
            }),
        )
        .await;
        // Let the subscription start before anything is committed.
        assert_eq!(ws_try_recv_json(&mut ws, Duration::from_millis(300)).await, None);

        // Committed, never published here.
        let event_type = skilj_core::db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let unpublished = Event {
            bounded_context: event_type.bounded_context.clone(),
            event_type: event_type.clone(),
            payload: r#"{"amount":1}"#.to_string(),
            metadata: skilj_core::shared::Metadata {
                r#type: event_type.name.clone(),
                version: event_type.schema_version,
                client_id: "another-instance".to_string(),
                created_at: test_now(),
                correlation_id: None,
                causation_id: None,
            },
            sequence: skilj_core::db::next_sequence(&pool, &bc_name).await.unwrap(),
            tags: Vec::new(),
            encryption_keys: Vec::new(),
            origin: skilj_core::event_store::EventOrigin::DirectlyCreated,
        };
        skilj_core::db::insert_event(&pool, &unpublished, None)
            .await
            .unwrap();

        // Committed and published normally.
        let response = graphql_request(
            &router,
            Some(&jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":2}"# }),
        )
        .await;
        let published = response["data"]["submitCommand"]["triggeredEventSequences"][0]
            .as_i64()
            .unwrap();

        let mut received = Vec::new();
        for _ in 0..2 {
            let message = ws_recv_json(&mut ws).await;
            assert_eq!(message["type"], "next", "{message}");
            received.push(message["payload"]["data"]["allEvents"]["sequence"].as_i64().unwrap());
        }
        assert_eq!(received, vec![unpublished.sequence, published]);
        assert_eq!(ws_try_recv_json(&mut ws, Duration::from_millis(300)).await, None);
    });
}
