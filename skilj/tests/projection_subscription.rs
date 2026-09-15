//! End-to-end tests for `skilj-graphql`'s `projectionUpdates` subscription
//! (Codeberg issue #23). Same real-Postgres/real-JWKS-server/real-websocket
//! harness as `skilj/tests/event_subscription.rs` - see its own doc comment
//! for the details, duplicated here rather than extracted into shared
//! test-support (the same call every prior pass in this project already
//! made). `Projection` fixtures below mirror `skilj/tests/projection_query.rs`'s
//! own (`AccountBalance`/`StaffOnlyBalance`/`CustomerPurchaseHistory`
//! shapes) - `projectionUpdates` pushes through the identical
//! `projection_query::fetch_projection_result` read path that surface's own
//! tests already exercise, so these tests focus on what's genuinely new:
//! what triggers a push, what gets pushed, and the two places
//! `projectionUpdates` deliberately diverges from `EventSubscription`
//! (`RecvError::Lagged` self-heals instead of closing; a non-matching key
//! is a silent skip, no DB round trip at all).
//!
//! Not re-tested here (already covered by `projection_query.rs` through the
//! shared `fetch_projection_result` function both surfaces call): owner-tag
//! cross-tenant scoping, sensitive-field decrypt-on-read, and
//! `waitForSequence`-style timeout behaviour beyond what the async-catch-up
//! test below already proves for the subscription's own use of it.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::SubsecRound;
use futures_util::{SinkExt, StreamExt};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use skilj::{CommandType, EventType, IdpConfig, Projection, SigningAlgorithm, Skilj};
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

// --- fixtures ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct MoneyDepositedPayload {
    amount: i64,
}

struct MoneyDeposited;

impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct ItemPurchasedPayload {
    customer_id: String,
    item: String,
}

struct ItemPurchased;

impl EventType for ItemPurchased {
    type Payload = ItemPurchasedPayload;
    const NAME: &'static str = "ItemPurchased";
}

enum BankingEvent {
    MoneyDeposited(MoneyDepositedPayload),
    ItemPurchased(ItemPurchasedPayload),
}

impl BoundedContextEvent for BankingEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "MoneyDeposited" => {
                Some(serde_json::from_str(&event.payload).map(BankingEvent::MoneyDeposited))
            }
            "ItemPurchased" => {
                Some(serde_json::from_str(&event.payload).map(BankingEvent::ItemPurchased))
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

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct PurchaseItemPayload {
    customer_id: String,
    item: String,
}

struct PurchaseItem;

impl CommandType for PurchaseItem {
    type Payload = PurchaseItemPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "PurchaseItem";
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "ItemPurchased".to_string(),
                payload: serde_json::json!({
                    "customer_id": payload.customer_id,
                    "item": payload.item,
                }),
            }],
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct LastDeposit {
    amount: i64,
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct AccountBalanceState {
    total: i64,
    last_deposit: LastDeposit,
}

/// `sync()` (the default) - the test bounded context this is registered
/// into also registers `CustomerPurchaseHistory`/`StaffOnlyBalance`,
/// neither of which needs async catch-up either; `AsyncAccountBalance`
/// below is the one deliberately-async projection, kept separate so its
/// own test's timing assertions aren't entangled with these.
struct AccountBalance;

impl Projection for AccountBalance {
    type State = AccountBalanceState;
    type Event = BankingEvent;
    const NAME: &'static str = "AccountBalance";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["MoneyDeposited"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        if let BankingEvent::MoneyDeposited(payload) = event {
            state.total += payload.amount;
            state.last_deposit = LastDeposit {
                amount: payload.amount,
            };
        }
    }
}

/// Identical to `AccountBalance` except `sync()` returns `false` - folded
/// only by the background `catch_up_bounded_context` poller, not inside
/// the triggering command's own transaction. Exists purely to prove
/// `projectionUpdates` actually waits for catch-up (via
/// `fetch_projection_result`'s own `wait_for_sequence` plumbing) before
/// pushing, rather than racing it and pushing a stale/default value.
struct AsyncAccountBalance;

impl Projection for AsyncAccountBalance {
    type State = AccountBalanceState;
    type Event = BankingEvent;
    const NAME: &'static str = "AsyncAccountBalance";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["MoneyDeposited"]
    }
    fn sync() -> bool {
        false
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        if let BankingEvent::MoneyDeposited(payload) = event {
            state.total += payload.amount;
            state.last_deposit = LastDeposit {
                amount: payload.amount,
            };
        }
    }
}

/// Identical to `AccountBalance` except `TEAM_ONLY` - exercises
/// `fetch_projection_result`'s team gate as reached through
/// `projectionUpdates` specifically (rejecting the subscription attempt
/// itself, before any stream is ever created - see this file's own
/// `team_only_projection_rejects_subscription_before_any_push` test).
struct StaffOnlyBalance;

impl Projection for StaffOnlyBalance {
    type State = AccountBalanceState;
    type Event = BankingEvent;
    const NAME: &'static str = "StaffOnlyBalance";
    const TEAM_ONLY: Option<&'static str> = Some("support");
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["MoneyDeposited"]
    }
    fn sync() -> bool {
        true
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        if let BankingEvent::MoneyDeposited(payload) = event {
            state.total += payload.amount;
            state.last_deposit = LastDeposit {
                amount: payload.amount,
            };
        }
    }
}

/// Keyed by `customer_id` - `projectionUpdates`'s own key-filtering path
/// (`ProjectionDispatcher::keys()` deciding whether a delivered event
/// even names this subscription's own `key`) needs a keyed projection to
/// exercise at all; `AccountBalance` above never has more than one
/// implicit `""` instance.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct PurchaseHistoryState {
    items: Vec<String>,
}

struct CustomerPurchaseHistory;

impl Projection for CustomerPurchaseHistory {
    type State = PurchaseHistoryState;
    type Event = BankingEvent;
    const NAME: &'static str = "CustomerPurchaseHistory";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["ItemPurchased"]
    }
    fn sync() -> bool {
        true
    }
    fn keys(event: &Self::Event) -> Vec<String> {
        match event {
            BankingEvent::ItemPurchased(payload) => vec![payload.customer_id.clone()],
            _ => Vec::new(),
        }
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        if let BankingEvent::ItemPurchased(payload) = event {
            state.items.push(payload.item.clone());
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
            .expect("failed to build a tokio runtime for projection_subscription tests")
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
    let database_name = "skilj_projection_subscription_test";
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

const PURCHASE_ITEM_MUTATION: &str = "\
    mutation($bc: String!, $payload: String!) { \
        submitCommand(boundedContext: $bc, commandTypeName: \"PurchaseItem\", payload: $payload) { \
            accepted triggeredEventSequences rejectionReason \
        } \
    }";

// --- graphql-transport-ws client helpers (identical to event_subscription.rs) ---

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
    ws_try_recv_json(ws, Duration::from_secs(5))
        .await
        .expect("timed out waiting for a websocket message")
}

async fn ws_recv_json_within(ws: &mut WsStream, timeout: Duration) -> serde_json::Value {
    ws_try_recv_json(ws, timeout)
        .await
        .expect("timed out waiting for a websocket message")
}

/// The full builder/role/mapping scaffolding every test below shares -
/// one `Admin`-level role (triggers real commands), one `Read`-level
/// `reader_role` (the subscriber, `RoleAccessMapping` inserted by the
/// caller afterward since its `level`/team membership varies per test).
struct Fixture {
    pool: skilj_core::db::Pool,
    bc_name: String,
    admin_subject: String,
    admin_role: Role,
    reader_role: Role,
}

async fn setup(pool: skilj_core::db::Pool, bc_name: String) -> Fixture {
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

    let reader_role = Role {
        id: generate_token_id(),
        external_subject: unique_name("reader"),
        name: "Reader".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    skilj_core::db::insert_role(&pool, &reader_role)
        .await
        .unwrap();

    Fixture {
        pool,
        bc_name,
        admin_subject,
        admin_role,
        reader_role,
    }
}

async fn grant_reader(fixture: &Fixture) {
    skilj_core::db::insert_role_access_mapping(
        &fixture.pool,
        &RoleAccessMapping {
            role: fixture.reader_role.clone(),
            bounded_context: BoundedContext {
                name: fixture.bc_name.clone(),
                status: BoundedContextStatus::Active,
                created_at: test_now(),
                created_by: ContextCreator::SystemCreator,
                template: None,
            },
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
}

/// Subscribes over a fresh websocket connection and returns it positioned
/// right after `connection_ack` + the `subscribe` message - the caller
/// reads whatever comes next (an initial push, or an immediate error for
/// an unauthorized subscribe attempt).
async fn subscribe(
    addr: std::net::SocketAddr,
    jwt: &str,
    query: &str,
    variables: serde_json::Value,
) -> WsStream {
    let mut ws = ws_connect(&format!("ws://{addr}/graphql")).await;
    ws_send_json(
        &mut ws,
        json!({
            "type": "connection_init",
            "payload": { "Authorization": format!("Bearer {jwt}") },
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
            "payload": { "query": query, "variables": variables },
        }),
    )
    .await;
    ws
}

#[test]
fn projection_updates_pushes_initial_state_then_each_change_then_closes_on_revocation() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let database_url = test_database_url().await.unwrap();
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();
        let fixture = setup(pool.clone(), unique_name("banking")).await;
        grant_reader(&fixture).await;

        let (skilj, report) = Skilj::builder(database_url.clone())
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(fixture.bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .projection::<AccountBalance>()
            .reconciliation_role(fixture.admin_subject.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&fixture.admin_role.external_subject);
        let reader_jwt = sign_jwt(&fixture.reader_role.external_subject);

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

        let type_name =
            skilj_graphql::projection_types::graphql_type_name(&fixture.bc_name, "AccountBalance");
        let subscription_query = format!(
            "subscription($bc: String!) {{ \
                projectionUpdates(boundedContext: $bc, name: \"AccountBalance\") {{ \
                    ... on {type_name} {{ total lastDeposit {{ amount }} }} \
                }} \
            }}"
        );

        let mut ws = subscribe(
            addr,
            &reader_jwt,
            &subscription_query,
            json!({ "bc": fixture.bc_name }),
        )
        .await;

        // --- initial push: default state, no event has ever happened ---

        let initial = ws_recv_json(&mut ws).await;
        assert_eq!(initial["id"], "1");
        assert_eq!(initial["type"], "next");
        assert_eq!(initial["payload"]["data"]["projectionUpdates"]["total"], 0);

        // --- a real matching event pushes the new total ---

        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": fixture.bc_name, "payload": r#"{"amount":20}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );

        let pushed = ws_recv_json(&mut ws).await;
        assert_eq!(pushed["payload"]["data"]["projectionUpdates"]["total"], 20);
        assert_eq!(
            pushed["payload"]["data"]["projectionUpdates"]["lastDeposit"]["amount"],
            20
        );

        // --- a second event pushes again, cumulatively - proves this
        //     isn't a one-shot push ---

        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": fixture.bc_name, "payload": r#"{"amount":5}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );

        let pushed = ws_recv_json(&mut ws).await;
        assert_eq!(pushed["payload"]["data"]["projectionUpdates"]["total"], 25);

        // --- revoking the subscriber mid-stream closes the connection,
        //     distinguishably, not silently (mirrors
        //     event_subscription.rs's identical assertion) ---

        skilj_core::db::revoke_active_role_access_mapping(
            &pool,
            &fixture.reader_role.id,
            &fixture.bc_name,
            test_now(),
        )
        .await
        .unwrap();

        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": fixture.bc_name, "payload": r#"{"amount":1}"# }),
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

/// `ProjectionDispatcher::keys()`-based filtering: an event that changes a
/// *different* key's own instance never triggers a push (no DB round trip
/// even happens - unobservable from outside, but the absence of any push
/// is), while an event naming the subscribed key does.
#[test]
fn projection_updates_only_fires_for_the_subscribed_key() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let database_url = test_database_url().await.unwrap();
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();
        let fixture = setup(pool.clone(), unique_name("shop")).await;
        grant_reader(&fixture).await;

        let (skilj, report) = Skilj::builder(database_url.clone())
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(fixture.bc_name.clone())
            .event_type::<ItemPurchased>()
            .command_type::<PurchaseItem>()
            .projection::<CustomerPurchaseHistory>()
            .reconciliation_role(fixture.admin_subject.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&fixture.admin_role.external_subject);
        let reader_jwt = sign_jwt(&fixture.reader_role.external_subject);

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

        let type_name = skilj_graphql::projection_types::graphql_type_name(
            &fixture.bc_name,
            "CustomerPurchaseHistory",
        );
        let subscription_query = format!(
            "subscription($bc: String!, $key: String) {{ \
                projectionUpdates(boundedContext: $bc, name: \"CustomerPurchaseHistory\", key: $key) {{ \
                    ... on {type_name} {{ items }} \
                }} \
            }}"
        );

        let mut ws = subscribe(
            addr,
            &reader_jwt,
            &subscription_query,
            json!({ "bc": fixture.bc_name, "key": "alice" }),
        )
        .await;

        let initial = ws_recv_json(&mut ws).await;
        assert_eq!(
            initial["payload"]["data"]["projectionUpdates"]["items"],
            serde_json::Value::Array(Vec::new())
        );

        // A purchase for a different customer never pushes anything.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            PURCHASE_ITEM_MUTATION,
            json!({ "bc": fixture.bc_name, "payload": r#"{"customer_id":"bob","item":"pen"}"# }),
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
            "an event for a different key must never push"
        );

        // A purchase for the subscribed customer pushes the new state.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            PURCHASE_ITEM_MUTATION,
            json!({ "bc": fixture.bc_name, "payload": r#"{"customer_id":"alice","item":"book"}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );

        let pushed = ws_recv_json(&mut ws).await;
        let items: Vec<&str> = pushed["payload"]["data"]["projectionUpdates"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(items, vec!["book"]);
    });
}

/// Codeberg issue #17's whole-projection team gate, reached through
/// `projectionUpdates` specifically: a Role not carrying `StaffOnlyBalance`'s
/// required `"support"` name never even gets a stream - the rejection
/// arrives as the subscription's own first (and only) message, before any
/// event could possibly have been involved, proving the check runs before
/// `state.event_broadcaster.subscribe()` rather than only gating pushes.
/// A Role that *does* carry the team name subscribes normally.
#[test]
fn team_only_projection_rejects_subscription_before_any_push() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let database_url = test_database_url().await.unwrap();
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();
        let fixture = setup(pool.clone(), unique_name("banking")).await;
        grant_reader(&fixture).await;

        // A second reader, on the required "support" team.
        let staff_role = Role {
            id: generate_token_id(),
            external_subject: unique_name("staff"),
            name: "support".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &staff_role)
            .await
            .unwrap();
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: staff_role.clone(),
                bounded_context: BoundedContext {
                    name: fixture.bc_name.clone(),
                    status: BoundedContextStatus::Active,
                    created_at: test_now(),
                    created_by: ContextCreator::SystemCreator,
                    template: None,
                },
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
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(fixture.bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .projection::<StaffOnlyBalance>()
            .reconciliation_role(fixture.admin_subject.clone())
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let reader_jwt = sign_jwt(&fixture.reader_role.external_subject);
        let staff_jwt = sign_jwt(&staff_role.external_subject);

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

        let type_name = skilj_graphql::projection_types::graphql_type_name(
            &fixture.bc_name,
            "StaffOnlyBalance",
        );
        let subscription_query = format!(
            "subscription($bc: String!) {{ \
                projectionUpdates(boundedContext: $bc, name: \"StaffOnlyBalance\") {{ \
                    ... on {type_name} {{ total }} \
                }} \
            }}"
        );

        // --- the non-team reader is rejected immediately ---

        let mut ws = subscribe(
            addr,
            &reader_jwt,
            &subscription_query,
            json!({ "bc": fixture.bc_name }),
        )
        .await;

        let rejected = ws_recv_json(&mut ws).await;
        assert_eq!(rejected["id"], "1");
        assert_eq!(rejected["type"], "next");
        assert_eq!(
            rejected["payload"]["errors"][0]["extensions"]["code"],
            "not_on_required_team"
        );

        let complete = ws_recv_json(&mut ws).await;
        assert_eq!(complete["id"], "1");
        assert_eq!(complete["type"], "complete");

        // --- the staff reader subscribes normally ---

        let mut staff_ws = subscribe(
            addr,
            &staff_jwt,
            &subscription_query,
            json!({ "bc": fixture.bc_name }),
        )
        .await;
        let initial = ws_recv_json(&mut staff_ws).await;
        assert_eq!(initial["payload"]["data"]["projectionUpdates"]["total"], 0);
    });
}

/// An async (`sync() == false`) projection: `MoneyDeposited`'s effect
/// isn't folded into `projection_state` until `catch_up_bounded_context`'s
/// own next poll tick, not inside the triggering command's transaction.
/// Proves `projectionUpdates` actually waits for that catch-up (via
/// `fetch_projection_result`'s `wait_for_sequence` plumbing, passed the
/// triggering event's own sequence) rather than racing it - a naive
/// immediate refetch would risk pushing the still-default `total: 0`
/// with no second event ever arriving to correct it, since
/// `ProjectionDispatcher::keys()` already matched on the first one.
#[test]
fn projection_updates_waits_for_async_projection_catch_up() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let database_url = test_database_url().await.unwrap();
        let jwks_url = serve_jwks().await;
        let pool = skilj_core::db::connect(&database_url).await.unwrap();
        let fixture = setup(pool.clone(), unique_name("banking")).await;
        grant_reader(&fixture).await;

        let (skilj, report) = Skilj::builder(database_url.clone())
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(fixture.bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .projection::<AsyncAccountBalance>()
            .reconciliation_role(fixture.admin_subject.clone())
            .async_projection_poll_interval(Duration::from_millis(50))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&fixture.admin_role.external_subject);
        let reader_jwt = sign_jwt(&fixture.reader_role.external_subject);

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

        let type_name = skilj_graphql::projection_types::graphql_type_name(
            &fixture.bc_name,
            "AsyncAccountBalance",
        );
        let subscription_query = format!(
            "subscription($bc: String!) {{ \
                projectionUpdates(boundedContext: $bc, name: \"AsyncAccountBalance\") {{ \
                    ... on {type_name} {{ total }} \
                }} \
            }}"
        );

        let mut ws = subscribe(
            addr,
            &reader_jwt,
            &subscription_query,
            json!({ "bc": fixture.bc_name }),
        )
        .await;

        let initial = ws_recv_json(&mut ws).await;
        assert_eq!(initial["payload"]["data"]["projectionUpdates"]["total"], 0);

        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            DEPOSIT_MONEY_MUTATION,
            json!({ "bc": fixture.bc_name, "payload": r#"{"amount":30}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );

        // Generous relative to the 50ms poll interval - the push must
        // already reflect the post-catch-up total the moment it arrives.
        let pushed = ws_recv_json_within(&mut ws, Duration::from_secs(5)).await;
        assert_eq!(pushed["payload"]["data"]["projectionUpdates"]["total"], 30);
    });
}
