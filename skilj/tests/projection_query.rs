//! End-to-end tests for `skilj-graphql`'s `ProjectionQuery` - the
//! JSON-Schema-driven `projection` field (see `skilj_graphql::projection_types`
//! for the type-generation mechanism itself, and its own crate-level unit
//! tests for the scalar/nullable/list/nested-object mapping in isolation;
//! this file is the real end-to-end path only). Same real-HTTP-through-
//! `Skilj::graphql_router()`, real-JWKS-server harness as
//! `skilj/tests/graphql_business_surfaces.rs` - see its own doc comment
//! for the details, duplicated here rather than extracted into shared
//! test-support (the same call every prior pass in this project already
//! made).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::SubsecRound;
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use skilj::{
    CommandType, EncryptionMasterKey, EventType, IdpConfig, Projection, SigningAlgorithm, Skilj,
};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{
    generate_token_id, generate_token_secret, CommandDecision, EventSpec, SensitiveField,
};
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
    fn direct_creation_allowed() -> bool {
        true
    }
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
    fn direct_creation_allowed() -> bool {
        true
    }
}

/// A real sensitive field on the *source* `EventType` - `email` is
/// declared sensitive, keyed by `customer_id`. `project()` below copies
/// it straight into `CustomerProfile`'s own state verbatim, still
/// ciphertext at that point (nothing decrypts before folding) - real
/// decrypt-on-read for a `Projection` happens entirely on the read side,
/// automatically, with no `Projection.sensitive_fields` declaration
/// anywhere ([§9](../../docs/architecture.md#next-steps)'s own "read_projection's own decrypt-on-read" pass).
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct AccountOpenedPayload {
    customer_id: String,
    email: String,
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
            subject_key: "customer".to_string(),
            subject_field: "customer_id".to_string(),
        }]
    }
}

enum BankingEvent {
    MoneyDeposited(MoneyDepositedPayload),
    ItemPurchased(ItemPurchasedPayload),
    AccountOpened(AccountOpenedPayload),
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
            "AccountOpened" => {
                Some(serde_json::from_str(&event.payload).map(BankingEvent::AccountOpened))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct WithdrawPayload {
    amount: i64,
}

struct WithdrawMoney;

impl CommandType for WithdrawMoney {
    type Payload = WithdrawPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "WithdrawMoney";
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "MoneyDeposited".to_string(),
                payload: serde_json::json!({ "amount": payload.amount }),
            }],
        }
    }
}

/// The one-level nested shape - exercises `projection_types`' `$ref`
/// resolution end-to-end, not just via its own crate-level unit tests.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct LastDeposit {
    amount: i64,
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct AccountBalanceState {
    total: i64,
    last_deposit: LastDeposit,
}

/// `sync()`, deliberately - `ProjectionQuery` is the surface under test
/// here, not the projection mechanism itself ([§8](../../docs/architecture.md#open-for-a-future-pass) item 6, already covered
/// by `skilj-core/tests/sync_projections.rs`/`async_projections.rs`) -
/// `sync` keeps `caught_up_to` deterministically current the moment a
/// command's write commits, so `waitForSequence` assertions below don't
/// also need to tolerate the background consumer's own poll cadence.
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

/// Identical to `AccountBalance` in every way except `TEAM_ONLY` -
/// exists purely to exercise Codeberg issue #17's whole-projection team
/// gate end to end, on both the `projection` field and (the fix itself)
/// `projectionSchema`.
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

/// Keyed by `customer_id` - [§9](../../docs/architecture.md#next-steps)'s own "keyed / multi-row Projections"
/// pass, exercised end-to-end: `ItemPurchased`'s own `customer_id` field
/// names which customer's row an event belongs to, so each customer gets
/// an independently-addressed instance from the shared `ItemPurchased`
/// stream, not one blob shared by all of them.
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

/// Keyed by `customer_id`, folding `AccountOpened`'s own sensitive
/// `email` field straight into its own state, verbatim - `project()`
/// never decrypts (nothing does, before folding), so `email` here is
/// real ciphertext until a granted caller queries it.
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct CustomerProfileState {
    email: String,
}

struct CustomerProfile;

impl Projection for CustomerProfile {
    type State = CustomerProfileState;
    type Event = BankingEvent;
    const NAME: &'static str = "CustomerProfile";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["AccountOpened"]
    }
    fn sync() -> bool {
        true
    }
    fn keys(event: &Self::Event) -> Vec<String> {
        match event {
            BankingEvent::AccountOpened(payload) => vec![payload.customer_id.clone()],
            _ => Vec::new(),
        }
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        if let BankingEvent::AccountOpened(payload) = event {
            state.email = payload.email.clone();
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
            .expect("failed to build a tokio runtime for projection_query tests")
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
    let database_name = "skilj_projection_query_test";
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

const SUBMIT_COMMAND_MUTATION: &str = "\
    mutation($bc: String!, $payload: String!) { \
        submitCommand(boundedContext: $bc, commandTypeName: \"WithdrawMoney\", payload: $payload) { \
            accepted triggeredEventSequences \
        } \
    }";

#[test]
fn projection_query_end_to_end() {
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
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<WithdrawMoney>()
            .projection::<AccountBalance>()
            .reconciliation_role(admin_subject)
            .projection_query_wait_timeout(std::time::Duration::from_millis(150))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let router = skilj.graphql_router().await.unwrap();

        let type_name =
            skilj_graphql::projection_types::graphql_type_name(&bc_name, "AccountBalance");
        let query = format!(
            "query($bc: String!, $name: String!, $wait: Int) {{ \
                projection(boundedContext: $bc, name: $name, waitForSequence: $wait) {{ \
                    ... on {type_name} {{ total lastDeposit {{ amount }} }} \
                }} \
            }}"
        );

        // Trigger a real command, producing a real event.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            SUBMIT_COMMAND_MUTATION,
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

        // No waitForSequence at all - answers from wherever the
        // projection currently is (sync, so already current).
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            &query,
            json!({ "bc": bc_name, "name": "AccountBalance", "wait": null }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["projection"]["total"], 20);
        assert_eq!(response["data"]["projection"]["lastDeposit"]["amount"], 20);

        // waitForSequence naming the triggering event's own sequence -
        // the read-your-writes lever actually runs and succeeds, not
        // just accepted syntactically.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            &query,
            json!({ "bc": bc_name, "name": "AccountBalance", "wait": first_sequence }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["projection"]["total"], 20);

        // waitForSequence past anything that will ever be reached within
        // the short configured timeout - a distinguishable timeout
        // rejection (ReadYourWritesWhenRequested), not a generic error.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            &query,
            json!({ "bc": bc_name, "name": "AccountBalance", "wait": first_sequence + 1000 }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "projection_caught_up_timed_out"
        );

        // A Read-level-only grant still succeeds - ProjectionQuery faces
        // ReadAccess, not AdminAccess (require_read_mapping, not
        // require_admin_mapping).
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
        let reader_mapping = RoleAccessMapping {
            role: reader_role,
            bounded_context: bc.clone(),
            level: AccessLevel::Read,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role_access_mapping(&pool, &reader_mapping)
            .await
            .unwrap();
        let reader_jwt = sign_jwt(&reader_subject);

        let response = graphql_request(
            &router,
            Some(&reader_jwt),
            &query,
            json!({ "bc": bc_name, "name": "AccountBalance", "wait": null }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["projection"]["total"], 20);

        // A caller with no grant on this bounded context at all is
        // rejected - GrantScopedToBoundedContext's own "no mapping
        // collapses into the same rejection a revoked one gets".
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
            &router,
            Some(&stranger_jwt),
            &query,
            json!({ "bc": bc_name, "name": "AccountBalance", "wait": null }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "grant_not_active"
        );

        // An unknown projection name - the resolver rejects before ever
        // needing to select a concrete fragment type, so the query
        // document (still naming the real type) stays valid regardless.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            &query,
            json!({ "bc": bc_name, "name": "NoSuchProjection", "wait": null }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "Projection_not_found"
        );
    });
}

/// Drift audit finding #9 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): `ProjectionQuery`'s own `exposes:
/// projection.name/schema/schema_version` had no route reachable at
/// `ReadAccess` level - only `TypeRegistration`'s `AdminAccess`-gated
/// `projections` query returned this data. Checks the new
/// `projectionSchema` field directly: reachable by a plain `ReadAccess`
/// grant (the fix), still rejects a caller with no grant at all, and
/// still rejects an unknown projection name.
#[test]
fn projection_schema_end_to_end() {
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
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<WithdrawMoney>()
            .projection::<AccountBalance>()
            .reconciliation_role(admin_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let router = skilj.graphql_router().await.unwrap();

        let query = "query($bc: String!, $name: String!) { \
            projectionSchema(boundedContext: $bc, name: $name) { \
                name schema schemaVersion \
            } \
        }";

        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            query,
            json!({ "bc": bc_name, "name": "AccountBalance" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(
            response["data"]["projectionSchema"]["name"],
            "AccountBalance"
        );
        assert_eq!(response["data"]["projectionSchema"]["schemaVersion"], 1);
        let schema = response["data"]["projectionSchema"]["schema"]
            .as_str()
            .unwrap();
        assert!(
            schema.contains("total"),
            "the real AccountBalanceState schema must be returned: {schema}"
        );

        // A plain Read-level grant reaches this too - the fix itself:
        // ProjectionQuery faces ReadAccess, not AdminAccess.
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
        let reader_mapping = RoleAccessMapping {
            role: reader_role,
            bounded_context: bc.clone(),
            level: AccessLevel::Read,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role_access_mapping(&pool, &reader_mapping)
            .await
            .unwrap();
        let reader_jwt = sign_jwt(&reader_subject);

        let response = graphql_request(
            &router,
            Some(&reader_jwt),
            query,
            json!({ "bc": bc_name, "name": "AccountBalance" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(
            response["data"]["projectionSchema"]["name"],
            "AccountBalance"
        );

        // No grant at all - still rejected.
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
            &router,
            Some(&stranger_jwt),
            query,
            json!({ "bc": bc_name, "name": "AccountBalance" }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "grant_not_active"
        );

        // An unknown projection name.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            query,
            json!({ "bc": bc_name, "name": "NoSuchProjection" }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "Projection_not_found"
        );
    });
}

/// Codeberg issue #17's own gap and fix, end to end over real HTTP: a
/// `TEAM_ONLY`-declaring projection rejects a Role not carrying the
/// required name on *both* `projection` and `projectionSchema`. Before
/// the fix, `projectionSchema` skipped this check entirely while
/// `projection` enforced it - a wrong-team caller could still learn the
/// projection's declared shape (name/schema/schemaVersion) even though
/// its actual data was correctly refused, contradicting
/// `TeamGatedWhenDeclared`'s "invisible, not merely unreadable" promise
/// for one field on the same surface.
#[test]
fn team_only_projection_gates_both_projection_and_projection_schema_end_to_end() {
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
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .command_type::<WithdrawMoney>()
            .projection::<StaffOnlyBalance>()
            .reconciliation_role(admin_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let router = skilj.graphql_router().await.unwrap();

        // Produce real state so an authorized read below proves it sees
        // actual data, not just an untouched default.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "payload": r#"{"amount":30}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );

        let type_name =
            skilj_graphql::projection_types::graphql_type_name(&bc_name, "StaffOnlyBalance");
        let data_query = format!(
            "query($bc: String!, $name: String!) {{ \
                projection(boundedContext: $bc, name: $name) {{ \
                    ... on {type_name} {{ total }} \
                }} \
            }}"
        );
        let schema_query = "query($bc: String!, $name: String!) { \
            projectionSchema(boundedContext: $bc, name: $name) { name schema } \
        }";

        // A "support" Role, plain read-level: on the team - both fields
        // succeed.
        let support_subject = unique_name("support");
        let support_role = Role {
            id: generate_token_id(),
            external_subject: support_subject.clone(),
            name: "support".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &support_role)
            .await
            .unwrap();
        let support_mapping = RoleAccessMapping {
            role: support_role,
            bounded_context: bc.clone(),
            level: AccessLevel::Read,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role_access_mapping(&pool, &support_mapping)
            .await
            .unwrap();
        let support_jwt = sign_jwt(&support_subject);

        let response = graphql_request(
            &router,
            Some(&support_jwt),
            &data_query,
            json!({ "bc": bc_name, "name": "StaffOnlyBalance" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "support role should read the data: {response:?}"
        );
        assert_eq!(response["data"]["projection"]["total"], 30);

        let response = graphql_request(
            &router,
            Some(&support_jwt),
            schema_query,
            json!({ "bc": bc_name, "name": "StaffOnlyBalance" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "support role should read the schema: {response:?}"
        );
        assert_eq!(
            response["data"]["projectionSchema"]["name"],
            "StaffOnlyBalance"
        );

        // A read-level Role with any other name: rejected on both
        // fields - `projectionSchema` is the fix this test exists to pin
        // down, `projection` is the pre-existing behaviour it must not
        // regress.
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
        let reader_mapping = RoleAccessMapping {
            role: reader_role,
            bounded_context: bc.clone(),
            level: AccessLevel::Read,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role_access_mapping(&pool, &reader_mapping)
            .await
            .unwrap();
        let reader_jwt = sign_jwt(&reader_subject);

        let response = graphql_request(
            &router,
            Some(&reader_jwt),
            &data_query,
            json!({ "bc": bc_name, "name": "StaffOnlyBalance" }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "not_on_required_team"
        );

        // The fix itself: before it, this query still reached
        // `get_projection` and returned the schema regardless of team
        // membership.
        let response = graphql_request(
            &router,
            Some(&reader_jwt),
            schema_query,
            json!({ "bc": bc_name, "name": "StaffOnlyBalance" }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "not_on_required_team"
        );
    });
}

/// [§9](../../docs/architecture.md#next-steps)'s own "keyed / multi-row Projections" pass, end-to-end: three real
/// `ItemPurchased` events (two for `"alice"`, one for `"bob"`), each
/// customer's own row queried independently, a never-touched key
/// answering with the default (empty) state rather than an error, and
/// the implicit `""` instance (`key` omitted entirely) - untouched by
/// any of these customer-keyed events - answering the same way.
#[test]
fn keyed_projection_end_to_end() {
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

        let bc_name = unique_name("shop");
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
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<ItemPurchased>()
            .projection::<CustomerPurchaseHistory>()
            .reconciliation_role(admin_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        // A real direct-creation token, minted the same way the admin
        // console already would - constructed directly here since
        // minting one isn't itself under test.
        let event_type = skilj_core::db::get_event_type(&pool, &bc_name, "ItemPurchased")
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
        for (customer, item) in [("alice", "book"), ("bob", "pen"), ("alice", "pen")] {
            let request = Request::builder()
                .method("POST")
                .uri("/v1/events/direct")
                .header("authorization", format!("Bearer {credential}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "payload": { "customer_id": customer, "item": item } }).to_string(),
                ))
                .unwrap();
            let response = rest_router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::CREATED);
        }

        let graphql_router = skilj.graphql_router().await.unwrap();
        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let type_name =
            skilj_graphql::projection_types::graphql_type_name(&bc_name, "CustomerPurchaseHistory");
        let query = format!(
            "query($bc: String!, $key: String) {{ \
                projection(boundedContext: $bc, name: \"CustomerPurchaseHistory\", key: $key) {{ \
                    ... on {type_name} {{ items }} \
                }} \
            }}"
        );

        let response = graphql_request(
            &graphql_router,
            Some(&admin_jwt),
            &query,
            json!({ "bc": bc_name, "key": "alice" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let items: Vec<&str> = response["data"]["projection"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(items, vec!["book", "pen"]);

        let response = graphql_request(
            &graphql_router,
            Some(&admin_jwt),
            &query,
            json!({ "bc": bc_name, "key": "bob" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let items: Vec<&str> = response["data"]["projection"]["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(items, vec!["pen"]);

        // A key nothing has touched yet - the default (empty) state, not
        // an error.
        let response = graphql_request(
            &graphql_router,
            Some(&admin_jwt),
            &query,
            json!({ "bc": bc_name, "key": "carol" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert!(response["data"]["projection"]["items"]
            .as_array()
            .unwrap()
            .is_empty());

        // key omitted entirely - the implicit "" instance, which none of
        // these customer-keyed events ever touched either.
        let response = graphql_request(
            &graphql_router,
            Some(&admin_jwt),
            &query,
            json!({ "bc": bc_name, "key": null }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert!(response["data"]["projection"]["items"]
            .as_array()
            .unwrap()
            .is_empty());
    });
}

const FORGET_SUBJECT_MUTATION: &str = "\
    mutation($bc: String!, $subjectKey: String!, $subjectValue: String!) { \
        forgetSubject(boundedContext: $bc, subjectKey: $subjectKey, subjectValue: $subjectValue) { \
            status \
        } \
    }";

/// [§9](../../docs/architecture.md#next-steps)'s own "read_projection's own decrypt-on-read" pass, end-to-end:
/// `CustomerProfile` is keyed by `customer_id` and folds `AccountOpened`'s
/// own sensitive `email` field straight into its own state, verbatim -
/// with no `Projection.sensitive_fields` declared anywhere. Neither
/// grant sees ciphertext; `can_read_sensitive` and the matching
/// `external_subject` both see plaintext; `forgetSubject` destroying the
/// key makes even the previously-granted caller see ciphertext again -
/// the crypto-shredding guarantee verified through a projection this
/// time, not just raw events (`skilj/tests/decrypt_on_read.rs` already
/// covers that for events).
#[test]
fn keyed_projection_decrypt_on_read_end_to_end() {
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

        let bc_name = unique_name("accounts");
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

        // can_read_sensitive = true - grant (a).
        let sensitive_subject = unique_name("sensitive-reader");
        let sensitive_role = Role {
            id: generate_token_id(),
            external_subject: sensitive_subject.clone(),
            name: "SensitiveReader".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &sensitive_role)
            .await
            .unwrap();
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: sensitive_role.clone(),
                bounded_context: bc.clone(),
                level: AccessLevel::Read,
                can_read_sensitive: true,
                scope: None,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();

        // external_subject matching the customer id used below - grant
        // (b), with no can_read_sensitive at all.
        let self_role = Role {
            id: generate_token_id(),
            external_subject: "cust-1".to_string(),
            name: "Self".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &self_role)
            .await
            .unwrap();
        skilj_core::db::insert_role_access_mapping(
            &pool,
            &RoleAccessMapping {
                role: self_role.clone(),
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
                SigningAlgorithm::Rs256,
            ))
            .bounded_context(bc_name.clone())
            .event_type::<AccountOpened>()
            .projection::<CustomerProfile>()
            .reconciliation_role(admin_subject)
            .encryption_master_key(EncryptionMasterKey::from_bytes([7u8; 32]))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        // A real direct-creation token, minted the same way the admin
        // console already would.
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
                json!({ "payload": { "customer_id": "cust-1", "email": "person@example.com" } })
                    .to_string(),
            ))
            .unwrap();
        let response = rest_router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let graphql_router = skilj.graphql_router().await.unwrap();
        let type_name =
            skilj_graphql::projection_types::graphql_type_name(&bc_name, "CustomerProfile");
        let query = format!(
            "query($bc: String!, $key: String) {{ \
                projection(boundedContext: $bc, name: \"CustomerProfile\", key: $key) {{ \
                    ... on {type_name} {{ email }} \
                }} \
            }}"
        );

        // Neither grant - ciphertext.
        let admin_jwt = sign_jwt(&admin_role.external_subject);
        let response = graphql_request(
            &graphql_router,
            Some(&admin_jwt),
            &query,
            json!({ "bc": bc_name, "key": "cust-1" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let ciphertext = response["data"]["projection"]["email"]
            .as_str()
            .unwrap()
            .to_string();
        assert_ne!(ciphertext, "person@example.com");

        // Grant (a): can_read_sensitive.
        let sensitive_jwt = sign_jwt(&sensitive_role.external_subject);
        let response = graphql_request(
            &graphql_router,
            Some(&sensitive_jwt),
            &query,
            json!({ "bc": bc_name, "key": "cust-1" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(
            response["data"]["projection"]["email"],
            "person@example.com"
        );

        // Grant (b): the caller's own external_subject matches the
        // instance's own key.
        let self_jwt = sign_jwt(&self_role.external_subject);
        let response = graphql_request(
            &graphql_router,
            Some(&self_jwt),
            &query,
            json!({ "bc": bc_name, "key": "cust-1" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(
            response["data"]["projection"]["email"],
            "person@example.com"
        );

        // forgetSubject destroys the key for real - even the previously
        // can_read_sensitive-granted caller now sees ciphertext again.
        let response = graphql_request(
            &graphql_router,
            Some(&admin_jwt),
            FORGET_SUBJECT_MUTATION,
            json!({ "bc": bc_name, "subjectKey": "customer", "subjectValue": "cust-1" }),
        )
        .await;
        assert_eq!(response["data"]["forgetSubject"]["status"], "DESTROYED");

        let response = graphql_request(
            &graphql_router,
            Some(&sensitive_jwt),
            &query,
            json!({ "bc": bc_name, "key": "cust-1" }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["projection"]["email"], ciphertext);
    });
}
