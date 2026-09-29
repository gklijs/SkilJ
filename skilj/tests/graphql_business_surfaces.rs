//! End-to-end tests for `skilj-graphql`'s Phase 3 - `EventQuery`,
//! `CommandQuery`, `CommandSubmission`. Same real-HTTP-through-
//! `Skilj::graphql_router()`, real-JWKS-server harness as
//! `skilj/tests/graphql_admin_console.rs`/`graphql_type_registration.rs`,
//! see either's own doc comment for the details, duplicated here rather
//! than extracted into shared test-support (the same call every prior
//! pass in this project already made).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::SubsecRound;
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use skilj::{requires_role, CommandType, EventType, IdpConfig, SigningAlgorithm, Skilj, Snapshot};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::Pool;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, CommandDecision, EventSpec, TagMapping};
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

enum BankingEvent {
    #[allow(dead_code)]
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
struct WithdrawPayload {
    amount: i64,
}

struct WithdrawMoney;

impl CommandType for WithdrawMoney {
    type Payload = WithdrawPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "WithdrawMoney";
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        if payload.amount > 1000 {
            CommandDecision::Rejected {
                reason: "insufficient funds".to_string(),
                kind: "insufficient_funds".to_string(),
            }
        } else {
            CommandDecision::Accepted {
                events: vec![EventSpec {
                    event_type: "MoneyDeposited".to_string(),
                    payload: serde_json::json!({ "amount": payload.amount }),
                }],
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct CloseAccountPayload {}

struct CloseAccount;

#[requires_role("treasury_officer")]
impl CommandType for CloseAccount {
    type Payload = CloseAccountPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "CloseAccount";
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

// --- a small, separate fixture for the private-field mechanism
// (docs/architecture.md's own write-up of this pass) - `note` is an
// `own`-kind private field, visible by default only to whoever submitted
// the `AddTicketNote` command that created it.

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct TicketNoteAddedPayload {
    note: String,
}

struct TicketNoteAdded;

impl EventType for TicketNoteAdded {
    type Payload = TicketNoteAddedPayload;
    const NAME: &'static str = "TicketNoteAdded";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn private_fields() -> Vec<skilj_core::shared::PrivateField> {
        vec![skilj_core::shared::PrivateField {
            field: "note".to_string(),
            kind: skilj_core::shared::PrivateFieldKind::Own,
            team: None,
            addressee_field: None,
        }]
    }
}

enum TicketNoteEvent {
    #[allow(dead_code)]
    TicketNoteAdded(TicketNoteAddedPayload),
}

impl BoundedContextEvent for TicketNoteEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "TicketNoteAdded" => {
                Some(serde_json::from_str(&event.payload).map(TicketNoteEvent::TicketNoteAdded))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct AddTicketNotePayload {
    note: String,
}

struct AddTicketNote;

impl CommandType for AddTicketNote {
    type Payload = AddTicketNotePayload;
    type Event = TicketNoteEvent;
    const NAME: &'static str = "AddTicketNote";
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "TicketNoteAdded".to_string(),
                payload: serde_json::json!({ "note": payload.note }),
            }],
        }
    }
}

// --- a small, separate fixture for inspectSnapshot (docs/architecture.md
// §19) - not `BankingEvent`/`WithdrawMoney` above, deliberately: those
// carry no tags at all, and `Snapshot` needs a real one to scope
// against. `skilj-demo/tests/snapshot.rs` already proves `decide_from_snapshot`
// itself end to end (including a tampered-row proof); this fixture's
// whole job is exercising `inspectSnapshot` - Admin-gated, `null` when
// cold, a real error for an unregistered name.

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct ThingHappenedPayload {
    thing_id: String,
    amount: i64,
}

struct ThingHappened;

impl EventType for ThingHappened {
    type Payload = ThingHappenedPayload;
    const NAME: &'static str = "ThingHappened";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping {
            key: "thing".to_string(),
            field: "thing_id".to_string(),
        }]
    }
}

enum ThingEvent {
    ThingHappened(ThingHappenedPayload),
}

impl BoundedContextEvent for ThingEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "ThingHappened" => {
                Some(serde_json::from_str(&event.payload).map(ThingEvent::ThingHappened))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct ThingTotalState {
    total: i64,
}

struct ThingTotalSnapshot;

impl Snapshot for ThingTotalSnapshot {
    type State = ThingTotalState;
    type Event = ThingEvent;
    const NAME: &'static str = "ThingTotalSnapshot";
    const TAG_KEY: &'static str = "thing";
    const VERSION: u64 = 1;
    fn fold(state: &mut Self::State, event: &Self::Event) {
        match event {
            ThingEvent::ThingHappened(p) => state.total += p.amount,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct DoThingFastPayload {
    thing_id: String,
    amount: i64,
}

struct DoThingFast;

impl CommandType for DoThingFast {
    type Payload = DoThingFastPayload;
    type Event = ThingEvent;
    const NAME: &'static str = "DoThingFast";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping {
            key: "thing".to_string(),
            field: "thing_id".to_string(),
        }]
    }
    fn snapshot() -> Option<&'static str> {
        Some("ThingTotalSnapshot")
    }
    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        let total = matching_events.iter().fold(0i64, |t, e| match e {
            ThingEvent::ThingHappened(p) => t + p.amount,
        });
        do_thing_decision(payload, total)
    }
    fn decide_from_snapshot(
        payload: &Self::Payload,
        snapshot_state_json: &str,
        events_since_snapshot: &[Self::Event],
    ) -> CommandDecision {
        let snapshot: ThingTotalState =
            serde_json::from_str(snapshot_state_json).unwrap_or_default();
        let total = events_since_snapshot
            .iter()
            .fold(snapshot.total, |t, e| match e {
                ThingEvent::ThingHappened(p) => t + p.amount,
            });
        do_thing_decision(payload, total)
    }
}

fn do_thing_decision(payload: &DoThingFastPayload, _total: i64) -> CommandDecision {
    CommandDecision::Accepted {
        events: vec![EventSpec {
            event_type: "ThingHappened".to_string(),
            payload: serde_json::json!({ "thing_id": payload.thing_id, "amount": payload.amount }),
        }],
    }
}

// --- a second, separate fixture for `inspectSnapshot`'s own owner-tag
// scoping (cross-tenant read fix, docs/architecture.md's own write-up of
// these passes) - not `ThingHappened`/`ThingTotalSnapshot` above,
// deliberately: those carry only one tag ("thing", the snapshot's own
// `TAG_KEY`), and owner-scoping needs a *second*, distinct tag key to
// prove the derivation doesn't just default to `TAG_KEY` itself.

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct TicketOpenedPayload {
    ticket_id: String,
    company_id: String,
    amount: i64,
}

struct TicketOpened;

impl EventType for TicketOpened {
    type Payload = TicketOpenedPayload;
    const NAME: &'static str = "TicketOpened";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn tag_mappings() -> Vec<TagMapping> {
        vec![
            TagMapping {
                key: "ticket".to_string(),
                field: "ticket_id".to_string(),
            },
            TagMapping {
                key: "company".to_string(),
                field: "company_id".to_string(),
            },
        ]
    }
}

enum TicketEvent {
    TicketOpened(TicketOpenedPayload),
}

impl BoundedContextEvent for TicketEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "TicketOpened" => {
                Some(serde_json::from_str(&event.payload).map(TicketEvent::TicketOpened))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct TicketTotalState {
    total: i64,
}

struct TicketTotalSnapshot;

impl Snapshot for TicketTotalSnapshot {
    type State = TicketTotalState;
    type Event = TicketEvent;
    const NAME: &'static str = "TicketTotalSnapshot";
    const TAG_KEY: &'static str = "ticket";
    const OWNER_TAG_KEY: Option<&'static str> = Some("company");
    const VERSION: u64 = 1;
    fn fold(state: &mut Self::State, event: &Self::Event) {
        match event {
            TicketEvent::TicketOpened(p) => state.total += p.amount,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OpenTicketPayload {
    ticket_id: String,
    company_id: String,
    amount: i64,
}

struct OpenTicket;

impl CommandType for OpenTicket {
    type Payload = OpenTicketPayload;
    type Event = TicketEvent;
    const NAME: &'static str = "OpenTicket";
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "TicketOpened".to_string(),
                payload: serde_json::json!({
                    "ticket_id": payload.ticket_id,
                    "company_id": payload.company_id,
                    "amount": payload.amount,
                }),
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
            .expect("failed to build a tokio runtime for graphql_business_surfaces tests")
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

    let url = skilj_test_support::database_url("skilj_graphql_business_surfaces_test").await?;
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

/// Builds a fully reconciled `Skilj` (`MoneyDeposited`/`WithdrawMoney`/
/// `CloseAccount` registered), a real admin-level Role + grant on a
/// fresh bounded context (admin level satisfies both `AdminAccess`'s own
/// requirement and `CommandSubmission`'s `Write | Admin` check), and that
/// Role's own signed JWT.
async fn setup() -> (Skilj, Pool, String, String, Role) {
    setup_with(|builder| builder).await
}

/// `setup()`, with a hook to adjust the builder before `build()`.
async fn setup_with(
    configure: impl FnOnce(skilj::SkiljBuilder) -> skilj::SkiljBuilder,
) -> (Skilj, Pool, String, String, Role) {
    let database_url = test_database_url()
        .await
        .expect("test_database_url() must be Some - caller already checked");
    let jwks_url = serve_jwks().await;

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

    // Small per-instance pool: every test leaks a live `Skilj`, so the default
    // 10-connection pools of ~10 tests exhaust the shared embedded Postgres's
    // 100 connections and the last `build()` fails with `PoolTimedOut`.
    let builder = Skilj::builder(database_url.clone())
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
        .identity_provider(IdpConfig::new(
            jwks_url.parse().unwrap(),
            TEST_ISSUER,
            TEST_AUDIENCE,
            SigningAlgorithm::Rs256,
        ))
        .bounded_context(bc_name.clone())
        .event_type::<MoneyDeposited>()
        .command_type::<WithdrawMoney>()
        .command_type::<CloseAccount>()
        .event_type::<TicketNoteAdded>()
        .command_type::<AddTicketNote>()
        .event_type::<ThingHappened>()
        .snapshot::<ThingTotalSnapshot>()
        .command_type::<DoThingFast>()
        .event_type::<TicketOpened>()
        .snapshot::<TicketTotalSnapshot>()
        .command_type::<OpenTicket>()
        .reconciliation_role(admin_subject);
    let (skilj, report) = configure(builder).build().await.unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let jwt = sign_jwt(&role.external_subject);
    (skilj, pool, bc_name, jwt, role)
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
    mutation($bc: String!, $name: String!, $payload: String!) { \
        submitCommand(boundedContext: $bc, commandTypeName: $name, payload: $payload) { \
            accepted triggeredEventSequences rejectionReason rejectionKind \
        } \
    }";

#[test]
fn full_business_surfaces_lifecycle_end_to_end() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt, _admin_role) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        // submitCommand - accepted path.
        let response = graphql_request(
            &router,
            Some(&jwt),
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "name": "WithdrawMoney", "payload": r#"{"amount":20}"# }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);
        let sequences = response["data"]["submitCommand"]["triggeredEventSequences"]
            .as_array()
            .unwrap();
        assert_eq!(sequences.len(), 1);
        let first_sequence = sequences[0].as_i64().unwrap();

        // submitCommand - a second, later command, for pagination below.
        let response = graphql_request(
            &router,
            Some(&jwt),
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "name": "WithdrawMoney", "payload": r#"{"amount":30}"# }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let second_sequence =
            response["data"]["submitCommand"]["triggeredEventSequences"][0].as_i64().unwrap();

        // submitCommand - rejected path renders as 200-shaped typed data,
        // not a GraphQL error (§5.4/§7.3).
        let response = graphql_request(
            &router,
            Some(&jwt),
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "name": "WithdrawMoney", "payload": r#"{"amount":5000}"# }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["submitCommand"]["accepted"], false);
        assert_eq!(response["data"]["submitCommand"]["rejectionKind"], "insufficient_funds");

        // submitCommand against a #[requires_role("treasury_officer")]
        // command type - rejected: the admin Role's own name is "Admin",
        // not "treasury_officer".
        let response = graphql_request(
            &router,
            Some(&jwt),
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "name": "CloseAccount", "payload": "{}" }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "insufficient_role"
        );

        // The same command type succeeds for a Role actually named
        // "treasury_officer" - still needs its own admin grant.
        let officer_subject = unique_name("officer");
        let officer_role = Role {
            id: generate_token_id(),
            external_subject: officer_subject.clone(),
            name: "treasury_officer".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&_pool, &officer_role).await.unwrap();
        let officer_mapping = RoleAccessMapping {
            role: officer_role,
            bounded_context: skilj_core::db::get_bounded_context(&_pool, &bc_name)
                .await
                .unwrap()
                .unwrap(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role_access_mapping(&_pool, &officer_mapping)
            .await
            .unwrap();
        let officer_jwt = sign_jwt(&officer_subject);

        let response = graphql_request(
            &router,
            Some(&officer_jwt),
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "name": "CloseAccount", "payload": "{}" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);

        // queryEvents - real pagination via afterSequence.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!, $after: Int) { \
                queryEvents(boundedContext: $bc, eventTypes: [], afterSequence: $after) { \
                    sequence payload \
                } \
            }",
            json!({ "bc": bc_name, "after": first_sequence }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let events = response["data"]["queryEvents"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["sequence"], second_sequence);
        assert_eq!(events[0]["payload"], r#"{"amount":30}"#);

        // countEvents.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { countEvents(boundedContext: $bc, eventTypes: []) }",
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["countEvents"], 2);

        // inspectEvent.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!, $seq: Int!) { \
                inspectEvent(boundedContext: $bc, sequence: $seq) { \
                    renderedPayload event { origin { kind } } \
                } \
            }",
            json!({ "bc": bc_name, "seq": first_sequence }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(
            response["data"]["inspectEvent"]["renderedPayload"],
            r#"{"amount":20}"#
        );
        assert_eq!(
            response["data"]["inspectEvent"]["event"]["origin"]["kind"],
            "COMMAND_TRIGGERED"
        );

        // fetchCommands.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { fetchCommands(boundedContext: $bc, commandTypes: [\"WithdrawMoney\"]) { id payload } }",
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let commands = response["data"]["fetchCommands"].as_array().unwrap();
        assert_eq!(commands.len(), 2);

        // Codeberg issue #18 - submitCommand echoes back the correlationId
        // it actually stored (the caller's own, verbatim), and both
        // queryEvents and fetchCommands can be scoped to exactly that one
        // transaction via their own new correlationId argument.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "mutation($bc: String!, $name: String!, $payload: String!, $corr: String) { \
                submitCommand(boundedContext: $bc, commandTypeName: $name, payload: $payload, \
                    correlationId: $corr) { \
                    accepted triggeredEventSequences correlationId \
                } \
            }",
            json!({
                "bc": bc_name,
                "name": "WithdrawMoney",
                "payload": r#"{"amount":1}"#,
                "corr": "test-correlation-42",
            }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);
        assert_eq!(
            response["data"]["submitCommand"]["correlationId"],
            "test-correlation-42"
        );
        let correlated_sequence = response["data"]["submitCommand"]["triggeredEventSequences"][0]
            .as_i64()
            .unwrap();

        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!, $corr: String) { \
                queryEvents(boundedContext: $bc, eventTypes: [], correlationId: $corr) { \
                    sequence \
                } \
            }",
            json!({ "bc": bc_name, "corr": "test-correlation-42" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let correlated_events = response["data"]["queryEvents"].as_array().unwrap();
        assert_eq!(correlated_events.len(), 1);
        assert_eq!(correlated_events[0]["sequence"], correlated_sequence);

        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!, $corr: String) { \
                fetchCommands(boundedContext: $bc, commandTypes: [], correlationId: $corr) { payload } \
            }",
            json!({ "bc": bc_name, "corr": "test-correlation-42" }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let correlated_commands = response["data"]["fetchCommands"].as_array().unwrap();
        assert_eq!(
            correlated_commands,
            &vec![json!({ "payload": r#"{"amount":1}"# })]
        );

        // Gating: no caller at all is rejected for submitCommand, before
        // anything runs.
        let response = graphql_request(
            &router,
            None,
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "name": "WithdrawMoney", "payload": r#"{"amount":1}"# }),
        )
        .await;
        assert_eq!(response["errors"][0]["extensions"]["code"], "unauthenticated");
    });
}

const SUBMIT_COMMAND_WITH_IDEMPOTENCY_KEY_MUTATION: &str = "\
    mutation($bc: String!, $name: String!, $payload: String!, $key: String) { \
        submitCommand(boundedContext: $bc, commandTypeName: $name, payload: $payload, \
            idempotencyKey: $key) { \
            accepted triggeredEventSequences deduplicated \
        } \
    }";

/// Codeberg issue #12, real end-to-end over GraphQL: a genuine retry
/// with the same `idempotencyKey` returns the identical
/// `triggeredEventSequences` and `deduplicated: true` - and only one
/// event actually exists, not just that the response looks right.
#[test]
fn submit_command_deduplicates_a_repeated_idempotency_key_over_graphql() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, pool, bc_name, jwt, _admin_role) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        let variables = json!({
            "bc": bc_name,
            "name": "WithdrawMoney",
            "payload": r#"{"amount":20}"#,
            "key": "retry-1",
        });

        let first = graphql_request(
            &router,
            Some(&jwt),
            SUBMIT_COMMAND_WITH_IDEMPOTENCY_KEY_MUTATION,
            variables.clone(),
        )
        .await;
        assert!(
            first.get("errors").is_none(),
            "unexpected errors: {first:?}"
        );
        assert_eq!(first["data"]["submitCommand"]["accepted"], true);
        assert_eq!(first["data"]["submitCommand"]["deduplicated"], false);
        let first_sequences = first["data"]["submitCommand"]["triggeredEventSequences"].clone();

        let second = graphql_request(
            &router,
            Some(&jwt),
            SUBMIT_COMMAND_WITH_IDEMPOTENCY_KEY_MUTATION,
            variables,
        )
        .await;
        assert!(
            second.get("errors").is_none(),
            "unexpected errors: {second:?}"
        );
        assert_eq!(second["data"]["submitCommand"]["accepted"], true);
        assert_eq!(
            second["data"]["submitCommand"]["deduplicated"], true,
            "a repeated idempotencyKey must be reported as deduplicated"
        );
        assert_eq!(
            second["data"]["submitCommand"]["triggeredEventSequences"], first_sequences,
            "a dedup hit must return the *original* sequences, not a fresh decision"
        );

        let events = skilj_core::db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "a deduplicated retry must not double-apply the command"
        );
    });
}

/// Security-review finding on `CrossContextRoute` (docs/architecture.md
/// [§36](../../docs/architecture.md#cross-context-route)): a caller-supplied `idempotencyKey` using the reserved
/// `skilj-cross-context-route:` prefix must be rejected outright, not
/// silently accepted into the same shared `idempotency_keys` table
/// `CrossContextRoute`'s own background task writes into - see
/// `skilj_core::event_store::reject_reserved_idempotency_key`'s own doc
/// comment for why this matters.
#[test]
fn submit_command_rejects_a_reserved_idempotency_key_prefix_over_graphql() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, pool, bc_name, jwt, _admin_role) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        let variables = json!({
            "bc": bc_name,
            "name": "WithdrawMoney",
            "payload": r#"{"amount":20}"#,
            "key": "skilj-cross-context-route:some-other-route:42",
        });

        let response = graphql_request(
            &router,
            Some(&jwt),
            SUBMIT_COMMAND_WITH_IDEMPOTENCY_KEY_MUTATION,
            variables,
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "reserved_idempotency_key_prefix"
        );

        // Nothing was written under that key, or at all.
        let events = skilj_core::db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(events.len(), 0);
    });
}

/// A rejection's `matchingEvents` (Codeberg issue #7's DCB conflict
/// visualizer) is full raw event content - the same visibility
/// `queryEvents`/`countEvents`/`inspectEvent` require `Admin` level for.
/// `CommandSubmission` itself faces `WriteAccess` (any write-level grant
/// can submit), but a Write-level caller must not use a deliberately
/// forced rejection as a read side channel into event history they have
/// no query access to - `MatchingEventsRequiresAdminLevel` in
/// `specs/skilj.allium`.
#[test]
fn matching_events_is_only_returned_to_an_admin_level_caller() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, pool, bc_name, admin_jwt, _admin_role) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        const SUBMIT_COMMAND_WITH_MATCHING_EVENTS: &str = "\
            mutation($bc: String!, $name: String!, $payload: String!) { \
                submitCommand(boundedContext: $bc, commandTypeName: $name, payload: $payload) { \
                    accepted rejectionKind \
                    matchingEvents { sequence eventTypeName payload } \
                } \
            }";

        // Admin-level caller: a rejection returns matchingEvents - Some
        // even though this file's own WithdrawMoney fixture declares no
        // tags (so the set is empty), matching submit_command's own
        // "Some even when empty" contract (see command_submission.rs's
        // doc comment) - the point under test is presence vs absence of
        // the field by access level, not its contents.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            SUBMIT_COMMAND_WITH_MATCHING_EVENTS,
            json!({ "bc": bc_name, "name": "WithdrawMoney", "payload": r#"{"amount":5000}"# }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["submitCommand"]["accepted"], false);
        assert_eq!(response["data"]["submitCommand"]["rejectionKind"], "insufficient_funds");
        assert!(
            response["data"]["submitCommand"]["matchingEvents"].is_array(),
            "an Admin-level caller's rejection must carry matchingEvents (even if empty): {response:?}"
        );

        // Write-level caller, same bounded context: submission itself
        // still succeeds (this surface faces WriteAccess), but the
        // identical rejection's matchingEvents is withheld.
        let write_subject = unique_name("write-only");
        let write_role = Role {
            id: generate_token_id(),
            external_subject: write_subject.clone(),
            name: "WriteOnly".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &write_role).await.unwrap();
        let write_mapping = RoleAccessMapping {
            role: write_role,
            bounded_context: skilj_core::db::get_bounded_context(&pool, &bc_name)
                .await
                .unwrap()
                .unwrap(),
            level: AccessLevel::Write,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role_access_mapping(&pool, &write_mapping)
            .await
            .unwrap();
        let write_jwt = sign_jwt(&write_subject);

        let response = graphql_request(
            &router,
            Some(&write_jwt),
            SUBMIT_COMMAND_WITH_MATCHING_EVENTS,
            json!({ "bc": bc_name, "name": "WithdrawMoney", "payload": r#"{"amount":5000}"# }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["submitCommand"]["accepted"], false);
        assert_eq!(response["data"]["submitCommand"]["rejectionKind"], "insufficient_funds");
        assert!(
            response["data"]["submitCommand"]["matchingEvents"].is_null(),
            "a Write-level caller must not receive matchingEvents: {response:?}"
        );
    });
}

/// `inspectSnapshot` ([docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)) - Admin-gated, `null`
/// for a real, registered snapshot that's simply never had a row
/// written yet (cold, not an error), and a real error for a
/// `snapshotName` that isn't registered at all.
/// `skilj-demo/tests/snapshot.rs` already proves `decide_from_snapshot`
/// itself end to end against real, tampered data - this test's own job
/// is the inspection endpoint alone.
#[test]
fn inspect_snapshot_is_admin_gated_null_when_cold_and_a_real_error_when_unregistered() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt, _admin_role) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        const INSPECT_SNAPSHOT: &str = "\
            query($bc: String!, $name: String!, $tagValue: String!) { \
                inspectSnapshot(boundedContext: $bc, snapshotName: $name, tagValue: $tagValue) { \
                    tagKey tagValue version asOfSequence state \
                } \
            }";

        let thing_id = unique_name("thing");

        // Cold: a real, registered snapshot, but nothing has ever been
        // written for this tag value.
        let response = graphql_request(
            &router,
            Some(&jwt),
            INSPECT_SNAPSHOT,
            json!({ "bc": bc_name, "name": "ThingTotalSnapshot", "tagValue": thing_id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert!(
            response["data"]["inspectSnapshot"].is_null(),
            "a real snapshot with nothing written yet must be null, not an error: {response:?}"
        );

        // Trigger a real DoThingFast, then force the background catch-up
        // tick a real deployment's own poll interval would eventually
        // run, so a real row exists to inspect.
        let response = graphql_request(
            &router,
            Some(&jwt),
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "name": "DoThingFast", "payload": format!(r#"{{"thing_id":"{thing_id}","amount":7}}"#) }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);

        skilj_core::db::catch_up_snapshots(&_pool, &bc_name, skilj.snapshot_dispatcher().as_ref())
            .await
            .unwrap();

        let response = graphql_request(
            &router,
            Some(&jwt),
            INSPECT_SNAPSHOT,
            json!({ "bc": bc_name, "name": "ThingTotalSnapshot", "tagValue": thing_id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["inspectSnapshot"]["tagKey"], "thing");
        assert_eq!(response["data"]["inspectSnapshot"]["tagValue"], thing_id);
        assert_eq!(response["data"]["inspectSnapshot"]["version"], 1);
        assert!(response["data"]["inspectSnapshot"]["asOfSequence"].as_i64().unwrap() >= 0);
        let state: serde_json::Value =
            serde_json::from_str(response["data"]["inspectSnapshot"]["state"].as_str().unwrap())
                .unwrap();
        assert_eq!(state["total"], 7);

        // An unregistered snapshot name is a real error, not null - the
        // caller named something that doesn't exist, distinguishable
        // from a real one that's simply cold.
        let response = graphql_request(
            &router,
            Some(&jwt),
            INSPECT_SNAPSHOT,
            json!({ "bc": bc_name, "name": "NoSuchSnapshot", "tagValue": thing_id }),
        )
        .await;
        assert_eq!(response["errors"][0]["extensions"]["code"], "Snapshot_not_found");

        // Admin-gated: a Write-level caller (mirroring the officer
        // fixture pattern already used above in this file) is rejected
        // before ever reaching the snapshot table.
        let write_subject = unique_name("write-only");
        let write_role = Role {
            id: generate_token_id(),
            external_subject: write_subject.clone(),
            name: "WriteOnly".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&_pool, &write_role).await.unwrap();
        let write_mapping = RoleAccessMapping {
            role: write_role,
            bounded_context: skilj_core::db::get_bounded_context(&_pool, &bc_name)
                .await
                .unwrap()
                .unwrap(),
            level: AccessLevel::Write,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role_access_mapping(&_pool, &write_mapping)
            .await
            .unwrap();
        let write_jwt = sign_jwt(&write_subject);

        let response = graphql_request(
            &router,
            Some(&write_jwt),
            INSPECT_SNAPSHOT,
            json!({ "bc": bc_name, "name": "ThingTotalSnapshot", "tagValue": thing_id }),
        )
        .await;
        assert_eq!(response["errors"][0]["extensions"]["code"], "grant_not_active");
    });
}

/// `inspectSnapshot`'s own owner-tag scoping (cross-tenant read fix,
/// docs/architecture.md's own write-up of these passes) - the real
/// vulnerability this whole pass fixes, end to end: before
/// `RoleAccessMapping.scope`/`Snapshot::OWNER_TAG_KEY` existed, any
/// active admin-level grant on this bounded context - `company-b`'s own
/// included - could inspect `company-a`'s own ticket snapshot by
/// `tagValue` alone. Uses `TicketTotalSnapshot` (`TAG_KEY: "ticket"`,
/// `OWNER_TAG_KEY: Some("company")`) - a distinct owner tag from the
/// snapshot's own keying tag, proving the derivation reads the *right*
/// one, not just `TAG_KEY` by coincidence.
#[test]
fn inspect_snapshot_scopes_by_owner_tag_and_fails_closed_on_an_unproven_one() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, pool, bc_name, admin_jwt, _admin_role) = setup().await;
        let router = skilj.graphql_router().await.unwrap();
        let bc = skilj_core::db::get_bounded_context(&pool, &bc_name)
            .await
            .unwrap()
            .unwrap();

        const INSPECT_SNAPSHOT: &str = "\
            query($bc: String!, $name: String!, $tagValue: String!) { \
                inspectSnapshot(boundedContext: $bc, snapshotName: $name, tagValue: $tagValue) { \
                    state \
                } \
            }";

        let ticket_id = unique_name("ticket");

        // A real TicketOpened for company-a, then force the background
        // catch-up tick, so a real row exists to inspect.
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "name": "OpenTicket", "payload": format!(
                r#"{{"ticket_id":"{ticket_id}","company_id":"company-a","amount":100}}"#
            ) }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);

        skilj_core::db::catch_up_snapshots(&pool, &bc_name, skilj.snapshot_dispatcher().as_ref())
            .await
            .unwrap();

        // Helper: a fresh Role with its own admin-level RoleAccessMapping
        // at the given `scope`, mirroring this file's own officer/write-only
        // fixture pattern.
        async fn scoped_admin_jwt(pool: &Pool, bc: &BoundedContext, scope: Option<&str>) -> String {
            let subject = unique_name("scoped-admin");
            let role = Role {
                id: generate_token_id(),
                external_subject: subject.clone(),
                name: "ScopedAdmin".to_string(),
                superadmin: false,
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            };
            skilj_core::db::insert_role(pool, &role).await.unwrap();
            let mapping = RoleAccessMapping {
                role,
                bounded_context: bc.clone(),
                level: AccessLevel::Admin,
                can_read_sensitive: false,
                scope: scope.map(str::to_string),
                status: RoleStatus::Active,
                created_at: test_now(),
                revoked_at: None,
            };
            skilj_core::db::insert_role_access_mapping(pool, &mapping)
                .await
                .unwrap();
            sign_jwt(&subject)
        }

        // Its own company: allowed, real state.
        let company_a_jwt = scoped_admin_jwt(&pool, &bc, Some("company-a")).await;
        let response = graphql_request(
            &router,
            Some(&company_a_jwt),
            INSPECT_SNAPSHOT,
            json!({ "bc": bc_name, "name": "TicketTotalSnapshot", "tagValue": ticket_id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let state: serde_json::Value =
            serde_json::from_str(response["data"]["inspectSnapshot"]["state"].as_str().unwrap())
                .unwrap();
        assert_eq!(state["total"], 100);

        // Company B's ticket: rejected - the concrete cross-tenant read
        // this pass closes.
        let company_b_jwt = scoped_admin_jwt(&pool, &bc, Some("company-b")).await;
        let response = graphql_request(
            &router,
            Some(&company_b_jwt),
            INSPECT_SNAPSHOT,
            json!({ "bc": bc_name, "name": "TicketTotalSnapshot", "tagValue": ticket_id }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "grant_scope_mismatch"
        );

        // A never-touched tag_value: fail-closed the same way, not the
        // ordinary "cold" null - a scoped caller can't tell the two
        // apart (see SnapshotInspection's own GrantScopedToOwnerWhenDeclared).
        let response = graphql_request(
            &router,
            Some(&company_a_jwt),
            INSPECT_SNAPSHOT,
            json!({ "bc": bc_name, "name": "TicketTotalSnapshot", "tagValue": unique_name("never-touched") }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "grant_scope_mismatch"
        );

        // An unscoped admin grant is unrestricted, deliberately - the
        // original admin fixture from setup().
        let response = graphql_request(
            &router,
            Some(&admin_jwt),
            INSPECT_SNAPSHOT,
            json!({ "bc": bc_name, "name": "TicketTotalSnapshot", "tagValue": ticket_id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let state: serde_json::Value =
            serde_json::from_str(response["data"]["inspectSnapshot"]["state"].as_str().unwrap())
                .unwrap();
        assert_eq!(state["total"], 100);
    });
}

/// The private-field mechanism's own real end-to-end proof
/// (docs/architecture.md's own write-up of this pass): an `own`-kind
/// field defaults to visible only to its creator; a colleague sees it
/// only after a real `grantPrivateFieldAccessForEvent`, loses it again
/// after `revokePrivateFieldAccess`, and `listPrivateFieldGrants` reads
/// both states back.
#[test]
fn private_field_grant_lifecycle_end_to_end() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, pool, bc_name, creator_jwt, creator_role) = setup().await;
        let router = skilj.graphql_router().await.unwrap();
        let bc = skilj_core::db::get_bounded_context(&pool, &bc_name)
            .await
            .unwrap()
            .unwrap();

        // A second, ordinary Role on the same bounded context - the
        // colleague this test shares (and stops sharing) the note with.
        let colleague_subject = unique_name("colleague");
        let colleague_role = Role {
            id: generate_token_id(),
            external_subject: colleague_subject.clone(),
            name: "Colleague".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role(&pool, &colleague_role)
            .await
            .unwrap();
        let colleague_mapping = RoleAccessMapping {
            role: colleague_role.clone(),
            bounded_context: bc,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        skilj_core::db::insert_role_access_mapping(&pool, &colleague_mapping)
            .await
            .unwrap();
        let colleague_jwt = sign_jwt(&colleague_subject);

        const INSPECT_EVENT: &str = "\
            query($bc: String!, $seq: Int!) { \
                inspectEvent(boundedContext: $bc, sequence: $seq) { renderedPayload } \
            }";

        // The creator submits the command that creates the note.
        let response = graphql_request(
            &router,
            Some(&creator_jwt),
            SUBMIT_COMMAND_MUTATION,
            json!({ "bc": bc_name, "name": "AddTicketNote", "payload": r#"{"note":"call back Monday"}"# }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);
        let sequence = response["data"]["submitCommand"]["triggeredEventSequences"][0]
            .as_i64()
            .unwrap();

        // The creator reads its own note back in full.
        let response = graphql_request(
            &router,
            Some(&creator_jwt),
            INSPECT_EVENT,
            json!({ "bc": bc_name, "seq": sequence }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(
            response["data"]["inspectEvent"]["renderedPayload"],
            r#"{"note":"call back Monday"}"#
        );

        // The colleague, with no grant yet, gets the record with the
        // private leaf redacted to null - not withheld, not an error.
        let response = graphql_request(
            &router,
            Some(&colleague_jwt),
            INSPECT_EVENT,
            json!({ "bc": bc_name, "seq": sequence }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(
            response["data"]["inspectEvent"]["renderedPayload"],
            r#"{"note":null}"#
        );

        // The creator shares this one record with the colleague.
        const GRANT_FOR_EVENT: &str = "\
            mutation($bc: String!, $grantee: ID!, $seq: Int!) { \
                grantPrivateFieldAccessForEvent(boundedContext: $bc, granteeRoleId: $grantee, eventSequence: $seq) { \
                    id status grantee { id } \
                } \
            }";
        let response = graphql_request(
            &router,
            Some(&creator_jwt),
            GRANT_FOR_EVENT,
            json!({ "bc": bc_name, "grantee": colleague_role.id, "seq": sequence }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(
            response["data"]["grantPrivateFieldAccessForEvent"]["status"],
            "ACTIVE"
        );
        assert_eq!(
            response["data"]["grantPrivateFieldAccessForEvent"]["grantee"]["id"],
            colleague_role.id
        );
        let grant_id = response["data"]["grantPrivateFieldAccessForEvent"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        // The colleague now reads the note in full.
        let response = graphql_request(
            &router,
            Some(&colleague_jwt),
            INSPECT_EVENT,
            json!({ "bc": bc_name, "seq": sequence }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(
            response["data"]["inspectEvent"]["renderedPayload"],
            r#"{"note":"call back Monday"}"#
        );

        // The creator lists its own outgoing grants and finds it, active.
        const LIST_GRANTS: &str = "\
            query($bc: String!) { \
                listPrivateFieldGrants(boundedContext: $bc) { id status grantee { id } } \
            }";
        let response = graphql_request(
            &router,
            Some(&creator_jwt),
            LIST_GRANTS,
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let grants = response["data"]["listPrivateFieldGrants"].as_array().unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0]["id"], grant_id);
        assert_eq!(grants[0]["status"], "ACTIVE");

        // The creator revokes it.
        const REVOKE_GRANT: &str = "\
            mutation($bc: String!, $id: ID!) { \
                revokePrivateFieldAccess(boundedContext: $bc, grantId: $id) { status } \
            }";
        let response = graphql_request(
            &router,
            Some(&creator_jwt),
            REVOKE_GRANT,
            json!({ "bc": bc_name, "id": grant_id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(
            response["data"]["revokePrivateFieldAccess"]["status"],
            "REVOKED"
        );

        // The colleague is redacted again - the same fail-closed answer
        // as before any grant ever existed.
        let response = graphql_request(
            &router,
            Some(&colleague_jwt),
            INSPECT_EVENT,
            json!({ "bc": bc_name, "seq": sequence }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(
            response["data"]["inspectEvent"]["renderedPayload"],
            r#"{"note":null}"#
        );

        // A colleague with no grant of their own may not revoke - a
        // grantor-only action - nor may they list someone else's grants
        // without admin level, even though this colleague happens to
        // hold Admin here: naming a different grantor still requires it,
        // and this colleague is not that grantor's own admin.
        let response = graphql_request(
            &router,
            Some(&colleague_jwt),
            LIST_GRANTS,
            json!({ "bc": bc_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        // The colleague's own outgoing grants: none made.
        assert_eq!(
            response["data"]["listPrivateFieldGrants"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        // Naming the creator as grantor from the colleague's own
        // (admin-level) mapping succeeds - any admin may audit any
        // grantor in its own bounded context.
        const LIST_GRANTS_FOR: &str = "\
            query($bc: String!, $grantor: ID!) { \
                listPrivateFieldGrants(boundedContext: $bc, grantorRoleId: $grantor) { id status } \
            }";
        let response = graphql_request(
            &router,
            Some(&colleague_jwt),
            LIST_GRANTS_FOR,
            json!({ "bc": bc_name, "grantor": creator_role.id }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        let grants = response["data"]["listPrivateFieldGrants"].as_array().unwrap();
        assert_eq!(grants.len(), 1);
        assert_eq!(grants[0]["status"], "REVOKED");
    });
}

/// `queryEvents` serves at most `max_events_per_read` events (3 here),
/// oldest first, and a caller pages on with `afterSequence` = the last
/// one's `sequence`. With a two-event cache window, the early pages come
/// from the bounded-context-wide Postgres read, `LIMIT`ed per chunk.
#[test]
fn query_events_serves_bounded_pages_over_graphql() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt, _role) = setup_with(|builder| {
            builder
                .max_events_per_read(3)
                .event_cache_warm_up_count(2)
        })
        .await;
        let router = skilj.graphql_router().await.unwrap();
        for amount in 1..=7 {
            let response = graphql_request(
                &router,
                Some(&jwt),
                SUBMIT_COMMAND_MUTATION,
                json!({
                    "bc": bc_name,
                    "name": "WithdrawMoney",
                    "payload": format!(r#"{{"amount":{amount}}}"#),
                }),
            )
            .await;
            assert_eq!(response["data"]["submitCommand"]["accepted"], true, "{response:?}");
        }

        let mut after: Option<i64> = None;
        let mut pages = Vec::new();
        loop {
            let response = graphql_request(
                &router,
                Some(&jwt),
                "query($bc: String!, $after: Int) { \
                    queryEvents(boundedContext: $bc, eventTypes: [\"MoneyDeposited\"], afterSequence: $after) \
                    { sequence payload } }",
                json!({ "bc": bc_name, "after": after }),
            )
            .await;
            assert!(response.get("errors").is_none(), "{response:?}");
            let events = response["data"]["queryEvents"].as_array().unwrap().clone();
            if events.is_empty() {
                break;
            }
            after = events.last().unwrap()["sequence"].as_i64();
            pages.push(
                events
                    .iter()
                    .map(|e| {
                        serde_json::from_str::<serde_json::Value>(e["payload"].as_str().unwrap())
                            .unwrap()["amount"]
                            .as_i64()
                            .unwrap()
                    })
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(pages, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);

        // countEvents stays a total over everything - summed across the
        // same three chunks, not capped at one.
        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { countEvents(boundedContext: $bc, eventTypes: [\"MoneyDeposited\"]) }",
            json!({ "bc": bc_name }),
        )
        .await;
        assert_eq!(response["data"]["countEvents"], 7, "{response:?}");
    });
}

/// docs/architecture.md §108: a tag-filtered `queryEvents` pages the same
/// way, reading through the tag index a chunk at a time, and a
/// tag-filtered `countEvents` sums over every chunk. Another tag's events,
/// interleaved, are never served or counted.
#[test]
fn tagged_query_events_serves_bounded_pages_over_graphql() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt, _role) =
            setup_with(|builder| builder.max_events_per_read(3)).await;
        let router = skilj.graphql_router().await.unwrap();
        for amount in 1..=7 {
            for thing in ["t1", "t2"] {
                let response = graphql_request(
                    &router,
                    Some(&jwt),
                    SUBMIT_COMMAND_MUTATION,
                    json!({
                        "bc": bc_name,
                        "name": "DoThingFast",
                        "payload": format!(r#"{{"thing_id":"{thing}","amount":{amount}}}"#),
                    }),
                )
                .await;
                assert_eq!(response["data"]["submitCommand"]["accepted"], true, "{response:?}");
            }
        }

        let mut after: Option<i64> = None;
        let mut pages = Vec::new();
        loop {
            let response = graphql_request(
                &router,
                Some(&jwt),
                "query($bc: String!, $after: Int) { \
                    queryEvents(boundedContext: $bc, eventTypes: [\"ThingHappened\"], \
                    tags: [{ key: \"thing\", value: \"t1\" }], afterSequence: $after) \
                    { sequence payload } }",
                json!({ "bc": bc_name, "after": after }),
            )
            .await;
            assert!(response.get("errors").is_none(), "{response:?}");
            let events = response["data"]["queryEvents"].as_array().unwrap().clone();
            if events.is_empty() {
                break;
            }
            after = events.last().unwrap()["sequence"].as_i64();
            pages.push(
                events
                    .iter()
                    .map(|e| {
                        let payload: serde_json::Value =
                            serde_json::from_str(e["payload"].as_str().unwrap()).unwrap();
                        assert_eq!(payload["thing_id"], "t1");
                        payload["amount"].as_i64().unwrap()
                    })
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(pages, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);

        let response = graphql_request(
            &router,
            Some(&jwt),
            "query($bc: String!) { countEvents(boundedContext: $bc, eventTypes: [\"ThingHappened\"], \
                tags: [{ key: \"thing\", value: \"t1\" }]) }",
            json!({ "bc": bc_name }),
        )
        .await;
        assert_eq!(response["data"]["countEvents"], 7, "{response:?}");
    });
}

/// `fetchCommands` serves at most `max_events_per_read` commands (3
/// here) in the order they were recorded, and a caller pages on with
/// `afterCommandId` = the last one's `id`. An unknown `afterCommandId` is
/// `Command_not_found`, not a silent restart from the beginning.
#[test]
fn fetch_commands_serves_bounded_pages_over_graphql() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt, _role) =
            setup_with(|builder| builder.max_events_per_read(3)).await;
        let router = skilj.graphql_router().await.unwrap();
        for amount in 1..=7 {
            let response = graphql_request(
                &router,
                Some(&jwt),
                SUBMIT_COMMAND_MUTATION,
                json!({
                    "bc": bc_name,
                    "name": "WithdrawMoney",
                    "payload": format!(r#"{{"amount":{amount}}}"#),
                }),
            )
            .await;
            assert_eq!(response["data"]["submitCommand"]["accepted"], true, "{response:?}");
        }

        let query = "query($bc: String!, $after: String) { \
            fetchCommands(boundedContext: $bc, commandTypes: [\"WithdrawMoney\"], afterCommandId: $after) \
            { id createdAt payload } }";
        let mut after: Option<String> = None;
        let mut pages = Vec::new();
        loop {
            let response =
                graphql_request(&router, Some(&jwt), query, json!({ "bc": bc_name, "after": after }))
                    .await;
            assert!(response.get("errors").is_none(), "{response:?}");
            let commands = response["data"]["fetchCommands"].as_array().unwrap().clone();
            if commands.is_empty() {
                break;
            }
            assert!(commands.iter().all(|c| c["createdAt"].is_string()));
            after = commands.last().unwrap()["id"].as_str().map(str::to_string);
            pages.push(
                commands
                    .iter()
                    .map(|c| {
                        serde_json::from_str::<serde_json::Value>(c["payload"].as_str().unwrap())
                            .unwrap()["amount"]
                            .as_i64()
                            .unwrap()
                    })
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(pages, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);

        let response = graphql_request(
            &router,
            Some(&jwt),
            query,
            json!({ "bc": bc_name, "after": "no-such-command" }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"], "Command_not_found",
            "{response:?}"
        );
    });
}

/// The standard full introspection query (graphql-js `getIntrospectionQuery`),
/// what GraphiQL and codegen tools send - must stay within every limit.
const FULL_INTROSPECTION_QUERY: &str = r#"
query IntrospectionQuery {
  __schema {
    queryType { name } mutationType { name } subscriptionType { name }
    types { ...FullType }
    directives { name description locations args { ...InputValue } }
  }
}
fragment FullType on __Type {
  kind name description
  fields(includeDeprecated: true) {
    name description args { ...InputValue } type { ...TypeRef } isDeprecated deprecationReason
  }
  inputFields { ...InputValue }
  interfaces { ...TypeRef }
  enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason }
  possibleTypes { ...TypeRef }
}
fragment InputValue on __InputValue { name description type { ...TypeRef } defaultValue }
fragment TypeRef on __Type {
  kind name
  ofType { kind name ofType { kind name ofType { kind name ofType { kind name
    ofType { kind name ofType { kind name ofType { kind name ofType { kind name } } } } } } } }
}
"#;

/// `/graphql` is bounded before anything else runs: an oversized body is
/// refused with 413 even unauthenticated (the body used to be read whole,
/// with no cap, before the handler's auth check); a query aliasing an
/// expensive list field many times is refused (each alias would otherwise
/// get its own full page, multiplying `max_events_per_read`); and so is a
/// query nested absurdly deep. The standard introspection query still
/// passes all of it.
#[test]
fn graphql_requests_are_bounded_in_size_depth_and_expensive_fields() {
    runtime().block_on(async {
        if test_database_url().await.is_none() {
            return;
        }
        let (skilj, _pool, bc_name, jwt, _role) = setup().await;
        let router = skilj.graphql_router().await.unwrap();

        let huge = json!({
            "query": "{ __typename }",
            "variables": { "padding": "x".repeat(3 * 1024 * 1024) },
        })
        .to_string();
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/graphql")
                    .header("content-type", "application/json")
                    .body(Body::from(huge))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let aliases: String = (0..20)
            .map(|i| format!(r#"c{i}: countEvents(boundedContext: $bc, eventTypes: []) "#))
            .collect();
        let response = graphql_request(
            &router,
            Some(&jwt),
            &format!("query($bc: String!) {{ {aliases} }}"),
            json!({ "bc": bc_name }),
        )
        .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"], "query_too_expensive",
            "{response:?}"
        );

        let deep = format!(
            "{{ __schema {{ types {{ fields {{ type {}name{} }} }} }} }}",
            "{ ofType ".repeat(30),
            " }".repeat(30)
        );
        let response = graphql_request(&router, Some(&jwt), &deep, json!({})).await;
        assert!(response.get("errors").is_some(), "{response:?}");

        let response =
            graphql_request(&router, Some(&jwt), FULL_INTROSPECTION_QUERY, json!({})).await;
        assert!(response.get("errors").is_none(), "{response:?}");
        assert!(
            response["data"]["__schema"]["types"]
                .as_array()
                .unwrap()
                .len()
                > 10
        );
    });
}
