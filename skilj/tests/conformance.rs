//! Golden-transcript conformance suite across GraphQL and REST (Codeberg
//! issue #43, docs/architecture.md §179).
//!
//! Every scenario below is one logical operation - submit a command, read
//! a type's events - run once over GraphQL (a Role's JWT, `submitCommand`/
//! `queryEvents`) and once over REST (an admin-minted `CommandToken`/
//! `EventReadToken`, `POST /v1/commands/trigger`/`GET /v1/events`), each
//! surface against its own fresh bounded context with the identical
//! history, so sequences line up. Each run prints one line of observable
//! outcome: `scenario`, surface, HTTP status, and the outcome as canonical
//! JSON - the fields both surfaces answer that operation with, or the
//! error `code`. Never raw bytes, timing, messages, or generated ids.
//!
//! Two checks:
//!
//! - the lines are compared, with exact equality, against the checked-in
//!   `fixtures/conformance.transcript`. A change in what either surface
//!   answers fails here until it is re-recorded on purpose:
//!   `SKILJ_RECORD_CONFORMANCE=1 cargo test -p skilj --test conformance`.
//!   The transcript's first line pins the skilj version it was recorded
//!   against; a different version fails with its own message rather
//!   than as a wall of false diffs.
//! - for each scenario, the GraphQL outcome must equal the REST outcome,
//!   unless the scenario is in [`DIVERGENCES`] with its reason. An entry
//!   whose outcomes agree after all fails too, so the catalogue can't go
//!   stale. Nothing is normalised to make the two agree: a difference is
//!   either fixed or catalogued.
//!
//! The HTTP status is recorded but not compared - GraphQL answers 200
//! with an `errors` array where REST maps the same `code` to a 4xx
//! (docs/architecture.md §7.5). Fields only one surface has (REST's
//! `nextCursor`/`epoch`/per-event `metadata`, GraphQL's Admin-only
//! `matchingEvents`) aren't part of an operation's outcome; they're
//! listed in the catalogue's module-level notes in §179.
//!
//! Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness and JWKS
//! test server as `graphql_business_surfaces.rs`, duplicated rather than
//! shared, like every other test binary here.

use axum::body::Body;
use axum::http::Request;
use chrono::SubsecRound;
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use skilj::{CommandType, EventType, IdpConfig, SigningAlgorithm, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::Pool;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, generate_token_secret, CommandDecision, EventSpec};
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

// --- the scenarios ---

/// The checked-in transcript, relative to this crate.
const TRANSCRIPT_PATH: &str = "tests/fixtures/conformance.transcript";

/// Re-records the transcript instead of comparing against it.
const RECORD_ENV: &str = "SKILJ_RECORD_CONFORMANCE";

/// A caller-supplied correlation id, so an accepted command's echoed one
/// is the same on every run.
const CORRELATION_ID: &str = "conformance";

/// Scenarios whose GraphQL and REST outcomes deliberately differ, each
/// with the reason. Every other scenario must agree.
const DIVERGENCES: &[(&str, &str)] = &[
    (
        "submit_without_credential",
        "the tracks authenticate differently: GraphQL resolves an optional JWT and refuses \
         in the resolver (`unauthenticated`), REST refuses a missing bearer credential \
         before any route runs (`missing_credential`, 401) - §7.5",
    ),
    (
        "read_without_credential",
        "the same split as submit_without_credential",
    ),
    (
        "read_after_past_latest",
        "`GET /v1/events`' `after` is a stream cursor and a cursor past the latest committed \
         sequence is refused (§148); `queryEvents`' `afterSequence` is an Admin's paging \
         argument with no cursor or epoch contract, and past the end it is an empty page",
    ),
];

/// One logical operation, run on either surface.
#[derive(Clone)]
enum Op {
    Submit {
        payload: Value,
        idempotency_key: Option<&'static str>,
    },
    SubmitWithoutCredential {
        payload: Value,
    },
    Read {
        after: Option<i64>,
    },
    ReadWithoutCredential,
    /// Archives the surface's bounded context - setup for the scenarios
    /// after it, with no outcome line of its own.
    Archive,
}

fn scenarios() -> Vec<(&'static str, Op)> {
    let submit = |amount: Value, idempotency_key| Op::Submit {
        payload: json!({ "amount": amount }),
        idempotency_key,
    };
    vec![
        ("submit_accepted", submit(json!(20), None)),
        ("submit_rejected", submit(json!(5000), None)),
        (
            "submit_with_idempotency_key",
            submit(json!(30), Some("k-1")),
        ),
        ("submit_idempotent_retry", submit(json!(30), Some("k-1"))),
        (
            "submit_idempotency_key_reused_for_other_payload",
            submit(json!(40), Some("k-1")),
        ),
        (
            "submit_reserved_idempotency_key",
            submit(json!(10), Some("skilj-cross-context-route:r:1")),
        ),
        ("submit_payload_off_schema", submit(json!("lots"), None)),
        (
            "submit_payload_missing_field",
            Op::Submit {
                payload: json!({}),
                idempotency_key: None,
            },
        ),
        (
            "submit_without_credential",
            Op::SubmitWithoutCredential {
                payload: json!({ "amount": 1 }),
            },
        ),
        ("read_all", Op::Read { after: None }),
        ("read_after_first", Op::Read { after: Some(0) }),
        ("read_after_latest", Op::Read { after: Some(1) }),
        ("read_after_past_latest", Op::Read { after: Some(1000) }),
        ("read_without_credential", Op::ReadWithoutCredential),
        ("archive", Op::Archive),
        ("submit_archived", submit(json!(20), None)),
        ("read_archived", Op::Read { after: None }),
    ]
}

// --- fixtures ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct MoneyDepositedPayload {
    amount: i64,
}

struct MoneyDeposited;

impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
    fn event_read_allowed() -> bool {
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
struct DepositPayload {
    amount: i64,
}

struct DepositMoney;

impl CommandType for DepositMoney {
    type Payload = DepositPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "DepositMoney";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        if payload.amount > 1000 {
            CommandDecision::Rejected {
                reason: "over the deposit limit".to_string(),
                kind: "over_limit".to_string(),
            }
        } else {
            CommandDecision::Accepted {
                events: vec![EventSpec {
                    event_type: "MoneyDeposited".to_string(),
                    payload: json!({ "amount": payload.amount }),
                }],
            }
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
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for conformance")
    })
}

async fn test_database_url() -> Option<String> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.database_url.clone())
}

async fn provision() -> Option<TestDb> {
    let url = skilj_test_support::database_url("skilj_conformance_test").await?;
    let pool = match skilj_core::db::connect(&url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to the test database failed: {e}");
            return None;
        }
    };
    if let Err(e) = skilj_core::db::migrate(&pool).await {
        eprintln!("skipping: migrating the test database failed: {e}");
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

// --- the two surfaces ---

/// A reconciled `Skilj` on a fresh bounded context, with an Admin Role,
/// that Role's JWT, and REST credentials minted from its grant.
struct Deployment {
    // Kept alive for its background tasks while the routers are used.
    _skilj: Skilj,
    pool: Pool,
    bounded_context: String,
    graphql: axum::Router,
    rest: axum::Router,
    jwt: String,
    command_credential: String,
    read_credential: String,
}

async fn deploy(database_url: &str, jwks_url: &str) -> Deployment {
    let pool = skilj_core::db::connect(database_url).await.unwrap();

    let subject = unique_name("admin");
    let role = Role {
        id: generate_token_id(),
        external_subject: subject.clone(),
        name: "Admin".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    skilj_core::db::insert_role(&pool, &role).await.unwrap();

    let bounded_context = unique_name("banking");
    let bc = BoundedContext {
        name: bounded_context.clone(),
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
        bounded_context: bc,
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

    let (skilj, report) = Skilj::builder(database_url.to_string())
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
        .identity_provider(IdpConfig::new(
            jwks_url.parse().unwrap(),
            TEST_ISSUER,
            TEST_AUDIENCE,
            SigningAlgorithm::Rs256,
        ))
        .bounded_context(bounded_context.clone())
        .event_type::<MoneyDeposited>()
        .command_type::<DepositMoney>()
        .reconciliation_role(subject.clone())
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let command_type = skilj_core::db::get_command_type(&pool, &bounded_context, "DepositMoney")
        .await
        .unwrap()
        .unwrap();
    let command_token = access_control::create_command_token(
        &mapping,
        &command_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        test_now(),
    )
    .unwrap();
    skilj_core::db::insert_command_token(&pool, &command_token)
        .await
        .unwrap();

    let event_type = skilj_core::db::get_event_type(&pool, &bounded_context, "MoneyDeposited")
        .await
        .unwrap()
        .unwrap();
    let read_token = access_control::create_event_read_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        None,
        None,
        None,
        test_now(),
    )
    .unwrap();
    skilj_core::db::insert_event_read_token(&pool, &read_token)
        .await
        .unwrap();

    Deployment {
        graphql: skilj.graphql_router().await.unwrap(),
        rest: skilj.rest_router(),
        _skilj: skilj,
        pool,
        bounded_context,
        jwt: sign_jwt(&subject),
        command_credential: format!("{}.{}", command_token.id, command_token.secret),
        read_credential: format!("{}.{}", read_token.id, read_token.secret),
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Surface {
    Graphql,
    Rest,
}

impl Surface {
    fn name(self) -> &'static str {
        match self {
            Surface::Graphql => "graphql",
            Surface::Rest => "rest",
        }
    }
}

/// Sends one request, returning the status and the JSON body.
async fn send(
    router: &axum::Router,
    method: &str,
    uri: &str,
    headers: &[(&str, String)],
    body: Option<Value>,
) -> (u16, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, value);
    }
    let request = match body {
        Some(body) => request
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())),
        None => request.body(Body::empty()),
    }
    .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        panic!(
            "{method} {uri} answered {status} with a body that isn't JSON: {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, body)
}

const SUBMIT_COMMAND: &str = "\
    mutation($bc: String!, $payload: String!, $key: String, $correlationId: String) { \
        submitCommand(boundedContext: $bc, commandTypeName: \"DepositMoney\", payload: $payload, \
                      idempotencyKey: $key, correlationId: $correlationId) { \
            accepted triggeredEventSequences rejectionReason rejectionKind deduplicated \
            correlationId \
        } \
    }";

const QUERY_EVENTS: &str = "\
    query($bc: String!, $after: Int) { \
        queryEvents(boundedContext: $bc, eventTypes: [\"MoneyDeposited\"], afterSequence: $after) { \
            sequence payload \
        } \
    }";

/// Runs `op` on `surface`, returning its status and outcome - `None` for
/// an op with no outcome line of its own.
async fn run(deployment: &Deployment, surface: Surface, op: &Op) -> Option<(u16, Value)> {
    let bearer = |credential: &str| ("authorization", format!("Bearer {credential}"));
    let graphql = |query: &'static str, variables: Value, authenticated: bool| {
        let headers = if authenticated {
            vec![bearer(&deployment.jwt)]
        } else {
            vec![]
        };
        async move {
            send(
                &deployment.graphql,
                "POST",
                "/graphql",
                &headers,
                Some(json!({ "query": query, "variables": variables })),
            )
            .await
        }
    };
    let submit_variables = |payload: &Value, key: Option<&str>| {
        json!({
            "bc": deployment.bounded_context,
            "payload": payload.to_string(),
            "key": key,
            "correlationId": CORRELATION_ID,
        })
    };
    let submit_body =
        |payload: &Value| json!({ "payload": payload, "correlationId": CORRELATION_ID });
    let read_variables =
        |after: Option<i64>| json!({ "bc": deployment.bounded_context, "after": after });
    let read_uri = |after: Option<i64>| match after {
        Some(after) => format!("/v1/events?after={after}"),
        None => "/v1/events".to_string(),
    };

    let (status, body) = match (surface, op) {
        (_, Op::Archive) => {
            skilj_core::db::update_bounded_context_status(
                &deployment.pool,
                &deployment.bounded_context,
                BoundedContextStatus::Archived,
            )
            .await
            .unwrap();
            return None;
        }
        (
            Surface::Graphql,
            Op::Submit {
                payload,
                idempotency_key,
            },
        ) => {
            graphql(
                SUBMIT_COMMAND,
                submit_variables(payload, *idempotency_key),
                true,
            )
            .await
        }
        (Surface::Graphql, Op::SubmitWithoutCredential { payload }) => {
            graphql(SUBMIT_COMMAND, submit_variables(payload, None), false).await
        }
        (Surface::Graphql, Op::Read { after }) => {
            graphql(QUERY_EVENTS, read_variables(*after), true).await
        }
        (Surface::Graphql, Op::ReadWithoutCredential) => {
            graphql(QUERY_EVENTS, read_variables(None), false).await
        }
        (
            Surface::Rest,
            Op::Submit {
                payload,
                idempotency_key,
            },
        ) => {
            let mut headers = vec![bearer(&deployment.command_credential)];
            if let Some(key) = idempotency_key {
                headers.push(("idempotency-key", key.to_string()));
            }
            send(
                &deployment.rest,
                "POST",
                "/v1/commands/trigger",
                &headers,
                Some(submit_body(payload)),
            )
            .await
        }
        (Surface::Rest, Op::SubmitWithoutCredential { payload }) => {
            send(
                &deployment.rest,
                "POST",
                "/v1/commands/trigger",
                &[],
                Some(submit_body(payload)),
            )
            .await
        }
        (Surface::Rest, Op::Read { after }) => {
            let headers = [bearer(&deployment.read_credential)];
            send(&deployment.rest, "GET", &read_uri(*after), &headers, None).await
        }
        (Surface::Rest, Op::ReadWithoutCredential) => {
            send(&deployment.rest, "GET", &read_uri(None), &[], None).await
        }
    };
    Some((status, outcome(surface, op, status, &body)))
}

/// The observable outcome of one response: the error `code`, or the
/// fields both surfaces answer the operation with.
fn outcome(surface: Surface, op: &Op, status: u16, body: &Value) -> Value {
    let reading = matches!(op, Op::Read { .. } | Op::ReadWithoutCredential);
    let data = match surface {
        Surface::Graphql => {
            if let Some(errors) = body.get("errors") {
                return json!({ "error": errors[0]["extensions"]["code"] });
            }
            body["data"][if reading {
                "queryEvents"
            } else {
                "submitCommand"
            }]
            .clone()
        }
        Surface::Rest => {
            if !(200..300).contains(&status) {
                return json!({ "error": body["code"] });
            }
            if reading {
                body["events"].clone()
            } else {
                body.clone()
            }
        }
    };
    if reading {
        // An event is its sequence and its payload. GraphQL's payload is
        // a JSON string, REST's the JSON itself (§7.3): the value is
        // compared, not its encoding.
        let events = data.as_array().unwrap().iter().map(|event| {
            let payload = match &event["payload"] {
                Value::String(s) => serde_json::from_str(s).unwrap(),
                other => other.clone(),
            };
            json!({ "sequence": event["sequence"], "payload": payload })
        });
        return json!({ "events": events.collect::<Vec<_>>() });
    }
    // A field GraphQL answers `null` is one REST leaves out.
    let fields = data
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, v)| !v.is_null());
    Value::Object(fields.map(|(k, v)| (k.clone(), v.clone())).collect())
}

/// `value` as JSON with every object's keys sorted, whatever order the
/// response had them in.
fn canonical(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort();
            let fields: Vec<String> = keys
                .into_iter()
                .map(|k| format!("{}:{}", Value::String(k.clone()), canonical(&map[k])))
                .collect();
            format!("{{{}}}", fields.join(","))
        }
        Value::Array(items) => {
            format!(
                "[{}]",
                items.iter().map(canonical).collect::<Vec<_>>().join(",")
            )
        }
        other => other.to_string(),
    }
}

#[test]
fn graphql_and_rest_match_the_recorded_transcript() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let jwks_url = serve_jwks().await;
        let graphql = deploy(&database_url, &jwks_url).await;
        let rest = deploy(&database_url, &jwks_url).await;

        let version_line = format!("# recorded against skilj {}", env!("CARGO_PKG_VERSION"));
        let mut transcript = vec![version_line.clone()];
        let mut disagreements = Vec::new();
        let mut stale_divergences = Vec::new();
        for (scenario, op) in scenarios() {
            let on_graphql = run(&graphql, Surface::Graphql, &op).await;
            let on_rest = run(&rest, Surface::Rest, &op).await;
            let (Some((graphql_status, graphql_outcome)), Some((rest_status, rest_outcome))) =
                (on_graphql, on_rest)
            else {
                continue;
            };
            for (surface, status, outcome) in [
                (Surface::Graphql, graphql_status, &graphql_outcome),
                (Surface::Rest, rest_status, &rest_outcome),
            ] {
                transcript.push(format!(
                    "{scenario}\t{}\t{status}\t{}",
                    surface.name(),
                    canonical(outcome)
                ));
            }
            let catalogued = DIVERGENCES.iter().any(|(name, _)| *name == scenario);
            match (graphql_outcome == rest_outcome, catalogued) {
                (false, false) => disagreements.push(format!(
                    "{scenario}: graphql {} but rest {}",
                    canonical(&graphql_outcome),
                    canonical(&rest_outcome)
                )),
                (true, true) => stale_divergences.push(scenario),
                _ => {}
            }
        }
        let unknown: Vec<_> = DIVERGENCES
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| !scenarios().iter().any(|(scenario, _)| scenario == name))
            .collect();
        let transcript = transcript.join("\n") + "\n";

        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(TRANSCRIPT_PATH);
        if std::env::var_os(RECORD_ENV).is_some() {
            std::fs::write(&path, &transcript).unwrap();
        } else {
            let recorded = std::fs::read_to_string(&path).unwrap_or_default();
            let recorded_version = recorded.lines().next().unwrap_or_default();
            assert_eq!(
                recorded_version, version_line,
                "{TRANSCRIPT_PATH} was recorded against another version - re-record it with \
                 {RECORD_ENV}=1 and review the diff"
            );
            let differing: Vec<_> = transcript
                .lines()
                .zip(recorded.lines())
                .filter(|(now, then)| now != then)
                .map(|(now, then)| format!("  recorded: {then}\n  now:      {now}"))
                .collect();
            assert!(
                differing.is_empty() && transcript.lines().count() == recorded.lines().count(),
                "the surfaces no longer answer as {TRANSCRIPT_PATH} records - if that's \
                 intended, re-record it with {RECORD_ENV}=1:\n{}",
                differing.join("\n")
            );
        }

        assert!(
            disagreements.is_empty(),
            "GraphQL and REST disagree - fix one, or catalogue the scenario in DIVERGENCES \
             with its reason:\n{}",
            disagreements.join("\n")
        );
        assert!(
            stale_divergences.is_empty(),
            "catalogued as divergent, but the surfaces agree: {stale_divergences:?}"
        );
        assert!(
            unknown.is_empty(),
            "DIVERGENCES names no scenario: {unknown:?}"
        );
    });
}
