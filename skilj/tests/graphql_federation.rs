//! `/graphql` as a federation subgraph (docs/architecture.md §194,
//! `surface FederatedSubgraph` in specs/skilj.allium).
//!
//! One database, three bounded contexts with the same types: one the
//! federation options publish, one they don't, and a tenant made from the
//! published one, which they name but which is never published. Checks
//! what the published description holds, that `_entities` answers through
//! the caller's own grant, and that an invalid prefix is refused.
//!
//! The description is also compared, line by line, against the checked-in
//! `fixtures/federation/`, recorded twice under two prefixes, as two skilj
//! services would publish it. `scripts/check-federation-composition.sh`
//! composes those with Apollo's and Hive's composition in CI. Re-record
//! after an intended change with
//! `SKILJ_RECORD_FEDERATION=1 cargo test -p skilj --test graphql_federation`
//! and review the diff.
//!
//! Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness and JWKS
//! test server as `conformance.rs`, duplicated rather than shared, like
//! every other test binary here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::SubsecRound;
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use skilj::{CommandType, EventType, IdpConfig, Projection, SigningAlgorithm, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::Pool;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, CommandDecision, EventSpec};
use skilj_graphql::federation::FederationOptions;
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

/// Re-records the fixtures instead of comparing against them.
const RECORD_ENV: &str = "SKILJ_RECORD_FEDERATION";

/// The checked-in descriptions, relative to this crate, by prefix.
const FIXTURES: &[(&str, &str)] = &[
    ("ledger", "tests/fixtures/federation/ledger.graphql"),
    ("bank", "tests/fixtures/federation/bank.graphql"),
];

/// Fixed names, so the recorded descriptions are the same on every run:
/// each test binary has a database of its own (docs/architecture.md
/// §167), and the deployment below is made once per run.
const PUBLISHED: &str = "fed_published";
const UNPUBLISHED: &str = "fed_unpublished";
const TENANT: &str = "fed_tenant";

// --- fixtures ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct MoneyDepositedPayload {
    account: String,
    amount: i64,
}

struct MoneyDeposited;

impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
}

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
    account: String,
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
                payload: json!({ "account": payload.account, "amount": payload.amount }),
            }],
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct AccountBalanceState {
    total: i64,
}

/// One instance per account - the instance key `_entities` names.
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
    fn keys(event: &Self::Event) -> Vec<String> {
        let BankingEvent::MoneyDeposited(payload) = event;
        vec![payload.account.clone()]
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        let BankingEvent::MoneyDeposited(payload) = event;
        state.total += payload.amount;
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for federation")
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
        "aud": TEST_AUDIENCE,
        "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp(),
    });
    let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes())
        .expect("the test private key PEM is well-formed");
    jsonwebtoken::encode(&header, &claims, &key).expect("signing a well-formed JWT never fails")
}

// --- the deployment ---

/// One skilj service per prefix, both publishing [`PUBLISHED`] and
/// [`TENANT`] from the same database, plus the Roles the tests act as.
struct Deployment {
    database_url: String,
    jwks_url: String,
    // Kept alive for their background tasks while the routers are used.
    services: Vec<(&'static str, Skilj, axum::Router)>,
    /// Read access to [`PUBLISHED`] only.
    reader_jwt: String,
    /// Write access to [`PUBLISHED`] only.
    writer_jwt: String,
    /// An active Role with no grant at all.
    stranger_jwt: String,
}

static DEPLOYMENT: tokio::sync::OnceCell<Option<Deployment>> = tokio::sync::OnceCell::const_new();

async fn deployment() -> Option<&'static Deployment> {
    DEPLOYMENT.get_or_init(deploy).await.as_ref()
}

async fn insert_role(pool: &Pool, subject: &str) -> Role {
    let role = Role {
        id: generate_token_id(),
        external_subject: subject.to_string(),
        name: subject.to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    skilj_core::db::insert_role(pool, &role).await.unwrap();
    role
}

async fn grant(pool: &Pool, role: &Role, bounded_context: &BoundedContext, level: AccessLevel) {
    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bounded_context.clone(),
        level,
        can_read_sensitive: false,
        scope: None,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    skilj_core::db::insert_role_access_mapping(pool, &mapping)
        .await
        .unwrap();
}

async fn deploy() -> Option<Deployment> {
    let database_url = skilj_test_support::database_url("skilj_graphql_federation_test").await?;
    let pool = skilj_core::db::connect(&database_url).await.unwrap();
    skilj_core::db::migrate(&pool).await.unwrap();
    let jwks_url = serve_jwks().await;

    let admin_subject = unique_name("admin");
    let admin = insert_role(&pool, &admin_subject).await;
    let reader = insert_role(&pool, &unique_name("reader")).await;
    let writer = insert_role(&pool, &unique_name("writer")).await;
    let stranger = insert_role(&pool, &unique_name("stranger")).await;

    let context = |name: &str, template: Option<&BoundedContext>| BoundedContext {
        name: name.to_string(),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: template.map(|t| Box::new(t.clone())),
    };
    let published = context(PUBLISHED, None);
    let unpublished = context(UNPUBLISHED, None);
    let tenant = context(TENANT, Some(&published));
    for bc in [&published, &unpublished, &tenant] {
        skilj_core::db::insert_bounded_context(&pool, bc)
            .await
            .unwrap();
        grant(&pool, &admin, bc, AccessLevel::Admin).await;
    }
    grant(&pool, &reader, &published, AccessLevel::Read).await;
    grant(&pool, &writer, &published, AccessLevel::Write).await;

    let mut services = Vec::new();
    for (prefix, _) in FIXTURES {
        let (skilj, _) = builder(&database_url, &jwks_url, &admin_subject)
            .graphql_federation(
                FederationOptions::new()
                    .prefix(*prefix)
                    .publish(PUBLISHED)
                    .publish(TENANT),
            )
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();
        services.push((*prefix, skilj, router));
    }

    Some(Deployment {
        database_url,
        jwks_url,
        services,
        reader_jwt: sign_jwt(&reader.external_subject),
        writer_jwt: sign_jwt(&writer.external_subject),
        stranger_jwt: sign_jwt(&stranger.external_subject),
    })
}

fn builder(database_url: &str, jwks_url: &str, admin_subject: &str) -> skilj::SkiljBuilder {
    let mut builder = Skilj::builder(database_url.to_string())
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
        .identity_provider(IdpConfig::new(
            jwks_url.parse().unwrap(),
            TEST_ISSUER,
            TEST_AUDIENCE,
            SigningAlgorithm::Rs256,
        ))
        .reconciliation_role(admin_subject.to_string());
    for bc in [PUBLISHED, UNPUBLISHED, TENANT] {
        builder = builder
            .bounded_context(bc)
            .event_type::<MoneyDeposited>()
            .command_type::<DepositMoney>()
            .projection::<AccountBalance>();
    }
    builder
}

impl Deployment {
    fn service(&self, prefix: &str) -> (&Skilj, &axum::Router) {
        let (_, skilj, router) = self.services.iter().find(|(p, _, _)| *p == prefix).unwrap();
        (skilj, router)
    }
}

async fn graphql(router: &axum::Router, jwt: Option<&str>, query: &str, variables: Value) -> Value {
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

async fn service_sdl(router: &axum::Router, jwt: Option<&str>) -> String {
    let response = graphql(router, jwt, "{ _service { sdl } }", json!({})).await;
    assert!(response.get("errors").is_none(), "{response}");
    response["data"]["_service"]["sdl"]
        .as_str()
        .unwrap()
        .to_string()
}

// --- the tests ---

/// `@guarantee PublishedScopeIsTheOperatorsChoice`, `TenantsAreNeverPublished`
/// and `OnlyGrantFacingSurfacesArePublished`: `_service` answers anyone
/// with the same description, naming the published bounded context and
/// not the unpublished one or the tenant, with the admin surface
/// `@inaccessible` and every name prefixed.
#[test]
fn the_published_description_names_only_what_the_options_publish() {
    runtime().block_on(async {
        let Some(deployment) = deployment().await else {
            return;
        };
        let (skilj, router) = deployment.service("ledger");
        let sdl = service_sdl(router, None).await;

        assert_eq!(service_sdl(router, Some(&deployment.reader_jwt)).await, sdl);
        assert_eq!(
            service_sdl(router, Some(&deployment.stranger_jwt)).await,
            sdl
        );
        assert_eq!(
            skilj.federation_sdl().await.unwrap().as_deref(),
            Some(sdl.as_str())
        );

        assert!(
            sdl.contains(&format!(
                "type Ledger{PUBLISHED}_AccountBalance @key(fields: \"projectionKey\")"
            )),
            "{sdl}"
        );
        assert!(!sdl.contains(UNPUBLISHED), "{sdl}");
        assert!(!sdl.contains(TENANT), "{sdl}");

        // Published root fields, prefixed and accessible.
        for field in [
            "ledgerProjection(",
            "ledgerSubmitCommand(",
            "ledgerEpoch:",
            "ledgerAllEvents(",
            "ledgerProjectionUpdates(",
        ] {
            let line = sdl
                .lines()
                .find(|line| line.trim_start().starts_with(field))
                .unwrap_or_else(|| panic!("no {field} in {sdl}"));
            assert!(!line.contains("@inaccessible"), "{line}");
        }
        // The admin surface, and a type only it reaches.
        for field in [
            "ledgerCreateRole(",
            "ledgerQueryEvents(",
            "ledgerDryRunCommand(",
        ] {
            let line = sdl
                .lines()
                .find(|line| line.trim_start().starts_with(field))
                .unwrap_or_else(|| panic!("no {field} in {sdl}"));
            assert!(line.contains("@inaccessible"), "{line}");
        }
        assert!(
            sdl.contains("type LedgerRoleAccessMapping @inaccessible"),
            "{sdl}"
        );
        // Reached through a published field too: private-field grants
        // name the Role they were granted to.
        assert!(sdl.contains("type LedgerRole {"), "{sdl}");
        assert!(sdl.contains("type LedgerQueriedEvent {"), "{sdl}");
        assert!(sdl.contains("type Subscription {"), "{sdl}");
        // Nothing of skilj's left unprefixed.
        for unprefixed in ["type Role ", "\tsubmitCommand(", "\tprojection("] {
            assert!(!sdl.contains(unprefixed), "{unprefixed} in {sdl}");
        }
    });
}

/// The descriptions both services publish, as checked in.
#[test]
fn the_published_descriptions_match_the_fixtures() {
    runtime().block_on(async {
        let Some(deployment) = deployment().await else {
            return;
        };
        let record = std::env::var_os(RECORD_ENV).is_some();
        for (prefix, path) in FIXTURES {
            let (skilj, _) = deployment.service(prefix);
            let sdl = skilj.federation_sdl().await.unwrap().unwrap();
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(path);
            if record {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, &sdl).unwrap();
                continue;
            }
            let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
                panic!("{}: {e} - record it with {RECORD_ENV}=1", path.display())
            });
            let differing: Vec<String> = expected
                .lines()
                .zip(sdl.lines())
                .enumerate()
                .filter(|(_, (a, b))| a != b)
                .map(|(i, (a, b))| format!("line {}: expected {a:?}, got {b:?}", i + 1))
                .collect();
            assert!(
                differing.is_empty() && expected.lines().count() == sdl.lines().count(),
                "{} differs from what skilj publishes - re-record with {RECORD_ENV}=1 if \
                 intended:\n{}",
                path.display(),
                differing.join("\n")
            );
        }
    });
}

/// `@guarantee PublishingGrantsNothing`: `_entities` answers a projection
/// instance the way `projection` does for the same caller - a reader of
/// the bounded context gets it, and a Role with no grant can't even name
/// its type. A representation that can't be answered fails the whole
/// batch: async-graphql's dynamic schema has no `null` for one item of a
/// list of union values.
#[test]
fn entities_resolve_through_the_callers_own_grant() {
    runtime().block_on(async {
        let Some(deployment) = deployment().await else {
            return;
        };
        let (_, router) = deployment.service("ledger");
        let account = unique_name("account");
        let other_account = unique_name("account");

        for (account, amount) in [(&account, 20), (&account, 22), (&other_account, 5)] {
            let response = graphql(
                router,
                Some(&deployment.writer_jwt),
                "mutation($bc: String!, $payload: String!) { \
                     ledgerSubmitCommand(boundedContext: $bc, commandTypeName: \"DepositMoney\", \
                                         payload: $payload) { accepted } }",
                json!({
                    "bc": PUBLISHED,
                    "payload": json!({ "account": account, "amount": amount }).to_string(),
                }),
            )
            .await;
            assert_eq!(
                response["data"]["ledgerSubmitCommand"]["accepted"],
                json!(true),
                "{response}"
            );
        }

        let type_name = format!("Ledger{PUBLISHED}_AccountBalance");
        let query = format!(
            "query($representations: [_Any!]!) {{ _entities(representations: $representations) \
             {{ __typename ... on {type_name} {{ projectionKey total }} }} }}"
        );
        let entities = |representations: Value| {
            let query = query.clone();
            async move {
                graphql(
                    router,
                    Some(&deployment.reader_jwt),
                    &query,
                    json!({ "representations": representations }),
                )
                .await
            }
        };

        let response = entities(json!([
            { "__typename": type_name, "projectionKey": account },
            { "__typename": type_name, "projectionKey": other_account },
        ]))
        .await;
        assert!(response.get("errors").is_none(), "{response}");
        assert_eq!(
            response["data"]["_entities"],
            json!([
                { "__typename": type_name, "projectionKey": account, "total": 42 },
                { "__typename": type_name, "projectionKey": other_account, "total": 5 },
            ]),
            "{response}"
        );

        let response = entities(json!([
            { "__typename": type_name, "projectionKey": account },
            { "__typename": "LedgerRole", "projectionKey": account },
        ]))
        .await;
        assert_eq!(response["data"], json!(null), "{response}");
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            json!("Projection type_not_found"),
            "{response}"
        );

        // A Role with no grant on the bounded context is served a schema
        // without its types (docs/architecture.md §138).
        let response = graphql(
            router,
            Some(&deployment.stranger_jwt),
            &query,
            json!({ "representations": [{ "__typename": type_name, "projectionKey": account }] }),
        )
        .await;
        assert!(response["data"].is_null(), "{response}");
        assert!(
            response["errors"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["message"].as_str().unwrap().contains(&type_name)),
            "{response}"
        );
    });
}

/// `build()` refuses a prefix that isn't letters and digits starting with
/// a letter, before it connects to anything.
#[test]
fn an_invalid_prefix_is_refused() {
    runtime().block_on(async {
        let Some(deployment) = deployment().await else {
            return;
        };
        let refused = builder(
            &deployment.database_url,
            &deployment.jwks_url,
            &unique_name("nobody"),
        )
        .graphql_federation(FederationOptions::new().prefix("led-ger"))
        .build()
        .await
        .err()
        .expect("an invalid prefix must be refused");
        assert!(refused.to_string().contains("led-ger"), "{refused}");
    });
}
