//! End-to-end tests for Codeberg issue #13's templated bounded-context
//! tenants (`createBoundedContextFromTemplate`/
//! `resyncBoundedContextFromTemplate`, `surface BoundedContextTemplating`
//! in specs/skilj.allium). Same real-HTTP-through-`Skilj::graphql_router()`,
//! real-JWKS-server harness as `skilj/tests/graphql_type_registration.rs` -
//! see its own doc comment for the details, duplicated here rather than
//! extracted into shared test-support (the same call this project's
//! earlier passes already made).
//!
//! `creating_a_tenant_from_a_template_lets_it_actually_process_commands`
//! is the load-bearing one: it's the test that would fail with a real
//! `no_decider_registered` GraphQL error if `TemplateCache`'s dispatch-
//! resolution indirection (`skilj/src/lib.rs`) were missing or wrong -
//! every `Registered*` map a dispatcher consults is keyed by whatever
//! literal name `#[auto_register]` declared at `.build()` time (always
//! the *template*'s own name), never a tenant's, so proving a tenant can
//! actually get a command accepted is the only real proof the mechanism
//! works, not just that the mutation itself returns success.
//!
//! `TEMPLATE_BOUNDED_CONTEXT` is a compile-time literal, not
//! `unique_name(...)` - `CommandType`/`EventType::BOUNDED_CONTEXT` is a
//! `const`, fixed at compile time by definition (see
//! `skilj/tests/auto_register.rs`'s own doc comment for the identical
//! reasoning). `resync_bounded_context_from_template_pulls_in_a_later_schema_change`
//! needs no compiled `decide()` at all - only `registerEventType`'s own
//! ordinary schema-evolution behaviour - so it uses a fresh,
//! `unique_name`-scoped template bounded context instead, fully isolated
//! from the compiled fixture the dispatch test needs.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use skilj::{auto_register, CommandType, EventType, IdpConfig, SigningAlgorithm, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::Pool;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, CommandDecision};
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
            .expect("failed to build a tokio runtime for bounded_context_templating tests")
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
        Err(_) => false,
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
    let database_name = "skilj_bounded_context_templating_test";
    if let Err(e) = server.create_database(database_name).await {
        eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
        return None;
    }
    let database_url = server.settings().url(database_name);
    if !check_reachable(&database_url, "embedded PostgreSQL").await {
        return None;
    }
    Some(TestDb {
        database_url,
        _embedded: Some(server),
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

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
            .expect("test JWKS server failed");
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

async fn insert_role(pool: &Pool, superadmin: bool, name: &str) -> (Role, String) {
    let external_subject = unique_name("subject");
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.clone(),
        name: name.to_string(),
        superadmin,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    skilj_core::db::insert_role(pool, &role).await.unwrap();
    (role, sign_jwt(&external_subject))
}

async fn insert_bounded_context(pool: &Pool, name: &str) -> BoundedContext {
    let bc = BoundedContext {
        name: name.to_string(),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    skilj_core::db::insert_bounded_context(pool, &bc)
        .await
        .unwrap();
    bc
}

async fn grant(pool: &Pool, role: &Role, bc: &BoundedContext, level: AccessLevel) {
    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
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

const CREATE_FROM_TEMPLATE_MUTATION: &str = "\
    mutation($template: String!, $name: String!, $roleId: ID!, $level: AccessLevel!) { \
        createBoundedContextFromTemplate(template: $template, name: $name, roleId: $roleId, \
            level: $level, canReadSensitive: false) { \
            name \
        } \
    }";

const RESYNC_MUTATION: &str = "\
    mutation($bc: String!) { \
        resyncBoundedContextFromTemplate(boundedContext: $bc) { name } \
    }";

const SUBMIT_COMMAND_MUTATION: &str = "\
    mutation($bc: String!, $type: String!, $payload: String!) { \
        submitCommand(boundedContext: $bc, commandTypeName: $type, payload: $payload) { \
            accepted rejectionReason rejectionKind \
        } \
    }";

const EVENT_TYPES_QUERY: &str = "\
    query($bc: String!) { eventTypes(boundedContext: $bc) { name schemaVersion schema } }";

const COMMAND_TYPES_QUERY: &str = "\
    query($bc: String!) { commandTypes(boundedContext: $bc) { name } }";

const ARCHIVE_MUTATION: &str = "\
    mutation($name: String!) { archiveBoundedContext(name: $name) { name } }";

const DELETE_MUTATION: &str = "\
    mutation($name: String!) { deleteBoundedContext(name: $name) { name } }";

// --- fixtures for the dispatch test - see this file's own doc comment
// for why these need a compile-time-fixed BOUNDED_CONTEXT ---

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct FixturePayload {}

const TEMPLATE_BOUNDED_CONTEXT: &str = "skilj_templating_test_template";

struct TemplateFixtureEvent;

#[auto_register(TEMPLATE_BOUNDED_CONTEXT)]
impl EventType for TemplateFixtureEvent {
    type Payload = FixturePayload;
    const NAME: &'static str = "TemplatingFixtureEvent";
    fn direct_creation_allowed() -> bool {
        true
    }
}

enum TemplateFixtureBoundedContextEvent {
    Fixture(FixturePayload),
}

impl BoundedContextEvent for TemplateFixtureBoundedContextEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "TemplatingFixtureEvent" => Some(
                serde_json::from_str(&event.payload)
                    .map(TemplateFixtureBoundedContextEvent::Fixture),
            ),
            _ => None,
        }
    }
}

struct TemplateFixtureCommand;

#[auto_register(TEMPLATE_BOUNDED_CONTEXT)]
impl CommandType for TemplateFixtureCommand {
    type Payload = FixturePayload;
    type Event = TemplateFixtureBoundedContextEvent;
    const NAME: &'static str = "TemplatingFixtureCommand";
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

// --- a second, independent fixture set for the template-deletion test
// below - it archives and deletes its own template, so it needs a
// compile-time-fixed name of its own, isolated from `TEMPLATE_BOUNDED_
// CONTEXT` above (deleting that one would corrupt the dispatch test,
// which runs concurrently in the same binary by default) ---

const DELETION_TEMPLATE_BOUNDED_CONTEXT: &str = "skilj_templating_test_deltpl";

struct DeletionTemplateFixtureEvent;

#[auto_register(DELETION_TEMPLATE_BOUNDED_CONTEXT)]
impl EventType for DeletionTemplateFixtureEvent {
    type Payload = FixturePayload;
    const NAME: &'static str = "DeletionTemplatingFixtureEvent";
    fn direct_creation_allowed() -> bool {
        true
    }
}

enum DeletionTemplateFixtureBoundedContextEvent {
    Fixture(FixturePayload),
}

impl BoundedContextEvent for DeletionTemplateFixtureBoundedContextEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "DeletionTemplatingFixtureEvent" => Some(
                serde_json::from_str(&event.payload)
                    .map(DeletionTemplateFixtureBoundedContextEvent::Fixture),
            ),
            _ => None,
        }
    }
}

struct DeletionTemplateFixtureCommand;

#[auto_register(DELETION_TEMPLATE_BOUNDED_CONTEXT)]
impl CommandType for DeletionTemplateFixtureCommand {
    type Payload = FixturePayload;
    type Event = DeletionTemplateFixtureBoundedContextEvent;
    const NAME: &'static str = "DeletionTemplatingFixtureCommand";
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

/// The decisive test - see this file's own doc comment. Without
/// `TemplateCache`'s dispatch-resolution indirection, `submitCommand`
/// below would fail with `no_decider_registered`: `TemplateFixtureCommand`
/// is only ever compiled into the `(TEMPLATE_BOUNDED_CONTEXT, ...)` map
/// entry, never into one keyed by the tenant's own, runtime-chosen name.
#[test]
fn creating_a_tenant_from_a_template_lets_it_actually_process_commands() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let pool = skilj_core::db::connect(&database_url).await.unwrap();
        let jwks_url = serve_jwks().await;

        // Idempotent get-or-insert, matching auto_register.rs's own
        // `ensure_bounded_context` - other tests in this same binary run
        // concurrently and may already have created this fixed-name
        // context.
        if skilj_core::db::get_bounded_context(&pool, TEMPLATE_BOUNDED_CONTEXT)
            .await
            .unwrap()
            .is_none()
        {
            let _ = insert_bounded_context(&pool, TEMPLATE_BOUNDED_CONTEXT).await;
        }
        let template_bc = skilj_core::db::get_bounded_context(&pool, TEMPLATE_BOUNDED_CONTEXT)
            .await
            .unwrap()
            .unwrap();

        let (reconciliation_role, _) = insert_role(&pool, false, "Reconciliation").await;
        grant(
            &pool,
            &reconciliation_role,
            &template_bc,
            AccessLevel::Admin,
        )
        .await;

        let (_superadmin, superadmin_jwt) = insert_role(&pool, true, "Superadmin").await;
        let (tenant_role, tenant_jwt) = insert_role(&pool, false, "Tenant Operator").await;

        let (skilj, _report) = Skilj::builder(database_url)
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .auto_register()
            .reconciliation_role(reconciliation_role.external_subject.clone())
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();

        let tenant_name = unique_name("tenant");
        let response = graphql_request(
            &router,
            &superadmin_jwt,
            CREATE_FROM_TEMPLATE_MUTATION,
            json!({
                "template": TEMPLATE_BOUNDED_CONTEXT,
                "name": tenant_name,
                "roleId": tenant_role.id,
                "level": "ADMIN",
            }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(
            response["data"]["createBoundedContextFromTemplate"]["name"],
            tenant_name
        );

        // The decisive assertion: a real command, submitted immediately -
        // zero intervening GraphQL round-trips - against the tenant's own
        // runtime-chosen name, reaches the exact `decide()` compiled
        // under the template's name.
        //
        // NOT a reliable regression test for ultra-review bug_001's own
        // same-instance race specifically, despite the shape suggesting
        // it should be: verified directly (temporarily removed the
        // create resolver's synchronous `template_cache.refresh` call
        // and reran) that this still passes without it, in this harness -
        // one local Postgres, no real network hop, everything in one
        // process, so the background cross-instance `PgListener` task
        // wins that race too reliably to ever demonstrate the gap here.
        // The synchronous refresh stays (it's still correct, and closes
        // a real window once network latency between instances is
        // real), but don't read a pass here as proof it's load-bearing.
        let response = graphql_request(
            &router,
            &tenant_jwt,
            SUBMIT_COMMAND_MUTATION,
            json!({
                "bc": tenant_name,
                "type": "TemplatingFixtureCommand",
                "payload": "{}",
            }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);

        // Structural check, now that the decisive assertion above is
        // safely past: the tenant's own schema really does carry the
        // template's type registrations as real rows, not just a live
        // dispatch fallback. `commandTypes` is `AdminAccess`-gated, so
        // this needs the tenant's own admin-level mapping granted above,
        // not the superadmin caller (a superadmin has no per-context
        // mapping of its own on any ordinary bounded context).
        let response = graphql_request(
            &router,
            &tenant_jwt,
            COMMAND_TYPES_QUERY,
            json!({ "bc": tenant_name }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(
            response["data"]["commandTypes"][0]["name"],
            "TemplatingFixtureCommand"
        );
    });
}

/// Proves `resyncBoundedContextFromTemplate` actually pulls in a real
/// schema change, not just that the call succeeds - fully via GraphQL's
/// own `registerEventType`/`eventTypes`, no compiled `decide()` needed,
/// so this uses a fresh `unique_name`-scoped template rather than the
/// compile-time-fixed one the dispatch test above needs.
#[test]
fn resync_bounded_context_from_template_pulls_in_a_later_schema_change() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let pool = skilj_core::db::connect(&database_url).await.unwrap();
        let jwks_url = serve_jwks().await;

        let template_name = unique_name("rtpl");
        let template_bc = insert_bounded_context(&pool, &template_name).await;
        let (admin_role, admin_jwt) = insert_role(&pool, false, "Template Admin").await;
        grant(&pool, &admin_role, &template_bc, AccessLevel::Admin).await;
        let (_superadmin, superadmin_jwt) = insert_role(&pool, true, "Superadmin").await;
        let (tenant_role, tenant_jwt) = insert_role(&pool, false, "Tenant Operator").await;

        let (skilj, _report) = Skilj::builder(database_url)
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();

        // v1, on the template.
        let register_v1 = "\
            mutation($bc: String!) { \
                registerEventType(boundedContext: $bc, name: \"ResyncFixtureEvent\", \
                    schema: \"{\\\"properties\\\":{\\\"amount\\\":{\\\"type\\\":\\\"number\\\"}}}\", \
                    tagMappings: [], sensitiveFields: [], externalCreationAllowed: true, \
                    directCreationAllowed: true, systemTriggeredAllowed: false, \
                    eventReadAllowed: true) { name } \
            }";
        let response =
            graphql_request(&router, &admin_jwt, register_v1, json!({ "bc": template_name })).await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");

        // Stamp the tenant out of the template at v1.
        let tenant_name = unique_name("rtnt");
        let response = graphql_request(
            &router,
            &superadmin_jwt,
            CREATE_FROM_TEMPLATE_MUTATION,
            json!({
                "template": template_name,
                "name": tenant_name,
                "roleId": tenant_role.id,
                "level": "ADMIN",
            }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");

        let response = graphql_request(
            &router,
            &tenant_jwt,
            EVENT_TYPES_QUERY,
            json!({ "bc": tenant_name }),
        )
        .await;
        assert_eq!(response["data"]["eventTypes"][0]["schemaVersion"], 1);

        // v2, on the template only - additive and backwards-compatible,
        // per schema_is_backwards_compatible's own rules (a new optional
        // property).
        let register_v2 = "\
            mutation($bc: String!) { \
                registerEventType(boundedContext: $bc, name: \"ResyncFixtureEvent\", \
                    schema: \"{\\\"properties\\\":{\\\"amount\\\":{\\\"type\\\":\\\"number\\\"},\\\"note\\\":{\\\"type\\\":\\\"string\\\"}}}\", \
                    tagMappings: [], sensitiveFields: [], externalCreationAllowed: true, \
                    directCreationAllowed: true, systemTriggeredAllowed: false, \
                    eventReadAllowed: true) { name schemaVersion } \
            }";
        let response =
            graphql_request(&router, &admin_jwt, register_v2, json!({ "bc": template_name })).await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");
        assert_eq!(response["data"]["registerEventType"]["schemaVersion"], 2);

        // Before resync: the tenant is still on v1 - the whole point
        // being tested here is that this doesn't happen automatically.
        let response = graphql_request(
            &router,
            &tenant_jwt,
            EVENT_TYPES_QUERY,
            json!({ "bc": tenant_name }),
        )
        .await;
        assert_eq!(response["data"]["eventTypes"][0]["schemaVersion"], 1);

        // Resync - now it does.
        let response = graphql_request(
            &router,
            &superadmin_jwt,
            RESYNC_MUTATION,
            json!({ "bc": tenant_name }),
        )
        .await;
        assert!(response.get("errors").is_none(), "unexpected errors: {response:?}");

        let response = graphql_request(
            &router,
            &tenant_jwt,
            EVENT_TYPES_QUERY,
            json!({ "bc": tenant_name }),
        )
        .await;
        assert_eq!(response["data"]["eventTypes"][0]["schemaVersion"], 2);
        assert!(response["data"]["eventTypes"][0]["schema"]
            .as_str()
            .unwrap()
            .contains("note"));
    });
}

/// Ultra-review bug_002's own regression test: a `roleId` that doesn't
/// resolve to any real `Role` must not leave an orphaned tenant behind.
/// Without the fix, `insert_bounded_context` had already committed by
/// the time the bad `roleId` was even looked up, so the name was
/// permanently stuck (`BoundedContextNameTaken` on any retry, with no
/// way to reach `deleteBoundedContext` either, since that itself
/// requires archiving first). The decisive assertion is the *second*
/// call: the exact same name, now with a real `roleId`, must succeed -
/// which is only possible if the first call's failure left nothing
/// behind to collide with.
#[test]
fn a_bad_role_id_leaves_no_orphaned_tenant_behind() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let pool = skilj_core::db::connect(&database_url).await.unwrap();
        let jwks_url = serve_jwks().await;

        let template_name = unique_name("atpl");
        insert_bounded_context(&pool, &template_name).await;
        let (_superadmin, superadmin_jwt) = insert_role(&pool, true, "Superadmin").await;
        let (real_role, _) = insert_role(&pool, false, "Real Role").await;

        let (skilj, _report) = Skilj::builder(database_url)
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();

        let tenant_name = unique_name("orphan");
        let bogus_role_id = generate_token_id();

        let response = graphql_request(
            &router,
            &superadmin_jwt,
            CREATE_FROM_TEMPLATE_MUTATION,
            json!({
                "template": template_name,
                "name": tenant_name,
                "roleId": bogus_role_id,
                "level": "ADMIN",
            }),
        )
        .await;
        assert!(
            response.get("errors").is_some(),
            "expected a not_found error: {response:?}"
        );

        // The decisive check: retrying the identical name with a real
        // role now succeeds - proving nothing from the failed call was
        // left behind to collide with it.
        let response = graphql_request(
            &router,
            &superadmin_jwt,
            CREATE_FROM_TEMPLATE_MUTATION,
            json!({
                "template": template_name,
                "name": tenant_name,
                "roleId": real_role.id,
                "level": "ADMIN",
            }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(
            response["data"]["createBoundedContextFromTemplate"]["name"],
            tenant_name
        );
    });
}

/// Ultra-review bug_005's own regression test: deleting a template must
/// not silently stop its tenants from processing commands. Without the
/// fix, `TemplateCache` read the same nullable `template` column
/// `DeleteBoundedContext`'s `ON DELETE SET NULL` cascade clears, so
/// dispatch resolution would fall back to the tenant's own name -  never
/// a real dispatcher-map key - the moment the template's row was gone.
#[test]
fn deleting_a_template_does_not_break_its_tenants_own_dispatch() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let pool = skilj_core::db::connect(&database_url).await.unwrap();
        let jwks_url = serve_jwks().await;

        if skilj_core::db::get_bounded_context(&pool, DELETION_TEMPLATE_BOUNDED_CONTEXT)
            .await
            .unwrap()
            .is_none()
        {
            let _ = insert_bounded_context(&pool, DELETION_TEMPLATE_BOUNDED_CONTEXT).await;
        }
        let template_bc =
            skilj_core::db::get_bounded_context(&pool, DELETION_TEMPLATE_BOUNDED_CONTEXT)
                .await
                .unwrap()
                .unwrap();

        let (reconciliation_role, _) = insert_role(&pool, false, "Reconciliation").await;
        grant(
            &pool,
            &reconciliation_role,
            &template_bc,
            AccessLevel::Admin,
        )
        .await;
        let (template_admin, template_admin_jwt) =
            insert_role(&pool, false, "Template Admin").await;
        grant(&pool, &template_admin, &template_bc, AccessLevel::Admin).await;
        let (_superadmin, superadmin_jwt) = insert_role(&pool, true, "Superadmin").await;
        let (tenant_role, tenant_jwt) = insert_role(&pool, false, "Tenant Operator").await;

        let (skilj, _report) = Skilj::builder(database_url)
            .identity_provider(IdpConfig::new(
                jwks_url.parse().unwrap(),
                TEST_ISSUER,
                SigningAlgorithm::Rs256,
            ))
            .auto_register()
            .reconciliation_role(reconciliation_role.external_subject.clone())
            .build()
            .await
            .unwrap();
        let router = skilj.graphql_router().await.unwrap();

        let tenant_name = unique_name("dtnt");
        let response = graphql_request(
            &router,
            &superadmin_jwt,
            CREATE_FROM_TEMPLATE_MUTATION,
            json!({
                "template": DELETION_TEMPLATE_BOUNDED_CONTEXT,
                "name": tenant_name,
                "roleId": tenant_role.id,
                "level": "ADMIN",
            }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );

        let submit = json!({
            "bc": tenant_name,
            "type": "DeletionTemplatingFixtureCommand",
            "payload": "{}",
        });

        // Baseline: dispatch works while the template still exists.
        let response = graphql_request(
            &router,
            &tenant_jwt,
            SUBMIT_COMMAND_MUTATION,
            submit.clone(),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);

        // Archive then delete the template - the tenant's own `template`
        // column is cleared by the FK cascade at this point, but its
        // `dispatch_template` is not.
        let response = graphql_request(
            &router,
            &template_admin_jwt,
            ARCHIVE_MUTATION,
            json!({ "name": DELETION_TEMPLATE_BOUNDED_CONTEXT }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        let response = graphql_request(
            &router,
            &superadmin_jwt,
            DELETE_MUTATION,
            json!({ "name": DELETION_TEMPLATE_BOUNDED_CONTEXT }),
        )
        .await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );

        // The decisive assertion: the tenant still answers exactly what
        // it answered before its template was deleted.
        let response = graphql_request(&router, &tenant_jwt, SUBMIT_COMMAND_MUTATION, submit).await;
        assert!(
            response.get("errors").is_none(),
            "unexpected errors: {response:?}"
        );
        assert_eq!(response["data"]["submitCommand"]["accepted"], true);
    });
}
