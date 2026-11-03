//! Blobs through their whole life (issue #132, docs/architecture.md §239):
//! the REST upload and download routes and who they let through; bytes
//! encrypted under a subject's key and erased with it; orphan collection,
//! and what holds a blob back from it; and a deleted bounded context's
//! bytes. Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `blob_references.rs`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use jsonwebtoken::{EncodingKey, Header};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use skilj::{CommandType, EventType, IdpConfig, SigningAlgorithm, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::blob::{BlobField, BlobRef};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, PostgresBlobStore};
use skilj_core::encryption::{InMemoryKeyProvider, KeyService};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, generate_token_secret, CommandDecision, PrivateField};
use tower::ServiceExt;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct FiledPayload {
    customer_id: String,
    document: BlobRef,
}

struct Filed;
impl EventType for Filed {
    type Payload = FiledPayload;
    const NAME: &'static str = "Filed";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn event_read_allowed() -> bool {
        true
    }
    fn blob_fields() -> Vec<BlobField> {
        vec![BlobField::new("document")]
    }
}

struct FiledPrivately;
impl EventType for FiledPrivately {
    type Payload = FiledPayload;
    const NAME: &'static str = "FiledPrivately";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn event_read_allowed() -> bool {
        true
    }
    fn blob_fields() -> Vec<BlobField> {
        vec![BlobField::new("document")]
    }
    fn private_fields() -> Vec<PrivateField> {
        vec![PrivateField {
            field: "document".to_string(),
            kind: skilj_core::shared::PrivateFieldKind::Own,
            capability: None,
            addressee_field: None,
        }]
    }
}

struct FiledForCustomer;
impl EventType for FiledForCustomer {
    type Payload = FiledPayload;
    const NAME: &'static str = "FiledForCustomer";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn event_read_allowed() -> bool {
        true
    }
    fn blob_fields() -> Vec<BlobField> {
        vec![BlobField::with_subject(
            "document",
            "customer",
            "customer_id",
        )]
    }
}

/// What a bridge's messages become: `body` always a reference, `note` free.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct InboundPayload {
    customer_id: String,
    body: BlobRef,
    #[serde(default)]
    note: Option<String>,
}

struct Inbound;
impl EventType for Inbound {
    type Payload = InboundPayload;
    const NAME: &'static str = "Inbound";
    fn external_creation_allowed() -> bool {
        true
    }
    fn event_read_allowed() -> bool {
        true
    }
    fn blob_fields() -> Vec<BlobField> {
        vec![BlobField::new("body")]
    }
}

enum NoEvents {}

impl BoundedContextEvent for NoEvents {
    fn try_from_event(_event: &Event) -> Option<Result<Self, serde_json::Error>> {
        None
    }
}

/// Needs an approval, so its payload is stored as a proposal first.
struct ApprovedFiling;
impl CommandType for ApprovedFiling {
    type Payload = FiledPayload;
    type Event = NoEvents;
    const NAME: &'static str = "ApprovedFiling";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn requires_approval() -> bool {
        true
    }
    fn blob_fields() -> Vec<BlobField> {
        vec![BlobField::new("document")]
    }
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: Vec::new() }
    }
}

struct Scoped;
impl EventType for Scoped {
    type Payload = FiledPayload;
    const NAME: &'static str = "Scoped";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn event_read_allowed() -> bool {
        true
    }
    fn tag_mappings() -> Vec<skilj_core::shared::TagMapping> {
        vec![skilj_core::shared::TagMapping::new(
            "customer",
            "customer_id",
        )]
    }
    fn owner_tag_key() -> Option<&'static str> {
        Some("customer")
    }
    fn blob_fields() -> Vec<BlobField> {
        vec![BlobField::new("document")]
    }
}

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

static TEST_DB: tokio::sync::OnceCell<Option<String>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("a tokio runtime"))
}

async fn test_db() -> Option<String> {
    TEST_DB
        .get_or_init(|| async {
            let url = skilj_test_support::database_url("skilj_blob_lifecycle_test").await?;
            let pool = match db::connect(&url).await {
                Ok(pool) => pool,
                Err(e) => {
                    eprintln!("skipping: connecting to the test database failed: {e}");
                    return None;
                }
            };
            if let Err(e) = db::migrate(&pool).await {
                eprintln!("skipping: migrating the test database failed: {e}");
                return None;
            }
            Some(url)
        })
        .await
        .clone()
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

struct Setup {
    skilj: Skilj,
    pool: Pool,
    bc: String,
    mapping: RoleAccessMapping,
}

impl Setup {
    async fn writer(&self, event_type: &str) -> String {
        let event_type = db::get_event_type(&self.pool, &self.bc, event_type)
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_direct_creation_token(
            &self.mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&self.pool, &token)
            .await
            .unwrap();
        format!("{}.{}", token.id, token.secret)
    }

    async fn reader(&self, event_type: &str, scope: Option<&str>) -> String {
        let event_type = db::get_event_type(&self.pool, &self.bc, event_type)
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_event_read_token(
            &self.mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            scope.map(str::to_string),
            None,
            None,
            None,
            now(),
        )
        .unwrap();
        db::insert_event_read_token(&self.pool, &token)
            .await
            .unwrap();
        format!("{}.{}", token.id, token.secret)
    }

    async fn upload(&self, credential: &str, query: &str, bytes: Vec<u8>) -> (StatusCode, Vec<u8>) {
        let request = Request::builder()
            .method("POST")
            .uri(format!("/v1/blobs/{}{query}", self.bc))
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/pdf")
            .body(Body::from(bytes))
            .unwrap();
        self.send(request).await
    }

    async fn download(&self, credential: &str, id: &str) -> (StatusCode, Vec<u8>) {
        let request = Request::builder()
            .uri(format!("/v1/blobs/{}/{id}", self.bc))
            .header("authorization", format!("Bearer {credential}"))
            .body(Body::empty())
            .unwrap();
        self.send(request).await
    }

    async fn file(&self, credential: &str, customer: &str, reference: &BlobRef) -> StatusCode {
        let body = serde_json::json!({
            "payload": {"customer_id": customer, "document": reference},
        });
        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        self.send(request).await.0
    }

    async fn send(&self, request: Request<Body>) -> (StatusCode, Vec<u8>) {
        let response = self.skilj.rest_router().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, body.to_vec())
    }

    async fn graphql(
        &self,
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
        let router = self.skilj.graphql_router().await.unwrap();
        let response = router.oneshot(request).await.unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&body).unwrap()
    }

    /// The real REST surface on a real port, for a bridge's own HTTP client.
    async fn serve(&self) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = self.skilj.rest_router();
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        url
    }

    async fn external_token(&self, event_type: &str) -> String {
        let event_type = db::get_event_type(&self.pool, &self.bc, event_type)
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_external_event_token(
            &self.mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            now(),
        )
        .unwrap();
        db::insert_external_event_token(&self.pool, &token)
            .await
            .unwrap();
        format!("{}.{}", token.id, token.secret)
    }

    async fn command_token(&self, command_type: &str) -> String {
        let command_type = db::get_command_type(&self.pool, &self.bc, command_type)
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_command_token(
            &self.mapping,
            &command_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            now(),
        )
        .unwrap();
        db::insert_command_token(&self.pool, &token).await.unwrap();
        format!("{}.{}", token.id, token.secret)
    }

    async fn trigger(&self, credential: &str, payload: serde_json::Value) -> (StatusCode, Vec<u8>) {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(json!({ "payload": payload }).to_string()))
            .unwrap();
        self.send(request).await
    }

    async fn state(&self, id: &str) -> &'static str {
        db::get_blob(&self.pool, &self.bc, id)
            .await
            .unwrap()
            .unwrap()
            .state
            .unwrap()
            .as_str()
    }

    async fn collect_at(&self, at: chrono::DateTime<Utc>) -> db::CollectedBlobs {
        db::collect_blobs(
            &self.pool,
            &store(self),
            &self.bc,
            at - chrono::Duration::hours(24),
            at,
            100,
        )
        .await
        .unwrap()
    }

    async fn stored_bytes(&self, storage_key: &str) -> Option<Vec<u8>> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT bytes FROM \"bc_{}\".blob_bytes WHERE storage_key = $1",
            self.bc
        )))
        .bind(storage_key)
        .fetch_optional(&self.pool)
        .await
        .unwrap();
        row.map(|(bytes,)| bytes)
    }
}

async fn setup() -> Setup {
    setup_with(None).await
}

async fn setup_with(idp: Option<IdpConfig>) -> Setup {
    let url = test_db().await.expect("caller already checked");
    let pool = db::connect(&url).await.unwrap();
    let external_subject = unique_name("subject");
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.clone(),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    db::insert_role(&pool, &role).await.unwrap();
    let bc_name = unique_name("files");
    let bc = BoundedContext {
        name: bc_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(&pool, &bc).await.unwrap();
    let mapping = RoleAccessMapping {
        role,
        bounded_context: bc,
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        scope: None,
        capabilities: Default::default(),
        delegable: Default::default(),
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(&pool, &mapping)
        .await
        .unwrap();
    let builder = match idp {
        Some(idp) => Skilj::builder(url).identity_provider(idp),
        None => Skilj::builder(url),
    };
    let (skilj, _) = builder
        .pool_options(db::PgPoolOptions::new().max_connections(4))
        .key_service(key_service())
        .bounded_context(bc_name.clone())
        .event_type::<Filed>()
        .event_type::<FiledPrivately>()
        .event_type::<FiledForCustomer>()
        .event_type::<Scoped>()
        .event_type::<Inbound>()
        .command_type::<ApprovedFiling>()
        .reconciliation_role(external_subject)
        .build()
        .await
        .unwrap();
    Setup {
        skilj,
        pool,
        bc: bc_name,
        mapping,
    }
}

fn key_service() -> KeyService {
    KeyService::new(InMemoryKeyProvider::from_bytes([9; 32]))
}

fn reference(body: &[u8]) -> BlobRef {
    serde_json::from_slice(body).unwrap()
}

fn store(setup: &Setup) -> PostgresBlobStore {
    PostgresBlobStore::new(setup.pool.clone())
}

#[test]
fn a_blob_goes_up_and_comes_down_for_a_reader_of_a_referencing_event() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let writer = setup.writer("Filed").await;
        // Over a megabyte, so it can't be taking a shortcut through a limit.
        let bytes = vec![7u8; 3 * 1024 * 1024 + 11];
        let (status, body) = setup.upload(&writer, "", bytes.clone()).await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&body)
        );
        let reference = reference(&body);
        assert_eq!(reference.size, bytes.len() as u64);

        // Nothing references it yet, so it's nobody's to read.
        let reader = setup.reader("Filed", None).await;
        assert_eq!(
            setup.download(&reader, &reference.id).await.0,
            StatusCode::NOT_FOUND
        );

        assert_eq!(
            setup.file(&writer, "c1", &reference).await,
            StatusCode::CREATED
        );
        let (status, served) = setup.download(&reader, &reference.id).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(served, bytes);

        // A reader of another type gets what a missing blob gets.
        let other = setup.reader("FiledPrivately", None).await;
        assert_eq!(
            setup.download(&other, &reference.id).await.0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            setup.download(&other, "no-such-blob").await.0,
            StatusCode::NOT_FOUND
        );
    });
}

#[test]
fn upload_needs_a_token_whose_type_declares_a_blob_field() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        // A reader's token doesn't write.
        let reader = setup.reader("Filed", None).await;
        assert_eq!(
            setup.upload(&reader, "", vec![1]).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            setup.upload("nope.nope", "", vec![1]).await.0,
            StatusCode::UNAUTHORIZED
        );
        // A subject its type doesn't declare.
        let writer = setup.writer("Filed").await;
        let (status, _) = setup
            .upload(&writer, "?subjectKey=customer&subjectValue=c1", vec![1])
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    });
}

#[test]
fn a_private_blob_field_is_refused_to_a_token() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let writer = setup.writer("FiledPrivately").await;
        let (status, body) = setup.upload(&writer, "", vec![1, 2, 3]).await;
        assert_eq!(status, StatusCode::CREATED);
        let reference = reference(&body);
        assert_eq!(
            setup.file(&writer, "c1", &reference).await,
            StatusCode::CREATED
        );
        let reader = setup.reader("FiledPrivately", None).await;
        assert_eq!(
            setup.download(&reader, &reference.id).await.0,
            StatusCode::NOT_FOUND
        );
        // The event's creator is the default private reader of an `own` field.
        let creator =
            db::blob_access_for_mapping(&setup.pool, &setup.bc, &reference.id, &setup.mapping)
                .await
                .unwrap();
        assert!(!creator, "the Role didn't create it; the token did");
    });
}

#[test]
fn a_blob_with_a_subject_is_stored_sealed_and_erased_with_the_subject() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let writer = setup.writer("FiledForCustomer").await;
        // More than one frame, and exactly the kind of bytes one could spot.
        let bytes: Vec<u8> = b"personal document "
            .iter()
            .copied()
            .cycle()
            .take(200_000)
            .collect();
        let (status, body) = setup
            .upload(
                &writer,
                "?subjectKey=customer&subjectValue=c1",
                bytes.clone(),
            )
            .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&body)
        );
        let reference = reference(&body);
        assert_eq!(reference.size, bytes.len() as u64);

        let record = db::get_blob(&setup.pool, &setup.bc, &reference.id)
            .await
            .unwrap()
            .unwrap();
        let stored = setup.stored_bytes(&record.storage_key).await.unwrap();
        assert!(
            !stored.windows(17).any(|w| w == b"personal document"),
            "the store holds plaintext"
        );

        // Another subject's event can't take this subject's blob.
        assert_eq!(
            setup.file(&writer, "c2", &reference).await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            setup.file(&writer, "c1", &reference).await,
            StatusCode::CREATED
        );

        // No token decrypts: REST reads never do.
        let reader = setup.reader("FiledForCustomer", None).await;
        assert_eq!(
            setup.download(&reader, &reference.id).await.0,
            StatusCode::NOT_FOUND
        );

        let key_service = key_service();
        let (_, mut stream) = db::read_blob(
            &setup.pool,
            &store(&setup),
            &setup.bc,
            &reference.id,
            Some(&key_service),
        )
        .await
        .unwrap()
        .unwrap();
        let mut served = Vec::new();
        while let Some(chunk) = stream.next().await {
            served.extend(chunk.unwrap());
        }
        assert_eq!(served, bytes);

        // Forgotten: not served at once, and the next pass removes the bytes.
        db::forget_subject_in_blobs(&setup.pool, &setup.bc, "customer", "c1", now())
            .await
            .unwrap();
        db::destroy_encryption_key(&setup.pool, &setup.bc, "customer", "c1", now())
            .await
            .unwrap();
        assert!(db::read_blob(
            &setup.pool,
            &store(&setup),
            &setup.bc,
            &reference.id,
            Some(&key_service),
        )
        .await
        .unwrap()
        .is_none());
        let collected = db::collect_blobs(
            &setup.pool,
            &store(&setup),
            &setup.bc,
            now() - chrono::Duration::hours(24),
            now(),
            100,
        )
        .await
        .unwrap();
        assert_eq!(collected.bytes_removed, 1);
        assert!(setup.stored_bytes(&record.storage_key).await.is_none());
    });
}

#[test]
fn a_role_reads_what_it_can_read_a_reference_to() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let access = |id: String, mapping: RoleAccessMapping| {
            let pool = setup.pool.clone();
            let bc = setup.bc.clone();
            async move {
                db::blob_access_for_mapping(&pool, &bc, &id, &mapping)
                    .await
                    .unwrap()
            }
        };
        // An ordinary field: any grant in the bounded context reads it.
        let writer = setup.writer("Filed").await;
        let (_, body) = setup.upload(&writer, "", vec![1]).await;
        let plain = reference(&body);
        assert!(
            !access(plain.id.clone(), setup.mapping.clone()).await,
            "nothing references it"
        );
        assert_eq!(setup.file(&writer, "c1", &plain).await, StatusCode::CREATED);
        assert!(access(plain.id.clone(), setup.mapping.clone()).await);

        // A private one: refused to a Role with no claim on it.
        let writer = setup.writer("FiledPrivately").await;
        let (_, body) = setup.upload(&writer, "", vec![2]).await;
        let private = reference(&body);
        assert_eq!(
            setup.file(&writer, "c1", &private).await,
            StatusCode::CREATED
        );
        assert!(!access(private.id.clone(), setup.mapping.clone()).await);

        // A subject's: needs `can_read_sensitive`, or to be the subject.
        let writer = setup.writer("FiledForCustomer").await;
        let (_, body) = setup
            .upload(&writer, "?subjectKey=customer&subjectValue=c1", vec![3])
            .await;
        let sealed = reference(&body);
        assert_eq!(
            setup.file(&writer, "c1", &sealed).await,
            StatusCode::CREATED
        );
        assert!(!access(sealed.id.clone(), setup.mapping.clone()).await);
        let mut reader = setup.mapping.clone();
        reader.can_read_sensitive = true;
        assert!(access(sealed.id.clone(), reader).await);
        let mut subject = setup.mapping.clone();
        subject.role.external_subject = "c1".to_string();
        assert!(access(sealed.id.clone(), subject).await);
    });
}

#[test]
fn a_roles_jwt_uploads_with_the_capability_and_downloads_what_it_can_read() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let jwks_url = serve_jwks().await;
        let setup = setup_with(Some(IdpConfig::new(
            jwks_url.parse().unwrap(),
            TEST_ISSUER,
            TEST_AUDIENCE,
            SigningAlgorithm::Rs256,
        )))
        .await;
        let jwt = sign_jwt(&setup.mapping.role.external_subject);

        // The grant has no `blob-upload` yet.
        assert_eq!(
            setup.upload(&jwt, "", vec![1]).await.0,
            StatusCode::FORBIDDEN
        );
        sqlx::query(
            "UPDATE role_access_mappings SET capabilities = array_append(capabilities, $1) \
             WHERE role_id = $2",
        )
        .bind(skilj_rest::blobs::BLOB_UPLOAD_CAPABILITY)
        .bind(&setup.mapping.role.id)
        .execute(&setup.pool)
        .await
        .unwrap();
        let (status, body) = setup.upload(&jwt, "", vec![1, 2]).await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&body)
        );
        let reference = reference(&body);

        // Nothing references it yet, so not even its uploader reads it.
        assert_eq!(
            setup.download(&jwt, &reference.id).await.0,
            StatusCode::NOT_FOUND
        );
        let writer = setup.writer("Filed").await;
        assert_eq!(
            setup.file(&writer, "c1", &reference).await,
            StatusCode::CREATED
        );
        let (status, served) = setup.download(&jwt, &reference.id).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(served, vec![1, 2]);

        // Someone else's JWT, with no Role behind it.
        let stranger = sign_jwt("nobody");
        assert_eq!(
            setup.download(&stranger, &reference.id).await.0,
            StatusCode::UNAUTHORIZED
        );
    });
}

#[test]
fn a_token_scoped_to_one_owner_reads_only_that_owners_blobs() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let writer = setup.writer("Scoped").await;
        let (_, body) = setup.upload(&writer, "", vec![4]).await;
        let reference = reference(&body);
        assert_eq!(
            setup.file(&writer, "c1", &reference).await,
            StatusCode::CREATED
        );
        let owner = setup.reader("Scoped", Some("c1")).await;
        assert_eq!(
            setup.download(&owner, &reference.id).await.0,
            StatusCode::OK
        );
        let other = setup.reader("Scoped", Some("c2")).await;
        assert_eq!(
            setup.download(&other, &reference.id).await.0,
            StatusCode::NOT_FOUND
        );
    });
}

#[test]
fn tampered_stored_bytes_end_the_stream_in_an_error() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let writer = setup.writer("FiledForCustomer").await;
        let (_, body) = setup
            .upload(
                &writer,
                "?subjectKey=customer&subjectValue=c9",
                vec![5; 100_000],
            )
            .await;
        let reference = reference(&body);
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{}\".blob_bytes SET bytes = substring(bytes FROM 1 FOR 65564)",
            setup.bc
        )))
        .execute(&setup.pool)
        .await
        .unwrap();
        let key_service = key_service();
        let (_, mut stream) = db::read_blob(
            &setup.pool,
            &store(&setup),
            &setup.bc,
            &reference.id,
            Some(&key_service),
        )
        .await
        .unwrap()
        .unwrap();
        let mut failed = false;
        while let Some(chunk) = stream.next().await {
            failed |= chunk.is_err();
        }
        assert!(failed, "a cut-off blob must not read as complete");
    });
}

#[test]
fn collection_takes_orphans_and_spares_what_is_referenced_or_pinned() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let writer = setup.writer("Filed").await;
        let mut references = Vec::new();
        for byte in 0..3u8 {
            let (_, body) = setup.upload(&writer, "", vec![byte]).await;
            references.push(reference(&body));
        }
        let (orphan, referenced, pinned) = (&references[0], &references[1], &references[2]);
        assert_eq!(
            setup.file(&writer, "c1", referenced).await,
            StatusCode::CREATED
        );
        let parked = db::insert_parked_delivery_holding_blobs(
            &setup.pool,
            &setup.bc,
            "kafka:test",
            db::ParkedDeliveryKind::ExternalEvent,
            "t:0:1",
            Some("token"),
            &json!({"payload": {"document": pinned}}),
            "refused",
            1,
            now(),
            now(),
        )
        .await
        .unwrap();

        // All three are past the grace period.
        let later = now() + chrono::Duration::hours(25);
        let cutoff = later - chrono::Duration::hours(24);
        let collected =
            db::collect_blobs(&setup.pool, &store(&setup), &setup.bc, cutoff, later, 100)
                .await
                .unwrap();
        assert_eq!(collected.orphans, 1);
        assert_eq!(collected.bytes_removed, 1);
        let state = |id: String| {
            let pool = setup.pool.clone();
            let bc = setup.bc.clone();
            async move {
                db::get_blob(&pool, &bc, &id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state
                    .unwrap()
                    .as_str()
            }
        };
        assert_eq!(state(orphan.id.clone()).await, "deleted");
        assert_eq!(state(referenced.id.clone()).await, "referenced");
        assert_eq!(state(pinned.id.clone()).await, "pending");
        // A reference to a collected blob is refused.
        assert_eq!(
            setup.file(&writer, "c1", orphan).await,
            StatusCode::BAD_REQUEST
        );

        // The parked delivery goes: a fresh grace period, not instant loss.
        db::delete_parked_delivery(&setup.pool, &setup.bc, &parked.id)
            .await
            .unwrap();
        let collected =
            db::collect_blobs(&setup.pool, &store(&setup), &setup.bc, cutoff, later, 100)
                .await
                .unwrap();
        assert_eq!(collected.orphans, 0);
        assert_eq!(state(pinned.id.clone()).await, "pending");
        let much_later = later + chrono::Duration::hours(25);
        let collected = db::collect_blobs(
            &setup.pool,
            &store(&setup),
            &setup.bc,
            much_later - chrono::Duration::hours(24),
            much_later,
            100,
        )
        .await
        .unwrap();
        assert_eq!(collected.orphans, 1);
        assert_eq!(state(pinned.id.clone()).await, "deleted");
    });
}

#[test]
fn a_deleted_bounded_context_leaves_its_blobs_queued_for_the_store() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let writer = setup.writer("Filed").await;
        let (_, body) = setup.upload(&writer, "", vec![1, 2, 3]).await;
        let reference = reference(&body);
        let record = db::get_blob(&setup.pool, &setup.bc, &reference.id)
            .await
            .unwrap()
            .unwrap();
        db::hard_delete_bounded_context(&setup.pool, &setup.bc)
            .await
            .unwrap();
        let queued: Vec<(String,)> =
            sqlx::query_as("SELECT storage_key FROM blob_deletions WHERE bounded_context = $1")
                .bind(&setup.bc)
                .fetch_all(&setup.pool)
                .await
                .unwrap();
        assert_eq!(queued, vec![(record.storage_key,)]);
        // The Postgres store's bytes went with the schema: nothing left to do.
        db::collect_deleted_contexts(&setup.pool, &store(&setup), 100_000)
            .await
            .unwrap();
        let queued: Vec<(String,)> =
            sqlx::query_as("SELECT storage_key FROM blob_deletions WHERE bounded_context = $1")
                .bind(&setup.bc)
                .fetch_all(&setup.pool)
                .await
                .unwrap();
        assert_eq!(queued, Vec::new());
    });
}

fn idp(jwks_url: &str) -> IdpConfig {
    IdpConfig::new(
        jwks_url.parse().unwrap(),
        TEST_ISSUER,
        TEST_AUDIENCE,
        SigningAlgorithm::Rs256,
    )
}

#[test]
fn blob_fields_registered_over_graphql_declare_and_read_back() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup_with(Some(idp(&serve_jwks().await))).await;
        let jwt = sign_jwt(&setup.mapping.role.external_subject);
        const REGISTER: &str = "\
            mutation($bc: String!, $blobFields: [BlobFieldInput!]) { \
                registerEventType(boundedContext: $bc, name: \"Scanned\", \
                    schema: \"{\\\"properties\\\":{\\\"id\\\":{\\\"type\\\":\\\"string\\\"},\\\"scan\\\":{\\\"type\\\":\\\"object\\\"}}}\", \
                    tagMappings: [], sensitiveFields: [], privateFields: [], blobFields: $blobFields, \
                    externalCreationAllowed: false, directCreationAllowed: true, \
                    systemTriggeredAllowed: false, eventReadAllowed: true) { \
                    blobFields { field subjectKey subjectField } \
                } \
            }";
        let scan = json!({"field": "scan", "subjectKey": "customer", "subjectField": "id"});
        let response = setup
            .graphql(&jwt, REGISTER, json!({"bc": setup.bc, "blobFields": [scan]}))
            .await;
        assert!(response.get("errors").is_none(), "{response}");
        assert_eq!(
            response["data"]["registerEventType"]["blobFields"][0],
            json!({"field": "scan", "subjectKey": "customer", "subjectField": "id"})
        );
        let stored = db::get_event_type(&setup.pool, &setup.bc, "Scanned")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.blob_fields,
            vec![BlobField::with_subject("scan", "customer", "id")]
        );

        // Dropping the subject would store new bytes in plaintext.
        let response = setup
            .graphql(&jwt, REGISTER, json!({"bc": setup.bc, "blobFields": [{"field": "scan"}]}))
            .await;
        assert_eq!(
            response["errors"][0]["extensions"]["code"],
            "invalid_blob_field",
            "{response}"
        );
        // A field the schema doesn't have, and half a subject.
        for bad in [
            json!({"field": "missing"}),
            json!({"field": "scan", "subjectKey": "customer"}),
        ] {
            let response = setup
                .graphql(&jwt, REGISTER, json!({"bc": setup.bc, "blobFields": [bad]}))
                .await;
            assert_eq!(
                response["errors"][0]["extensions"]["code"],
                "invalid_blob_field",
                "{response}"
            );
        }
    });
}

#[test]
fn the_blobs_of_a_read_are_listed_unless_redacted() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup_with(Some(idp(&serve_jwks().await))).await;
        let jwt = sign_jwt(&setup.mapping.role.external_subject);
        const QUERY: &str = "query($bc: String!, $types: [String!]!) { \
            queryEvents(boundedContext: $bc, eventTypes: $types) { \
                blobs { field id sha256 size mediaType } \
            } }";
        let writer = setup.writer("Filed").await;
        let (_, body) = setup.upload(&writer, "", vec![1, 2, 3]).await;
        let plain = reference(&body);
        assert_eq!(setup.file(&writer, "c1", &plain).await, StatusCode::CREATED);
        let response = setup
            .graphql(&jwt, QUERY, json!({"bc": setup.bc, "types": ["Filed"]}))
            .await;
        assert_eq!(
            response["data"]["queryEvents"][0]["blobs"][0],
            json!({
                "field": "document",
                "id": plain.id,
                "sha256": plain.sha256,
                "size": "3",
                "mediaType": "application/pdf",
            }),
            "{response}"
        );

        let writer = setup.writer("FiledPrivately").await;
        let (_, body) = setup.upload(&writer, "", vec![4]).await;
        let private = reference(&body);
        assert_eq!(
            setup.file(&writer, "c1", &private).await,
            StatusCode::CREATED
        );
        let response = setup
            .graphql(
                &jwt,
                QUERY,
                json!({"bc": setup.bc, "types": ["FiledPrivately"]}),
            )
            .await;
        assert_eq!(
            response["data"]["queryEvents"][0]["blobs"],
            json!([]),
            "{response}"
        );
    });
}

#[test]
fn forget_subject_over_graphql_reaches_the_subjects_blobs_only() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup_with(Some(idp(&serve_jwks().await))).await;
        let writer = setup.writer("FiledForCustomer").await;
        let mut references = Vec::new();
        for customer in ["c1", "c2"] {
            let (status, body) = setup
                .upload(
                    &writer,
                    &format!("?subjectKey=customer&subjectValue={customer}"),
                    vec![6; 100],
                )
                .await;
            assert_eq!(status, StatusCode::CREATED);
            let reference = reference(&body);
            assert_eq!(setup.file(&writer, customer, &reference).await, StatusCode::CREATED);
            references.push(reference);
        }
        let record = db::get_blob(&setup.pool, &setup.bc, &references[0].id)
            .await
            .unwrap()
            .unwrap();
        // The subject itself may read its blob, up to the moment it's forgotten.
        let subject_jwt = {
            sqlx::query("UPDATE roles SET external_subject = 'c1' WHERE id = $1")
                .bind(&setup.mapping.role.id)
                .execute(&setup.pool)
                .await
                .unwrap();
            sign_jwt("c1")
        };
        let (status, served) = setup.download(&subject_jwt, &references[0].id).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(served, vec![6; 100]);

        let response = setup
            .graphql(
                &subject_jwt,
                "mutation($bc: String!) { \
                    forgetSubject(boundedContext: $bc, subjectKey: \"customer\", subjectValue: \"c1\") { status } \
                }",
                json!({"bc": setup.bc}),
            )
            .await;
        assert!(response.get("errors").is_none(), "{response}");

        assert_eq!(setup.state(&references[0].id).await, "deleted");
        assert_eq!(setup.state(&references[1].id).await, "referenced");
        assert_eq!(
            setup.download(&subject_jwt, &references[0].id).await.0,
            StatusCode::NOT_FOUND
        );
        assert!(setup.stored_bytes(&record.storage_key).await.is_some());
        assert_eq!(setup.collect_at(now()).await.bytes_removed, 1);
        assert!(setup.stored_bytes(&record.storage_key).await.is_none());
    });
}

#[test]
fn a_proposal_is_refused_when_too_large_or_undeclared_and_pins_its_blob() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let proposals = || {
            let (pool, bc) = (setup.pool.clone(), setup.bc.clone());
            async move {
                let (count,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT count(*) FROM \"bc_{bc}\".command_proposals"
                )))
                .fetch_one(&pool)
                .await
                .unwrap();
                count
            }
        };
        let writer = setup.writer("Filed").await;
        let (_, body) = setup.upload(&writer, "", vec![1]).await;
        let held = reference(&body);
        let token = setup.command_token("ApprovedFiling").await;

        // A reference to nothing this bounded context has.
        let mut unknown = held.clone();
        unknown.id = "no-such-blob".to_string();
        let (status, body) = setup
            .trigger(&token, json!({"customer_id": "c1", "document": unknown}))
            .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(proposals().await, 0);

        // Past the limit.
        db::set_payload_limits(
            &setup.pool,
            &setup.bc,
            db::PayloadLimitsOverride {
                max_payload_bytes: Some(32),
                warn_payload_bytes: None,
            },
        )
        .await
        .unwrap();
        let (status, _) = setup
            .trigger(&token, json!({"customer_id": "c1", "document": held}))
            .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(proposals().await, 0);
        db::set_payload_limits(&setup.pool, &setup.bc, db::PayloadLimitsOverride::default())
            .await
            .unwrap();

        // Stored as a proposal: held past the grace period while it waits.
        let (status, body) = setup
            .trigger(&token, json!({"customer_id": "c1", "document": held}))
            .await;
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
        assert_eq!(proposals().await, 1);
        let later = now() + chrono::Duration::hours(25);
        assert_eq!(setup.collect_at(later).await.orphans, 0);
        assert_eq!(setup.state(&held.id).await, "pending");

        // Decided: a fresh grace period, then collected.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{}\".command_proposals SET status = 'rejected'",
            setup.bc
        )))
        .execute(&setup.pool)
        .await
        .unwrap();
        assert_eq!(setup.collect_at(later).await.orphans, 0);
        let much_later = later + chrono::Duration::hours(25);
        assert_eq!(setup.collect_at(much_later).await.orphans, 1);
        assert_eq!(setup.state(&held.id).await, "deleted");
    });
}

fn inbound_mapping(credential: &str, bc: &str, field: &str) -> skilj_bridge::InboundMapping {
    skilj_bridge::InboundMapping {
        blobs: Some(skilj_bridge::InboundBlobs {
            bounded_context: bc.to_string(),
            fields: vec![skilj_bridge::InboundBlobField {
                field: field.to_string(),
                media_type: "text/plain".to_string(),
                subject: None,
            }],
        }),
        credential: credential.to_string(),
        action: skilj_bridge::InboundAction::Record {
            event_type: "Inbound".to_string(),
        },
    }
}

#[test]
fn a_bridge_against_a_real_skilj_uploads_marked_fields_and_only_those_are_spared_the_limit() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let url = setup.serve().await;
        let credential = setup.external_token("Inbound").await;
        let mapping = inbound_mapping(&credential, &setup.bc, "body");
        let http = reqwest::Client::new();
        let post = |payload: serde_json::Value, sequence: i64| {
            let (http, url, mapping) = (http.clone(), url.clone(), &mapping);
            async move {
                skilj_bridge::post_inbound(
                    &http,
                    &url,
                    mapping,
                    &json!({
                        "payload": payload,
                        "sourceContent": "test",
                        "dedupe": {"partitionKey": "t:0", "sequence": sequence},
                    }),
                    None,
                )
                .await
            }
        };

        // A value far over the payload limit fits, once it is a blob.
        let large = "x".repeat(1024 * 1024);
        let message = json!({"customer_id": "c1", "body": large}).to_string();
        let offloaded =
            skilj_bridge::offload_blob_fields(&http, &url, &mapping, message.as_bytes())
                .await
                .unwrap()
                .expect("the marked field was replaced");
        let payload: serde_json::Value = serde_json::from_slice(&offloaded).unwrap();
        post(payload.clone(), 1).await.unwrap();
        let id = payload["body"]["$blob"].as_str().unwrap();
        assert_eq!(setup.state(id).await, "referenced");
        let reader = setup.reader("Inbound", None).await;
        let (status, served) = setup.download(&reader, id).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(served, large.as_bytes());

        // Over the limit in a field that isn't marked: refused as too large.
        let message = json!({"customer_id": "c1", "body": "y", "note": large}).to_string();
        let offloaded =
            skilj_bridge::offload_blob_fields(&http, &url, &mapping, message.as_bytes())
                .await
                .unwrap()
                .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&offloaded).unwrap();
        match post(payload, 2).await {
            Err(skilj_bridge::SkiljError::SkiljStatus { status, body }) => {
                assert_eq!(status, reqwest::StatusCode::PAYLOAD_TOO_LARGE);
                assert!(skilj_bridge::too_large_refusal(&body), "{body}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }

        // A marked field the type doesn't declare: not a size problem, so it parks.
        let wrong = inbound_mapping(&credential, &setup.bc, "note");
        let message = json!({"customer_id": "c1", "body": null, "note": "z"}).to_string();
        let offloaded = skilj_bridge::offload_blob_fields(&http, &url, &wrong, message.as_bytes())
            .await
            .unwrap()
            .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&offloaded).unwrap();
        match post(payload, 3).await {
            Err(skilj_bridge::SkiljError::SkiljStatus { body, .. }) => {
                assert!(!skilj_bridge::too_large_refusal(&body), "{body}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    });
}

#[test]
fn a_parked_message_keeps_its_blob_past_the_grace_period_and_its_retry_succeeds() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup_with(Some(idp(&serve_jwks().await))).await;
        let url = setup.serve().await;
        let credential = setup.external_token("Inbound").await;
        let mapping = inbound_mapping(&credential, &setup.bc, "body");
        let http = reqwest::Client::new();

        let offloaded = skilj_bridge::offload_blob_fields(
            &http,
            &url,
            &mapping,
            json!({"customer_id": "c1", "body": "the bytes"})
                .to_string()
                .as_bytes(),
        )
        .await
        .unwrap()
        .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&offloaded).unwrap();
        let id = payload["body"]["$blob"].as_str().unwrap().to_string();
        skilj_bridge::report_parked_delivery(
            &http,
            &url,
            &mapping,
            "kafka-inbound",
            "t:0:1",
            &json!({"payload": payload, "sourceContent": "test"}),
            None,
            "refused",
            3,
            now(),
        )
        .await
        .unwrap();

        // Long past its grace period, the parked row still holds it.
        let later = now() + chrono::Duration::days(30);
        assert_eq!(setup.collect_at(later).await.orphans, 0);
        assert_eq!(setup.state(&id).await, "pending");

        // So the retry, which replays inside skilj, finds it.
        let parked = db::list_parked_deliveries(&setup.pool, &setup.bc)
            .await
            .unwrap();
        assert_eq!(parked.len(), 1);
        let jwt = sign_jwt(&setup.mapping.role.external_subject);
        let response = setup
            .graphql(
                &jwt,
                "mutation($bc: String!, $id: String!) { \
                    retryParkedDelivery(boundedContext: $bc, id: $id) { id } \
                }",
                json!({"bc": setup.bc, "id": parked[0].id}),
            )
            .await;
        assert!(response.get("errors").is_none(), "{response}");
        assert_eq!(setup.state(&id).await, "referenced");
        assert_eq!(setup.collect_at(later).await.orphans, 0);
    });
}

/// Uploads a blob with a subject that fills its frames exactly, so reading
/// the last frame doesn't happen to run into the end of the stream.
async fn sealed_exact_frames(setup: &Setup) -> BlobRef {
    let writer = setup.writer("FiledForCustomer").await;
    let (status, body) = setup
        .upload(
            &writer,
            "?subjectKey=customer&subjectValue=c7",
            vec![8; 2 * skilj_core::encryption::BLOB_FRAME_PLAINTEXT],
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    reference(&body)
}

/// Every chunk of a blob's download, and whether any was an error.
async fn read_to_the_end(setup: &Setup, id: &str) -> (usize, bool) {
    let (_, mut stream) = db::read_blob(
        &setup.pool,
        &store(setup),
        &setup.bc,
        id,
        Some(&key_service()),
    )
    .await
    .unwrap()
    .unwrap();
    let (mut bytes, mut failed) = (0, false);
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => bytes += chunk.len(),
            Err(_) => failed = true,
        }
    }
    (bytes, failed)
}

#[test]
fn an_encrypted_download_checks_the_hash_even_when_frames_end_exactly() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let reference = sealed_exact_frames(&setup).await;
        assert_eq!(
            read_to_the_end(&setup, &reference.id).await,
            (2 * skilj_core::encryption::BLOB_FRAME_PLAINTEXT, false)
        );
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{}\".blobs SET sha256 = 'not-the-hash'",
            setup.bc
        )))
        .execute(&setup.pool)
        .await
        .unwrap();
        assert!(read_to_the_end(&setup, &reference.id).await.1);
    });
}

#[test]
fn an_encrypted_download_refuses_bytes_after_its_last_frame() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let reference = sealed_exact_frames(&setup).await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE \"bc_{}\".blob_bytes SET bytes = bytes || '\\x00'::bytea",
            setup.bc
        )))
        .execute(&setup.pool)
        .await
        .unwrap();
        assert!(read_to_the_end(&setup, &reference.id).await.1);
    });
}

/// A failure writing the pin: what is left must be nothing, and a retry
/// must end up with both the holder and its pin.
async fn fail_pins(setup: &Setup) {
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION \"bc_{bc}\".refuse_pin() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN RAISE EXCEPTION 'no pins'; END $$; \
         CREATE TRIGGER refuse_pin BEFORE INSERT ON \"bc_{bc}\".blob_pins \
         FOR EACH ROW EXECUTE FUNCTION \"bc_{bc}\".refuse_pin()",
        bc = setup.bc
    )))
    .execute(&setup.pool)
    .await
    .unwrap();
}

async fn allow_pins(setup: &Setup) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TRIGGER refuse_pin ON \"bc_{}\".blob_pins",
        setup.bc
    )))
    .execute(&setup.pool)
    .await
    .unwrap();
}

async fn count_rows(setup: &Setup, table: &str) -> i64 {
    let (count,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"bc_{}\".{table}",
        setup.bc
    )))
    .fetch_one(&setup.pool)
    .await
    .unwrap();
    count
}

#[test]
fn a_proposal_that_cannot_pin_its_blob_is_not_stored() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let writer = setup.writer("Filed").await;
        let (_, body) = setup.upload(&writer, "", vec![1]).await;
        let held = reference(&body);
        let token = setup.command_token("ApprovedFiling").await;
        let payload = json!({"customer_id": "c1", "document": held});

        fail_pins(&setup).await;
        let (status, _) = setup.trigger(&token, payload.clone()).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(count_rows(&setup, "command_proposals").await, 0);

        allow_pins(&setup).await;
        let (status, _) = setup.trigger(&token, payload).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(count_rows(&setup, "command_proposals").await, 1);
        assert_eq!(count_rows(&setup, "blob_pins").await, 1);
    });
}

#[test]
fn a_parked_delivery_that_cannot_pin_its_blob_is_not_stored() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let url = setup.serve().await;
        let credential = setup.external_token("Inbound").await;
        let mapping = inbound_mapping(&credential, &setup.bc, "body");
        let http = reqwest::Client::new();
        let offloaded = skilj_bridge::offload_blob_fields(
            &http,
            &url,
            &mapping,
            json!({"customer_id": "c1", "body": "x"})
                .to_string()
                .as_bytes(),
        )
        .await
        .unwrap()
        .unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&offloaded).unwrap();
        let report = || {
            let (http, url, mapping, payload) = (&http, &url, &mapping, &payload);
            async move {
                skilj_bridge::report_parked_delivery(
                    http,
                    url,
                    mapping,
                    "kafka-inbound",
                    "t:0:1",
                    &json!({"payload": payload, "sourceContent": "test"}),
                    None,
                    "refused",
                    3,
                    now(),
                )
                .await
            }
        };

        fail_pins(&setup).await;
        assert!(report().await.is_err());
        assert_eq!(count_rows(&setup, "parked_deliveries").await, 0);

        allow_pins(&setup).await;
        report().await.unwrap();
        assert_eq!(count_rows(&setup, "parked_deliveries").await, 1);
        assert_eq!(count_rows(&setup, "blob_pins").await, 1);
    });
}
