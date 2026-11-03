//! Blob references end to end (issue #132, docs/architecture.md §239):
//! a blob uploaded through the Postgres store comes back byte for byte;
//! an event whose declared blob field references it is stored and marks
//! it referenced, with the reference recorded; a reference to a blob the
//! bounded context doesn't have, or with another hash, size or media type,
//! or in a field the type doesn't declare, is refused and nothing is
//! written; a command and the event its `decide()` returns can both carry
//! references; and `build()` refuses a blob field overlapping a sensitive
//! one. `blob_lifecycle.rs` covers the REST routes, encryption and erasure.
//! Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `payload_validation.rs`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::blob::{BlobField, BlobRef, BlobState, DEFAULT_MAX_BLOB_BYTES};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, PostgresBlobStore};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, generate_token_secret, CommandDecision, EventSpec};
use tower::ServiceExt;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct ContractSignedPayload {
    contract_id: String,
    document: BlobRef,
    #[serde(default)]
    attachments: Vec<BlobRef>,
}

struct ContractSigned;

impl EventType for ContractSigned {
    type Payload = ContractSignedPayload;
    const NAME: &'static str = "ContractSigned";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn blob_fields() -> Vec<BlobField> {
        vec![BlobField::new("document"), BlobField::new("attachments")]
    }
}

/// Carries a reference in a field it doesn't declare.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct NoteTakenPayload {
    note: serde_json::Value,
}

struct NoteTaken;

impl EventType for NoteTaken {
    type Payload = NoteTakenPayload;
    const NAME: &'static str = "NoteTaken";
    fn direct_creation_allowed() -> bool {
        true
    }
}

enum ContractsEvent {
    #[allow(dead_code)]
    ContractSigned(ContractSignedPayload),
}

impl BoundedContextEvent for ContractsEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "ContractSigned" => {
                Some(serde_json::from_str(&event.payload).map(ContractsEvent::ContractSigned))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct SignContractPayload {
    contract_id: String,
    document: BlobRef,
}

struct SignContract;

impl CommandType for SignContract {
    type Payload = SignContractPayload;
    type Event = ContractsEvent;
    const NAME: &'static str = "SignContract";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn blob_fields() -> Vec<BlobField> {
        vec![BlobField::new("document")]
    }
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "ContractSigned".to_string(),
                payload: serde_json::json!({
                    "contract_id": payload.contract_id,
                    "document": payload.document,
                }),
            }],
        }
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

static TEST_DB: tokio::sync::OnceCell<Option<String>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for blob_references tests")
    })
}

async fn test_db() -> Option<String> {
    TEST_DB.get_or_init(provision).await.clone()
}

async fn provision() -> Option<String> {
    let database_url = skilj_test_support::database_url("skilj_blob_references_test").await?;
    let pool = match db::connect(&database_url).await {
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
    Some(database_url)
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

struct Setup {
    skilj: Skilj,
    pool: Pool,
    bc_name: String,
    contract_credential: String,
    note_credential: String,
    command_credential: String,
}

async fn setup() -> Setup {
    let database_url = test_db().await.expect("caller already checked");
    let pool = db::connect(&database_url).await.unwrap();

    let external_subject = unique_name("subject");
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.clone(),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role(&pool, &role).await.unwrap();

    let bc_name = unique_name("contracts");
    let bc = BoundedContext {
        name: bc_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(&pool, &bc).await.unwrap();

    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        scope: None,
        capabilities: Default::default(),
        delegable: Default::default(),
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(&pool, &mapping)
        .await
        .unwrap();

    let (skilj, report) = Skilj::builder(database_url)
        .pool_options(db::PgPoolOptions::new().max_connections(3))
        .bounded_context(bc_name.clone())
        .event_type::<ContractSigned>()
        .event_type::<NoteTaken>()
        .command_type::<SignContract>()
        .reconciliation_role(external_subject)
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let mut credentials = Vec::new();
    for name in ["ContractSigned", "NoteTaken"] {
        let event_type = db::get_event_type(&pool, &bc_name, name)
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &token)
            .await
            .unwrap();
        credentials.push(format!("{}.{}", token.id, token.secret));
    }
    let command_type = db::get_command_type(&pool, &bc_name, "SignContract")
        .await
        .unwrap()
        .unwrap();
    let command = access_control::create_command_token(
        &mapping,
        &command_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        test_now(),
    )
    .unwrap();
    db::insert_command_token(&pool, &command).await.unwrap();

    Setup {
        skilj,
        pool,
        bc_name,
        note_credential: credentials.pop().unwrap(),
        contract_credential: credentials.pop().unwrap(),
        command_credential: format!("{}.{}", command.id, command.secret),
    }
}

async fn upload(setup: &Setup, bytes: &[u8]) -> BlobRef {
    let store = PostgresBlobStore::new(setup.pool.clone());
    // Two chunks, so the store hashes across a boundary.
    let (a, b) = bytes.split_at(bytes.len() / 2);
    let chunks = vec![Ok(a.to_vec()), Ok(b.to_vec())];
    db::upload_blob(
        &setup.pool,
        &store,
        &setup.bc_name,
        "application/pdf",
        futures_util::stream::iter(chunks).boxed(),
        DEFAULT_MAX_BLOB_BYTES,
        None,
        test_now(),
    )
    .await
    .unwrap()
}

async fn post(
    setup: &Setup,
    uri: &str,
    credential: &str,
    payload: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::json!({ "payload": payload }).to_string(),
        ))
        .unwrap();
    let response = setup.skilj.rest_router().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or_default())
}

async fn sign(setup: &Setup, document: serde_json::Value) -> (StatusCode, serde_json::Value) {
    post(
        setup,
        "/v1/events/direct",
        &setup.contract_credential,
        serde_json::json!({ "contract_id": "c-1", "document": document }),
    )
    .await
}

async fn events(setup: &Setup) -> Vec<Event> {
    db::list_events_for_bounded_context(&setup.pool, &setup.bc_name)
        .await
        .unwrap()
}

async fn state(setup: &Setup, id: &str) -> Option<BlobState> {
    db::get_blob(&setup.pool, &setup.bc_name, id)
        .await
        .unwrap()
        .unwrap()
        .state
}

#[test]
fn an_uploaded_blob_reads_back_byte_for_byte_and_its_reference_is_recorded() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let bytes: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        let reference = upload(&setup, &bytes).await;
        assert_eq!(reference.size, bytes.len() as u64);
        assert_eq!(state(&setup, &reference.id).await, Some(BlobState::Pending));

        let store = PostgresBlobStore::new(setup.pool.clone());
        let (served, stream) =
            db::read_blob(&setup.pool, &store, &setup.bc_name, &reference.id, None)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(served, reference);
        let read: Vec<u8> = stream
            .map(|chunk| chunk.unwrap())
            .collect::<Vec<_>>()
            .await
            .concat();
        assert_eq!(read, bytes);

        let (status, body) = sign(&setup, serde_json::to_value(&reference).unwrap()).await;
        assert!(status.is_success(), "{status} {body}");
        assert_eq!(
            state(&setup, &reference.id).await,
            Some(BlobState::Referenced)
        );
        let stored = events(&setup).await;
        assert_eq!(
            db::blob_references(&setup.pool, &setup.bc_name, &reference.id)
                .await
                .unwrap(),
            vec![(Some(stored[0].sequence), None, "document".to_string())]
        );
        // The event carries the reference, never the bytes.
        let payload: ContractSignedPayload = serde_json::from_str(&stored[0].payload).unwrap();
        assert_eq!(payload.document, reference);
    });
}

#[test]
fn a_reference_that_does_not_match_a_stored_blob_is_refused_and_nothing_is_written() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let reference = upload(&setup, b"%PDF-1.7 a contract").await;

        let unknown = BlobRef {
            id: generate_token_id(),
            ..reference.clone()
        };
        let wrong_hash = BlobRef {
            sha256: "00".repeat(32),
            ..reference.clone()
        };
        let wrong_size = BlobRef {
            size: reference.size + 1,
            ..reference.clone()
        };
        let wrong_type = BlobRef {
            media_type: "image/png".to_string(),
            ..reference.clone()
        };
        for bad in [unknown, wrong_hash, wrong_size, wrong_type] {
            let (status, body) = sign(&setup, serde_json::to_value(&bad).unwrap()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(body["code"], "blob_reference_invalid", "{body}");
        }

        // A reference in a field the type doesn't declare.
        let (status, body) = post(
            &setup,
            "/v1/events/direct",
            &setup.note_credential,
            serde_json::json!({ "note": { "inner": reference } }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "blob_reference_invalid", "{body}");

        assert!(events(&setup).await.is_empty());
        assert_eq!(state(&setup, &reference.id).await, Some(BlobState::Pending));
        assert!(
            db::blob_references(&setup.pool, &setup.bc_name, &reference.id)
                .await
                .unwrap()
                .is_empty()
        );

        // A list of references, declared by the list's field.
        let second = upload(&setup, b"an annex").await;
        let (status, body) = post(
            &setup,
            "/v1/events/direct",
            &setup.contract_credential,
            serde_json::json!({
                "contract_id": "c-2",
                "document": reference,
                "attachments": [second],
            }),
        )
        .await;
        assert!(status.is_success(), "{status} {body}");
        assert_eq!(events(&setup).await[0].sequence, 0);
        assert_eq!(state(&setup, &second.id).await, Some(BlobState::Referenced));
    });
}

#[test]
fn a_command_and_the_event_it_decides_both_record_their_references() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let reference = upload(&setup, b"%PDF-1.7 signed").await;

        let (status, body) = post(
            &setup,
            "/v1/commands/trigger",
            &setup.command_credential,
            serde_json::json!({ "contract_id": "c-3", "document": reference }),
        )
        .await;
        assert!(status.is_success(), "{status} {body}");
        assert_eq!(body["accepted"], serde_json::json!(true), "{body}");

        let references = db::blob_references(&setup.pool, &setup.bc_name, &reference.id)
            .await
            .unwrap();
        assert_eq!(references.len(), 2, "{references:?}");
        assert!(references[0].0.is_none() && references[0].1.is_some());
        assert_eq!(references[1].0, Some(0));
        assert_eq!(
            state(&setup, &reference.id).await,
            Some(BlobState::Referenced)
        );

        let unknown = BlobRef {
            id: generate_token_id(),
            ..reference
        };
        let (status, body) = post(
            &setup,
            "/v1/commands/trigger",
            &setup.command_credential,
            serde_json::json!({ "contract_id": "c-4", "document": unknown }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(body["code"], "blob_reference_invalid");
        assert_eq!(events(&setup).await.len(), 1);
    });
}

#[test]
fn an_upload_past_the_blob_limit_is_refused_and_leaves_nothing_behind() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let store = PostgresBlobStore::new(setup.pool.clone());
        let err = db::upload_blob(
            &setup.pool,
            &store,
            &setup.bc_name,
            "application/octet-stream",
            futures_util::stream::iter(vec![Ok(vec![0u8; 64])]).boxed(),
            16,
            None,
            test_now(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            skilj_core::Error::EventStore(skilj_core::event_store::Error::BlobTooLarge {
                limit: 16
            })
        ));
    });
}

struct SensitiveBlob;

impl EventType for SensitiveBlob {
    type Payload = ContractSignedPayload;
    const NAME: &'static str = "SensitiveBlob";
    fn sensitive_fields() -> Vec<skilj_core::shared::SensitiveField> {
        vec![skilj_core::shared::SensitiveField::new(
            "document",
            "contract",
            "contract_id",
        )]
    }
    fn blob_fields() -> Vec<BlobField> {
        vec![BlobField::new("document")]
    }
}

#[test]
fn build_refuses_a_blob_field_that_is_also_sensitive() {
    runtime().block_on(async {
        let Some(database_url) = test_db().await else {
            return;
        };
        let err = Skilj::builder(database_url)
            .pool_options(db::PgPoolOptions::new().max_connections(2))
            .bounded_context(unique_name("contracts"))
            .event_type::<SensitiveBlob>()
            .build()
            .await
            .err()
            .expect("build() must refuse");
        assert!(
            matches!(
                err,
                skilj_core::Error::EventStore(skilj_core::event_store::Error::InvalidBlobField(_))
            ),
            "{err:?}"
        );
    });
}
