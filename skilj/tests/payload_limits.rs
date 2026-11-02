//! Payload size limits end to end (issue #132, docs/architecture.md §238):
//! the builder's limit refuses an oversized payload over REST with 413 and
//! `payload_too_large`, for a direct event, a command's own payload and an
//! event a command's `decide()` returns; nothing is written, and the
//! sequence stays gapless; a bounded context's stored override replaces
//! the builder's limit for the next write, and clearing it brings the
//! builder's back. Same `DATABASE_URL`-then-embedded-Postgres-then-skip
//! harness as `payload_validation.rs`.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, PayloadLimitsOverride, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::payload_limits::PayloadLimits;
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, generate_token_secret, CommandDecision, EventSpec};
use tower::ServiceExt;

/// The builder's limit in these tests. The JSON around a note adds a few
/// bytes, so a note of `LIMIT` characters is always over it.
const LIMIT: usize = 1024;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct NoteAddedPayload {
    note: String,
}

struct NoteAdded;

impl EventType for NoteAdded {
    type Payload = NoteAddedPayload;
    const NAME: &'static str = "NoteAdded";
    fn direct_creation_allowed() -> bool {
        true
    }
}

enum NotesEvent {
    #[allow(dead_code)]
    NoteAdded(NoteAddedPayload),
}

impl BoundedContextEvent for NotesEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "NoteAdded" => Some(serde_json::from_str(&event.payload).map(NotesEvent::NoteAdded)),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct AddNotePayload {
    note: String,
    /// The event's note is `note` repeated this many times: a small
    /// command can decide a large event.
    repeat: usize,
}

struct AddNote;

impl CommandType for AddNote {
    type Payload = AddNotePayload;
    type Event = NotesEvent;
    const NAME: &'static str = "AddNote";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "NoteAdded".to_string(),
                payload: serde_json::json!({ "note": payload.note.repeat(payload.repeat) }),
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
            .expect("failed to build a tokio runtime for payload_limits tests")
    })
}

async fn test_db() -> Option<String> {
    TEST_DB.get_or_init(provision).await.clone()
}

async fn provision() -> Option<String> {
    let database_url = skilj_test_support::database_url("skilj_payload_limits_test").await?;
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
    direct_credential: String,
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

    let bc_name = unique_name("notes");
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
        .event_type::<NoteAdded>()
        .command_type::<AddNote>()
        .payload_limits(PayloadLimits::new(LIMIT).with_warning(Some(LIMIT / 2)))
        .reconciliation_role(external_subject)
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let event_type = db::get_event_type(&pool, &bc_name, "NoteAdded")
        .await
        .unwrap()
        .unwrap();
    let direct = access_control::create_direct_creation_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        test_now(),
    )
    .unwrap();
    db::insert_direct_creation_token(&pool, &direct)
        .await
        .unwrap();

    let command_type = db::get_command_type(&pool, &bc_name, "AddNote")
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
        direct_credential: format!("{}.{}", direct.id, direct.secret),
        command_credential: format!("{}.{}", command.id, command.secret),
    }
}

async fn post(
    skilj: &Skilj,
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
    let response = skilj.rest_router().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or_default())
}

async fn direct(setup: &Setup, note_len: usize) -> (StatusCode, serde_json::Value) {
    post(
        &setup.skilj,
        "/v1/events/direct",
        &setup.direct_credential,
        serde_json::json!({ "note": "x".repeat(note_len) }),
    )
    .await
}

async fn events(setup: &Setup) -> Vec<Event> {
    db::list_events_for_bounded_context(&setup.pool, &setup.bc_name)
        .await
        .unwrap()
}

#[test]
fn a_direct_event_over_the_limit_is_refused_with_413_and_not_stored() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;

        let (status, body) = direct(&setup, LIMIT).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert_eq!(body["code"], "payload_too_large");
        assert!(events(&setup).await.is_empty());

        // Over the warning threshold but under the limit: stored.
        let (status, body) = direct(&setup, LIMIT * 3 / 4).await;
        assert!(status.is_success(), "{status} {body}");
        let stored = events(&setup).await;
        assert_eq!(stored.len(), 1);
        // The refusal took no sequence number.
        assert_eq!(stored[0].sequence, 0);
    });
}

#[test]
fn a_command_whose_payload_or_decided_event_is_over_the_limit_is_refused() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        let trigger = |note: String, repeat: usize| {
            post(
                &setup.skilj,
                "/v1/commands/trigger",
                &setup.command_credential,
                serde_json::json!({ "note": note, "repeat": repeat }),
            )
        };

        // The command's own payload.
        let (status, body) = trigger("x".repeat(LIMIT), 0).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert_eq!(body["code"], "payload_too_large");

        // A small command whose decide() returns a large event.
        let (status, body) = trigger("x".repeat(64), LIMIT / 64 + 1).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
        assert_eq!(body["code"], "payload_too_large");

        assert!(events(&setup).await.is_empty());
        assert!(
            db::list_commands_for_bounded_context(&setup.pool, &setup.bc_name)
                .await
                .unwrap()
                .is_empty()
        );

        let (status, body) = trigger("x".repeat(64), 2).await;
        assert!(status.is_success(), "{status} {body}");
        assert_eq!(body["accepted"], serde_json::json!(true), "{body}");
        assert_eq!(events(&setup).await[0].sequence, 0);
    });
}

#[test]
fn a_bounded_contexts_override_replaces_the_builders_limit_until_cleared() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let setup = setup().await;
        assert_eq!(
            db::payload_limits_override(&setup.pool, &setup.bc_name)
                .await
                .unwrap(),
            PayloadLimitsOverride::default()
        );

        let raised = PayloadLimitsOverride {
            max_payload_bytes: Some((LIMIT * 4) as i64),
            warn_payload_bytes: None,
        };
        db::set_payload_limits(&setup.pool, &setup.bc_name, raised)
            .await
            .unwrap();
        assert_eq!(
            db::payload_limits_override(&setup.pool, &setup.bc_name)
                .await
                .unwrap(),
            raised
        );
        let (status, body) = direct(&setup, LIMIT * 2).await;
        assert!(status.is_success(), "{status} {body}");

        let lowered = PayloadLimitsOverride {
            max_payload_bytes: Some(16),
            warn_payload_bytes: Some(0),
        };
        db::set_payload_limits(&setup.pool, &setup.bc_name, lowered)
            .await
            .unwrap();
        let (status, _) = direct(&setup, 32).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

        db::set_payload_limits(
            &setup.pool,
            &setup.bc_name,
            PayloadLimitsOverride::default(),
        )
        .await
        .unwrap();
        let (status, _) = direct(&setup, 32).await;
        assert!(status.is_success());
        let (status, _) = direct(&setup, LIMIT * 2).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    });
}

#[test]
fn only_an_active_superadmin_may_set_an_override_and_never_a_negative_one() {
    let role = |superadmin: bool| Role {
        id: generate_token_id(),
        external_subject: unique_name("subject"),
        name: "Someone".to_string(),
        superadmin,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    let fine = PayloadLimitsOverride {
        max_payload_bytes: Some(1),
        warn_payload_bytes: None,
    };
    assert!(skilj_core::bootstrap::set_payload_limits(&role(true), &fine).is_ok());
    assert!(skilj_core::bootstrap::set_payload_limits(&role(false), &fine).is_err());
    let negative = PayloadLimitsOverride {
        max_payload_bytes: None,
        warn_payload_bytes: Some(-1),
    };
    let err = skilj_core::bootstrap::set_payload_limits(&role(true), &negative).unwrap_err();
    assert!(matches!(
        err,
        skilj_core::Error::Bootstrap(skilj_core::bootstrap::Error::InvalidPayloadLimit)
    ));
}
