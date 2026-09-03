//! End-to-end tests for `POST /v1/commands/trigger` (docs/architecture.md
//! §8 item 4) - a real HTTP request, through `Skilj::rest_router()`,
//! through the `Arc<dyn CommandDispatcher>` bridge, into a real
//! `decide()`, and back out through `process_command`'s persistence.
//! Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj-core/tests/persistence.rs`/`skilj/tests/reconciliation.rs` -
//! see either's own doc comment for the details, not repeated a third
//! time here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{requires_role, CommandType, EventType, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{
    generate_token_id, generate_token_secret, CommandDecision, EventSpec, TagMapping,
};
use tower::ServiceExt;

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
    // Never read - decide() below doesn't consult matching_events -
    // built only to satisfy CommandType::Event's own BoundedContextEvent
    // bound.
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
    fn rest_trigger_allowed() -> bool {
        true
    }
    /// A real, non-empty `tag_mappings` entry - the previously-panicking
    /// `derive_tags` path (`todo!()` for any non-empty `tag_mappings`),
    /// now exercised for real over the actual REST surface, not just at
    /// the pure-function layer. See `command_trigger_derives_real_tags_
    /// from_a_real_tag_mapping` below.
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping {
            key: "amount".into(),
            field: "amount".into(),
        }]
    }
    /// Deliberately simple: rejects anything over 1000, otherwise emits
    /// one `MoneyDeposited` event carrying the same amount - just enough
    /// behaviour to exercise both `CommandTrigger` outcomes (§5.4/§7.3)
    /// without needing a second registered command type.
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

/// Registered but never actually triggered over REST in these tests
/// (`rest_trigger_allowed` defaults to `false`, left unset here) - exists
/// solely so `command_dispatcher().required_role(...)` has a real,
/// reconciled command type to look up. `#[requires_role(...)]` (§1.3.1)
/// is the whole point of this fixture.
struct CloseAccount;

#[requires_role("treasury_officer")]
impl CommandType for CloseAccount {
    type Payload = WithdrawPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "CloseAccount";
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
    pool: Pool,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for command_trigger tests")
    })
}

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| (db.database_url.clone(), db.pool.clone()))
}

async fn connect_and_migrate(database_url: &str, label: &str) -> Option<Pool> {
    let pool = match db::connect(database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to {label} failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating {label} failed: {e}");
        return None;
    }
    Some(pool)
}

async fn provision() -> Option<TestDb> {
    let database_url = if let Ok(database_url) = std::env::var("DATABASE_URL") {
        database_url
    } else {
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
        let database_name = "skilj_command_trigger_test";
        if let Err(e) = server.create_database(database_name).await {
            eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
            return None;
        }
        let url = server.settings().url(database_name);
        let pool = connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            pool,
            _embedded: Some(server),
        });
    };

    let pool = connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb {
        database_url,
        pool,
        _embedded: None,
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

/// Builds a fully reconciled `Skilj` (both `MoneyDeposited`/`WithdrawMoney`
/// registered) plus a real, minted `CommandToken` for `WithdrawMoney` -
/// everything a `POST /v1/commands/trigger` test needs to send a real
/// request against a real router.
async fn setup() -> (Skilj, String, Pool, String) {
    let (database_url, pool) = test_db()
        .await
        .expect("test_db() must be Some - caller already checked");

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

    let bc_name = unique_name("banking");
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
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(&pool, &mapping)
        .await
        .unwrap();

    let (skilj, report) = Skilj::builder(database_url)
        .bounded_context(bc_name.clone())
        .event_type::<MoneyDeposited>()
        .command_type::<WithdrawMoney>()
        .command_type::<CloseAccount>()
        .reconciliation_role(external_subject)
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let command_type = db::get_command_type(&pool, &bc_name, "WithdrawMoney")
        .await
        .unwrap()
        .unwrap();
    let token = access_control::create_command_token(
        &mapping,
        &command_type,
        generate_token_id(),
        generate_token_secret(),
        test_now(),
    )
    .unwrap();
    db::insert_command_token(&pool, &token).await.unwrap();

    let credential = format!("{}.{}", token.id, token.secret);
    (skilj, credential, pool, bc_name)
}

#[test]
fn command_trigger_accepts_and_persists_triggered_events() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"amount":20}}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["accepted"], true);
        let sequences = json["triggeredEventSequences"]
            .as_array()
            .expect("triggeredEventSequences must be present and an array when accepted");
        assert_eq!(sequences.len(), 1);

        // The highest-risk new code path this pass added: reading a
        // stored `command_triggered` origin back reconstructs the whole
        // embedded `Command` correctly, not just the Event's own fields.
        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0].origin {
            skilj_core::event_store::EventOrigin::CommandTriggered { command } => {
                assert_eq!(command.command_type.name, "WithdrawMoney");
                assert_eq!(command.payload, r#"{"amount":20}"#);
            }
            other => panic!("expected a CommandTriggered origin, got {other:?}"),
        }
    });
}

/// Codeberg issue #12, real end-to-end: a genuine retry with the same
/// `Idempotency-Key` header returns the identical `triggeredEventSequences`
/// and `deduplicated: true`, and only one set of events actually exists -
/// not just that the response looks right, but that nothing was
/// double-applied.
#[test]
fn command_trigger_deduplicates_a_repeated_idempotency_key() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name) = setup().await;
        let router = skilj.rest_router();

        let request = || {
            Request::builder()
                .method("POST")
                .uri("/v1/commands/trigger")
                .header("authorization", format!("Bearer {credential}"))
                .header("content-type", "application/json")
                .header("Idempotency-Key", "retry-1")
                .body(Body::from(r#"{"payload":{"amount":20}}"#))
                .unwrap()
        };

        let first_response = router.clone().oneshot(request()).await.unwrap();
        assert_eq!(first_response.status(), StatusCode::OK);
        let first_body = first_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["accepted"], true);
        assert_eq!(first_json["deduplicated"], false);
        let first_sequences = first_json["triggeredEventSequences"].clone();

        let second_response = router.oneshot(request()).await.unwrap();
        assert_eq!(second_response.status(), StatusCode::OK);
        let second_body = second_response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["accepted"], true);
        assert_eq!(
            second_json["deduplicated"], true,
            "a repeated Idempotency-Key must be reported as deduplicated"
        );
        assert_eq!(
            second_json["triggeredEventSequences"], first_sequences,
            "a dedup hit must return the *original* sequences, not a fresh decision"
        );

        // Only one event actually exists - the second request inserted
        // nothing.
        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "a deduplicated retry must not double-apply the command"
        );
    });
}

/// Real end-to-end proof of the drift audit's #10 fix (`valid_payload` -
/// see project memory `skilj-drift-audit-2026-08-18`): the request body
/// is decoded as generic JSON at the wire layer (`CommandTriggerRequest.
/// payload: serde_json::Value`), so a wrong-typed field like this one
/// reaches `authorise_command_trigger`'s own new `valid_payload` guard
/// rather than failing to deserialize earlier - proving the rejection is
/// real over the actual REST surface, not just at the pure-function
/// layer `skilj-core/tests/command_processing.rs` already covers.
#[test]
fn command_trigger_rejects_a_payload_that_does_not_match_the_schema_with_400() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, _pool, _bc_name) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"amount":"not a number"}}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "payload_does_not_match_schema");
    });
}

/// Proof, over the real REST surface (not just `skilj-core`'s pure
/// `derive_tags` unit tests), that a bounded context registering a real
/// `tag_mappings` entry and then triggering a matching command no longer
/// panics - `WithdrawMoney::tag_mappings()` above declares a real
/// mapping, and `db::insert_command_and_events`'s round trip is what
/// `process_command`'s own `derive_tags` call feeds into `Command.
/// consistency_tags`/`Event.tags` alike.
#[test]
fn command_trigger_derives_real_tags_from_a_real_tag_mapping() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"amount":20}}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        match &events[0].origin {
            skilj_core::event_store::EventOrigin::CommandTriggered { command } => {
                assert_eq!(
                    command.consistency_tags,
                    vec![skilj_core::shared::Tag {
                        key: "amount".into(),
                        value: Some("20".into()),
                    }]
                );
            }
            other => panic!("expected a CommandTriggered origin, got {other:?}"),
        }
    });
}

#[test]
fn command_trigger_returns_a_rejection_as_200_not_an_error() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, _pool, _bc_name) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"amount":5000}}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["accepted"], false);
        assert_eq!(json["rejectionKind"], "insufficient_funds");
        assert!(json.get("triggeredEventSequences").is_none());
    });
}

#[test]
fn command_trigger_rejects_a_malformed_credential() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _credential, _pool, _bc_name) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", "Bearer not-a-real-credential")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"amount":20}}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    });
}

/// `CommandDispatcher::required_role` (§1.3.1) after a real reconciliation
/// pass - not exercised over HTTP, since REST triggering never consults
/// it (`CommandToken` is its own, separate grant); this is what
/// `skilj-graphql`'s eventual mutation resolver will call before
/// dispatching (§8 item 5).
#[test]
fn command_dispatcher_required_role_reflects_the_requires_role_attribute() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _credential, _pool, bc_name) = setup().await;
        let dispatcher = skilj.command_dispatcher();

        // #[requires_role("treasury_officer")] on CloseAccount...
        assert_eq!(
            dispatcher.required_role(&bc_name, "CloseAccount"),
            Some(Some("treasury_officer"))
        );
        // ...vs. WithdrawMoney, registered with no attribute at all -
        // still `Some`, since it's a real registered command type, but
        // no extra gate.
        assert_eq!(
            dispatcher.required_role(&bc_name, "WithdrawMoney"),
            Some(None)
        );
        // A name that was never registered at all - the outer `None`,
        // mirroring `dispatch`'s own "pair doesn't match anything"
        // convention.
        assert_eq!(dispatcher.required_role(&bc_name, "NoSuchCommand"), None);
    });
}
