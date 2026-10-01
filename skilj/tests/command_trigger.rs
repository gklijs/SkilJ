//! End-to-end tests for `POST /v1/commands/trigger` (docs/architecture.md
//! [§8](../../docs/architecture.md#open-for-a-future-pass) item 4) - a real HTTP request, through `Skilj::rest_router()`,
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
    /// Cross-tenant write fix (docs/architecture.md's own write-up of
    /// these passes) - reuses the existing `amount` tag mapping as the
    /// owner dimension, purely so `command_trigger_rejects_...`/
    /// `command_trigger_succeeds_when_...` below have a real declared
    /// owner to test a scoped `CommandToken` against, over the actual
    /// REST surface. Every *other* test in this file mints its token
    /// with `scope: None` (`setup()`'s own default), so this is
    /// vacuously true for them - no behaviour change to anything already
    /// passing here.
    fn owner_tag_key() -> Option<&'static str> {
        Some("amount")
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
    let database_url = skilj_test_support::database_url("skilj_command_trigger_test").await?;

    let pool = connect_and_migrate(&database_url, "the test database").await?;
    Some(TestDb { database_url, pool })
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
/// request against a real router. The trailing `RoleAccessMapping`/
/// `CommandType` are for callers that need to mint a second,
/// differently-scoped token of their own (cross-tenant write fix,
/// docs/architecture.md's own write-up of these passes) - every other
/// test in this file only destructures the first four.
async fn setup() -> (
    Skilj,
    String,
    Pool,
    String,
    RoleAccessMapping,
    skilj_core::event_store::CommandType,
) {
    setup_with_pool(skilj_core::db::PgPoolOptions::new().max_connections(4)).await
}

/// [`setup`] with the `Skilj`'s own pool options chosen by the caller.
async fn setup_with_pool(
    pool_options: skilj_core::db::PgPoolOptions,
) -> (
    Skilj,
    String,
    Pool,
    String,
    RoleAccessMapping,
    skilj_core::event_store::CommandType,
) {
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

    // Small per-instance pool: every test leaks a live `Skilj`, so the default
    // 10-connection pools of ~10 tests exhaust the shared embedded Postgres's
    // 100 connections and the last `build()` fails with `PoolTimedOut`.

    let (skilj, report) = Skilj::builder(database_url)
        .pool_options(pool_options)
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
        None,
        test_now(),
    )
    .unwrap();
    db::insert_command_token(&pool, &token).await.unwrap();

    let credential = format!("{}.{}", token.id, token.secret);
    (skilj, credential, pool, bc_name, mapping, command_type)
}

/// Every write to a bounded context holds its `sequence` lock, and every
/// other writer waits on that lock holding a pooled connection. A holder
/// that then read through the pool - a write's sync projections (§116), a
/// command batch leader's idempotency-key lookup (§117, forced here by
/// giving every command a key) - could never get a connection once more
/// writers waited than the pool had, and everything failed at the
/// acquire timeout.
#[test]
fn commands_and_direct_writes_beyond_the_pool_size_all_complete() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, command_credential, pool, bc_name, mapping, _) = setup_with_pool(
            skilj_core::db::PgPoolOptions::new()
                .max_connections(3)
                .acquire_timeout(std::time::Duration::from_secs(10)),
        )
        .await;
        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let direct_credential = format!("{}.{}", direct_token.id, direct_token.secret);
        let router = skilj.rest_router();

        let requests = (0..48).map(|i| {
            let router = router.clone();
            let (uri, credential, status) = if i % 2 == 0 {
                (
                    "/v1/commands/trigger",
                    command_credential.clone(),
                    StatusCode::OK,
                )
            } else {
                (
                    "/v1/events/direct",
                    direct_credential.clone(),
                    StatusCode::CREATED,
                )
            };
            async move {
                let mut request = Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("authorization", format!("Bearer {credential}"))
                    .header("content-type", "application/json");
                if status == StatusCode::OK {
                    request = request.header("Idempotency-Key", format!("key-{i}"));
                }
                let request = request
                    .body(Body::from(format!(r#"{{"payload":{{"amount":{i}}}}}"#)))
                    .unwrap();
                let response = router.oneshot(request).await.unwrap();
                let actual = response.status();
                let body = response.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(actual, status, "{uri}: {}", String::from_utf8_lossy(&body));
            }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(8),
            futures_util::future::join_all(requests),
        )
        .await
        .expect("concurrent writes stalled on the connection pool");

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(events.len(), 48);
    });
}

/// The deterministic form of the test above, for the command path. The
/// bounded context's `sequence` lock is held from outside; a command with
/// an idempotency key queues for it first, then two direct writes, each
/// holding one of the pool's three connections. When the lock is released
/// the command gets it and must look its key up. It used to do that
/// through the pool - all three connections were taken, two of them by
/// writers waiting behind it, so it waited out the acquire timeout and
/// failed, and so did they (docs/architecture.md §117).
#[test]
fn a_command_holding_the_lock_needs_no_second_connection() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, command_credential, pool, bc_name, mapping, _) = setup_with_pool(
            skilj_core::db::PgPoolOptions::new()
                .max_connections(3)
                .acquire_timeout(std::time::Duration::from_secs(5)),
        )
        .await;
        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let direct_credential = format!("{}.{}", direct_token.id, direct_token.secret);
        let router = skilj.rest_router();

        let send = |uri: &'static str, credential: String, key: Option<&'static str>| {
            let router = router.clone();
            tokio::spawn(async move {
                let mut request = Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("authorization", format!("Bearer {credential}"))
                    .header("content-type", "application/json");
                if let Some(key) = key {
                    request = request.header("Idempotency-Key", key);
                }
                let request = request
                    .body(Body::from(r#"{"payload":{"amount":5}}"#))
                    .unwrap();
                let response = router.oneshot(request).await.unwrap();
                let status = response.status();
                let body = response.into_body().collect().await.unwrap().to_bytes();
                (status, String::from_utf8_lossy(&body).into_owned())
            })
        };

        // The test's own pool, not the `Skilj`'s.
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT next_value FROM \"bc_{bc_name}\".sequence FOR UPDATE"
        )))
        .execute(&mut *blocker)
        .await
        .unwrap();

        let command = send(
            "/v1/commands/trigger",
            command_credential,
            Some("only-once"),
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let writes = [
            send("/v1/events/direct", direct_credential.clone(), None),
            send("/v1/events/direct", direct_credential, None),
        ];
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        blocker.commit().await.unwrap();

        let (status, body) = tokio::time::timeout(std::time::Duration::from_secs(4), command)
            .await
            .expect("the command stalled waiting for a second pooled connection")
            .unwrap();
        assert_eq!(status, StatusCode::OK, "{body}");
        for write in writes {
            let (status, body) = tokio::time::timeout(std::time::Duration::from_secs(4), write)
                .await
                .expect("a direct write stalled behind the command")
                .unwrap();
            assert_eq!(status, StatusCode::CREATED, "{body}");
        }
    });
}

#[test]
fn command_trigger_accepts_and_persists_triggered_events() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name, _, _) = setup().await;
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
        let (skilj, credential, pool, bc_name, _, _) = setup().await;
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

/// Security-review finding on `CrossContextRoute` (docs/architecture.md
/// [§36](../../docs/architecture.md#cross-context-route)): a caller-supplied `Idempotency-Key` using the reserved
/// `skilj-cross-context-route:` prefix must be rejected outright, not
/// silently accepted into the same shared `idempotency_keys` table
/// `CrossContextRoute`'s own background task writes into - see
/// `skilj_core::event_store::reject_reserved_idempotency_key`'s own doc
/// comment for why this matters (without it, an ordinary Write-level
/// caller could pre-plant a route's own future key and silently
/// swallow a real cross-context delivery).
#[test]
fn command_trigger_rejects_a_reserved_idempotency_key_prefix() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name, _, _) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .header(
                "Idempotency-Key",
                "skilj-cross-context-route:some-other-route:42",
            )
            .body(Body::from(r#"{"payload":{"amount":20}}"#))
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "reserved_idempotency_key_prefix");

        // Nothing was written under that key, or at all - the rejection
        // happens before dispatch, the same as any other authorisation
        // failure.
        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(events.len(), 0);
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
        let (skilj, credential, _pool, _bc_name, _, _) = setup().await;
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
        let (skilj, credential, pool, bc_name, _, _) = setup().await;
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

/// Cross-tenant write fix (docs/architecture.md's own write-up of these
/// passes), the real end-to-end proof - the previously open gap this
/// pass closes: `setup()`'s own token has `scope: None`, so this mints a
/// *second*, real `CommandToken` scoped to `"20"` and proves it can no
/// longer trigger `WithdrawMoney` for a *different* owner - over the
/// actual REST surface and a real HTTP status, not just
/// `authorise_command_trigger`'s own pure-function unit tests in
/// `skilj-core/tests/command_processing.rs`. Also the regression test
/// for a real bug this same pass found while writing it:
/// `Error::GrantScopeMismatch` had no entry in `skilj-rest`'s own
/// `status_for` table, so it fell through to 500 instead of 403 the
/// first time this became reachable over REST at all - see that table's
/// own comment.
#[test]
fn command_trigger_rejects_a_command_whose_owner_does_not_match_the_tokens_scope() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _credential, pool, bc_name, mapping, command_type) = setup().await;
        let scoped_token = access_control::create_command_token(
            &mapping,
            &command_type,
            generate_token_id(),
            generate_token_secret(),
            Some("20".into()),
            test_now(),
        )
        .unwrap();
        db::insert_command_token(&pool, &scoped_token)
            .await
            .unwrap();
        let scoped_credential = format!("{}.{}", scoped_token.id, scoped_token.secret);
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {scoped_credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"amount":999}}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "grant_scope_mismatch");

        // Nothing was written - a rejected authorisation never reaches
        // process_command at all.
        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert!(events.is_empty());
    });
}

/// The success half of the same obligation - the identical scoped token
/// triggers normally when the payload's own derived owner tag matches.
#[test]
fn command_trigger_succeeds_when_the_owner_matches_the_tokens_scope() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _credential, pool, bc_name, mapping, command_type) = setup().await;
        let scoped_token = access_control::create_command_token(
            &mapping,
            &command_type,
            generate_token_id(),
            generate_token_secret(),
            Some("20".into()),
            test_now(),
        )
        .unwrap();
        db::insert_command_token(&pool, &scoped_token)
            .await
            .unwrap();
        let scoped_credential = format!("{}.{}", scoped_token.id, scoped_token.secret);
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {scoped_credential}"))
            .header("content-type", "application/json")
            .body(Body::from(r#"{"payload":{"amount":20}}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["accepted"], true);

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
    });
}

#[test]
fn command_trigger_returns_a_rejection_as_200_not_an_error() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, _pool, _bc_name, _, _) = setup().await;
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
        let (skilj, _credential, _pool, _bc_name, _, _) = setup().await;
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
/// dispatching ([§8](../../docs/architecture.md#open-for-a-future-pass) item 5).
#[test]
fn command_dispatcher_required_role_reflects_the_requires_role_attribute() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _credential, _pool, bc_name, _, _) = setup().await;
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

/// Every read of many events resolves their origins in one batch - a
/// command-triggered event embeds its whole originating command - where it
/// used to resolve each row's command one at a time, several queries each
/// (docs/architecture.md §125). Each such read must still give exactly
/// what the unchanged single-row read gives for every event, over a mix of
/// command-triggered and directly created ones.
#[test]
fn batched_event_reads_match_the_single_row_read() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, command_credential, pool, bc_name, mapping, _) = setup().await;
        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let direct_credential = format!("{}.{}", direct_token.id, direct_token.secret);
        let router = skilj.rest_router();
        for i in 0..12 {
            let (uri, credential) = if i % 3 == 2 {
                ("/v1/events/direct", &direct_credential)
            } else {
                ("/v1/commands/trigger", &command_credential)
            };
            let request = Request::builder()
                .method("POST")
                .uri(uri)
                .header("authorization", format!("Bearer {credential}"))
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"payload":{{"amount":{i}}}}}"#)))
                .unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            assert!(
                response.status().is_success(),
                "{uri}: {}",
                response.status()
            );
        }

        let mut expected = Vec::new();
        for sequence in 0..12 {
            expected.push(
                db::get_event_by_sequence(&pool, &bc_name, sequence)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(
            expected
                .iter()
                .filter(|e| matches!(
                    e.origin,
                    skilj_core::event_store::EventOrigin::CommandTriggered { .. }
                ))
                .count(),
            8
        );

        assert_eq!(
            db::list_events_for_bounded_context(&pool, &bc_name)
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            db::list_events_for_bounded_context_from(&pool, &bc_name, 3)
                .await
                .unwrap(),
            expected[4..]
        );
        assert_eq!(
            db::list_events_for_bounded_context_from_limited(&pool, &bc_name, 3, 5)
                .await
                .unwrap(),
            expected[4..9]
        );
        assert_eq!(
            db::list_recent_events_for_bounded_context(&pool, &bc_name, 4)
                .await
                .unwrap(),
            expected[8..]
        );
        assert_eq!(
            db::list_events(&pool, &bc_name, "MoneyDeposited")
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            db::list_events_from(&pool, &bc_name, "MoneyDeposited", 5)
                .await
                .unwrap(),
            expected[6..]
        );
        assert_eq!(
            db::list_events_from_limited(&pool, &bc_name, "MoneyDeposited", 5, 3)
                .await
                .unwrap(),
            expected[6..9]
        );
    });
}

/// Listing commands resolves their command types in one batch, where it
/// used to resolve each row's type (and that type's bounded context and
/// creator Role) one at a time (docs/architecture.md §126). The listing
/// and a `fetchCommands` page walked in chunks of 3 must each give exactly
/// what the single-row read gives, in recording order.
#[test]
fn batched_command_reads_match_the_single_row_read() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name, _, _) = setup().await;
        let router = skilj.rest_router();
        for i in 0..8 {
            let request = Request::builder()
                .method("POST")
                .uri("/v1/commands/trigger")
                .header("authorization", format!("Bearer {credential}"))
                .header("content-type", "application/json")
                .body(Body::from(format!(r#"{{"payload":{{"amount":{i}}}}}"#)))
                .unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let listed = db::list_commands_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(listed.len(), 8);
        for command in &listed {
            assert_eq!(
                Some(command),
                db::get_command_by_external_id(&pool, &bc_name, &command.id)
                    .await
                    .unwrap()
                    .as_ref()
            );
        }

        let mut walked = Vec::new();
        let page = db::collect_command_page(&pool, &bc_name, -1, 3, |chunk, remaining| {
            walked.extend(chunk.iter().cloned());
            Ok(chunk.iter().take(remaining).cloned().collect())
        })
        .await
        .unwrap();
        assert_eq!(page, listed[..3]);
        assert_eq!(walked, listed[..3]);
        let mut all = Vec::new();
        db::collect_command_page(&pool, &bc_name, -1, 100, |chunk, _| {
            all.extend(chunk.iter().cloned());
            Ok(Vec::new())
        })
        .await
        .unwrap();
        assert_eq!(all, listed);
    });
}

/// An `Idempotency-Key` longer than 255 characters is a 400 before
/// anything runs (docs/architecture.md §134).
#[test]
fn command_trigger_refuses_an_overlong_idempotency_key() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, credential, pool, bc_name, _, _) = setup().await;
        let request = Request::builder()
            .method("POST")
            .uri("/v1/commands/trigger")
            .header("authorization", format!("Bearer {credential}"))
            .header("content-type", "application/json")
            .header("Idempotency-Key", "k".repeat(256))
            .body(Body::from(r#"{"payload":{"amount":20}}"#))
            .unwrap();
        let response = skilj.rest_router().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["code"], "idempotency_key_too_long");
        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(events.len(), 0);
    });
}
