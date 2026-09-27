//! Phase 1 of [docs/architecture.md §34](../../docs/architecture.md#skilj-temporal-plan)'s `skilj-temporal` plan: proof
//! that skilj's existing `Idempotency-Key` mechanism (Codeberg issue
//! #12, [§21](../../docs/architecture.md#optional-idempotency-key-submission)) is a drop-in fit for a Temporal Activity that calls
//! `POST /v1/commands/trigger`, using Temporal's own documented
//! idempotency-key derivation - a Workflow Run ID plus an Activity ID,
//! "guaranteed to be consistent across retry attempts but unique among
//! Workflow Executions" (https://docs.temporal.io/activity-definition).
//! No new skilj code, no Temporal dependency here - this exercises the
//! real REST surface exactly the way an Activity implementation in any
//! language (Go, Java, Python, Rust's own still-Preview SDK, ...) would,
//! composing the key client-side and sending it as a header. See
//! docs/temporal-integration.md for the write-up this test backs.
//!
//! Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj/tests/command_trigger.rs` - see that file's own doc comment
//! for the details, not repeated a third time here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{generate_token_id, generate_token_secret, CommandDecision, EventSpec};
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
    #[allow(dead_code)]
    MoneyDeposited(MoneyDepositedPayload),
}

impl BoundedContextEvent for BankingEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "MoneyDeposited" => {
                Some(serde_json::from_str(&event.payload).map(Self::MoneyDeposited))
            }
            _ => None,
        }
    }
}

/// Always accepts - the point of this file is the idempotency-key
/// derivation, not `decide()` logic, so kept as simple as
/// `command_trigger.rs`'s own trivial commands.
struct WithdrawMoney;

impl CommandType for WithdrawMoney {
    type Payload = MoneyDepositedPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "WithdrawMoney";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "MoneyDeposited".to_string(),
                payload: serde_json::json!({ "amount": payload.amount }),
            }],
        }
    }
}

// --- harness (same shape as command_trigger.rs) ---

struct TestDb {
    database_url: String,
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for temporal_activity_idempotency tests")
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
        let url = skilj_test_support::database_url("skilj_temporal_idempotency_test").await?;
        let pool = connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            pool,
        });
    };

    let pool = connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb { database_url, pool })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn setup() -> (Router, String, Pool, String) {
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
    (skilj.rest_router(), credential, pool, bc_name)
}

/// The one piece of client-side glue [docs/architecture.md §34](../../docs/architecture.md#skilj-temporal-plan)'s phase 1
/// names - not shipped by skilj (it has no Temporal dependency to build
/// against, and there is no logic here beyond the format string), but
/// this is exactly what a Temporal Activity implementation composes
/// before calling `POST /v1/commands/trigger`. Deliberately keyed by
/// `run_id`, not `workflow_id` alone - Temporal's own guidance is Run ID
/// plus Activity ID, because a Workflow ID can outlive more than one Run
/// (continue-as-new, a reset) and an Activity ID is only unique *within*
/// a run, so `workflow_id` alone would collide across runs that reuse
/// the same Workflow ID and Activity ID naming.
fn temporal_idempotency_key(run_id: &str, activity_id: &str) -> String {
    format!("{run_id}:{activity_id}")
}

/// Submits `WithdrawMoney` the way a Temporal Activity body would: build
/// the request, attach the derived key, POST it, return the decoded
/// response.
async fn submit_withdraw_money_activity(
    router: Router,
    credential: &str,
    run_id: &str,
    activity_id: &str,
    amount: i64,
) -> serde_json::Value {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/commands/trigger")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .header(
            "Idempotency-Key",
            temporal_idempotency_key(run_id, activity_id),
        )
        .body(Body::from(format!(
            r#"{{"payload":{{"amount":{amount}}}}}"#
        )))
        .unwrap();

    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&body).unwrap()
}

/// The scenario [docs/architecture.md §34](../../docs/architecture.md#skilj-temporal-plan) phase 1 exists to prove: a
/// Temporal Activity executes successfully, but the Worker crashes
/// before Temporal's own server records that outcome - from the
/// Workflow's point of view this is indistinguishable from the Activity
/// never having run at all, so Temporal retries it with the *same* Run
/// ID and Activity ID. Without the idempotency key this would
/// double-apply `WithdrawMoney`; with it, the retry is deduplicated and
/// nothing is double-applied - not just that the response looks right,
/// but that only one event actually exists.
#[test]
fn a_retried_temporal_activity_does_not_double_apply_the_command() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (router, credential, pool, bc_name) = setup().await;
        let run_id = unique_name("run");
        let activity_id = "withdraw-1";

        let first =
            submit_withdraw_money_activity(router.clone(), &credential, &run_id, activity_id, 50)
                .await;
        assert_eq!(first["accepted"], true);
        assert_eq!(first["deduplicated"], false);
        let first_sequences = first["triggeredEventSequences"].clone();

        // Simulates Temporal redelivering the same Activity Task after a
        // Worker crash - identical Run ID and Activity ID, the Worker
        // (this test) has no memory of the first attempt having
        // succeeded.
        let retried =
            submit_withdraw_money_activity(router, &credential, &run_id, activity_id, 50).await;
        assert_eq!(retried["accepted"], true);
        assert_eq!(
            retried["deduplicated"], true,
            "a redelivered Activity Task must be reported as deduplicated"
        );
        assert_eq!(
            retried["triggeredEventSequences"], first_sequences,
            "a dedup hit must return the *original* sequences, not a fresh decision"
        );

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            1,
            "a redelivered Activity Task must not double-apply the command"
        );
    });
}

/// The negative case that proves the key isn't over-broad: two distinct
/// Activity invocations within the *same* Workflow Run (different
/// Activity IDs, as Temporal itself guarantees per invocation) must not
/// be coalesced into one - each is its own real `WithdrawMoney`.
#[test]
fn two_different_activities_in_the_same_run_are_not_deduplicated_against_each_other() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (router, credential, pool, bc_name) = setup().await;
        let run_id = unique_name("run");

        submit_withdraw_money_activity(router.clone(), &credential, &run_id, "withdraw-1", 50)
            .await;
        submit_withdraw_money_activity(router, &credential, &run_id, "withdraw-2", 30).await;

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            2,
            "different Activity IDs in the same run must both apply"
        );
    });
}

/// The other negative case, and the reason the key includes `run_id` at
/// all rather than just `activity_id`: two separate Workflow Runs that
/// happen to reuse the same Activity ID naming (an ordinary occurrence -
/// Activity IDs are typically sequential/positional within a workflow
/// definition, e.g. "withdraw-1" for whichever run) must not collide.
#[test]
fn the_same_activity_id_in_different_runs_is_not_deduplicated_against_the_other_run() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (router, credential, pool, bc_name) = setup().await;
        let activity_id = "withdraw-1";

        submit_withdraw_money_activity(
            router.clone(),
            &credential,
            &unique_name("run"),
            activity_id,
            50,
        )
        .await;
        submit_withdraw_money_activity(router, &credential, &unique_name("run"), activity_id, 30)
            .await;

        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert_eq!(
            events.len(),
            2,
            "the same Activity ID in two different runs must not be deduplicated"
        );
    });
}
