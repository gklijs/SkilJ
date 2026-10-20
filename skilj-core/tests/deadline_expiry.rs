//! issue #97 - a deadline's optional `expires_at`, and the firing
//! time `fire_due_deadlines` hands a command's `decide_with` through
//! `DecisionContext`.
//!
//! `skilj-test-fixture` proves the pure plumbing (`Decision::now` /
//! `Decision::deadline`, no database). Here the real fire path is driven
//! against Postgres and the `DecisionContext` a registered
//! `CommandDispatcher` actually receives is captured and asserted. The
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness
//! `deadline_cancel_ordering.rs` uses is reused, not re-explained.

use std::sync::{Arc, Mutex};

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::{
    CommandDispatcher, DeadlineContext, DeadlinePollStartFrom, DecisionContext, ErasedDeadlineSpec,
    ScheduleDeadlineDispatcher, ScheduleDeadlineInfo, SnapshotState,
};
use skilj_core::shared::{generate_token_id, CommandDecision, Metadata, Tag};

struct TestDb {
    pool: Pool,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

async fn test_pool() -> Option<Pool> {
    TEST_DB
        .get_or_init(|| async {
            let url = skilj_test_support::database_url("skilj_deadline_expiry_test").await?;
            let pool = db::connect(&url).await.ok()?;
            db::migrate(&pool).await.ok()?;
            Some(TestDb { pool })
        })
        .await
        .as_ref()
        .map(|db| db.pool.clone())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn seed_bounded_context(pool: &Pool) -> BoundedContext {
    let bc = BoundedContext {
        name: format!("bc_{}", generate_token_id()),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    bc
}

async fn seed_event_type(pool: &Pool, bc: &BoundedContext) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: "OrderPlaced".to_string(),
        schema: r#"{"properties":{"order_id":{"type":"string"}}}"#.to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: true,
        direct_creation_allowed: true,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: true,
    };
    db::upsert_event_type(pool, &et).await.unwrap();
    et
}

async fn seed_command_type(pool: &Pool, bc: &BoundedContext, name: &str) {
    let ct = skilj_core::event_store::CommandType {
        bounded_context: bc.clone(),
        name: name.to_string(),
        schema: r#"{"properties":{}}"#.to_string(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        rest_trigger_allowed: true,
    };
    db::upsert_command_type(pool, &ct).await.unwrap();
}

async fn append(pool: &Pool, et: &EventType) {
    let sequence = db::next_sequence(pool, &et.bounded_context.name)
        .await
        .unwrap();
    let event = Event {
        bounded_context: et.bounded_context.clone(),
        event_type: et.clone(),
        payload: r#"{"order_id":"o-1"}"#.to_string(),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: 1,
            client_id: "someone".to_string(),
            created_at: test_now(),
            correlation_id: None,
            causation_id: None,
        },
        sequence,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };
    db::insert_event(pool, &event, None).await.unwrap();
}

fn schedule_info(bc_name: &'static str) -> ScheduleDeadlineInfo {
    ScheduleDeadlineInfo {
        name: "ScheduleReminder",
        source_bounded_context: bc_name,
        source_event_type: "OrderPlaced",
        target_bounded_context: bc_name,
        target_command_type: "SendReminder",
        start_from: DeadlinePollStartFrom::Beginning,
    }
}

/// Schedules one deadline at a fixed `fire_at`, with an optional
/// `expires_at` (issue #97).
struct SchedulesAt {
    info: ScheduleDeadlineInfo,
    fire_at: chrono::DateTime<Utc>,
    expires_at: Option<chrono::DateTime<Utc>>,
}

impl ScheduleDeadlineDispatcher for SchedulesAt {
    fn schedules(&self) -> Vec<ScheduleDeadlineInfo> {
        vec![self.info]
    }
    fn schedule(
        &self,
        _schedule_name: &str,
        _source_payload_json: &str,
    ) -> Option<Result<Option<ErasedDeadlineSpec>, serde_json::Error>> {
        Some(Ok(Some(ErasedDeadlineSpec {
            fire_at: self.fire_at,
            expires_at: self.expires_at,
            tags: vec![Tag {
                key: "order".to_string(),
                value: Some("o-1".to_string()),
            }],
            payload_json: "{}".to_string(),
            recurrence: None,
        })))
    }
}

/// One recorded decision: the `now` it was made against, and, for a
/// deadline firing, the deadline's context.
type Recording = (chrono::DateTime<Utc>, Option<DeadlineContext>);

/// Records the `DecisionContext` every decision is dispatched with, then
/// rejects (an outcome the fire path treats as a settled, non-error one, so
/// no event type is needed).
#[derive(Clone, Default)]
struct RecordingContext {
    seen: Arc<Mutex<Vec<Recording>>>,
}

impl RecordingContext {
    fn new() -> Self {
        Self::default()
    }

    fn calls(&self) -> Vec<Recording> {
        self.seen.lock().unwrap().clone()
    }

    fn record(&self, context: &DecisionContext) -> CommandDecision {
        self.seen
            .lock()
            .unwrap()
            .push((context.now, context.deadline));
        CommandDecision::Rejected {
            reason: "recorded".to_string(),
            kind: "recorded".to_string(),
        }
    }
}

impl CommandDispatcher for RecordingContext {
    fn dispatch(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        _payload: &str,
        _matching_events: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        None
    }
    fn dispatch_with(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        _payload: &str,
        _matching_events: &[Event],
        _snapshots: &[SnapshotState],
        context: &DecisionContext,
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        Some(Ok(self.record(context)))
    }
    fn required_role(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn snapshot_name(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn dispatch_from_snapshot(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        _payload: &str,
        _snapshot_state_json: &str,
        _events_since_snapshot: &[Event],
        context: &DecisionContext,
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        Some(Ok(self.record(context)))
    }
}

struct NoProjections;

impl skilj_core::plugin::ProjectionDispatcher for NoProjections {
    fn keys(&self, _: &str, _: &str, _: &Event) -> Option<Vec<String>> {
        None
    }
    fn project(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &Event,
        _: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }
    fn default_state(&self, _: &str, _: &str) -> Option<String> {
        None
    }
    fn owner_tag_key(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn team_only(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
}

struct NoSnapshots;

impl skilj_core::plugin::SnapshotDispatcher for NoSnapshots {
    fn snapshot_names(&self, _: &str) -> Vec<&'static str> {
        Vec::new()
    }
    fn tag_key(&self, _: &str, _: &str) -> Option<&'static str> {
        None
    }
    fn owner_tag_key(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn version(&self, _: &str, _: &str) -> Option<u64> {
        None
    }
    fn fold(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }
    fn default_state(&self, _: &str, _: &str) -> Option<String> {
        None
    }
}

async fn deadline_statuses(pool: &Pool, bc: &BoundedContext) -> Vec<String> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT status FROM \"bc_{}\".deadlines ORDER BY id",
        bc.name
    )))
    .fetch_all(pool)
    .await
    .unwrap()
}

/// Schedules a `SendReminder` deadline at `fire_at` (a moment already past)
/// with `expires_at`, then fires it at `tick`. Returns what the dispatcher
/// recorded and the row's status afterwards.
async fn fire_one(
    pool: &Pool,
    fire_at: chrono::DateTime<Utc>,
    expires_at: Option<chrono::DateTime<Utc>>,
    tick: chrono::DateTime<Utc>,
) -> (Vec<Recording>, String) {
    let bc = seed_bounded_context(pool).await;
    let placed = seed_event_type(pool, &bc).await;
    append(pool, &placed).await;
    seed_command_type(pool, &bc, "SendReminder").await;
    let name: &'static str = Box::leak(bc.name.clone().into_boxed_str());
    let schedules = SchedulesAt {
        info: schedule_info(name),
        fire_at,
        expires_at,
    };
    let cache = skilj_core::event_cache::EventCache::new(0);
    db::catch_up_schedule_deadline(pool, &schedules.info, &schedules, &cache)
        .await
        .unwrap();

    let dispatcher = RecordingContext::new();
    db::fire_due_deadlines(
        pool,
        &dispatcher,
        &NoProjections,
        &NoSnapshots,
        &skilj_core::event_store::EventBroadcaster::new(16),
        &cache,
        &bc.name,
        tick,
        None,
        &skilj_retry::RetryPolicy::default(),
        &[],
    )
    .await
    .unwrap();
    let status = deadline_statuses(pool, &bc).await.remove(0);
    (dispatcher.calls(), status)
}

/// The optional expiry lands on the row (issue #97).
#[test]
fn a_deadline_row_stores_its_expiry() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let placed = seed_event_type(&pool, &bc).await;
        append(&pool, &placed).await;
        let name: &'static str = Box::leak(bc.name.clone().into_boxed_str());
        let fire_at = test_now() + chrono::Duration::hours(1);
        let expires_at = fire_at + chrono::Duration::minutes(5);
        let schedules = SchedulesAt {
            info: schedule_info(name),
            fire_at,
            expires_at: Some(expires_at),
        };
        let cache = skilj_core::event_cache::EventCache::new(0);
        db::catch_up_schedule_deadline(&pool, &schedules.info, &schedules, &cache)
            .await
            .unwrap();
        let stored: Option<chrono::DateTime<Utc>> =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT expires_at FROM \"bc_{}\".deadlines LIMIT 1",
                bc.name
            )))
            .fetch_optional(&pool)
            .await
            .unwrap();
        assert_eq!(stored, Some(expires_at));
    });
}

/// Fired one second past its expiry: `decide_with` is handed the deadline
/// it descends from, marked `expired`, plus the tick's own `now`
/// (issue #97).
#[test]
fn a_late_fire_hands_decide_an_expired_deadline() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let base = test_now();
        let fire_at = base - chrono::Duration::seconds(2);
        let expires_at = base - chrono::Duration::seconds(1);
        let (calls, status) = fire_one(&pool, fire_at, Some(expires_at), base).await;
        assert_eq!(status, "fired", "a rejection is still a settled fire");
        assert!(!calls.is_empty(), "the fire must reach the dispatcher");
        for (now, deadline) in calls {
            assert_eq!(now, base, "decide() sees the tick's own now");
            let deadline = deadline.expect("a deadline-fired command sees its deadline");
            assert_eq!(deadline.scheduled_at, fire_at);
            assert_eq!(deadline.deadline_at, Some(expires_at));
            assert!(deadline.expired, "fired one second past its expiry");
        }
    });
}

/// Fired before its expiry: the deadline context is present but not expired.
#[test]
fn an_on_time_fire_is_not_expired() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let base = test_now();
        let fire_at = base - chrono::Duration::seconds(1);
        let expires_at = base + chrono::Duration::seconds(60);
        let (calls, status) = fire_one(&pool, fire_at, Some(expires_at), base).await;
        assert_eq!(status, "fired");
        assert!(!calls.is_empty());
        for (_, deadline) in calls {
            let deadline = deadline.expect("a deadline-fired command sees its deadline");
            assert_eq!(deadline.deadline_at, Some(expires_at));
            assert!(!deadline.expired);
        }
    });
}

/// A deadline with no `expires_at` is never expired - the feature's
/// backward-compatible default.
#[test]
fn a_deadline_without_an_expiry_is_never_expired() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let base = test_now();
        let fire_at = base - chrono::Duration::seconds(1);
        let (calls, _) = fire_one(&pool, fire_at, None, base).await;
        assert!(!calls.is_empty());
        for (_, deadline) in calls {
            let deadline = deadline.expect("a deadline-fired command sees its deadline");
            assert_eq!(deadline.deadline_at, None);
            assert!(!deadline.expired);
        }
    });
}

/// An ordinary submission (no deadline) still gets its own `now`, and no
/// deadline context at all.
#[test]
fn an_ordinary_submission_has_a_now_and_no_deadline() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_command_type(&pool, &bc, "SendReminder").await;
        let ct = db::get_command_type(&pool, &bc.name, "SendReminder")
            .await
            .unwrap()
            .unwrap();
        let dispatcher = RecordingContext::new();
        let cache = skilj_core::event_cache::EventCache::new(0);
        let now = test_now();
        db::resolve_command_submission(
            &pool,
            &dispatcher,
            &NoSnapshots,
            &cache,
            &ct,
            "{}",
            &DecisionContext::at(now),
        )
        .await
        .unwrap();
        assert_eq!(dispatcher.calls(), vec![(now, None)]);
    });
}
