//! A `CancelDeadline` waits for its `ScheduleDeadline` (docs/architecture.md
//! §130). The two run as independent loops: when the cancel loop reached a
//! cancelling event before the schedule loop had turned the scheduling
//! event into a deadline, the cancel matched nothing, the deadline was
//! created a moment later and fired regardless. Here the two catch-ups are
//! driven by hand in exactly that order.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::{
    CancelDeadlineDispatcher, CancelDeadlineInfo, DeadlinePollStartFrom, ErasedDeadlineSpec,
    ScheduleDeadlineDispatcher, ScheduleDeadlineInfo,
};
use skilj_core::shared::{generate_token_id, Metadata, Tag};

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
            let url = match std::env::var("DATABASE_URL") {
                Ok(url) => url,
                Err(_) => {
                    skilj_test_support::database_url("skilj_deadline_cancel_ordering_test").await?
                }
            };
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

async fn seed_event_type(pool: &Pool, bc: &BoundedContext, name: &str) -> EventType {
    let et = EventType {
        bounded_context: bc.clone(),
        name: name.to_string(),
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

async fn append(pool: &Pool, et: &EventType, order_id: &str) {
    let sequence = db::next_sequence(pool, &et.bounded_context.name)
        .await
        .unwrap();
    let event = Event {
        bounded_context: et.bounded_context.clone(),
        event_type: et.clone(),
        payload: format!(r#"{{"order_id":"{order_id}"}}"#),
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

fn order_tag(payload: &str) -> Tag {
    let parsed: serde_json::Value = serde_json::from_str(payload).unwrap();
    Tag {
        key: "order".to_string(),
        value: parsed["order_id"].as_str().map(str::to_string),
    }
}

struct Schedules(ScheduleDeadlineInfo);

impl ScheduleDeadlineDispatcher for Schedules {
    fn schedules(&self) -> Vec<ScheduleDeadlineInfo> {
        vec![self.0]
    }
    fn schedule(
        &self,
        _schedule_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<ErasedDeadlineSpec>, serde_json::Error>> {
        Schedules::at(Utc::now() + chrono::Duration::hours(1), source_payload_json)
    }
}

impl Schedules {
    fn at(
        fire_at: chrono::DateTime<Utc>,
        source_payload_json: &str,
    ) -> Option<Result<Option<ErasedDeadlineSpec>, serde_json::Error>> {
        Some(Ok(Some(ErasedDeadlineSpec {
            fire_at,
            tags: vec![order_tag(source_payload_json)],
            payload_json: "{}".to_string(),
        })))
    }
}

/// Schedules every deadline at one fixed `fire_at`.
struct SchedulesAt(ScheduleDeadlineInfo, chrono::DateTime<Utc>);

impl ScheduleDeadlineDispatcher for SchedulesAt {
    fn schedules(&self) -> Vec<ScheduleDeadlineInfo> {
        vec![self.0]
    }
    fn schedule(
        &self,
        _schedule_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<ErasedDeadlineSpec>, serde_json::Error>> {
        Schedules::at(self.1, source_payload_json)
    }
}

struct Cancels(CancelDeadlineInfo);

impl CancelDeadlineDispatcher for Cancels {
    fn cancels(&self) -> Vec<CancelDeadlineInfo> {
        vec![self.0]
    }
    fn cancel_tags(
        &self,
        _cancel_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<Vec<Tag>>, serde_json::Error>> {
        Some(Ok(Some(vec![order_tag(source_payload_json)])))
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

/// Schedule the `OrderPlaced` source into `placed`'s context and cancel on
/// `paid`'s events (the same context or another): the cancel tick runs
/// first, before the schedule has made the deadline; then the schedule;
/// then the cancel again. The deadline must end up cancelled.
async fn cancel_before_schedule_still_cancels(
    pool: &Pool,
    placed_bc: &BoundedContext,
    paid_bc: &BoundedContext,
) {
    let placed = seed_event_type(pool, placed_bc, "OrderPlaced").await;
    let paid = if paid_bc.name == placed_bc.name {
        seed_event_type(pool, paid_bc, "OrderPaid").await
    } else {
        seed_event_type(pool, paid_bc, "PaymentReceived").await
    };
    append(pool, &placed, "o-1").await;
    // Across contexts, order is by creation time; keep them apart.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    append(pool, &paid, "o-1").await;

    let schedule = ScheduleDeadlineInfo {
        name: "CancelUnpaid",
        source_bounded_context: Box::leak(placed_bc.name.clone().into_boxed_str()),
        source_event_type: "OrderPlaced",
        target_bounded_context: Box::leak(placed_bc.name.clone().into_boxed_str()),
        target_command_type: "CancelOrder",
        start_from: DeadlinePollStartFrom::Beginning,
    };
    let cancel = CancelDeadlineInfo {
        name: "CancelOnPaid",
        source_bounded_context: Box::leak(paid_bc.name.clone().into_boxed_str()),
        source_event_type: Box::leak(paid.name.clone().into_boxed_str()),
        deadline_schedule_name: "CancelUnpaid",
        deadline_schedule_bounded_context: Box::leak(placed_bc.name.clone().into_boxed_str()),
        deadline_schedule_source_event_type: "OrderPlaced",
        start_from: DeadlinePollStartFrom::Beginning,
    };
    let cache = skilj_core::event_cache::EventCache::new(0);

    db::catch_up_cancel_deadline(pool, &cancel, &Cancels(cancel), &cache)
        .await
        .unwrap();
    db::catch_up_schedule_deadline(pool, &schedule, &Schedules(schedule), &cache)
        .await
        .unwrap();
    assert_eq!(deadline_statuses(pool, placed_bc).await, ["pending"]);
    db::catch_up_cancel_deadline(pool, &cancel, &Cancels(cancel), &cache)
        .await
        .unwrap();
    assert_eq!(
        deadline_statuses(pool, placed_bc).await,
        ["cancelled"],
        "the payment must cancel the deadline its order scheduled, whichever loop got there first"
    );
}

#[test]
fn a_cancel_processed_before_its_schedule_still_cancels_within_one_context() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        cancel_before_schedule_still_cancels(&pool, &bc, &bc).await;
    });
}

#[test]
fn a_cancel_processed_before_its_schedule_still_cancels_across_contexts() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let placed_bc = seed_bounded_context(&pool).await;
        let paid_bc = seed_bounded_context(&pool).await;
        cancel_before_schedule_still_cancels(&pool, &placed_bc, &paid_bc).await;
    });
}

// --- a fire waits for its cancels (docs/architecture.md §131) ---

struct NoCommands;

impl skilj_core::plugin::CommandDispatcher for NoCommands {
    fn dispatch(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &[Event],
    ) -> Option<skilj_core::error::Result<skilj_core::shared::CommandDecision>> {
        None
    }
    fn required_role(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn snapshot_name(&self, _: &str, _: &str) -> Option<Option<&'static str>> {
        None
    }
    fn dispatch_from_snapshot(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &str,
        _: &[Event],
    ) -> Option<skilj_core::error::Result<skilj_core::shared::CommandDecision>> {
        None
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

/// An order's cancel deadline at `fire_at`, and a payment appended once
/// `pay_after` has passed; the cancel loop hasn't run when the deadline
/// comes due. Its target command type isn't registered, so a fire is just
/// the row marked `fired`. Returns the row's status after one fire tick,
/// then after the cancel catches up.
async fn fire_with_a_lagging_cancel(
    pool: &Pool,
    fire_in: chrono::Duration,
    pay_after: std::time::Duration,
) -> (String, String) {
    let bc = seed_bounded_context(pool).await;
    let placed = seed_event_type(pool, &bc, "OrderPlaced").await;
    let paid = seed_event_type(pool, &bc, "OrderPaid").await;
    let name: &'static str = Box::leak(bc.name.clone().into_boxed_str());
    let schedule = ScheduleDeadlineInfo {
        name: "CancelUnpaid",
        source_bounded_context: name,
        source_event_type: "OrderPlaced",
        target_bounded_context: name,
        target_command_type: "NotRegistered",
        start_from: DeadlinePollStartFrom::Beginning,
    };
    let cancel = CancelDeadlineInfo {
        name: "CancelOnPaid",
        source_bounded_context: name,
        source_event_type: "OrderPaid",
        deadline_schedule_name: "CancelUnpaid",
        deadline_schedule_bounded_context: name,
        deadline_schedule_source_event_type: "OrderPlaced",
        start_from: DeadlinePollStartFrom::Beginning,
    };
    let cache = skilj_core::event_cache::EventCache::new(0);

    append(pool, &placed, "o-1").await;
    let fire_at = test_now() + fire_in;
    db::catch_up_schedule_deadline(pool, &schedule, &SchedulesAt(schedule, fire_at), &cache)
        .await
        .unwrap();
    tokio::time::sleep(pay_after).await;
    append(pool, &paid, "o-1").await;
    // Due now, whichever way the payment fell.
    let until_due = (fire_at - Utc::now()).to_std().unwrap_or_default();
    tokio::time::sleep(until_due + std::time::Duration::from_millis(20)).await;

    db::fire_due_deadlines(
        pool,
        &NoCommands,
        &NoProjections,
        &NoSnapshots,
        &skilj_core::event_store::EventBroadcaster::new(16),
        &cache,
        &bc.name,
        Utc::now(),
        None,
        &skilj_retry::RetryPolicy::default(),
        &[cancel],
    )
    .await
    .unwrap();
    let after_fire = deadline_statuses(pool, &bc).await.remove(0);
    db::catch_up_cancel_deadline(pool, &cancel, &Cancels(cancel), &cache)
        .await
        .unwrap();
    let after_cancel = deadline_statuses(pool, &bc).await.remove(0);
    (after_fire, after_cancel)
}

/// Paid before the deadline came due, with the cancel loop behind: the
/// fire waits, and the payment cancels the deadline.
#[test]
fn a_deadline_waits_for_a_cancel_committed_before_it_came_due() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let statuses = fire_with_a_lagging_cancel(
            &pool,
            chrono::Duration::milliseconds(300),
            std::time::Duration::ZERO,
        )
        .await;
        assert_eq!(statuses, ("pending".to_string(), "cancelled".to_string()));
    });
}

/// Paid only after the deadline came due: that payment was too late, and
/// the fire doesn't wait for it.
#[test]
fn a_deadline_does_not_wait_for_a_cancel_after_it_came_due() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let statuses = fire_with_a_lagging_cancel(
            &pool,
            chrono::Duration::milliseconds(50),
            std::time::Duration::from_millis(200),
        )
        .await;
        assert_eq!(statuses, ("fired".to_string(), "fired".to_string()));
    });
}

/// A cancel whose source bounded context is gone - hard-deleted while
/// still registered - can never cancel anything, so it doesn't hold a
/// fire. Asking its dropped schema used to fail every fire tick of the
/// deadlines' own bounded context, so none of them fired
/// (docs/architecture.md §133).
#[test]
fn a_cancel_whose_source_is_gone_does_not_stop_deadlines_firing() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        let placed = seed_event_type(&pool, &bc, "OrderPlaced").await;
        let name: &'static str = Box::leak(bc.name.clone().into_boxed_str());
        let schedule = ScheduleDeadlineInfo {
            name: "CancelUnpaid",
            source_bounded_context: name,
            source_event_type: "OrderPlaced",
            target_bounded_context: name,
            target_command_type: "NotRegistered",
            start_from: DeadlinePollStartFrom::Beginning,
        };
        let cancel = CancelDeadlineInfo {
            name: "CancelOnPaid",
            source_bounded_context: "a_bounded_context_that_was_deleted",
            source_event_type: "OrderPaid",
            deadline_schedule_name: "CancelUnpaid",
            deadline_schedule_bounded_context: name,
            deadline_schedule_source_event_type: "OrderPlaced",
            start_from: DeadlinePollStartFrom::Beginning,
        };
        let cache = skilj_core::event_cache::EventCache::new(0);
        append(&pool, &placed, "o-1").await;
        db::catch_up_schedule_deadline(
            &pool,
            &schedule,
            &SchedulesAt(schedule, test_now() - chrono::Duration::seconds(1)),
            &cache,
        )
        .await
        .unwrap();

        db::fire_due_deadlines(
            &pool,
            &NoCommands,
            &NoProjections,
            &NoSnapshots,
            &skilj_core::event_store::EventBroadcaster::new(16),
            &cache,
            &bc.name,
            Utc::now(),
            None,
            &skilj_retry::RetryPolicy::default(),
            &[cancel],
        )
        .await
        .expect("a gone cancel source must not fail the fire tick");
        assert_eq!(deadline_statuses(&pool, &bc).await, ["fired"]);
    });
}
