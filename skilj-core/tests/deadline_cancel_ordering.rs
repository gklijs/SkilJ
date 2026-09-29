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
        Some(Ok(Some(ErasedDeadlineSpec {
            fire_at: Utc::now() + chrono::Duration::hours(1),
            tags: vec![order_tag(source_payload_json)],
            payload_json: "{}".to_string(),
        })))
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
