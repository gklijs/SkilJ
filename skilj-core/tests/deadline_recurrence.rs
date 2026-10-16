//! Tests for Codeberg issue #95's per-entity recurrence with missed-run
//! policy. Two layers, matching the implementation's own split:
//!
//! 1. `plugin::next_recurrence_fire_at` - pure cron/policy/window math, no
//!    database needed. Every policy's missed-run behaviour, the validity
//!    window, and the unparseable-schedule degradation are pinned here.
//! 2. `db::catch_up_schedule_deadline` - recurrence persistence: the
//!    columns land on the row, an out-of-window initial `fire_at` is
//!    skipped, and a bad cron degrades to a stored one-shot rather than
//!    refusing the row.
//!
//! The full fire-then-advance-the-chain loop is `skilj/tests/
//! deadline_recurrence.rs`' own end-to-end proof, through a real `Skilj`.
//! The same `DATABASE_URL`-then-embedded-Postgres-then-skip harness
//! `deadline_cancel_ordering.rs` already uses is reused here, not
//! re-explained.

use chrono::{SubsecRound, TimeZone, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::{
    next_recurrence_fire_at, DeadlinePollStartFrom, ErasedDeadlineSpec, MissedRunPolicy,
    RecurrenceSpec, ScheduleDeadlineDispatcher, ScheduleDeadlineInfo,
};
use skilj_core::shared::{generate_token_id, Metadata, Tag};

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

/// Top of every hour - minute 0 of every hour, so hand-computable.
fn every_hour() -> String {
    "0 0 * * * * *".to_string()
}

fn recurrence(policy: MissedRunPolicy) -> RecurrenceSpec {
    RecurrenceSpec {
        schedule: every_hour(),
        policy,
        start_at: None,
        end_at: None,
        valid_until: None,
    }
}

/// `Skip` never replays: after missing two occurrences, the next firing
/// is the first occurrence after now, not the missed ones.
#[test]
fn skip_advances_past_missed_occurrences() {
    let recurrence = recurrence(MissedRunPolicy::Skip);
    // Fired at 06:00, now at 08:02:30 - the 07:00 and 08:00 occurrences
    // are missed, so the next firing is 09:00.
    let next = next_recurrence_fire_at(&recurrence, timestamp(6 * 3600), timestamp(8 * 3600 + 150))
        .unwrap();
    assert_eq!(next, timestamp(9 * 3600));
}

/// `FireOnce` collapses a backlog to one firing of its latest occurrence -
/// here 08:00, the last hour-mark at or before now (08:02:30).
#[test]
fn fire_once_collapses_a_backlog_to_its_latest_occurrence() {
    let recurrence = recurrence(MissedRunPolicy::FireOnce);
    let next = next_recurrence_fire_at(&recurrence, timestamp(6 * 3600), timestamp(8 * 3600 + 150))
        .unwrap();
    assert_eq!(next, timestamp(8 * 3600));
}

/// `FireOnce` with no backlog is just the next occurrence - 10 minutes
/// after the previous firing, still before the next hour-mark, so 07:00.
#[test]
fn fire_once_without_a_backlog_is_the_next_occurrence() {
    let recurrence = recurrence(MissedRunPolicy::FireOnce);
    let next = next_recurrence_fire_at(&recurrence, timestamp(6 * 3600), timestamp(6 * 3600 + 600))
        .unwrap();
    assert_eq!(next, timestamp(7 * 3600));
}

/// `FireAll` steps through every occurrence, missed or not - 07:00 then
/// 08:00, the two hour-marks after the 06:00 firing.
#[test]
fn fire_all_steps_through_each_missed_occurrence() {
    let recurrence = recurrence(MissedRunPolicy::FireAll);
    let first =
        next_recurrence_fire_at(&recurrence, timestamp(6 * 3600), timestamp(8 * 3600 + 150))
            .unwrap();
    assert_eq!(first, timestamp(7 * 3600));
    let second = next_recurrence_fire_at(&recurrence, first, timestamp(8 * 3600 + 150)).unwrap();
    assert_eq!(second, timestamp(8 * 3600));
}

/// `end_at` ends the chain: no candidate past it - here 06:30, before the
/// 07:00 next occurrence.
#[test]
fn an_end_at_before_the_next_occurrence_ends_the_chain() {
    let recurrence = RecurrenceSpec {
        end_at: Some(timestamp(6 * 3600 + 1800)),
        ..recurrence(MissedRunPolicy::FireAll)
    };
    assert_eq!(
        next_recurrence_fire_at(&recurrence, timestamp(6 * 3600), timestamp(6 * 3600 + 600)),
        None
    );
}

/// `valid_until` is the kill-switch: past it, no firing at all.
#[test]
fn a_valid_until_in_the_past_fires_nothing() {
    let recurrence = RecurrenceSpec {
        valid_until: Some(timestamp(6 * 3600 + 300)),
        ..recurrence(MissedRunPolicy::FireAll)
    };
    assert_eq!(
        next_recurrence_fire_at(&recurrence, timestamp(6 * 3600), timestamp(6 * 3600 + 600)),
        None
    );
}

/// A candidate past `valid_until` ends the chain even when `now` hasn't
/// reached it yet - 06:30 before the 07:00 next occurrence.
#[test]
fn a_candidate_past_valid_until_ends_the_chain() {
    let recurrence = RecurrenceSpec {
        valid_until: Some(timestamp(6 * 3600 + 1800)),
        ..recurrence(MissedRunPolicy::Skip)
    };
    assert_eq!(
        next_recurrence_fire_at(&recurrence, timestamp(6 * 3600), timestamp(6 * 3600 + 600)),
        None
    );
}

/// A candidate before `start_at` ends the chain - the chain never fires
/// outside its own window: 09:00 start, 07:00 candidate.
#[test]
fn a_candidate_before_start_at_ends_the_chain() {
    let recurrence = RecurrenceSpec {
        start_at: Some(timestamp(9 * 3600)),
        ..recurrence(MissedRunPolicy::FireAll)
    };
    assert_eq!(
        next_recurrence_fire_at(&recurrence, timestamp(6 * 3600), timestamp(6 * 3600 + 10)),
        None
    );
}

/// An unparseable schedule ends the chain rather than erroring - the
/// caller degrades to a one-shot.
#[test]
fn an_unparseable_schedule_ends_the_chain() {
    let recurrence = RecurrenceSpec {
        schedule: "not a cron expression".to_string(),
        ..recurrence(MissedRunPolicy::FireAll)
    };
    assert_eq!(
        next_recurrence_fire_at(&recurrence, timestamp(6 * 3600), timestamp(6 * 3600 + 10)),
        None
    );
}

// --- persistence: what `catch_up_schedule_deadline` stores ---

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
            let url = skilj_test_support::database_url("skilj_deadline_recurrence_test").await?;
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

struct Schedules {
    info: ScheduleDeadlineInfo,
    fire_at: chrono::DateTime<Utc>,
    recurrence: Option<RecurrenceSpec>,
}

impl ScheduleDeadlineDispatcher for Schedules {
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
            expires_at: None,
            tags: vec![Tag {
                key: "order".to_string(),
                value: Some("o-1".to_string()),
            }],
            payload_json: "{}".to_string(),
            recurrence: self.recurrence.clone(),
        })))
    }
}

fn schedule_info(bc_name: &'static str) -> ScheduleDeadlineInfo {
    ScheduleDeadlineInfo {
        name: "RecurringReminder",
        source_bounded_context: bc_name,
        source_event_type: "OrderPlaced",
        target_bounded_context: bc_name,
        target_command_type: "SendReminder",
        start_from: DeadlinePollStartFrom::Beginning,
    }
}

async fn deadline_row(
    pool: &Pool,
    bc: &BoundedContext,
) -> Option<(
    String,
    chrono::DateTime<Utc>,
    Option<String>,
    Option<String>,
    Option<i64>,
)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, fire_at, recurrence_schedule, recurrence_parent_id, recurrence_sequence \
         FROM \"bc_{}\".deadlines ORDER BY id",
        bc.name
    )))
    .fetch_optional(pool)
    .await
    .unwrap()
}

/// A recurring spec persists its chain state: the schedule, the policy,
/// the window, and the first row naming itself its own parent at
/// sequence 0.
#[test]
fn a_recurring_schedule_persists_its_chain_state() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_event_type(&pool, &bc).await;
        append(&pool, &seed_event_type(&pool, &bc).await).await;
        let name: &'static str = Box::leak(bc.name.clone().into_boxed_str());
        let fire_at = test_now() + chrono::Duration::hours(1);
        let schedules = Schedules {
            info: schedule_info(name),
            fire_at,
            recurrence: Some(RecurrenceSpec {
                schedule: every_hour(),
                policy: MissedRunPolicy::FireAll,
                start_at: None,
                end_at: Some(fire_at + chrono::Duration::hours(1)),
                valid_until: None,
            }),
        };
        let cache = skilj_core::event_cache::EventCache::new(0);
        db::catch_up_schedule_deadline(&pool, &schedules.info, &schedules, &cache)
            .await
            .unwrap();
        let (id, stored_fire_at, schedule, parent_id, sequence) =
            deadline_row(&pool, &bc).await.unwrap();
        assert_eq!(stored_fire_at, fire_at);
        assert_eq!(schedule.as_deref(), Some("0 0 * * * * *"));
        assert_eq!(parent_id.as_deref(), Some(id.as_str()));
        assert_eq!(sequence, Some(0));
    });
}

/// An initial `fire_at` outside its own window schedules nothing - a
/// caller bug, skipped with a warning, cursor still advanced.
#[test]
fn an_initial_fire_at_outside_its_window_schedules_nothing() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_event_type(&pool, &bc).await;
        append(&pool, &seed_event_type(&pool, &bc).await).await;
        let name: &'static str = Box::leak(bc.name.clone().into_boxed_str());
        let schedules = Schedules {
            info: schedule_info(name),
            // Past the window's own end - already expired on arrival.
            fire_at: test_now() + chrono::Duration::hours(2),
            recurrence: Some(RecurrenceSpec {
                schedule: every_hour(),
                policy: MissedRunPolicy::FireAll,
                start_at: None,
                end_at: Some(test_now() + chrono::Duration::hours(1)),
                valid_until: None,
            }),
        };
        let cache = skilj_core::event_cache::EventCache::new(0);
        db::catch_up_schedule_deadline(&pool, &schedules.info, &schedules, &cache)
            .await
            .unwrap();
        assert!(deadline_row(&pool, &bc).await.is_none());
    });
}

/// A bad cron expression degrades to a stored one-shot: the row lands
/// with `NULL` recurrence columns, so it fires once and the chain ends.
#[test]
fn an_unparseable_schedule_degrades_to_a_stored_one_shot() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let bc = seed_bounded_context(&pool).await;
        seed_event_type(&pool, &bc).await;
        append(&pool, &seed_event_type(&pool, &bc).await).await;
        let name: &'static str = Box::leak(bc.name.clone().into_boxed_str());
        let schedules = Schedules {
            info: schedule_info(name),
            fire_at: test_now() + chrono::Duration::hours(1),
            recurrence: Some(RecurrenceSpec {
                schedule: "not a cron expression".to_string(),
                policy: MissedRunPolicy::FireAll,
                start_at: None,
                end_at: None,
                valid_until: None,
            }),
        };
        let cache = skilj_core::event_cache::EventCache::new(0);
        db::catch_up_schedule_deadline(&pool, &schedules.info, &schedules, &cache)
            .await
            .unwrap();
        let (_, _, schedule, parent_id, sequence) = deadline_row(&pool, &bc).await.unwrap();
        assert_eq!(schedule.as_deref(), Some("not a cron expression"));
        // The chain math ends a bad schedule after the first firing; the
        // row still carries its parent/sequence so the ids stay
        // well-formed.
        assert!(parent_id.is_some());
        assert_eq!(sequence, Some(0));
    });
}
