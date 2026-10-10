//! Snapshot catch-up benchmark - not a test, so `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --release -p skilj-core --test snapshot_catch_up_throughput -- --ignored --nocapture
//! ```
//!
//! (`DATABASE_URL` to run against a server of your own, otherwise the
//! embedded one.) The `Snapshot` twin of `projection_catch_up_throughput`:
//! each scenario commits `EVENTS` events into a fresh bounded context
//! first, then times `db::catch_up_snapshots` - called back to back, as
//! the background task would with no idle wait - until every snapshot
//! has caught up. The snapshots, each a running sum:
//!
//! - **one**: every event carries the same tag value, so every event
//!   folds into the same row;
//! - **per-account**: keyed by the event's account tag, `ACCOUNTS` of
//!   them;
//! - **partitioned**: per-account, with `PARTITION_COUNT` 4;
//! - **reset**: per-account caught up once, then its `VERSION` bumped, so
//!   every row is refolded from its tag's history.
//!
//! See docs/architecture.md §197 for the numbers.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::SnapshotDispatcher;
use skilj_core::shared::{generate_token_id, Metadata, Tag};
use std::time::Instant;

/// Events per scenario.
const EVENTS: i64 = 5000;
/// Distinct accounts the per-account snapshots are keyed by.
const ACCOUNTS: i64 = 500;

struct BenchDispatcher {
    name: &'static str,
    tag_key: &'static str,
    version: u64,
    partition_count: u32,
}

impl SnapshotDispatcher for BenchDispatcher {
    fn snapshot_names(&self, _bc: &str) -> Vec<&'static str> {
        vec![self.name]
    }

    fn tag_key(&self, _bc: &str, snapshot_name: &str) -> Option<&'static str> {
        (snapshot_name == self.name).then_some(self.tag_key)
    }

    fn owner_tag_key(&self, _bc: &str, snapshot_name: &str) -> Option<Option<&'static str>> {
        (snapshot_name == self.name).then_some(None)
    }

    fn version(&self, _bc: &str, snapshot_name: &str) -> Option<u64> {
        (snapshot_name == self.name).then_some(self.version)
    }

    fn fold(
        &self,
        _bc: &str,
        snapshot_name: &str,
        state_json: &str,
        event: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        if snapshot_name != self.name {
            return None;
        }
        let current: i64 = state_json.parse().unwrap_or(0);
        let payload: serde_json::Value =
            serde_json::from_str(&event.payload).expect("bench payload is JSON");
        let amount = payload["amount"].as_i64().unwrap_or(0);
        Some(Ok((current + amount).to_string()))
    }

    fn default_state(&self, _bc: &str, snapshot_name: &str) -> Option<String> {
        (snapshot_name == self.name).then(|| "0".to_string())
    }

    fn partition_count(&self, _bc: &str, snapshot_name: &str) -> Option<u32> {
        (snapshot_name == self.name).then_some(self.partition_count)
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio runtime"))
}

async fn pool() -> Option<Pool> {
    let url = skilj_test_support::database_url("skilj_snapshot_catch_up_throughput").await?;
    let pool = db::connect(&url).await.ok()?;
    db::migrate(&pool).await.ok()?;
    Some(pool)
}

fn now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

fn tag(key: &str, value: String) -> Tag {
    Tag {
        key: key.to_string(),
        value: Some(value),
    }
}

async fn seed(pool: &Pool) -> BoundedContext {
    let bc = BoundedContext {
        name: format!("bench_{}", generate_token_id()),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    let et = EventType {
        bounded_context: bc.clone(),
        name: "Deposited".to_string(),
        schema: r#"{"properties":{"account":{"type":"string"},"amount":{"type":"number"}}}"#
            .to_string(),
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

    let mut tx = pool.begin().await.unwrap();
    for i in 0..EVENTS {
        let sequence = db::next_sequence(&mut *tx, &bc.name).await.unwrap();
        let account = format!("a{}", i % ACCOUNTS);
        let event = Event {
            bounded_context: bc.clone(),
            event_type: et.clone(),
            payload: format!(r#"{{"account":"{account}","amount":1}}"#),
            metadata: Metadata {
                r#type: et.name.clone(),
                version: 1,
                client_id: "bench".to_string(),
                created_at: now(),
                correlation_id: None,
                causation_id: None,
            },
            sequence,
            tags: vec![tag("all", "x".to_string()), tag("account", account)],
            encryption_keys: Vec::new(),
            origin: EventOrigin::DirectlyCreated,
        };
        db::insert_event(&mut *tx, &event, None).await.unwrap();
    }
    tx.commit().await.unwrap();
    bc
}

async fn caught_up_to(pool: &Pool, bc: &BoundedContext, name: &str) -> Option<i64> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT caught_up_to FROM \"bc_{}\".snapshot_progress WHERE snapshot_name = $1",
        bc.name
    )))
    .bind(name)
    .fetch_optional(pool)
    .await
    .unwrap()
}

/// Ticks until `dispatcher`'s snapshot is caught up, and reports the rate.
async fn time_catch_up(
    pool: &Pool,
    bc: &BoundedContext,
    label: &str,
    dispatcher: &BenchDispatcher,
) {
    let latest = db::latest_sequence(pool, &bc.name).await.unwrap();
    let started = Instant::now();
    let mut ticks = 0;
    loop {
        db::catch_up_snapshots(pool, &bc.name, dispatcher)
            .await
            .unwrap();
        ticks += 1;
        if caught_up_to(pool, bc, dispatcher.name).await == latest {
            break;
        }
    }
    let elapsed = started.elapsed();

    let (value, expected) = if dispatcher.tag_key == "all" {
        ("x", EVENTS)
    } else {
        ("a0", EVENTS / ACCOUNTS)
    };
    let (_, _, state, _) =
        db::get_snapshot_state(pool, &bc.name, dispatcher.name, dispatcher.tag_key, value)
            .await
            .unwrap()
            .expect("the snapshot row exists");
    assert_eq!(
        state,
        expected.to_string(),
        "{label} folded every event exactly once"
    );
    println!(
        "{label:<12} events={EVENTS} ticks={ticks} elapsed={:>7.0} ms  {:>6.0} events/s",
        elapsed.as_secs_f64() * 1000.0,
        EVENTS as f64 / elapsed.as_secs_f64(),
    );
}

async fn scenario(pool: &Pool, label: &str, dispatcher: BenchDispatcher) {
    let bc = seed(pool).await;
    time_catch_up(pool, &bc, label, &dispatcher).await;
}

#[test]
#[ignore = "benchmark - run explicitly, see the module doc comment"]
fn snapshot_catch_up_throughput() {
    runtime().block_on(async {
        let Some(pool) = pool().await else {
            eprintln!("skipping: no Postgres available");
            return;
        };
        for _ in 0..2 {
            scenario(
                &pool,
                "one",
                BenchDispatcher {
                    name: "One",
                    tag_key: "all",
                    version: 1,
                    partition_count: 1,
                },
            )
            .await;
            scenario(
                &pool,
                "per-account",
                BenchDispatcher {
                    name: "PerAccount",
                    tag_key: "account",
                    version: 1,
                    partition_count: 1,
                },
            )
            .await;
            scenario(
                &pool,
                "partitioned",
                BenchDispatcher {
                    name: "PerAccount",
                    tag_key: "account",
                    version: 1,
                    partition_count: 4,
                },
            )
            .await;

            // Caught up once at version 1, untimed, then refolded at 2:
            // progress stays where it is, so only the rows' resets and
            // the events after them are walked - here, a 5000-event
            // history appended after the first catch-up.
            let bc = seed(&pool).await;
            let first = BenchDispatcher {
                name: "PerAccount",
                tag_key: "account",
                version: 1,
                partition_count: 1,
            };
            while caught_up_to(&pool, &bc, first.name).await
                != db::latest_sequence(&pool, &bc.name).await.unwrap()
            {
                db::catch_up_snapshots(&pool, &bc.name, &first)
                    .await
                    .unwrap();
            }
            reseed_more(&pool, &bc).await;
            time_catch_up(
                &pool,
                &bc,
                "reset",
                &BenchDispatcher {
                    name: "PerAccount",
                    tag_key: "account",
                    version: 2,
                    partition_count: 1,
                },
            )
            .await;
        }
    });
}

/// Appends another `EVENTS` events to `bc`, so a reset snapshot has both
/// history to refold and new events to walk. Their amount is 0, so the
/// folded sums are what `time_catch_up` expects either way.
async fn reseed_more(pool: &Pool, bc: &BoundedContext) {
    let et = db::get_event_type(pool, &bc.name, "Deposited")
        .await
        .unwrap()
        .unwrap();
    let mut tx = pool.begin().await.unwrap();
    for i in 0..EVENTS {
        let sequence = db::next_sequence(&mut *tx, &bc.name).await.unwrap();
        let account = format!("a{}", i % ACCOUNTS);
        let event = Event {
            bounded_context: bc.clone(),
            event_type: et.clone(),
            payload: format!(r#"{{"account":"{account}","amount":0}}"#),
            metadata: Metadata {
                r#type: et.name.clone(),
                version: 1,
                client_id: "bench".to_string(),
                created_at: now(),
                correlation_id: None,
                causation_id: None,
            },
            sequence,
            tags: vec![tag("all", "x".to_string()), tag("account", account)],
            encryption_keys: Vec::new(),
            origin: EventOrigin::DirectlyCreated,
        };
        db::insert_event(&mut *tx, &event, None).await.unwrap();
    }
    tx.commit().await.unwrap();
}
