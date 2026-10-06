//! Async projection catch-up benchmark for Codeberg issue #52 - not a
//! test, so `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --release -p skilj-core --test projection_catch_up_throughput -- --ignored --nocapture
//! ```
//!
//! (`DATABASE_URL` to run against a server of your own, otherwise the
//! embedded one.) Each scenario commits `EVENTS` events into a fresh
//! bounded context first, then registers its async projections and times
//! `db::catch_up_bounded_context` - called back to back, as the background
//! task would with no idle wait - until every projection has caught up.
//! The projections:
//!
//! - **total**: one key for the whole bounded context, so every event
//!   folds into the same row;
//! - **per-account**: keyed by the event's account, `ACCOUNTS` of them;
//! - **both**: the two above, plus a copy of each - four projections
//!   walked by the same tick.
//!
//! See docs/architecture.md §182 for the numbers.

use chrono::{SubsecRound, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::plugin::ProjectionDispatcher;
use skilj_core::projections::Projection;
use skilj_core::shared::{generate_token_id, Metadata};
use std::time::Instant;

/// Events per scenario.
const EVENTS: i64 = 5000;
/// Distinct accounts the per-account projections are keyed by.
const ACCOUNTS: i64 = 500;

struct BenchDispatcher;

impl ProjectionDispatcher for BenchDispatcher {
    fn keys(&self, _bc: &str, projection_name: &str, event: &Event) -> Option<Vec<String>> {
        match projection_name {
            "Total" | "TotalCopy" => Some(vec![String::new()]),
            "PerAccount" | "PerAccountCopy" => {
                let payload: serde_json::Value = serde_json::from_str(&event.payload).ok()?;
                Some(vec![payload["account"].as_str()?.to_string()])
            }
            _ => None,
        }
    }

    fn project(
        &self,
        _bc: &str,
        projection_name: &str,
        state_json: &str,
        event: &Event,
        _key: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        self.default_state(_bc, projection_name)?;
        let current: i64 = state_json.parse().unwrap_or(0);
        let payload: serde_json::Value =
            serde_json::from_str(&event.payload).expect("bench payload is JSON");
        let amount = payload["amount"].as_i64().unwrap_or(0);
        Some(Ok((current + amount).to_string()))
    }

    fn default_state(&self, _bc: &str, projection_name: &str) -> Option<String> {
        matches!(
            projection_name,
            "Total" | "TotalCopy" | "PerAccount" | "PerAccountCopy"
        )
        .then(|| "0".to_string())
    }

    fn owner_tag_key(&self, bc: &str, projection_name: &str) -> Option<Option<&'static str>> {
        self.default_state(bc, projection_name).map(|_| None)
    }

    fn team_only(&self, bc: &str, projection_name: &str) -> Option<Option<&'static str>> {
        self.default_state(bc, projection_name).map(|_| None)
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("tokio runtime"))
}

async fn pool() -> Option<Pool> {
    let url = skilj_test_support::database_url("skilj_projection_catch_up_throughput").await?;
    let pool = db::connect(&url).await.ok()?;
    db::migrate(&pool).await.ok()?;
    Some(pool)
}

fn now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn seed(pool: &Pool) -> (BoundedContext, EventType) {
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
        let event = Event {
            bounded_context: bc.clone(),
            event_type: et.clone(),
            payload: format!(r#"{{"account":"a{}","amount":1}}"#, i % ACCOUNTS),
            metadata: Metadata {
                r#type: et.name.clone(),
                version: 1,
                client_id: "bench".to_string(),
                created_at: now(),
                correlation_id: None,
                causation_id: None,
            },
            sequence,
            tags: Vec::new(),
            encryption_keys: Vec::new(),
            origin: EventOrigin::DirectlyCreated,
        };
        db::insert_event(&mut *tx, &event, None).await.unwrap();
    }
    tx.commit().await.unwrap();
    (bc, et)
}

async fn scenario(pool: &Pool, label: &str, projections: &[&str]) {
    let (bc, et) = seed(pool).await;
    for name in projections {
        db::upsert_projection(
            pool,
            &Projection {
                bounded_context: bc.clone(),
                name: name.to_string(),
                schema: r#"{"properties":{}}"#.to_string(),
                schema_version: 1,
                consumed_event_types: vec![et.clone()],
                sync: false,
                caught_up_to: None,
            },
        )
        .await
        .unwrap();
    }
    let latest = db::latest_sequence(pool, &bc.name).await.unwrap();

    let started = Instant::now();
    let mut ticks = 0;
    loop {
        db::catch_up_bounded_context(pool, &bc.name, &BenchDispatcher)
            .await
            .unwrap();
        ticks += 1;
        let mut done = true;
        for name in projections {
            let projection = db::get_projection(pool, &bc.name, name)
                .await
                .unwrap()
                .unwrap();
            done &= projection.caught_up_to == latest;
        }
        if done {
            break;
        }
    }
    let elapsed = started.elapsed();

    for name in projections {
        let expected = if name.starts_with("Total") {
            EVENTS
        } else {
            EVENTS / ACCOUNTS
        };
        let key = if name.starts_with("Total") { "" } else { "a0" };
        assert_eq!(
            db::get_projection_state(pool, &bc.name, name, key)
                .await
                .unwrap(),
            Some(expected.to_string()),
            "{name} folded every event exactly once"
        );
    }
    println!(
        "{label:<12} projections={} events={EVENTS} ticks={ticks} elapsed={:>7.0} ms  \
         {:>6.0} events/s",
        projections.len(),
        elapsed.as_secs_f64() * 1000.0,
        EVENTS as f64 / elapsed.as_secs_f64(),
    );
}

#[test]
#[ignore = "benchmark - run explicitly, see the module doc comment"]
fn async_projection_catch_up_throughput() {
    runtime().block_on(async {
        let Some(pool) = pool().await else {
            eprintln!("skipping: no Postgres available");
            return;
        };
        for _ in 0..2 {
            scenario(&pool, "total", &["Total"]).await;
            scenario(&pool, "per-account", &["PerAccount"]).await;
            scenario(
                &pool,
                "both",
                &["Total", "TotalCopy", "PerAccount", "PerAccountCopy"],
            )
            .await;
        }
    });
}
