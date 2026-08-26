//! Verifies `SkiljBuilder::build`'s two background `tokio::spawn` loops
//! (the async-projection poller and the scheduler - `skilj/src/lib.rs`)
//! each record the `skilj.background_task.tick.duration` histogram, with
//! no OTel collector involved - a real `SdkMeterProvider` backed by
//! `opentelemetry_sdk`'s own in-process `InMemoryMetricExporter`
//! (feature `testing`), the same pattern
//! `tracing_background_tasks.rs` uses for its own (trace) signal.
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness, same as every
//! other `skilj/tests/*.rs` file - `.build()` itself is what spawns the
//! tasks under test, so this can't be a pure-unit test.

use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use skilj::Skilj;
use skilj_core::db::{self, Pool};
use std::time::Duration;

async fn provisioned_database_url() -> Option<(String, Option<postgresql_embedded::PostgreSQL>)> {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        return Some((url, None));
    }
    let mut server = postgresql_embedded::PostgreSQL::default();
    if server.setup().await.is_err() || server.start().await.is_err() {
        eprintln!(
            "skipping: DATABASE_URL not set and embedded PostgreSQL setup/start failed \
             (no network egress, or a missing system library like libxml2)"
        );
        return None;
    }
    let database_name = "skilj_metrics_background_tasks_test";
    if server.create_database(database_name).await.is_err() {
        eprintln!("skipping: embedded PostgreSQL create_database failed");
        return None;
    }
    let url = server.settings().url(database_name);
    Some((url, Some(server)))
}

#[tokio::test]
async fn each_background_loop_records_the_tick_duration_histogram() {
    let Some((database_url, _embedded)) = provisioned_database_url().await else {
        return;
    };
    let pool: Pool = match db::connect(&database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting failed: {e}");
            return;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating failed: {e}");
        return;
    }

    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone()).build();
    let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    let (_skilj, _report) = Skilj::builder(database_url)
        .async_projection_poll_interval(Duration::from_millis(20))
        .scheduler_poll_interval(Duration::from_millis(20))
        .build()
        .await
        .expect("an empty builder - no registrations, no reconciliation role - should build fine");

    // Both loops run their first tick immediately, before their first
    // sleep (`skilj/src/lib.rs`'s own doc comments on each spawned
    // task) - this just needs to give the runtime a chance to actually
    // poll those spawned tasks at all.
    tokio::time::sleep(Duration::from_millis(150)).await;

    meter_provider
        .force_flush()
        .expect("the periodic reader's own force_flush shouldn't fail");

    let finished = exporter
        .get_finished_metrics()
        .expect("get_finished_metrics shouldn't fail");
    let has_tick_for = |task: &str| {
        finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .filter(|m| m.name() == "skilj.background_task.tick.duration")
            .any(|m| {
                let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = m.data() else {
                    return false;
                };
                histogram.data_points().any(|dp| {
                    dp.count() >= 1
                        && dp
                            .attributes()
                            .any(|kv| kv.key.as_str() == "task" && kv.value.as_str().as_ref() == task)
                })
            })
    };

    assert!(
        has_tick_for("async_projection"),
        "expected a tick.duration data point for task=async_projection"
    );
    assert!(
        has_tick_for("scheduler"),
        "expected a tick.duration data point for task=scheduler"
    );
}
