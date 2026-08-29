//! Verifies that `skilj-core`'s domain engine actually records OTel
//! metrics - this crate never installs a `MeterProvider` itself (see
//! docs/architecture.md's tracing section), so the only way to verify
//! instrumentation is real is to install a throwaway, real
//! `SdkMeterProvider` backed by `opentelemetry_sdk`'s own in-process
//! `InMemoryMetricExporter` (feature `testing`) - no OTel collector
//! needed, same pattern the tracing/logging tests already use for their
//! own signals.
//!
//! `insert_event_and_update_sync_projections` is the representative case
//! exercised here: one of the five call sites `record_event_appended`
//! lives at (`skilj-core/src/db/mod.rs`) - deliberately *not* inside
//! `insert_event` itself, since that runs inside a still-open transaction
//! its own caller might yet roll back; see that function's own doc
//! comment.
//!
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness, same as
//! every other `skilj-core/tests/*.rs` file needing a real Postgres.

use chrono::Utc;
use opentelemetry::global;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventBroadcaster, EventOrigin, EventType,
};
use skilj_core::plugin::ProjectionDispatcher;
use skilj_core::shared::Metadata;

/// No sync projections are ever registered for this test's bounded
/// context, so `insert_event_and_update_sync_projections` never actually
/// calls any of these - a dispatcher recognising nothing is enough.
struct NoopProjectionDispatcher;
impl ProjectionDispatcher for NoopProjectionDispatcher {
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
}

async fn provisioned_pool() -> Option<(db::Pool, Option<postgresql_embedded::PostgreSQL>)> {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        let pool = db::connect(&url).await.ok()?;
        db::migrate(&pool).await.ok()?;
        return Some((pool, None));
    }
    let mut server = postgresql_embedded::PostgreSQL::default();
    if server.setup().await.is_err() || server.start().await.is_err() {
        eprintln!(
            "skipping: DATABASE_URL not set and embedded PostgreSQL setup/start failed \
             (no network egress, or a missing system library like libxml2)"
        );
        return None;
    }
    let database_name = "skilj_metrics_instrumentation_test";
    if server.create_database(database_name).await.is_err() {
        eprintln!("skipping: embedded PostgreSQL create_database failed");
        return None;
    }
    let url = server.settings().url(database_name);
    let pool = db::connect(&url).await.ok()?;
    db::migrate(&pool).await.ok()?;
    Some((pool, Some(server)))
}

#[tokio::test]
async fn appending_an_event_records_the_events_appended_counter() {
    let Some((pool, _embedded)) = provisioned_pool().await else {
        return;
    };

    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone()).build();
    let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();
    global::set_meter_provider(meter_provider.clone());

    let bounded_context_name = format!("metrics_test_{}", skilj_core::shared::generate_token_id());
    let bounded_context = BoundedContext {
        name: bounded_context_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: Utc::now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(&pool, &bounded_context)
        .await
        .expect("inserting a fresh bounded context should succeed");

    let event_type = EventType {
        bounded_context: bounded_context.clone(),
        name: "SomethingHappened".into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        sensitive_fields: Vec::new(),
        external_creation_allowed: false,
        direct_creation_allowed: false,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: false,
    };
    db::upsert_event_type(&pool, &event_type)
        .await
        .expect("registering the event type should succeed");

    let event = Event {
        bounded_context,
        event_type,
        payload: "{}".into(),
        metadata: Metadata {
            r#type: "SomethingHappened".into(),
            version: 1,
            client_id: "test".into(),
            created_at: Utc::now(),
        },
        sequence: 1,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    };

    db::insert_event_and_update_sync_projections(
        &pool,
        &event,
        None,
        &NoopProjectionDispatcher,
        &[],
        &EventBroadcaster::new(4),
        &EventCache::new(4),
    )
    .await
    .expect("appending the event should succeed");

    meter_provider
        .force_flush()
        .expect("the periodic reader's own force_flush shouldn't fail");

    let finished = exporter
        .get_finished_metrics()
        .expect("get_finished_metrics shouldn't fail");
    let data_point_matches = finished
        .iter()
        .flat_map(|rm| rm.scope_metrics())
        .flat_map(|sm| sm.metrics())
        .filter(|m| m.name() == "skilj.events.appended")
        .any(|m| {
            let opentelemetry_sdk::metrics::data::AggregatedMetrics::U64(
                opentelemetry_sdk::metrics::data::MetricData::Sum(sum),
            ) = m.data()
            else {
                return false;
            };
            sum.data_points().any(|dp| {
                dp.value() >= 1
                    && dp.attributes().any(|kv| {
                        kv.key.as_str() == "bounded_context"
                            && kv.value.as_str().as_ref() == bounded_context_name.as_str()
                    })
                    && dp.attributes().any(|kv| {
                        kv.key.as_str() == "event_type"
                            && kv.value.as_str().as_ref() == "SomethingHappened"
                    })
            })
        });

    assert!(
        data_point_matches,
        "expected a skilj.events.appended data point for bounded_context={bounded_context_name:?}, \
         event_type=\"SomethingHappened\""
    );
}
