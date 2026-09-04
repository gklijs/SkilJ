//! Verifies `skilj-rest`'s own `trace_request` middleware records the
//! `http.server.request.duration` histogram, with no real OTel collector
//! needed - a real `SdkMeterProvider` backed by `opentelemetry_sdk`'s own
//! in-process `InMemoryMetricExporter` (feature `testing`), the same
//! pattern `tracing_middleware.rs` uses for its own (trace) signal.
//!
//! One test per file, same reasoning as `tracing_middleware.rs`:
//! `opentelemetry::global::set_meter_provider` is process-wide state, and
//! `cargo test` runs every `#[tokio::test]` in one file concurrently by
//! default.

use axum::body::Body;
use axum::http::Request;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{Event, EventBroadcaster};
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher, SnapshotDispatcher};
use skilj_core::shared::CommandDecision;
use tower::ServiceExt;

struct NoopCommandDispatcher;
impl CommandDispatcher for NoopCommandDispatcher {
    fn dispatch(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        _payload: &str,
        _matching_events: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        None
    }
    fn required_role(
        &self,
        _bounded_context: &str,
        _command_type: &str,
    ) -> Option<Option<&'static str>> {
        None
    }
    fn snapshot_name(
        &self,
        _bounded_context: &str,
        _command_type: &str,
    ) -> Option<Option<&'static str>> {
        None
    }
    fn dispatch_from_snapshot(
        &self,
        _bounded_context: &str,
        _command_type: &str,
        _payload: &str,
        _snapshot_state_json: &str,
        _events_since_snapshot: &[Event],
    ) -> Option<skilj_core::error::Result<CommandDecision>> {
        None
    }
}

struct NoopProjectionDispatcher;
impl ProjectionDispatcher for NoopProjectionDispatcher {
    fn keys(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
        _event: &Event,
    ) -> Option<Vec<String>> {
        None
    }
    fn project(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
        _state_json: &str,
        _event: &Event,
        _key: &str,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }
    fn default_state(&self, _bounded_context: &str, _projection_name: &str) -> Option<String> {
        None
    }
    fn owner_tag_key(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
    ) -> Option<Option<&'static str>> {
        None
    }

    // `team_only` intentionally not overridden - the trait's own default
    // (`None`) already reads identically to what a plain-`None` override
    // here would return.
}

struct NoopSnapshotDispatcher;
impl SnapshotDispatcher for NoopSnapshotDispatcher {
    fn snapshot_names(&self, _bounded_context: &str) -> Vec<&'static str> {
        Vec::new()
    }
    fn tag_key(&self, _bounded_context: &str, _snapshot_name: &str) -> Option<&'static str> {
        None
    }
    fn owner_tag_key(
        &self,
        _bounded_context: &str,
        _snapshot_name: &str,
    ) -> Option<Option<&'static str>> {
        None
    }
    fn version(&self, _bounded_context: &str, _snapshot_name: &str) -> Option<u64> {
        None
    }
    fn fold(
        &self,
        _bounded_context: &str,
        _snapshot_name: &str,
        _state_json: &str,
        _event: &Event,
    ) -> Option<skilj_core::error::Result<String>> {
        None
    }
    fn default_state(&self, _bounded_context: &str, _snapshot_name: &str) -> Option<String> {
        None
    }
}

#[tokio::test]
async fn a_request_records_the_http_server_request_duration_histogram() {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone()).build();
    let meter_provider = SdkMeterProvider::builder().with_reader(reader).build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    // Same `connect_lazy` + short `acquire_timeout` shortcut as
    // `tracing_middleware.rs` - this test only cares about the metric
    // the request records, not a successful response.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(200))
        .connect_lazy("postgres://user:pass@127.0.0.1:1/db")
        .expect("connect_lazy doesn't dial the database, so this can't fail");

    let app = skilj_rest::router(
        pool,
        std::sync::Arc::new(NoopCommandDispatcher),
        std::sync::Arc::new(NoopProjectionDispatcher),
        std::sync::Arc::new(NoopSnapshotDispatcher),
        None,
        EventBroadcaster::new(4),
        EventCache::new(4),
    );

    let request = Request::builder()
        .method("GET")
        .uri("/v1/events")
        .header("authorization", "Bearer fake-id.fake-secret")
        .body(Body::empty())
        .unwrap();

    let _ = app.oneshot(request).await.unwrap();

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
        .filter(|m| m.name() == "http.server.request.duration")
        .any(|m| {
            let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = m.data() else {
                return false;
            };
            histogram.data_points().any(|dp| {
                dp.count() >= 1
                    && dp.attributes().any(|kv| {
                        kv.key.as_str() == "http.request.method"
                            && kv.value.as_str().as_ref() == "GET"
                    })
                    && dp.attributes().any(|kv| {
                        kv.key.as_str() == "http.route"
                            && kv.value.as_str().as_ref() == "/v1/events"
                    })
            })
        });

    assert!(
        data_point_matches,
        "expected an http.server.request.duration data point for GET /v1/events"
    );
}
