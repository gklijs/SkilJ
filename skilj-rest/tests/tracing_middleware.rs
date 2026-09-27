//! Verifies `skilj-rest`'s own tracing wiring end to end, with no real
//! OTel collector needed - `router()`'s `TraceLayer` produces an HTTP
//! span for every request, and its `extract_trace_context` middleware
//! (`routes::extract_trace_context`) makes a request carrying a W3C
//! `traceparent` header continue that trace rather than starting a new
//! one. Uses `opentelemetry_sdk`'s own `testing` exporter (feature-gated,
//! dev-only - see docs/architecture.md's tracing section) to observe the
//! actually-exported `SpanData`, the same real `tracing-opentelemetry`
//! plumbing a consuming application like `skilj-demo` installs, just
//! pointed at an in-process channel instead of a collector.
//!
//! One test per file (not per crate convention, but deliberate here):
//! `opentelemetry::global::set_text_map_propagator` is process-wide
//! state, and `cargo test` runs every `#[tokio::test]` in one file
//! concurrently by default - a second test setting its own propagator
//! would race this one.

use axum::body::Body;
use axum::http::Request;
use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::testing::trace::new_test_exporter;
use opentelemetry_sdk::trace::SdkTracerProvider;
use skilj_core::command_batcher::CommandBatcher;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{Event, EventBroadcaster};
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher, SnapshotDispatcher};
use skilj_core::shared::CommandDecision;
use tower::ServiceExt;
use tracing_subscriber::prelude::*;

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
async fn a_request_with_a_traceparent_header_continues_that_trace() {
    let (exporter, mut rx, _rx_shutdown) = new_test_exporter();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build();
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let tracer = provider.tracer("test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    let _guard = tracing::subscriber::set_default(subscriber);

    // `connect_lazy` never actually dials Postgres until a query runs -
    // fine here, since this test only cares about the span the request
    // produces, not a successful response (every route fails auth
    // against an unreachable pool, which is fine: the request's own span
    // still wraps the whole response, error or not). A short
    // `acquire_timeout` keeps that failure fast rather than waiting out
    // sqlx's own 30s default.
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
        chrono::Duration::minutes(5),
        skilj_core::event_store::DEFAULT_MAX_EVENTS_PER_READ,
        CommandBatcher::new(),
    );

    let injected_trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
    let request = Request::builder()
        .method("GET")
        .uri("/v1/events")
        .header("authorization", "Bearer fake-id.fake-secret")
        .header(
            "traceparent",
            format!("00-{injected_trace_id}-00f067aa0ba902b7-01"),
        )
        .body(Body::empty())
        .unwrap();

    let _ = app.oneshot(request).await.unwrap();

    provider
        .force_flush()
        .expect("the simple exporter's own export is synchronous - nothing to flush should fail");
    // More than one span exports here - `db::access_token_kind` (inside
    // `resolve_token`) is `#[tracing::instrument]`d too, and closes (so
    // exports) before its own parent does - so this finds the *outer*
    // one, named "request" (`trace_request`'s own span name), by name
    // rather than assuming the first message off the channel is it.
    let mut exported_by_name = std::collections::HashMap::new();
    while let Ok(span_data) = rx.try_recv() {
        exported_by_name.insert(span_data.name.clone(), span_data);
    }
    let exported = exported_by_name
        .remove("request")
        .expect("trace_request's own \"request\" span should have exported a SpanData");
    assert_eq!(
        exported.span_context.trace_id().to_string(),
        injected_trace_id,
        "the exported span's trace id should be the one injected via the traceparent header, \
         not a fresh trace root"
    );
    assert_ne!(
        exported.parent_span_id,
        opentelemetry::trace::SpanId::INVALID,
        "the exported span should have a real, non-invalid parent (the injected span id)"
    );

    // The unreachable pool makes `db::access_token_kind` fail with a real
    // `Database` error inside `resolve_token` - a genuine 500, not a
    // business rejection or an auth 4xx - so `trace_request` should have
    // marked this span's own OTel status as errored (§10b's "a 5xx is the
    // one status class that's always a genuine, unexpected server-side
    // failure").
    assert!(
        matches!(exported.status, opentelemetry::trace::Status::Error { .. }),
        "expected the span's status to be Error for a 500 response, got {:?}",
        exported.status
    );
}
