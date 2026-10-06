//! Verifies `SkiljBuilder::build`'s two background `tokio::spawn` loops
//! (the async-projection poller and the scheduler - `skilj/src/lib.rs`)
//! each emit a `tracing` span per tick, with no HTTP request or OTel
//! collector involved - just an in-process capturing subscriber, the
//! same pattern `skilj-core`'s own `tracing_instrumentation.rs` and
//! `skilj-rest`'s `tracing_middleware.rs` use. `DATABASE_URL`-then-
//! embedded-Postgres-then-skip harness, same as every other
//! `skilj/tests/*.rs` file (see `skilj/tests/command_trigger.rs`'s own
//! doc comment for the details) - `.build()` itself is what spawns the
//! tasks under test, so this can't be a pure-unit test.

use skilj::Skilj;
use skilj_core::db::{self, Pool};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

// Same minimal span-capturing `Layer` as `skilj-core`/`skilj-graphql`'s
// own tracing tests - duplicated rather than shared, per this
// repository's existing "each test file owns its own fixtures"
// convention (see e.g. `skilj/tests/auto_register.rs`'s own doc comment
// on its provisioning harness).
#[derive(Clone, Default)]
struct CapturedSpans(Arc<Mutex<Vec<String>>>);

struct CapturingLayer(CapturedSpans);

// This test only needs span *names*, not fields - a no-op `Visit` is
// enough to satisfy `Attributes::record`'s signature.
struct NoopVisitor;
impl Visit for NoopVisitor {
    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

impl<S: tracing::Subscriber> Layer<S> for CapturingLayer {
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &tracing::span::Id, _ctx: Context<'_, S>) {
        attrs.record(&mut NoopVisitor);
        self.0
             .0
            .lock()
            .unwrap()
            .push(attrs.metadata().name().to_string());
    }
}

async fn provisioned_database_url() -> Option<String> {
    skilj_test_support::database_url("skilj_tracing_background_tasks_test").await
}

#[tokio::test]
async fn each_background_loop_emits_a_span_per_tick() {
    let Some(database_url) = provisioned_database_url().await else {
        return;
    };
    let pool: Pool = match db::connect(&database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to the test database failed: {e}");
            return;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating the test database failed: {e}");
        return;
    }

    let captured = CapturedSpans::default();
    let subscriber = tracing_subscriber::registry().with(CapturingLayer(captured.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);

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

    let spans = captured.0.lock().unwrap();
    assert!(
        spans.iter().any(|name| name == "async_projection_tick"),
        "expected at least one async_projection_tick span, got {spans:?}"
    );
    assert!(
        spans.iter().any(|name| name == "scheduler_tick"),
        "expected at least one scheduler_tick span, got {spans:?}"
    );
}
