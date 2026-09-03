//! Verifies that `skilj-core`'s domain engine actually emits `tracing`
//! spans - this crate never installs a subscriber itself (see
//! docs/architecture.md's tracing section), so the only way to verify
//! instrumentation is real is to install a throwaway, in-process
//! capturing one for the duration of one test, exactly as a real
//! consuming application's own subscriber (`skilj-demo`'s
//! `init_telemetry`) would observe spans - no OTel collector needed.
//!
//! `process_command` is the one representative case exercised here: the
//! domain engine's real entry point (docs/architecture.md), instrumented
//! with explicit `bounded_context`/`command_type` fields rather than the
//! mechanical `skip_all` every `skilj-core::db` function otherwise gets.

use chrono::Utc;
use skilj_core::event_store::{self, BoundedContext, BoundedContextStatus, CommandType};
use skilj_core::shared::{CommandDecision, EventSpec};
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::Attributes;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

// ---------------------------------------------------------------------
// A minimal in-process span-capturing `Layer` - records every span's
// name and its fields (as debug-formatted strings) the moment it's
// created. Good enough for asserting "this span happened, with these
// fields" without a real exporter/collector.
// ---------------------------------------------------------------------

type SpanFields = Vec<(String, String)>;

#[derive(Clone, Default)]
struct CapturedSpans(Arc<Mutex<Vec<(String, SpanFields)>>>);

impl CapturedSpans {
    fn find(&self, name: &str) -> Option<SpanFields> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|(span_name, _)| span_name == name)
            .map(|(_, fields)| fields.clone())
    }
}

struct CapturingLayer(CapturedSpans);

#[derive(Default)]
struct FieldVisitor(SpanFields);

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }
}

impl<S: tracing::Subscriber> Layer<S> for CapturingLayer {
    fn on_new_span(&self, attrs: &Attributes<'_>, _id: &tracing::span::Id, _ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);
        self.0
             .0
            .lock()
            .unwrap()
            .push((attrs.metadata().name().to_string(), visitor.0));
    }
}

// ---------------------------------------------------------------------
// Fixtures - the minimal shape `process_command` needs, same pattern
// `command_processing.rs` uses.
// ---------------------------------------------------------------------

fn bounded_context() -> BoundedContext {
    BoundedContext {
        name: "accounts".into(),
        status: BoundedContextStatus::Active,
        created_at: Utc.timestamp_opt(0, 0).unwrap(),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
        template: None,
    }
}

fn command_type() -> CommandType {
    CommandType {
        bounded_context: bounded_context(),
        name: "WithdrawFunds".into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        rest_trigger_allowed: true,
    }
}

use chrono::TimeZone;

#[test]
fn process_command_emits_a_span_with_bounded_context_and_command_type_fields() {
    let captured = CapturedSpans::default();
    let subscriber = tracing_subscriber::registry().with(CapturingLayer(captured.clone()));

    tracing::subscriber::with_default(subscriber, || {
        let ct = command_type();
        let _ = event_store::process_command(
            skilj_core::shared::generate_token_id(),
            &ct,
            "{}",
            "trigger-adapter",
            &[],
            CommandDecision::Accepted {
                events: Vec::<EventSpec>::new(),
            },
            |_| None,
            || 1,
            Utc.timestamp_opt(1000, 0).unwrap(),
            |_, _| unreachable!("no sensitive fields in this test"),
        );
    });

    let fields = captured
        .find("process_command")
        .expect("process_command should have emitted a span named \"process_command\"");
    assert!(
        fields
            .iter()
            .any(|(k, v)| k == "bounded_context" && v == "accounts"),
        "expected a bounded_context=accounts field, got {fields:?}"
    );
    assert!(
        fields
            .iter()
            .any(|(k, v)| k == "command_type" && v == "WithdrawFunds"),
        "expected a command_type=WithdrawFunds field, got {fields:?}"
    );
}
