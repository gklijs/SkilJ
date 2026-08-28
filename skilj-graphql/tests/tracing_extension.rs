//! Verifies that registering `async_graphql::extensions::Tracing` on the
//! schema builder (`schema::build`, see its own doc comment) actually
//! produces a `tracing` span per resolved field - the mechanism that
//! covers all 17 resolver modules without any of them needing hand-written
//! spans. Built against a tiny standalone schema here rather than the
//! real production one (`schema::build` needs a live Postgres for
//! `projection_types::build`, which this crate has no fixture for yet -
//! every other test in this repository that needs one goes through
//! `skilj`'s own integration tests instead); what's under test is
//! whether *registering the extension* wires field-level tracing at all,
//! which doesn't depend on which fields the schema happens to have.

use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, Schema, TypeRef};
use std::sync::{Arc, Mutex};
use tracing::field::{Field as TracingField, Visit};
use tracing::span::Attributes;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;

// Same minimal span-capturing `Layer` as `skilj-core`'s own
// `tracing_instrumentation.rs` test - duplicated rather than shared,
// since there's no crate both already depend on that this small a
// helper would justify adding.
type SpanFields = Vec<(String, String)>;

#[derive(Clone, Default)]
struct CapturedSpans(Arc<Mutex<Vec<(String, SpanFields)>>>);

struct CapturingLayer(CapturedSpans);

#[derive(Default)]
struct FieldVisitor(SpanFields);

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &TracingField, value: &dyn std::fmt::Debug) {
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

/// `type Widget { id: String! }` / `type Query { widget: Widget! }` -
/// `widget` returns an `Object` type, so it's traced by default
/// (`Tracing`'s own `trace_scalars: false` skips leaf/scalar-returning
/// fields like `Widget.id` - exercising that distinction here too, not
/// just the happy path).
fn schema() -> Schema {
    let widget = Object::new("Widget").field(Field::new(
        "id",
        TypeRef::named_nn(TypeRef::STRING),
        |ctx| {
            FieldFuture::new(async move {
                let parent = ctx.parent_value.try_downcast_ref::<String>()?;
                Ok(Some(FieldValue::value(parent.clone())))
            })
        },
    ));
    let query =
        Object::new("Query").field(Field::new("widget", TypeRef::named_nn("Widget"), |_ctx| {
            FieldFuture::new(async move { Ok(Some(FieldValue::value("w1".to_string()))) })
        }));

    Schema::build("Query", None, None)
        .register(query)
        .register(widget)
        .extension(async_graphql::extensions::Tracing)
        .finish()
        .expect("this schema's own shape is fixed and valid")
}

#[tokio::test]
async fn a_resolved_object_returning_field_emits_a_tracing_span() {
    let captured = CapturedSpans::default();
    let subscriber = tracing_subscriber::registry().with(CapturingLayer(captured.clone()));
    // `set_default`, not `with_default`: the spans under test are
    // created while `schema.execute(...)`'s future is *polled*, not when
    // it's constructed - the guard needs to stay live across the
    // `.await` itself, which `with_default`'s sync-only closure can't do.
    let _guard = tracing::subscriber::set_default(subscriber);

    let _ = schema().execute("{ widget { id } }").await;

    let spans = captured.0.lock().unwrap();
    let field_spans: Vec<_> = spans.iter().filter(|(name, _)| name == "field").collect();

    assert_eq!(
        field_spans.len(),
        1,
        "expected exactly one \"field\" span - the Object-returning `widget` field; \
         `Widget.id` returns a scalar, which `Tracing`'s own default `trace_scalars: false` \
         should skip. Got {field_spans:?}"
    );
    assert!(
        field_spans[0]
            .1
            .iter()
            .any(|(k, v)| k == "path" && v.contains("widget")),
        "expected the one field span to be for `widget`, got {field_spans:?}"
    );
}
