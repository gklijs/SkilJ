//! Tests for `EventType`/`CommandType`/`Projection::BOUNDED_CONTEXT` (see
//! each trait's own doc comment in `skilj-core::plugin`) - the associated
//! const `skilj::SkiljBuilder::auto_register()` reads to decide which
//! bounded context a `#[skilj::auto_register]`-tagged impl registers
//! itself under. No database needed, no macro involved: this is a plain
//! associated const with a default, the same shape `requires_role_macro.rs`
//! already tests `CommandType::required_role()` with.

use skilj_core::event_store::Event;
use skilj_core::plugin::{
    BoundedContextEvent, CommandType, EventType, Projection, DEFAULT_BOUNDED_CONTEXT,
};
use skilj_core::shared::CommandDecision;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct FixturePayload {}

/// Never actually converted from a real `Event` in these tests - same
/// "never read" treatment `requires_role_macro.rs`'s own `FixtureEvent`
/// uses.
enum FixtureEvent {}

impl BoundedContextEvent for FixtureEvent {
    fn try_from_event(_event: &Event) -> Option<Result<Self, serde_json::Error>> {
        None
    }
}

struct DefaultScopedCommand;

impl CommandType for DefaultScopedCommand {
    type Payload = FixturePayload;
    type Event = FixtureEvent;
    const NAME: &'static str = "DefaultScopedCommand";
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

struct CustomScopedCommand;

impl CommandType for CustomScopedCommand {
    type Payload = FixturePayload;
    type Event = FixtureEvent;
    const NAME: &'static str = "CustomScopedCommand";
    const BOUNDED_CONTEXT: &'static str = "banking";
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

struct DefaultScopedEvent;

impl EventType for DefaultScopedEvent {
    type Payload = FixturePayload;
    const NAME: &'static str = "DefaultScopedEvent";
}

struct CustomScopedEvent;

impl EventType for CustomScopedEvent {
    type Payload = FixturePayload;
    const NAME: &'static str = "CustomScopedEvent";
    const BOUNDED_CONTEXT: &'static str = "banking";
}

struct DefaultScopedProjection;

impl Projection for DefaultScopedProjection {
    type State = FixturePayload;
    type Event = FixtureEvent;
    const NAME: &'static str = "DefaultScopedProjection";
    fn consumed_event_types() -> Vec<&'static str> {
        vec![]
    }
    fn project(_state: &mut Self::State, _event: &Self::Event, _key: &str) {}
}

struct CustomScopedProjection;

impl Projection for CustomScopedProjection {
    type State = FixturePayload;
    type Event = FixtureEvent;
    const NAME: &'static str = "CustomScopedProjection";
    const BOUNDED_CONTEXT: &'static str = "banking";
    fn consumed_event_types() -> Vec<&'static str> {
        vec![]
    }
    fn project(_state: &mut Self::State, _event: &Self::Event, _key: &str) {}
}

#[test]
fn bounded_context_defaults_to_default_for_every_plugin_trait() {
    assert_eq!(
        DefaultScopedCommand::BOUNDED_CONTEXT,
        DEFAULT_BOUNDED_CONTEXT
    );
    assert_eq!(DefaultScopedEvent::BOUNDED_CONTEXT, DEFAULT_BOUNDED_CONTEXT);
    assert_eq!(
        DefaultScopedProjection::BOUNDED_CONTEXT,
        DEFAULT_BOUNDED_CONTEXT
    );
    assert_eq!(DEFAULT_BOUNDED_CONTEXT, "default");
}

#[test]
fn bounded_context_is_overridable_for_every_plugin_trait() {
    assert_eq!(CustomScopedCommand::BOUNDED_CONTEXT, "banking");
    assert_eq!(CustomScopedEvent::BOUNDED_CONTEXT, "banking");
    assert_eq!(CustomScopedProjection::BOUNDED_CONTEXT, "banking");
}
