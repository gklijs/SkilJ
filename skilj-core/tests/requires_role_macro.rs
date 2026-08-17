//! Tests for `#[skilj_core::plugin::requires_role(...)]` (the
//! `skilj-macros` crate) - the one deliberate proc-macro exception to
//! `skilj-core`'s otherwise macro-free plugin API (docs/architecture.md
//! §1.3.1). No database needed: `CommandType::required_role()` is a
//! plain `&'static str` baked in at compile time, nothing this pass
//! persists (see that method's own doc comment).

use skilj_core::event_store::Event;
use skilj_core::plugin::{requires_role, BoundedContextEvent, CommandType};
use skilj_core::shared::CommandDecision;

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct FixturePayload {}

/// Never actually converted from a real `Event` in these tests - just
/// enough of a type to satisfy `CommandType::Event`'s own
/// `BoundedContextEvent` bound, the same "never read" treatment
/// `command_trigger.rs`'s own `BankingEvent` fixture uses.
enum FixtureEvent {}

impl BoundedContextEvent for FixtureEvent {
    fn try_from_event(_event: &Event) -> Option<Result<Self, serde_json::Error>> {
        None
    }
}

struct GatedCommand;

#[requires_role("treasury_officer")]
impl CommandType for GatedCommand {
    type Payload = FixturePayload;
    type Event = FixtureEvent;
    const NAME: &'static str = "GatedCommand";
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

struct UngatedCommand;

impl CommandType for UngatedCommand {
    type Payload = FixturePayload;
    type Event = FixtureEvent;
    const NAME: &'static str = "UngatedCommand";
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

#[test]
fn requires_role_overrides_the_trait_default() {
    assert_eq!(GatedCommand::required_role(), Some("treasury_officer"));
}

/// The whole point of a default trait method: a `CommandType` that never
/// writes `#[requires_role(...)]` at all keeps today's behavior exactly -
/// `None`, no extra gate beyond the ordinary write-level grant.
#[test]
fn required_role_defaults_to_none_without_the_attribute() {
    assert_eq!(UngatedCommand::required_role(), None);
}
