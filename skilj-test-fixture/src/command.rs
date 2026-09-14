//! `GivenEvents<T: CommandType>` - the command half of this crate's
//! given/when/then harness. See the crate root doc comment for what this
//! deliberately does and doesn't stand in for.

use skilj_core::plugin::CommandType;
use skilj_core::shared::{CommandDecision, EventSpec};

use crate::pretty;

/// Builds the `&[T::Event]` slice `T::decide()` itself takes as
/// `matching_events` - one call to `.event()`/`.events()` per event the
/// test wants `decide()` to see, in the order given (the same order a
/// real `matching_events` arrives in: ascending `Event.sequence`, per
/// `consistency_boundary_and_matching_events`). Nothing here filters or
/// derives tags - the events given *are* the matching events, by
/// construction; see the crate root doc comment for why that's the right
/// scope for a plugin author's own decide() test.
pub struct GivenEvents<T: CommandType> {
    events: Vec<T::Event>,
}

impl<T: CommandType> GivenEvents<T> {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub fn event(mut self, event: T::Event) -> Self {
        self.events.push(event);
        self
    }

    pub fn events(mut self, events: impl IntoIterator<Item = T::Event>) -> Self {
        self.events.extend(events);
        self
    }

    /// Calls `T::decide(&payload, &self.events)` directly - no database,
    /// no HTTP, no access-control check (those are `skilj`'s own
    /// surfaces' job, not `decide()`'s).
    pub fn when(self, payload: T::Payload) -> CommandOutcome {
        CommandOutcome {
            decision: T::decide(&payload, &self.events),
        }
    }
}

impl<T: CommandType> Default for GivenEvents<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// What `.when()` returns - the `then_*` half of given/when/then. Holds
/// the raw `CommandDecision` so `.then()` can assert anything the two
/// canned assertions below don't cover.
pub struct CommandOutcome {
    decision: CommandDecision,
}

impl CommandOutcome {
    /// Asserts `decide()` accepted, and that the accepted `events` match
    /// `expected` exactly - same length, same `event_type`/`payload` at
    /// each index, in order. Compared via `serde_json::Value` rather than
    /// requiring `PartialEq` on `expected`'s payload type, so any
    /// `Serialize` payload works with no extra derive; a mismatch panics
    /// with both sides pretty-printed.
    pub fn then_accepted(self, expected: Vec<EventSpec>) {
        let CommandDecision::Accepted { events: actual } = self.decision else {
            panic!(
                "expected CommandDecision::Accepted with {} event(s), got: {:?}",
                expected.len(),
                self.decision
            );
        };
        if actual.len() != expected.len() {
            panic!(
                "expected {} accepted event(s), got {}:\n  expected: {}\n  actual:   {}",
                expected.len(),
                actual.len(),
                pretty(&events_to_value(&expected)),
                pretty(&events_to_value(&actual)),
            );
        }
        for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
            if a.event_type != e.event_type || a.payload != e.payload {
                panic!(
                    "accepted event {i} didn't match:\n  expected: {}\n  actual:   {}",
                    pretty(&event_to_value(e)),
                    pretty(&event_to_value(a)),
                );
            }
        }
    }

    /// Asserts `decide()` rejected, with the given `kind`. Deliberately
    /// doesn't check `reason` - that's human-facing prose, not something
    /// a test should pin down word for word.
    pub fn then_rejected(self, expected_kind: &str) {
        match &self.decision {
            CommandDecision::Rejected { kind, .. } if kind == expected_kind => {}
            CommandDecision::Rejected { kind, reason } => panic!(
                "expected rejection kind {expected_kind:?}, got {kind:?} (reason: {reason:?})"
            ),
            CommandDecision::Accepted { events } => panic!(
                "expected CommandDecision::Rejected(kind: {expected_kind:?}), got Accepted: {}",
                pretty(&events_to_value(events)),
            ),
        }
    }

    /// Escape hatch: run an arbitrary assertion against the raw
    /// `CommandDecision`, for anything `then_accepted`/`then_rejected`
    /// don't cover.
    pub fn then(self, f: impl FnOnce(&CommandDecision)) -> Self {
        f(&self.decision);
        self
    }

    /// Unwraps the raw decision, for a caller that wants to assert on it
    /// directly rather than through `.then()`.
    pub fn into_decision(self) -> CommandDecision {
        self.decision
    }
}

fn event_to_value(event: &EventSpec) -> serde_json::Value {
    serde_json::json!({ "event_type": event.event_type, "payload": event.payload })
}

fn events_to_value(events: &[EventSpec]) -> serde_json::Value {
    serde_json::Value::Array(events.iter().map(event_to_value).collect())
}
