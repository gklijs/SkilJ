//! Proves `GivenEvents`'s own builder/assertion logic - a tiny hand-rolled
//! bounded context, not a real skilj deployment. `skilj-demo/tests/
//! banking_fixture.rs` is the real-world adopter proof; this file is
//! about the fixture crate itself.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use skilj_core::event_store::Event;
use skilj_core::plugin::{BoundedContextEvent, CommandType, Decision, Projection, Snapshot};
use skilj_core::shared::{CommandDecision, EventSpec, QueryItemMapping};

use skilj_test_fixture::command::GivenEvents as GivenCommandEvents;
use skilj_test_fixture::projection::GivenEvents as GivenProjectionEvents;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct IncrementedPayload {
    amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct DecrementedPayload {
    amount: i64,
}

enum CounterEvent {
    Incremented(IncrementedPayload),
    Decremented(DecrementedPayload),
}

impl BoundedContextEvent for CounterEvent {
    fn try_from_event(_event: &Event) -> Option<Result<Self, serde_json::Error>> {
        // Never exercised by these tests - GivenEvents builds `Self::Event`
        // directly, never through a raw `Event`.
        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct AdjustCounterPayload {
    delta: i64,
}

struct AdjustCounter;

impl CommandType for AdjustCounter {
    type Payload = AdjustCounterPayload;
    type Event = CounterEvent;
    const NAME: &'static str = "AdjustCounter";

    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        if payload.delta == 0 {
            return CommandDecision::Rejected {
                reason: "delta must not be zero".into(),
                kind: "zero_delta".into(),
            };
        }
        let total: i64 = matching_events
            .iter()
            .map(|e| match e {
                CounterEvent::Incremented(p) => p.amount,
                CounterEvent::Decremented(p) => -p.amount,
            })
            .sum();
        if total + payload.delta < 0 {
            return CommandDecision::Rejected {
                reason: "counter would go negative".into(),
                kind: "would_go_negative".into(),
            };
        }
        let event = if payload.delta > 0 {
            EventSpec {
                event_type: "Incremented".into(),
                payload: serde_json::json!({ "amount": payload.delta }),
            }
        } else {
            EventSpec {
                event_type: "Decremented".into(),
                payload: serde_json::json!({ "amount": -payload.delta }),
            }
        };
        CommandDecision::Accepted {
            events: vec![event],
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct CounterTotalState {
    total: i64,
}

struct CounterTotal;

impl Projection for CounterTotal {
    type State = CounterTotalState;
    type Event = CounterEvent;
    const NAME: &'static str = "CounterTotal";

    fn consumed_event_types() -> Vec<&'static str> {
        vec!["Incremented", "Decremented"]
    }

    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            CounterEvent::Incremented(p) => state.total += p.amount,
            CounterEvent::Decremented(p) => state.total -= p.amount,
        }
    }
}

/// The counter's total as a snapshot, for `AdjustCounterFast`.
struct CounterSnapshot;

impl Snapshot for CounterSnapshot {
    type State = CounterTotalState;
    type Event = CounterEvent;
    const NAME: &'static str = "CounterSnapshot";
    const TAG_KEY: &'static str = "counter";
    const VERSION: u64 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct AdjustNamedCounterPayload {
    counter: String,
    delta: i64,
}

/// Decides on the counter's total from a snapshot item
/// (docs/architecture.md §202) rather than from events.
struct AdjustCounterFast;

impl CommandType for AdjustCounterFast {
    type Payload = AdjustNamedCounterPayload;
    type Event = CounterEvent;
    const NAME: &'static str = "AdjustCounterFast";

    fn consistency_query() -> Vec<QueryItemMapping> {
        vec![QueryItemMapping::snapshot::<CounterSnapshot>("counter")]
    }

    fn decide_with(
        payload: &Self::Payload,
        decision: &Decision<'_, Self::Event>,
    ) -> CommandDecision {
        let total = decision
            .snapshot::<CounterSnapshot>(&payload.counter)
            .map_or(0, |state| state.total);
        if total + payload.delta < 0 {
            return CommandDecision::Rejected {
                reason: "counter would go negative".into(),
                kind: "would_go_negative".into(),
            };
        }
        CommandDecision::Accepted { events: vec![] }
    }
}

/// Reads the injected time and deadline context (Codeberg issue #97) and
/// reports what it saw through its rejection kind/reason, so a test can
/// assert on it.
struct TimeAware;

impl CommandType for TimeAware {
    type Payload = AdjustCounterPayload;
    type Event = CounterEvent;
    const NAME: &'static str = "TimeAware";

    fn decide_with(
        _payload: &Self::Payload,
        decision: &Decision<'_, Self::Event>,
    ) -> CommandDecision {
        match decision.deadline() {
            Some(deadline) if deadline.expired => CommandDecision::Rejected {
                reason: decision.now().to_rfc3339(),
                kind: "expired".into(),
            },
            Some(_) => CommandDecision::Rejected {
                reason: decision.now().to_rfc3339(),
                kind: "on_time".into(),
            },
            None => CommandDecision::Rejected {
                reason: decision.now().to_rfc3339(),
                kind: "no_deadline".into(),
            },
        }
    }
}

fn ts(secs: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(secs, 0).unwrap()
}

/// The injected `now` (not the wall clock) reaches `decide()`, and an
/// ordinary submission carries no deadline (Codeberg issue #97).
#[test]
fn an_injected_now_reaches_decide_with_no_deadline() {
    let now = ts(1_700_000_000);
    GivenCommandEvents::<TimeAware>::new()
        .now(now)
        .when(AdjustCounterPayload { delta: 1 })
        .then(|decision| match decision {
            CommandDecision::Rejected { reason, kind } => {
                assert_eq!(kind, "no_deadline");
                assert_eq!(reason, &now.to_rfc3339());
            }
            CommandDecision::Accepted { .. } => panic!("expected a rejection"),
        });
}

/// A deadline-fired decision fired past its `expires_at` is `expired`.
#[test]
fn a_late_deadline_is_expired() {
    GivenCommandEvents::<TimeAware>::new()
        .now(ts(2500))
        .deadline(ts(1000), Some(ts(2000)))
        .when(AdjustCounterPayload { delta: 1 })
        .then_rejected("expired");
}

/// A deadline-fired decision fired before its `expires_at` is not.
#[test]
fn an_on_time_deadline_is_not_expired() {
    GivenCommandEvents::<TimeAware>::new()
        .now(ts(1500))
        .deadline(ts(1000), Some(ts(2000)))
        .when(AdjustCounterPayload { delta: 1 })
        .then_rejected("on_time");
}

/// A deadline with no expiry is never expired.
#[test]
fn a_deadline_without_an_expiry_is_not_expired() {
    GivenCommandEvents::<TimeAware>::new()
        .now(ts(2500))
        .deadline(ts(1000), None)
        .when(AdjustCounterPayload { delta: 1 })
        .then_rejected("on_time");
}

#[test]
fn a_given_snapshot_state_reaches_decide_with() {
    GivenCommandEvents::<AdjustCounterFast>::new()
        .snapshot::<CounterSnapshot>("c1", CounterTotalState { total: 3 })
        .when(AdjustNamedCounterPayload {
            counter: "c1".into(),
            delta: -5,
        })
        .then_rejected("would_go_negative");
    GivenCommandEvents::<AdjustCounterFast>::new()
        .snapshot::<CounterSnapshot>("c1", CounterTotalState { total: 7 })
        .when(AdjustNamedCounterPayload {
            counter: "c1".into(),
            delta: -5,
        })
        .then_accepted(vec![]);
}

#[test]
fn accepts_and_matches_expected_event() {
    GivenCommandEvents::<AdjustCounter>::new()
        .event(CounterEvent::Incremented(IncrementedPayload { amount: 10 }))
        .when(AdjustCounterPayload { delta: 5 })
        .then_accepted(vec![EventSpec {
            event_type: "Incremented".into(),
            payload: serde_json::json!({ "amount": 5 }),
        }]);
}

#[test]
fn rejects_with_matching_kind() {
    GivenCommandEvents::<AdjustCounter>::new()
        .when(AdjustCounterPayload { delta: 0 })
        .then_rejected("zero_delta");
}

#[test]
fn given_events_scope_the_decision() {
    // Without the given Incremented(3), delta: -5 would go negative.
    GivenCommandEvents::<AdjustCounter>::new()
        .event(CounterEvent::Incremented(IncrementedPayload { amount: 3 }))
        .when(AdjustCounterPayload { delta: -5 })
        .then_rejected("would_go_negative");
}

#[test]
fn escape_hatch_sees_raw_decision() {
    GivenCommandEvents::<AdjustCounter>::new()
        .when(AdjustCounterPayload { delta: 7 })
        .then(|decision| match decision {
            CommandDecision::Accepted { events } => assert_eq!(events.len(), 1),
            CommandDecision::Rejected { .. } => panic!("expected acceptance"),
        });
}

#[test]
#[should_panic(expected = "expected CommandDecision::Rejected")]
fn then_rejected_panics_on_unexpected_acceptance() {
    GivenCommandEvents::<AdjustCounter>::new()
        .when(AdjustCounterPayload { delta: 7 })
        .then_rejected("zero_delta");
}

#[test]
#[should_panic(expected = "accepted event 0 didn't match")]
fn then_accepted_panics_on_payload_mismatch() {
    GivenCommandEvents::<AdjustCounter>::new()
        .when(AdjustCounterPayload { delta: 7 })
        .then_accepted(vec![EventSpec {
            event_type: "Incremented".into(),
            payload: serde_json::json!({ "amount": 999 }),
        }]);
}

#[test]
fn projection_folds_given_events_in_order() {
    GivenProjectionEvents::<CounterTotal>::new()
        .event(CounterEvent::Incremented(IncrementedPayload { amount: 10 }))
        .event(CounterEvent::Decremented(DecrementedPayload { amount: 4 }))
        .then_state(CounterTotalState { total: 6 });
}

#[test]
#[should_panic(expected = "projected state didn't match")]
fn then_state_panics_on_mismatch() {
    GivenProjectionEvents::<CounterTotal>::new()
        .event(CounterEvent::Incremented(IncrementedPayload { amount: 10 }))
        .then_state(CounterTotalState { total: 0 });
}

#[test]
fn projection_escape_hatch() {
    GivenProjectionEvents::<CounterTotal>::new()
        .event(CounterEvent::Incremented(IncrementedPayload { amount: 10 }))
        .then(|state| assert_eq!(state.total, 10));
}
