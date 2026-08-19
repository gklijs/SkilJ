//! Tests for `event_store::EventBroadcaster` - the real-time delivery
//! mechanism `EventSubscription` needs (docs/architecture.md's own
//! write-up of this pass, and the plan at
//! `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`). No
//! database involved: `EventBroadcaster` is a thin, pool-independent
//! wrapper around `tokio::sync::broadcast`, so this exercises it
//! directly, the same "test the mechanism in isolation" shape
//! `tests/event_subscription.rs` already gives the pure
//! `deliver_to_subscriptions` rule it's paired with. Fixture shapes
//! mirror that file's own `bounded_context`/`event_type`/`event`
//! helpers exactly.

use chrono::{TimeZone, Utc};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, Event, EventBroadcaster, EventOrigin, EventType,
};
use skilj_core::shared::Metadata;

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn bounded_context() -> BoundedContext {
    BoundedContext {
        name: "orders".into(),
        status: BoundedContextStatus::Active,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
    }
}

fn event_type() -> EventType {
    EventType {
        bounded_context: bounded_context(),
        name: "OrderPlaced".into(),
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
    }
}

fn event(sequence: i64) -> Event {
    Event {
        bounded_context: bounded_context(),
        event_type: event_type(),
        payload: format!("{{\"n\":{sequence}}}"),
        metadata: Metadata {
            r#type: "OrderPlaced".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: timestamp(0),
        },
        sequence,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

/// The basic contract: publishing after a `subscribe()` reaches that
/// receiver, unchanged.
#[tokio::test]
async fn publish_reaches_a_subscribed_receiver() {
    let broadcaster = EventBroadcaster::new(16);
    let e = event(1);

    let mut rx = broadcaster.subscribe();
    broadcaster.publish(&e);

    let received = rx.recv().await.unwrap();
    assert_eq!(received, e);
}

/// `publish`'s own doc comment: zero receivers is the expected steady
/// state, not an error - nothing to assert beyond "this doesn't panic".
#[test]
fn publish_with_zero_receivers_does_not_panic() {
    let broadcaster = EventBroadcaster::new(16);
    broadcaster.publish(&event(1));
}

/// Real fan-out, not single-consumer: two independent subscribers each
/// see the same event, on their own receiver.
#[tokio::test]
async fn two_subscribers_both_receive_the_same_event() {
    let broadcaster = EventBroadcaster::new(16);
    let e = event(1);

    let mut rx_a = broadcaster.subscribe();
    let mut rx_b = broadcaster.subscribe();
    broadcaster.publish(&e);

    assert_eq!(rx_a.recv().await.unwrap(), e);
    assert_eq!(rx_b.recv().await.unwrap(), e);
}

/// A subscriber that falls more than `capacity` events behind gets
/// `RecvError::Lagged` on its next `recv()` - the signal
/// `resolvers::event_subscription` treats as "end this stream", per
/// `DeliveryIsAtMostOnce`.
#[tokio::test]
async fn a_lagging_subscriber_gets_a_lagged_error() {
    let broadcaster = EventBroadcaster::new(2);

    let mut rx = broadcaster.subscribe();
    for seq in 1..=5 {
        broadcaster.publish(&event(seq));
    }

    let result = rx.recv().await;
    assert!(matches!(
        result,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
    ));
}
