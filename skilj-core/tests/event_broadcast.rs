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
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, Event, EventBroadcaster, EventOrigin, EventType,
    Subscription,
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
        template: None,
    }
}

fn event_type() -> EventType {
    EventType {
        bounded_context: bounded_context(),
        name: "OrderPlaced".into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
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

fn access_mapping() -> RoleAccessMapping {
    RoleAccessMapping {
        role: Role {
            id: "role-1".into(),
            external_subject: "someone@example.com".into(),
            name: "Someone".into(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: timestamp(0),
            revoked_at: None,
        },
        bounded_context: bounded_context(),
        level: AccessLevel::Read,
        can_read_sensitive: false,
        scope: None,
        status: RoleStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
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

/// Pins the exact hazard drift audit finding #7 (2026-08-20, see project
/// memory `skilj-drift-audit-2026-08-20`) fixed by reordering
/// `resolvers::event_subscription`'s own `subscribe()`/snapshot-read
/// calls: `subscribe()` never replays anything published before it - an
/// event published first is simply gone for a receiver created
/// afterwards, not queued or backfilled. This is the reason the resolver
/// must subscribe *before* it reads its own `bounded_context_events`
/// snapshot, not merely documentation of a `tokio::sync::broadcast`
/// detail already covered elsewhere.
#[tokio::test]
async fn subscribing_after_an_event_is_published_misses_it_entirely() {
    let broadcaster = EventBroadcaster::new(16);

    broadcaster.publish(&event(1));
    let mut rx = broadcaster.subscribe();

    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

/// The other half of drift audit finding #7 - subscribing *before*
/// reading the snapshot closes the gap without opening a duplicate-
/// delivery one in its place. An event published in the window between
/// `subscribe()` and the snapshot read (simulated here by literal
/// sequencing, not a timing race: the two really can happen in either
/// order in production, and both must be handled correctly) is real
/// history-committed-before-the-snapshot-finished, so it lands in
/// `bounded_context_events` and sets `from_sequence` to its own
/// sequence - but it also already reached this receiver live, via `rx`,
/// since it was published after `subscribe()`. `deliver_to_subscriptions`'s
/// own `starting_sequence() < event.sequence` filter is what keeps that
/// from being delivered twice: `from_sequence` already covers it, so the
/// live copy is correctly dropped, not re-delivered.
#[tokio::test]
async fn subscribing_before_the_snapshot_catches_a_racing_event_without_delivering_it_twice() {
    let broadcaster = EventBroadcaster::new(16);

    // Subscribed first, the fixed order.
    let mut rx = broadcaster.subscribe();

    // The racing event: committed (published) after subscribe() but
    // before the snapshot read below runs.
    let racing_event = event(5);
    broadcaster.publish(&racing_event);

    // Not missed: still reaches this receiver, unlike the test above.
    let received = rx.try_recv().unwrap();
    assert_eq!(received, racing_event);

    // The snapshot read, simulated as running after the publish above -
    // the real `list_events_for_bounded_context` would too, since the
    // event's own insert already committed by the time this fires.
    let bounded_context_events = vec![racing_event.clone()];
    let mapping = access_mapping();
    let subscription = event_store::create_all_events_subscription(
        &mapping,
        Vec::new(),
        None,
        &bounded_context_events,
        timestamp(0),
    )
    .unwrap();
    assert_eq!(subscription.from_sequence, 5);

    // Not delivered a second time: from_sequence already covers it.
    let delivered = event_store::deliver_to_subscriptions(
        &received,
        &[Subscription::AllEventsSubscription(Box::new(subscription))],
        |_, _| None,
        &[],
    );
    assert!(
        delivered.is_empty(),
        "an event already reflected in the snapshot's own from_sequence must not also be \
         delivered live, or a caller would see it twice"
    );
}
