//! Tests for the `EventSubscription` surface (`specs/skilj.allium`) -
//! propagated after `projection_query.rs` (docs/architecture.md §9):
//! entity `Subscription` (+ its two variants `AllEventsSubscription`/
//! `EventTypeSubscription`), rules `CreateAllEventsSubscription`/
//! `CreateEventTypeSubscription`/`DeliverToSubscriptions`.
//! `DeliverToSubscriptions`' own real delivery is asynchronous and best-
//! effort, outside any request's transaction, per the spec's own note -
//! `deliver_to_subscriptions` is the pure "who matches and what do they
//! get" computation underneath that, not the dispatch loop itself.
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 14 of 16 total.
//! Uncovered, with reason - see the doc comment at the bottom of this
//! file: `surface-actor`/`surface-provides.EventSubscription` (2) - the
//! usual GraphQL-scaffolding gap.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, AllEventsSubscription, BoundedContext, BoundedContextStatus, Event, EventOrigin,
    EventType, EventTypeSubscription, Subscription,
};
use skilj_core::shared::{Filter, FilterOperator, Metadata};

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn bounded_context(status: BoundedContextStatus) -> BoundedContext {
    BoundedContext {
        name: "orders".into(),
        status,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
    }
}

fn access_mapping(status: RoleStatus, level: AccessLevel) -> RoleAccessMapping {
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
        bounded_context: bounded_context(BoundedContextStatus::Active),
        level,
        can_read_sensitive: false,
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn event_type(name: &str) -> EventType {
    EventType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: name.into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        sensitive_fields: Vec::new(),
        external_creation_allowed: false,
        direct_creation_allowed: false,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        event_read_allowed: false,
    }
}

fn event(event_type: EventType, sequence: i64, payload: &str) -> Event {
    Event {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        event_type,
        payload: payload.into(),
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

// ---------------------------------------------------------------------
// sum-type-variant.AllEventsSubscription / sum-type-variant.EventTypeSubscription
// ---------------------------------------------------------------------

#[test]
fn all_events_subscription_carries_its_variant_specific_field() {
    let et = event_type("OrderPlaced");
    let sub = Subscription::AllEventsSubscription(Box::new(AllEventsSubscription {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        access_mapping: access_mapping(RoleStatus::Active, AccessLevel::Read),
        created_at: timestamp(0),
        from_sequence: -1,
        event_types: vec![et.clone()],
    }));

    match sub {
        Subscription::AllEventsSubscription(a) => assert_eq!(a.event_types, vec![et]),
        other => panic!("expected AllEventsSubscription, got {other:?}"),
    }
}

#[test]
fn event_type_subscription_carries_its_variant_specific_fields() {
    let et = event_type("OrderPlaced");
    let sub = Subscription::EventTypeSubscription(Box::new(EventTypeSubscription {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        access_mapping: access_mapping(RoleStatus::Active, AccessLevel::Read),
        created_at: timestamp(0),
        from_sequence: -1,
        event_type: et.clone(),
        filters: Vec::new(),
    }));

    match sub {
        Subscription::EventTypeSubscription(e) => {
            assert_eq!(e.event_type, et);
            assert!(e.filters.is_empty());
        }
        other => panic!("expected EventTypeSubscription, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// rule-success.CreateAllEventsSubscription / rule-failure.{1,2,3}
// / rule-entity-creation.1
// ---------------------------------------------------------------------

#[test]
fn create_all_events_subscription_succeeds_and_stamps_the_full_entity_shape() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);
    let et = event_type("OrderPlaced");

    let sub = event_store::create_all_events_subscription(
        &mapping,
        vec![et.clone()],
        None, // defaults to the bounded context's current latest sequence
        &[event(et.clone(), 0, "a"), event(et, 1, "b")],
        timestamp(1000),
    )
    .unwrap();

    assert_eq!(sub.bounded_context, mapping.bounded_context);
    assert_eq!(sub.access_mapping, mapping);
    assert_eq!(sub.created_at, timestamp(1000));
    assert_eq!(sub.from_sequence, 1); // highest committed sequence
    assert_eq!(sub.event_types.len(), 1);
}

/// An explicit `from_sequence` is honoured rather than defaulted.
#[test]
fn create_all_events_subscription_honours_an_explicit_from_sequence() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);

    let sub = event_store::create_all_events_subscription(
        &mapping,
        Vec::new(),
        Some(7),
        &[],
        timestamp(0),
    )
    .unwrap();

    assert_eq!(sub.from_sequence, 7);
}

/// No prior events - the default starting point is -1.
#[test]
fn create_all_events_subscription_defaults_to_minus_one_when_the_stream_is_empty() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);

    let sub =
        event_store::create_all_events_subscription(&mapping, Vec::new(), None, &[], timestamp(0))
            .unwrap();

    assert_eq!(sub.from_sequence, -1);
}

/// rule-failure.CreateAllEventsSubscription.1 - `requires: access_mapping.status = active`.
#[test]
fn create_all_events_subscription_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Read);

    let err =
        event_store::create_all_events_subscription(&mapping, Vec::new(), None, &[], timestamp(0))
            .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.CreateAllEventsSubscription.2 - `requires: access_mapping.bounded_context.status = active`.
#[test]
fn create_all_events_subscription_rejects_an_archived_bounded_context() {
    let mapping = RoleAccessMapping {
        bounded_context: bounded_context(BoundedContextStatus::Archived),
        ..access_mapping(RoleStatus::Active, AccessLevel::Read)
    };

    let err =
        event_store::create_all_events_subscription(&mapping, Vec::new(), None, &[], timestamp(0))
            .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.CreateAllEventsSubscription.3 - `requires: event_types.all(et
/// => et.bounded_context = access_mapping.bounded_context)`.
#[test]
fn create_all_events_subscription_rejects_a_named_event_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);
    let foreign_type = EventType {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        ..event_type("InvoiceIssued")
    };

    let err = event_store::create_all_events_subscription(
        &mapping,
        vec![foreign_type],
        None,
        &[],
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::EventTypeNotInBoundedContext.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.CreateEventTypeSubscription / rule-failure.{1,2,3,4}
// / rule-entity-creation.1
// ---------------------------------------------------------------------

#[test]
fn create_event_type_subscription_succeeds_and_stamps_the_full_entity_shape() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);
    let et = event_type("OrderPlaced");
    // Empty filters - the one shape valid_filters is real for today (see
    // create_event_type_subscription_rejects_an_invalid_filter below for
    // the non-empty, still-deferred case).
    let sub = event_store::create_event_type_subscription(
        &mapping,
        &et,
        Vec::new(),
        None,
        &[event(et.clone(), 0, "a")],
        timestamp(1000),
    )
    .unwrap();

    assert_eq!(sub.bounded_context, et.bounded_context);
    assert_eq!(sub.access_mapping, mapping);
    assert_eq!(sub.created_at, timestamp(1000));
    assert_eq!(sub.from_sequence, 0);
    assert_eq!(sub.event_type, et);
    assert!(sub.filters.is_empty());
}

/// rule-failure.CreateEventTypeSubscription.1 - `requires: access_mapping.status = active`.
#[test]
fn create_event_type_subscription_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Read);
    let et = event_type("OrderPlaced");

    let err = event_store::create_event_type_subscription(
        &mapping,
        &et,
        Vec::new(),
        None,
        &[],
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.CreateEventTypeSubscription.2 - `requires: access_mapping.bounded_context = event_type.bounded_context`.
#[test]
fn create_event_type_subscription_rejects_an_event_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);
    let foreign_type = EventType {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        ..event_type("InvoiceIssued")
    };

    let err = event_store::create_event_type_subscription(
        &mapping,
        &foreign_type,
        Vec::new(),
        None,
        &[],
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.CreateEventTypeSubscription.3 - `requires: event_type.bounded_context.status = active`.
#[test]
fn create_event_type_subscription_rejects_an_archived_bounded_context() {
    let et = EventType {
        bounded_context: bounded_context(BoundedContextStatus::Archived),
        ..event_type("OrderPlaced")
    };
    let mapping = RoleAccessMapping {
        bounded_context: et.bounded_context.clone(),
        ..access_mapping(RoleStatus::Active, AccessLevel::Read)
    };

    let err = event_store::create_event_type_subscription(
        &mapping,
        &et,
        Vec::new(),
        None,
        &[],
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.CreateEventTypeSubscription.4 - `requires: valid_filters(event_type, filters)`.
/// `valid_filters`' real type-checking is a deferred black box (see its
/// own doc comment) - same treatment as `fetch_events_rejects_an_invalid_filter`
/// in event_fetch_surface.rs: this asserts today's contract, that
/// `create_event_type_subscription` reaches for `valid_filters` at all
/// (a non-empty filter list hits its `todo!()` branch), not the eventual
/// type-checking behaviour itself.
#[test]
fn create_event_type_subscription_rejects_an_invalid_filter() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);
    let et = event_type("OrderPlaced");
    let non_empty_filters = vec![Filter {
        field: "amount".into(),
        operator: FilterOperator::GreaterThan,
        value: "10".into(),
    }];

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        event_store::create_event_type_subscription(
            &mapping,
            &et,
            non_empty_filters,
            None,
            &[],
            timestamp(0),
        )
    }));
    assert!(
        result.is_err(),
        "valid_filters is a deferred black box for non-empty filters - see its doc comment"
    );
}

// ---------------------------------------------------------------------
// rule-success.DeliverToSubscriptions
// ---------------------------------------------------------------------

#[test]
fn deliver_to_subscriptions_delivers_to_an_all_events_subscription_with_no_restriction() {
    let et = event_type("OrderPlaced");
    let e = event(et.clone(), 5, r#"{"amount":10}"#);
    let sub = Subscription::AllEventsSubscription(Box::new(AllEventsSubscription {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        access_mapping: access_mapping(RoleStatus::Active, AccessLevel::Read),
        created_at: timestamp(0),
        from_sequence: 0,
        event_types: Vec::new(), // empty - no restriction
    }));

    let delivered = event_store::deliver_to_subscriptions(
        &e,
        std::slice::from_ref(&sub),
        |_, _| unreachable!(),
    );

    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].subscription, sub);
    assert_eq!(delivered[0].event, e);
    assert_eq!(delivered[0].rendered_payload, r#"{"amount":10}"#);
}

#[test]
fn deliver_to_subscriptions_all_events_subscription_filters_by_named_event_types() {
    let placed = event_type("OrderPlaced");
    let cancelled = event_type("OrderCancelled");
    let e = event(cancelled, 5, "x");
    let sub = Subscription::AllEventsSubscription(Box::new(AllEventsSubscription {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        access_mapping: access_mapping(RoleStatus::Active, AccessLevel::Read),
        created_at: timestamp(0),
        from_sequence: 0,
        event_types: vec![placed], // doesn't include the event's own type
    }));

    let delivered = event_store::deliver_to_subscriptions(&e, &[sub], |_, _| unreachable!());

    assert!(delivered.is_empty());
}

#[test]
fn deliver_to_subscriptions_event_type_subscription_matches_type_and_filters() {
    let et = event_type("OrderPlaced");
    let e = event(et.clone(), 5, "x");
    let sub = Subscription::EventTypeSubscription(Box::new(EventTypeSubscription {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        access_mapping: access_mapping(RoleStatus::Active, AccessLevel::Read),
        created_at: timestamp(0),
        from_sequence: 0,
        event_type: et,
        filters: Vec::new(), // empty filters - real, always matches
    }));

    let delivered = event_store::deliver_to_subscriptions(&e, &[sub], |_, _| unreachable!());

    assert_eq!(delivered.len(), 1);
}

#[test]
fn deliver_to_subscriptions_event_type_subscription_never_matches_a_different_event_type() {
    let subscribed_type = event_type("OrderPlaced");
    let other_type = event_type("OrderCancelled");
    let e = event(other_type, 5, "x");
    let sub = Subscription::EventTypeSubscription(Box::new(EventTypeSubscription {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        access_mapping: access_mapping(RoleStatus::Active, AccessLevel::Read),
        created_at: timestamp(0),
        from_sequence: 0,
        event_type: subscribed_type,
        filters: Vec::new(),
    }));

    let delivered = event_store::deliver_to_subscriptions(&e, &[sub], |_, _| unreachable!());

    assert!(delivered.is_empty());
}

/// `from_sequence < event.sequence` - never redelivers what a
/// subscription already started past.
#[test]
fn deliver_to_subscriptions_never_delivers_at_or_before_from_sequence() {
    let et = event_type("OrderPlaced");
    let e = event(et.clone(), 5, "x");
    let sub = Subscription::AllEventsSubscription(Box::new(AllEventsSubscription {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        access_mapping: access_mapping(RoleStatus::Active, AccessLevel::Read),
        created_at: timestamp(0),
        from_sequence: 5, // equal to the event's own sequence
        event_types: Vec::new(),
    }));

    let delivered = event_store::deliver_to_subscriptions(&e, &[sub], |_, _| unreachable!());

    assert!(delivered.is_empty());
}

/// A revoked grant stops delivery immediately - re-checked live on every
/// delivery, not a snapshot from creation time.
#[test]
fn deliver_to_subscriptions_never_delivers_to_a_revoked_grant() {
    let et = event_type("OrderPlaced");
    let e = event(et.clone(), 5, "x");
    let sub = Subscription::AllEventsSubscription(Box::new(AllEventsSubscription {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        access_mapping: access_mapping(RoleStatus::Revoked, AccessLevel::Read),
        created_at: timestamp(0),
        from_sequence: 0,
        event_types: Vec::new(),
    }));

    let delivered = event_store::deliver_to_subscriptions(&e, &[sub], |_, _| unreachable!());

    assert!(delivered.is_empty());
}

/// A subscription scoped to a different bounded context never receives
/// this event.
#[test]
fn deliver_to_subscriptions_never_delivers_across_bounded_contexts() {
    let et = event_type("OrderPlaced");
    let e = event(et.clone(), 5, "x");
    let sub = Subscription::AllEventsSubscription(Box::new(AllEventsSubscription {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        access_mapping: access_mapping(RoleStatus::Active, AccessLevel::Read),
        created_at: timestamp(0),
        from_sequence: 0,
        event_types: Vec::new(),
    }));

    let delivered = event_store::deliver_to_subscriptions(&e, &[sub], |_, _| unreachable!());

    assert!(delivered.is_empty());
}

// ---------------------------------------------------------------------
// surface-actor/surface-provides.EventSubscription - uncovered
// ---------------------------------------------------------------------
//
// GraphQL-scaffolding gap, same as every prior surface's deferred set
// (see command_processing.rs's header comment) - no resolver/schema
// wiring in skilj-graphql yet.
