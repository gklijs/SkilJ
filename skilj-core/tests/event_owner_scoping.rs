//! Tests for the raw-event half of the cross-tenant read fix
//! (docs/architecture.md's own write-up of these passes) - the follow-up
//! to `projection_owner_scoping.rs`'s own projection-instance half.
//! `ProjectionQuery` was the first surface fixed; `QueryEvents`/
//! `CountEvents`/`InspectEvent`, `FetchEvents`/`ConsumeEvents`, and
//! `EventSubscription`'s own delivery all shared the identical gap -
//! `require_read_mapping`'s "any active grant on this bounded context"
//! check, with no check that a queried/delivered *event* belonged to
//! that caller.
//!
//! Unlike the projection fix, every function this pass touches
//! (`event_owner_scope_satisfied`, `query_events`, `count_events`,
//! `inspect_event`, `fetch_events`, `consume_events`,
//! `deliver_to_subscriptions`) is already pure - no Postgres, no
//! `ProjectionDispatcher`-style type-erasure - so this file needs none
//! of `sync_projections.rs`'s embedded-Postgres provisioning harness;
//! same "test the layer in isolation" shape `type_registration.rs`
//! already uses. The DB round-trip of `EventType.owner_tag_key`/
//! `EventReadToken.scope` themselves is covered by `persistence.rs`'s
//! existing whole-struct `assert_eq!` round-trip tests, which now
//! include both fields by construction - no separate DB test needed
//! here.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{
    self, AccessLevel, EventReadStartPosition, EventReadToken, Role, RoleAccessMapping, RoleStatus,
    TokenStatus,
};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, AllEventsSubscription, BoundedContext, BoundedContextStatus, Event, EventOrigin,
    EventType, Subscription,
};
use skilj_core::shared::{Metadata, Tag};

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn bounded_context() -> BoundedContext {
    BoundedContext {
        name: "helpdesk".into(),
        status: BoundedContextStatus::Active,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
        template: None,
    }
}

/// `owner_tag_key: Some("company")` - every event of this type derives
/// its owner from its own `company` tag.
fn ticket_opened() -> EventType {
    EventType {
        bounded_context: bounded_context(),
        name: "TicketOpened".into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "company".into(),
            field: "company_id".into(),
        }],
        owner_tag_key: Some("company".into()),
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: true,
        direct_creation_allowed: true,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: true,
    }
}

/// Declares no owner dimension at all - `banking.rs`/`courses.rs`-shaped,
/// every existing event type before this pass.
fn unscoped_event_type() -> EventType {
    EventType {
        owner_tag_key: None,
        tag_mappings: Vec::new(),
        name: "MoneyDeposited".into(),
        ..ticket_opened()
    }
}

fn event(et: &EventType, sequence: i64, company: Option<&str>) -> Event {
    Event {
        bounded_context: et.bounded_context.clone(),
        event_type: et.clone(),
        payload: "{}".into(),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: et.schema_version,
            client_id: "someone".into(),
            created_at: timestamp(0),
            correlation_id: None,
            causation_id: None,
        },
        sequence,
        tags: match company {
            Some(c) => vec![Tag {
                key: "company".into(),
                value: Some(c.into()),
            }],
            None => Vec::new(),
        },
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

fn access_mapping(level: AccessLevel, scope: Option<&str>) -> RoleAccessMapping {
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
        level,
        can_read_sensitive: false,
        scope: scope.map(str::to_string),
        status: RoleStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn read_token(et: &EventType, scope: Option<&str>) -> EventReadToken {
    EventReadToken {
        id: "token-1".into(),
        secret: "s3cr3t".into(),
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
        event_type: et.clone(),
        scope: scope.map(str::to_string),
        start_from: EventReadStartPosition::Beginning,
        start_at_sequence: None,
        start_at_time: None,
    }
}

fn resolve_data_key(
    _subject_key: &str,
    _subject_value: &str,
) -> Option<skilj_core::encryption::DataKey> {
    None
}

// ---------------------------------------------------------------------
// event_owner_scope_satisfied - in isolation
// ---------------------------------------------------------------------

#[test]
fn owner_scope_satisfied_when_scope_is_none() {
    let et = ticket_opened();
    let e = event(&et, 0, Some("company-b"));
    assert!(event_store::event_owner_scope_satisfied(&e, None));
}

#[test]
fn owner_scope_satisfied_when_the_type_declares_no_owner_dimension() {
    let et = unscoped_event_type();
    let e = event(&et, 0, None);
    assert!(event_store::event_owner_scope_satisfied(
        &e,
        Some("company-a")
    ));
}

#[test]
fn owner_scope_satisfied_when_the_tag_value_matches() {
    let et = ticket_opened();
    let e = event(&et, 0, Some("company-a"));
    assert!(event_store::event_owner_scope_satisfied(
        &e,
        Some("company-a")
    ));
}

#[test]
fn owner_scope_not_satisfied_when_the_tag_value_differs() {
    let et = ticket_opened();
    let e = event(&et, 0, Some("company-b"));
    assert!(!event_store::event_owner_scope_satisfied(
        &e,
        Some("company-a")
    ));
}

/// Fail-closed: no tag at all is treated the same as a proven mismatch,
/// not as "no conflict" - the identical stance
/// `projections::query_projection` takes for an unestablished owner.
#[test]
fn owner_scope_not_satisfied_when_the_event_carries_no_tag_at_all() {
    let et = ticket_opened();
    let e = event(&et, 0, None);
    assert!(!event_store::event_owner_scope_satisfied(
        &e,
        Some("company-a")
    ));
}

// ---------------------------------------------------------------------
// query_events / count_events - filter, not reject
// ---------------------------------------------------------------------

#[test]
fn query_events_filters_out_events_owned_by_a_different_scope() {
    let et = ticket_opened();
    let events = vec![
        event(&et, 0, Some("company-a")),
        event(&et, 1, Some("company-b")),
    ];
    let mapping = access_mapping(AccessLevel::Admin, Some("company-a"));

    let result = event_store::query_events(
        &mapping,
        &[],
        None,
        None,
        None,
        &events,
        resolve_data_key,
        &[],
    )
    .unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].0, 0);
}

#[test]
fn query_events_returns_everything_for_an_unscoped_grant() {
    let et = ticket_opened();
    let events = vec![
        event(&et, 0, Some("company-a")),
        event(&et, 1, Some("company-b")),
    ];
    let mapping = access_mapping(AccessLevel::Admin, None);

    let result = event_store::query_events(
        &mapping,
        &[],
        None,
        None,
        None,
        &events,
        resolve_data_key,
        &[],
    )
    .unwrap();

    assert_eq!(result.len(), 2);
}

#[test]
fn count_events_reflects_the_same_owner_filtering_as_query_events() {
    let et = ticket_opened();
    let events = vec![
        event(&et, 0, Some("company-a")),
        event(&et, 1, Some("company-b")),
        event(&et, 2, Some("company-a")),
    ];
    let mapping = access_mapping(AccessLevel::Admin, Some("company-a"));

    let count = event_store::count_events(&mapping, &[], None, None, &events).unwrap();

    assert_eq!(count, 2);
}

// ---------------------------------------------------------------------
// inspect_event - reject, single-record
// ---------------------------------------------------------------------

#[test]
fn inspect_event_rejects_an_event_owned_by_a_different_scope() {
    let et = ticket_opened();
    let e = event(&et, 0, Some("company-b"));
    let mapping = access_mapping(AccessLevel::Admin, Some("company-a"));

    let err = event_store::inspect_event(&mapping, &e, resolve_data_key, &[]).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantScopeMismatch.code());
}

#[test]
fn inspect_event_succeeds_when_the_owner_matches() {
    let et = ticket_opened();
    let e = event(&et, 0, Some("company-a"));
    let mapping = access_mapping(AccessLevel::Admin, Some("company-a"));

    event_store::inspect_event(&mapping, &e, resolve_data_key, &[]).unwrap();
}

// ---------------------------------------------------------------------
// fetch_events / consume_events - REST track, scoped by token.scope
// ---------------------------------------------------------------------

#[test]
fn fetch_events_filters_by_the_tokens_own_scope() {
    let et = ticket_opened();
    let events = vec![
        event(&et, 0, Some("company-a")),
        event(&et, 1, Some("company-b")),
    ];
    let token = read_token(&et, Some("company-a"));

    let result = event_store::fetch_events(&token, &events, &[], None, None).unwrap();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].sequence, 0);
}

#[test]
fn fetch_events_returns_everything_for_an_unscoped_token() {
    let et = ticket_opened();
    let events = vec![
        event(&et, 0, Some("company-a")),
        event(&et, 1, Some("company-b")),
    ];
    let token = read_token(&et, None);

    let result = event_store::fetch_events(&token, &events, &[], None, None).unwrap();

    assert_eq!(result.len(), 2);
}

#[test]
fn consume_events_serves_only_the_tokens_own_scope() {
    let et = ticket_opened();
    let events = vec![
        event(&et, 0, Some("company-a")),
        event(&et, 1, Some("company-b")),
    ];
    let token = read_token(&et, Some("company-a"));

    let result = event_store::consume_events(
        &token,
        None,
        Some(event_store::AckMode::AutoAdvance),
        &events,
        &[],
        timestamp(0),
        chrono::Duration::minutes(5),
    )
    .unwrap();

    assert_eq!(result.served.len(), 1);
    assert_eq!(result.served[0].sequence, 0);
}

// ---------------------------------------------------------------------
// deliver_to_subscriptions - a subscription never receives an event it
// isn't scoped to
// ---------------------------------------------------------------------

#[test]
fn deliver_to_subscriptions_skips_a_scoped_subscription_for_a_different_companys_event() {
    let et = ticket_opened();
    let e = event(&et, 0, Some("company-b"));
    let mapping = access_mapping(AccessLevel::Read, Some("company-a"));
    let subscription = Subscription::AllEventsSubscription(Box::new(AllEventsSubscription {
        bounded_context: bounded_context(),
        access_mapping: mapping,
        created_at: timestamp(0),
        from_sequence: -1,
        event_types: Vec::new(),
    }));

    let delivered =
        event_store::deliver_to_subscriptions(&e, &[subscription], resolve_data_key, &[]);

    assert!(delivered.is_empty());
}

#[test]
fn deliver_to_subscriptions_delivers_a_scoped_subscriptions_own_company_event() {
    let et = ticket_opened();
    let e = event(&et, 0, Some("company-a"));
    let mapping = access_mapping(AccessLevel::Read, Some("company-a"));
    let subscription = Subscription::AllEventsSubscription(Box::new(AllEventsSubscription {
        bounded_context: bounded_context(),
        access_mapping: mapping,
        created_at: timestamp(0),
        from_sequence: -1,
        event_types: Vec::new(),
    }));

    let delivered =
        event_store::deliver_to_subscriptions(&e, &[subscription], resolve_data_key, &[]);

    assert_eq!(delivered.len(), 1);
}

#[test]
fn deliver_to_subscriptions_delivers_to_an_unscoped_subscription_regardless_of_owner() {
    let et = ticket_opened();
    let e = event(&et, 0, Some("company-b"));
    let mapping = access_mapping(AccessLevel::Read, None);
    let subscription = Subscription::AllEventsSubscription(Box::new(AllEventsSubscription {
        bounded_context: bounded_context(),
        access_mapping: mapping,
        created_at: timestamp(0),
        from_sequence: -1,
        event_types: Vec::new(),
    }));

    let delivered =
        event_store::deliver_to_subscriptions(&e, &[subscription], resolve_data_key, &[]);

    assert_eq!(delivered.len(), 1);
}
