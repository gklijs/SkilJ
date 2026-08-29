//! Tests for the `CommandQuery` surface (`specs/skilj.allium`) -
//! propagated after `event_query.rs` (docs/architecture.md §9): rule
//! `FetchCommands`, and a real (empty-`sensitive_fields`-case)
//! `render_command`.
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 5 of 7 total.
//! Uncovered, with reason - see the doc comment at the bottom of this
//! file: `surface-actor`/`surface-provides.CommandQuery` (2) - the usual
//! GraphQL-scaffolding gap.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, Command, CommandType, Event, EventOrigin, EventType,
};
use skilj_core::shared::Metadata;

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
        template: None,
    }
}

fn access_mapping(status: RoleStatus, level: AccessLevel) -> RoleAccessMapping {
    RoleAccessMapping {
        role: Role {
            id: "role-1".into(),
            external_subject: "admin@example.com".into(),
            name: "Admin".into(),
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

fn command_type(name: &str) -> CommandType {
    CommandType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: name.into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        sensitive_fields: Vec::new(),
        rest_trigger_allowed: false,
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
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: false,
    }
}

fn command(command_type: CommandType, payload: &str, created_at_secs: i64) -> Command {
    Command {
        // Deterministic, unique per distinct payload used in this file's
        // own fixtures - real identity now backs the triggered_event
        // lookup (drift audit finding #12), so tests that need two
        // otherwise-identical commands told apart construct an explicit
        // override rather than relying on this default colliding or not.
        id: format!("cmd-{payload}"),
        bounded_context: bounded_context(BoundedContextStatus::Active),
        command_type,
        payload: payload.into(),
        metadata: Metadata {
            r#type: "PlaceOrder".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: timestamp(created_at_secs),
        },
        encryption_keys: Vec::new(),
        consistency_tags: Vec::new(),
        consistency_boundary: None,
    }
}

fn event_triggered_by(command: Command) -> Event {
    Event {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        event_type: event_type("OrderPlaced"),
        payload: "{}".into(),
        metadata: Metadata {
            r#type: "OrderPlaced".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: timestamp(0),
        },
        sequence: 0,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::CommandTriggered {
            command: Box::new(command),
        },
    }
}

fn directly_created_event() -> Event {
    Event {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        event_type: event_type("OrderPlaced"),
        payload: "{}".into(),
        metadata: Metadata {
            r#type: "OrderPlaced".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: timestamp(0),
        },
        sequence: 0,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

// ---------------------------------------------------------------------
// render_command - direct coverage of the new black-box logic itself
// ---------------------------------------------------------------------

#[test]
fn render_command_passes_the_payload_through_unchanged_when_no_fields_are_sensitive() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let c = command(command_type("PlaceOrder"), r#"{"amount":10}"#, 0);

    assert_eq!(
        event_store::render_command(&c, &mapping, &|_, _| unreachable!()),
        r#"{"amount":10}"#
    );
}

// ---------------------------------------------------------------------
// rule-success.FetchCommands / rule-failure.FetchCommands.{1,2,3,4}
// ---------------------------------------------------------------------

#[test]
fn fetch_commands_returns_every_matching_command_rendered() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let ct = command_type("PlaceOrder");
    let commands = vec![command(ct.clone(), "a", 0), command(ct, "b", 1)];

    let rendered = event_store::fetch_commands(
        &mapping,
        &[],
        None,
        None,
        None,
        &commands,
        |_, _| unreachable!(),
    )
    .unwrap();

    assert_eq!(rendered, vec!["a".to_string(), "b".to_string()]);
}

/// An empty `command_types` means "no restriction" - the unrestricted
/// state itself, the same convention `query_events`' `event_types` uses.
#[test]
fn fetch_commands_with_empty_command_types_matches_every_type() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let commands = vec![
        command(command_type("PlaceOrder"), "a", 0),
        command(command_type("CancelOrder"), "b", 1),
    ];

    let rendered = event_store::fetch_commands(
        &mapping,
        &[],
        None,
        None,
        None,
        &commands,
        |_, _| unreachable!(),
    )
    .unwrap();

    assert_eq!(rendered.len(), 2);
}

#[test]
fn fetch_commands_filters_by_named_command_type() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let place = command_type("PlaceOrder");
    let cancel = command_type("CancelOrder");
    let commands = vec![command(place.clone(), "a", 0), command(cancel, "b", 1)];

    let rendered = event_store::fetch_commands(
        &mapping,
        &[place],
        None,
        None,
        None,
        &commands,
        |_, _| unreachable!(),
    )
    .unwrap();

    assert_eq!(rendered, vec!["a".to_string()]);
}

/// `after`/`before` on `metadata.created_at`, both inclusive, each
/// omittable independently.
#[test]
fn fetch_commands_filters_by_created_at_window_inclusive_on_both_ends() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let ct = command_type("PlaceOrder");
    let commands = vec![
        command(ct.clone(), "too-early", 0),
        command(ct.clone(), "in-window-start", 10),
        command(ct.clone(), "in-window-end", 20),
        command(ct, "too-late", 30),
    ];

    let rendered = event_store::fetch_commands(
        &mapping,
        &[],
        Some(timestamp(10)),
        Some(timestamp(20)),
        None,
        &commands,
        |_, _| unreachable!(),
    )
    .unwrap();

    assert_eq!(
        rendered,
        vec!["in-window-start".to_string(), "in-window-end".to_string()]
    );
}

/// The reverse lookup: which command produced this event.
#[test]
fn fetch_commands_filters_by_triggered_event() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let ct = command_type("PlaceOrder");
    let target_command = command(ct.clone(), "the-one", 0);
    let other_command = command(ct, "not-the-one", 1);
    let triggered_event = event_triggered_by(target_command.clone());
    let commands = vec![target_command, other_command];

    let rendered = event_store::fetch_commands(
        &mapping,
        &[],
        None,
        None,
        Some(&triggered_event),
        &commands,
        |_, _| unreachable!(),
    )
    .unwrap();

    assert_eq!(rendered, vec!["the-one".to_string()]);
}

/// Drift audit finding #12 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): the reverse lookup used to match by
/// whole-`Command` equality, so two field-identical-but-distinct commands
/// were indistinguishable to it. Now that `Command.id` is real, two
/// commands with byte-identical content but different ids must still be
/// told apart correctly - the one the triggered_event's own origin
/// actually names, not "whichever content-identical row happens to be
/// first."
#[test]
fn fetch_commands_filters_by_triggered_event_even_when_another_command_has_identical_content() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let ct = command_type("PlaceOrder");
    let base = command(ct, "same-content", 0);
    let target_command = Command {
        id: "cmd-target".into(),
        ..base.clone()
    };
    let decoy_command = Command {
        id: "cmd-decoy".into(),
        ..base
    };
    let triggered_event = event_triggered_by(target_command.clone());
    // The decoy first, so a content-based match (the old bug) would have
    // returned it instead - order alone doesn't save this test.
    let commands = vec![decoy_command, target_command];

    let rendered = event_store::fetch_commands(
        &mapping,
        &[],
        None,
        None,
        Some(&triggered_event),
        &commands,
        |_, _| unreachable!(),
    )
    .unwrap();

    // Exactly one match, not two - the decoy's identical content must not
    // also satisfy the lookup.
    assert_eq!(rendered, vec!["same-content".to_string()]);
}

/// Supplying `triggered_event` alongside a command_types/time filter its
/// command does not satisfy yields nothing, rather than the command
/// anyway - every filter narrows, none overrides another.
#[test]
fn fetch_commands_triggered_event_conjuncts_with_other_filters() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let target_command = command(command_type("PlaceOrder"), "the-one", 0);
    let triggered_event = event_triggered_by(target_command.clone());
    let commands = vec![target_command];

    let rendered = event_store::fetch_commands(
        &mapping,
        &[command_type("CancelOrder")], // doesn't match the command's own type
        None,
        None,
        Some(&triggered_event),
        &commands,
        |_, _| unreachable!(),
    )
    .unwrap();

    assert!(rendered.is_empty());
}

/// A directly-created event (no command behind it) never matches any
/// `triggered_event` lookup.
#[test]
fn fetch_commands_triggered_event_matches_nothing_for_a_non_command_triggered_event() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let commands = vec![command(command_type("PlaceOrder"), "a", 0)];
    let unrelated_event = directly_created_event();

    let rendered = event_store::fetch_commands(
        &mapping,
        &[],
        None,
        None,
        Some(&unrelated_event),
        &commands,
        |_, _| unreachable!(),
    )
    .unwrap();

    assert!(rendered.is_empty());
}

/// Commands outside the grant's own bounded context never appear.
#[test]
fn fetch_commands_never_returns_commands_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let foreign_context = BoundedContext {
        name: "billing".into(),
        ..bounded_context(BoundedContextStatus::Active)
    };
    let foreign_command = Command {
        bounded_context: foreign_context,
        ..command(command_type("IssueInvoice"), "a", 0)
    };

    let rendered = event_store::fetch_commands(
        &mapping,
        &[],
        None,
        None,
        None,
        &[foreign_command],
        |_, _| unreachable!(),
    )
    .unwrap();

    assert!(rendered.is_empty());
}

/// rule-failure.FetchCommands.1 - `requires: access_mapping.status = active`.
#[test]
fn fetch_commands_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);

    let err =
        event_store::fetch_commands(&mapping, &[], None, None, None, &[], |_, _| unreachable!())
            .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.FetchCommands.2 - `requires: access_mapping.level = admin`.
#[test]
fn fetch_commands_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);

    let err =
        event_store::fetch_commands(&mapping, &[], None, None, None, &[], |_, _| unreachable!())
            .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.FetchCommands.3 - `requires: command_types.all(ct =>
/// ct.bounded_context = access_mapping.bounded_context)`.
#[test]
fn fetch_commands_rejects_a_named_command_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let foreign_type = CommandType {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        ..command_type("IssueInvoice")
    };

    let err = event_store::fetch_commands(
        &mapping,
        &[foreign_type],
        None,
        None,
        None,
        &[],
        |_, _| unreachable!(),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::CommandTypeNotInBoundedContext.code()
    );
}

/// rule-failure.FetchCommands.4 - `requires: triggered_event = null or
/// triggered_event?.bounded_context = access_mapping.bounded_context`.
#[test]
fn fetch_commands_rejects_a_triggered_event_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let foreign_event = Event {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        ..directly_created_event()
    };

    let err = event_store::fetch_commands(
        &mapping,
        &[],
        None,
        None,
        Some(&foreign_event),
        &[],
        |_, _| unreachable!(),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::TriggeredEventNotInBoundedContext.code()
    );
}

// ---------------------------------------------------------------------
// surface-actor/surface-provides.CommandQuery - uncovered
// ---------------------------------------------------------------------
//
// GraphQL-scaffolding gap, same as every prior admin surface's deferred
// pair (see command_processing.rs's header comment) - no resolver/schema
// wiring in skilj-graphql yet.
