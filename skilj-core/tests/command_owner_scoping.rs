//! Tests for the command-read half of the cross-tenant read fix
//! (docs/architecture.md's own write-up of these passes) - the third and
//! final pass, after `projection_owner_scoping.rs`'s projection-instance
//! half and `event_owner_scoping.rs`'s raw-event half. `CommandQuery`
//! (`FetchCommands`) is the only read surface for `Command` - the REST
//! track's `CommandToken` only triggers one command type and reads
//! nothing back, so unlike pass 2 there is no token-scope side to this
//! one at all.
//!
//! `fetch_commands`/`command_owner_scope_satisfied` are pure, like every
//! function pass 2 touched - no Postgres needed. Same "test the layer in
//! isolation" shape `event_owner_scoping.rs` already uses. The DB
//! round-trip of `CommandType.owner_tag_key` is covered by
//! `persistence.rs`'s existing whole-struct `assert_eq!` round-trip
//! tests, which now include it by construction - no separate DB test
//! needed here.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, Command, CommandType, EventOrigin,
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

/// A real schema declaring `company_id`, so `tag_mapping`/`owner_tag_key`
/// referencing it as a field passes `valid_tag_mappings`/
/// `valid_owner_tag_key`'s own schema-declared-field checks.
fn ticket_schema() -> String {
    r#"{"type":"object","properties":{"company_id":{"type":"string"}}}"#.into()
}

/// `owner_tag_key: Some("company")` - every command of this type derives
/// its owner from its own `company` consistency tag.
fn open_ticket() -> CommandType {
    CommandType {
        bounded_context: bounded_context(),
        name: "OpenTicket".into(),
        schema: ticket_schema(),
        schema_version: 1,
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "company".into(),
            field: "company_id".into(),
        }],
        owner_tag_key: Some("company".into()),
        sensitive_fields: Vec::new(),
        rest_trigger_allowed: false,
    }
}

/// Declares no owner dimension at all - `banking.rs`/`courses.rs`-shaped,
/// every existing command type before this pass.
fn unscoped_command_type() -> CommandType {
    CommandType {
        owner_tag_key: None,
        tag_mappings: Vec::new(),
        name: "DepositMoney".into(),
        ..open_ticket()
    }
}

fn command(id: &str, ct: &CommandType, company: Option<&str>) -> Command {
    Command {
        id: id.into(),
        bounded_context: ct.bounded_context.clone(),
        command_type: ct.clone(),
        payload: "{}".into(),
        metadata: Metadata {
            r#type: ct.name.clone(),
            version: ct.schema_version,
            client_id: "someone".into(),
            created_at: timestamp(0),
        },
        encryption_keys: Vec::new(),
        consistency_tags: match company {
            Some(c) => vec![Tag {
                key: "company".into(),
                value: Some(c.into()),
            }],
            None => Vec::new(),
        },
        consistency_boundary: None,
    }
}

fn access_mapping(scope: Option<&str>) -> RoleAccessMapping {
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
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        scope: scope.map(str::to_string),
        status: RoleStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn resolve_data_key(
    _subject_key: &str,
    _subject_value: &str,
) -> Option<skilj_core::encryption::DataKey> {
    None
}

// ---------------------------------------------------------------------
// command_owner_scope_satisfied - in isolation
// ---------------------------------------------------------------------

#[test]
fn owner_scope_satisfied_when_scope_is_none() {
    let ct = open_ticket();
    let c = command("cmd-1", &ct, Some("company-b"));
    assert!(event_store::command_owner_scope_satisfied(&c, None));
}

#[test]
fn owner_scope_satisfied_when_the_type_declares_no_owner_dimension() {
    let ct = unscoped_command_type();
    let c = command("cmd-1", &ct, None);
    assert!(event_store::command_owner_scope_satisfied(
        &c,
        Some("company-a")
    ));
}

#[test]
fn owner_scope_satisfied_when_the_tag_value_matches() {
    let ct = open_ticket();
    let c = command("cmd-1", &ct, Some("company-a"));
    assert!(event_store::command_owner_scope_satisfied(
        &c,
        Some("company-a")
    ));
}

#[test]
fn owner_scope_not_satisfied_when_the_tag_value_differs() {
    let ct = open_ticket();
    let c = command("cmd-1", &ct, Some("company-b"));
    assert!(!event_store::command_owner_scope_satisfied(
        &c,
        Some("company-a")
    ));
}

/// Fail-closed, the identical stance `event_owner_scope_satisfied`/
/// `projections::query_projection` both take for an unestablished owner:
/// no consistency tag at all is a proven mismatch, not "no conflict".
#[test]
fn owner_scope_not_satisfied_when_the_command_carries_no_consistency_tag_at_all() {
    let ct = open_ticket();
    let c = command("cmd-1", &ct, None);
    assert!(!event_store::command_owner_scope_satisfied(
        &c,
        Some("company-a")
    ));
}

// ---------------------------------------------------------------------
// fetch_commands - filter, not reject (same shape as query_events)
// ---------------------------------------------------------------------

#[test]
fn fetch_commands_filters_out_commands_owned_by_a_different_scope() {
    let ct = open_ticket();
    let commands = vec![
        command("cmd-a", &ct, Some("company-a")),
        command("cmd-b", &ct, Some("company-b")),
    ];
    let mapping = access_mapping(Some("company-a"));

    let result =
        event_store::fetch_commands(&mapping, &[], None, None, None, &commands, resolve_data_key)
            .unwrap();

    assert_eq!(result.len(), 1);
}

#[test]
fn fetch_commands_returns_everything_for_an_unscoped_grant() {
    let ct = open_ticket();
    let commands = vec![
        command("cmd-a", &ct, Some("company-a")),
        command("cmd-b", &ct, Some("company-b")),
    ];
    let mapping = access_mapping(None);

    let result =
        event_store::fetch_commands(&mapping, &[], None, None, None, &commands, resolve_data_key)
            .unwrap();

    assert_eq!(result.len(), 2);
}

/// The concrete leak this pass closes: a company's own admin, scoped to
/// its own company, previously read every other company's commands too
/// via the exact same `FetchCommands` surface `QueryEvents`' fix left
/// untouched.
#[test]
fn fetch_commands_hides_another_companys_command_from_a_scoped_admin() {
    let ct = open_ticket();
    let own = command("cmd-a", &ct, Some("company-a"));
    let other = command("cmd-b", &ct, Some("company-b"));
    let commands = vec![own, other];
    let mapping = access_mapping(Some("company-a"));

    let result =
        event_store::fetch_commands(&mapping, &[], None, None, None, &commands, resolve_data_key)
            .unwrap();

    assert_eq!(result.len(), 1);
}

/// The `triggered_event` reverse-lookup shape is covered by the same
/// filter: naming an event whose command belongs to another company
/// yields nothing, not an error.
#[test]
fn fetch_commands_filters_the_triggered_event_reverse_lookup_by_owner_too() {
    let ct = open_ticket();
    let other = command("cmd-b", &ct, Some("company-b"));
    let triggering_event = skilj_core::event_store::Event {
        bounded_context: ct.bounded_context.clone(),
        event_type: skilj_core::event_store::EventType {
            bounded_context: ct.bounded_context.clone(),
            name: "TicketOpened".into(),
            schema: "{}".into(),
            schema_version: 1,
            tag_mappings: Vec::new(),
            owner_tag_key: None,
            sensitive_fields: Vec::new(),
            external_creation_allowed: false,
            direct_creation_allowed: false,
            system_triggered_allowed: false,
            system_triggered_schedule: None,
            missed_occurrence_policy: None,
            schedule_position: None,
            last_fired_at: None,
            event_read_allowed: true,
        },
        payload: "{}".into(),
        metadata: Metadata {
            r#type: "TicketOpened".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: timestamp(0),
        },
        sequence: 0,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::CommandTriggered {
            command: Box::new(other.clone()),
        },
    };
    let commands = vec![other];
    let mapping = access_mapping(Some("company-a"));

    let result = event_store::fetch_commands(
        &mapping,
        &[],
        None,
        None,
        Some(&triggering_event),
        &commands,
        resolve_data_key,
    )
    .unwrap();

    assert!(result.is_empty());
}

// ---------------------------------------------------------------------
// RegisterCommandType - valid_owner_tag_key
// ---------------------------------------------------------------------

#[test]
fn register_command_type_rejects_an_owner_tag_key_naming_an_undeclared_tag_mapping_key() {
    let mapping = access_mapping(None);
    let bc = bounded_context();

    let err = event_store::register_command_type(
        &mapping,
        &bc,
        "OpenTicket".into(),
        "{}".into(),
        Vec::new(),
        Some("company".into()), // never declared as a tag_mappings key
        Vec::new(),
        false,
        None,
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::InvalidOwnerTagKey.code());
}

#[test]
fn register_command_type_accepts_an_owner_tag_key_naming_a_real_tag_mapping_key() {
    let mapping = access_mapping(None);
    let bc = bounded_context();

    let result = event_store::register_command_type(
        &mapping,
        &bc,
        "OpenTicket".into(),
        ticket_schema(),
        vec![skilj_core::shared::TagMapping {
            key: "company".into(),
            field: "company_id".into(),
        }],
        Some("company".into()),
        Vec::new(),
        false,
        None,
    )
    .unwrap();

    assert_eq!(
        result.command_type().owner_tag_key.as_deref(),
        Some("company")
    );
}

/// `owner_tag_key` is re-validated fresh on every registration, not
/// additive - a later registration is free to clear it even though
/// `tag_mappings` keys themselves are additive-only.
#[test]
fn register_command_type_allows_clearing_a_previously_set_owner_tag_key() {
    let mapping = access_mapping(None);
    let bc = bounded_context();
    let existing = CommandType {
        owner_tag_key: Some("company".into()),
        ..open_ticket()
    };

    let result = event_store::register_command_type(
        &mapping,
        &bc,
        "OpenTicket".into(),
        ticket_schema(),
        vec![skilj_core::shared::TagMapping {
            key: "company".into(),
            field: "company_id".into(),
        }],
        None,
        Vec::new(),
        false,
        Some(&existing),
    )
    .unwrap();

    assert_eq!(result.command_type().owner_tag_key, None);
}
