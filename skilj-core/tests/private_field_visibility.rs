//! Tests for the private-field mechanism (docs/architecture.md's own
//! write-up of this pass) - a third field-level protection, alongside
//! `sensitive_fields` (crypto-shredding-shaped PII protection, subject
//! named by the payload) and the owner-tag `scope` cross-tenant series
//! ([§23](../../docs/architecture.md#cross-tenant-projection-read-fix-owner-tag)-30). Unlike both of those, this is a plain read-time redaction
//! rule with no encryption anywhere behind it: a private field is always
//! stored in plaintext, and `own`/`team`/`addressed` each name a
//! different default reader for it (see `value PrivateField` in the
//! spec).
//!
//! Every function this pass touches (`valid_private_fields`,
//! `is_default_private_reader`, `render_event`/`render_command`'s own
//! redaction pass, `redact_private_fields`,
//! `grant_private_field_access_for_event`/`_for_command`,
//! `revoke_private_field_access`, `list_private_field_grants`) is pure -
//! no Postgres - the same "test the layer in isolation" shape
//! `event_owner_scoping.rs`/`command_owner_scoping.rs` already use. The
//! DB round-trip of `EventType.private_fields`/`CommandType.private_fields`/
//! `PrivateFieldGrant` itself is covered by `persistence.rs`'s existing
//! whole-struct `assert_eq!` round-trip convention.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{
    self, AccessLevel, PrivateFieldGrant, Role, RoleAccessMapping, RoleStatus, TokenStatus,
};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, Command, CommandType, Event, EventOrigin, EventType,
};
use skilj_core::shared::{Metadata, PrivateField, PrivateFieldKind, TagMapping};

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

fn role(id: &str, name: &str, external_subject: &str) -> Role {
    Role {
        id: id.into(),
        external_subject: external_subject.into(),
        name: name.into(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn access_mapping(role: Role, level: AccessLevel) -> RoleAccessMapping {
    RoleAccessMapping {
        role,
        bounded_context: bounded_context(),
        level,
        can_read_sensitive: false,
        scope: None,
        status: RoleStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn own_field() -> PrivateField {
    PrivateField {
        field: "note".into(),
        kind: PrivateFieldKind::Own,
        team: None,
        addressee_field: None,
    }
}

fn team_field(team: &str) -> PrivateField {
    PrivateField {
        field: "note".into(),
        kind: PrivateFieldKind::Team,
        team: Some(team.into()),
        addressee_field: None,
    }
}

fn addressed_field() -> PrivateField {
    PrivateField {
        field: "note".into(),
        kind: PrivateFieldKind::Addressed,
        team: None,
        addressee_field: Some("addressee_id".into()),
    }
}

/// A schema real enough for `valid_private_fields` to resolve `note` and
/// `addressee_id` as real leaves.
fn schema() -> String {
    r#"{"properties":{"note":{"type":"string"},"addressee_id":{"type":"string"}}}"#.into()
}

fn event_type(private_fields: Vec<PrivateField>) -> EventType {
    EventType {
        bounded_context: bounded_context(),
        name: "TicketNoteAdded".into(),
        schema: schema(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields,
        external_creation_allowed: false,
        direct_creation_allowed: true,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: true,
    }
}

fn command_type(private_fields: Vec<PrivateField>) -> CommandType {
    CommandType {
        bounded_context: bounded_context(),
        name: "AddTicketNote".into(),
        schema: schema(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields,
        rest_trigger_allowed: false,
    }
}

fn event(et: &EventType, sequence: i64, client_id: &str, payload: &str) -> Event {
    Event {
        bounded_context: bounded_context(),
        event_type: et.clone(),
        payload: payload.into(),
        metadata: Metadata {
            r#type: et.name.clone(),
            version: 1,
            client_id: client_id.into(),
            created_at: timestamp(0),
            correlation_id: None,
            causation_id: None,
        },
        sequence,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

fn command(ct: &CommandType, id: &str, client_id: &str, payload: &str) -> Command {
    Command {
        id: id.into(),
        bounded_context: bounded_context(),
        command_type: ct.clone(),
        payload: payload.into(),
        metadata: Metadata {
            r#type: ct.name.clone(),
            version: 1,
            client_id: client_id.into(),
            created_at: timestamp(0),
            correlation_id: None,
            causation_id: None,
        },
        encryption_keys: Vec::new(),
        consistency_tags: Vec::new(),
        consistency_query: Vec::new(),
        consistency_boundary: None,
    }
}

fn no_key(_: &str, _: &str) -> Option<skilj_core::encryption::DataKey> {
    unreachable!("no sensitive fields in these tests")
}

fn blanket_grant(grantor: Role, grantee: Role) -> PrivateFieldGrant {
    PrivateFieldGrant {
        id: "grant-1".into(),
        bounded_context: bounded_context(),
        grantor,
        grantee,
        event_sequence: None,
        command_id: None,
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

// ---------------------------------------------------------------------
// valid_private_fields
// ---------------------------------------------------------------------

#[test]
fn valid_private_fields_accepts_a_real_own_field() {
    assert!(event_store::valid_private_fields(&schema(), &[own_field()]));
}

#[test]
fn valid_private_fields_accepts_a_real_team_field() {
    assert!(event_store::valid_private_fields(
        &schema(),
        &[team_field("staff")]
    ));
}

#[test]
fn valid_private_fields_accepts_a_real_addressed_field() {
    assert!(event_store::valid_private_fields(
        &schema(),
        &[addressed_field()]
    ));
}

#[test]
fn valid_private_fields_rejects_a_field_the_schema_does_not_declare() {
    let mut f = own_field();
    f.field = "no_such_field".into();
    assert!(!event_store::valid_private_fields(&schema(), &[f]));
}

#[test]
fn valid_private_fields_rejects_a_team_field_missing_its_team() {
    let mut f = team_field("staff");
    f.team = None;
    assert!(!event_store::valid_private_fields(&schema(), &[f]));
}

#[test]
fn valid_private_fields_rejects_an_own_field_carrying_a_team() {
    let mut f = own_field();
    f.team = Some("staff".into());
    assert!(!event_store::valid_private_fields(&schema(), &[f]));
}

#[test]
fn valid_private_fields_rejects_an_addressed_field_missing_its_addressee_field() {
    let mut f = addressed_field();
    f.addressee_field = None;
    assert!(!event_store::valid_private_fields(&schema(), &[f]));
}

#[test]
fn valid_private_fields_rejects_an_addressed_field_whose_addressee_field_is_undeclared() {
    let mut f = addressed_field();
    f.addressee_field = Some("no_such_field".into());
    assert!(!event_store::valid_private_fields(&schema(), &[f]));
}

#[test]
fn valid_private_fields_is_vacuously_true_when_empty() {
    assert!(event_store::valid_private_fields(&schema(), &[]));
}

// ---------------------------------------------------------------------
// RegisterEventType/RegisterCommandType - the two new requires clauses
// ---------------------------------------------------------------------

#[test]
fn register_event_type_rejects_a_private_field_naming_an_undeclared_field() {
    let mapping = access_mapping(role("admin", "Admin", "admin@x"), AccessLevel::Admin);
    let mut f = own_field();
    f.field = "no_such_field".into();

    let err = event_store::register_event_type(
        &mapping,
        &bounded_context(),
        "TicketNoteAdded".into(),
        schema(),
        Vec::new(),
        None,
        Vec::new(),
        vec![f],
        false,
        true,
        false,
        None,
        None,
        true,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::InvalidPrivateField.code());
}

#[test]
fn register_event_type_rejects_a_private_field_that_is_also_a_tag_mapping() {
    let mapping = access_mapping(role("admin", "Admin", "admin@x"), AccessLevel::Admin);
    let mut f = own_field();
    f.field = "note".into();

    let err = event_store::register_event_type(
        &mapping,
        &bounded_context(),
        "TicketNoteAdded".into(),
        schema(),
        vec![TagMapping {
            key: "note".into(),
            field: "note".into(),
        }],
        None,
        Vec::new(),
        vec![f],
        false,
        true,
        false,
        None,
        None,
        true,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::PrivateFieldOverlap.code());
}

#[test]
fn register_event_type_rejects_a_private_field_that_is_also_sensitive() {
    let mapping = access_mapping(role("admin", "Admin", "admin@x"), AccessLevel::Admin);

    let err = event_store::register_event_type(
        &mapping,
        &bounded_context(),
        "TicketNoteAdded".into(),
        schema(),
        Vec::new(),
        None,
        vec![skilj_core::shared::SensitiveField {
            field: "note".into(),
            subject_key: "user".into(),
            subject_field: "addressee_id".into(),
        }],
        vec![own_field()],
        false,
        true,
        false,
        None,
        None,
        true,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::PrivateFieldOverlap.code());
}

#[test]
fn register_event_type_accepts_a_real_private_field() {
    let mapping = access_mapping(role("admin", "Admin", "admin@x"), AccessLevel::Admin);

    let registration = event_store::register_event_type(
        &mapping,
        &bounded_context(),
        "TicketNoteAdded".into(),
        schema(),
        Vec::new(),
        None,
        Vec::new(),
        vec![own_field()],
        false,
        true,
        false,
        None,
        None,
        true,
        None,
        timestamp(0),
    )
    .unwrap();

    assert_eq!(registration.event_type().private_fields, vec![own_field()]);
}

// ---------------------------------------------------------------------
// is_default_private_reader
// ---------------------------------------------------------------------

#[test]
fn is_default_private_reader_holds_for_the_events_own_creator() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    let creator = role("role-1", "Agent", "agent@x");

    assert!(event_store::is_default_private_reader(&e, &creator));
}

#[test]
fn is_default_private_reader_fails_for_a_different_role() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    let someone_else = role("role-2", "Other Agent", "other@x");

    assert!(!event_store::is_default_private_reader(&e, &someone_else));
}

#[test]
fn is_default_private_reader_holds_for_the_payloads_own_addressee() {
    let et = event_type(vec![addressed_field()]);
    let e = event(
        &et,
        0,
        "creator-role",
        r#"{"note":"for you","addressee_id":"customer@x"}"#,
    );
    let addressee = role("cust-role", "Customer", "customer@x");

    assert!(event_store::is_default_private_reader(&e, &addressee));
}

#[test]
fn is_default_private_reader_fails_closed_when_the_addressee_field_is_absent() {
    let et = event_type(vec![addressed_field()]);
    let e = event(&et, 0, "creator-role", r#"{"note":"for you"}"#);
    let someone = role("cust-role", "Customer", "customer@x");

    assert!(!event_store::is_default_private_reader(&e, &someone));
}

#[test]
fn is_default_private_reader_fails_for_a_type_declaring_only_team_kind_fields() {
    let et = event_type(vec![team_field("staff")]);
    let e = event(&et, 0, "role-1", r#"{"note":"internal"}"#);
    let creator = role("role-1", "Agent", "agent@x");

    // Even the creator is not the "default reader" of a team-kind field -
    // there is no default reader to be, see enum PrivateFieldKind.
    assert!(!event_store::is_default_private_reader(&e, &creator));
}

#[test]
fn is_default_private_reader_works_identically_for_commands() {
    let ct = command_type(vec![own_field()]);
    let c = command(&ct, "cmd-1", "role-1", r#"{"note":"private"}"#);
    let creator = role("role-1", "Agent", "agent@x");
    let someone_else = role("role-2", "Other", "other@x");

    assert!(event_store::is_default_private_reader(&c, &creator));
    assert!(!event_store::is_default_private_reader(&c, &someone_else));
}

// ---------------------------------------------------------------------
// render_event's third redaction pass
// ---------------------------------------------------------------------

#[test]
fn render_event_leaves_an_own_field_visible_to_its_creator() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    let mapping = access_mapping(role("role-1", "Agent", "agent@x"), AccessLevel::Admin);

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[]);

    assert_eq!(rendered, r#"{"note":"private"}"#);
}

#[test]
fn render_event_redacts_an_own_field_from_a_non_creator_to_null() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    let mapping = access_mapping(role("role-2", "Other Agent", "other@x"), AccessLevel::Admin);

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[]);

    assert_eq!(rendered, r#"{"note":null}"#);
}

#[test]
fn render_event_leaves_a_team_field_visible_to_a_matching_role_name() {
    let et = event_type(vec![team_field("staff")]);
    let e = event(&et, 0, "someone", r#"{"note":"internal"}"#);
    let mapping = access_mapping(role("role-9", "staff", "staff@x"), AccessLevel::Admin);

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[]);

    assert_eq!(rendered, r#"{"note":"internal"}"#);
}

#[test]
fn render_event_redacts_a_team_field_from_a_non_matching_role_name() {
    let et = event_type(vec![team_field("staff")]);
    let e = event(&et, 0, "someone", r#"{"note":"internal"}"#);
    let mapping = access_mapping(role("role-9", "customer", "cust@x"), AccessLevel::Admin);

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[]);

    assert_eq!(rendered, r#"{"note":null}"#);
}

#[test]
fn render_event_leaves_an_addressed_field_visible_to_the_addressee() {
    let et = event_type(vec![addressed_field()]);
    let e = event(
        &et,
        0,
        "creator-role",
        r#"{"note":"for you","addressee_id":"customer@x"}"#,
    );
    let mapping = access_mapping(
        role("cust-role", "Customer", "customer@x"),
        AccessLevel::Admin,
    );

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[]);

    assert_eq!(
        rendered,
        r#"{"addressee_id":"customer@x","note":"for you"}"#
    );
}

#[test]
fn render_event_redacts_an_addressed_field_from_anyone_else() {
    let et = event_type(vec![addressed_field()]);
    let e = event(
        &et,
        0,
        "creator-role",
        r#"{"note":"for you","addressee_id":"customer@x"}"#,
    );
    let mapping = access_mapping(
        role("other-role", "Someone Else", "else@x"),
        AccessLevel::Admin,
    );

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[]);

    assert_eq!(rendered, r#"{"addressee_id":"customer@x","note":null}"#);
}

#[test]
fn render_event_a_per_record_grant_opens_an_own_field_to_its_grantee() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    let grantee = role("role-2", "Colleague", "colleague@x");
    let mapping = access_mapping(grantee.clone(), AccessLevel::Admin);
    let grant = PrivateFieldGrant {
        id: "grant-1".into(),
        bounded_context: bounded_context(),
        grantor: role("role-1", "Agent", "agent@x"),
        grantee,
        event_sequence: Some(0),
        command_id: None,
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
    };

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[grant]);

    assert_eq!(rendered, r#"{"note":"private"}"#);
}

#[test]
fn render_event_a_per_record_grant_does_not_open_a_different_record() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 5, "role-1", r#"{"note":"private"}"#); // sequence 5, not 0
    let grantee = role("role-2", "Colleague", "colleague@x");
    let mapping = access_mapping(grantee.clone(), AccessLevel::Admin);
    let grant = PrivateFieldGrant {
        id: "grant-1".into(),
        bounded_context: bounded_context(),
        grantor: role("role-1", "Agent", "agent@x"),
        grantee,
        event_sequence: Some(0), // names a different event
        command_id: None,
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
    };

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[grant]);

    assert_eq!(rendered, r#"{"note":null}"#);
}

#[test]
fn render_event_a_blanket_grant_opens_every_record_the_grantor_is_the_default_reader_of() {
    let et = event_type(vec![own_field()]);
    let e1 = event(&et, 0, "role-1", r#"{"note":"first"}"#);
    let e2 = event(&et, 1, "role-1", r#"{"note":"second"}"#);
    let grantor = role("role-1", "Agent", "agent@x");
    let grantee = role("role-2", "Colleague", "colleague@x");
    let mapping = access_mapping(grantee.clone(), AccessLevel::Admin);
    let grant = blanket_grant(grantor, grantee);

    assert_eq!(
        event_store::render_event(&e1, &mapping, &no_key, std::slice::from_ref(&grant)),
        r#"{"note":"first"}"#
    );
    assert_eq!(
        event_store::render_event(&e2, &mapping, &no_key, &[grant]),
        r#"{"note":"second"}"#
    );
}

#[test]
fn render_event_a_blanket_grant_never_reaches_past_what_its_own_grantor_could_reach() {
    let et = event_type(vec![own_field()]);
    // Created by someone other than the grantor - the grantor is not this
    // record's own default reader, so the blanket grant re-derived
    // against it fails, exactly as a per-record grant would.
    let e = event(&et, 0, "someone-else", r#"{"note":"not the grantors"}"#);
    let grantor = role("role-1", "Agent", "agent@x");
    let grantee = role("role-2", "Colleague", "colleague@x");
    let mapping = access_mapping(grantee.clone(), AccessLevel::Admin);
    let grant = blanket_grant(grantor, grantee);

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[grant]);

    assert_eq!(rendered, r#"{"note":null}"#);
}

#[test]
fn render_event_a_revoked_grant_no_longer_opens_the_field() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    let grantee = role("role-2", "Colleague", "colleague@x");
    let mapping = access_mapping(grantee.clone(), AccessLevel::Admin);
    let grant = PrivateFieldGrant {
        status: TokenStatus::Revoked,
        revoked_at: Some(timestamp(1)),
        ..blanket_grant(role("role-1", "Agent", "agent@x"), grantee)
    };

    let rendered = event_store::render_event(&e, &mapping, &no_key, &[grant]);

    assert_eq!(rendered, r#"{"note":null}"#);
}

#[test]
fn render_command_redacts_the_same_way_render_event_does() {
    let ct = command_type(vec![own_field()]);
    let c = command(&ct, "cmd-1", "role-1", r#"{"note":"private"}"#);
    let creator = access_mapping(role("role-1", "Agent", "agent@x"), AccessLevel::Admin);
    let stranger = access_mapping(role("role-2", "Other", "other@x"), AccessLevel::Admin);

    assert_eq!(
        event_store::render_command(&c, &creator, &no_key, &[]),
        r#"{"note":"private"}"#
    );
    assert_eq!(
        event_store::render_command(&c, &stranger, &no_key, &[]),
        r#"{"note":null}"#
    );
}

// ---------------------------------------------------------------------
// redact_private_fields - the REST track's unconditional counterpart
// ---------------------------------------------------------------------

#[test]
fn redact_private_fields_nulls_an_own_field_unconditionally() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);

    let redacted = event_store::redact_private_fields(&e);

    assert_eq!(redacted.payload, r#"{"note":null}"#);
}

#[test]
fn redact_private_fields_nulls_a_team_field_unconditionally() {
    let et = event_type(vec![team_field("staff")]);
    let e = event(&et, 0, "role-1", r#"{"note":"internal"}"#);

    let redacted = event_store::redact_private_fields(&e);

    assert_eq!(redacted.payload, r#"{"note":null}"#);
}

#[test]
fn redact_private_fields_is_a_no_op_when_the_type_declares_none() {
    let et = event_type(Vec::new());
    let e = event(&et, 0, "role-1", r#"{"note":"plain"}"#);

    let redacted = event_store::redact_private_fields(&e);

    assert_eq!(redacted.payload, r#"{"note":"plain"}"#);
}

// ---------------------------------------------------------------------
// grant_private_field_access_for_event / _for_command
// ---------------------------------------------------------------------

#[test]
fn grant_private_field_access_for_event_succeeds_for_the_events_own_creator() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    let mapping = access_mapping(role("role-1", "Agent", "agent@x"), AccessLevel::Write);
    let grantee = role("role-2", "Colleague", "colleague@x");

    let grant = access_control::grant_private_field_access_for_event(
        &mapping,
        &grantee,
        Some(&e),
        "grant-1".into(),
        timestamp(0),
    )
    .unwrap();

    assert_eq!(grant.grantor, mapping.role);
    assert_eq!(grant.grantee, grantee);
    assert_eq!(grant.event_sequence, Some(0));
}

#[test]
fn grant_private_field_access_for_event_rejects_a_caller_who_is_not_the_default_reader() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    let mapping = access_mapping(role("role-2", "Not The Creator", "x@x"), AccessLevel::Write);
    let grantee = role("role-3", "Colleague", "colleague@x");

    let err = access_control::grant_private_field_access_for_event(
        &mapping,
        &grantee,
        Some(&e),
        "grant-1".into(),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::NotDefaultPrivateReader.code()
    );
}

#[test]
fn grant_private_field_access_for_event_rejects_a_record_from_another_bounded_context() {
    let et = event_type(vec![own_field()]);
    let mut e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    e.bounded_context = BoundedContext {
        name: "other".into(),
        ..bounded_context()
    };
    let mapping = access_mapping(role("role-1", "Agent", "agent@x"), AccessLevel::Write);
    let grantee = role("role-2", "Colleague", "colleague@x");

    let err = access_control::grant_private_field_access_for_event(
        &mapping,
        &grantee,
        Some(&e),
        "grant-1".into(),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::PrivateFieldRecordWrongBoundedContext.code()
    );
}

#[test]
fn grant_private_field_access_for_event_a_blanket_grant_needs_no_record_at_all() {
    let mapping = access_mapping(role("role-1", "Agent", "agent@x"), AccessLevel::Write);
    let grantee = role("role-2", "Colleague", "colleague@x");

    let grant = access_control::grant_private_field_access_for_event(
        &mapping,
        &grantee,
        None,
        "grant-1".into(),
        timestamp(0),
    )
    .unwrap();

    assert_eq!(grant.event_sequence, None);
    assert_eq!(grant.command_id, None);
}

#[test]
fn grant_private_field_access_for_command_succeeds_for_the_commands_own_creator() {
    let ct = command_type(vec![own_field()]);
    let c = command(&ct, "cmd-1", "role-1", r#"{"note":"private"}"#);
    let mapping = access_mapping(role("role-1", "Agent", "agent@x"), AccessLevel::Write);
    let grantee = role("role-2", "Colleague", "colleague@x");

    let grant = access_control::grant_private_field_access_for_command(
        &mapping,
        &grantee,
        Some(&c),
        "grant-1".into(),
        timestamp(0),
    )
    .unwrap();

    assert_eq!(grant.command_id, Some("cmd-1".to_string()));
}

#[test]
fn grant_private_field_access_rejects_an_inactive_grantee() {
    let et = event_type(vec![own_field()]);
    let e = event(&et, 0, "role-1", r#"{"note":"private"}"#);
    let mapping = access_mapping(role("role-1", "Agent", "agent@x"), AccessLevel::Write);
    let grantee = Role {
        status: RoleStatus::Revoked,
        ..role("role-2", "Colleague", "colleague@x")
    };

    let err = access_control::grant_private_field_access_for_event(
        &mapping,
        &grantee,
        Some(&e),
        "grant-1".into(),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

// ---------------------------------------------------------------------
// revoke_private_field_access
// ---------------------------------------------------------------------

#[test]
fn revoke_private_field_access_succeeds_for_the_grants_own_grantor() {
    let grantor = role("role-1", "Agent", "agent@x");
    let grantee = role("role-2", "Colleague", "colleague@x");
    let grant = blanket_grant(grantor.clone(), grantee);
    let mapping = access_mapping(grantor, AccessLevel::Write);

    let revoked =
        access_control::revoke_private_field_access(&mapping, &grant, timestamp(500)).unwrap();

    assert_eq!(revoked.status, TokenStatus::Revoked);
    assert_eq!(revoked.revoked_at, Some(timestamp(500)));
}

#[test]
fn revoke_private_field_access_rejects_the_grantee() {
    let grantor = role("role-1", "Agent", "agent@x");
    let grantee = role("role-2", "Colleague", "colleague@x");
    let grant = blanket_grant(grantor, grantee.clone());
    let mapping = access_mapping(grantee, AccessLevel::Write);

    let err =
        access_control::revoke_private_field_access(&mapping, &grant, timestamp(500)).unwrap_err();

    assert_eq!(err.code(), access_control::Error::NotGrantor.code());
}

#[test]
fn revoke_private_field_access_rejects_an_already_revoked_grant() {
    let grantor = role("role-1", "Agent", "agent@x");
    let grantee = role("role-2", "Colleague", "colleague@x");
    let grant = PrivateFieldGrant {
        status: TokenStatus::Revoked,
        revoked_at: Some(timestamp(1)),
        ..blanket_grant(grantor.clone(), grantee)
    };
    let mapping = access_mapping(grantor, AccessLevel::Write);

    let err =
        access_control::revoke_private_field_access(&mapping, &grant, timestamp(500)).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::PrivateFieldGrantNotActive.code()
    );
}

// ---------------------------------------------------------------------
// list_private_field_grants
// ---------------------------------------------------------------------

#[test]
fn list_private_field_grants_defaults_to_the_callers_own_outgoing_grants() {
    let me = role("role-1", "Agent", "agent@x");
    let someone_else = role("role-2", "Other", "other@x");
    let grantee = role("role-3", "Colleague", "colleague@x");
    let mine = blanket_grant(me.clone(), grantee.clone());
    let theirs = PrivateFieldGrant {
        id: "grant-2".into(),
        ..blanket_grant(someone_else, grantee)
    };
    let mapping = access_mapping(me, AccessLevel::Write);

    let grants =
        access_control::list_private_field_grants(&mapping, None, &[mine.clone(), theirs]).unwrap();

    assert_eq!(grants, vec![mine]);
}

#[test]
fn list_private_field_grants_lets_an_admin_list_someone_elses() {
    let admin = role("admin", "Admin", "admin@x");
    let someone_else = role("role-2", "Other", "other@x");
    let grantee = role("role-3", "Colleague", "colleague@x");
    let theirs = blanket_grant(someone_else.clone(), grantee);
    let mapping = access_mapping(admin, AccessLevel::Admin);

    let grants = access_control::list_private_field_grants(
        &mapping,
        Some(&someone_else),
        std::slice::from_ref(&theirs),
    )
    .unwrap();

    assert_eq!(grants, vec![theirs]);
}

#[test]
fn list_private_field_grants_rejects_a_non_admin_listing_someone_elses() {
    let me = role("role-1", "Agent", "agent@x");
    let someone_else = role("role-2", "Other", "other@x");
    let mapping = access_mapping(me, AccessLevel::Write);

    let err =
        access_control::list_private_field_grants(&mapping, Some(&someone_else), &[]).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

#[test]
fn list_private_field_grants_includes_revoked_grants_too() {
    let me = role("role-1", "Agent", "agent@x");
    let grantee = role("role-2", "Colleague", "colleague@x");
    let revoked = PrivateFieldGrant {
        status: TokenStatus::Revoked,
        revoked_at: Some(timestamp(1)),
        ..blanket_grant(me.clone(), grantee)
    };
    let mapping = access_mapping(me, AccessLevel::Write);

    let grants =
        access_control::list_private_field_grants(&mapping, None, std::slice::from_ref(&revoked))
            .unwrap();

    assert_eq!(grants, vec![revoked]);
}
