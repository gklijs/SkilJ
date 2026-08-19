//! Tests for the `TypeRegistration` surface's two idempotent-upsert rules
//! (`specs/skilj.allium`) - propagated after the bounded context lifecycle
//! pass (docs/architecture.md §9): `RegisterEventType`/
//! `RegisterCommandType`, and the schema-validation black boxes both rely
//! on - `valid_tag_mappings`/`valid_sensitive_fields`/
//! `schema_is_backwards_compatible` - real for both a bare field name and
//! a two-segment dotted path into a named nested shape (see
//! `resolve_field`'s own doc comment) rather than the `todo!()` stubs
//! every earlier pass left them as.
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pair of rules' 20 total obligations): 20 of 20 - both
//! are pure upsert functions with no actor/`when`-gated surface exposure
//! of their own to defer (unlike every other pass's `surface-actor`/
//! `surface-provides` gap; `TypeRegistration`'s own pair of those
//! obligations belongs to `RegisterProjection`/`RebuildProjection`/
//! `DiscardProjectionRebuild`, not yet propagated - see
//! `event_store`'s own doc comment).

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, CommandType, CommandTypeRegistration, EventType,
    EventTypeRegistration,
};
use skilj_core::shared::{SensitiveField, TagMapping};

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

/// `amount` required, `currency` optional - the common flat shape every
/// test below builds on.
fn schema_v1() -> String {
    r#"{"type":"object","properties":{"amount":{"type":"integer"},"currency":{"type":"string"}},"required":["amount"]}"#.into()
}

/// Adds an optional field on top of `schema_v1` - compatible: a newly
/// added field, and it's optional.
fn schema_v2_added_optional_field() -> String {
    r#"{"type":"object","properties":{"amount":{"type":"integer"},"currency":{"type":"string"},"note":{"type":"string"}},"required":["amount"]}"#.into()
}

/// Loosens `amount` from required to optional on top of `schema_v1` -
/// compatible: loosening, not tightening.
fn schema_v2_loosened_amount() -> String {
    r#"{"type":"object","properties":{"amount":{"type":"integer"},"currency":{"type":"string"}},"required":[]}"#.into()
}

/// Drops `currency` entirely - incompatible: a declared field is
/// permanent.
fn schema_v2_dropped_currency() -> String {
    r#"{"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"]}"#.into()
}

/// Tightens `currency` from optional to required - incompatible.
fn schema_v2_tightened_currency() -> String {
    r#"{"type":"object","properties":{"amount":{"type":"integer"},"currency":{"type":"string"}},"required":["amount","currency"]}"#.into()
}

/// Retypes `amount` from integer to string - incompatible: a field's
/// declared type never changes.
fn schema_v2_retyped_amount() -> String {
    r#"{"type":"object","properties":{"amount":{"type":"string"},"currency":{"type":"string"}},"required":["amount"]}"#.into()
}

/// Adds a newly-required field - incompatible: a newly added field must
/// be optional.
fn schema_v2_added_required_field() -> String {
    r#"{"type":"object","properties":{"amount":{"type":"integer"},"currency":{"type":"string"},"note":{"type":"string"}},"required":["amount","note"]}"#.into()
}

fn tag_mapping(key: &str, field: &str) -> TagMapping {
    TagMapping {
        key: key.into(),
        field: field.into(),
    }
}

fn sensitive_field(field: &str, subject_field: &str) -> SensitiveField {
    SensitiveField {
        field: field.into(),
        subject_key: "account".into(),
        subject_field: subject_field.into(),
    }
}

fn existing_event_type(schema: String, tag_mappings: Vec<TagMapping>) -> EventType {
    EventType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: "OrderPlaced".into(),
        schema,
        schema_version: 1,
        tag_mappings,
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

fn existing_command_type(schema: String, tag_mappings: Vec<TagMapping>) -> CommandType {
    CommandType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: "PlaceOrder".into(),
        schema,
        schema_version: 1,
        tag_mappings,
        sensitive_fields: Vec::new(),
        rest_trigger_allowed: false,
    }
}

// ---------------------------------------------------------------------
// schema_is_backwards_compatible / valid_tag_mappings / valid_sensitive_fields
// - direct coverage of the new black-box logic itself, beyond what the
// register_* obligations below exercise
// ---------------------------------------------------------------------

#[test]
fn schema_is_backwards_compatible_for_an_identical_schema() {
    assert!(event_store::schema_is_backwards_compatible(
        &schema_v1(),
        &schema_v1()
    ));
}

#[test]
fn schema_is_backwards_compatible_when_adding_an_optional_field() {
    assert!(event_store::schema_is_backwards_compatible(
        &schema_v1(),
        &schema_v2_added_optional_field()
    ));
}

#[test]
fn schema_is_backwards_compatible_when_loosening_a_required_field_to_optional() {
    assert!(event_store::schema_is_backwards_compatible(
        &schema_v1(),
        &schema_v2_loosened_amount()
    ));
}

#[test]
fn schema_is_incompatible_when_dropping_a_declared_field() {
    assert!(!event_store::schema_is_backwards_compatible(
        &schema_v1(),
        &schema_v2_dropped_currency()
    ));
}

#[test]
fn schema_is_incompatible_when_tightening_an_optional_field_to_required() {
    assert!(!event_store::schema_is_backwards_compatible(
        &schema_v1(),
        &schema_v2_tightened_currency()
    ));
}

#[test]
fn schema_is_incompatible_when_retyping_a_declared_field() {
    assert!(!event_store::schema_is_backwards_compatible(
        &schema_v1(),
        &schema_v2_retyped_amount()
    ));
}

#[test]
fn schema_is_incompatible_when_a_newly_added_field_is_required() {
    assert!(!event_store::schema_is_backwards_compatible(
        &schema_v1(),
        &schema_v2_added_required_field()
    ));
}

#[test]
fn valid_tag_mappings_accepts_a_field_the_schema_declares() {
    assert!(event_store::valid_tag_mappings(
        &schema_v1(),
        &[tag_mapping("currency", "currency")]
    ));
}

#[test]
fn valid_tag_mappings_rejects_a_field_the_schema_does_not_declare() {
    assert!(!event_store::valid_tag_mappings(
        &schema_v1(),
        &[tag_mapping("bogus", "no_such_field")]
    ));
}

#[test]
fn valid_sensitive_fields_accepts_fields_the_schema_declares() {
    assert!(event_store::valid_sensitive_fields(
        &schema_v1(),
        &[sensitive_field("currency", "amount")]
    ));
}

#[test]
fn valid_sensitive_fields_rejects_a_subject_field_the_schema_does_not_declare() {
    assert!(!event_store::valid_sensitive_fields(
        &schema_v1(),
        &[sensitive_field("currency", "no_such_field")]
    ));
}

// ---------------------------------------------------------------------
// valid_payload - see the "Payload schema shape" note above entity
// CommandType in specs/skilj.allium; checked directly here, at the
// object requires: valid_payload(...) calls it from - CreateExternalEvent.4,
// CreateDirectEvent.4, AuthoriseCommandSubmission.5, AuthoriseCommandTrigger.5
// each cover the caller-facing rejection shape only, not this function's
// own edge cases.
// ---------------------------------------------------------------------

#[test]
fn valid_payload_accepts_a_payload_matching_every_declared_field() {
    assert!(event_store::valid_payload(
        &schema_v1(),
        r#"{"amount":10,"currency":"EUR"}"#
    ));
}

#[test]
fn valid_payload_accepts_an_optional_field_being_absent() {
    // currency is not in schema_v1's own "required" - absent is fine.
    assert!(event_store::valid_payload(&schema_v1(), r#"{"amount":10}"#));
}

#[test]
fn valid_payload_rejects_a_missing_required_field() {
    assert!(!event_store::valid_payload(
        &schema_v1(),
        r#"{"currency":"EUR"}"#
    ));
}

#[test]
fn valid_payload_rejects_a_field_of_the_wrong_type() {
    assert!(!event_store::valid_payload(
        &schema_v1(),
        r#"{"amount":"not an integer"}"#
    ));
}

#[test]
fn valid_payload_rejects_a_malformed_payload() {
    assert!(!event_store::valid_payload(&schema_v1(), "not json at all"));
}

#[test]
fn valid_payload_rejects_a_malformed_schema() {
    assert!(!event_store::valid_payload(
        "not json at all",
        r#"{"amount":10}"#
    ));
}

/// No `additionalProperties: false` anywhere this spec's own payload
/// schema shape ever declares (see the "Payload schema shape" note) - an
/// extra field beyond what's declared is not itself a violation.
#[test]
fn valid_payload_allows_an_undeclared_extra_field() {
    assert!(event_store::valid_payload(
        &schema_v1(),
        r#"{"amount":10,"currency":"EUR","note":"unexpected but harmless"}"#
    ));
}

// ---------------------------------------------------------------------
// rule-success.RegisterEventType (create path) / rule-failure.RegisterEventType.{1..7}
// ---------------------------------------------------------------------

#[test]
fn register_event_type_creates_a_new_type_when_none_exists() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let result = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        vec![tag_mapping("currency", "currency")],
        Vec::new(),
        true,
        false,
        false,
        None,
        None,
        true,
        None, // no existing type
        timestamp(0),
    )
    .unwrap();

    match result {
        EventTypeRegistration::Created(et) => {
            assert_eq!(et.name, "OrderPlaced");
            assert_eq!(et.schema_version, 1);
            assert!(et.external_creation_allowed);
            assert!(et.event_read_allowed);
        }
        other => panic!("expected Created, got {other:?}"),
    }
}

/// rule-failure.RegisterEventType.1 - `requires: access_mapping.status = active`.
#[test]
fn register_event_type_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        false,
        None,
        None,
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.RegisterEventType.2 - `requires: access_mapping.level = admin`.
#[test]
fn register_event_type_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        false,
        None,
        None,
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.RegisterEventType.3 - `requires: access_mapping.bounded_context = bounded_context`.
#[test]
fn register_event_type_rejects_a_bounded_context_the_mapping_is_not_scoped_to() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let other = BoundedContext {
        name: "billing".into(),
        ..bounded_context(BoundedContextStatus::Active)
    };

    let err = event_store::register_event_type(
        &mapping,
        &other,
        "OrderPlaced".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        false,
        None,
        None,
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.RegisterEventType.4 - `requires: bounded_context.status = active`.
#[test]
fn register_event_type_rejects_an_archived_bounded_context() {
    let mapping = RoleAccessMapping {
        bounded_context: bounded_context(BoundedContextStatus::Archived),
        ..access_mapping(RoleStatus::Active, AccessLevel::Admin)
    };
    let bc = bounded_context(BoundedContextStatus::Archived);

    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        false,
        None,
        None,
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.RegisterEventType.5 - `requires: valid_tag_mappings(schema, tag_mappings)`.
#[test]
fn register_event_type_rejects_a_tag_mapping_naming_an_undeclared_field() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        vec![tag_mapping("bogus", "no_such_field")],
        Vec::new(),
        false,
        false,
        false,
        None,
        None,
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::InvalidTagMapping.code());
}

/// rule-failure.RegisterEventType.6 - `requires: valid_sensitive_fields(schema, sensitive_fields)`.
#[test]
fn register_event_type_rejects_a_sensitive_field_naming_an_undeclared_field() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        Vec::new(),
        vec![sensitive_field("no_such_field", "amount")],
        false,
        false,
        false,
        None,
        None,
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::InvalidSensitiveField.code());
}

/// rule-failure.RegisterEventType.7 - a `TagMapping` and a `SensitiveField`
/// may not name the same field.
#[test]
fn register_event_type_rejects_a_tag_mapping_and_sensitive_field_overlap() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        vec![tag_mapping("currency", "currency")],
        vec![sensitive_field("currency", "amount")],
        false,
        false,
        false,
        None,
        None,
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::SensitiveFieldTagOverlap.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.RegisterEventType (update path) / rule-failure.RegisterEventType.{8,9}
// ---------------------------------------------------------------------

#[test]
fn register_event_type_updates_in_place_and_bumps_schema_version_when_schema_changed() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_event_type(schema_v1(), vec![tag_mapping("amount", "amount")]);

    let result = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v2_added_optional_field(),
        vec![tag_mapping("amount", "amount")],
        Vec::new(),
        true,
        false,
        false,
        None,
        None,
        false,
        Some(&existing),
        timestamp(0),
    )
    .unwrap();

    match result {
        EventTypeRegistration::Updated(et) => {
            assert_eq!(et.schema_version, 2); // schema_changed -> +1
            assert_eq!(et.schema, schema_v2_added_optional_field());
            assert!(et.external_creation_allowed); // reconciled to the given value
        }
        other => panic!("expected Updated, got {other:?}"),
    }
}

/// Re-registering an unchanged shape is a no-op on `schema_version` - the
/// idempotent-upsert property the reconciliation loop relies on (see the
/// note above the rule).
#[test]
fn register_event_type_leaves_schema_version_unchanged_when_schema_is_identical() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_event_type(schema_v1(), Vec::new());

    let result = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        false,
        None,
        None,
        false,
        Some(&existing),
        timestamp(0),
    )
    .unwrap();

    assert_eq!(result.event_type().schema_version, 1);
}

/// rule-failure.RegisterEventType.8 - `not exists existing or
/// schema_is_backwards_compatible(existing.schema, schema)`.
#[test]
fn register_event_type_rejects_an_incompatible_schema_change() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_event_type(schema_v1(), Vec::new());

    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v2_dropped_currency(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        false,
        None,
        None,
        false,
        Some(&existing),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::SchemaIncompatible.code());
}

/// rule-failure.RegisterEventType.9 - `not exists existing or
/// existing.tag_mappings.all(m => tag_mappings.any(n => n.key = m.key))`.
#[test]
fn register_event_type_rejects_dropping_an_existing_tag_mapping_key() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_event_type(schema_v1(), vec![tag_mapping("amount", "amount")]);

    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        Vec::new(), // dropped the "amount" key
        Vec::new(),
        false,
        false,
        false,
        None,
        None,
        false,
        Some(&existing),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::TagMappingKeyDropped.code());
}

/// The field a key reads from may change freely - only the key itself is
/// pinned (see the note above the rule).
#[test]
fn register_event_type_allows_retargeting_a_tag_mapping_key_to_a_different_field() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_event_type(schema_v1(), vec![tag_mapping("amount", "amount")]);

    let result = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        vec![tag_mapping("amount", "currency")], // same key, different field
        Vec::new(),
        false,
        false,
        false,
        None,
        None,
        false,
        Some(&existing),
        timestamp(0),
    )
    .unwrap();

    assert_eq!(result.event_type().tag_mappings[0].field, "currency");
}

// ---------------------------------------------------------------------
// RegisterEventType's scheduling opt-in: MissedOccurrencePolicy,
// schedule_position, last_fired_at (the missed-occurrence spec pass)
// ---------------------------------------------------------------------

#[test]
fn register_event_type_rejects_system_triggered_allowed_with_no_schedule_or_policy() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    // Neither schedule nor policy.
    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "DailyDigest".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        true,
        None,
        None,
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(
        err.code(),
        event_store::Error::MissingScheduleOrPolicy.code()
    );

    // Schedule but no policy.
    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "DailyDigest".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        true,
        Some("0 0 3 * * * *".into()),
        None,
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(
        err.code(),
        event_store::Error::MissingScheduleOrPolicy.code()
    );

    // Policy but no schedule.
    let err = event_store::register_event_type(
        &mapping,
        &bc,
        "DailyDigest".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        true,
        None,
        Some(event_store::MissedOccurrencePolicy::Skip),
        false,
        None,
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(
        err.code(),
        event_store::Error::MissingScheduleOrPolicy.code()
    );
}

#[test]
fn register_event_type_anchors_schedule_position_on_first_opt_in() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let result = event_store::register_event_type(
        &mapping,
        &bc,
        "DailyDigest".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        true,
        Some("0 0 3 * * * *".into()),
        Some(event_store::MissedOccurrencePolicy::ReplayBacklog),
        false,
        None,
        timestamp(1000),
    )
    .unwrap();

    let et = result.event_type();
    assert_eq!(
        et.missed_occurrence_policy,
        Some(event_store::MissedOccurrencePolicy::ReplayBacklog)
    );
    assert_eq!(et.schedule_position, Some(timestamp(1000)));
    assert_eq!(et.last_fired_at, None);
}

/// Re-registration with scheduling already on leaves both positions
/// exactly where the scheduler left them - a deploy is not a way to
/// re-fire (resetting back) or skip (resetting forward) a backlog.
#[test]
fn register_event_type_leaves_schedule_position_untouched_on_re_registration() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = EventType {
        system_triggered_allowed: true,
        system_triggered_schedule: Some("0 0 3 * * * *".into()),
        missed_occurrence_policy: Some(event_store::MissedOccurrencePolicy::Skip),
        schedule_position: Some(timestamp(500)),
        last_fired_at: Some(timestamp(400)),
        ..existing_event_type(schema_v1(), Vec::new())
    };

    let result = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        true,
        Some("0 0 4 * * * *".into()), // schedule string itself may change freely
        Some(event_store::MissedOccurrencePolicy::FireOnce), // policy may change too
        false,
        Some(&existing),
        timestamp(9999), // even a much later "now" doesn't move the position
    )
    .unwrap();

    let et = result.event_type();
    assert_eq!(et.schedule_position, Some(timestamp(500)));
    assert_eq!(et.last_fired_at, Some(timestamp(400)));
    assert_eq!(
        et.missed_occurrence_policy,
        Some(event_store::MissedOccurrencePolicy::FireOnce)
    );
}

/// Switching scheduling off and on again re-anchors the position - the
/// off period is not a backlog anybody is owed.
#[test]
fn register_event_type_re_anchors_schedule_position_when_switched_off_and_on_again() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = EventType {
        system_triggered_allowed: false, // was switched off
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: Some(timestamp(400)), // what once fired stays a fact
        ..existing_event_type(schema_v1(), Vec::new())
    };

    let result = event_store::register_event_type(
        &mapping,
        &bc,
        "OrderPlaced".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        false,
        true, // switched back on
        Some("0 0 3 * * * *".into()),
        Some(event_store::MissedOccurrencePolicy::Skip),
        false,
        Some(&existing),
        timestamp(9999),
    )
    .unwrap();

    let et = result.event_type();
    assert_eq!(et.schedule_position, Some(timestamp(9999)));
    assert_eq!(et.last_fired_at, Some(timestamp(400))); // untouched
}

// ---------------------------------------------------------------------
// rule-success.RegisterCommandType (create + update paths) /
// rule-failure.RegisterCommandType.{1..9}
// ---------------------------------------------------------------------
//
// Same shape and requires-clauses as RegisterEventType above (see
// register_command_type's own doc comment) - full parallel coverage,
// since RegisterCommandType is separately obligated.

#[test]
fn register_command_type_creates_a_new_type_when_none_exists() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let result = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v1(),
        vec![tag_mapping("currency", "currency")],
        Vec::new(),
        true,
        None,
    )
    .unwrap();

    match result {
        CommandTypeRegistration::Created(ct) => {
            assert_eq!(ct.name, "PlaceOrder");
            assert_eq!(ct.schema_version, 1);
            assert!(ct.rest_trigger_allowed);
        }
        other => panic!("expected Created, got {other:?}"),
    }
}

/// rule-failure.RegisterCommandType.1
#[test]
fn register_command_type_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        None,
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.RegisterCommandType.2
#[test]
fn register_command_type_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.RegisterCommandType.3
#[test]
fn register_command_type_rejects_a_bounded_context_the_mapping_is_not_scoped_to() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let other = BoundedContext {
        name: "billing".into(),
        ..bounded_context(BoundedContextStatus::Active)
    };

    let err = event_store::register_command_type(
        &mapping,
        &other,
        "PlaceOrder".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.RegisterCommandType.4
#[test]
fn register_command_type_rejects_an_archived_bounded_context() {
    let mapping = RoleAccessMapping {
        bounded_context: bounded_context(BoundedContextStatus::Archived),
        ..access_mapping(RoleStatus::Active, AccessLevel::Admin)
    };
    let bc = bounded_context(BoundedContextStatus::Archived);

    let err = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.RegisterCommandType.5
#[test]
fn register_command_type_rejects_a_tag_mapping_naming_an_undeclared_field() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v1(),
        vec![tag_mapping("bogus", "no_such_field")],
        Vec::new(),
        false,
        None,
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::InvalidTagMapping.code());
}

/// rule-failure.RegisterCommandType.6
#[test]
fn register_command_type_rejects_a_sensitive_field_naming_an_undeclared_field() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v1(),
        Vec::new(),
        vec![sensitive_field("no_such_field", "amount")],
        false,
        None,
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::InvalidSensitiveField.code());
}

/// rule-failure.RegisterCommandType.7
#[test]
fn register_command_type_rejects_a_tag_mapping_and_sensitive_field_overlap() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v1(),
        vec![tag_mapping("currency", "currency")],
        vec![sensitive_field("currency", "amount")],
        false,
        None,
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::SensitiveFieldTagOverlap.code()
    );
}

#[test]
fn register_command_type_updates_in_place_and_bumps_schema_version_when_schema_changed() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_command_type(schema_v1(), vec![tag_mapping("amount", "amount")]);

    let result = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v2_added_optional_field(),
        vec![tag_mapping("amount", "amount")],
        Vec::new(),
        true,
        Some(&existing),
    )
    .unwrap();

    match result {
        CommandTypeRegistration::Updated(ct) => {
            assert_eq!(ct.schema_version, 2);
            assert!(ct.rest_trigger_allowed);
        }
        other => panic!("expected Updated, got {other:?}"),
    }
}

/// rule-failure.RegisterCommandType.8
#[test]
fn register_command_type_rejects_an_incompatible_schema_change() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_command_type(schema_v1(), Vec::new());

    let err = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v2_dropped_currency(),
        Vec::new(),
        Vec::new(),
        false,
        Some(&existing),
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::SchemaIncompatible.code());
}

/// rule-failure.RegisterCommandType.9
#[test]
fn register_command_type_rejects_dropping_an_existing_tag_mapping_key() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_command_type(schema_v1(), vec![tag_mapping("amount", "amount")]);

    let err = event_store::register_command_type(
        &mapping,
        &bc,
        "PlaceOrder".into(),
        schema_v1(),
        Vec::new(),
        Vec::new(),
        false,
        Some(&existing),
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::TagMappingKeyDropped.code());
}
