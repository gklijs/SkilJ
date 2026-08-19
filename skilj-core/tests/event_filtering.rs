//! Tests for `event_store::valid_filters`/`matches_filters` - the
//! `Filter`/`FilterOperator` mechanism - and the shared "reject anything
//! that does not land on a scalar or list-of-scalar leaf" retrofit to
//! `valid_tag_mappings`/`valid_sensitive_fields` (see the plan at
//! `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`). Pure logic
//! only, no DB.

use chrono::{TimeZone, Utc};
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::shared::{Filter, FilterOperator, Metadata, SensitiveField, TagMapping};

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

/// One schema exercising every shape `valid_filters`'/`valid_tag_mappings`'/
/// `valid_sensitive_fields`' shared classification needs to distinguish -
/// including the two shapes found via a real `schema_for!` probe
/// (`Option<T>`'s `["x","null"]`-shaped `type`, and a unit enum's bare
/// `$ref`) and the one-level nested-object shape (`address`) that stays
/// `Other` for a bare reference.
const SCHEMA: &str = r##"{
    "properties": {
        "name": {"type": "string"},
        "when": {"type": "string", "format": "date-time"},
        "day": {"type": "string", "format": "date"},
        "clock": {"type": "string", "format": "partial-date-time"},
        "id": {"type": "string", "format": "uuid"},
        "amount": {"type": "integer"},
        "price": {"type": "number"},
        "active": {"type": "boolean"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "optional_name": {"type": ["string", "null"]},
        "optional_tags": {"type": ["array", "null"], "items": {"type": "string"}},
        "status": {"$ref": "#/definitions/Status"},
        "address": {"$ref": "#/definitions/Address"}
    },
    "definitions": {
        "Status": {"type": "string", "enum": ["Active", "Archived"]},
        "Address": {
            "type": "object",
            "properties": {"country": {"type": "string"}}
        }
    }
}"##;

fn event_type() -> EventType {
    EventType {
        bounded_context: bounded_context(),
        name: "OrderPlaced".into(),
        schema: SCHEMA.into(),
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

fn filter(field: &str, operator: FilterOperator, value: &str) -> Filter {
    Filter {
        field: field.into(),
        operator,
        value: value.into(),
    }
}

// ---------------------------------------------------------------------
// valid_filters: the type-to-operator matrix
// ---------------------------------------------------------------------

#[test]
fn valid_filters_plain_string_accepts_equals_contains_is_like_only() {
    let et = event_type();
    for op in [
        FilterOperator::Equals,
        FilterOperator::Contains,
        FilterOperator::IsLike,
    ] {
        assert!(event_store::valid_filters(&et, &[filter("name", op, "x")]));
    }
    for op in [FilterOperator::GreaterThan, FilterOperator::LessThan] {
        assert!(!event_store::valid_filters(&et, &[filter("name", op, "x")]));
    }
}

#[test]
fn valid_filters_date_time_date_and_partial_date_time_additionally_accept_ordering() {
    let et = event_type();
    for field in ["when", "day", "clock"] {
        for op in [FilterOperator::GreaterThan, FilterOperator::LessThan] {
            assert!(
                event_store::valid_filters(&et, &[filter(field, op, "x")]),
                "{field} should accept {op:?}"
            );
        }
    }
}

#[test]
fn valid_filters_uuid_format_is_a_plain_string_no_ordering() {
    let et = event_type();
    assert!(event_store::valid_filters(
        &et,
        &[filter("id", FilterOperator::Equals, "x")]
    ));
    assert!(!event_store::valid_filters(
        &et,
        &[filter("id", FilterOperator::GreaterThan, "x")]
    ));
}

#[test]
fn valid_filters_integer_and_number_accept_equals_and_ordering_only() {
    let et = event_type();
    for field in ["amount", "price"] {
        for op in [
            FilterOperator::Equals,
            FilterOperator::GreaterThan,
            FilterOperator::LessThan,
        ] {
            assert!(event_store::valid_filters(&et, &[filter(field, op, "1")]));
        }
        for op in [FilterOperator::Contains, FilterOperator::IsLike] {
            assert!(!event_store::valid_filters(&et, &[filter(field, op, "1")]));
        }
    }
}

#[test]
fn valid_filters_boolean_accepts_equals_only() {
    let et = event_type();
    assert!(event_store::valid_filters(
        &et,
        &[filter("active", FilterOperator::Equals, "true")]
    ));
    assert!(!event_store::valid_filters(
        &et,
        &[filter("active", FilterOperator::Contains, "true")]
    ));
}

#[test]
fn valid_filters_list_of_scalar_accepts_contains_only() {
    let et = event_type();
    assert!(event_store::valid_filters(
        &et,
        &[filter("tags", FilterOperator::Contains, "a")]
    ));
    assert!(!event_store::valid_filters(
        &et,
        &[filter("tags", FilterOperator::Equals, "a")]
    ));
}

#[test]
fn valid_filters_optional_scalar_and_optional_list_are_accepted_like_their_non_optional_counterparts(
) {
    let et = event_type();
    assert!(event_store::valid_filters(
        &et,
        &[filter("optional_name", FilterOperator::Equals, "x")]
    ));
    assert!(event_store::valid_filters(
        &et,
        &[filter("optional_tags", FilterOperator::Contains, "x")]
    ));
}

#[test]
fn valid_filters_a_bare_unit_enum_field_is_accepted_as_a_plain_string() {
    let et = event_type();
    assert!(event_store::valid_filters(
        &et,
        &[filter("status", FilterOperator::Equals, "Active")]
    ));
    assert!(!event_store::valid_filters(
        &et,
        &[filter("status", FilterOperator::GreaterThan, "Active")]
    ));
}

#[test]
fn valid_filters_rejects_a_bare_nested_object_field_directly() {
    let et = event_type();
    assert!(!event_store::valid_filters(
        &et,
        &[filter("address", FilterOperator::Equals, "x")]
    ));
}

#[test]
fn valid_filters_accepts_a_dotted_path_into_a_nested_shape() {
    let et = event_type();
    assert!(event_store::valid_filters(
        &et,
        &[filter("address.country", FilterOperator::Equals, "NL")]
    ));
}

#[test]
fn valid_filters_rejects_a_field_the_schema_does_not_declare() {
    let et = event_type();
    assert!(!event_store::valid_filters(
        &et,
        &[filter("bogus", FilterOperator::Equals, "x")]
    ));
}

#[test]
fn valid_filters_is_true_for_an_empty_filter_list() {
    assert!(event_store::valid_filters(&event_type(), &[]));
}

// ---------------------------------------------------------------------
// valid_tag_mappings / valid_sensitive_fields: the shared Other-rejection
// ---------------------------------------------------------------------

#[test]
fn valid_tag_mappings_rejects_a_bare_nested_object_field() {
    let mappings = vec![TagMapping {
        key: "k".into(),
        field: "address".into(),
    }];
    assert!(!event_store::valid_tag_mappings(SCHEMA, &mappings));
}

#[test]
fn valid_tag_mappings_accepts_a_bare_unit_enum_field() {
    let mappings = vec![TagMapping {
        key: "k".into(),
        field: "status".into(),
    }];
    assert!(event_store::valid_tag_mappings(SCHEMA, &mappings));
}

#[test]
fn valid_tag_mappings_accepts_a_dotted_path_into_a_nested_shape() {
    let mappings = vec![TagMapping {
        key: "k".into(),
        field: "address.country".into(),
    }];
    assert!(event_store::valid_tag_mappings(SCHEMA, &mappings));
}

#[test]
fn valid_sensitive_fields_rejects_a_bare_nested_object_field() {
    let sensitive_fields = vec![SensitiveField {
        field: "address".into(),
        subject_key: "user".into(),
        subject_field: "name".into(),
    }];
    assert!(!event_store::valid_sensitive_fields(
        SCHEMA,
        &sensitive_fields
    ));
}

#[test]
fn valid_sensitive_fields_accepts_a_bare_unit_enum_subject_field() {
    let sensitive_fields = vec![SensitiveField {
        field: "name".into(),
        subject_key: "user".into(),
        subject_field: "status".into(),
    }];
    assert!(event_store::valid_sensitive_fields(
        SCHEMA,
        &sensitive_fields
    ));
}

// ---------------------------------------------------------------------
// matches_filters
// ---------------------------------------------------------------------

fn event(payload: &str) -> Event {
    Event {
        bounded_context: bounded_context(),
        event_type: event_type(),
        payload: payload.into(),
        metadata: Metadata {
            r#type: "OrderPlaced".into(),
            version: 1,
            client_id: "test-client".into(),
            created_at: timestamp(0),
        },
        sequence: 0,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

const PAYLOAD: &str = r#"{
    "name": "Alice",
    "when": "2027-01-15T08:00:00Z",
    "day": "2027-01-15",
    "clock": "08:00:00",
    "id": "5b1f6e2a-0000-4000-8000-000000000000",
    "amount": 42,
    "price": 9.99,
    "active": true,
    "tags": ["a", "b"],
    "status": "Active",
    "address": {"country": "NL"}
}"#;

#[test]
fn matches_filters_is_true_for_an_empty_filter_list() {
    assert!(event_store::matches_filters(&event(PAYLOAD), &[]));
}

#[test]
fn matches_filters_string_equals_contains_is_like() {
    let e = event(PAYLOAD);
    assert!(event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::Equals, "Alice")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::Equals, "Bob")]
    ));
    assert!(event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::Contains, "lic")]
    ));
    assert!(event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::IsLike, "A%e")]
    ));
    assert!(event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::IsLike, "A_ice")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::IsLike, "B%")]
    ));
}

#[test]
fn matches_filters_number_equals_greater_than_less_than() {
    let e = event(PAYLOAD);
    assert!(event_store::matches_filters(
        &e,
        &[filter("amount", FilterOperator::Equals, "42")]
    ));
    assert!(event_store::matches_filters(
        &e,
        &[filter("amount", FilterOperator::GreaterThan, "10")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("amount", FilterOperator::LessThan, "10")]
    ));
    assert!(event_store::matches_filters(
        &e,
        &[filter("price", FilterOperator::GreaterThan, "9")]
    ));
}

#[test]
fn matches_filters_boolean_equals() {
    let e = event(PAYLOAD);
    assert!(event_store::matches_filters(
        &e,
        &[filter("active", FilterOperator::Equals, "true")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("active", FilterOperator::Equals, "false")]
    ));
}

#[test]
fn matches_filters_list_membership() {
    let e = event(PAYLOAD);
    assert!(event_store::matches_filters(
        &e,
        &[filter("tags", FilterOperator::Contains, "a")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("tags", FilterOperator::Contains, "z")]
    ));
}

#[test]
fn matches_filters_dotted_path() {
    let e = event(PAYLOAD);
    assert!(event_store::matches_filters(
        &e,
        &[filter("address.country", FilterOperator::Equals, "NL")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("address.country", FilterOperator::Equals, "BE")]
    ));
}

#[test]
fn matches_filters_absent_field_never_matches() {
    let e = event(r#"{"name":"Alice"}"#);
    assert!(!event_store::matches_filters(
        &e,
        &[filter("amount", FilterOperator::GreaterThan, "0")]
    ));
}

#[test]
fn matches_filters_every_filter_must_match_and_semantics() {
    let e = event(PAYLOAD);
    let both_match = vec![
        filter("name", FilterOperator::Equals, "Alice"),
        filter("amount", FilterOperator::GreaterThan, "10"),
    ];
    assert!(event_store::matches_filters(&e, &both_match));

    let one_fails = vec![
        filter("name", FilterOperator::Equals, "Alice"),
        filter("amount", FilterOperator::GreaterThan, "1000"),
    ];
    assert!(!event_store::matches_filters(&e, &one_fails));
}

// --- string_ordering, including the exact chrono trimming bug found ---

#[test]
fn matches_filters_date_time_ordering_uses_real_chrono_parsing_not_string_ord() {
    // "when" is exactly on the second (no fractional part, per chrono's
    // own trimming) - a naive lexicographic string comparison against a
    // value with a `.500` fraction gets this backwards ('Z' > '.'), the
    // real bug this design was corrected for. The chronologically correct
    // answer is: 08:00:00 (no fraction) IS earlier than 08:00:00.500.
    let e = event(PAYLOAD); // when = "2027-01-15T08:00:00Z"
    assert!(event_store::matches_filters(
        &e,
        &[filter(
            "when",
            FilterOperator::LessThan,
            "2027-01-15T08:00:00.500Z"
        )]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter(
            "when",
            FilterOperator::GreaterThan,
            "2027-01-15T08:00:00.500Z"
        )]
    ));
}

#[test]
fn matches_filters_date_ordering() {
    let e = event(PAYLOAD); // day = "2027-01-15"
    assert!(event_store::matches_filters(
        &e,
        &[filter("day", FilterOperator::GreaterThan, "2027-01-01")]
    ));
    assert!(event_store::matches_filters(
        &e,
        &[filter("day", FilterOperator::LessThan, "2027-02-01")]
    ));
}

#[test]
fn matches_filters_partial_date_time_ordering() {
    let e = event(PAYLOAD); // clock = "08:00:00"
    assert!(event_store::matches_filters(
        &e,
        &[filter("clock", FilterOperator::GreaterThan, "07:00:00")]
    ));
    assert!(event_store::matches_filters(
        &e,
        &[filter("clock", FilterOperator::LessThan, "09:00:00")]
    ));
}

#[test]
fn matches_filters_ordering_falls_through_to_false_on_a_malformed_date() {
    let e = event(PAYLOAD);
    assert!(!event_store::matches_filters(
        &e,
        &[filter("when", FilterOperator::GreaterThan, "not-a-date")]
    ));
}
