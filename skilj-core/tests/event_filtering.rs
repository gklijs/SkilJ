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
        template: None,
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
        "address": {"$ref": "#/definitions/Address"},
        "geo": {"type": "string", "format": "geo-point"},
        "color": {"type": "string", "format": "color"},
        "ip": {"type": "string", "format": "ip"}
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
fn valid_filters_geo_point_format_additionally_accepts_near() {
    let et = event_type();
    assert!(event_store::valid_filters(
        &et,
        &[filter("geo", FilterOperator::Near, "52.37,4.90,5000")]
    ));
    assert!(event_store::valid_filters(
        &et,
        &[filter("geo", FilterOperator::Equals, "52.37,4.90")]
    ));
    // Near is format-gated - a plain string field with no geo-point
    // format must not accept it.
    assert!(!event_store::valid_filters(
        &et,
        &[filter("name", FilterOperator::Near, "52.37,4.90,5000")]
    ));
    assert!(!event_store::valid_filters(
        &et,
        &[filter("geo", FilterOperator::GreaterThan, "52.37,4.90")]
    ));
}

#[test]
fn valid_filters_color_format_additionally_accepts_similar_color() {
    let et = event_type();
    assert!(event_store::valid_filters(
        &et,
        &[filter("color", FilterOperator::SimilarColor, "#FF0000,30")]
    ));
    assert!(!event_store::valid_filters(
        &et,
        &[filter("name", FilterOperator::SimilarColor, "#FF0000,30")]
    ));
}

#[test]
fn valid_filters_ip_format_additionally_accepts_in_subnet() {
    let et = event_type();
    assert!(event_store::valid_filters(
        &et,
        &[filter("ip", FilterOperator::InSubnet, "192.168.1.0/24")]
    ));
    assert!(!event_store::valid_filters(
        &et,
        &[filter("name", FilterOperator::InSubnet, "192.168.1.0/24")]
    ));
}

#[test]
fn valid_filters_in_is_accepted_for_any_scalar_kind() {
    let et = event_type();
    for field in ["name", "amount", "price", "active", "status"] {
        assert!(
            event_store::valid_filters(&et, &[filter(field, FilterOperator::In, "a,b")]),
            "{field} should accept In"
        );
    }
    // Not for a list-of-scalar or nested-object leaf - same "only ever a
    // scalar leaf" cap the rest of this matrix already enforces.
    assert!(!event_store::valid_filters(
        &et,
        &[filter("tags", FilterOperator::In, "a,b")]
    ));
    assert!(!event_store::valid_filters(
        &et,
        &[filter("address", FilterOperator::In, "a,b")]
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
            correlation_id: None,
            causation_id: None,
        },
        sequence: 0,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

const PAYLOAD: &str = r##"{
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
    "address": {"country": "NL"},
    "geo": "52.3676,4.9041",
    "color": "#FF0000",
    "ip": "192.168.1.42"
}"##;

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
fn matches_filters_near_geo_distance() {
    let e = event(PAYLOAD);
    // Same point as the payload's own "geo" (52.3676,4.9041) - distance
    // 0, well within a 100m radius.
    assert!(event_store::matches_filters(
        &e,
        &[filter("geo", FilterOperator::Near, "52.3676,4.9041,100")]
    ));
    // New York - thousands of km away, well outside a 5km radius.
    assert!(!event_store::matches_filters(
        &e,
        &[filter("geo", FilterOperator::Near, "40.7128,-74.0060,5000")]
    ));
    // Malformed filter value never matches, doesn't panic.
    assert!(!event_store::matches_filters(
        &e,
        &[filter("geo", FilterOperator::Near, "not-a-point")]
    ));
}

#[test]
fn matches_filters_similar_color_distance() {
    let e = event(PAYLOAD);
    // Payload's own "color" is "#FF0000" (red) - identical color, distance 0.
    assert!(event_store::matches_filters(
        &e,
        &[filter("color", FilterOperator::SimilarColor, "#FF0000,10")]
    ));
    // Blue is maximally far from red on this metric - well outside a
    // threshold of 10.
    assert!(!event_store::matches_filters(
        &e,
        &[filter("color", FilterOperator::SimilarColor, "#0000FF,10")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("color", FilterOperator::SimilarColor, "not-a-color")]
    ));
}

#[test]
fn matches_filters_in_subnet() {
    let e = event(PAYLOAD);
    // Payload's own "ip" is "192.168.1.42".
    assert!(event_store::matches_filters(
        &e,
        &[filter("ip", FilterOperator::InSubnet, "192.168.1.0/24")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("ip", FilterOperator::InSubnet, "10.0.0.0/8")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("ip", FilterOperator::InSubnet, "not-a-cidr")]
    ));
}

#[test]
fn matches_filters_in_one_of_several_values() {
    let e = event(PAYLOAD);
    assert!(event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::In, "Alice,Bob")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::In, "Bob,Carol")]
    ));
    assert!(event_store::matches_filters(
        &e,
        &[filter("amount", FilterOperator::In, "42,43")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("amount", FilterOperator::In, "1,2")]
    ));
    assert!(event_store::matches_filters(
        &e,
        &[filter("active", FilterOperator::In, "true,maybe")]
    ));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("active", FilterOperator::In, "false")]
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

/// An `IS_LIKE` pattern costs pattern length x string length to match,
/// for every event a read examines, so `valid_filters` caps it at
/// `MAX_LIKE_PATTERN_CHARS` (docs/architecture.md §121).
#[test]
fn valid_filters_rejects_an_is_like_pattern_over_the_cap() {
    let at_cap = "%".repeat(event_store::MAX_LIKE_PATTERN_CHARS);
    let over_cap = "%".repeat(event_store::MAX_LIKE_PATTERN_CHARS + 1);
    assert!(event_store::valid_filters(
        &event_type(),
        &[filter("name", FilterOperator::IsLike, &at_cap)]
    ));
    assert!(!event_store::valid_filters(
        &event_type(),
        &[filter("name", FilterOperator::IsLike, &over_cap)]
    ));
    // Only IS_LIKE is capped - Contains is a linear substring search.
    assert!(event_store::valid_filters(
        &event_type(),
        &[filter("name", FilterOperator::Contains, &over_cap)]
    ));
}

/// A pattern at the cap against a 2-million-character string. The old
/// full-table DP allocated about 2 GB for this, per event, and a plain
/// one-row DP took over a minute in a debug build; the bit-parallel one
/// takes a fraction of a second. Wildcards, `_` and literals still match
/// as before.
#[test]
fn is_like_matches_a_long_string_in_linear_memory() {
    let long = format!("A{}e", "x".repeat(2_000_000));
    let e = event(&serde_json::json!({ "name": long }).to_string());
    let pattern = format!("A{}%e", "_".repeat(event_store::MAX_LIKE_PATTERN_CHARS - 3));
    assert!(event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::IsLike, &pattern)]
    ));
    let pattern = format!("A{}%f", "_".repeat(event_store::MAX_LIKE_PATTERN_CHARS - 3));
    assert!(!event_store::matches_filters(
        &e,
        &[filter("name", FilterOperator::IsLike, &pattern)]
    ));
    for (pattern, expected) in [
        ("%", true),
        ("%%", true),
        ("A%", true),
        ("%e", true),
        ("%x%", true),
        ("B%", false),
        ("A_e", false),
    ] {
        assert_eq!(
            event_store::matches_filters(&e, &[filter("name", FilterOperator::IsLike, pattern)]),
            expected,
            "{pattern}"
        );
    }
}

/// The bit-parallel matcher against the textbook full-table DP, over
/// every pattern and text of a small alphabet up to a few characters,
/// multi-byte characters and patterns wider than one 64-bit word
/// included.
#[test]
fn is_like_agrees_with_the_reference_dp() {
    fn reference(text: &str, pattern: &str) -> bool {
        let (t, p): (Vec<char>, Vec<char>) = (text.chars().collect(), pattern.chars().collect());
        let mut dp = vec![vec![false; p.len() + 1]; t.len() + 1];
        dp[0][0] = true;
        for j in 1..=p.len() {
            dp[0][j] = dp[0][j - 1] && p[j - 1] == '%';
        }
        for i in 1..=t.len() {
            for j in 1..=p.len() {
                dp[i][j] = match p[j - 1] {
                    '%' => dp[i - 1][j] || dp[i][j - 1],
                    '_' => dp[i - 1][j - 1],
                    c => dp[i - 1][j - 1] && t[i - 1] == c,
                };
            }
        }
        dp[t.len()][p.len()]
    }
    fn strings(alphabet: &[char], max_len: usize) -> Vec<String> {
        let mut all = vec![String::new()];
        let mut frontier = vec![String::new()];
        for _ in 0..max_len {
            frontier = frontier
                .iter()
                .flat_map(|s| alphabet.iter().map(move |c| format!("{s}{c}")))
                .collect();
            all.extend(frontier.iter().cloned());
        }
        all
    }
    let check = |text: &str, pattern: &str| {
        let e = event(&serde_json::json!({ "name": text }).to_string());
        assert_eq!(
            event_store::matches_filters(&e, &[filter("name", FilterOperator::IsLike, pattern)]),
            reference(text, pattern),
            "text {text:?} pattern {pattern:?}"
        );
    };
    let texts = strings(&['a', 'b', 'é'], 5);
    for pattern in strings(&['a', 'é', '%', '_'], 4) {
        for text in &texts {
            check(text, &pattern);
        }
    }
    // Across the 64-bit word boundary.
    for n in [62, 63, 64, 65, 127, 128, 129] {
        let text = "ab".repeat(n);
        for pattern in [
            "ab".repeat(n),
            format!("{}%", "_".repeat(n)),
            format!("%{}", "b_".repeat(n / 2)),
            format!("{}%b", "a%".repeat(n)),
            "_".repeat(2 * n + 1),
        ] {
            check(&text, &pattern);
        }
    }
}

/// Every filter runs against every event a read examines - for a
/// subscription, every event committed while it lives - so how many a
/// request may carry, and how long each value may be, are bounded
/// (docs/architecture.md §122).
#[test]
fn valid_filters_bounds_the_filter_count_and_value_length() {
    let many = |n: usize| vec![filter("name", FilterOperator::Equals, "Alice"); n];
    assert!(event_store::valid_filters(
        &event_type(),
        &many(event_store::MAX_FILTERS)
    ));
    assert!(!event_store::valid_filters(
        &event_type(),
        &many(event_store::MAX_FILTERS + 1)
    ));

    let value = |n: usize| "x".repeat(n);
    for operator in [FilterOperator::Equals, FilterOperator::Contains] {
        assert!(event_store::valid_filters(
            &event_type(),
            &[filter(
                "name",
                operator,
                &value(event_store::MAX_FILTER_VALUE_CHARS)
            )]
        ));
        assert!(!event_store::valid_filters(
            &event_type(),
            &[filter(
                "name",
                operator,
                &value(event_store::MAX_FILTER_VALUE_CHARS + 1)
            )]
        ));
    }
}

/// Each tag in `queryEvents`/`countEvents` becomes its own condition in
/// the tag-index query, so at most `MAX_QUERY_TAGS` are accepted
/// (docs/architecture.md §122).
#[test]
fn valid_query_tags_bounds_the_tag_count() {
    let tags = |n: usize| {
        (0..n)
            .map(|i| skilj_core::shared::Tag {
                key: "k".into(),
                value: Some(i.to_string()),
            })
            .collect::<Vec<_>>()
    };
    assert!(event_store::valid_query_tags(None).is_ok());
    assert!(event_store::valid_query_tags(Some(&tags(event_store::MAX_QUERY_TAGS))).is_ok());
    let err =
        event_store::valid_query_tags(Some(&tags(event_store::MAX_QUERY_TAGS + 1))).unwrap_err();
    assert!(err.to_string().contains("at most 32 tags"), "{err}");
}
