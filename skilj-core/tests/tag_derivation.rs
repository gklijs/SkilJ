//! Tests for `event_store::derive_tags` and the shared dotted-path
//! primitives it shares with `valid_tag_mappings`/`valid_sensitive_fields`/
//! `protect_sensitive_fields` (`resolve_field`/`payload_field_value`/
//! `payload_field_value_mut` - see the plan at
//! `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`). Pure logic
//! only, no DB.
//!
//! `derive_tags`'s own contract, per its doc comment: a present scalar (or
//! explicit `null`) becomes one `Tag(key, value)`; an absent field becomes
//! one `Tag(key, value: null)` - the identical "None" state as an explicit
//! `null`; a non-empty list of scalars becomes one `Tag(key, value: e)`
//! per *distinct* element (Set<Tag> semantics - duplicates collapse); an
//! empty list, or a list-typed field absent altogether, is the same
//! single "None" state as a missing scalar, not a third state; a dotted
//! path resolves to a leaf via the identical rules, with no fifth case.

use skilj_core::encryption::{self, EncryptionMasterKey};
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, EncryptionKey, EncryptionKeyStatus,
};
use skilj_core::shared::{SensitiveField, Tag, TagMapping};

fn tag(key: &str, value: Option<&str>) -> Tag {
    Tag {
        key: key.into(),
        value: value.map(String::from),
    }
}

/// A schema with one named nested shape (`Address`), reached via `$ref`
/// from the top-level `address` property - the one level of nesting the
/// payload schema shape note above `entity CommandType` permits, and the
/// shape every dotted-path test below resolves a field through.
const NESTED_SCHEMA: &str = r##"{
    "properties": {
        "email": {"type": "string"},
        "user_id": {"type": "string"},
        "address": {"$ref": "#/definitions/Address"}
    },
    "definitions": {
        "Address": {
            "properties": {
                "country": {"type": "string"}
            }
        }
    }
}"##;

// --- derive_tags: bare-field cases ---

#[test]
fn derive_tags_is_empty_when_tag_mappings_is_empty() {
    assert!(event_store::derive_tags(&[], r#"{"account_id":"A"}"#).is_empty());
}

#[test]
fn derive_tags_produces_one_tag_for_a_present_scalar() {
    let mappings = vec![TagMapping {
        key: "account".into(),
        field: "account_id".into(),
    }];
    let tags = event_store::derive_tags(&mappings, r#"{"account_id":"A"}"#);
    assert_eq!(tags, vec![tag("account", Some("A"))]);
}

#[test]
fn derive_tags_produces_a_null_tag_for_an_explicit_null() {
    let mappings = vec![TagMapping {
        key: "account".into(),
        field: "account_id".into(),
    }];
    let tags = event_store::derive_tags(&mappings, r#"{"account_id":null}"#);
    assert_eq!(tags, vec![tag("account", None)]);
}

#[test]
fn derive_tags_produces_a_null_tag_for_an_absent_field() {
    let mappings = vec![TagMapping {
        key: "account".into(),
        field: "account_id".into(),
    }];
    let tags = event_store::derive_tags(&mappings, r#"{}"#);
    assert_eq!(tags, vec![tag("account", None)]);
}

#[test]
fn derive_tags_produces_one_tag_per_distinct_list_element() {
    let mappings = vec![TagMapping {
        key: "participant".into(),
        field: "participants".into(),
    }];
    let tags = event_store::derive_tags(&mappings, r#"{"participants":["alice","bob","alice"]}"#);
    // Duplicate "alice" collapses - Set<Tag> semantics.
    assert_eq!(
        tags,
        vec![
            tag("participant", Some("alice")),
            tag("participant", Some("bob"))
        ]
    );
}

#[test]
fn derive_tags_produces_a_null_tag_for_an_empty_list() {
    let mappings = vec![TagMapping {
        key: "participant".into(),
        field: "participants".into(),
    }];
    let tags = event_store::derive_tags(&mappings, r#"{"participants":[]}"#);
    assert_eq!(tags, vec![tag("participant", None)]);
}

#[test]
fn derive_tags_produces_a_null_tag_for_an_absent_list_typed_field() {
    let mappings = vec![TagMapping {
        key: "participant".into(),
        field: "participants".into(),
    }];
    let tags = event_store::derive_tags(&mappings, r#"{}"#);
    assert_eq!(tags, vec![tag("participant", None)]);
}

#[test]
fn derive_tags_skips_non_scalar_list_elements_rather_than_faking_absent() {
    let mappings = vec![TagMapping {
        key: "participant".into(),
        field: "participants".into(),
    }];
    // One scalar, one nested object - the object is silently skipped, not
    // turned into a spurious "absent" tag alongside the real one.
    let tags =
        event_store::derive_tags(&mappings, r#"{"participants":["alice", {"nested":true}]}"#);
    assert_eq!(tags, vec![tag("participant", Some("alice"))]);
}

#[test]
fn derive_tags_covers_every_mapping_independently() {
    let mappings = vec![
        TagMapping {
            key: "account".into(),
            field: "account_id".into(),
        },
        TagMapping {
            key: "kind".into(),
            field: "kind".into(),
        },
    ];
    let tags = event_store::derive_tags(&mappings, r#"{"account_id":"A"}"#);
    assert_eq!(tags, vec![tag("account", Some("A")), tag("kind", None)]);
}

// --- derive_tags: dotted-path cases ---

#[test]
fn derive_tags_resolves_a_dotted_path_present_scalar() {
    let mappings = vec![TagMapping {
        key: "country".into(),
        field: "address.country".into(),
    }];
    let tags = event_store::derive_tags(&mappings, r#"{"address":{"country":"NL"}}"#);
    assert_eq!(tags, vec![tag("country", Some("NL"))]);
}

#[test]
fn derive_tags_resolves_a_dotted_path_absent_outer_object() {
    let mappings = vec![TagMapping {
        key: "country".into(),
        field: "address.country".into(),
    }];
    let tags = event_store::derive_tags(&mappings, r#"{}"#);
    assert_eq!(tags, vec![tag("country", None)]);
}

#[test]
fn derive_tags_resolves_a_dotted_path_absent_leaf() {
    let mappings = vec![TagMapping {
        key: "country".into(),
        field: "address.country".into(),
    }];
    let tags = event_store::derive_tags(&mappings, r#"{"address":{}}"#);
    assert_eq!(tags, vec![tag("country", None)]);
}

// --- valid_tag_mappings / valid_sensitive_fields: dotted-path field ---

#[test]
fn valid_tag_mappings_accepts_a_real_dotted_path_field() {
    let mappings = vec![TagMapping {
        key: "country".into(),
        field: "address.country".into(),
    }];
    assert!(event_store::valid_tag_mappings(NESTED_SCHEMA, &mappings));
}

#[test]
fn valid_tag_mappings_rejects_a_dotted_path_into_an_unknown_definition() {
    let mappings = vec![TagMapping {
        key: "country".into(),
        field: "address.province".into(),
    }];
    assert!(!event_store::valid_tag_mappings(NESTED_SCHEMA, &mappings));
}

#[test]
fn valid_sensitive_fields_accepts_a_real_dotted_path_field() {
    let sensitive_fields = vec![SensitiveField {
        field: "address.country".into(),
        subject_key: "user".into(),
        subject_field: "user_id".into(),
    }];
    assert!(event_store::valid_sensitive_fields(
        NESTED_SCHEMA,
        &sensitive_fields
    ));
}

// --- protect_sensitive_fields: dotted-path leaf, real encryption ---

fn master_key(seed: u8) -> EncryptionMasterKey {
    EncryptionMasterKey::from_bytes([seed; 32])
}

fn bounded_context() -> BoundedContext {
    BoundedContext {
        name: "customers".into(),
        status: BoundedContextStatus::Active,
        created_at: chrono::Utc::now(),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
    }
}

#[test]
fn protect_sensitive_fields_encrypts_a_dotted_path_leaf() {
    let master = master_key(9);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let key = EncryptionKey {
        bounded_context: bounded_context(),
        subject_key: "user".into(),
        subject_value: "42".into(),
        status: EncryptionKeyStatus::Active,
        created_at: chrono::Utc::now(),
        destroyed_at: None,
    };

    let sensitive_fields = vec![SensitiveField {
        field: "address.country".into(),
        subject_key: "user".into(),
        subject_field: "user_id".into(),
    }];
    let payload = r#"{"address":{"country":"NL"},"user_id":"42"}"#;

    let protected = event_store::protect_sensitive_fields(&sensitive_fields, payload, |sk, sv| {
        assert_eq!(sk, "user");
        assert_eq!(sv, "42");
        (key.clone(), data_key.clone())
    });

    assert_eq!(protected.encryption_keys, vec![key]);
    let parsed: serde_json::Value = serde_json::from_str(&protected.payload).unwrap();
    assert_eq!(parsed["user_id"], "42");
    let ciphertext = parsed["address"]["country"].as_str().unwrap();
    assert_ne!(ciphertext, "NL");
    assert_eq!(
        encryption::decrypt_leaf(&data_key, ciphertext).unwrap(),
        "NL"
    );
}
