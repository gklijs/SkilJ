//! Tests for `skilj_core::encryption` and the newly-real branches of
//! `event_store::protect_sensitive_fields`/`sensitive_field_subjects`/
//! `render_event`/`render_command` (§SubjectErasure - see the plan at
//! `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`). Pure logic
//! only, no DB - `skilj-core/tests/persistence.rs` covers the
//! `encryption_keys` table round-trip, `skilj/tests/subject_erasure.rs`
//! the real end-to-end GraphQL path.

use chrono::Utc;
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::encryption::{self, EncryptionMasterKey};
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, Command, CommandType, EncryptionKey,
    EncryptionKeyStatus, Event, EventOrigin, EventType,
};
use skilj_core::shared::{Metadata, SensitiveField};

fn master_key(seed: u8) -> EncryptionMasterKey {
    EncryptionMasterKey::from_bytes([seed; 32])
}

fn bounded_context() -> BoundedContext {
    BoundedContext {
        name: "banking".into(),
        status: BoundedContextStatus::Active,
        created_at: Utc::now(),
        created_by: ContextCreator::SystemCreator,
    }
}

// --- skilj_core::encryption, in isolation ---

#[test]
fn generate_and_wrap_then_unwrap_recovers_the_same_data_key() {
    let master = master_key(1);
    let (data_key, wrapped, nonce) = encryption::generate_and_wrap_data_key(&master);
    let unwrapped = encryption::unwrap_data_key(&master, &wrapped, &nonce).unwrap();

    // `DataKey` has no `PartialEq`/`Debug` by design - proven equal
    // indirectly, by encrypting under one and decrypting under the other.
    let ciphertext = encryption::encrypt_leaf(&data_key, "hello");
    let plaintext = encryption::decrypt_leaf(&unwrapped, &ciphertext).unwrap();
    assert_eq!(plaintext, "hello");
}

#[test]
fn unwrap_data_key_fails_under_the_wrong_master_key() {
    let master = master_key(1);
    let other = master_key(2);
    let (_data_key, wrapped, nonce) = encryption::generate_and_wrap_data_key(&master);

    // `DataKey` deliberately has no `Debug` impl, so `Result::unwrap_err`
    // (which requires the `Ok` side to be `Debug`) can't be used here.
    match encryption::unwrap_data_key(&other, &wrapped, &nonce) {
        Err(e) => assert_eq!(e.to_string(), encryption::Error::DecryptFailed.to_string()),
        Ok(_) => panic!("unwrapping under the wrong master key must fail"),
    }
}

#[test]
fn encrypt_leaf_round_trips_through_decrypt_leaf() {
    let master = master_key(3);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);

    let ciphertext = encryption::encrypt_leaf(&data_key, "secret-value");
    assert_ne!(ciphertext, "secret-value");
    let plaintext = encryption::decrypt_leaf(&data_key, &ciphertext).unwrap();
    assert_eq!(plaintext, "secret-value");
}

#[test]
fn encrypt_leaf_never_produces_the_same_ciphertext_twice() {
    // A fresh random nonce every call - the same plaintext under the same
    // key must still differ ciphertext-to-ciphertext.
    let master = master_key(4);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let a = encryption::encrypt_leaf(&data_key, "same-value");
    let b = encryption::encrypt_leaf(&data_key, "same-value");
    assert_ne!(a, b);
}

#[test]
fn decrypt_leaf_fails_on_tampered_ciphertext() {
    let master = master_key(5);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let mut ciphertext = encryption::encrypt_leaf(&data_key, "secret");
    ciphertext.push('A'); // corrupts the base64/AEAD tag
    assert!(encryption::decrypt_leaf(&data_key, &ciphertext).is_err());
}

// --- event_store::sensitive_field_subjects ---

#[test]
fn sensitive_field_subjects_finds_every_distinct_subject_once() {
    let sensitive_fields = vec![
        SensitiveField {
            field: "email".into(),
            subject_key: "user".into(),
            subject_field: "user_id".into(),
        },
        SensitiveField {
            field: "ssn".into(),
            subject_key: "user".into(),
            subject_field: "user_id".into(),
        },
    ];
    let payload = r#"{"email":"a@b.com","ssn":"123-45-6789","user_id":"42"}"#;

    let subjects = event_store::sensitive_field_subjects(&sensitive_fields, payload);
    assert_eq!(subjects, vec![("user".to_string(), "42".to_string())]);
}

#[test]
fn sensitive_field_subjects_is_empty_when_the_subject_field_is_absent() {
    let sensitive_fields = vec![SensitiveField {
        field: "email".into(),
        subject_key: "user".into(),
        subject_field: "user_id".into(),
    }];
    let payload = r#"{"email":"a@b.com"}"#;

    assert!(event_store::sensitive_field_subjects(&sensitive_fields, payload).is_empty());
}

// --- event_store::protect_sensitive_fields, the real non-empty branch ---

#[test]
fn protect_sensitive_fields_encrypts_the_declared_leaf_and_leaves_the_rest_untouched() {
    let master = master_key(6);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let key = EncryptionKey {
        bounded_context: bounded_context(),
        subject_key: "user".into(),
        subject_value: "42".into(),
        status: EncryptionKeyStatus::Active,
        created_at: Utc::now(),
        destroyed_at: None,
    };

    let sensitive_fields = vec![SensitiveField {
        field: "email".into(),
        subject_key: "user".into(),
        subject_field: "user_id".into(),
    }];
    let payload = r#"{"email":"person@example.com","user_id":"42","other":"unchanged"}"#;

    let protected = event_store::protect_sensitive_fields(&sensitive_fields, payload, |sk, sv| {
        assert_eq!(sk, "user");
        assert_eq!(sv, "42");
        (key.clone(), data_key.clone())
    });

    assert_eq!(protected.encryption_keys, vec![key]);
    let parsed: serde_json::Value = serde_json::from_str(&protected.payload).unwrap();
    assert_eq!(parsed["other"], "unchanged");
    assert_eq!(parsed["user_id"], "42"); // subject_field itself is read, never encrypted
    let ciphertext = parsed["email"].as_str().unwrap();
    assert_ne!(ciphertext, "person@example.com");
    assert_eq!(
        encryption::decrypt_leaf(&data_key, ciphertext).unwrap(),
        "person@example.com"
    );
}

#[test]
fn protect_sensitive_fields_is_a_no_op_when_sensitive_fields_is_empty() {
    let protected = event_store::protect_sensitive_fields(&[], r#"{"a":1}"#, |_, _| {
        panic!("resolve_key must never be called when sensitive_fields is empty")
    });
    assert_eq!(protected.payload, r#"{"a":1}"#);
    assert!(protected.encryption_keys.is_empty());
}

// --- render_event/render_command: the previously-panicking branch ---

fn event_type_with_sensitive_field() -> EventType {
    EventType {
        bounded_context: bounded_context(),
        name: "AccountOpened".into(),
        schema: r#"{"properties":{"email":{"type":"string"},"user_id":{"type":"string"}}}"#.into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        sensitive_fields: vec![SensitiveField {
            field: "email".into(),
            subject_key: "user".into(),
            subject_field: "user_id".into(),
        }],
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

/// `can_read_sensitive`/`external_subject` are the two-grant test's own
/// two independent inputs - every test below picks them deliberately to
/// land on one specific branch (neither/either/both), rather than one
/// fixed fixture reused everywhere.
fn access_mapping(can_read_sensitive: bool, external_subject: &str) -> RoleAccessMapping {
    RoleAccessMapping {
        role: Role {
            id: "role-1".into(),
            external_subject: external_subject.into(),
            name: "Reader".into(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: Utc::now(),
            revoked_at: None,
        },
        bounded_context: bounded_context(),
        level: AccessLevel::Read,
        can_read_sensitive,
        status: RoleStatus::Active,
        created_at: Utc::now(),
        revoked_at: None,
    }
}

fn event_with_payload(payload: &str) -> Event {
    Event {
        bounded_context: bounded_context(),
        event_type: event_type_with_sensitive_field(),
        payload: payload.into(),
        metadata: Metadata {
            r#type: "AccountOpened".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: Utc::now(),
        },
        sequence: 0,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

/// `subject_field` (`user_id`) is always `"42"` throughout this file's
/// fixtures - real ciphertext for the declared `email` sensitive field,
/// produced via the real `protect_sensitive_fields`, not a fake string -
/// so decrypting it back is a genuine round trip, not just "did the
/// function run."
fn event_with_real_ciphertext(data_key: &encryption::DataKey) -> Event {
    let ciphertext = encryption::encrypt_leaf(data_key, "person@example.com");
    event_with_payload(&format!(r#"{{"email":"{ciphertext}","user_id":"42"}}"#))
}

fn command_type_with_sensitive_field() -> CommandType {
    CommandType {
        bounded_context: bounded_context(),
        name: "OpenAccount".into(),
        schema: r#"{"properties":{"email":{"type":"string"},"user_id":{"type":"string"}}}"#.into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        sensitive_fields: vec![SensitiveField {
            field: "email".into(),
            subject_key: "user".into(),
            subject_field: "user_id".into(),
        }],
        rest_trigger_allowed: false,
    }
}

fn command_with_real_ciphertext(data_key: &encryption::DataKey) -> Command {
    let ciphertext = encryption::encrypt_leaf(data_key, "person@example.com");
    Command {
        id: "cmd-1".into(),
        bounded_context: bounded_context(),
        command_type: command_type_with_sensitive_field(),
        payload: format!(r#"{{"email":"{ciphertext}","user_id":"42"}}"#),
        metadata: Metadata {
            r#type: "OpenAccount".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: Utc::now(),
        },
        encryption_keys: Vec::new(),
        consistency_tags: Vec::new(),
        consistency_boundary: None,
    }
}

// --- event_store::sensitive_field_is_granted ---

#[test]
fn sensitive_field_is_granted_via_can_read_sensitive() {
    let mapping = access_mapping(true, "unrelated-subject");
    assert!(event_store::sensitive_field_is_granted(&mapping, "42"));
}

#[test]
fn sensitive_field_is_granted_via_matching_external_subject() {
    let mapping = access_mapping(false, "42");
    assert!(event_store::sensitive_field_is_granted(&mapping, "42"));
}

#[test]
fn sensitive_field_is_not_granted_with_neither() {
    let mapping = access_mapping(false, "someone-else");
    assert!(!event_store::sensitive_field_is_granted(&mapping, "42"));
}

// --- render_event/render_command: the previously-panicking branch, and
// now the real decrypt branch ---

/// The exact condition that used to panic: `render_event` called against
/// an `Event` whose type has a real, non-empty `sensitive_fields` - now
/// that `protect_sensitive_fields` actually produces one, this is
/// reachable in practice, not just in principle. Neither grant applies
/// here, so `resolve_data_key` must never be called - "left as stored
/// ciphertext, never decrypted and then redacted afterwards" per the
/// spec's own repeated wording.
#[test]
fn render_event_leaves_ciphertext_untouched_with_neither_grant() {
    let event = event_with_payload(r#"{"email":"ZmFrZS1jaXBoZXJ0ZXh0","user_id":"42"}"#);
    let mapping = access_mapping(false, "someone-else");

    let rendered = event_store::render_event(&event, &mapping, &|_, _| unreachable!());
    assert_eq!(rendered, event.payload);
}

/// Grant (a): `can_read_sensitive = true` decrypts, regardless of who the
/// caller is.
#[test]
fn render_event_decrypts_under_can_read_sensitive() {
    let master = master_key(7);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let event = event_with_real_ciphertext(&data_key);
    let mapping = access_mapping(true, "not-the-subject");

    let rendered = event_store::render_event(&event, &mapping, &|sk, sv| {
        assert_eq!((sk, sv), ("user", "42"));
        Some(data_key.clone())
    });

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed["email"], "person@example.com");
    assert_eq!(parsed["user_id"], "42");
}

/// Grant (b): the caller's own verified identity matching the field's
/// subject decrypts, with no `can_read_sensitive` at all - "a caller
/// reading their own data needs no separate grant."
#[test]
fn render_event_decrypts_under_matching_external_subject() {
    let master = master_key(8);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let event = event_with_real_ciphertext(&data_key);
    let mapping = access_mapping(false, "42");

    let rendered = event_store::render_event(&event, &mapping, &|_, _| Some(data_key.clone()));

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed["email"], "person@example.com");
}

/// A granted caller whose `resolve_data_key` returns `None` (no active
/// key - destroyed by `ForgetSubject`, or never provisioned) still sees
/// ciphertext, not a panic or an error - correct crypto-shredding
/// behaviour, no special-casing needed.
#[test]
fn render_event_leaves_ciphertext_when_granted_but_no_active_key() {
    let master = master_key(9);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let event = event_with_real_ciphertext(&data_key);
    let mapping = access_mapping(true, "not-the-subject");

    let rendered = event_store::render_event(&event, &mapping, &|_, _| None);
    assert_eq!(rendered, event.payload);
}

/// A granted caller against a leaf that was never actually ciphertext -
/// the spec's own acknowledged "empty `encryption_keys` for a type that
/// now declares `sensitive_fields`" historical row. `decrypt_leaf` fails
/// (not valid base64/AEAD), and that failure is left untouched, not a
/// panic - one fallback covers both this and the destroyed-key case
/// above with no separate flag.
#[test]
fn render_event_leaves_a_historical_plaintext_leaf_untouched_even_when_granted() {
    let master = master_key(10);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let event = event_with_payload(r#"{"email":"person@example.com","user_id":"42"}"#);
    let mapping = access_mapping(true, "not-the-subject");

    let rendered = event_store::render_event(&event, &mapping, &|_, _| Some(data_key.clone()));
    assert_eq!(rendered, event.payload);
}

/// `render_command` shares the identical decrypt logic - one positive
/// test confirming the wiring on the `Command` side too, not just
/// `Event`.
#[test]
fn render_command_decrypts_under_can_read_sensitive() {
    let master = master_key(11);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let command = command_with_real_ciphertext(&data_key);
    let mapping = access_mapping(true, "not-the-subject");

    let rendered = event_store::render_command(&command, &mapping, &|_, _| Some(data_key.clone()));

    let parsed: serde_json::Value = serde_json::from_str(&rendered).unwrap();
    assert_eq!(parsed["email"], "person@example.com");
}

// --- encryption::decrypt_ciphertext_leaves: automatic Projection
// decrypt-on-read, no `Projection.sensitive_fields` declaration at all
// (§9's own "read_projection's own decrypt-on-read" pass) ---

#[test]
fn decrypt_ciphertext_leaves_decrypts_a_top_level_string_leaf() {
    let master = master_key(12);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let ciphertext = encryption::encrypt_leaf(&data_key, "person@example.com");
    let mut value = serde_json::json!({ "email": ciphertext, "total": 42 });

    encryption::decrypt_ciphertext_leaves(&mut value, &[data_key]);

    assert_eq!(value["email"], "person@example.com");
    assert_eq!(value["total"], 42); // never a decrypt candidate - not a string
}

#[test]
fn decrypt_ciphertext_leaves_decrypts_a_leaf_nested_inside_an_object() {
    let master = master_key(13);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let ciphertext = encryption::encrypt_leaf(&data_key, "555-1234");
    let mut value = serde_json::json!({ "contact": { "phone": ciphertext, "note": "unchanged" } });

    encryption::decrypt_ciphertext_leaves(&mut value, &[data_key]);

    assert_eq!(value["contact"]["phone"], "555-1234");
    assert_eq!(value["contact"]["note"], "unchanged");
}

#[test]
fn decrypt_ciphertext_leaves_decrypts_leaves_nested_inside_a_list_of_objects() {
    let master = master_key(14);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let a = encryption::encrypt_leaf(&data_key, "alice@example.com");
    let b = encryption::encrypt_leaf(&data_key, "bob@example.com");
    let mut value = serde_json::json!({
        "participants": [
            { "email": a },
            { "email": b, "grade": "A" },
        ]
    });

    encryption::decrypt_ciphertext_leaves(&mut value, &[data_key]);

    assert_eq!(value["participants"][0]["email"], "alice@example.com");
    assert_eq!(value["participants"][1]["email"], "bob@example.com");
    assert_eq!(value["participants"][1]["grade"], "A");
}

/// A leaf that isn't ciphertext under any candidate key - genuinely plain
/// data, or ciphertext for some other subject - is left completely
/// untouched, no panic.
#[test]
fn decrypt_ciphertext_leaves_leaves_a_non_matching_string_untouched() {
    let master = master_key(15);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let mut value = serde_json::json!({ "note": "just a plain string" });

    encryption::decrypt_ciphertext_leaves(&mut value, &[data_key]);

    assert_eq!(value["note"], "just a plain string");
}

/// The first candidate key that actually decrypts a leaf wins - a leaf
/// only ever matches the one key it was really encrypted under, the rest
/// are tried and fail harmlessly.
#[test]
fn decrypt_ciphertext_leaves_tries_every_candidate_key_until_one_matches() {
    let master = master_key(16);
    let (wrong_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let (right_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let ciphertext = encryption::encrypt_leaf(&right_key, "secret");
    let mut value = serde_json::json!({ "field": ciphertext });

    encryption::decrypt_ciphertext_leaves(&mut value, &[wrong_key, right_key]);

    assert_eq!(value["field"], "secret");
}

#[test]
fn decrypt_ciphertext_leaves_never_touches_non_string_leaves() {
    let master = master_key(17);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let mut value = serde_json::json!({ "count": 7, "active": true, "ratio": 1.5, "tag": null });

    encryption::decrypt_ciphertext_leaves(&mut value, &[data_key]);

    assert_eq!(value["count"], 7);
    assert_eq!(value["active"], true);
    assert_eq!(value["ratio"], 1.5);
    assert!(value["tag"].is_null());
}

// --- projections::read_projection ---

#[test]
fn read_projection_returns_state_verbatim_when_no_data_keys_were_resolved() {
    let state = skilj_core::projections::read_projection(r#"{"email":"ZmFrZQ=="}"#, &[]);
    assert_eq!(state, r#"{"email":"ZmFrZQ=="}"#);
}

#[test]
fn read_projection_decrypts_via_the_resolved_data_keys() {
    let master = master_key(18);
    let (data_key, _, _) = encryption::generate_and_wrap_data_key(&master);
    let ciphertext = encryption::encrypt_leaf(&data_key, "person@example.com");
    let state_json = format!(r#"{{"email":"{ciphertext}","total":3}}"#);

    let decrypted = skilj_core::projections::read_projection(&state_json, &[data_key]);

    let parsed: serde_json::Value = serde_json::from_str(&decrypted).unwrap();
    assert_eq!(parsed["email"], "person@example.com");
    assert_eq!(parsed["total"], 3);
}
