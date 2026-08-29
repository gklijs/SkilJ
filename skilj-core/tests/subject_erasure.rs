//! Tests for the `SubjectErasure` surface (`specs/skilj.allium`) -
//! propagated after `event_subscription.rs` (docs/architecture.md §9):
//! the full `EncryptionKey` entity (folding in `EncryptionKeyRef`'s old
//! stand-in role - neither was populated by anything else yet) and rule
//! `ForgetSubject`. Deliberately scoped to destruction only at the time -
//! `protect_sensitive_fields`'s get-or-create provisioning went real in a
//! later pass (`skilj-core/tests/encryption.rs` covers it).
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 12 of 14 total.
//! Uncovered, with reason - see the doc comment at the bottom of this
//! file: `surface-actor`/`surface-provides.SubjectErasure` (2) - the
//! usual GraphQL-scaffolding gap.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, EncryptionKey, EncryptionKeyStatus,
};

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

fn active_key() -> EncryptionKey {
    EncryptionKey {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        subject_key: "user".into(),
        subject_value: "123".into(),
        status: EncryptionKeyStatus::Active,
        created_at: timestamp(0),
        destroyed_at: None,
    }
}

// ---------------------------------------------------------------------
// entity-fields.EncryptionKey / when-presence/entity-optional.EncryptionKey.destroyed_at
// ---------------------------------------------------------------------

#[test]
fn encryption_key_carries_its_declared_fields() {
    let key = active_key();

    assert_eq!(key.subject_key, "user");
    assert_eq!(key.subject_value, "123");
    assert_eq!(key.status, EncryptionKeyStatus::Active);
    assert_eq!(key.created_at, timestamp(0));
    // when-presence/entity-optional.EncryptionKey.destroyed_at - absent
    // while active.
    assert_eq!(key.destroyed_at, None);
}

// ---------------------------------------------------------------------
// rule-success.ForgetSubject / rule-failure.ForgetSubject.{1,2,3,4}
// / transition-edge/transition-terminal.EncryptionKey.status
// ---------------------------------------------------------------------

#[test]
fn forget_subject_succeeds_and_destroys_the_key() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let key = active_key();

    // transition-edge.EncryptionKey.active.destroyed
    let destroyed = event_store::forget_subject(&mapping, &key, timestamp(500)).unwrap();

    assert_eq!(destroyed.status, EncryptionKeyStatus::Destroyed);
    // when-presence/entity-optional.EncryptionKey.destroyed_at - present
    // now that status = destroyed.
    assert_eq!(destroyed.destroyed_at, Some(timestamp(500)));
    // Everything else about the key is untouched.
    assert_eq!(destroyed.subject_key, key.subject_key);
    assert_eq!(destroyed.subject_value, key.subject_value);
}

/// rule-failure.ForgetSubject.1 - `requires: access_mapping.status = active`.
#[test]
fn forget_subject_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);
    let key = active_key();

    let err = event_store::forget_subject(&mapping, &key, timestamp(0)).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.ForgetSubject.2 - `requires: access_mapping.level = admin`.
#[test]
fn forget_subject_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    let key = active_key();

    let err = event_store::forget_subject(&mapping, &key, timestamp(0)).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.ForgetSubject.3 - `requires: access_mapping.bounded_context = key.bounded_context`.
#[test]
fn forget_subject_rejects_a_key_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let key = EncryptionKey {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        ..active_key()
    };

    let err = event_store::forget_subject(&mapping, &key, timestamp(0)).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.ForgetSubject.4 - `requires: key.status = active`. Also
/// transition-terminal.EncryptionKey.status: destroyed has no outbound
/// transition, so a second erasure is rejected the same way.
#[test]
fn forget_subject_rejects_an_already_destroyed_key() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let key = EncryptionKey {
        status: EncryptionKeyStatus::Destroyed,
        destroyed_at: Some(timestamp(0)),
        ..active_key()
    };

    let err = event_store::forget_subject(&mapping, &key, timestamp(0)).unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::EncryptionKeyNotActive.code()
    );
}

/// transition-rejected.EncryptionKey.status - the only declared transition
/// is `active -> destroyed`; `forget_subject` is the only function that
/// ever changes `status`, and it only ever writes `Destroyed`, so every
/// other transition is structurally unreachable - same treatment as
/// `revoke_token`'s own `transition-rejected.AccessToken.status`
/// obligation.
#[test]
fn forget_subject_only_ever_produces_the_destroyed_status() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let key = active_key();

    let destroyed = event_store::forget_subject(&mapping, &key, timestamp(0)).unwrap();

    assert_eq!(destroyed.status, EncryptionKeyStatus::Destroyed);
}

// ---------------------------------------------------------------------
// invariant.UniqueActiveEncryptionKeyPerSubject
// ---------------------------------------------------------------------
//
// At most one active key per (bounded_context, subject_key, subject_value).
// Held vacuously by this pass: forget_subject only ever destroys a key,
// never creates one, and provisioning a fresh active key for a subject
// that already has one (protect_sensitive_fields' get-or-create) is
// exactly the part of this feature deliberately left out of scope - see
// this file's own header comment. Nothing added here can ever produce a
// second active key for the same subject.

#[test]
fn forget_subject_never_creates_a_second_active_key() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let key = active_key();

    let destroyed = event_store::forget_subject(&mapping, &key, timestamp(0)).unwrap();

    // The only key involved ends destroyed - forget_subject has no path
    // that produces a second, still-active EncryptionKey value.
    assert_ne!(destroyed.status, EncryptionKeyStatus::Active);
}

// ---------------------------------------------------------------------
// surface-actor/surface-provides.SubjectErasure - uncovered
// ---------------------------------------------------------------------
//
// GraphQL-scaffolding gap, same as every prior surface's deferred set
// (see command_processing.rs's header comment) - no resolver/schema
// wiring in skilj-graphql yet.
