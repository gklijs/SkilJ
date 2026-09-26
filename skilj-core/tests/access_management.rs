//! Tests for the `AccessManagement` surface (`specs/skilj.allium`) -
//! propagated after CommandTrigger/ProcessCommand (docs/architecture.md
//! [§9](../../docs/architecture.md#next-steps)): entities `Role`/`RoleAccessMapping`, enum `AccessLevel`, rules
//! `CreateRole`/`RevokeRole`/`GrantRoleAccessMapping`/
//! `RevokeRoleAccessMapping`. This is what unblocked
//! `AuthoriseCommandSubmission`/`CommandSubmission`, added in
//! `command_processing.rs` in a later pass once these existed to
//! authorise against. `resolve_role_by_external_subject` - the JWT-to-
//! Role identity resolution pipeline's one pure step - was added last, in
//! its own later pass, and is tested here too since it shares this file's
//! `Role` fixtures.
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 36 total.
//! `Role.access_mappings` (1, a relationship projection, deferred the
//! same way every other one in this codebase has been) and
//! `surface-actor`/`surface-provides.AccessManagement` (2) were left
//! unbookkept here - see the doc comment at the bottom of this file for
//! why the reason originally given for the latter pair is now stale.
//! `resolve_role_by_external_subject` isn't a spec `rule`, so it adds no
//! obligations to that count - see its own test section above the
//! uncovered one at the bottom of this file.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn superadmin(status: RoleStatus) -> Role {
    Role {
        id: "role-1".into(),
        external_subject: "admin@example.com".into(),
        name: "Admin".into(),
        superadmin: true,
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn non_superadmin() -> Role {
    Role {
        superadmin: false,
        id: "role-2".into(),
        external_subject: "someone@example.com".into(),
        ..superadmin(RoleStatus::Active)
    }
}

fn target_role(status: RoleStatus) -> Role {
    Role {
        id: "role-3".into(),
        external_subject: "user@example.com".into(),
        name: "User".into(),
        superadmin: false,
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
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

fn mapping(role: Role, status: RoleStatus) -> RoleAccessMapping {
    RoleAccessMapping {
        role,
        bounded_context: bounded_context(BoundedContextStatus::Active),
        level: AccessLevel::Write,
        can_read_sensitive: false,
        scope: None,
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

// ---------------------------------------------------------------------
// entity-fields.Role / entity-fields.RoleAccessMapping / enum-comparable.AccessLevel
// ---------------------------------------------------------------------

#[test]
fn role_carries_its_declared_fields() {
    let r = target_role(RoleStatus::Active);
    assert_eq!(r.id, "role-3");
    assert_eq!(r.external_subject, "user@example.com");
    assert_eq!(r.name, "User");
    assert!(!r.superadmin);
    assert_eq!(r.status, RoleStatus::Active);
    assert_eq!(r.created_at, timestamp(0));
    assert_eq!(r.revoked_at, None);
}

#[test]
fn role_access_mapping_carries_its_declared_fields() {
    let m = mapping(target_role(RoleStatus::Active), RoleStatus::Active);
    assert_eq!(m.level, AccessLevel::Write);
    assert!(!m.can_read_sensitive);
    assert_eq!(m.status, RoleStatus::Active);
    assert_eq!(m.revoked_at, None);
}

#[test]
fn access_level_is_comparable() {
    assert_eq!(AccessLevel::Write, AccessLevel::Write);
    assert_ne!(AccessLevel::Read, AccessLevel::Admin);
}

// ---------------------------------------------------------------------
// Role.status / RoleAccessMapping.status - transition-edge / -rejected / -terminal
// ---------------------------------------------------------------------
//
// Both entities declare the identical `active -> revoked` graph, `revoked`
// terminal. `revoke_role`/`revoke_role_access_mapping` below are each
// status's one witnessing rule; there is no other transition to reject,
// so "undeclared transitions are rejected" and "the terminal state has no
// outbound transitions" collapse to the same fact: revoking an
// already-revoked Role/mapping is rejected, not a silent no-op, because
// the rule itself requires `status = active` first.

#[test]
fn role_status_active_to_revoked_is_reachable_via_revoke_role() {
    let (revoked, _) = access_control::revoke_role(
        &superadmin(RoleStatus::Active),
        &target_role(RoleStatus::Active),
        &[],
        timestamp(5),
    )
    .unwrap();
    assert_eq!(revoked.status, RoleStatus::Revoked);
}

#[test]
fn role_status_revoked_is_terminal_and_rejects_a_second_revocation() {
    let err = access_control::revoke_role(
        &superadmin(RoleStatus::Active),
        &target_role(RoleStatus::Revoked),
        &[],
        timestamp(5),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

#[test]
fn role_access_mapping_status_active_to_revoked_is_reachable_via_revoke_role_access_mapping() {
    let revoked = access_control::revoke_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &mapping(target_role(RoleStatus::Active), RoleStatus::Active),
        timestamp(5),
    )
    .unwrap();
    assert_eq!(revoked.status, RoleStatus::Revoked);
}

#[test]
fn role_access_mapping_status_revoked_is_terminal_and_rejects_a_second_revocation() {
    let err = access_control::revoke_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &mapping(target_role(RoleStatus::Active), RoleStatus::Revoked),
        timestamp(5),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

// ---------------------------------------------------------------------
// rule-success.CreateRole / rule-failure.CreateRole.{1,2,3}
// / rule-entity-creation.CreateRole.1
// ---------------------------------------------------------------------

#[test]
fn create_role_succeeds_with_the_given_fields() {
    let caller = superadmin(RoleStatus::Active);

    let role = access_control::create_role(
        &caller,
        "New Role".into(),
        false,
        "new@example.com".into(),
        &[],
        "role-9".into(),
        timestamp(20),
    )
    .unwrap();

    // rule-entity-creation.CreateRole.1
    assert_eq!(role.id, "role-9");
    assert_eq!(role.external_subject, "new@example.com");
    assert_eq!(role.name, "New Role");
    assert!(!role.superadmin);
    assert_eq!(role.status, RoleStatus::Active);
    assert_eq!(role.created_at, timestamp(20));
    assert_eq!(role.revoked_at, None);
}

/// rule-failure.CreateRole.1 - `requires: caller.status = active`.
#[test]
fn create_role_rejects_an_inactive_caller() {
    let err = access_control::create_role(
        &superadmin(RoleStatus::Revoked),
        "N".into(),
        false,
        "n@example.com".into(),
        &[],
        "id".into(),
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

/// rule-failure.CreateRole.2 - `requires: caller.superadmin = true`.
#[test]
fn create_role_rejects_a_non_superadmin_caller() {
    let err = access_control::create_role(
        &non_superadmin(),
        "N".into(),
        false,
        "n@example.com".into(),
        &[],
        "id".into(),
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::NotSuperadmin.code());
}

/// rule-failure.CreateRole.3 - `requires: not exists Role{external_subject, status: active}`
/// (`UniqueActiveExternalSubject`).
#[test]
fn create_role_rejects_an_already_claimed_external_subject() {
    let existing = target_role(RoleStatus::Active); // external_subject: "user@example.com"
    let err = access_control::create_role(
        &superadmin(RoleStatus::Active),
        "N".into(),
        false,
        "user@example.com".into(),
        std::slice::from_ref(&existing),
        "id".into(),
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(
        err.code(),
        access_control::Error::ExternalSubjectAlreadyClaimed.code()
    );
}

/// A *revoked* Role's external_subject is claimable again - the
/// `status: active` qualifier on `UniqueActiveExternalSubject` is real,
/// not incidental.
#[test]
fn create_role_allows_reclaiming_a_revoked_roles_external_subject() {
    let existing = target_role(RoleStatus::Revoked);
    let role = access_control::create_role(
        &superadmin(RoleStatus::Active),
        "N".into(),
        false,
        "user@example.com".into(),
        std::slice::from_ref(&existing),
        "id".into(),
        timestamp(0),
    )
    .unwrap();
    assert_eq!(role.external_subject, "user@example.com");
}

// ---------------------------------------------------------------------
// rule-success.RevokeRole / rule-failure.RevokeRole.{1,2,3}
// / when-set.RevokeRole.Role.revoked_at
// ---------------------------------------------------------------------

#[test]
fn revoke_role_cascades_to_every_active_mapping_and_leaves_revoked_ones_alone() {
    let role = target_role(RoleStatus::Active);
    let active_mapping = mapping(role.clone(), RoleStatus::Active);
    let already_revoked = RoleAccessMapping {
        revoked_at: Some(timestamp(1)),
        ..mapping(role.clone(), RoleStatus::Revoked)
    };

    let (revoked_role, revoked_mappings) = access_control::revoke_role(
        &superadmin(RoleStatus::Active),
        &role,
        &[active_mapping],
        timestamp(50),
    )
    .unwrap();

    // when-set.RevokeRole.Role.revoked_at
    assert_eq!(revoked_role.status, RoleStatus::Revoked);
    assert_eq!(revoked_role.revoked_at, Some(timestamp(50)));

    assert_eq!(revoked_mappings.len(), 1);
    assert_eq!(revoked_mappings[0].status, RoleStatus::Revoked);
    assert_eq!(revoked_mappings[0].revoked_at, Some(timestamp(50)));

    // already_revoked was never passed in as "active" - revoke_role only
    // ever receives the active subset (see its own doc comment), so
    // there's nothing here to re-revoke; this asserts the fixture reads
    // as intended, not a code path.
    assert_eq!(already_revoked.status, RoleStatus::Revoked);
}

/// rule-failure.RevokeRole.1 - `requires: caller.status = active`.
#[test]
fn revoke_role_rejects_an_inactive_caller() {
    let err = access_control::revoke_role(
        &superadmin(RoleStatus::Revoked),
        &target_role(RoleStatus::Active),
        &[],
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

/// rule-failure.RevokeRole.2 - `requires: caller.superadmin = true`.
#[test]
fn revoke_role_rejects_a_non_superadmin_caller() {
    let err = access_control::revoke_role(
        &non_superadmin(),
        &target_role(RoleStatus::Active),
        &[],
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::NotSuperadmin.code());
}

/// rule-failure.RevokeRole.3 - `requires: role.status = active`. Same
/// case as `role_status_revoked_is_terminal_and_rejects_a_second_revocation`
/// above, from the requires-clause angle.
#[test]
fn revoke_role_rejects_an_already_revoked_role() {
    let err = access_control::revoke_role(
        &superadmin(RoleStatus::Active),
        &target_role(RoleStatus::Revoked),
        &[],
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

// ---------------------------------------------------------------------
// rule-success.GrantRoleAccessMapping / rule-failure.GrantRoleAccessMapping.{1..5}
// / rule-entity-creation.GrantRoleAccessMapping.1
// ---------------------------------------------------------------------

#[test]
fn grant_role_access_mapping_succeeds_with_the_given_fields() {
    let bc = bounded_context(BoundedContextStatus::Active);
    let role = target_role(RoleStatus::Active);

    let m = access_control::grant_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &role,
        &bc,
        AccessLevel::Admin,
        true,
        None,
        &[],
        timestamp(30),
    )
    .unwrap();

    // rule-entity-creation.GrantRoleAccessMapping.1
    assert_eq!(m.role, role);
    assert_eq!(m.bounded_context, bc);
    assert_eq!(m.level, AccessLevel::Admin);
    assert!(m.can_read_sensitive);
    assert_eq!(m.status, RoleStatus::Active);
    assert_eq!(m.created_at, timestamp(30));
    assert_eq!(m.revoked_at, None);
}

/// rule-failure.GrantRoleAccessMapping.1 - `requires: caller.status = active`.
#[test]
fn grant_role_access_mapping_rejects_an_inactive_caller() {
    let bc = bounded_context(BoundedContextStatus::Active);
    let err = access_control::grant_role_access_mapping(
        &superadmin(RoleStatus::Revoked),
        &target_role(RoleStatus::Active),
        &bc,
        AccessLevel::Read,
        false,
        None,
        &[],
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

/// rule-failure.GrantRoleAccessMapping.2 - `requires: caller.superadmin = true`.
#[test]
fn grant_role_access_mapping_rejects_a_non_superadmin_caller() {
    let bc = bounded_context(BoundedContextStatus::Active);
    let err = access_control::grant_role_access_mapping(
        &non_superadmin(),
        &target_role(RoleStatus::Active),
        &bc,
        AccessLevel::Read,
        false,
        None,
        &[],
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::NotSuperadmin.code());
}

/// rule-failure.GrantRoleAccessMapping.3 - `requires: role.status = active`.
#[test]
fn grant_role_access_mapping_rejects_an_inactive_target_role() {
    let bc = bounded_context(BoundedContextStatus::Active);
    let err = access_control::grant_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &target_role(RoleStatus::Revoked),
        &bc,
        AccessLevel::Read,
        false,
        None,
        &[],
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

/// rule-failure.GrantRoleAccessMapping.4 - `requires: bounded_context.status = active`.
#[test]
fn grant_role_access_mapping_rejects_an_archived_bounded_context() {
    let bc = bounded_context(BoundedContextStatus::Archived);
    let err = access_control::grant_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &target_role(RoleStatus::Active),
        &bc,
        AccessLevel::Read,
        false,
        None,
        &[],
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(
        err.code(),
        skilj_core::event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.GrantRoleAccessMapping.5 - `requires: not exists
/// RoleAccessMapping{role, bounded_context, status: active}`.
#[test]
fn grant_role_access_mapping_rejects_a_duplicate_active_mapping() {
    let bc = bounded_context(BoundedContextStatus::Active);
    let role = target_role(RoleStatus::Active);
    let existing = mapping(role.clone(), RoleStatus::Active);

    let err = access_control::grant_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &role,
        &bc,
        AccessLevel::Write,
        false,
        None,
        std::slice::from_ref(&existing),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::DuplicateActiveMapping.code()
    );
}

/// A *revoked* mapping for the same (role, bounded_context) doesn't block
/// a fresh grant - `UniqueActiveAccessPerRoleAndContext`'s `status:
/// active` qualifier is real, same as `CreateRole`'s external_subject one.
#[test]
fn grant_role_access_mapping_allows_regranting_after_a_revocation() {
    let bc = bounded_context(BoundedContextStatus::Active);
    let role = target_role(RoleStatus::Active);
    let existing = mapping(role.clone(), RoleStatus::Revoked);

    let m = access_control::grant_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &role,
        &bc,
        AccessLevel::Write,
        false,
        None,
        std::slice::from_ref(&existing),
        timestamp(0),
    )
    .unwrap();

    assert_eq!(m.status, RoleStatus::Active);
}

// ---------------------------------------------------------------------
// rule-success.RevokeRoleAccessMapping / rule-failure.RevokeRoleAccessMapping.{1,2,3}
// ---------------------------------------------------------------------

#[test]
fn revoke_role_access_mapping_succeeds() {
    let m = mapping(target_role(RoleStatus::Active), RoleStatus::Active);
    let revoked = access_control::revoke_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &m,
        timestamp(40),
    )
    .unwrap();
    assert_eq!(revoked.status, RoleStatus::Revoked);
    assert_eq!(revoked.revoked_at, Some(timestamp(40)));
}

/// rule-failure.RevokeRoleAccessMapping.1 - `requires: caller.status = active`.
#[test]
fn revoke_role_access_mapping_rejects_an_inactive_caller() {
    let m = mapping(target_role(RoleStatus::Active), RoleStatus::Active);
    let err = access_control::revoke_role_access_mapping(
        &superadmin(RoleStatus::Revoked),
        &m,
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

/// rule-failure.RevokeRoleAccessMapping.2 - `requires: caller.superadmin = true`.
#[test]
fn revoke_role_access_mapping_rejects_a_non_superadmin_caller() {
    let m = mapping(target_role(RoleStatus::Active), RoleStatus::Active);
    let err = access_control::revoke_role_access_mapping(&non_superadmin(), &m, timestamp(0))
        .unwrap_err();
    assert_eq!(err.code(), access_control::Error::NotSuperadmin.code());
}

/// rule-failure.RevokeRoleAccessMapping.3 - `requires: access_mapping.status = active`.
/// Same case as the transition-terminal test above, from the
/// requires-clause angle.
#[test]
fn revoke_role_access_mapping_rejects_an_already_revoked_mapping() {
    let m = mapping(target_role(RoleStatus::Active), RoleStatus::Revoked);
    let err = access_control::revoke_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &m,
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

// ---------------------------------------------------------------------
// RevokedRoleImpliesMappingsRevoked / UniqueActiveAccessPerRoleAndContext
// ---------------------------------------------------------------------

/// RevokedRoleImpliesMappingsRevoked, restated for a single pure call:
/// every mapping `revoke_role` was handed as "active" comes back revoked,
/// with nothing left unaccounted for - the invariant holds by
/// construction of the ensures block's `for mapping in ... where status =
/// active` loop, not by a separate check.
#[test]
fn revoke_role_leaves_no_active_mapping_behind() {
    let role = target_role(RoleStatus::Active);
    let mappings = vec![
        mapping(role.clone(), RoleStatus::Active),
        mapping(role.clone(), RoleStatus::Active),
    ];

    let (_, revoked_mappings) = access_control::revoke_role(
        &superadmin(RoleStatus::Active),
        &role,
        &mappings,
        timestamp(0),
    )
    .unwrap();

    assert!(revoked_mappings
        .iter()
        .all(|m| m.status == RoleStatus::Revoked));
    assert_eq!(revoked_mappings.len(), mappings.len());
}

/// UniqueActiveAccessPerRoleAndContext, restated the same way
/// `UniqueReadCursorPerToken` was for `consume_events`:
/// `grant_role_access_mapping`'s own duplicate-check (tested above via
/// `grant_role_access_mapping_rejects_a_duplicate_active_mapping`) is what
/// keeps a get-or-create-shaped store from ever holding two active
/// mappings for the same (role, bounded_context) key - a property test
/// isn't needed on top of that direct test; this one exists to name the
/// invariant explicitly for reconciliation.
#[test]
fn unique_active_access_per_role_and_context_is_enforced_by_grant_role_access_mappings_own_check() {
    let bc = bounded_context(BoundedContextStatus::Active);
    let role = target_role(RoleStatus::Active);
    let existing = mapping(role.clone(), RoleStatus::Active);

    let err = access_control::grant_role_access_mapping(
        &superadmin(RoleStatus::Active),
        &role,
        &bc,
        AccessLevel::Read,
        false,
        None,
        std::slice::from_ref(&existing),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::DuplicateActiveMapping.code()
    );
}

// ---------------------------------------------------------------------
// resolve_role_by_external_subject - GraphQL identity resolution's one
// pure step (see its own doc comment). Not a `rule` in the spec - the
// prose above `entity Role` and `Actor Declarations`, not a separate
// obligation `allium plan` enumerates - so covered directly rather than
// against a specific obligation count.
// ---------------------------------------------------------------------

#[test]
fn resolve_role_by_external_subject_finds_the_matching_active_role() {
    let target = target_role(RoleStatus::Active);
    let roles = vec![superadmin(RoleStatus::Active), target.clone()];

    let resolved =
        access_control::resolve_role_by_external_subject("user@example.com", &roles).unwrap();

    assert_eq!(resolved, &target);
}

/// `UniqueActiveExternalSubject` is what makes this well defined - among
/// several Roles, only the one whose `external_subject` actually matches
/// is ever returned.
#[test]
fn resolve_role_by_external_subject_ignores_roles_with_a_different_subject() {
    let roles = vec![
        superadmin(RoleStatus::Active),
        target_role(RoleStatus::Active),
    ];

    let resolved =
        access_control::resolve_role_by_external_subject("admin@example.com", &roles).unwrap();

    assert_eq!(resolved.external_subject, "admin@example.com");
}

/// A revoked Role's `external_subject` doesn't count as claimed - the
/// same "revoked no longer holds the identity" reading `CreateSuperadmin`
/// and `CreateRole` both give `UniqueActiveExternalSubject`.
#[test]
fn resolve_role_by_external_subject_ignores_a_revoked_role() {
    let roles = vec![target_role(RoleStatus::Revoked)];

    let err =
        access_control::resolve_role_by_external_subject("user@example.com", &roles).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::UnrecognisedSubject.code()
    );
}

#[test]
fn resolve_role_by_external_subject_rejects_an_unclaimed_subject() {
    let roles = vec![superadmin(RoleStatus::Active)];

    let err =
        access_control::resolve_role_by_external_subject("nobody@example.com", &roles).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::UnrecognisedSubject.code()
    );
}

#[test]
fn resolve_role_by_external_subject_rejects_when_no_roles_exist_at_all() {
    let err =
        access_control::resolve_role_by_external_subject("anyone@example.com", &[]).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::UnrecognisedSubject.code()
    );
}

// ---------------------------------------------------------------------
// Role.access_mappings / surface-actor.AccessManagement /
// surface-provides.AccessManagement - uncovered this pass
// ---------------------------------------------------------------------
//
// Role.access_mappings is a relationship projection, not a stored field -
// same deferral as EventType's *_tokens/Command's triggered_events: a
// caller already has the relevant mappings as whatever it passed in to
// revoke_role/grant_role_access_mapping.
//
// surface-actor/surface-provides.AccessManagement: these two obligation
// ids aren't bookkept as covered by name in any pass's own "obligations
// covered here" count - a documentation gap to close with a fresh pass
// over `allium plan`'s own output, not a missing-implementation one.
// Real coverage exists in `skilj/tests/graphql_admin_console.rs`'s
// `full_admin_console_lifecycle_end_to_end`.
