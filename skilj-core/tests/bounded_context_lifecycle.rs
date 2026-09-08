//! Tests for the `BoundedContextCreation`/`BoundedContextDirectory`/
//! `BoundedContextArchival`/`BoundedContextDeletion`/`SuperadminBootstrap`
//! surfaces (`specs/skilj.allium`) - propagated after the token lifecycle
//! pass ([docs/architecture.md §9](../../docs/architecture.md#next-steps)): entities `BootstrapSecret`/
//! `ContextCreator` (+ `SuperadminCreator`/`SystemCreator`), rules
//! `AddBoundedContext`/`ListBoundedContexts`/`ArchiveBoundedContext`/
//! `CreateSuperadmin`. This is what grew `event_store::BoundedContext`
//! from `name`/`status` alone to the full entity - `created_at`/
//! `created_by` - and `secret_matches` from `// TODO` to real (see each's
//! own doc comment).
//!
//! `DeleteBoundedContext` (5 further obligations) was added in the
//! schema-per-bounded-context pass and is covered here too - see
//! `bootstrap::delete_bounded_context`'s own doc comment for why it's a
//! pure decision only, with no `db::hard_delete_bounded_context` call in
//! these tests (that's `skilj-core/tests/persistence.rs`'s job, against
//! real Postgres). The same pass also added `AddBoundedContext`'s new
//! `requires: valid_bounded_context_name(name)` (1 further obligation,
//! `rule-failure.AddBoundedContext.4`).
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 32 total.
//! Uncovered this pass, with reason - see the doc comment at the bottom of
//! this file: `entity-relationship.BoundedContext.*` (8) - every
//! relationship projection deferred the same "caller resolves it, not a
//! stored field" way as everywhere else in this codebase - and
//! `surface-actor`/`surface-exposure`/`surface-provides` for the four
//! GraphQL-only surfaces (9, `BoundedContextDeletion` included), same
//! scaffolding gap as `CommandSubmission`'s.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::{self, BootstrapSecret, ContextCreator};
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

fn bounded_context(name: &str, status: BoundedContextStatus) -> BoundedContext {
    BoundedContext {
        name: name.into(),
        status,
        created_at: timestamp(0),
        created_by: ContextCreator::SuperadminCreator {
            role: superadmin(RoleStatus::Active),
        },
        template: None,
    }
}

fn admin_bounded_context() -> BoundedContext {
    bounded_context(
        bootstrap::ADMIN_BOUNDED_CONTEXT_NAME,
        BoundedContextStatus::Active,
    )
}

fn access_mapping(
    status: RoleStatus,
    level: AccessLevel,
    bounded_context: BoundedContext,
) -> RoleAccessMapping {
    RoleAccessMapping {
        role: superadmin(RoleStatus::Active),
        bounded_context,
        level,
        can_read_sensitive: false,
        scope: None,
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn bootstrap_secret() -> BootstrapSecret {
    BootstrapSecret {
        secret: "high-entropy-bootstrap-secret".into(),
    }
}

// ---------------------------------------------------------------------
// entity-fields.BootstrapSecret / entity-fields.ContextCreator
// / sum-type-variant.{SuperadminCreator,SystemCreator}
// ---------------------------------------------------------------------

#[test]
fn bootstrap_secret_carries_its_declared_field() {
    let b = bootstrap_secret();
    assert_eq!(b.secret, "high-entropy-bootstrap-secret");
}

/// `generate_bootstrap_secret` - added propagating `skilj-graphql`'s
/// Phase 1 admin console ([docs/architecture.md §8](../../docs/architecture.md#open-for-a-future-pass) item 5's own plan).
/// `Some` when no active superadmin exists yet - the reachable case,
/// checked here only for "produces a secret at all"; `create_superadmin`'s
/// own tests already cover matching against a real one end-to-end.
#[test]
fn generate_bootstrap_secret_produces_a_secret_when_no_active_superadmin_exists() {
    let secret = bootstrap::generate_bootstrap_secret(&[]);
    assert!(secret.is_some());

    let secret = bootstrap::generate_bootstrap_secret(&[non_superadmin()]);
    assert!(secret.is_some());

    let secret = bootstrap::generate_bootstrap_secret(&[superadmin(RoleStatus::Revoked)]);
    assert!(secret.is_some());
}

/// `ClosesPermanentlyOnFirstClaim` (`surface SuperadminBootstrap`): once
/// an active superadmin exists, there is nothing left to generate.
#[test]
fn generate_bootstrap_secret_is_none_once_an_active_superadmin_exists() {
    let secret = bootstrap::generate_bootstrap_secret(&[superadmin(RoleStatus::Active)]);
    assert!(secret.is_none());
}

/// `stamp_admin_bounded_context` - the drift audit's #4 fix (see project
/// memory `skilj-drift-audit-2026-08-18`): `default BoundedContext admin`'s
/// own `created_at`/`created_by`, real recorded values a static default
/// can't provide, stamped once at SkilJ's own first startup. `None`
/// existing (this database's genuine first startup) produces the row,
/// `SystemCreator`-owned, at exactly the `now` passed in - independent of
/// whether any superadmin exists yet, unlike `generate_bootstrap_secret`
/// above.
#[test]
fn stamp_admin_bounded_context_produces_the_row_on_a_genuine_first_startup() {
    let stamped = bootstrap::stamp_admin_bounded_context(None, timestamp(1000)).unwrap();
    assert_eq!(stamped.name, bootstrap::ADMIN_BOUNDED_CONTEXT_NAME);
    assert_eq!(stamped.status, BoundedContextStatus::Active);
    assert_eq!(stamped.created_at, timestamp(1000));
    assert_eq!(stamped.created_by, ContextCreator::SystemCreator);
}

/// "acts only while the values are unset, so a later restart never
/// restamps them" (the note above `default BoundedContext admin` in
/// specs/skilj.allium): a later startup's own `now` is never used,
/// whatever `existing` already carries.
#[test]
fn stamp_admin_bounded_context_is_none_once_the_row_already_exists() {
    let already_stamped = admin_bounded_context();
    let restamp_attempt =
        bootstrap::stamp_admin_bounded_context(Some(&already_stamped), timestamp(9999));
    assert!(restamp_attempt.is_none());
}

#[test]
fn superadmin_creator_carries_its_variant_specific_field() {
    let role = superadmin(RoleStatus::Active);
    let creator = ContextCreator::SuperadminCreator { role: role.clone() };

    match creator {
        ContextCreator::SuperadminCreator { role: r } => assert_eq!(r, role),
        other => panic!("expected SuperadminCreator, got {other:?}"),
    }
}

/// `SystemCreator` has no variant-specific fields - the type guard itself
/// (matching the variant at all) is the whole of this obligation, the
/// same treatment `EventOrigin::SystemTriggered`'s own test gets.
#[test]
fn system_creator_is_reachable_through_the_context_creator_type_guard() {
    let creator = ContextCreator::SystemCreator;

    match creator {
        ContextCreator::SystemCreator => {}
        other => panic!("expected SystemCreator, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// rule-success.AddBoundedContext / rule-failure.AddBoundedContext.{1,2,3}
// / rule-entity-creation.AddBoundedContext.1
// ---------------------------------------------------------------------

#[test]
fn add_bounded_context_succeeds_and_stamps_the_full_entity_shape() {
    let caller = superadmin(RoleStatus::Active);

    let bc =
        bootstrap::add_bounded_context(&caller, "billing".into(), &[], timestamp(1000)).unwrap();

    // rule-entity-creation.AddBoundedContext.1
    assert_eq!(bc.name, "billing");
    assert_eq!(bc.status, BoundedContextStatus::Active);
    assert_eq!(bc.created_at, timestamp(1000));
    match bc.created_by {
        ContextCreator::SuperadminCreator { role } => assert_eq!(role, caller),
        other => panic!("expected SuperadminCreator, got {other:?}"),
    }
}

/// rule-failure.AddBoundedContext.1 - `requires: caller.status = active`.
#[test]
fn add_bounded_context_rejects_a_revoked_caller() {
    let caller = superadmin(RoleStatus::Revoked);

    let err =
        bootstrap::add_bounded_context(&caller, "billing".into(), &[], timestamp(0)).unwrap_err();

    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

/// rule-failure.AddBoundedContext.2 - `requires: caller.superadmin = true`.
#[test]
fn add_bounded_context_rejects_a_non_superadmin_caller() {
    let caller = non_superadmin();

    let err =
        bootstrap::add_bounded_context(&caller, "billing".into(), &[], timestamp(0)).unwrap_err();

    assert_eq!(err.code(), access_control::Error::NotSuperadmin.code());
}

/// rule-failure.AddBoundedContext.3 - `requires: not exists BoundedContext
/// {name: name}`.
#[test]
fn add_bounded_context_rejects_a_name_already_taken() {
    let caller = superadmin(RoleStatus::Active);
    let existing = vec![bounded_context("billing", BoundedContextStatus::Active)];

    let err = bootstrap::add_bounded_context(&caller, "billing".into(), &existing, timestamp(0))
        .unwrap_err();

    assert_eq!(err.code(), bootstrap::Error::BoundedContextNameTaken.code());
}

/// The uniqueness guard sees the `admin` default too - it is a real
/// `BoundedContext` in `existing_contexts` like any other, not exempted
/// by name.
#[test]
fn add_bounded_context_rejects_the_admin_name_even_though_admin_was_never_added_by_this_rule() {
    let caller = superadmin(RoleStatus::Active);
    let existing = vec![admin_bounded_context()];

    let err = bootstrap::add_bounded_context(
        &caller,
        bootstrap::ADMIN_BOUNDED_CONTEXT_NAME.into(),
        &existing,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), bootstrap::Error::BoundedContextNameTaken.code());
}

/// rule-failure.AddBoundedContext.4 - `requires: valid_bounded_context_name(name)`.
#[test]
fn add_bounded_context_rejects_a_name_with_an_uppercase_character() {
    let caller = superadmin(RoleStatus::Active);

    let err =
        bootstrap::add_bounded_context(&caller, "Billing".into(), &[], timestamp(0)).unwrap_err();

    assert_eq!(
        err.code(),
        bootstrap::Error::InvalidBoundedContextName.code()
    );
}

/// rule-failure.AddBoundedContext.4 - a name that doesn't start with a
/// letter is refused the same way.
#[test]
fn add_bounded_context_rejects_a_name_starting_with_a_digit() {
    let caller = superadmin(RoleStatus::Active);

    let err =
        bootstrap::add_bounded_context(&caller, "1billing".into(), &[], timestamp(0)).unwrap_err();

    assert_eq!(
        err.code(),
        bootstrap::Error::InvalidBoundedContextName.code()
    );
}

/// rule-failure.AddBoundedContext.4 - over the 40-character cap.
#[test]
fn add_bounded_context_rejects_a_name_over_forty_characters() {
    let caller = superadmin(RoleStatus::Active);
    let name = "a".repeat(41);

    let err = bootstrap::add_bounded_context(&caller, name, &[], timestamp(0)).unwrap_err();

    assert_eq!(
        err.code(),
        bootstrap::Error::InvalidBoundedContextName.code()
    );
}

/// A name made only of lowercase letters, digits and underscores,
/// starting with a letter, at exactly the 40-character cap, is accepted.
#[test]
fn add_bounded_context_accepts_a_name_at_exactly_the_length_cap() {
    let caller = superadmin(RoleStatus::Active);
    let name = format!("a{}", "1".repeat(39));
    assert_eq!(name.len(), 40);

    let bc = bootstrap::add_bounded_context(&caller, name.clone(), &[], timestamp(0)).unwrap();

    assert_eq!(bc.name, name);
}

// ---------------------------------------------------------------------
// rule-success.ListBoundedContexts / rule-failure.ListBoundedContexts.{1,2}
// ---------------------------------------------------------------------

#[test]
fn list_bounded_contexts_succeeds_and_returns_every_context_unrestricted() {
    let caller = superadmin(RoleStatus::Active);
    let contexts = vec![
        bounded_context("billing", BoundedContextStatus::Active),
        bounded_context("legacy", BoundedContextStatus::Archived), // archived contexts stay listed
    ];

    let listed = bootstrap::list_bounded_contexts(&caller, &contexts).unwrap();

    assert_eq!(listed, contexts);
}

/// rule-failure.ListBoundedContexts.1 - `requires: caller.status = active`.
#[test]
fn list_bounded_contexts_rejects_a_revoked_caller() {
    let caller = superadmin(RoleStatus::Revoked);

    let err = bootstrap::list_bounded_contexts(&caller, &[]).unwrap_err();

    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

/// rule-failure.ListBoundedContexts.2 - `requires: caller.superadmin = true`.
#[test]
fn list_bounded_contexts_rejects_a_non_superadmin_caller() {
    let caller = non_superadmin();

    let err = bootstrap::list_bounded_contexts(&caller, &[]).unwrap_err();

    assert_eq!(err.code(), access_control::Error::NotSuperadmin.code());
}

// ---------------------------------------------------------------------
// rule-success.ArchiveBoundedContext / rule-failure.ArchiveBoundedContext.{1..5}
// / transition-edge/transition-terminal.BoundedContext.status
// ---------------------------------------------------------------------

#[test]
fn archive_bounded_context_succeeds_and_moves_status_to_archived() {
    let bc = bounded_context("billing", BoundedContextStatus::Active);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin, bc.clone());

    // transition-edge.BoundedContext.active.archived
    let archived = bootstrap::archive_bounded_context(&mapping, &bc).unwrap();

    assert_eq!(archived.status, BoundedContextStatus::Archived);
    assert_eq!(archived.name, bc.name);
}

/// rule-failure.ArchiveBoundedContext.1 - `requires: access_mapping.status = active`.
#[test]
fn archive_bounded_context_rejects_a_revoked_mapping() {
    let bc = bounded_context("billing", BoundedContextStatus::Active);
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin, bc.clone());

    let err = bootstrap::archive_bounded_context(&mapping, &bc).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.ArchiveBoundedContext.2 - `requires: access_mapping.level = admin`.
#[test]
fn archive_bounded_context_rejects_a_write_level_mapping() {
    let bc = bounded_context("billing", BoundedContextStatus::Active);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write, bc.clone());

    let err = bootstrap::archive_bounded_context(&mapping, &bc).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.ArchiveBoundedContext.3 - `requires: access_mapping.bounded_context = bounded_context`.
#[test]
fn archive_bounded_context_rejects_a_mapping_scoped_to_a_different_context() {
    let bc = bounded_context("billing", BoundedContextStatus::Active);
    let other = bounded_context("orders", BoundedContextStatus::Active);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin, other);

    let err = bootstrap::archive_bounded_context(&mapping, &bc).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.ArchiveBoundedContext.4 - `requires: bounded_context.status = active`.
/// Also transition-terminal.BoundedContext.status: archived has no
/// outbound transition, so a second archival is rejected the same way.
#[test]
fn archive_bounded_context_rejects_an_already_archived_context() {
    let bc = bounded_context("billing", BoundedContextStatus::Archived);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin, bc.clone());

    let err = bootstrap::archive_bounded_context(&mapping, &bc).unwrap_err();

    assert_eq!(
        err.code(),
        skilj_core::event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.ArchiveBoundedContext.5 - `requires: bounded_context != admin`.
#[test]
fn archive_bounded_context_rejects_the_admin_context() {
    let bc = admin_bounded_context();
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin, bc.clone());

    let err = bootstrap::archive_bounded_context(&mapping, &bc).unwrap_err();

    assert_eq!(
        err.code(),
        bootstrap::Error::CannotArchiveAdminContext.code()
    );
}

/// transition-rejected.BoundedContext.status - the only declared
/// transition is `active -> archived`; `archive_bounded_context` is the
/// only function that ever changes `status`, and it only ever writes
/// `Archived`, so every other transition is structurally unreachable
/// rather than runtime-tested - same treatment as `revoke_token`'s own
/// `transition-rejected.AccessToken.status` obligation.
#[test]
fn archive_bounded_context_only_ever_produces_the_archived_status() {
    let bc = bounded_context("billing", BoundedContextStatus::Active);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin, bc.clone());

    let archived = bootstrap::archive_bounded_context(&mapping, &bc).unwrap();

    assert_eq!(archived.status, BoundedContextStatus::Archived);
}

// ---------------------------------------------------------------------
// rule-success.DeleteBoundedContext / rule-failure.DeleteBoundedContext.{1..4}
// ---------------------------------------------------------------------

#[test]
fn delete_bounded_context_succeeds_for_an_archived_non_admin_context() {
    let caller = superadmin(RoleStatus::Active);
    let bc = bounded_context("billing", BoundedContextStatus::Archived);

    bootstrap::delete_bounded_context(&caller, &bc).unwrap();
}

/// rule-failure.DeleteBoundedContext.1 - `requires: caller.status = active`.
#[test]
fn delete_bounded_context_rejects_a_revoked_caller() {
    let caller = superadmin(RoleStatus::Revoked);
    let bc = bounded_context("billing", BoundedContextStatus::Archived);

    let err = bootstrap::delete_bounded_context(&caller, &bc).unwrap_err();

    assert_eq!(err.code(), access_control::Error::RoleNotActive.code());
}

/// rule-failure.DeleteBoundedContext.2 - `requires: caller.superadmin = true`.
#[test]
fn delete_bounded_context_rejects_a_non_superadmin_caller() {
    let caller = non_superadmin();
    let bc = bounded_context("billing", BoundedContextStatus::Archived);

    let err = bootstrap::delete_bounded_context(&caller, &bc).unwrap_err();

    assert_eq!(err.code(), access_control::Error::NotSuperadmin.code());
}

/// rule-failure.DeleteBoundedContext.3 - `requires: bounded_context.status = archived`.
#[test]
fn delete_bounded_context_rejects_a_still_active_context() {
    let caller = superadmin(RoleStatus::Active);
    let bc = bounded_context("billing", BoundedContextStatus::Active);

    let err = bootstrap::delete_bounded_context(&caller, &bc).unwrap_err();

    assert_eq!(
        err.code(),
        bootstrap::Error::BoundedContextNotArchived.code()
    );
}

/// rule-failure.DeleteBoundedContext.4 - `requires: bounded_context != admin`.
/// Checked even for a (hypothetically) archived `admin` - `admin` can
/// never reach `Archived` through `archive_bounded_context` (see
/// `archive_bounded_context_rejects_the_admin_context` above), but this
/// rule's own guard doesn't rely on that to hold.
#[test]
fn delete_bounded_context_rejects_the_admin_context_even_if_archived() {
    let caller = superadmin(RoleStatus::Active);
    let bc = bounded_context(
        bootstrap::ADMIN_BOUNDED_CONTEXT_NAME,
        BoundedContextStatus::Archived,
    );

    let err = bootstrap::delete_bounded_context(&caller, &bc).unwrap_err();

    assert_eq!(
        err.code(),
        bootstrap::Error::CannotDeleteAdminContext.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.CreateSuperadmin / rule-failure.CreateSuperadmin.{1,2,3}
// / rule-entity-creation.CreateSuperadmin.1
// ---------------------------------------------------------------------

#[test]
fn create_superadmin_succeeds_and_stamps_the_full_entity_shape() {
    let secret = bootstrap_secret();

    let role = bootstrap::create_superadmin(
        &secret,
        "high-entropy-bootstrap-secret",
        "Root".into(),
        "root@example.com".into(),
        &[],
        "role-1".into(),
        timestamp(1000),
    )
    .unwrap();

    // rule-entity-creation.CreateSuperadmin.1
    assert_eq!(role.id, "role-1");
    assert_eq!(role.external_subject, "root@example.com");
    assert_eq!(role.name, "Root");
    assert!(role.superadmin);
    assert_eq!(role.status, RoleStatus::Active);
    assert_eq!(role.created_at, timestamp(1000));
}

/// rule-failure.CreateSuperadmin.1 - `requires: not exists Role{superadmin: true, status: active}`.
#[test]
fn create_superadmin_rejects_when_an_active_superadmin_already_exists() {
    let secret = bootstrap_secret();
    let existing = vec![superadmin(RoleStatus::Active)];

    let err = bootstrap::create_superadmin(
        &secret,
        "high-entropy-bootstrap-secret",
        "Root".into(),
        "root@example.com".into(),
        &existing,
        "role-2".into(),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), bootstrap::Error::SuperadminAlreadyExists.code());
}

/// A revoked superadmin doesn't count - the door reopens once every
/// superadmin Role has been revoked (see the note above `rule
/// CreateSuperadmin`).
#[test]
fn create_superadmin_succeeds_when_the_only_prior_superadmin_is_revoked() {
    let secret = bootstrap_secret();
    let existing = vec![superadmin(RoleStatus::Revoked)];

    let role = bootstrap::create_superadmin(
        &secret,
        "high-entropy-bootstrap-secret",
        "Root2".into(),
        "root2@example.com".into(),
        &existing,
        "role-2".into(),
        timestamp(0),
    )
    .unwrap();

    assert!(role.superadmin);
}

/// rule-failure.CreateSuperadmin.2 - `requires: secret_matches(bootstrap_secret, bootstrap.secret)`.
#[test]
fn create_superadmin_rejects_a_wrong_secret() {
    let secret = bootstrap_secret();

    let err = bootstrap::create_superadmin(
        &secret,
        "wrong-secret",
        "Root".into(),
        "root@example.com".into(),
        &[],
        "role-1".into(),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), bootstrap::Error::BootstrapSecretMismatch.code());
}

/// rule-failure.CreateSuperadmin.3 - `requires: not exists Role{external_subject: external_subject, status: active}`.
#[test]
fn create_superadmin_rejects_an_external_subject_already_claimed() {
    let secret = bootstrap_secret();
    let existing = vec![non_superadmin()]; // external_subject: "someone@example.com"

    let err = bootstrap::create_superadmin(
        &secret,
        "high-entropy-bootstrap-secret",
        "Root".into(),
        "someone@example.com".into(),
        &existing,
        "role-1".into(),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        bootstrap::Error::ExternalSubjectAlreadyClaimed.code()
    );
}

// ---------------------------------------------------------------------
// entity-relationship.BoundedContext.* - uncovered
// ---------------------------------------------------------------------
//
// event_types/command_types/commands/events/projections/subscriptions/
// access_mappings/encryption_keys (8): relationship projections, not
// stored fields - the same "caller resolves it" treatment every other
// relationship projection in this codebase gets (Role.access_mappings,
// EventType.*_tokens, ...).
//
// surface-actor/surface-exposure/surface-provides.{BoundedContextCreation,
// BoundedContextDirectory,SuperadminBootstrap} (7) - GraphQL-scaffolding
// gap, same as CommandSubmission's own deferred pair (see
// command_processing.rs's header comment) - no resolver/schema wiring in
// skilj-graphql yet.
