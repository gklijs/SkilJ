//! `BootstrapSecret`, `ContextCreator`/`SystemCreator`; `CreateSuperadmin`,
//! `AddBoundedContext`, `ArchiveBoundedContext`, `ListBoundedContexts`;
//! the startup reconciliation loop (§1.5 - the reconciliation-loop entry
//! point and its authentication as a real, pre-existing Role both live
//! here). See docs/architecture.md §3.2.

use crate::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use crate::error::SkiljRejection;
use crate::event_store::{BoundedContext, BoundedContextStatus};

// TODO: the `admin`/`skilj` defaults' own `created_at`/`created_by`
// stamping and the startup reconciliation loop itself
// (Skilj::builder().build(), per §1.5) - the rules below (propagating
// EventTypeAdminOperations/CommandTypeAdminOperations/TokenRevocation's
// follow-on pass) cover `BootstrapSecret`, `ContextCreator`/
// `SuperadminCreator`/`SystemCreator`, `CreateSuperadmin`,
// `AddBoundedContext`, `ListBoundedContexts` and `ArchiveBoundedContext`.
// `ADMIN_BOUNDED_CONTEXT_NAME` stands in for the literal half of the
// `admin` default (its `created_at`/`created_by` are real recorded values
// stamped once at first startup, per the note above the spec's Defaults
// section - not this module's static constant to provide).

/// The literal half of `default BoundedContext admin` - the one bounded
/// context `AddBoundedContext`/`ArchiveBoundedContext` never create or
/// archive (see `add_bounded_context`/`archive_bounded_context`'s own doc
/// comments). Its `created_at`/`created_by` are real recorded values
/// stamped once at SkilJ's own first startup, not spec-time constants, so
/// they stay with the still-TODO reconciliation loop above rather than
/// living here.
pub const ADMIN_BOUNDED_CONTEXT_NAME: &str = "admin";

/// See `entity BootstrapSecret`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapSecret {
    pub secret: String,
}

/// See `entity ContextCreator`. A Rust sum type over its two variants
/// stands in for `kind` directly, the same "the enum variant tag is the
/// field" treatment `Event.origin`/`EventOrigin` and
/// `AccessToken`/`purpose` get - there is no separate `kind` field to
/// carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextCreator {
    /// See `variant SuperadminCreator`.
    SuperadminCreator { role: Role },
    /// See `variant SystemCreator` - no variant-specific fields, exactly
    /// as `EventOrigin::SystemTriggered` has none: SkilJ is the creator,
    /// so there is no Role to point at.
    SystemCreator,
}

/// Library-level errors this module's own rules reject for.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("a superadmin Role already exists; the bootstrap secret can never be used again")]
    SuperadminAlreadyExists,

    #[error("the presented bootstrap secret doesn't match")]
    BootstrapSecretMismatch,

    #[error("a bounded context with this name already exists")]
    BoundedContextNameTaken,

    #[error("the admin bounded context can never be archived")]
    CannotArchiveAdminContext,

    #[error("this external_subject is already bound to another active Role")]
    ExternalSubjectAlreadyClaimed,
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::SuperadminAlreadyExists => "superadmin_already_exists",
            Error::BootstrapSecretMismatch => "bootstrap_secret_mismatch",
            Error::BoundedContextNameTaken => "bounded_context_name_taken",
            Error::CannotArchiveAdminContext => "cannot_archive_admin_context",
            Error::ExternalSubjectAlreadyClaimed => "external_subject_already_claimed",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

/// See `rule CreateSuperadmin`. `presented_secret` is `bootstrap_secret`
/// in the spec's own parameter list, renamed here only to avoid shadowing
/// `bootstrap: &BootstrapSecret` in the same signature. `id` is the
/// caller's own `generate_token_id()` output, the same treatment
/// `access_control::create_role`'s `id` gets (see its own doc comment).
/// `existing_roles` is every `Role` this engine currently knows of, for
/// both `not exists` guards - same full-snapshot treatment
/// `access_control::create_role`'s `existing_roles` gets, checked in the
/// spec's own order: no active superadmin first, then the secret, then
/// `UniqueActiveExternalSubject`.
pub fn create_superadmin(
    bootstrap: &BootstrapSecret,
    presented_secret: &str,
    name: String,
    external_subject: String,
    existing_roles: &[Role],
    id: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<Role> {
    if existing_roles
        .iter()
        .any(|r| r.superadmin && r.status == RoleStatus::Active)
    {
        return Err(Error::SuperadminAlreadyExists.into());
    }
    if !crate::shared::secret_matches(presented_secret, &bootstrap.secret) {
        return Err(Error::BootstrapSecretMismatch.into());
    }
    if existing_roles
        .iter()
        .any(|r| r.external_subject == external_subject && r.status == RoleStatus::Active)
    {
        return Err(Error::ExternalSubjectAlreadyClaimed.into());
    }

    Ok(Role {
        id,
        external_subject,
        name,
        superadmin: true,
        status: RoleStatus::Active,
        created_at: now,
        revoked_at: None,
    })
}

/// See `rule AddBoundedContext`. `existing_contexts` is every
/// `BoundedContext` this engine currently knows of (the `admin` default
/// included), for the `not exists BoundedContext{name: name}` check -
/// same full-snapshot treatment as `create_superadmin`'s `existing_roles`.
/// Structurally never produces the `admin` context itself: its own
/// `created_by` is always `SuperadminCreator`, never `SystemCreator` (see
/// `ContextCreator`'s own doc comment and invariant
/// `SystemCreatedContextIsTheAdminContext`).
pub fn add_bounded_context(
    caller: &Role,
    name: String,
    existing_contexts: &[BoundedContext],
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<BoundedContext> {
    crate::access_control::require_active_superadmin(caller)?;
    if existing_contexts.iter().any(|bc| bc.name == name) {
        return Err(Error::BoundedContextNameTaken.into());
    }

    Ok(BoundedContext {
        name,
        status: BoundedContextStatus::Active,
        created_at: now,
        created_by: ContextCreator::SuperadminCreator {
            role: caller.clone(),
        },
    })
}

/// See `rule ListBoundedContexts`. Unrestricted across contexts and
/// statuses (see the note above the rule) - the caller check is the whole
/// of this function's own logic; `bounded_contexts` passes straight
/// through, since a superadmin already reaches every context by
/// construction.
pub fn list_bounded_contexts(
    caller: &Role,
    bounded_contexts: &[BoundedContext],
) -> crate::error::Result<Vec<BoundedContext>> {
    crate::access_control::require_active_superadmin(caller)?;
    Ok(bounded_contexts.to_vec())
}

/// See `rule ArchiveBoundedContext`. Reuses
/// `access_control::Error::{GrantNotActive,InsufficientAccessLevel,GrantBoundedContextMismatch}`
/// for the first three `requires` - same grant-shape checks
/// `revoke_token` makes, just against a `BoundedContext` instead of an
/// `AccessToken` - and `event_store::Error::BoundedContextArchived` for
/// "already archived", the same rejection every other rule in this
/// codebase uses for that state. `bounded_context != admin` is this
/// module's own `Error::CannotArchiveAdminContext` - see
/// `ADMIN_BOUNDED_CONTEXT_NAME`'s own doc comment for why identity is
/// name equality rather than a literal `admin` value to compare against.
pub fn archive_bounded_context(
    access_mapping: &RoleAccessMapping,
    bounded_context: &BoundedContext,
) -> crate::error::Result<BoundedContext> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if &access_mapping.bounded_context != bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if bounded_context.status != BoundedContextStatus::Active {
        return Err(crate::event_store::Error::BoundedContextArchived.into());
    }
    if bounded_context.name == ADMIN_BOUNDED_CONTEXT_NAME {
        return Err(Error::CannotArchiveAdminContext.into());
    }

    Ok(BoundedContext {
        status: BoundedContextStatus::Archived,
        ..bounded_context.clone()
    })
}
