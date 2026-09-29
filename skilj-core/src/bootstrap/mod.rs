//! `BootstrapSecret`, `ContextCreator`/`SystemCreator`; `CreateSuperadmin`,
//! `AddBoundedContext`, `ArchiveBoundedContext`, `ListBoundedContexts`;
//! the startup reconciliation loop (§1.5 - the reconciliation-loop entry
//! point and its authentication as a real, pre-existing Role both live
//! here). See docs/architecture.md §3.2.

use crate::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use crate::error::SkiljRejection;
use crate::event_store::{BoundedContext, BoundedContextStatus};

/// The literal half of `default BoundedContext admin` - the one bounded
/// context `AddBoundedContext`/`ArchiveBoundedContext` never create or
/// archive (see `add_bounded_context`/`archive_bounded_context`'s own doc
/// comments). Its `created_at`/`created_by` are real recorded values,
/// stamped once at SkilJ's own first startup - see `stamp_admin_bounded_context`
/// below, not a static constant here.
pub const ADMIN_BOUNDED_CONTEXT_NAME: &str = "admin";

/// See the note above `default BoundedContext admin` in specs/skilj.allium:
/// "both are written by SkilJ itself, at its own first startup... stamped
/// only once: unlike the bootstrap secret, which is regenerated on every
/// startup until it is claimed, this acts only while the values are
/// unset, so a later restart never restamps them." `existing` is
/// `BoundedContext{name: admin}` as already looked up by the caller
/// (`Skilj::builder().build()`, per §1.5) - the same get-or-create lookup
/// treatment `register_event_type`'s own `existing` gets. `None` on every
/// startup but the process's genuine first one against this database;
/// this function does nothing on every later one, the same "acts only
/// while unset" guarantee the spec's own text describes. Unlike
/// `generate_bootstrap_secret`, this doesn't depend on whether a
/// superadmin exists yet - the spec is explicit that the two are
/// unrelated ("It is not part of, and does not wait for, the arrival of
/// the first superadmin Role").
pub fn stamp_admin_bounded_context(
    existing: Option<&BoundedContext>,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<BoundedContext> {
    if existing.is_some() {
        return None;
    }
    Some(BoundedContext {
        name: ADMIN_BOUNDED_CONTEXT_NAME.to_string(),
        status: BoundedContextStatus::Active,
        created_at: now,
        created_by: ContextCreator::SystemCreator,
        template: None,
    })
}

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

    #[error(
        "bounded context names must start with a lowercase letter, contain only lowercase \
         letters, digits and underscores, and be at most 40 characters long"
    )]
    InvalidBoundedContextName,

    #[error("the admin bounded context can never be archived")]
    CannotArchiveAdminContext,

    #[error("a bounded context must be archived before it can be deleted")]
    BoundedContextNotArchived,

    #[error("the admin bounded context can never be deleted")]
    CannotDeleteAdminContext,

    #[error("this external_subject is already bound to another active Role")]
    ExternalSubjectAlreadyClaimed,

    #[error("a BoundedContext used as a template must have no template of its own")]
    TemplateItselfTemplated,

    #[error("this bounded context has no template to resync from")]
    BoundedContextHasNoTemplate,
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::SuperadminAlreadyExists => "superadmin_already_exists",
            Error::BootstrapSecretMismatch => "bootstrap_secret_mismatch",
            Error::BoundedContextNameTaken => "bounded_context_name_taken",
            Error::InvalidBoundedContextName => "invalid_bounded_context_name",
            Error::CannotArchiveAdminContext => "cannot_archive_admin_context",
            Error::BoundedContextNotArchived => "bounded_context_not_archived",
            Error::CannotDeleteAdminContext => "cannot_delete_admin_context",
            Error::ExternalSubjectAlreadyClaimed => "external_subject_already_claimed",
            Error::TemplateItselfTemplated => "template_itself_templated",
            Error::BoundedContextHasNoTemplate => "bounded_context_has_no_template",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

/// `entity BootstrapSecret`'s own generation, run once at process start:
/// "generated fresh at each process start for as long as no active
/// superadmin Role exists". `None` is the `ClosesPermanentlyOnFirstClaim`
/// case (see `surface SuperadminBootstrap`) - once an active superadmin
/// exists, there is nothing left to generate or print, since
/// `CreateSuperadmin` itself can never fire again regardless. Built on
/// `crate::shared::generate_token_secret` - "how it is generated is a
/// black box, the same register as `generate_token_secret`" per the
/// entity's own doc comment, so this reuses that black box rather than
/// inventing a second one.
/// This process's bootstrap secret, shared by everything that serves
/// `createSuperadmin` (docs/architecture.md §109). `entity BootstrapSecret`:
/// held in this process's memory only, and it "ends either at the next
/// restart or at the moment the first superadmin exists" - so a claim
/// consumes it (`take`) rather than leaving it to be refused only while an
/// active superadmin exists. Otherwise revoking every superadmin later made
/// the originally printed secret, perhaps days old in shipped logs, valid
/// again on every process that hadn't restarted since. A restart while no
/// active superadmin exists generates a fresh one - the spec's recovery
/// path. The lock is held for a whole claim, so two claims on one process
/// can't both use the secret.
#[derive(Clone, Default)]
pub struct BootstrapGate(std::sync::Arc<tokio::sync::Mutex<Option<BootstrapSecret>>>);

impl BootstrapGate {
    pub fn new(secret: Option<BootstrapSecret>) -> Self {
        Self(std::sync::Arc::new(tokio::sync::Mutex::new(secret)))
    }

    /// Held for a whole `createSuperadmin` claim; set it to `None` once
    /// the claim has succeeded.
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, Option<BootstrapSecret>> {
        self.0.lock().await
    }

    /// The secret, if this process still holds one and no claim is in
    /// progress right now.
    pub fn current(&self) -> Option<String> {
        self.0
            .try_lock()
            .ok()
            .and_then(|secret| secret.as_ref().map(|s| s.secret.clone()))
    }
}

pub fn generate_bootstrap_secret(existing_roles: &[Role]) -> Option<BootstrapSecret> {
    if existing_roles
        .iter()
        .any(|r| r.superadmin && r.status == RoleStatus::Active)
    {
        return None;
    }
    Some(BootstrapSecret {
        secret: crate::shared::generate_token_secret(),
    })
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

/// `valid_bounded_context_name(name)` (`rule AddBoundedContext`'s own
/// note above its `requires`): lowercase letters, digits and underscores
/// only, starting with a letter, at most 40 characters - short enough to
/// leave room for the `bc_` prefix `db::schema_ident` adds under
/// Postgres's 63-byte identifier limit. A name is carried through
/// verbatim as a physical Postgres schema name (see docs/architecture.md
/// §2.2.2), so this is checked here rather than left to fail later,
/// less legibly, inside `CREATE SCHEMA`.
fn valid_bounded_context_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 40 {
        return false;
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
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
    if !valid_bounded_context_name(&name) {
        return Err(Error::InvalidBoundedContextName.into());
    }

    Ok(BoundedContext {
        name,
        status: BoundedContextStatus::Active,
        created_at: now,
        created_by: ContextCreator::SuperadminCreator {
            role: caller.clone(),
        },
        template: None,
    })
}

/// See `rule CreateBoundedContextFromTemplate` (Codeberg issue #13).
/// Same shape as `add_bounded_context` immediately above - identical
/// name-uniqueness/validity checks, since this is the spec's own second
/// and only other way a `BoundedContext` name is chosen - plus the two
/// checks specific to templating: `template` must be `active` (an
/// archived one's declared types are a frozen record, not a moving
/// statement of what a tenant should have) and must itself have no
/// `template` (invariant `TemplateIsNeverItselfTemplated` - one level
/// only, enforced here as well as by the invariant since this is the
/// only rule that ever sets the field).
///
/// Only decides the new `BoundedContext` itself. The caller (the
/// `skilj-graphql` resolver) still has to separately call
/// `access_control::grant_role_access_mapping` (the combined
/// create-and-grant step this rule's own `@guarantee
/// AccessGrantedWithCreation` describes) and then apply the template's
/// current type registrations - see this module's own doc comment on
/// why that fan-out isn't a single function here: three genuinely
/// different `register_*` pure functions across three modules
/// (`event_store`/`projections`) are involved, the same
/// `RegisterEventType`/`RegisterCommandType`/`RegisterProjection` split
/// `skilj/src/lib.rs`'s own `reconcile_*` functions already keep apart
/// rather than collapsing into one generic.
pub fn create_bounded_context_from_template(
    caller: &Role,
    template: &BoundedContext,
    name: String,
    existing_contexts: &[BoundedContext],
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<BoundedContext> {
    crate::access_control::require_active_superadmin(caller)?;
    if existing_contexts.iter().any(|bc| bc.name == name) {
        return Err(Error::BoundedContextNameTaken.into());
    }
    if !valid_bounded_context_name(&name) {
        return Err(Error::InvalidBoundedContextName.into());
    }
    if template.status != BoundedContextStatus::Active {
        return Err(crate::event_store::Error::BoundedContextArchived.into());
    }
    if template.template.is_some() {
        return Err(Error::TemplateItselfTemplated.into());
    }

    Ok(BoundedContext {
        name,
        status: BoundedContextStatus::Active,
        created_at: now,
        created_by: ContextCreator::SuperadminCreator {
            role: caller.clone(),
        },
        template: Some(Box::new(template.clone())),
    })
}

/// See `rule ResyncBoundedContextFromTemplate` (Codeberg issue #13).
/// Pure validation only, returning the template to resync *from* on
/// success - applying its current registrations is the caller's own
/// loop, the identical one `create_bounded_context_from_template`'s own
/// doc comment describes. Safe to call as often as wanted: every
/// registration the caller applies afterward is already the same
/// upsert-shaped call a deploy-time reconciliation pass makes, so
/// resyncing a tenant already in step changes nothing.
pub fn resync_bounded_context_from_template<'a>(
    caller: &Role,
    bounded_context: &'a BoundedContext,
) -> crate::error::Result<&'a BoundedContext> {
    crate::access_control::require_active_superadmin(caller)?;
    if bounded_context.status != BoundedContextStatus::Active {
        return Err(crate::event_store::Error::BoundedContextArchived.into());
    }
    let Some(template) = bounded_context.template.as_deref() else {
        return Err(Error::BoundedContextHasNoTemplate.into());
    };
    if template.status != BoundedContextStatus::Active {
        return Err(crate::event_store::Error::BoundedContextArchived.into());
    }
    Ok(template)
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

/// See `rule DeleteBoundedContext`. Superadmin-gated like
/// `add_bounded_context` (a deliberately higher bar than
/// `archive_bounded_context`'s per-context admin grant - see
/// `surface BoundedContextDeletion`'s own `@guarantee SuperadminOnly`),
/// not a per-context `RoleAccessMapping`. `bounded_context.status =
/// archived` must already hold - deleting is the separate, deliberate
/// second act after archiving stops work, not a shortcut around it (see
/// the note above the rule for why an ungranted context can never reach
/// either state). `bounded_context != admin` is this module's own
/// `Error::CannotDeleteAdminContext`, mirroring
/// `archive_bounded_context`'s identical guard.
///
/// Only decides *whether* the deletion is allowed - the caller runs
/// `db::hard_delete_bounded_context` only after this returns `Ok`, the
/// same pure-decision/impure-effect split every rule in this crate keeps.
pub fn delete_bounded_context(
    caller: &Role,
    bounded_context: &BoundedContext,
) -> crate::error::Result<()> {
    crate::access_control::require_active_superadmin(caller)?;
    if bounded_context.status != BoundedContextStatus::Archived {
        return Err(Error::BoundedContextNotArchived.into());
    }
    if bounded_context.name == ADMIN_BOUNDED_CONTEXT_NAME {
        return Err(Error::CannotDeleteAdminContext.into());
    }
    Ok(())
}
