//! `Role`, `RoleAccessMapping`, `AccessToken` (and its four variants);
//! `CreateRole`, `RevokeRole`, `GrantRoleAccessMapping`,
//! `RevokeRoleAccessMapping`, `TokenRevocation`; actor resolution
//! (`ReadAccess`/`WriteAccess`/`AdminAccess`/`SensitiveDataAccess`/
//! `Superadmin`) and the JWT-to-Role identity resolution entry point. See
//! docs/architecture.md §3.2 and §6 (JWKS/JWT verification lives here,
//! behind the `jsonwebtoken` crate).

use crate::error::SkiljRejection;
use crate::event_store::BoundedContext;

// TODO: the base AccessToken shape (secret/created_at/revoked_at), actor
// resolution (ReadAccess/WriteAccess/AdminAccess/SensitiveDataAccess -
// `Superadmin` is a plain `&Role` with `superadmin = true, status =
// active`, already enough for every function below), the JWT-to-Role
// identity resolution entry point, and TokenRevocation's own rule.
// `EventReadToken`/`ExternalEventToken`/`DirectCreationToken`/
// `CommandToken`/`Role`/`RoleAccessMapping` below are first, partial cuts
// - see their own doc comments - added while propagating tests for the
// EventFetch/ExternalEventIngestion/DirectEventCreation/CommandTrigger/
// AccessManagement surfaces (docs/architecture.md §9's pilot and its
// follow-ups).

/// See `entity Role`'s `status` field/transition graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleStatus {
    Active,
    Revoked,
}

/// See `entity Role`. `access_mappings` (a relationship projection, not a
/// stored field - the same treatment `EventType`'s `*_tokens` get) is
/// omitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    pub id: String,
    pub external_subject: String,
    pub name: String,
    pub superadmin: bool,
    pub status: RoleStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// See `enum AccessLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessLevel {
    Read,
    Write,
    Admin,
}

/// See `entity RoleAccessMapping`. Reuses `RoleStatus` for its own
/// `status` - the spec declares both as the identical `active | revoked`
/// shape, and nothing here needs to tell "a revoked Role" apart from "a
/// revoked grant" by type alone (each already lives on its own struct).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleAccessMapping {
    pub role: Role,
    pub bounded_context: BoundedContext,
    pub level: AccessLevel,
    pub can_read_sensitive: bool,
    pub status: RoleStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// See `entity AccessToken`'s `status` field/transition graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenStatus {
    Active,
    Revoked,
}

/// See `variant EventReadToken` in the spec. Only the fields the
/// `event_store` module's `FetchEvents`/`ConsumeEvents`/
/// `AcknowledgeEvents` touch are here - `id`/`secret`/`created_at`/
/// `revoked_at` belong to `TypeRegistration`/`TokenRevocation`'s own
/// obligations, not yet propagated. Unlike `ExternalEventToken`/
/// `DirectCreationToken` below, `id` isn't needed here: nothing in
/// EventFetch's rules reads it (it never becomes a `Metadata.client_id`,
/// unlike a write).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventReadToken {
    pub status: TokenStatus,
    pub event_type: crate::event_store::EventType,
}

/// See `variant ExternalEventToken`. `id` is here (unlike
/// `EventReadToken` above) because `CreateExternalEvent` stamps it into
/// `Metadata.client_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEventToken {
    pub id: String,
    pub status: TokenStatus,
    pub event_type: crate::event_store::EventType,
}

/// See `variant DirectCreationToken`. Same shape and reasoning as
/// `ExternalEventToken` above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectCreationToken {
    pub id: String,
    pub status: TokenStatus,
    pub event_type: crate::event_store::EventType,
}

/// See `variant CommandToken`. Same shape and reasoning as
/// `ExternalEventToken`/`DirectCreationToken` above, scoped to a
/// `CommandType` instead of an `EventType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandToken {
    pub id: String,
    pub status: TokenStatus,
    pub command_type: crate::event_store::CommandType,
}

/// Library-level errors this module's own rules reject for - an
/// enumerable, closed set, unlike a bounded context's own
/// `CommandDecision::Rejected` (see crate::error and
/// docs/architecture.md §4.1).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("this Role is not active")]
    RoleNotActive,

    #[error("this RoleAccessMapping is not active")]
    GrantNotActive,

    #[error("this grant's access level is too low for this action")]
    InsufficientAccessLevel,

    #[error("this AccessToken is not active")]
    TokenNotActive,

    #[error("only an active superadmin Role may perform this action")]
    NotSuperadmin,

    #[error("no active Role's external_subject matches this JWT's subject claim")]
    UnrecognisedSubject,

    #[error("this external_subject is already claimed by another active Role")]
    ExternalSubjectAlreadyClaimed,

    #[error("this Role already holds an active RoleAccessMapping for this bounded context")]
    DuplicateActiveMapping,
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::RoleNotActive => "role_not_active",
            Error::GrantNotActive => "grant_not_active",
            Error::InsufficientAccessLevel => "insufficient_access_level",
            Error::TokenNotActive => "token_not_active",
            Error::NotSuperadmin => "not_superadmin",
            Error::UnrecognisedSubject => "unrecognised_subject",
            Error::ExternalSubjectAlreadyClaimed => "external_subject_already_claimed",
            Error::DuplicateActiveMapping => "duplicate_active_mapping",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

fn require_active_superadmin(caller: &Role) -> crate::error::Result<()> {
    if caller.status != RoleStatus::Active {
        return Err(Error::RoleNotActive.into());
    }
    if !caller.superadmin {
        return Err(Error::NotSuperadmin.into());
    }
    Ok(())
}

/// See `rule CreateRole`. `id` is the value the caller's own
/// `generate_token_id()` already produced - not this function's to
/// generate, the same caller-supplied-black-box-output treatment
/// `next_sequence` gets elsewhere in this codebase (`generate_token_id`
/// is still `// TODO` itself - see `crate::shared`). `existing_roles` is
/// every `Role` this engine currently knows of, for the
/// `UniqueActiveExternalSubject` check - a full-snapshot parameter, the
/// same shape `consistency_boundary_and_matching_events` takes `Event`s
/// in, since there's no single key to look this existential check up by.
pub fn create_role(
    caller: &Role,
    name: String,
    superadmin: bool,
    external_subject: String,
    existing_roles: &[Role],
    id: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<Role> {
    require_active_superadmin(caller)?;
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
        superadmin,
        status: RoleStatus::Active,
        created_at: now,
        revoked_at: None,
    })
}

/// See `rule RevokeRole`. `active_mappings` is `role.access_mappings
/// where status = active` as already looked up by the caller - the same
/// "relationship projection resolved by the caller" treatment
/// `Role.access_mappings` gets everywhere else (see the doc comment on
/// `Role`). Returns the revoked `Role` and every mapping it just
/// cascaded the revocation to - together they *are*
/// `RevokedRoleImpliesMappingsRevoked`, established in the same
/// transaction rather than checked after the fact.
pub fn revoke_role(
    caller: &Role,
    role: &Role,
    active_mappings: &[RoleAccessMapping],
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<(Role, Vec<RoleAccessMapping>)> {
    require_active_superadmin(caller)?;
    if role.status != RoleStatus::Active {
        return Err(Error::RoleNotActive.into());
    }

    let revoked_role = Role {
        status: RoleStatus::Revoked,
        revoked_at: Some(now),
        ..role.clone()
    };
    let revoked_mappings = active_mappings
        .iter()
        .cloned()
        .map(|m| RoleAccessMapping {
            status: RoleStatus::Revoked,
            revoked_at: Some(now),
            ..m
        })
        .collect();

    Ok((revoked_role, revoked_mappings))
}

/// See `rule GrantRoleAccessMapping`. `existing_mappings` is every
/// `RoleAccessMapping` this engine currently knows of, for the `not
/// exists RoleAccessMapping{role, bounded_context, status: active}`
/// check - same full-snapshot treatment as `create_role`'s
/// `existing_roles`.
pub fn grant_role_access_mapping(
    caller: &Role,
    role: &Role,
    bounded_context: &BoundedContext,
    level: AccessLevel,
    can_read_sensitive: bool,
    existing_mappings: &[RoleAccessMapping],
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<RoleAccessMapping> {
    require_active_superadmin(caller)?;
    if role.status != RoleStatus::Active {
        return Err(Error::RoleNotActive.into());
    }
    if bounded_context.status != crate::event_store::BoundedContextStatus::Active {
        return Err(crate::event_store::Error::BoundedContextArchived.into());
    }
    if existing_mappings.iter().any(|m| {
        &m.role == role && &m.bounded_context == bounded_context && m.status == RoleStatus::Active
    }) {
        return Err(Error::DuplicateActiveMapping.into());
    }

    Ok(RoleAccessMapping {
        role: role.clone(),
        bounded_context: bounded_context.clone(),
        level,
        can_read_sensitive,
        status: RoleStatus::Active,
        created_at: now,
        revoked_at: None,
    })
}

/// See `rule RevokeRoleAccessMapping`.
pub fn revoke_role_access_mapping(
    caller: &Role,
    access_mapping: &RoleAccessMapping,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<RoleAccessMapping> {
    require_active_superadmin(caller)?;
    if access_mapping.status != RoleStatus::Active {
        return Err(Error::GrantNotActive.into());
    }

    Ok(RoleAccessMapping {
        status: RoleStatus::Revoked,
        revoked_at: Some(now),
        ..access_mapping.clone()
    })
}
