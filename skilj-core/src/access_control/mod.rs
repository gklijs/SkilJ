//! `Role`, `RoleAccessMapping`, `AccessToken` (and its four variants);
//! `CreateRole`, `RevokeRole`, `GrantRoleAccessMapping`,
//! `RevokeRoleAccessMapping`, `TokenRevocation`; actor resolution
//! (`ReadAccess`/`WriteAccess`/`AdminAccess`/`SensitiveDataAccess`/
//! `Superadmin`) and the JWT-to-Role identity resolution entry point. See
//! docs/architecture.md §3.2 and §6 (JWKS/JWT verification lives here,
//! behind the `jsonwebtoken` crate).

use crate::error::SkiljRejection;
use crate::event_store::BoundedContext;

// TODO: real JWKS fetching and JWT signature verification (step 1 of the
// identity resolution note above `entity Role`) - genuine I/O and a
// black box per the spec's own text, so it doesn't fit this crate's
// pure-function register the way `resolve_role_by_external_subject`
// below (step 3, the one pure part of that pipeline) does. The four
// actors (`ReadAccess`/`WriteAccess`/`AdminAccess`/`SensitiveDataAccess`)
// and `Superadmin` have no resolution function of their own to add
// beyond that: each is a predicate over an already-resolved
// `RoleAccessMapping`/`Role` (see `actor ReadAccess` etc. in the spec),
// and every rule elsewhere in this crate that faces one already inlines
// exactly that predicate as its own `requires` checks - there is no
// separate obligation for the actor declarations themselves to
// propagate, only each surface's own still-deferred `surface-actor`
// obligation (the GraphQL-scaffolding gap every test file in this crate
// already defers for the same reason).
// `EventReadToken`/`ExternalEventToken`/`DirectCreationToken`/
// `CommandToken`/`Role`/`RoleAccessMapping` below are first, partial cuts
// - see their own doc comments - added while propagating tests for the
// EventFetch/ExternalEventIngestion/DirectEventCreation/CommandTrigger/
// AccessManagement surfaces (docs/architecture.md §9's pilot and its
// follow-ups). The base `AccessToken` shape (`secret`/`created_at`/
// `revoked_at`, plus `id` on `EventReadToken`) and the `AccessToken` sum
// type itself, `create_external_event_token`/`create_direct_creation_token`/
// `create_command_token`/`create_event_read_token`, and `revoke_token` were
// added next, propagating EventTypeAdminOperations/
// CommandTypeAdminOperations/TokenRevocation - the token lifecycle's
// minting and revocation halves, completing `AccessToken`'s entity shape.
// `resolve_role_by_external_subject` was added last - the one pure
// fragment of JWT-to-Role identity resolution (see its own doc comment
// for why the JWT/JWKS verification itself stays unmodelled).

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

/// See `variant EventReadToken` in the spec. Now carries the full
/// `entity AccessToken` base shape - `id` included, promoted from the
/// earlier EventFetch-only cut (nothing in EventFetch's own rules reads
/// it, but `RevokeToken` and its `AccessToken` sum type below need every
/// variant to carry the same base fields uniformly).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventReadToken {
    pub id: String,
    pub secret: String,
    pub status: TokenStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub event_type: crate::event_store::EventType,
}

/// See `variant ExternalEventToken`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEventToken {
    pub id: String,
    pub secret: String,
    pub status: TokenStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub event_type: crate::event_store::EventType,
}

/// See `variant DirectCreationToken`. Same shape as `ExternalEventToken`
/// above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectCreationToken {
    pub id: String,
    pub secret: String,
    pub status: TokenStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub event_type: crate::event_store::EventType,
}

/// See `variant CommandToken`. Same shape as `ExternalEventToken`/
/// `DirectCreationToken` above, scoped to a `CommandType` instead of an
/// `EventType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandToken {
    pub id: String,
    pub secret: String,
    pub status: TokenStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub command_type: crate::event_store::CommandType,
}

/// See `entity AccessToken`'s `purpose` field - a Rust sum type over the
/// four variants stands in for it directly, the same "the enum variant
/// tag is the purpose" treatment `Event.origin`/`EventOrigin` gets, so
/// there is no separate `purpose` field to carry: matching on this enum
/// already tells the four apart. Exists for `RevokeToken`, the one rule
/// that operates on "any `AccessToken`" polymorphically rather than on one
/// concrete variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessToken {
    ExternalEventToken(ExternalEventToken),
    DirectCreationToken(DirectCreationToken),
    CommandToken(CommandToken),
    EventReadToken(EventReadToken),
}

impl AccessToken {
    fn status(&self) -> TokenStatus {
        match self {
            AccessToken::ExternalEventToken(t) => t.status,
            AccessToken::DirectCreationToken(t) => t.status,
            AccessToken::CommandToken(t) => t.status,
            AccessToken::EventReadToken(t) => t.status,
        }
    }

    /// `token_scope` from `rule RevokeToken`: `token.command_type.bounded_context`
    /// for a `CommandToken`, `token.event_type.bounded_context` for the
    /// other three.
    fn scope(&self) -> &BoundedContext {
        match self {
            AccessToken::ExternalEventToken(t) => &t.event_type.bounded_context,
            AccessToken::DirectCreationToken(t) => &t.event_type.bounded_context,
            AccessToken::CommandToken(t) => &t.command_type.bounded_context,
            AccessToken::EventReadToken(t) => &t.event_type.bounded_context,
        }
    }
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

    #[error("this RoleAccessMapping is scoped to a different bounded context")]
    GrantBoundedContextMismatch,

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
            Error::GrantBoundedContextMismatch => "grant_bounded_context_mismatch",
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

/// `pub(crate)`: reused by `bootstrap::add_bounded_context`/
/// `bootstrap::list_bounded_contexts` (both `facing caller: Superadmin`,
/// the same actor every rule in this module already checks this way).
pub(crate) fn require_active_superadmin(caller: &Role) -> crate::error::Result<()> {
    if caller.status != RoleStatus::Active {
        return Err(Error::RoleNotActive.into());
    }
    if !caller.superadmin {
        return Err(Error::NotSuperadmin.into());
    }
    Ok(())
}

/// See the GraphQL identity resolution note above `entity Role`, step 3:
/// "That claim value resolves to the one active Role whose
/// external_subject matches it". The one pure part of the JWT-to-Role
/// resolution pipeline - step 1, verifying the presented JWT's signature
/// against the trusted IdP's JWKS, is a black box this engine doesn't own
/// ("a black box here, the same register as `protect_sensitive_fields`
/// and `generate_token_secret`" per the spec's own text), so it stays a
/// caller-supplied `verified_subject: &str` rather than something this
/// function fetches or verifies itself - the caller has already done
/// that and is handing over the JWT's trusted subject claim, nothing
/// else from it (step 2: "SkilJ trusts the subject claim of a verified
/// JWT, and nothing else in it"). `existing_roles` is every `Role` this
/// engine currently knows of, the same full-snapshot treatment
/// `create_role`'s own `existing_roles` gets below.
/// `UniqueActiveExternalSubject` is what makes "the one" well defined -
/// at most one active `Role` can ever match.
pub fn resolve_role_by_external_subject<'a>(
    verified_subject: &str,
    existing_roles: &'a [Role],
) -> crate::error::Result<&'a Role> {
    existing_roles
        .iter()
        .find(|r| r.status == RoleStatus::Active && r.external_subject == verified_subject)
        .ok_or_else(|| Error::UnrecognisedSubject.into())
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

/// `access_mapping.status = active`/`access_mapping.level = admin` -
/// shared by every `Create*Token` rule below and by `revoke_token`, all
/// four of which require this exact pair (see
/// `EventTypeAdminOperations`/`CommandTypeAdminOperations`/
/// `TokenRevocation`'s own `facing access_mapping: AdminAccess`).
fn require_active_admin(access_mapping: &RoleAccessMapping) -> crate::error::Result<()> {
    if access_mapping.status != RoleStatus::Active {
        return Err(Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(Error::InsufficientAccessLevel.into());
    }
    Ok(())
}

/// See `rule CreateExternalEventToken`. `id`/`secret` are the caller's own
/// `generate_token_id()`/`generate_token_secret()` output - not this
/// function's to generate, the same treatment `create_role`'s `id` gets
/// (see its own doc comment; `generate_token_secret` is still `// TODO` -
/// see `crate::shared`).
pub fn create_external_event_token(
    access_mapping: &RoleAccessMapping,
    event_type: &crate::event_store::EventType,
    id: String,
    secret: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<ExternalEventToken> {
    require_active_admin(access_mapping)?;
    if access_mapping.bounded_context != event_type.bounded_context {
        return Err(Error::GrantBoundedContextMismatch.into());
    }

    Ok(ExternalEventToken {
        id,
        secret,
        status: TokenStatus::Active,
        created_at: now,
        revoked_at: None,
        event_type: event_type.clone(),
    })
}

/// See `rule CreateDirectCreationToken`. Same shape and reasoning as
/// `create_external_event_token` above.
pub fn create_direct_creation_token(
    access_mapping: &RoleAccessMapping,
    event_type: &crate::event_store::EventType,
    id: String,
    secret: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<DirectCreationToken> {
    require_active_admin(access_mapping)?;
    if access_mapping.bounded_context != event_type.bounded_context {
        return Err(Error::GrantBoundedContextMismatch.into());
    }

    Ok(DirectCreationToken {
        id,
        secret,
        status: TokenStatus::Active,
        created_at: now,
        revoked_at: None,
        event_type: event_type.clone(),
    })
}

/// See `rule CreateEventReadToken`. Same shape and reasoning as
/// `create_external_event_token` above.
pub fn create_event_read_token(
    access_mapping: &RoleAccessMapping,
    event_type: &crate::event_store::EventType,
    id: String,
    secret: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<EventReadToken> {
    require_active_admin(access_mapping)?;
    if access_mapping.bounded_context != event_type.bounded_context {
        return Err(Error::GrantBoundedContextMismatch.into());
    }

    Ok(EventReadToken {
        id,
        secret,
        status: TokenStatus::Active,
        created_at: now,
        revoked_at: None,
        event_type: event_type.clone(),
    })
}

/// See `rule CreateCommandToken`. Same shape and reasoning as
/// `create_external_event_token` above, scoped to a `CommandType` instead
/// of an `EventType`.
pub fn create_command_token(
    access_mapping: &RoleAccessMapping,
    command_type: &crate::event_store::CommandType,
    id: String,
    secret: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<CommandToken> {
    require_active_admin(access_mapping)?;
    if access_mapping.bounded_context != command_type.bounded_context {
        return Err(Error::GrantBoundedContextMismatch.into());
    }

    Ok(CommandToken {
        id,
        secret,
        status: TokenStatus::Active,
        created_at: now,
        revoked_at: None,
        command_type: command_type.clone(),
    })
}

/// See `rule RevokeToken`. One function for all four `AccessToken`
/// variants, matching the spec's own "one revocation rule for every
/// variant" framing - `token_scope` is `AccessToken::scope`, the only
/// place the variants differ here.
pub fn revoke_token(
    access_mapping: &RoleAccessMapping,
    token: &AccessToken,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<AccessToken> {
    require_active_admin(access_mapping)?;
    if &access_mapping.bounded_context != token.scope() {
        return Err(Error::GrantBoundedContextMismatch.into());
    }
    if token.status() != TokenStatus::Active {
        return Err(Error::TokenNotActive.into());
    }

    Ok(match token.clone() {
        AccessToken::ExternalEventToken(t) => AccessToken::ExternalEventToken(ExternalEventToken {
            status: TokenStatus::Revoked,
            revoked_at: Some(now),
            ..t
        }),
        AccessToken::DirectCreationToken(t) => {
            AccessToken::DirectCreationToken(DirectCreationToken {
                status: TokenStatus::Revoked,
                revoked_at: Some(now),
                ..t
            })
        }
        AccessToken::CommandToken(t) => AccessToken::CommandToken(CommandToken {
            status: TokenStatus::Revoked,
            revoked_at: Some(now),
            ..t
        }),
        AccessToken::EventReadToken(t) => AccessToken::EventReadToken(EventReadToken {
            status: TokenStatus::Revoked,
            revoked_at: Some(now),
            ..t
        }),
    })
}
