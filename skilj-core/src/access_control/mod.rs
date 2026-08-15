//! `Role`, `RoleAccessMapping`, `AccessToken` (and its four variants);
//! `CreateRole`, `RevokeRole`, `GrantRoleAccessMapping`,
//! `RevokeRoleAccessMapping`, `TokenRevocation`; actor resolution
//! (`ReadAccess`/`WriteAccess`/`AdminAccess`/`SensitiveDataAccess`/
//! `Superadmin`) and the JWT-to-Role identity resolution entry point. See
//! docs/architecture.md §3.2 and §6 (JWKS/JWT verification lives here,
//! behind the `jsonwebtoken` crate).

use crate::error::SkiljRejection;

// TODO: entity Role, entity RoleAccessMapping, entity AccessToken (+ its
// four variants: ExternalEventToken, DirectCreationToken, CommandToken,
// EventReadToken), enum AccessLevel, actor resolution
// (ReadAccess/WriteAccess/AdminAccess/SensitiveDataAccess/Superadmin),
// and the rules this module owns (CreateRole, RevokeRole,
// GrantRoleAccessMapping, RevokeRoleAccessMapping, the token rules).

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
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}
