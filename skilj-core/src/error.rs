//! One error type for every rule in the spec.
//!
//! The spec distinguishes a handful of outcomes that callers must be able to
//! tell apart (a timeout from a permanent refusal, "invisible" from
//! "forbidden", a malformed request from a domain rejection). Each gets its
//! own variant so the GraphQL and REST layers can map them without parsing
//! strings.

use thiserror::Error;

/// Result alias used across the crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    /// The thing named does not exist, *or* exists but the caller may not
    /// know it does. The spec deliberately collapses both ("invisible, not
    /// merely unreadable").
    #[error("not found: {0}")]
    NotFound(String),

    /// The caller is known and the target visible, but the caller's grant or
    /// token does not authorise this action.
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// A malformed request: bad payload, bad filter, bad name, incoherent
    /// argument combination. Retrying the same request will not help.
    #[error("invalid: {0}")]
    Invalid(String),

    /// The request conflicts with current state (name taken, nothing staged,
    /// already revoked, ...).
    #[error("conflict: {0}")]
    Conflict(String),

    /// decide() rejected the command. Both halves are always present.
    #[error("rejected ({kind}): {reason}")]
    Rejected { reason: String, kind: String },

    /// A read-your-writes wait ran out. Retryable.
    #[error("timed out waiting for sequence {0}")]
    Timeout(i64),

    /// The bounded context is archived and the action writes.
    #[error("bounded context {0} is archived")]
    Archived(String),

    /// A position was handed back from a different database epoch.
    #[error("epoch mismatch: position was read in another epoch")]
    EpochMismatch,

    /// This process does not carry code another process registered
    /// (rolling deploy): e.g. a sync projection it cannot fold.
    #[error("not declared in this process: {0}")]
    NotDeclared(String),

    /// Configuration or startup refused.
    #[error("configuration: {0}")]
    Config(String),

    #[error("crypto: {0}")]
    Crypto(String),

    #[error("database: {0}")]
    Database(#[from] sqlx::Error),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

impl Error {
    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::Invalid(msg.into())
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Error::NotFound(msg.into())
    }
    pub fn forbidden(msg: impl Into<String>) -> Self {
        Error::Forbidden(msg.into())
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Error::Conflict(msg.into())
    }

    /// A stable machine-readable code for the wire layers.
    pub fn code(&self) -> &'static str {
        match self {
            Error::NotFound(_) => "NOT_FOUND",
            Error::Forbidden(_) => "FORBIDDEN",
            Error::Invalid(_) => "INVALID",
            Error::Conflict(_) => "CONFLICT",
            Error::Rejected { .. } => "REJECTED",
            Error::Timeout(_) => "TIMEOUT",
            Error::Archived(_) => "ARCHIVED",
            Error::EpochMismatch => "EPOCH_MISMATCH",
            Error::NotDeclared(_) => "NOT_DECLARED",
            Error::Config(_) => "CONFIG",
            Error::Crypto(_) => "CRYPTO",
            Error::Database(_) => "INTERNAL",
            Error::Json(_) => "INVALID",
        }
    }
}
