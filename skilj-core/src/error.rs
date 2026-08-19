//! Two-tier error handling - see docs/architecture.md §4.
//!
//! Library-level errors are enumerable, one `thiserror` enum per
//! domain module (`access_control::Error`, `event_store::Error`,
//! `projections::Error`, `bootstrap::Error`), aggregated here.
//! `CommandRejected` is the other tier: a bounded context's own
//! `decide()` declined a command, which is a legitimate business
//! outcome, not a library failure - the spec deliberately keeps
//! `rejection_kind` an opaque `String` it never enumerates (see
//! `value CommandDecision`), so this variant is never narrowed further
//! either.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    AccessControl(#[from] crate::access_control::Error),

    #[error(transparent)]
    EventStore(#[from] crate::event_store::Error),

    #[error(transparent)]
    Projections(#[from] crate::projections::Error),

    #[error(transparent)]
    Bootstrap(#[from] crate::bootstrap::Error),

    #[error(transparent)]
    Encryption(#[from] crate::encryption::Error),

    #[error("command rejected: {reason}")]
    CommandRejected { reason: String, kind: String },

    /// Only ever produced by `db::submit_command`'s own DCB-conflict
    /// retry path (see its own doc comment) - the redispatch it runs
    /// under the sequence row's lock finds no decider registered for a
    /// `(bounded_context, command_type)` pair the caller's own first,
    /// optimistic `dispatch` call just found one for. Vanishingly
    /// unlikely in practice (plugin registration is fixed for a
    /// process's whole lifetime), but a real possible outcome of a
    /// second dispatch call, so a real variant rather than a panic.
    /// `skilj-rest`/`skilj-graphql` both already have their own
    /// first-dispatch-call equivalent of this message; kept identical
    /// here so the two paths read the same either way.
    #[error("this CommandType has no decide() registered in the running process")]
    NoDeciderRegistered,

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Distinct from `Database` above - a migration failure means the
    /// schema itself couldn't be brought up to date (a startup-time
    /// concern, raised by `Skilj::builder().build()` - see docs/
    /// architecture.md §1.5/§1.7), not that an otherwise-healthy schema
    /// rejected one query.
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
}

/// Implemented by every error tier so the eventual GraphQL/REST rendering
/// layer (`skilj-graphql`/`skilj-rest`) can treat "a library error" and "a
/// business rejection" uniformly, without needing to know which tier
/// produced a given error - see §4.2. `code()` is the stable,
/// machine-readable identifier (`extensions.code` on the wire, GraphQL or
/// REST alike - §5.4, §7.5); `message()` is the human-readable text.
pub trait SkiljRejection {
    fn code(&self) -> &str;
    fn message(&self) -> String;
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::AccessControl(e) => e.code(),
            Error::EventStore(e) => e.code(),
            Error::Projections(e) => e.code(),
            Error::Bootstrap(e) => e.code(),
            Error::Encryption(e) => e.code(),
            Error::CommandRejected { kind, .. } => kind,
            Error::NoDeciderRegistered => "no_decider_registered",
            Error::Database(_) => "database_error",
            Error::Migration(_) => "migration_error",
        }
    }

    fn message(&self) -> String {
        match self {
            Error::AccessControl(e) => e.message(),
            Error::EventStore(e) => e.message(),
            Error::Projections(e) => e.message(),
            Error::Bootstrap(e) => e.message(),
            Error::Encryption(e) => e.message(),
            Error::CommandRejected { reason, .. } => reason.clone(),
            Error::NoDeciderRegistered => self.to_string(),
            Error::Database(e) => e.to_string(),
            Error::Migration(e) => e.to_string(),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
