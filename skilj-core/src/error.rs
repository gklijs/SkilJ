//! Two-tier error handling - see [docs/architecture.md §4](../../docs/architecture.md#error-handling).
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

    /// Only ever produced by `command_batcher::CommandBatcher` - a
    /// follower's own outcome when the batch leader that took its
    /// request couldn't produce one at all: the leader's own lock
    /// acquisition or final commit failed (in which case every command
    /// in that batch shares this, not just the leader's own error,
    /// since nothing in the batch is trustworthy once the shared
    /// transaction itself is in doubt - see `db::submit_command_batch`'s
    /// own doc comment), or the leader's task ended without ever sending
    /// a reply at all (a bug, not a real database error, but a follower
    /// still needs *some* `Result` to return). Carries the underlying
    /// failure's own rendered message - `crate::error::Error` isn't
    /// `Clone` (it wraps `sqlx::Error`, which isn't either), so this is
    /// the one case where the original typed error can't be handed to
    /// every affected caller directly.
    ///
    /// `code` is the *original* error's own [`SkiljRejection::code`], so a
    /// follower's machine-readable `extensions.code` matches what the
    /// leader's own typed error reports, rather than collapsing to a
    /// generic value; only the typed variant itself (and so its HTTP
    /// status mapping, always 500 here) is lost.
    #[error("command batch failed: {message}")]
    BatchFailed { code: String, message: String },

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

    /// Detail that must not reach a caller but belongs in the server's
    /// log: the raw database or migration error behind a generic
    /// [`message`](Self::message) (docs/architecture.md §93). The rendering
    /// layers log it, in the request's span, when they turn this rejection
    /// into a response. `None` for every business or access rejection,
    /// whose message is already meant for the caller.
    fn internal_detail(&self) -> Option<String> {
        None
    }
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
            Error::BatchFailed { code, .. } => code,
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
            // docs/architecture.md §93: never the raw `sqlx`/Postgres text,
            // which names schemas (bounded contexts, i.e. tenants),
            // constraints and SQL. The caller gets the code, this, and the
            // response's trace id; the detail goes to the server log.
            Error::Database(sqlx::Error::RowNotFound) => {
                "a record this request needed no longer exists".to_string()
            }
            Error::Database(sqlx::Error::PoolTimedOut) => {
                "the server's database connections are all busy - retry shortly".to_string()
            }
            Error::Database(_) => "an internal database error occurred".to_string(),
            Error::Migration(_) => "an internal database migration error occurred".to_string(),
            Error::BatchFailed { message, .. } => message.clone(),
        }
    }

    fn internal_detail(&self) -> Option<String> {
        match self {
            Error::Database(e) => Some(e.to_string()),
            Error::Migration(e) => Some(e.to_string()),
            _ => None,
        }
    }
}

impl Error {
    /// The follower-facing stand-in for `cause` - see [`Error::BatchFailed`].
    pub fn batch_failed(cause: &Error) -> Error {
        Error::BatchFailed {
            code: cause.code().to_string(),
            message: cause.message(),
        }
    }

    /// A batch failure that has no underlying `Error` to copy from
    /// (a cancelled or vanished leader).
    pub fn batch_failed_msg(message: impl Into<String>) -> Error {
        Error::BatchFailed {
            code: "database_error".to_string(),
            message: message.into(),
        }
    }

    /// A row a caller expected to still exist doesn't - the same shape
    /// `db::require_bounded_context`/`require_event_type`/
    /// `require_command_type`/`require_command` already produce for the
    /// identical situation inside this crate's own `db` module, exposed
    /// here so a caller outside it (`skilj-graphql`'s own
    /// `redrive_parked_delivery`, for one) can report the same outcome
    /// without needing `sqlx` as a direct dependency just for this.
    pub fn row_not_found() -> Error {
        Error::Database(sqlx::Error::RowNotFound)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
