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

/// Every library-level failure `skilj-core` reports, and a command a
/// `decide()` rejected. [`SkiljRejection::code`] gives each a stable code.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// A grant, token or role refused the operation.
    #[error(transparent)]
    AccessControl(#[from] crate::access_control::Error),

    /// A registration, event or command broke an event-store rule.
    #[error(transparent)]
    EventStore(#[from] crate::event_store::Error),

    /// A projection registration, rebuild or query was refused.
    #[error(transparent)]
    Projections(#[from] crate::projections::Error),

    /// Creating a bounded context or the first superadmin was refused.
    #[error(transparent)]
    Bootstrap(#[from] crate::bootstrap::Error),

    /// Encrypting or decrypting a sensitive field failed.
    #[error(transparent)]
    Encryption(#[from] crate::encryption::Error),

    /// A blind tag could not be derived or checked.
    #[error(transparent)]
    Blind(#[from] crate::blind::Error),

    /// A secret operation was refused.
    #[error(transparent)]
    Secrets(#[from] crate::secrets::Error),

    /// A command proposal could not be approved or rejected.
    #[error(transparent)]
    Approvals(#[from] crate::approvals::Error),

    /// A `decide()` rejected the command: a business outcome, not a
    /// library failure.
    #[error("command rejected: {reason}")]
    CommandRejected {
        /// The human-readable explanation `decide()` gave.
        reason: String,
        /// The machine-readable code `decide()` gave.
        kind: String,
    },

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

    /// A query failed. Its detail is logged, never shown to a caller
    /// (docs/architecture.md §93).
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
    BatchFailed {
        /// The original error's code.
        code: String,
        /// The original error's message.
        message: String,
        /// Whether the original was a `decide()`'s rejection, whose kind
        /// `code` copies: the application's own, so no hint or guide, as
        /// for `CommandRejected` (docs/architecture.md §258).
        rejected: bool,
    },

    /// Distinct from `Database` above - a migration failure means the
    /// schema itself couldn't be brought up to date (a startup-time
    /// concern, raised by `Skilj::builder().build()` - see docs/
    /// architecture.md §1.5/§1.7), not that an otherwise-healthy schema
    /// rejected one query.
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// A value of a `#[non_exhaustive]` enum this crate's caller doesn't
    /// know (docs/architecture.md §219): only reachable when that caller -
    /// `skilj-graphql`, `skilj-rest`, a bridge - is an older patch release
    /// than the `skilj-core` it runs with, and the newer one added a
    /// variant. Names the enum, so the log says what to update.
    #[error("{what} has a value this build does not support; update all skilj crates to the same version")]
    Unsupported {
        /// The enum's name.
        what: &'static str,
    },

    /// An item of a batch submitted with "stop at the first failure" that
    /// came after the batch's first failed item, and so was not processed
    /// at all (docs/architecture.md §227). Sending it again is the same as
    /// sending it for the first time.
    #[error("not processed: an earlier item of the batch failed")]
    NotProcessed,

    /// A [`crate::blob::BlobStore`] failed to store or serve bytes (issue
    /// #132). Like `Database`, the detail is for the server log only.
    #[error("blob store error: {0}")]
    BlobStore(String),

    /// The database was upgraded by a release that doesn't declare this
    /// one compatible (docs/architecture.md §249). Startup only.
    #[error(
        "the database was upgraded by {upgraded_by}, which requires schema level \
         {min_compatible_level} or later; this release is level {level} - deploy the newer \
         release, or restore a backup taken before the upgrade"
    )]
    SchemaTooNew {
        /// This release's schema level.
        level: i64,
        /// The oldest level the database accepts.
        min_compatible_level: i64,
        /// The release that upgraded the database.
        upgraded_by: String,
    },

    /// Startup was configured not to change the schema, and the schema
    /// needs upgrading (docs/architecture.md §249). Startup only.
    #[error("the database schema needs upgrading before this release can start: {pending}")]
    SchemaUpgradeRequired {
        /// What the upgrade would change, in brief.
        pending: String,
    },
}

impl Error {
    /// Whether this failure is this instance's alone: it doesn't declare
    /// what the work needs - the target command type's `decide()`, or a
    /// sync projection the resulting event feeds - and an instance that
    /// does can do the same work. Mid rolling deploy that's ordinary, so
    /// deadlines and cross-context routes retry it after a short delay
    /// without spending their retry policy, and never park it
    /// (docs/architecture.md §161).
    pub fn another_instance_can_do_it(&self) -> bool {
        match self {
            Error::BatchFailed { code, .. } => skilj_retry::another_instance_can_do_it(code),
            other => skilj_retry::another_instance_can_do_it(other.code()),
        }
    }
}

/// Implemented by every error tier so the eventual GraphQL/REST rendering
/// layer (`skilj-graphql`/`skilj-rest`) can treat "a library error" and "a
/// business rejection" uniformly, without needing to know which tier
/// produced a given error - see §4.2. `code()` is the stable,
/// machine-readable identifier (`extensions.code` on the wire, GraphQL or
/// REST alike - §5.4, §7.5); `message()` is the human-readable text.
pub trait SkiljRejection {
    /// The stable, machine-readable code, e.g. `insufficient_funds`.
    fn code(&self) -> &str;
    /// The human-readable message a caller may see.
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

    /// What to change, in one line, when an agent guide section explains
    /// this rejection's code (docs/architecture.md §258); `None` otherwise.
    /// The wording isn't stable, the code is. GraphQL puts it in
    /// `extensions.hint`, REST in the body's `hint`.
    fn hint(&self) -> Option<&'static str> {
        guidance(self.code()).map(|guidance| guidance.hint)
    }

    /// The agent guide section that explains this rejection's code,
    /// `<skill>` or `<skill>#<anchor>`, a skill under `skilj/agent-guides/`;
    /// `Some` exactly when [`hint`](Self::hint) is.
    fn guide(&self) -> Option<&'static str> {
        guidance(self.code()).map(|guidance| guidance.guide)
    }
}

/// The hint and the guide section for one wire or startup code
/// (docs/architecture.md §258).
#[derive(Debug)]
#[non_exhaustive]
pub struct Guidance {
    /// The stable code, as `SkiljRejection::code` returns it.
    pub code: &'static str,
    /// What to change, in one line.
    pub hint: &'static str,
    /// The agent guide section that explains the code, `<skill>` or
    /// `<skill>#<anchor>`.
    pub guide: &'static str,
}

const fn entry(code: &'static str, hint: &'static str, guide: &'static str) -> Guidance {
    Guidance { code, hint, guide }
}

/// The one table of guided wire and startup codes, one entry per line,
/// sorted by code. Every other code has neither hint nor guide. A code gets
/// an entry only when an agent guide section explains it, or links directly
/// to the reference that does; `skilj/src/error_codes.md` lists the same
/// rows, and `scripts/agent-guides/check.mjs` holds the two together.
#[rustfmt::skip]
pub static GUIDANCE: &[Guidance] = &[
    entry("blind_index_key_changed", "configure the blind_index_key the stored tags were hashed with; replace a key by rotating it with previous_blind_index_keys", "skilj-uniqueness#errors"),
    entry("blind_index_key_not_configured", "configure the new blind_index_key and previous_blind_index_keys on every instance; another instance retries meanwhile", "skilj-uniqueness#errors"),
    entry("blind_normalizer_changed", "revert the normalizer change: NORMALIZER_VERSION has to stay the stored one", "skilj-uniqueness#errors"),
    entry("blind_tag_key_in_use", "use a new tag key for the blind constraint: this one is already mapped in the clear", "skilj-uniqueness#errors"),
    entry("blind_tag_lookup_not_granted", "look a blind tag up by plaintext from a grant with can_read_sensitive", "skilj-uniqueness#errors"),
    entry("blind_tag_mismatch", "use TagMapping::blind::<B> everywhere the key is mapped", "skilj-uniqueness#errors"),
    entry("blind_tag_not_declared", "declare the BlindTag and blind_index_key on every instance serving the bounded context", "skilj-uniqueness#errors"),
    entry("blind_tags_rotated", "retry: a key rotation committed while the command was decided", "skilj-uniqueness#errors"),
    entry("blob_not_found", "check that a record references the blob and that its field is visible to you", "skilj-large-payloads#when-something-is-refused"),
    entry("blob_reference_invalid", "put the reference the upload answered in the payload unchanged, in its own bounded context; the message says what is wrong", "skilj-large-payloads#when-something-is-refused"),
    entry("blob_too_large", "raise SkiljBuilder::max_blob_bytes, with an S3 store past what the Postgres store holds comfortably", "skilj-large-payloads#when-something-is-refused"),
    entry("blob_upload_not_allowed", "grant the Role the blob-upload capability (grantCapability)", "skilj-large-payloads#when-something-is-refused"),
    entry("direct_creation_not_allowed", "the event type doesn't opt into direct creation, which is false by default: override direct_creation_allowed() to return true", "skilj#decision-tree"),
    entry("encryption_legacy_master_key_required", "configure .legacy_encryption_master_key(...) until the background task has moved the old data keys to the key provider", "skilj-bootstrap#keys-the-builder-needs-before-the-first-deploy"),
    entry("encryption_master_key_not_configured", "configure .key_provider(...) on the builder: a type declares sensitive fields", "skilj-bootstrap#keys-the-builder-needs-before-the-first-deploy"),
    entry("event_read_not_allowed", "declare fn event_read_allowed() -> bool { true } on the event type to read it with an EventReadToken", "skilj-temporal#getting-the-eventreadtoken"),
    entry("external_creation_not_allowed", "the event type doesn't opt into external creation, which is false by default: override external_creation_allowed() to return true", "skilj#decision-tree"),
    entry("invalid_blind_tag_mapping", "tag the owner key plainly, and drop the blind mapping on a scheduled type", "skilj-uniqueness#errors"),
    entry("invalid_blob_field", "fix the blob field's declaration: never also in sensitive_fields, and a subject can't change or be removed", "skilj-large-payloads#when-something-is-refused"),
    entry("invalid_owner_tag_key", "map the owner field as a tag first: owner_tag_key() must be one of tag_mappings()'s keys", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("invalid_private_field", "name a field the schema declares, with capability for Capability and addressee_field for Addressed", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("invalid_schema", "derive JsonSchema on the payload or state struct, using types schemars can represent", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("invalid_sensitive_field", "name scalar leaf fields the payload schema declares (check the spelling), for both field and subject_field", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("invalid_tag_mapping", "map the tag to a scalar leaf field the payload schema declares (check the spelling)", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("jwt_verification_failed", "check that .identity_provider names the issuer, audience and JWKS of the tokens being sent", "skilj-bootstrap#identity_provider-is-a-separate-axis-easy-to-conflate-with-the-above"),
    entry("live_fold_paused", "wait for the rebuild to be promoted, or declare LIVE_FOLD_COMPATIBLE = true when the new fold keeps the live state's schema and meaning", "skilj#decision-tree"),
    entry("missing_schedule_or_policy", "override both system_triggered_schedule() and missed_occurrence_policy() on a system-triggered event type", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("no_decider_registered", "register the CommandType in the binary that serves requests (#[auto_register] or .command_type::<T>()); another instance may have it", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("no_identity_provider_configured", "configure .identity_provider(IdpConfig::new(jwks_url, issuer, audience, algorithm)) so a bearer JWT can be verified", "skilj-bootstrap#identity_provider-is-a-separate-axis-easy-to-conflate-with-the-above"),
    entry("payload_does_not_match_schema", "send a payload that validates against the registered schema; check what schemars renders for Option and enum fields", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("payload_too_large", "move the large field to a blob, or raise the payload limit", "skilj-large-payloads#when-something-is-refused"),
    entry("reserved_event_type_name", "EventRetracted, EventDoubted and DoubtLifted are skilj's built-in event types: rename your type, or remove its declaration and enable retractions/doubts so skilj registers the built-in", "skilj#when-an-event-was-wrong"),
    entry("schema_incompatible", "add new fields as Option<T>; never remove, narrow or retype an existing field, and use a new type for a reshape", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("sensitive_field_tag_overlap", "make the tag on the sensitive field blind (TagMapping::blind), or tag the subject field instead", "skilj-uniqueness#errors"),
    entry("sensitive_history_unavailable", "decide() can't get this event's decrypted copy: its subject was forgotten or its key is unavailable", "skilj-uniqueness#the-pattern"),
    entry("skipped_no_access", "check that the bounded context exists, that the reconciliation role has an active, unscoped Admin grant on it (for a blind-index key rotation, one on the tenant's template works too), and, for a projection, that every event type it consumes is registered", "skilj-bootstrap#the-one-thing-every-mistake-here-comes-back-to"),
    entry("snapshot_retracted_meanwhile", "retry the command: a retraction reached a snapshot it read", "skilj-uniqueness#correcting-a-value"),
    entry("sync_projection_not_declared", "keep it retryable: this instance doesn't declare the sync projection, another instance can do the work", "skilj-temporal#two-independent-pieces---use-either-alone-or-both-together"),
    entry("tag_mapping_key_dropped", "keep every tag key an earlier registration mapped; to stop tagging a field, make a new type", "skilj#if-a-registration-or-a-submission-gets-rejected"),
    entry("unregistered_event_type", "register an EventType with that NAME in the bounded context: usually a typo, or a missing #[auto_register] impl", "skilj#if-a-registration-or-a-submission-gets-rejected"),
];

/// The hint and guide for `code`, when a guide section explains it.
pub fn guidance(code: &str) -> Option<&'static Guidance> {
    GUIDANCE
        .binary_search_by(|entry| entry.code.cmp(code))
        .ok()
        .and_then(|index| GUIDANCE.get(index))
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::AccessControl(e) => e.code(),
            Error::EventStore(e) => e.code(),
            Error::Projections(e) => e.code(),
            Error::Bootstrap(e) => e.code(),
            Error::Encryption(e) => e.code(),
            Error::Blind(e) => e.code(),
            Error::Secrets(e) => e.code(),
            Error::Approvals(e) => e.code(),
            Error::CommandRejected { kind, .. } => kind,
            Error::NoDeciderRegistered => "no_decider_registered",
            Error::Database(_) => "database_error",
            Error::Migration(_) => "migration_error",
            Error::BatchFailed { code, .. } => code,
            Error::Unsupported { .. } => "unsupported",
            Error::NotProcessed => "not_processed",
            Error::BlobStore(_) => "blob_store_error",
            Error::SchemaTooNew { .. } => "schema_too_new",
            Error::SchemaUpgradeRequired { .. } => "schema_upgrade_required",
        }
    }

    fn message(&self) -> String {
        match self {
            Error::AccessControl(e) => e.message(),
            Error::EventStore(e) => e.message(),
            Error::Projections(e) => e.message(),
            Error::Bootstrap(e) => e.message(),
            Error::Encryption(e) => e.message(),
            Error::Blind(e) => e.message(),
            Error::Secrets(e) => e.message(),
            Error::Approvals(e) => e.message(),
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
            Error::Unsupported { .. } => self.to_string(),
            Error::NotProcessed => self.to_string(),
            Error::BlobStore(_) => "an internal blob store error occurred".to_string(),
            Error::SchemaTooNew { .. } | Error::SchemaUpgradeRequired { .. } => self.to_string(),
        }
    }

    // A business rejection's kind is the application's own: never skilj's
    // advice, even when it is spelled like a skilj code, nor for a batch
    // follower's copy of one.
    fn hint(&self) -> Option<&'static str> {
        match self {
            Error::CommandRejected { .. } | Error::BatchFailed { rejected: true, .. } => None,
            _ => guidance(self.code()).map(|guidance| guidance.hint),
        }
    }

    fn guide(&self) -> Option<&'static str> {
        match self {
            Error::CommandRejected { .. } | Error::BatchFailed { rejected: true, .. } => None,
            _ => guidance(self.code()).map(|guidance| guidance.guide),
        }
    }

    fn internal_detail(&self) -> Option<String> {
        match self {
            Error::Database(e) => Some(e.to_string()),
            Error::Migration(e) => Some(e.to_string()),
            Error::BlobStore(e) => Some(e.clone()),
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
            rejected: matches!(
                cause,
                Error::CommandRejected { .. } | Error::BatchFailed { rejected: true, .. }
            ),
        }
    }

    /// A batch failure that has no underlying `Error` to copy from
    /// (a cancelled or vanished leader).
    pub fn batch_failed_msg(message: impl Into<String>) -> Error {
        Error::BatchFailed {
            code: "database_error".to_string(),
            message: message.into(),
            rejected: false,
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

    /// Connection options that can't work, refused before connecting -
    /// `skilj`'s `SkiljBuilder::build()` refusing a pool too small for
    /// it (docs/architecture.md §186), without `sqlx` as a direct
    /// dependency of its own.
    pub fn configuration(message: impl Into<String>) -> Error {
        Error::Database(sqlx::Error::Configuration(message.into().into()))
    }
}

/// `skilj-core`'s result type.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guidance_is_sorted_unique_and_well_formed() {
        for pair in GUIDANCE.windows(2) {
            assert!(
                pair[0].code < pair[1].code,
                "{} before {}",
                pair[0].code,
                pair[1].code
            );
        }
        for entry in GUIDANCE {
            assert!(
                !entry.hint.is_empty() && !entry.hint.contains('\n'),
                "{}",
                entry.code
            );
            assert!(
                !entry.guide.contains('/'),
                "{}: <skill>#<anchor>",
                entry.code
            );
            assert!(std::ptr::eq(guidance(entry.code).unwrap(), entry));
        }
        assert!(guidance("database_error").is_none());
    }

    /// Through the trait, as the renderers call it.
    fn advice(rejection: &dyn SkiljRejection) -> (Option<&'static str>, Option<&'static str>) {
        (rejection.hint(), rejection.guide())
    }

    #[test]
    fn a_guided_error_has_hint_and_guide_and_an_unguided_one_neither() {
        let guided = Error::from(crate::event_store::Error::PayloadDoesNotMatchSchema);
        let (hint, guide) = advice(&guided);
        assert!(hint.is_some());
        assert_eq!(
            guide,
            Some("skilj#if-a-registration-or-a-submission-gets-rejected")
        );
        assert_eq!(
            advice(&Error::Database(sqlx::Error::RowNotFound)),
            (None, None)
        );
        // A batch follower's error is guided as the error it copies.
        let follower = Error::batch_failed(&guided);
        assert_eq!(advice(&follower), (hint, guide));
    }

    #[test]
    fn a_business_rejection_never_gets_skilj_s_advice() {
        let rejected = Error::CommandRejected {
            reason: "too big for this account".to_string(),
            kind: "payload_too_large".to_string(),
        };
        assert!(guidance("payload_too_large").is_some());
        assert_eq!(rejected.code(), "payload_too_large");
        assert_eq!(rejected.message(), "too big for this account");
        assert_eq!(advice(&rejected), (None, None));
        // Nor a batch follower's copy of it, nor a copy of that copy, though
        // the code is copied.
        let follower = Error::batch_failed(&rejected);
        assert_eq!(follower.code(), "payload_too_large");
        assert_eq!(advice(&follower), (None, None));
        assert_eq!(advice(&Error::batch_failed(&follower)), (None, None));
    }
}
