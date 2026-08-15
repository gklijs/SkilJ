//! `BoundedContext`, `EventType`, `CommandType`, `Event` (+ its four
//! variants), `Command`, `EncryptionKey`, `Subscription` (+ its two
//! variants), `ReadCursor`; `ProcessCommand`, `RegisterEventType`,
//! `RegisterCommandType`, `CreateExternalEvent`, `CreateDirectEvent`,
//! `FetchEvents`, `ConsumeEvents`, `AcknowledgeEvents`,
//! `QueryEvents`/`CountEvents`/`InspectEvent`, `ForgetSubject`; the
//! in-memory per-bounded-context event cache; and most of this crate's
//! black boxes (`protect_sensitive_fields`, `derive_tags`,
//! `valid_filters`, `valid_tag_mappings`, `valid_sensitive_fields`,
//! `schema_is_backwards_compatible`, `next_sequence`, `render_event`,
//! `render_command`). See docs/architecture.md §3.2.

use crate::error::SkiljRejection;

// TODO: the entities and rules listed above. `Event.sequence` and every
// field/argument derived from it are `i64` - see docs/architecture.md
// §2.2.1 - not `i32`, to avoid the overflow risk that decision exists to
// close.

/// Library-level errors this module's own rules reject for.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("this bounded context is archived and accepts no new writes")]
    BoundedContextArchived,

    #[error("this schema change is not backwards compatible")]
    SchemaIncompatible,

    #[error("this filter is invalid for its field's declared type")]
    InvalidFilter,

    #[error("this tag mapping names a field the schema doesn't declare")]
    InvalidTagMapping,

    #[error("a TagMapping and a SensitiveField may not name the same field")]
    SensitiveFieldTagOverlap,

    #[error("this EventReadToken's cursor mode doesn't match what was requested")]
    CursorAckModeMismatch,

    #[error("acknowledging requires a manual_ack cursor")]
    NotManualAckCursor,
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::BoundedContextArchived => "bounded_context_archived",
            Error::SchemaIncompatible => "schema_incompatible",
            Error::InvalidFilter => "invalid_filter",
            Error::InvalidTagMapping => "invalid_tag_mapping",
            Error::SensitiveFieldTagOverlap => "sensitive_field_tag_overlap",
            Error::CursorAckModeMismatch => "cursor_ack_mode_mismatch",
            Error::NotManualAckCursor => "not_manual_ack_cursor",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}
