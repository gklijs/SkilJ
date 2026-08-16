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

use crate::access_control::{
    CommandToken, DirectCreationToken, EventReadToken, ExternalEventToken, TokenStatus,
};
use crate::error::SkiljRejection;
use crate::shared::{CommandDecision, Filter, Metadata, SensitiveField, Tag, TagMapping};

// TODO: the entities and rules listed above still to add:
// `EncryptionKey` (full entity - `EncryptionKeyRef` below is a
// placeholder), `Subscription` (+ its two variants),
// `EventOrigin::SystemTriggered`'s registration fields on `EventType`
// (`system_triggered_allowed`/`system_triggered_schedule`);
// `RegisterEventType`, `RegisterCommandType`,
// `QueryEvents`/`CountEvents`/`InspectEvent`, `ForgetSubject`; the
// in-memory per-bounded-context event cache; and most of this crate's
// black boxes (`valid_tag_mappings`, `valid_sensitive_fields`,
// `schema_is_backwards_compatible`, `render_event`, `render_command`, the
// non-empty-list branches of `protect_sensitive_fields`/`derive_tags`
// below). `Event.sequence` and every field/argument derived from it are
// `i64` - see docs/architecture.md §2.2.1 - not `i32`, to avoid the
// overflow risk that decision exists to close.
//
// `EventType`, `Event`, `ReadCursor`, `AckMode` and the `FetchEvents`/
// `ConsumeEvents`/`AcknowledgeEvents` rules were a first cut, added while
// propagating tests for the EventFetch surface (docs/architecture.md
// §9's pilot). `EventType`/`Event` grew their remaining fields, and
// `EventOrigin`/`create_external_event`/`create_direct_event` were added
// next, propagating ExternalEventIngestion/DirectEventCreation.
// `BoundedContext`, `CommandType`, `Command`,
// `EventOrigin::CommandTriggered`'s field, `authorise_command_trigger`
// and `process_command` were added propagating CommandTrigger/
// ProcessCommand - `AuthoriseCommandSubmission`/`CommandSubmission` (the
// GraphQL/Role path) stayed out of that pass; see `process_command`'s own
// doc comment for why.

/// See `entity BoundedContext`'s `status` field/transition graph. Only
/// `name`/`status` are here - `created_at`/`created_by` and the
/// relationship projections (`event_types`, `commands`, ...) belong to
/// `BoundedContextCreation`/`BoundedContextDirectory`'s own obligations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedContextStatus {
    Active,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedContext {
    pub name: String,
    pub status: BoundedContextStatus,
}

/// See `entity EventType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventType {
    pub bounded_context: BoundedContext,
    pub name: String,
    pub schema: String,
    pub schema_version: i64,
    pub tag_mappings: Vec<TagMapping>,
    pub sensitive_fields: Vec<SensitiveField>,
    pub external_creation_allowed: bool,
    pub direct_creation_allowed: bool,
    pub event_read_allowed: bool,
}

/// See `entity CommandType`. `command_tokens` (a relationship
/// projection, not a stored field - the same treatment `EventType`'s
/// `*_tokens` relationships get) is omitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandType {
    pub bounded_context: BoundedContext,
    pub name: String,
    pub schema: String,
    pub schema_version: i64,
    pub tag_mappings: Vec<TagMapping>,
    pub sensitive_fields: Vec<SensitiveField>,
    pub rest_trigger_allowed: bool,
}

/// Stands in for the full `entity EncryptionKey` (its own `status`/
/// `subject_key`/`subject_value`/lifecycle belong to whichever surface
/// propagates `ForgetSubject`/crypto-shredding) - just enough identity to
/// be a distinct value in `Event.encryption_keys`. Never actually
/// populated yet: `protect_sensitive_fields` below only has a real
/// implementation for the empty-`sensitive_fields` case, which always
/// returns none of these.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionKeyRef {
    pub subject_key: String,
    pub subject_value: String,
}

/// See `value EncryptedPayload`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtectedPayload {
    pub payload: String,
    pub encryption_keys: Vec<EncryptionKeyRef>,
}

/// See `entity Command`. `triggered_events` (a relationship projection -
/// the events this command's own `ProcessCommand` call created, not a
/// stored field) is deliberately omitted, the same treatment
/// `EventType`'s `*_tokens` get; a `process_command` caller already has
/// them as `ProcessCommandResult::events`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub bounded_context: BoundedContext,
    pub command_type: CommandType,
    pub payload: String,
    pub metadata: Metadata,
    pub encryption_keys: Vec<EncryptionKeyRef>,
    pub consistency_tags: Vec<Tag>,
    /// `None` exactly when `highest_sequence` over this command's own
    /// `consistency_tags`-matching events returns `None` - which covers
    /// both "this command type declares no `tag_mappings`, so DCB is not
    /// in play" and "it does, but no event has matched any of these tags
    /// yet" alike. See `consistency_boundary_and_matching_events`'s doc
    /// comment for why those two cases don't need to be told apart.
    pub consistency_boundary: Option<i64>,
}

/// See `Event.origin`'s sum type. `SystemTriggered` is a placeholder with
/// no fields yet - per the spec it has no variant-specific fields at all,
/// so this placeholder is already its final shape (see
/// `SystemTriggeredClientIdIsSystem`, still to propagate, for the
/// `metadata.client_id` constraint that comes with it). `CommandTriggered`
/// boxes its `Command` - `Command` is by far this enum's largest variant
/// payload (it embeds a whole `CommandType`), and every `Event` carries
/// an `EventOrigin` regardless of which variant, so leaving it unboxed
/// would size every `Event` - including the far more common
/// `DirectlyCreated`/`ExternalTriggered` ones - to its worst case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventOrigin {
    CommandTriggered {
        command: Box<Command>,
    },
    ExternalTriggered {
        source_content: String,
        source_context: Option<String>,
    },
    DirectlyCreated,
    SystemTriggered,
}

/// See `entity Event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub bounded_context: BoundedContext,
    pub event_type: EventType,
    pub payload: String,
    pub metadata: Metadata,
    pub sequence: i64,
    pub tags: Vec<Tag>,
    pub encryption_keys: Vec<EncryptionKeyRef>,
    pub origin: EventOrigin,
}

/// See `enum AckMode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    AutoAdvance,
    ManualAck,
}

/// See `entity ReadCursor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadCursor {
    pub token: EventReadToken,
    pub ack_mode: AckMode,
    pub sequence: i64,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

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

    #[error("this EventType is not opted into event reads")]
    EventReadNotAllowed,

    #[error("this EventReadToken has no read cursor yet - consume before acknowledging")]
    NoReadCursor,

    #[error("an acknowledgement may not move a cursor backwards")]
    AcknowledgementRegresses,

    #[error("this EventType is not opted into external event ingestion")]
    ExternalCreationNotAllowed,

    #[error("this EventType is not opted into direct event creation")]
    DirectCreationNotAllowed,

    #[error("this CommandType is not opted into REST triggering")]
    RestTriggerNotAllowed,

    /// Not spec-modeled: the spec's own `ProcessCommand` assumes `decide()`
    /// only ever names an `EventType` its bounded context actually
    /// registered - a plugin-author responsibility, not a case the spec
    /// enumerates a rejection for. Kept here anyway as a defensive check
    /// rather than an `unwrap()`/panic, since `decide()` is exactly the
    /// kind of black-box plugin code a misconfigured bounded context could
    /// get wrong.
    #[error("decide() named an event type this bounded context has not registered: {0}")]
    UnregisteredEventType(String),
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
            Error::EventReadNotAllowed => "event_read_not_allowed",
            Error::NoReadCursor => "no_read_cursor",
            Error::AcknowledgementRegresses => "acknowledgement_regresses",
            Error::ExternalCreationNotAllowed => "external_creation_not_allowed",
            Error::DirectCreationNotAllowed => "direct_creation_not_allowed",
            Error::RestTriggerNotAllowed => "rest_trigger_not_allowed",
            Error::UnregisteredEventType(_) => "unregistered_event_type",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

/// Black box shared with `QueryEvents`/`CountEvents` (see the note above
/// rule `FetchEvents` and docs/architecture.md's "black boxes" list) -
/// not owned by the EventFetch pilot. The empty-filter case is real and
/// sufficient for every EventFetch obligation (none of them exercise
/// filter semantics themselves); field/operator type-checking against
/// `EventType.schema` is deferred to whichever surface propagates filter
/// semantics first.
pub fn valid_filters(_event_type: &EventType, filters: &[Filter]) -> bool {
    if filters.is_empty() {
        true
    } else {
        todo!("valid_filters: field/operator type-checking against EventType.schema - deferred, see doc comment above")
    }
}

/// See `valid_filters` above - same deferral, same empty-filter fast path.
pub fn matches_filters(_event: &Event, filters: &[Filter]) -> bool {
    if filters.is_empty() {
        true
    } else {
        todo!("matches_filters - deferred, see valid_filters above")
    }
}

/// The greatest `Event.sequence` among a set of events, or `None` when
/// empty - see the note above rule `ConsumeEvents`.
fn highest_sequence(events: &[Event]) -> Option<i64> {
    events.iter().map(|e| e.sequence).max()
}

/// Black box (see the note above rule `CreateExternalEvent`): "a no-op -
/// unchanged payload, empty key set - when `sensitive_fields` is empty"
/// is the one part of its contract this pilot needs and implements for
/// real. Get-or-creating an `EncryptionKey` per distinct subject and
/// substituting ciphertext at each declared field's leaf is
/// `ForgetSubject`/crypto-shredding's own pass to add. Takes
/// `sensitive_fields` alone, not a whole `EventType`/`CommandType` -
/// "it takes an EventType and a CommandType interchangeably" per the
/// spec's own note, since `sensitive_fields` is the identical
/// `Set<SensitiveField>` on both.
pub fn protect_sensitive_fields(
    sensitive_fields: &[SensitiveField],
    payload: &str,
) -> ProtectedPayload {
    if sensitive_fields.is_empty() {
        ProtectedPayload {
            payload: payload.to_string(),
            encryption_keys: Vec::new(),
        }
    } else {
        todo!("protect_sensitive_fields: get-or-create EncryptionKey per subject, substitute ciphertext at each leaf - deferred, see doc comment above")
    }
}

/// Black box (see the note above `derive_tags` in the spec). Same
/// deferral as `protect_sensitive_fields` above: real for the empty-
/// `tag_mappings` case (no tags to derive), `todo!()` otherwise - walking
/// a JSON payload to read a mapped field out of it is a different
/// surface's obligation to add. Same "takes `tag_mappings` alone, not a
/// whole `EventType`/`CommandType`" treatment as `protect_sensitive_fields`
/// above, for the identical reason.
pub fn derive_tags(tag_mappings: &[TagMapping], _payload: &str) -> Vec<Tag> {
    if tag_mappings.is_empty() {
        Vec::new()
    } else {
        todo!("derive_tags: walk payload per TagMapping, including the list/optional-field cases - deferred, see doc comment above")
    }
}

/// See `rule FetchEvents`. `read_type` isn't a separate parameter - it's
/// always `token.event_type` (the surface's own `context` binding), so
/// it's derived here rather than accepted from the caller, which makes
/// the spec's `requires: token.event_type = read_type` true by
/// construction instead of a runtime check with its own failure mode.
pub fn fetch_events(
    token: &EventReadToken,
    events: &[Event],
    filters: &[Filter],
    after_sequence: Option<i64>,
) -> crate::error::Result<Vec<Event>> {
    if token.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    let read_type = &token.event_type;
    if !read_type.event_read_allowed {
        return Err(Error::EventReadNotAllowed.into());
    }
    if !valid_filters(read_type, filters) {
        return Err(Error::InvalidFilter.into());
    }

    let after = after_sequence.unwrap_or(-1);
    Ok(events
        .iter()
        .filter(|e| e.bounded_context == read_type.bounded_context)
        .filter(|e| &e.event_type == read_type)
        .filter(|e| e.sequence > after)
        .filter(|e| matches_filters(e, filters))
        .cloned()
        .collect())
}

/// What `consume_events` did to the token's `ReadCursor` - the caller
/// (the eventual sqlx-backed persistence layer) is responsible for
/// actually storing this; the same pure/no-I/O split `decide()`/
/// `project()` use (§1.1), extended here since this rule's own logic has
/// no I/O of its own either.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CursorUpdate {
    /// First `ConsumeEvents` call for this token - `ReadCursor.created`.
    Created(ReadCursor),
    /// `auto_advance` - the spec's `ensures` block writes
    /// `cursor.sequence = next_position` unconditionally on this branch,
    /// even when nothing new matched and `next_position` falls back to
    /// the unchanged `position` (`highest_sequence(served) ?? position`).
    /// So this fires on every `auto_advance` call after the first, not
    /// only when something new was served.
    Advanced {
        sequence: i64,
        updated_at: chrono::DateTime<chrono::Utc>,
    },
    /// `manual_ack` - only `AcknowledgeEvents` moves this cursor, so
    /// serving never does, matched or not.
    Unchanged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeEventsResult {
    pub served: Vec<Event>,
    pub cursor_update: CursorUpdate,
}

/// See `rule ConsumeEvents`. `existing_cursor` is this token's current
/// `ReadCursor` as already looked up by the caller (`None` when this
/// token has never consumed before) - the get-or-create lookup itself is
/// the caller's persistence concern, not this rule's.
pub fn consume_events(
    token: &EventReadToken,
    existing_cursor: Option<&ReadCursor>,
    ack_mode: Option<AckMode>,
    events: &[Event],
    filters: &[Filter],
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<ConsumeEventsResult> {
    if token.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    let read_type = &token.event_type;
    if !read_type.event_read_allowed {
        return Err(Error::EventReadNotAllowed.into());
    }
    if !valid_filters(read_type, filters) {
        return Err(Error::InvalidFilter.into());
    }

    let is_new = existing_cursor.is_none();
    if is_new && ack_mode.is_none() {
        return Err(Error::CursorAckModeMismatch.into());
    }
    if let (Some(cursor), Some(requested)) = (existing_cursor, ack_mode) {
        if requested != cursor.ack_mode {
            return Err(Error::CursorAckModeMismatch.into());
        }
    }

    let mode = ack_mode
        .or_else(|| existing_cursor.map(|c| c.ack_mode))
        .expect("is_new implies ack_mode.is_some(), checked above");
    let position = existing_cursor.map(|c| c.sequence).unwrap_or(-1);

    let served: Vec<Event> = events
        .iter()
        .filter(|e| e.bounded_context == read_type.bounded_context)
        .filter(|e| &e.event_type == read_type)
        .filter(|e| e.sequence > position)
        .filter(|e| matches_filters(e, filters))
        .cloned()
        .collect();

    let next_position = match mode {
        AckMode::AutoAdvance => highest_sequence(&served).unwrap_or(position),
        AckMode::ManualAck => position,
    };

    let cursor_update = if is_new {
        CursorUpdate::Created(ReadCursor {
            token: token.clone(),
            ack_mode: mode,
            sequence: next_position,
            updated_at: now,
        })
    } else if mode == AckMode::AutoAdvance {
        CursorUpdate::Advanced {
            sequence: next_position,
            updated_at: now,
        }
    } else {
        CursorUpdate::Unchanged
    };

    Ok(ConsumeEventsResult {
        served,
        cursor_update,
    })
}

/// See `rule AcknowledgeEvents`. `cursor` is this token's current
/// `ReadCursor` as already looked up by the caller - same split as
/// `consume_events` above. Returns the cursor's new `(sequence,
/// updated_at)` on success.
pub fn acknowledge_events(
    token: &EventReadToken,
    cursor: Option<&ReadCursor>,
    sequence: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<(i64, chrono::DateTime<chrono::Utc>)> {
    if token.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    let cursor = cursor.ok_or(Error::NoReadCursor)?;
    if cursor.ack_mode != AckMode::ManualAck {
        return Err(Error::NotManualAckCursor.into());
    }
    if sequence < cursor.sequence {
        return Err(Error::AcknowledgementRegresses.into());
    }

    Ok((sequence, now))
}

/// See `rule CreateExternalEvent`. `next_sequence` is the value the
/// caller's own `next_sequence(bounded_context)` already allocated under
/// its Postgres row lock (see the note above the rules) - not this
/// function's to compute, the same pure/no-I/O split every other rule in
/// this module uses. Same "derived, not a separate parameter" treatment
/// as `fetch_events`' `read_type` for `adapter.event_type = event_type`:
/// there's no `event_type` argument to disagree with `adapter.event_type`.
pub fn create_external_event(
    adapter: &ExternalEventToken,
    payload: String,
    source_content: String,
    source_context: Option<String>,
    next_sequence: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<Event> {
    if adapter.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    let event_type = &adapter.event_type;
    if !event_type.external_creation_allowed {
        return Err(Error::ExternalCreationNotAllowed.into());
    }

    let protected = protect_sensitive_fields(&event_type.sensitive_fields, &payload);
    Ok(Event {
        bounded_context: event_type.bounded_context.clone(),
        event_type: event_type.clone(),
        payload: protected.payload,
        metadata: Metadata {
            r#type: event_type.name.clone(),
            version: event_type.schema_version,
            client_id: adapter.id.clone(),
            created_at: now,
        },
        sequence: next_sequence,
        tags: derive_tags(&event_type.tag_mappings, &payload),
        encryption_keys: protected.encryption_keys,
        origin: EventOrigin::ExternalTriggered {
            source_content,
            source_context,
        },
    })
}

/// See `rule CreateDirectEvent`. Same shape and reasoning as
/// `create_external_event` above - the only differences are the opt-in
/// checked (`direct_creation_allowed`), the absence of the two opaque
/// `source_content`/`source_context` fields, and the resulting
/// `EventOrigin` variant.
pub fn create_direct_event(
    adapter: &DirectCreationToken,
    payload: String,
    next_sequence: i64,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<Event> {
    if adapter.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    let event_type = &adapter.event_type;
    if !event_type.direct_creation_allowed {
        return Err(Error::DirectCreationNotAllowed.into());
    }

    let protected = protect_sensitive_fields(&event_type.sensitive_fields, &payload);
    Ok(Event {
        bounded_context: event_type.bounded_context.clone(),
        event_type: event_type.clone(),
        payload: protected.payload,
        metadata: Metadata {
            r#type: event_type.name.clone(),
            version: event_type.schema_version,
            client_id: adapter.id.clone(),
            created_at: now,
        },
        sequence: next_sequence,
        tags: derive_tags(&event_type.tag_mappings, &payload),
        encryption_keys: protected.encryption_keys,
        origin: EventOrigin::DirectlyCreated,
    })
}

/// The `CommandAuthorised` fact `AuthoriseCommandSubmission`/
/// `AuthoriseCommandTrigger` each produce, and `process_command` below
/// consumes - the join point between REST/GraphQL authorisation and the
/// one command-processing pipeline both funnel into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandAuthorised {
    pub command_type: CommandType,
    pub payload: String,
    pub client_id: String,
}

/// See `rule AuthoriseCommandTrigger`. Same "derived, not a separate
/// parameter" treatment as `fetch_events`/`create_external_event` for
/// `token.command_type = command_type`. Reuses `Error::BoundedContextArchived`
/// for `command_type.bounded_context.status = active` - same rejection,
/// same meaning, whichever rule hits it.
pub fn authorise_command_trigger(
    token: &CommandToken,
    payload: String,
) -> crate::error::Result<CommandAuthorised> {
    if token.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    let command_type = &token.command_type;
    if !command_type.rest_trigger_allowed {
        return Err(Error::RestTriggerNotAllowed.into());
    }
    if command_type.bounded_context.status != BoundedContextStatus::Active {
        return Err(Error::BoundedContextArchived.into());
    }

    Ok(CommandAuthorised {
        command_type: command_type.clone(),
        payload,
        client_id: token.id.clone(),
    })
}

/// See the `boundary`/`matching_events` `let`s above rule `ProcessCommand`.
/// Reuses `highest_sequence` unmodified - the spec calls it with
/// different arguments here (`command_type.bounded_context,
/// consistency_tags`, rather than a `Vec<Event>`) but it's "a black box in
/// the same trivial register" either way, and once reduced to "the
/// highest sequence among this bounded context's events matching any of
/// these tags", it's the identical computation, so no second black box is
/// declared for it. The spec's own `matching_events` binding adds a
/// `sequence <= boundary` filter on top - definitionally satisfied by
/// every member of the tag-matching set computed here, since `boundary`
/// *is* that set's own maximum - so it drops out entirely in a
/// single-snapshot pure function. It only bites under concurrent appends
/// between reading and committing, which is `next_sequence`'s caller's
/// own re-check to make under its Postgres lock (see the @guidance note
/// above `ProcessCommand`), not this function's.
///
/// The `None` case - no matching event found, so no boundary - covers
/// "this command type declares no `tag_mappings`" and "it does, but
/// nothing has matched any of them yet" alike; `Command.consistency_boundary`
/// (`Option<i64>`) carries that same `None` through unchanged. The spec's
/// own doc for that field reads "null when the command does not use a
/// consistency boundary", which literally describes only the first case,
/// but `highest_sequence`'s contract ("the greatest sequence... or null
/// when empty") doesn't distinguish an empty-because-no-tags-were-given
/// set from an empty-because-nothing-matched-yet one - and
/// `DynamicConsistencyBoundaryHonoured` doesn't need to either: a `null`
/// boundary makes its `e.sequence > c.consistency_boundary` comparison
/// vacuous either way, and a command whose tags have zero prior matches
/// has nothing a concurrent writer could have raced it on regardless.
pub fn consistency_boundary_and_matching_events(
    bounded_context_events: &[Event],
    consistency_tags: &[Tag],
) -> (Option<i64>, Vec<Event>) {
    let matching: Vec<Event> = bounded_context_events
        .iter()
        .filter(|e| consistency_tags.iter().any(|t| e.tags.contains(t)))
        .cloned()
        .collect();
    let boundary = highest_sequence(&matching);
    (boundary, matching)
}

/// What `process_command` produced - the caller (the eventual sqlx-backed
/// persistence layer) stores `command` and every one of `events` in one
/// transaction, alongside updating whichever sync projections consume
/// them (see the @guidance note above rule `ProcessCommand`). Not named
/// `CommandTriggered` because that's `Event.origin`'s variant name for
/// each individual event, not this pairing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessCommandResult {
    pub command: Command,
    pub events: Vec<Event>,
}

/// See `rule ProcessCommand`. Split from `AuthoriseCommandSubmission`/
/// `AuthoriseCommandTrigger` above at exactly the spec's own boundary -
/// `when: CommandAuthorised(...)` - so this one function serves both
/// authorisation paths; only `AuthoriseCommandTrigger`'s (the REST/
/// `CommandToken` one) is implemented so far. `AuthoriseCommandSubmission`
/// (the GraphQL/`RoleAccessMapping` one) needs `access_control::Role`/
/// `RoleAccessMapping`/`AccessLevel`, none of which exist yet - a
/// materially larger chunk of work than adding one more token variant was,
/// so it's deferred to its own pass rather than folded into this one;
/// `process_command` itself doesn't care which authorisation path produced
/// its `command_type`/`payload`/`client_id` input, so nothing here blocks
/// on that.
///
/// `decision` is already-computed, not called from inside this function:
/// invoking the right bounded context's own typed `CommandType::decide()`
/// crosses a type-erasure boundary this internal engine sits on the far
/// side of (raw JSON/`EventType`-by-name) - the spec itself says that
/// wiring "stays a prose contract here, not a formally specified
/// interface" (see the note above the rule), so resolving it isn't this
/// pure function's job. Same reasoning for `resolve_event_type`: turning
/// `decision`'s `EventSpec.event_type: String` names back into full
/// `EventType`s is a bounded-context-scoped registry lookup
/// (`RegisterEventType`'s own registry, not yet propagated), supplied by
/// the caller. `next_sequence` is called once per accepted event, in
/// order - the same Postgres-lock-backed, caller-supplied value as
/// `create_external_event`/`create_direct_event`'s.
///
/// Eight parameters because this function's own scope is genuinely that
/// wide - it's `ProcessCommand`'s entire `let`/`ensures` body, not
/// something a smaller grouping would simplify without inventing a
/// struct that exists only to satisfy the lint.
#[allow(clippy::too_many_arguments)]
pub fn process_command(
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    bounded_context_events: &[Event],
    decision: CommandDecision,
    resolve_event_type: impl Fn(&str) -> Option<EventType>,
    mut next_sequence: impl FnMut() -> i64,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<ProcessCommandResult> {
    let event_specs = match decision {
        CommandDecision::Accepted { events } => events,
        CommandDecision::Rejected { reason, kind } => {
            return Err(crate::error::Error::CommandRejected { reason, kind });
        }
    };

    let consistency_tags = derive_tags(&command_type.tag_mappings, payload);
    let (consistency_boundary, _matching_events) =
        consistency_boundary_and_matching_events(bounded_context_events, &consistency_tags);

    let protected = protect_sensitive_fields(&command_type.sensitive_fields, payload);
    let command = Command {
        bounded_context: command_type.bounded_context.clone(),
        command_type: command_type.clone(),
        payload: protected.payload,
        metadata: Metadata {
            r#type: command_type.name.clone(),
            version: command_type.schema_version,
            client_id: client_id.to_string(),
            created_at: now,
        },
        encryption_keys: protected.encryption_keys,
        consistency_tags,
        consistency_boundary,
    };

    let mut events = Vec::with_capacity(event_specs.len());
    for spec in event_specs {
        let event_type = resolve_event_type(&spec.event_type)
            .ok_or_else(|| Error::UnregisteredEventType(spec.event_type.clone()))?;
        let spec_payload = spec.payload.to_string();
        let event_protected = protect_sensitive_fields(&event_type.sensitive_fields, &spec_payload);
        events.push(Event {
            bounded_context: command_type.bounded_context.clone(),
            event_type: event_type.clone(),
            payload: event_protected.payload,
            metadata: Metadata {
                r#type: event_type.name.clone(),
                version: event_type.schema_version,
                client_id: client_id.to_string(),
                created_at: now,
            },
            sequence: next_sequence(),
            tags: derive_tags(&event_type.tag_mappings, &spec_payload),
            encryption_keys: event_protected.encryption_keys,
            origin: EventOrigin::CommandTriggered {
                command: Box::new(command.clone()),
            },
        });
    }

    Ok(ProcessCommandResult { command, events })
}
