//! `BoundedContext`, `EventType`, `CommandType`, `Event` (+ its four
//! variants), `Command`, `EncryptionKey`, `Subscription` (+ its two
//! variants), `ReadCursor`; `ProcessCommand`, `RegisterEventType`,
//! `RegisterCommandType`, `CreateExternalEvent`, `CreateDirectEvent`,
//! `FetchEvents`, `ConsumeEvents`, `AcknowledgeEvents`, `FetchCommands`,
//! `QueryEvents`/`CountEvents`/`InspectEvent`, `ForgetSubject`; the
//! in-memory per-bounded-context event cache; and most of this crate's
//! black boxes (`protect_sensitive_fields`, `derive_tags`,
//! `valid_filters`, `valid_tag_mappings`, `valid_sensitive_fields`,
//! `schema_is_backwards_compatible`, `next_sequence`, `render_event`,
//! `render_command`). `Projection`/`ProjectionRebuild` and their four
//! rules live in `crate::projections` instead, not here - see this
//! module's own history note below for why they briefly weren't. See
//! docs/architecture.md §3.2.

use crate::access_control::{
    AccessLevel, CommandToken, DirectCreationToken, EventReadToken, ExternalEventToken,
    RoleAccessMapping, RoleStatus, TokenStatus,
};
use crate::error::SkiljRejection;
use crate::shared::{CommandDecision, Filter, Metadata, SensitiveField, Tag, TagMapping};

// TODO: the in-memory per-bounded-context event cache
// (`query_events`/`count_events`/`create_all_events_subscription`/
// `create_event_type_subscription` below take a full `Event` snapshot as
// a stand-in - see their own doc comments); and most of this crate's
// remaining black boxes (the non-empty-list branches of
// `protect_sensitive_fields`/`derive_tags`/`render_event`/`render_command`
// below - `EncryptionKey` provisioning specifically, not yet needed by
// `forget_subject`'s own destruction-only scope - and `resolve_field`'s
// dotted-path branch - see its own comment). `Event.sequence` and every
// field/argument derived from it are `i64` - see docs/architecture.md
// §2.2.1 - not `i32`, to avoid the overflow risk that decision exists to
// close.
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
// doc comment for why. `authorise_command_submission` was added next,
// once Role/RoleAccessMapping existed to authorise it against.
// `register_event_type`/`register_command_type`, real implementations of
// `valid_tag_mappings`/`valid_sensitive_fields`/
// `schema_is_backwards_compatible` (flat, bare-field-name schemas - see
// `resolve_field`'s own doc comment for the dotted-path elaboration left
// deferred), and `EventType`'s two `system_triggered_*` fields were added
// next, propagating `RegisterEventType`/`RegisterCommandType`.
// `Projection`/`ProjectionRebuild`, `register_projection`,
// `rebuild_projection` and `discard_projection_rebuild` were added here
// next, propagating `RegisterProjection`/`RebuildProjection`/
// `DiscardProjectionRebuild` - promotion (a rebuild replacing the live
// `Projection` once caught up) is a background-process concern per the
// spec's own text, not something any of these three rules perform, so it
// stayed unmodelled along with the async `project()` consumer that would
// drive it. `query_events`/`count_events`/`inspect_event` and a real
// (empty-`sensitive_fields`-case) `render_event` were added next,
// propagating `QueryEvents`/`CountEvents`/`InspectEvent` -
// `FetchCommands`/`QueryProjection` (the other two admin-facing reads
// sharing this same two-grant redaction contract, via `render_command`/
// `read_projection`) stayed out of that pass, left for their own.
// `fetch_commands` and `render_command` (`render_event`'s own
// `Command`-shaped twin) were added next, propagating `FetchCommands`.
// `query_projection` was added next, propagating `QueryProjection` -
// `read_projection` and `await_projection_caught_up` are both fully
// caller-supplied (see `crate::projections::query_projection`'s own doc
// comment for why), the same treatment `process_command`'s `decision`
// gets, rather than getting the trivial/deferred split
// `render_event`/`render_command` have. `Subscription` (+ its two
// variants), `create_all_events_subscription`/
// `create_event_type_subscription`/`deliver_to_subscriptions` were added
// next, propagating `EventSubscription` - the real delivery loop's own
// asynchronous, best-effort dispatch (outside any request's transaction,
// per the note above rule `DeliverToSubscriptions`) stays unmodelled;
// `deliver_to_subscriptions` is the pure "who matches and what do they
// get" computation underneath it. `EncryptionKey` (full entity -
// `EncryptionKeyRef`'s stand-in role folded into it, since neither is
// populated by anything else yet) and `forget_subject` were added last,
// propagating `ForgetSubject` - deliberately scoped to destruction only;
// see `EncryptionKey`'s own doc comment for why provisioning
// (`protect_sensitive_fields`'s get-or-create) stayed out of this pass.
// `Projection`/`ProjectionRebuild` and their four rules were moved out to
// `crate::projections` last, after the fact: `docs/architecture.md`'s own
// module map had always put them there, in a `projections` module
// already scaffolded with its own placeholder `Error` - missed when they
// were first built directly here instead. See `crate::projections`' own
// doc comment for the rest of that history.

/// See `entity BoundedContext`'s `status` field/transition graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedContextStatus {
    Active,
    Archived,
}

/// See `entity BoundedContext`. `created_at`/`created_by` were added
/// propagating `BoundedContextCreation`/`BoundedContextDirectory` (the
/// `admin`/`skilj` defaults' own `created_at`/`created_by` stay with the
/// still-TODO startup reconciliation loop - see `crate::bootstrap`'s own
/// doc comment). The relationship projections (`event_types`, `commands`,
/// ...) are omitted, the same "caller resolves it, not a stored field"
/// treatment every other relationship projection in this codebase gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedContext {
    pub name: String,
    pub status: BoundedContextStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub created_by: crate::bootstrap::ContextCreator,
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
    /// See `EventOrigin::SystemTriggered`'s registration opt-in - added
    /// propagating `RegisterEventType`, alongside `system_triggered_schedule`
    /// below. Neither is read by any rule propagated so far
    /// (`EventOrigin::SystemTriggered` itself - the scheduler that would
    /// consult them - is still `// TODO`, see this module's own doc
    /// comment).
    pub system_triggered_allowed: bool,
    pub system_triggered_schedule: Option<String>,
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

/// See `entity EncryptionKey`'s `status` field/transition graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionKeyStatus {
    Active,
    Destroyed,
}

/// See `entity EncryptionKey`. An `EncryptionKey` is identified by
/// `bounded_context`/`subject_key`/`subject_value` alone, never by which
/// type declared the field (see the note above rule `CreateExternalEvent`),
/// which is what lets a command and an event naming the same subject
/// encrypt under the very same key. Provisioning
/// (`protect_sensitive_fields`'s get-or-create) still stays `todo!()` for
/// its non-empty-`sensitive_fields` case - only `forget_subject`
/// (destruction) is real this pass; see this module's own doc comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionKey {
    pub bounded_context: BoundedContext,
    pub subject_key: String,
    pub subject_value: String,
    pub status: EncryptionKeyStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub destroyed_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// See `value EncryptedPayload`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtectedPayload {
    pub payload: String,
    pub encryption_keys: Vec<EncryptionKey>,
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
    pub encryption_keys: Vec<EncryptionKey>,
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
    pub encryption_keys: Vec<EncryptionKey>,
    pub origin: EventOrigin,
}

/// See `variant AllEventsSubscription`. `bounded_context`/`access_mapping`/
/// `created_at`/`from_sequence` are `entity Subscription`'s own base
/// fields, carried on each variant directly - the same "no wrapper
/// struct, the variant carries the base fields too" treatment
/// `AccessToken`'s four variants get.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllEventsSubscription {
    pub bounded_context: BoundedContext,
    pub access_mapping: RoleAccessMapping,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub from_sequence: i64,
    pub event_types: Vec<EventType>,
}

/// See `variant EventTypeSubscription`. Same base-fields-on-the-variant
/// treatment as `AllEventsSubscription` above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventTypeSubscription {
    pub bounded_context: BoundedContext,
    pub access_mapping: RoleAccessMapping,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub from_sequence: i64,
    pub event_type: EventType,
    pub filters: Vec<Filter>,
}

/// See `entity Subscription`'s `kind` field - a Rust sum type over its two
/// variants stands in for it directly, the same "the enum variant tag is
/// the field" treatment `Event.origin`/`EventOrigin`, `AccessToken`/
/// `purpose` and `ContextCreator`/`kind` all get. Both variants are boxed:
/// each already embeds a whole `BoundedContext` plus a `RoleAccessMapping`
/// (itself embedding a `Role` and a second `BoundedContext`) before any
/// variant-specific field, so an unboxed enum would size every
/// `Subscription` - and everything that embeds one, like `EventDelivered`
/// below - to whichever variant happens to be larger. Same reasoning
/// `EventOrigin::CommandTriggered`'s boxed `Command` has.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Subscription {
    AllEventsSubscription(Box<AllEventsSubscription>),
    EventTypeSubscription(Box<EventTypeSubscription>),
}

impl Subscription {
    fn bounded_context(&self) -> &BoundedContext {
        match self {
            Subscription::AllEventsSubscription(s) => &s.bounded_context,
            Subscription::EventTypeSubscription(s) => &s.bounded_context,
        }
    }

    fn access_mapping(&self) -> &RoleAccessMapping {
        match self {
            Subscription::AllEventsSubscription(s) => &s.access_mapping,
            Subscription::EventTypeSubscription(s) => &s.access_mapping,
        }
    }

    fn starting_sequence(&self) -> i64 {
        match self {
            Subscription::AllEventsSubscription(s) => s.from_sequence,
            Subscription::EventTypeSubscription(s) => s.from_sequence,
        }
    }
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

    #[error("this sensitive field names a field the schema doesn't declare")]
    InvalidSensitiveField,

    #[error("a re-registration may not drop a tag mapping key already in use")]
    TagMappingKeyDropped,

    #[error("this EncryptionKey is not active")]
    EncryptionKeyNotActive,

    #[error("this named EventType belongs to a different bounded context than the grant's")]
    EventTypeNotInBoundedContext,

    #[error("this named CommandType belongs to a different bounded context than the grant's")]
    CommandTypeNotInBoundedContext,

    #[error("this triggered_event belongs to a different bounded context than the grant's")]
    TriggeredEventNotInBoundedContext,

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
            Error::InvalidSensitiveField => "invalid_sensitive_field",
            Error::TagMappingKeyDropped => "tag_mapping_key_dropped",
            Error::EventTypeNotInBoundedContext => "event_type_not_in_bounded_context",
            Error::CommandTypeNotInBoundedContext => "command_type_not_in_bounded_context",
            Error::TriggeredEventNotInBoundedContext => "triggered_event_not_in_bounded_context",
            Error::EncryptionKeyNotActive => "encryption_key_not_active",
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

/// Parses `schema` as a JSON Schema object and returns its top-level
/// `properties` map - the one piece `valid_tag_mappings`/
/// `valid_sensitive_fields`/`schema_is_backwards_compatible` below all
/// need, per the payload schema shape note above `entity CommandType`.
/// `None` for anything that doesn't parse as `{"properties": {...}, ...}`,
/// which callers treat as "no fields exist", the same closed-world answer
/// an empty `properties` map gives.
fn schema_properties(schema: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    let parsed: serde_json::Value = serde_json::from_str(schema).ok()?;
    parsed.get("properties")?.as_object().cloned()
}

/// `schema`'s top-level `required` array, as field names - `[]` for
/// anything that doesn't parse or declares none, the same "absence reads
/// as empty" treatment `schema_properties` gives `properties`.
fn schema_required(schema: &str) -> Vec<String> {
    serde_json::from_str::<serde_json::Value>(schema)
        .ok()
        .and_then(|v| v.get("required").cloned())
        .and_then(|v| v.as_array().cloned())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Resolves a `FieldPath` - a bare top-level field name, or a two-segment
/// dotted path reaching one leaf inside a named nested shape - to the
/// property schema object it names, per the payload schema shape note
/// above `entity CommandType`. Real for the bare-name case, the common
/// one every obligation propagated so far exercises; a dotted path
/// additionally requires walking one level into the outer field's own
/// nested `properties`, deferred (`todo!()`) to whichever pass needs
/// named reusable nested shapes exercised for real - the same
/// trivial/deferred split every other black box in this module has.
fn resolve_field<'a>(
    properties: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Option<&'a serde_json::Value> {
    match field.split_once('.') {
        None => properties.get(field),
        Some(_) => todo!(
            "resolve_field: one-level dotted-path resolution into a named nested shape - deferred, see doc comment above"
        ),
    }
}

/// Black box (see the note above rule `RegisterEventType`): "every
/// `TagMapping.field` names a field the schema declares" - real for the
/// bare-field-name case (see `resolve_field`). Takes `schema` alone, not
/// a whole `EventType`/`CommandType` - the identical `String` on both,
/// same treatment `protect_sensitive_fields`/`derive_tags` get for
/// `sensitive_fields`/`tag_mappings`.
pub fn valid_tag_mappings(schema: &str, tag_mappings: &[TagMapping]) -> bool {
    if tag_mappings.is_empty() {
        return true;
    }
    let Some(properties) = schema_properties(schema) else {
        return false;
    };
    tag_mappings
        .iter()
        .all(|m| resolve_field(&properties, &m.field).is_some())
}

/// Black box (see the note above rule `RegisterEventType`): the same
/// existence check as `valid_tag_mappings` above, for both
/// `SensitiveField.field` and `SensitiveField.subject_field`.
pub fn valid_sensitive_fields(schema: &str, sensitive_fields: &[SensitiveField]) -> bool {
    if sensitive_fields.is_empty() {
        return true;
    }
    let Some(properties) = schema_properties(schema) else {
        return false;
    };
    sensitive_fields.iter().all(|s| {
        resolve_field(&properties, &s.field).is_some()
            && resolve_field(&properties, &s.subject_field).is_some()
    })
}

/// Black box (see the note above rule `RegisterEventType`): the four-
/// bullet contract stated there, in full - a declared field is
/// permanent, requiredness may only loosen, a newly added field must be
/// optional, and a field's declared `type` never changes. Real, not
/// deferred: unlike `valid_tag_mappings`/`valid_sensitive_fields`, this
/// contract governs the top-level schema only by its own text ("a nested
/// shape's own internal evolution... is not tracked, validated or
/// enforced here at all" - see the note above `entity CommandType`), so
/// there is no dotted-path elaboration left to defer.
pub fn schema_is_backwards_compatible(existing_schema: &str, schema: &str) -> bool {
    if existing_schema == schema {
        return true;
    }
    let (Some(existing_properties), Some(new_properties)) = (
        schema_properties(existing_schema),
        schema_properties(schema),
    ) else {
        return false;
    };
    let existing_required = schema_required(existing_schema);
    let new_required = schema_required(schema);

    for (field, existing_def) in &existing_properties {
        let Some(new_def) = new_properties.get(field) else {
            return false; // bullet 1: a declared field is permanent
        };
        if existing_def.get("type") != new_def.get("type") {
            return false; // bullet 4: a field's declared type never changes
        }
        let was_required = existing_required.iter().any(|f| f == field);
        let now_required = new_required.iter().any(|f| f == field);
        if !was_required && now_required {
            return false; // bullet 2: optional -> required is tightening, forbidden
        }
    }
    for field in new_properties.keys() {
        if !existing_properties.contains_key(field) && new_required.iter().any(|f| f == field) {
            return false; // bullet 3: a newly added field must be optional
        }
    }
    true
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

/// Black box (see the note above rule `DeliverToSubscriptions`, reused by
/// `QueryEvents`/`CountEvents`/`InspectEvent` and `read_projection`'s own
/// event-side counterpart): "leave every field as stored ciphertext -
/// nothing to redact" is the one part of its contract this pass needs and
/// implements for real, the same trivial/deferred split
/// `protect_sensitive_fields` above has. That's not a narrower cut here:
/// until `protect_sensitive_fields`' own non-empty branch actually
/// encrypts a field, no `Event` this engine produces ever has a non-empty
/// `sensitive_fields` type with anything encrypted to decrypt in the
/// first place, so the empty case is every case currently reachable.
/// Decrypting for real - the two-grant test against each field's own
/// `EncryptionKey.subject_value` (`access_mapping.can_read_sensitive`, or
/// `access_mapping.role.external_subject` matching it) - is deferred to
/// whichever pass gives `protect_sensitive_fields` its own real
/// encryption to invert.
pub fn render_event(event: &Event, _access_mapping: &RoleAccessMapping) -> String {
    if event.event_type.sensitive_fields.is_empty() {
        event.payload.clone()
    } else {
        todo!(
            "render_event: two-grant per-field decrypt test against EncryptionKey.subject_value - deferred, see doc comment above"
        )
    }
}

/// See `render_event` above - same black box, same trivial/deferred
/// split, applied to `Command.payload`/`CommandType.sensitive_fields`
/// instead. See `rule FetchCommands`' own `@guidance` for why this is a
/// distinct function rather than `render_event` reused: the two-grant
/// test is identical, but the value being rendered is a `Command`, not an
/// `Event`.
pub fn render_command(command: &Command, _access_mapping: &RoleAccessMapping) -> String {
    if command.command_type.sensitive_fields.is_empty() {
        command.payload.clone()
    } else {
        todo!(
            "render_command: two-grant per-field decrypt test against EncryptionKey.subject_value - deferred, see render_event's doc comment above"
        )
    }
}

/// The outcome of `register_event_type` below - the spec's own `if exists
/// existing: ... else: EventType.created(...)` branch, made explicit
/// rather than folded into a single return value, so a caller (and a
/// test) can tell "this registration created a new row" from "this
/// registration updated one in place" without comparing timestamps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventTypeRegistration {
    Created(EventType),
    Updated(EventType),
}

impl EventTypeRegistration {
    pub fn event_type(&self) -> &EventType {
        match self {
            EventTypeRegistration::Created(et) | EventTypeRegistration::Updated(et) => et,
        }
    }
}

/// See `rule RegisterEventType`. `existing` is `EventType{bounded_context,
/// name}` as already looked up by the caller - the same get-or-create
/// lookup treatment `consume_events`' `existing_cursor` gets. Checks run
/// in the spec's own `requires` order; the two `not exists existing or
/// ...` guards below only fire on the update path, exactly as written.
#[allow(clippy::too_many_arguments)]
pub fn register_event_type(
    access_mapping: &RoleAccessMapping,
    bounded_context: &BoundedContext,
    name: String,
    schema: String,
    tag_mappings: Vec<TagMapping>,
    sensitive_fields: Vec<SensitiveField>,
    external_creation_allowed: bool,
    direct_creation_allowed: bool,
    system_triggered_allowed: bool,
    system_triggered_schedule: Option<String>,
    event_read_allowed: bool,
    existing: Option<&EventType>,
) -> crate::error::Result<EventTypeRegistration> {
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
        return Err(Error::BoundedContextArchived.into());
    }
    if !valid_tag_mappings(&schema, &tag_mappings) {
        return Err(Error::InvalidTagMapping.into());
    }
    if !valid_sensitive_fields(&schema, &sensitive_fields) {
        return Err(Error::InvalidSensitiveField.into());
    }
    if tag_mappings
        .iter()
        .any(|m| sensitive_fields.iter().any(|s| s.field == m.field))
    {
        return Err(Error::SensitiveFieldTagOverlap.into());
    }

    let Some(existing) = existing else {
        return Ok(EventTypeRegistration::Created(EventType {
            bounded_context: bounded_context.clone(),
            name,
            schema,
            schema_version: 1,
            tag_mappings,
            sensitive_fields,
            external_creation_allowed,
            direct_creation_allowed,
            system_triggered_allowed,
            system_triggered_schedule,
            event_read_allowed,
        }));
    };

    if !schema_is_backwards_compatible(&existing.schema, &schema) {
        return Err(Error::SchemaIncompatible.into());
    }
    if !existing
        .tag_mappings
        .iter()
        .all(|m| tag_mappings.iter().any(|n| n.key == m.key))
    {
        return Err(Error::TagMappingKeyDropped.into());
    }

    let schema_changed = existing.schema != schema;
    Ok(EventTypeRegistration::Updated(EventType {
        bounded_context: bounded_context.clone(),
        name,
        schema,
        schema_version: if schema_changed {
            existing.schema_version + 1
        } else {
            existing.schema_version
        },
        tag_mappings,
        sensitive_fields,
        external_creation_allowed,
        direct_creation_allowed,
        system_triggered_allowed,
        system_triggered_schedule,
        event_read_allowed,
    }))
}

/// See `EventTypeRegistration` above - same shape, for `CommandType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandTypeRegistration {
    Created(CommandType),
    Updated(CommandType),
}

impl CommandTypeRegistration {
    pub fn command_type(&self) -> &CommandType {
        match self {
            CommandTypeRegistration::Created(ct) | CommandTypeRegistration::Updated(ct) => ct,
        }
    }
}

/// See `rule RegisterCommandType`. Same shape and reasoning as
/// `register_event_type` above - the only differences are the absence of
/// the three `EventType`-only permission fields and the presence of
/// `rest_trigger_allowed` in their place.
#[allow(clippy::too_many_arguments)]
pub fn register_command_type(
    access_mapping: &RoleAccessMapping,
    bounded_context: &BoundedContext,
    name: String,
    schema: String,
    tag_mappings: Vec<TagMapping>,
    sensitive_fields: Vec<SensitiveField>,
    rest_trigger_allowed: bool,
    existing: Option<&CommandType>,
) -> crate::error::Result<CommandTypeRegistration> {
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
        return Err(Error::BoundedContextArchived.into());
    }
    if !valid_tag_mappings(&schema, &tag_mappings) {
        return Err(Error::InvalidTagMapping.into());
    }
    if !valid_sensitive_fields(&schema, &sensitive_fields) {
        return Err(Error::InvalidSensitiveField.into());
    }
    if tag_mappings
        .iter()
        .any(|m| sensitive_fields.iter().any(|s| s.field == m.field))
    {
        return Err(Error::SensitiveFieldTagOverlap.into());
    }

    let Some(existing) = existing else {
        return Ok(CommandTypeRegistration::Created(CommandType {
            bounded_context: bounded_context.clone(),
            name,
            schema,
            schema_version: 1,
            tag_mappings,
            sensitive_fields,
            rest_trigger_allowed,
        }));
    };

    if !schema_is_backwards_compatible(&existing.schema, &schema) {
        return Err(Error::SchemaIncompatible.into());
    }
    if !existing
        .tag_mappings
        .iter()
        .all(|m| tag_mappings.iter().any(|n| n.key == m.key))
    {
        return Err(Error::TagMappingKeyDropped.into());
    }

    let schema_changed = existing.schema != schema;
    Ok(CommandTypeRegistration::Updated(CommandType {
        bounded_context: bounded_context.clone(),
        name,
        schema,
        schema_version: if schema_changed {
            existing.schema_version + 1
        } else {
            existing.schema_version
        },
        tag_mappings,
        sensitive_fields,
        rest_trigger_allowed,
    }))
}

/// See `rule ForgetSubject`. Same grant-shape checks
/// `archive_bounded_context`/`revoke_token` make (`access_mapping.status`/
/// `.level`/`.bounded_context`), reusing the same
/// `access_control::Error` variants, plus this module's own
/// `EncryptionKeyNotActive` for "already destroyed" - the same rejection
/// register `BoundedContextArchived`/`TokenNotActive` are in. Crypto-
/// shredding itself needs nothing beyond this status flip: every payload
/// ciphertext written under this key becomes unreadable the instant no
/// plaintext key material exists to reverse it, which this function's own
/// caller enacts by destroying the key row - there is no second action
/// this pure function performs or defers.
pub fn forget_subject(
    access_mapping: &RoleAccessMapping,
    key: &EncryptionKey,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<EncryptionKey> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if access_mapping.bounded_context != key.bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if key.status != EncryptionKeyStatus::Active {
        return Err(Error::EncryptionKeyNotActive.into());
    }

    Ok(EncryptionKey {
        status: EncryptionKeyStatus::Destroyed,
        destroyed_at: Some(now),
        ..key.clone()
    })
}

/// See `rule QueryEvents`. `bounded_context_events` is every `Event` this
/// engine currently knows of for `access_mapping.bounded_context` - the
/// same full-snapshot treatment `consistency_boundary_and_matching_events`
/// takes `Event`s in, standing in for the in-memory per-bounded-context
/// event cache the spec's own note describes (not modelled itself - see
/// this module's own doc comment). An empty `event_types`/`tags` means
/// "no restriction", per the spec's own convention - not a vacuous case,
/// so both are checked with `is_empty()`/`is_none()` rather than treated
/// as "matches nothing". Returns the rendered payloads only, exactly what
/// `EventsQueried.events: map(candidates, e => render_event(e,
/// access_mapping))` binds - the wire contract around what else a caller
/// sees per event stays deferred, same register as everywhere else this
/// spec is "deliberately coarse on the wire contract" (see the surface's
/// own guidance).
pub fn query_events(
    access_mapping: &RoleAccessMapping,
    event_types: &[EventType],
    tags: Option<&[Tag]>,
    after_sequence: Option<i64>,
    bounded_context_events: &[Event],
) -> crate::error::Result<Vec<String>> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if !event_types
        .iter()
        .all(|et| et.bounded_context == access_mapping.bounded_context)
    {
        return Err(Error::EventTypeNotInBoundedContext.into());
    }

    let after = after_sequence.unwrap_or(-1);
    Ok(bounded_context_events
        .iter()
        .filter(|e| e.bounded_context == access_mapping.bounded_context)
        .filter(|e| event_types.is_empty() || event_types.contains(&e.event_type))
        .filter(|e| tags.is_none_or(|wanted| wanted.iter().any(|t| e.tags.contains(t))))
        .filter(|e| e.sequence > after)
        .map(|e| render_event(e, access_mapping))
        .collect())
}

/// See `rule CountEvents`. Same shape and narrowing as `query_events`
/// above, minus the `after_sequence` cursor - a count is a live aggregate
/// over everything matching, not a page of it (see the note above the
/// rule).
pub fn count_events(
    access_mapping: &RoleAccessMapping,
    event_types: &[EventType],
    tags: Option<&[Tag]>,
    bounded_context_events: &[Event],
) -> crate::error::Result<i64> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if !event_types
        .iter()
        .all(|et| et.bounded_context == access_mapping.bounded_context)
    {
        return Err(Error::EventTypeNotInBoundedContext.into());
    }

    Ok(bounded_context_events
        .iter()
        .filter(|e| e.bounded_context == access_mapping.bounded_context)
        .filter(|e| event_types.is_empty() || event_types.contains(&e.event_type))
        .filter(|e| tags.is_none_or(|wanted| wanted.iter().any(|t| e.tags.contains(t))))
        .count() as i64)
}

/// See `rule InspectEvent`'s own `ensures` - `event` and `rendered_payload`
/// delivered alongside each other, not one folded into the other, since
/// `event` carries provenance (`metadata`, `origin`) `render_event`'s
/// output alone doesn't.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventInspected {
    pub event: Event,
    pub rendered_payload: String,
}

/// See `rule InspectEvent`. Unlike `EventTypeAdminOperations`'s `event_type`
/// context binding, `event` here is a caller-supplied argument the surface's
/// own `context event: Event where bounded_context = access_mapping.bounded_context`
/// scopes structurally on the GraphQL side, but `RoleAccessMapping` and
/// `Event` are independent entities with no field tying them together -
/// the same "explicit check, not derived" treatment
/// `authorise_command_submission`'s bounded-context match gets.
pub fn inspect_event(
    access_mapping: &RoleAccessMapping,
    event: &Event,
) -> crate::error::Result<EventInspected> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if access_mapping.bounded_context != event.bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }

    Ok(EventInspected {
        event: event.clone(),
        rendered_payload: render_event(event, access_mapping),
    })
}

/// See `rule FetchCommands`. `bounded_context_commands` is every `Command`
/// this engine currently knows of for `access_mapping.bounded_context` -
/// the same full-snapshot treatment `query_events`'/`count_events`'
/// `bounded_context_events` get. `triggered_event in triggered_events` -
/// the reverse "which command produced this event" lookup - has no
/// separate relationship to resolve: `Command.triggered_events` is itself
/// a caller-resolved relationship projection everywhere else in this
/// codebase, but here it doesn't need resolving at all, since a
/// candidate's own membership in it is exactly "does `triggered_event`'s
/// `origin` name this candidate" - already answerable from
/// `EventOrigin::CommandTriggered`'s own boxed `Command` field, with no
/// second collection required. Returns rendered payloads only, the same
/// "deliberately coarse on the wire contract" choice `query_events` makes
/// for `EventsQueried.events` - see its own doc comment.
pub fn fetch_commands(
    access_mapping: &RoleAccessMapping,
    command_types: &[CommandType],
    after: Option<chrono::DateTime<chrono::Utc>>,
    before: Option<chrono::DateTime<chrono::Utc>>,
    triggered_event: Option<&Event>,
    bounded_context_commands: &[Command],
) -> crate::error::Result<Vec<String>> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if !command_types
        .iter()
        .all(|ct| ct.bounded_context == access_mapping.bounded_context)
    {
        return Err(Error::CommandTypeNotInBoundedContext.into());
    }
    if triggered_event.is_some_and(|te| te.bounded_context != access_mapping.bounded_context) {
        return Err(Error::TriggeredEventNotInBoundedContext.into());
    }

    Ok(bounded_context_commands
        .iter()
        .filter(|c| c.bounded_context == access_mapping.bounded_context)
        .filter(|c| command_types.is_empty() || command_types.contains(&c.command_type))
        .filter(|c| after.is_none_or(|a| c.metadata.created_at >= a))
        .filter(|c| before.is_none_or(|b| c.metadata.created_at <= b))
        .filter(|c| {
            triggered_event.is_none_or(|te| match &te.origin {
                EventOrigin::CommandTriggered { command } => command.as_ref() == *c,
                _ => false,
            })
        })
        .map(|c| render_command(c, access_mapping))
        .collect())
}

/// See `rule CreateAllEventsSubscription`. `bounded_context_events` is
/// every `Event` this engine currently knows of for
/// `access_mapping.bounded_context`, for `starting_point`'s default - the
/// same full-snapshot treatment `query_events`' `bounded_context_events`
/// gets. The spec's own `(Events where ...).count - 1` and computing the
/// highest sequence directly are equivalent given
/// `SequenceIsGaplessPerBoundedContext` (both read as "the bounded
/// context's current latest committed sequence, or -1 when empty"); this
/// takes the latter, more direct route.
pub fn create_all_events_subscription(
    access_mapping: &RoleAccessMapping,
    event_types: Vec<EventType>,
    from_sequence: Option<i64>,
    bounded_context_events: &[Event],
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<AllEventsSubscription> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.bounded_context.status != BoundedContextStatus::Active {
        return Err(Error::BoundedContextArchived.into());
    }
    if !event_types
        .iter()
        .all(|et| et.bounded_context == access_mapping.bounded_context)
    {
        return Err(Error::EventTypeNotInBoundedContext.into());
    }

    let starting_point = from_sequence.unwrap_or_else(|| {
        bounded_context_events
            .iter()
            .filter(|e| e.bounded_context == access_mapping.bounded_context)
            .map(|e| e.sequence)
            .max()
            .unwrap_or(-1)
    });

    Ok(AllEventsSubscription {
        bounded_context: access_mapping.bounded_context.clone(),
        access_mapping: access_mapping.clone(),
        created_at: now,
        from_sequence: starting_point,
        event_types,
    })
}

/// See `rule CreateEventTypeSubscription`. Same `starting_point` treatment
/// as `create_all_events_subscription` above.
pub fn create_event_type_subscription(
    access_mapping: &RoleAccessMapping,
    event_type: &EventType,
    filters: Vec<Filter>,
    from_sequence: Option<i64>,
    bounded_context_events: &[Event],
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<EventTypeSubscription> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.bounded_context != event_type.bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if event_type.bounded_context.status != BoundedContextStatus::Active {
        return Err(Error::BoundedContextArchived.into());
    }
    if !valid_filters(event_type, &filters) {
        return Err(Error::InvalidFilter.into());
    }

    let starting_point = from_sequence.unwrap_or_else(|| {
        bounded_context_events
            .iter()
            .filter(|e| e.bounded_context == event_type.bounded_context)
            .map(|e| e.sequence)
            .max()
            .unwrap_or(-1)
    });

    Ok(EventTypeSubscription {
        bounded_context: event_type.bounded_context.clone(),
        access_mapping: access_mapping.clone(),
        created_at: now,
        from_sequence: starting_point,
        event_type: event_type.clone(),
        filters,
    })
}

/// See `rule DeliverToSubscriptions`'s own `ensures` - `event` is
/// delivered alongside `rendered_payload`, the same "not folded into one
/// value" treatment `EventInspected` gives the two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventDelivered {
    pub subscription: Subscription,
    pub event: Event,
    pub rendered_payload: String,
}

/// See `rule DeliverToSubscriptions`. No `requires` clause in the spec -
/// this rule is an unconditional `for subscription in Subscriptions
/// where ...` loop triggered by `Event.created`, not gated by anything a
/// caller can fail, so unlike every other rule in this module it returns
/// a plain `Vec`, not a `Result`. The `where` clause's three conditions
/// (`bounded_context = event.bounded_context`, `from_sequence <
/// event.sequence`, `access_mapping.status = active`) are this
/// function's own filter, not the caller's to pre-apply - `subscriptions`
/// is every `Subscription` this engine currently knows of, the usual
/// full-snapshot treatment. Real delivery is asynchronous and best-effort
/// per the note above the rule (outside any request's transaction); this
/// function is the pure "who matches and what do they get" computation
/// underneath that, the same split `project()`'s own async/sync halves
/// have.
pub fn deliver_to_subscriptions(
    event: &Event,
    subscriptions: &[Subscription],
) -> Vec<EventDelivered> {
    subscriptions
        .iter()
        .filter(|s| {
            s.bounded_context() == &event.bounded_context
                && s.starting_sequence() < event.sequence
                && s.access_mapping().status == RoleStatus::Active
        })
        .filter(|s| match s {
            Subscription::AllEventsSubscription(a) => {
                a.event_types.is_empty() || a.event_types.contains(&event.event_type)
            }
            Subscription::EventTypeSubscription(e) => {
                e.event_type == event.event_type && matches_filters(event, &e.filters)
            }
        })
        .map(|s| EventDelivered {
            subscription: s.clone(),
            event: event.clone(),
            rendered_payload: render_event(event, s.access_mapping()),
        })
        .collect()
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
    /// Boxed: `ReadCursor` embeds a whole `EventReadToken` (itself grown to
    /// the full `AccessToken` base shape - `secret`/`created_at`/
    /// `revoked_at` - propagating `TokenRevocation`), which would otherwise
    /// size every `CursorUpdate` - including the far more common
    /// `Advanced`/`Unchanged` ones - to this variant's worst case. Same
    /// reasoning as `EventOrigin::CommandTriggered`'s boxed `Command`.
    Created(Box<ReadCursor>),
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
        CursorUpdate::Created(Box::new(ReadCursor {
            token: token.clone(),
            ack_mode: mode,
            sequence: next_position,
            updated_at: now,
        }))
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

/// See `rule AuthoriseCommandSubmission`. The GraphQL/Role counterpart to
/// `authorise_command_trigger` above - same `CommandAuthorised` hand-off,
/// same reused `Error::BoundedContextArchived` for
/// `command_type.bounded_context.status = active`, but authorised against
/// a `RoleAccessMapping` rather than a `CommandToken`: `access_mapping.role.id`
/// becomes `client_id` (a `CommandToken` has no `Role` to attribute to, so
/// it uses its own `id` instead), and `access_mapping.bounded_context =
/// command_type.bounded_context` is an explicit check here rather than
/// structurally derived, since - unlike `CommandToken.command_type` - a
/// `RoleAccessMapping` and a `CommandType` are independent entities with no
/// field tying them together short of comparing this pair.
pub fn authorise_command_submission(
    access_mapping: &RoleAccessMapping,
    command_type: &CommandType,
    payload: String,
) -> crate::error::Result<CommandAuthorised> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if !matches!(
        access_mapping.level,
        AccessLevel::Write | AccessLevel::Admin
    ) {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if access_mapping.bounded_context != command_type.bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if command_type.bounded_context.status != BoundedContextStatus::Active {
        return Err(Error::BoundedContextArchived.into());
    }

    Ok(CommandAuthorised {
        command_type: command_type.clone(),
        payload,
        client_id: access_mapping.role.id.clone(),
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
