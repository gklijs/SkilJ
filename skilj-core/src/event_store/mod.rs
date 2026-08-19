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
use crate::encryption::DataKey;
use crate::error::SkiljRejection;
use crate::shared::{
    CommandDecision, Filter, FilterOperator, Metadata, SensitiveField, Tag, TagMapping,
};

// The in-memory per-bounded-context event cache the spec describes
// (drift audit finding #8, closed 2026-08-19) lives in `crate::
// event_cache` - `db::list_events_cached`/
// `list_events_for_bounded_context_cached`/`get_event_by_sequence_cached`
// are what `query_events`/`count_events`/`fetch_events`/`consume_events`/
// `process_command`'s own callers now read through instead of an
// unconditional Postgres load; `query_events`/`count_events` below still
// just take `bounded_context_events: &[Event]` as a caller-supplied
// snapshot, unchanged - which snapshot that is (cache-served or a
// Postgres fallback) is entirely the caller's own concern, the same
// "pure function, I/O resolved before the call" split every other rule
// in this module already follows. `create_all_events_subscription`/
// `create_event_type_subscription` take the identical shape of snapshot
// but are deliberately NOT wired to the cache - see `crate::event_cache`'s
// own module doc comment for which four rule-groups are and aren't.
// `Event.sequence` and every field/argument derived from it are `i64` -
// see docs/architecture.md §2.2.1 - not `i32`, to avoid the overflow
// risk that decision exists to close.
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

/// See `enum MissedOccurrencePolicy`. No `Default` impl, deliberately -
/// see that type's own doc comment on why this library never picks one
/// on a bounded context's behalf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissedOccurrencePolicy {
    Skip,
    FireOnce,
    ReplayBacklog,
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
    /// See `EventOrigin::SystemTriggered`'s registration opt-in, and
    /// `rule CreateSystemEvent`/`rule SkipMissedOccurrences`, the real
    /// scheduler mechanism these fields drive.
    pub system_triggered_allowed: bool,
    pub system_triggered_schedule: Option<String>,
    /// What this type owes for an occurrence of its own schedule that
    /// passed while nothing was watching for it - `None` for a type that
    /// hasn't opted into scheduling (nothing to choose), mandatory the
    /// moment it has (see `register_event_type`'s own
    /// `MissingScheduleOrPolicy` guard and invariant
    /// `ScheduledTypeIsFullyConfigured`). No default, anywhere - see
    /// `MissedOccurrencePolicy`'s own doc comment.
    pub missed_occurrence_policy: Option<MissedOccurrencePolicy>,
    /// How far this type's schedule has been accounted for: every
    /// occurrence at or before this instant has either produced an event
    /// or been deliberately passed over, and none of them is considered
    /// again. An instant, not an occurrence index - see `create_system_event`'s
    /// own doc comment for why that's what lets a later schedule-string
    /// change not reopen anything. `None` exactly when
    /// `missed_occurrence_policy` is `None`.
    pub schedule_position: Option<chrono::DateTime<chrono::Utc>>,
    /// The occurrence instant of the most recent occurrence of this
    /// type's schedule that actually produced an event - not the wall
    /// clock of the write, which is already on that event's own
    /// `Metadata.created_at` (see `create_system_event`'s own doc
    /// comment). `None` until the first one fires; never reset by
    /// switching scheduling off.
    pub last_fired_at: Option<chrono::DateTime<chrono::Utc>>,
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
/// encrypt under the very same key. Both provisioning
/// (`protect_sensitive_fields`'s get-or-create) and destruction
/// (`forget_subject`) are real.
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

    /// See `rule RegisterEventType`'s own note on scheduling being
    /// "opted into whole": `requires: system_triggered_allowed = true
    /// implies (system_triggered_schedule != null and
    /// missed_occurrence_policy != null)`. A real new rejection this
    /// pass introduced - a registration with `system_triggered_allowed:
    /// true` and no schedule used to be silently accepted (and simply
    /// never fired); it no longer is, since the same "no default" reasoning
    /// that now requires a policy would be incoherent if it left the
    /// schedule itself still optional.
    #[error(
        "a scheduled EventType must declare both system_triggered_schedule and \
         missed_occurrence_policy - neither has a default"
    )]
    MissingScheduleOrPolicy,

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

    /// `valid_payload(schema, payload)` rejected a caller-supplied
    /// payload at the one of four boundary crossings that checks it -
    /// see the note above `entity CommandType`'s "Payload schema shape"
    /// for why `decide()`'s `EventSpec`s and `scheduled_payload`'s own
    /// output are deliberately not covered by this variant; those are an
    /// obligation on the implementer, never runtime-checked.
    #[error("this payload does not validate against its type's registered schema")]
    PayloadDoesNotMatchSchema,

    /// Not spec-modeled: the spec's own `ProcessCommand` assumes `decide()`
    /// only ever names an `EventType` its bounded context actually
    /// registered - a plugin-author responsibility, not a case the spec
    /// enumerates a rejection for. Kept here anyway as a defensive check
    /// rather than an `unwrap()`/panic, since `decide()` is exactly the
    /// kind of black-box plugin code a misconfigured bounded context could
    /// get wrong.
    #[error("decide() named an event type this bounded context has not registered: {0}")]
    UnregisteredEventType(String),

    /// Not spec-modeled either, same register as `UnregisteredEventType`
    /// right above: a command payload, or a stored `Event`'s own payload,
    /// didn't deserialize into the compiled binary's typed
    /// `CommandType::Payload`/`BoundedContextEvent`-implementing enum
    /// variant. `valid_payload`/`PayloadDoesNotMatchSchema` closes this
    /// for a caller-supplied payload that's outright schema-invalid, but
    /// this stays reachable for the narrower gap `valid_payload` was
    /// never meant to cover: the compiled Rust type can still be
    /// *stricter* than the registered JSON Schema (e.g. an
    /// `#[serde(deny_unknown_fields)]`, a numeric type narrower than
    /// `"number"`, an enum whose variants the schema's own `"string"`
    /// doesn't enumerate), and a `decide()`/`scheduled_payload`-produced
    /// payload is never schema-checked at all (see the note above
    /// `entity CommandType`'s "Payload schema shape") - so this remains a
    /// real, unaddressed decode failure mode for a plugin-authored
    /// payload specifically. See docs/architecture.md §1.6/§1.7 for where
    /// this is raised (the `skilj` facade's decide()-dispatch bridge).
    #[error("stored payload did not decode into its expected type: {0}")]
    PayloadDecodeFailed(String),
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::BoundedContextArchived => "bounded_context_archived",
            Error::SchemaIncompatible => "schema_incompatible",
            Error::MissingScheduleOrPolicy => "missing_schedule_or_policy",
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
            Error::PayloadDoesNotMatchSchema => "payload_does_not_match_schema",
            Error::UnregisteredEventType(_) => "unregistered_event_type",
            Error::PayloadDecodeFailed(_) => "payload_decode_failed",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

/// A resolved leaf schema's own shape, classified for the "reject
/// anything that does not land on a scalar or list-of-scalar leaf" check
/// `valid_tag_mappings`/`valid_sensitive_fields`/`valid_filters` all share
/// (see the payload schema shape note above `entity CommandType`) -
/// `valid_filters` additionally uses the `Scalar` case's own `json_type`/
/// `format` to pick which `FilterOperator`s are valid for that field.
/// Follows a bare `$ref` itself: `schemars` renders a unit enum via
/// `$ref` even for a *bare* top-level field (e.g.
/// `{"$ref": "#/definitions/OrderStatus"}` where `OrderStatus` is itself
/// `{"type": "string", "enum": [...]}`) - a plain scalar leaf, just
/// declared indirectly, unlike a `$ref` resolving to a real nested-object
/// shape (has its own `"properties"` rather than a scalar `"type"`),
/// which stays `Other` for a bare reference (needs one more dot to reach
/// a leaf inside it). `resolve_field`'s own dotted branch only follows a
/// `$ref` when a caller supplies a dot, so a bare reference still needs
/// resolving here.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FieldKind {
    Scalar {
        json_type: &'static str,
        format: Option<String>,
    },
    ListOfScalar,
    Other,
}

/// A JSON Schema `"type"` value as one string - handles both the plain
/// `"string"` shape and the `["string","null"]` shape an `Option<T>`
/// field's `type` carries (nullability itself is decided from `required`
/// elsewhere - this only recovers the *scalar* kind either shape names).
/// Mirrors `skilj-graphql::projection_types::json_type_str` exactly - the
/// identical extraction, needed again here since `skilj-core` can't
/// depend on `skilj-graphql`.
fn json_type_str(type_value: &serde_json::Value) -> Option<&str> {
    match type_value {
        serde_json::Value::String(s) => Some(s.as_str()),
        serde_json::Value::Array(arr) => {
            arr.iter().find_map(|v| v.as_str().filter(|s| *s != "null"))
        }
        _ => None,
    }
}

fn classify(
    schema: &serde_json::Value,
    definitions: Option<&serde_json::Map<String, serde_json::Value>>,
) -> FieldKind {
    if let Some(def_name) = schema
        .get("$ref")
        .and_then(|v| v.as_str())
        .and_then(|r| r.rsplit('/').next())
    {
        return definitions
            .and_then(|defs| defs.get(def_name))
            .map(|resolved| classify(resolved, definitions))
            .unwrap_or(FieldKind::Other);
    }
    match schema.get("type").and_then(json_type_str) {
        Some(t @ ("string" | "integer" | "number" | "boolean")) => FieldKind::Scalar {
            // Interned via a small match, not the borrowed &str - FieldKind::Scalar
            // needs 'static so it can be constructed without carrying schema's own lifetime.
            json_type: match t {
                "string" => "string",
                "integer" => "integer",
                "number" => "number",
                _ => "boolean",
            },
            format: schema
                .get("format")
                .and_then(|f| f.as_str())
                .map(String::from),
        },
        Some("array") => match schema
            .get("items")
            .and_then(|i| i.get("type"))
            .and_then(json_type_str)
        {
            Some("string" | "integer" | "number" | "boolean") => FieldKind::ListOfScalar,
            _ => FieldKind::Other, // an array of non-scalar items - not a target either
        },
        _ => FieldKind::Other, // "object" (incl. a $ref resolved to one), unknown, or absent
    }
}

/// `resolve_field` plus the shape check every caller of it here actually
/// needs: `None` when the field doesn't resolve at all, or resolves to
/// something that isn't a scalar or list-of-scalar leaf.
fn resolve_field_kind(
    properties: &serde_json::Map<String, serde_json::Value>,
    definitions: Option<&serde_json::Map<String, serde_json::Value>>,
    field: &str,
) -> Option<FieldKind> {
    match classify(resolve_field(properties, definitions, field)?, definitions) {
        FieldKind::Other => None,
        kind => Some(kind),
    }
}

/// The type-to-operator matrix (see the note above rule `FetchEvents`:
/// "the exact type-to-operator matrix... is deliberately not enumerated
/// here... exactly the kind of mechanism this spec consistently hands to
/// a black box"). `format` values `"date-time"`/`"date"`/
/// `"partial-date-time"` (`chrono::DateTime<Utc>`/`NaiveDate`/`NaiveTime`'s
/// own three renderings) additionally get ordering operators - see
/// `matches_filters`' own `string_ordering` for why the comparison itself
/// needs real parsing, not plain string `Ord`, despite the check here
/// being schema-driven.
fn filter_operator_is_valid(kind: &FieldKind, operator: FilterOperator) -> bool {
    match kind {
        FieldKind::Scalar {
            json_type: "string",
            format,
        } => {
            matches!(
                operator,
                FilterOperator::Equals | FilterOperator::Contains | FilterOperator::IsLike
            ) || (matches!(
                format.as_deref(),
                Some("date-time" | "date" | "partial-date-time")
            ) && matches!(
                operator,
                FilterOperator::GreaterThan | FilterOperator::LessThan
            ))
        }
        FieldKind::Scalar {
            json_type: "integer" | "number",
            ..
        } => matches!(
            operator,
            FilterOperator::Equals | FilterOperator::GreaterThan | FilterOperator::LessThan
        ),
        FieldKind::Scalar {
            json_type: "boolean",
            ..
        } => operator == FilterOperator::Equals,
        FieldKind::Scalar { .. } => false, // unreachable - classify only ever sets one of the four above
        FieldKind::ListOfScalar => operator == FilterOperator::Contains,
        FieldKind::Other => false,
    }
}

/// Black box shared with `QueryEvents`/`CountEvents` (see the note above
/// rule `FetchEvents` and docs/architecture.md's "black boxes" list) -
/// not owned by the EventFetch pilot. Checks two things per filter: that
/// `field` names a field `event_type.schema` actually declares (bare or
/// a two-segment dotted path - see `resolve_field`), and that `operator`
/// is one that field's declared type supports (see `filter_operator_is_valid`).
pub fn valid_filters(event_type: &EventType, filters: &[Filter]) -> bool {
    if filters.is_empty() {
        return true;
    }
    let Some(properties) = schema_properties(&event_type.schema) else {
        return false;
    };
    let definitions = schema_definitions(&event_type.schema);
    filters.iter().all(|f| {
        resolve_field_kind(&properties, definitions.as_ref(), &f.field)
            .is_some_and(|kind| filter_operator_is_valid(&kind, f.operator))
    })
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

/// `schema`'s own top-level `"definitions"` map - named reusable nested
/// shapes a `"$ref"` elsewhere in the same schema points at (see
/// `skilj-graphql::projection_types`'s own identical extraction for
/// GraphQL type generation). `None` for anything that doesn't parse or
/// declares none, the same "absence reads as empty" treatment
/// `schema_properties` gives `properties`.
fn schema_definitions(schema: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    let parsed: serde_json::Value = serde_json::from_str(schema).ok()?;
    parsed.get("definitions")?.as_object().cloned()
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
/// above `entity CommandType`. The bare-name case is a direct
/// `properties` lookup; a dotted path resolves its outer segment in
/// `properties`, follows that property's own `"$ref"` into `definitions`
/// (the same pattern `skilj-graphql::projection_types::build_field`
/// already uses for GraphQL type generation, reused for consistency
/// rather than reinvented), then looks the inner segment up in the
/// resolved nested shape's own `properties`. `?`-chained throughout, so
/// anything not shaped exactly right (no `$ref`, unknown definition
/// name, no nested `properties`) falls through to `None` - "reject,
/// don't guess", same as the bare-name case already does.
fn resolve_field<'a>(
    properties: &'a serde_json::Map<String, serde_json::Value>,
    definitions: Option<&'a serde_json::Map<String, serde_json::Value>>,
    field: &str,
) -> Option<&'a serde_json::Value> {
    match field.split_once('.') {
        None => properties.get(field),
        Some((outer, inner)) => {
            let def_name = properties
                .get(outer)?
                .get("$ref")?
                .as_str()?
                .rsplit('/')
                .next()?;
            definitions?.get(def_name)?.get("properties")?.get(inner)
        }
    }
}

/// `resolve_field`'s own counterpart against a *payload* (real JSON data)
/// rather than a *schema* (property definitions) - `protect_sensitive_fields`'s
/// own field/subject_field lookup. No `definitions` needed here: real
/// data has no `$ref`s, so a dotted path is simply nested `get` calls -
/// absence at either segment falls through to `None`, the same as the
/// bare-name case already does.
fn payload_field_value<'a>(
    payload: &'a serde_json::Value,
    field: &str,
) -> Option<&'a serde_json::Value> {
    match field.split_once('.') {
        None => payload.get(field),
        Some((outer, inner)) => payload.get(outer)?.get(inner),
    }
}

/// The mutable counterpart to `payload_field_value` - `protect_sensitive_fields`'s
/// own substitution step needs a place to write ciphertext back into.
fn payload_field_value_mut<'a>(
    payload: &'a mut serde_json::Value,
    field: &str,
) -> Option<&'a mut serde_json::Value> {
    match field.split_once('.') {
        None => payload.get_mut(field),
        Some((outer, inner)) => payload.get_mut(outer)?.get_mut(inner),
    }
}

/// A payload field's own JSON value, as the plain string
/// `EncryptionKey.subject_value`/`protect_sensitive_fields`'s own
/// plaintext-to-encrypt both need - a bare JSON string is used as-is; any
/// other scalar (number, boolean) is rendered via its own JSON literal
/// text. `None` for anything structured (object/array/null), which has no
/// sound single-string reading.
fn json_scalar_to_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => Some(value.to_string()),
        _ => None,
    }
}

/// Black box (see the note above rule `RegisterEventType`): "every
/// `TagMapping.field` names a field the schema declares" - covers both a
/// bare field name and a two-segment dotted path into a named nested
/// shape, and rejects a field that doesn't land on a scalar or
/// list-of-scalar leaf (see `resolve_field_kind` - the payload schema
/// shape note above `entity CommandType` calls for this on all three of
/// `valid_tag_mappings`/`valid_sensitive_fields`/`valid_filters`, not
/// just the last). Takes `schema` alone, not a whole `EventType`/
/// `CommandType` - the identical `String` on both, same treatment
/// `protect_sensitive_fields`/`derive_tags` get for `sensitive_fields`/
/// `tag_mappings`.
pub fn valid_tag_mappings(schema: &str, tag_mappings: &[TagMapping]) -> bool {
    if tag_mappings.is_empty() {
        return true;
    }
    let Some(properties) = schema_properties(schema) else {
        return false;
    };
    let definitions = schema_definitions(schema);
    tag_mappings
        .iter()
        .all(|m| resolve_field_kind(&properties, definitions.as_ref(), &m.field).is_some())
}

/// Black box (see the note above rule `RegisterEventType`): the same
/// existence-and-shape check as `valid_tag_mappings` above, for both
/// `SensitiveField.field` and `SensitiveField.subject_field`.
pub fn valid_sensitive_fields(schema: &str, sensitive_fields: &[SensitiveField]) -> bool {
    if sensitive_fields.is_empty() {
        return true;
    }
    let Some(properties) = schema_properties(schema) else {
        return false;
    };
    let definitions = schema_definitions(schema);
    sensitive_fields.iter().all(|s| {
        resolve_field_kind(&properties, definitions.as_ref(), &s.field).is_some()
            && resolve_field_kind(&properties, definitions.as_ref(), &s.subject_field).is_some()
    })
}

/// Black box (see the note above `entity CommandType`'s "Payload schema
/// shape"): does `payload`, read as JSON, validate against the JSON
/// Schema string `schema`. Checked wherever a caller-supplied payload
/// crosses into the system - `create_external_event`/`create_direct_event`
/// for an event's, `authorise_command_trigger`/`authorise_command_submission`
/// for a command's - and nowhere else: a payload produced by a bounded
/// context's own compiled-in code (`decide()`'s `EventSpec`s,
/// `scheduled_payload`) carries the same obligation but is never
/// re-checked at runtime (see that note's own explanation of why). `false`
/// for a `schema`/`payload` that doesn't even parse as JSON, or a
/// `schema` that isn't itself a valid JSON Schema document - the same
/// "malformed input never validates" register `schema_properties`/
/// `schema_definitions` already use elsewhere in this module, just
/// surfaced here as an outright rejection rather than an absent
/// `properties`/`definitions` map.
pub fn valid_payload(schema: &str, payload: &str) -> bool {
    let Ok(schema_value) = serde_json::from_str::<serde_json::Value>(schema) else {
        return false;
    };
    let Ok(payload_value) = serde_json::from_str::<serde_json::Value>(payload) else {
        return false;
    };
    let Ok(validator) = jsonschema::validator_for(&schema_value) else {
        return false;
    };
    validator.is_valid(&payload_value)
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

/// Classic SQL-LIKE matching for `FilterOperator::IsLike` - `%` matches
/// any run of characters (including none), `_` matches exactly one
/// character, everything else matches itself literally. Case-sensitive,
/// whole-string anchored (no implicit substring search - that's what
/// `Contains` is for). Standard DP wildcard-matching, operating on
/// `Vec<char>` for UTF-8 safety rather than byte indexing; no `regex`
/// dependency needed for this.
fn like_matches(text: &str, pattern: &str) -> bool {
    let text: Vec<char> = text.chars().collect();
    let pattern: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; pattern.len() + 1]; text.len() + 1];
    dp[0][0] = true;
    for j in 1..=pattern.len() {
        if pattern[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        }
    }
    for i in 1..=text.len() {
        for j in 1..=pattern.len() {
            dp[i][j] = match pattern[j - 1] {
                '%' => dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i - 1][j - 1],
                c => dp[i - 1][j - 1] && text[i - 1] == c,
            };
        }
    }
    dp[text.len()][pattern.len()]
}

/// `GreaterThan`/`LessThan` for a string leaf. `matches_filters` has no
/// schema, so unlike `valid_filters` it can't know in advance which of
/// the three ordered formats (`date-time`/`date`/`partial-date-time`)
/// it's looking at - tries each of `chrono::DateTime`'s/`NaiveDate`'s/
/// `NaiveTime`'s own parsers in turn on *both* sides, since each format
/// only parses its own shape. Real typed comparison (`PartialOrd`), not
/// raw string `Ord` - plain string comparison is provably wrong here:
/// chrono's own serde serialization trims trailing-zero fractional
/// digits, even down to no fractional part at all when it's exactly
/// zero, so e.g. `"...:00Z"` sorts *after* `"...:00.500Z"` as plain
/// strings despite being chronologically earlier (`'Z'` > `'.'`).
/// `valid_filters` already gated the field to one of these three formats
/// before a caller could reach this at all; a value that doesn't parse
/// under any of them here just doesn't match - the same "reject
/// gracefully rather than panic" register as everywhere else in this
/// module.
fn string_ordering(payload: &str, filter_value: &str, operator: FilterOperator) -> bool {
    fn cmp<T: PartialOrd>(a: T, b: T, operator: FilterOperator) -> bool {
        match operator {
            FilterOperator::GreaterThan => a > b,
            FilterOperator::LessThan => a < b,
            _ => false,
        }
    }
    if let (Ok(a), Ok(b)) = (
        chrono::DateTime::parse_from_rfc3339(payload),
        chrono::DateTime::parse_from_rfc3339(filter_value),
    ) {
        return cmp(a, b, operator);
    }
    if let (Ok(a), Ok(b)) = (
        chrono::NaiveDate::parse_from_str(payload, "%Y-%m-%d"),
        chrono::NaiveDate::parse_from_str(filter_value, "%Y-%m-%d"),
    ) {
        return cmp(a, b, operator);
    }
    if let (Ok(a), Ok(b)) = (
        chrono::NaiveTime::parse_from_str(payload, "%H:%M:%S%.f"),
        chrono::NaiveTime::parse_from_str(filter_value, "%H:%M:%S%.f"),
    ) {
        return cmp(a, b, operator);
    }
    false
}

fn matches_one_filter(payload: &serde_json::Value, filter: &Filter) -> bool {
    // Absent field never matches a comparison filter - `valid_filters`
    // guarantees the field exists and is scalar/list-shaped in the
    // *schema*; this only fires for a legitimately-optional field absent
    // on this one specific event instance.
    let Some(value) = payload_field_value(payload, &filter.field) else {
        return false;
    };
    match value {
        serde_json::Value::Array(items) => {
            filter.operator == FilterOperator::Contains
                && items.iter().any(|item| {
                    json_scalar_to_string(item).as_deref() == Some(filter.value.as_str())
                })
        }
        serde_json::Value::String(s) => match filter.operator {
            FilterOperator::Equals => s == &filter.value,
            FilterOperator::Contains => s.contains(filter.value.as_str()),
            FilterOperator::IsLike => like_matches(s, &filter.value),
            FilterOperator::GreaterThan | FilterOperator::LessThan => {
                string_ordering(s, &filter.value, filter.operator)
            }
        },
        serde_json::Value::Number(n) => {
            let (Some(a), Ok(b)) = (n.as_f64(), filter.value.parse::<f64>()) else {
                return false;
            };
            match filter.operator {
                FilterOperator::Equals => a == b,
                FilterOperator::GreaterThan => a > b,
                FilterOperator::LessThan => a < b,
                FilterOperator::Contains | FilterOperator::IsLike => false,
            }
        }
        serde_json::Value::Bool(b) => {
            filter.operator == FilterOperator::Equals && b.to_string() == filter.value
        }
        serde_json::Value::Null | serde_json::Value::Object(_) => false,
    }
}

/// Black box (see the note above rule `FetchEvents`, also used by
/// `ConsumeEvents`/`DeliverToSubscriptions`). Real now: parses
/// `event.payload` once, then per filter resolves `field` via
/// `payload_field_value` (bare or dotted, real since `derive_tags`) and
/// matches on the actual runtime `serde_json::Value` - no schema access
/// needed here, unlike `valid_filters`, which is what already gates
/// which operator a given field can ever receive. A payload that somehow
/// fails to parse as JSON (shouldn't happen - always valid JSON by the
/// time it's stored) falls through to no match rather than panicking.
pub fn matches_filters(event: &Event, filters: &[Filter]) -> bool {
    if filters.is_empty() {
        return true;
    }
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(&event.payload) else {
        return false;
    };
    filters.iter().all(|f| matches_one_filter(&payload, f))
}

/// The greatest `Event.sequence` among a set of events, or `None` when
/// empty - see the note above rule `ConsumeEvents`.
fn highest_sequence(events: &[Event]) -> Option<i64> {
    events.iter().map(|e| e.sequence).max()
}

/// Black box (see the note above rule `CreateExternalEvent`), now real for
/// both branches. Still pure/I/O-free (§1.1) - `resolve_key` is a plain
/// `Fn` closure the caller builds from *already*-provisioned
/// `EncryptionKey`s (`db::get_or_create_encryption_key`, called once per
/// distinct subject `sensitive_field_subjects` names, before this is ever
/// invoked) - the identical "pure core needs a value only I/O can
/// produce, so the caller pre-resolves it and hands over a closure"
/// pattern `process_command`'s own `resolve_event_type`/`next_sequence`
/// parameters already use, not a new one (see docs/architecture.md's
/// write-up of this pass for the full reasoning). An unresolved lookup is
/// a caller bug - `resolve_key` is trusted to always have what's needed,
/// the same trust convention `next_sequence`'s own closure already gets.
///
/// Takes `sensitive_fields` alone, not a whole `EventType`/`CommandType`,
/// since "it takes an EventType and a CommandType interchangeably" per
/// the spec's own note - `sensitive_fields` is the identical
/// `Set<SensitiveField>` on both. A field or its own `subject_field`
/// missing from `payload` (an optional field the caller omitted) is
/// skipped rather than encrypted with nothing to key it by - the same
/// "absence reads as empty/no-op" register `schema_properties`/
/// `schema_required` already use elsewhere in this module.
pub fn protect_sensitive_fields(
    sensitive_fields: &[SensitiveField],
    payload: &str,
    resolve_key: impl Fn(&str, &str) -> (EncryptionKey, DataKey),
) -> ProtectedPayload {
    if sensitive_fields.is_empty() {
        return ProtectedPayload {
            payload: payload.to_string(),
            encryption_keys: Vec::new(),
        };
    }

    let mut parsed: serde_json::Value = serde_json::from_str(payload).expect(
        "protect_sensitive_fields: payload is always valid JSON by the time this is called",
    );
    let mut used_keys: Vec<EncryptionKey> = Vec::new();

    for sf in sensitive_fields {
        let Some(subject_value) =
            payload_field_value(&parsed, &sf.subject_field).and_then(json_scalar_to_string)
        else {
            continue;
        };
        let Some(plaintext) = payload_field_value(&parsed, &sf.field).map(|v| {
            v.as_str()
                .map(str::to_string)
                .unwrap_or_else(|| v.to_string())
        }) else {
            continue;
        };

        let (key, data_key) = resolve_key(&sf.subject_key, &subject_value);
        let ciphertext = crate::encryption::encrypt_leaf(&data_key, &plaintext);

        if let Some(slot) = payload_field_value_mut(&mut parsed, &sf.field) {
            *slot = serde_json::Value::String(ciphertext);
        }
        if !used_keys.contains(&key) {
            used_keys.push(key);
        }
    }

    ProtectedPayload {
        payload: serde_json::to_string(&parsed)
            .expect("re-serialising a parsed JSON Value is infallible"),
        encryption_keys: used_keys,
    }
}

/// Every distinct `(subject_key, subject_value)` pair `payload` will need
/// an `EncryptionKey` for, per `sensitive_fields` - `protect_sensitive_fields`'s
/// own caller uses this to know what to pre-provision (via
/// `db::get_or_create_encryption_key`) before ever calling it, the same
/// pre-resolution step `resolve_event_type`/`next_sequence` already need.
/// Pure, no DB, no encryption - just a payload walk. A field whose
/// `subject_field` is absent or non-scalar contributes nothing, matching
/// `protect_sensitive_fields`'s own skip for that case.
pub fn sensitive_field_subjects(
    sensitive_fields: &[SensitiveField],
    payload: &str,
) -> Vec<(String, String)> {
    if sensitive_fields.is_empty() {
        return Vec::new();
    }
    let parsed: serde_json::Value = serde_json::from_str(payload).expect(
        "sensitive_field_subjects: payload is always valid JSON by the time this is called",
    );

    let mut subjects = Vec::new();
    for sf in sensitive_fields {
        if let Some(subject_value) =
            payload_field_value(&parsed, &sf.subject_field).and_then(json_scalar_to_string)
        {
            let pair = (sf.subject_key.clone(), subject_value);
            if !subjects.contains(&pair) {
                subjects.push(pair);
            }
        }
    }
    subjects
}

/// Pushes `Tag { key, value }` onto `tags` unless an identical tag is
/// already present - `derive_tags`'s own "Set<Tag>" dedup, kept as a
/// small helper rather than inlined since it's needed at two call sites
/// below (the per-mapping "absent" fallback, and each distinct list
/// element).
fn push_unique_tag(tags: &mut Vec<Tag>, key: &str, value: Option<String>) {
    let tag = Tag {
        key: key.to_string(),
        value,
    };
    if !tags.contains(&tag) {
        tags.push(tag);
    }
}

/// Black box (see the note above `derive_tags` in the spec). Real now:
/// per `TagMapping`, `payload_field_value` resolves `field` (bare or
/// dotted, see its own doc comment) against the parsed payload. A
/// non-empty JSON array pushes one `Tag(key, value: e)` per **distinct**
/// scalar element (`json_scalar_to_string`) - a non-scalar element is
/// silently skipped, undefined input rather than a spurious "absent"
/// tag. Everything else - a present scalar, an explicit `null`, an empty
/// array, or the field missing entirely - falls through to
/// `json_scalar_to_string`'s own `None`-for-non-scalar behaviour, which
/// already collapses every one of those onto the single correct
/// `Tag(key, value: null)` "absent" state with no extra branching.
/// `tag_mappings` empty needs no payload parse at all.
pub fn derive_tags(tag_mappings: &[TagMapping], payload: &str) -> Vec<Tag> {
    if tag_mappings.is_empty() {
        return Vec::new();
    }
    let parsed: serde_json::Value =
        serde_json::from_str(payload).unwrap_or(serde_json::Value::Null);
    let mut tags = Vec::new();
    for mapping in tag_mappings {
        let value = payload_field_value(&parsed, &mapping.field);
        match value.and_then(|v| v.as_array()) {
            Some(elements) if !elements.is_empty() => {
                for element in elements {
                    if let Some(scalar) = json_scalar_to_string(element) {
                        push_unique_tag(&mut tags, &mapping.key, Some(scalar));
                    }
                }
            }
            _ => {
                let scalar = value.and_then(json_scalar_to_string);
                push_unique_tag(&mut tags, &mapping.key, scalar);
            }
        }
    }
    tags
}

/// The two-grant test itself (see the note above rule
/// `DeliverToSubscriptions`, restated identically above `QueryEvents`/
/// `FetchCommands`/`QueryProjection`): a sensitive field decrypts under
/// either `access_mapping.can_read_sensitive`, or the caller's own
/// verified identity matching the specific subject that field is about.
/// Factored out as its own function so `render_event`/`render_command`
/// (whether to actually substitute a decrypted value) and their own
/// caller's pre-resolution step (which subjects are even worth fetching
/// a key for) share the identical check, never two independently-drifting
/// copies of the same boolean.
pub fn sensitive_field_is_granted(access_mapping: &RoleAccessMapping, subject_value: &str) -> bool {
    access_mapping.can_read_sensitive || access_mapping.role.external_subject == subject_value
}

/// Black box (see the note above rule `DeliverToSubscriptions`, reused by
/// `QueryEvents`/`CountEvents`/`InspectEvent` and `read_projection`'s own
/// event-side counterpart - `read_projection` itself stays deferred, see
/// docs/architecture.md's own write-up of this pass for why). Real now,
/// both branches: per `sensitive_fields` entry, `sensitive_field_is_granted`
/// decides whether to even attempt a decrypt; `resolve_data_key` (the
/// caller's own pre-resolved closure, the identical "impure resolution,
/// pure decision" split `resolve_key` already has on the write side) is
/// only ever called for a field this caller is actually granted. `None`
/// back from it - no active key, whether never provisioned or destroyed
/// by `ForgetSubject` - leaves the leaf untouched, correct crypto-
/// shredding behaviour with no special-casing needed. A field that *is*
/// granted a key but fails to decrypt (`encryption::decrypt_leaf`
/// returning `Err`) is left untouched too, not a panic - the spec's own
/// acknowledged case of a historical row that predates `sensitive_fields`
/// being declared on this type, so the leaf was never actually ciphertext
/// to begin with (see the note above `AddBoundedContext`/`ForgetSubject`:
/// "declaring it later protects the future only, never the past"). A
/// field with neither grant is never even attempted - "left as stored
/// ciphertext, never decrypted and then redacted afterwards" per the
/// spec's own repeated wording. The decrypted leaf is always rendered as
/// a JSON string regardless of the field's original scalar type - an
/// already-shipped consequence of `protect_sensitive_fields` itself
/// always storing ciphertext as a string leaf, discarding the original
/// type at encrypt time; restoring it would need schema-aware re-parsing,
/// a separate, out-of-scope piece of work.
pub fn render_event(
    event: &Event,
    access_mapping: &RoleAccessMapping,
    resolve_data_key: &impl Fn(&str, &str) -> Option<DataKey>,
) -> String {
    decrypt_sensitive_fields(
        &event.event_type.sensitive_fields,
        &event.payload,
        access_mapping,
        resolve_data_key,
    )
}

/// See `render_event` above - same black box, same real decrypt logic,
/// applied to `Command.payload`/`CommandType.sensitive_fields` instead.
/// See `rule FetchCommands`' own `@guidance` for why this is a distinct
/// function rather than `render_event` reused: the two-grant test is
/// identical, but the value being rendered is a `Command`, not an
/// `Event`.
pub fn render_command(
    command: &Command,
    access_mapping: &RoleAccessMapping,
    resolve_data_key: &impl Fn(&str, &str) -> Option<DataKey>,
) -> String {
    decrypt_sensitive_fields(
        &command.command_type.sensitive_fields,
        &command.payload,
        access_mapping,
        resolve_data_key,
    )
}

/// The shared walk `render_event`/`render_command` both need - same
/// per-field shape `protect_sensitive_fields` itself already walks
/// (`payload_field_value`/`json_scalar_to_string` reused unchanged), just
/// decrypting in place instead of encrypting. Returns `payload` unchanged
/// (not even re-serialised) when `sensitive_fields` is empty - the
/// identical "empty is a no-op" register every other black box in this
/// module already uses.
fn decrypt_sensitive_fields(
    sensitive_fields: &[SensitiveField],
    payload: &str,
    access_mapping: &RoleAccessMapping,
    resolve_data_key: &impl Fn(&str, &str) -> Option<DataKey>,
) -> String {
    if sensitive_fields.is_empty() {
        return payload.to_string();
    }

    let mut parsed: serde_json::Value = serde_json::from_str(payload).expect(
        "decrypt_sensitive_fields: payload is always valid JSON by the time this is called",
    );

    for sf in sensitive_fields {
        let Some(subject_value) =
            payload_field_value(&parsed, &sf.subject_field).and_then(json_scalar_to_string)
        else {
            continue;
        };
        if !sensitive_field_is_granted(access_mapping, &subject_value) {
            continue;
        }
        let Some(data_key) = resolve_data_key(&sf.subject_key, &subject_value) else {
            continue;
        };
        let Some(ciphertext) = payload_field_value(&parsed, &sf.field).and_then(|v| v.as_str())
        else {
            continue;
        };
        let Ok(plaintext) = crate::encryption::decrypt_leaf(&data_key, ciphertext) else {
            continue;
        };
        if let Some(slot) = payload_field_value_mut(&mut parsed, &sf.field) {
            *slot = serde_json::Value::String(plaintext);
        }
    }

    serde_json::to_string(&parsed).expect("re-serialising a parsed JSON Value is infallible")
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
///
/// `now` is new - needed to anchor `schedule_position` the moment
/// scheduling is newly opted into (see the note above the rule: "opting
/// in also anchors the schedule position, and only opting in does"). Every
/// other field this function sets stays timestamp-free, so this is the
/// one place `EventType` itself needs a clock at all.
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
    missed_occurrence_policy: Option<MissedOccurrencePolicy>,
    event_read_allowed: bool,
    existing: Option<&EventType>,
    now: chrono::DateTime<chrono::Utc>,
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
    // Scheduling is opted into whole - a type saying it fires on a
    // schedule has to say when *and* what a missed occurrence means, no
    // default for either. A real new rejection: `system_triggered_allowed
    // = true` with no schedule used to be silently accepted here and
    // simply never fire.
    if system_triggered_allowed
        && (system_triggered_schedule.is_none() || missed_occurrence_policy.is_none())
    {
        return Err(Error::MissingScheduleOrPolicy.into());
    }

    // The one act that anchors schedule_position, whether for a brand
    // new type or one that had scheduling switched off - see the note
    // above the rule. Computed from the caller-supplied `existing`
    // before it's shadowed below.
    let scheduling_newly_enabled =
        system_triggered_allowed && existing.is_none_or(|e| !e.system_triggered_allowed);

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
            missed_occurrence_policy,
            schedule_position: scheduling_newly_enabled.then_some(now),
            last_fired_at: None,
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
        missed_occurrence_policy,
        // Re-registration leaves the position exactly where the
        // scheduler left it, unless this *is* the opt-in moment - a
        // deploy must not be a way to re-fire (resetting back) or skip
        // (resetting forward) a backlog. last_fired_at is never touched
        // here at all: what once fired stays a fact regardless of
        // re-registration.
        schedule_position: if scheduling_newly_enabled {
            Some(now)
        } else {
            existing.schedule_position
        },
        last_fired_at: existing.last_fired_at,
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
/// as "matches nothing".
///
/// Returns each candidate's own `sequence` alongside its rendered
/// payload, not the rendered payload alone - the wire contract around
/// what else a caller sees per event stays deferred, same register as
/// everywhere else this spec is "deliberately coarse on the wire
/// contract" (see the surface's own guidance), but `sequence` isn't
/// optional scenery: it's the one thing a paging caller needs back to
/// supply as the next call's own `after_sequence`, propagating
/// `skilj-graphql`'s `EventQuery` resolver (§8 item 5, Phase 3) - a
/// caller with only the rendered strings back could never page past the
/// first call.
pub fn query_events(
    access_mapping: &RoleAccessMapping,
    event_types: &[EventType],
    tags: Option<&[Tag]>,
    after_sequence: Option<i64>,
    bounded_context_events: &[Event],
    resolve_data_key: impl Fn(&str, &str) -> Option<DataKey>,
) -> crate::error::Result<Vec<(i64, String)>> {
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
        .map(|e| {
            (
                e.sequence,
                render_event(e, access_mapping, &resolve_data_key),
            )
        })
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
    resolve_data_key: impl Fn(&str, &str) -> Option<DataKey>,
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
        rendered_payload: render_event(event, access_mapping, &resolve_data_key),
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
    resolve_data_key: impl Fn(&str, &str) -> Option<DataKey>,
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
        .map(|c| render_command(c, access_mapping, &resolve_data_key))
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
    resolve_data_key: impl Fn(&str, &str) -> Option<DataKey>,
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
            rendered_payload: render_event(event, s.access_mapping(), &resolve_data_key),
        })
        .collect()
}

/// The real-time delivery mechanism `DeliverToSubscriptions`/
/// `EventSubscription` need, deliberately as small as possible - see
/// `docs/architecture.md`'s own write-up of this pass for why this is a
/// single-process, in-memory broadcast rather than anything
/// Postgres-`LISTEN`/`NOTIFY`- or broker-backed: "Multi-instance /
/// distributed deployment" is explicitly out of this spec's scope
/// (§Excludes), so there is exactly one process any event this engine
/// produces could ever need to reach a live subscriber from.
///
/// `Clone`-cheap - `tokio::sync::broadcast::Sender` is already an `Arc`
/// around its own internal state, so every clone shares the same
/// channel, the same "hand out cheap clones from one shared thing"
/// treatment `db::Pool`/the two dispatchers already get.
#[derive(Clone)]
pub struct EventBroadcaster(tokio::sync::broadcast::Sender<Event>);

impl EventBroadcaster {
    /// `capacity` is how many not-yet-delivered events a single slow
    /// subscriber may lag behind by before its next `subscribe()`d
    /// receiver starts reporting `Lagged` - see
    /// `SkiljBuilder::event_broadcast_capacity`'s own doc comment.
    pub fn new(capacity: usize) -> Self {
        let (sender, _receiver) = tokio::sync::broadcast::channel(capacity);
        Self(sender)
    }

    /// A fresh, independent receiver - `resolvers::event_subscription`'s
    /// own entry point, one call per live GraphQL subscription. Multiple
    /// receivers each see every event published after they were created,
    /// with no coordination needed between them - `broadcast`'s own
    /// native fan-out, not something this type manages by hand.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Event> {
        self.0.subscribe()
    }

    /// `db::insert_event_and_update_sync_projections`'s own post-commit
    /// step - called once per committed event, regardless of which
    /// call site produced it (REST or GraphQL alike), since this is the
    /// one choke point every event write already funnels through. A
    /// `SendError` (zero receivers currently subscribed) is the expected
    /// steady state, not a failure - silently ignored, the same
    /// "nobody's listening right now" non-error `tokio::sync::broadcast`
    /// itself already models this way.
    pub fn publish(&self, event: &Event) {
        let _ = self.0.send(event.clone());
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
    resolve_key: impl Fn(&str, &str) -> (EncryptionKey, DataKey),
) -> crate::error::Result<Event> {
    if adapter.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    let event_type = &adapter.event_type;
    if !event_type.external_creation_allowed {
        return Err(Error::ExternalCreationNotAllowed.into());
    }
    if !valid_payload(&event_type.schema, &payload) {
        return Err(Error::PayloadDoesNotMatchSchema.into());
    }

    let protected = protect_sensitive_fields(&event_type.sensitive_fields, &payload, resolve_key);
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
    resolve_key: impl Fn(&str, &str) -> (EncryptionKey, DataKey),
) -> crate::error::Result<Event> {
    if adapter.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    let event_type = &adapter.event_type;
    if !event_type.direct_creation_allowed {
        return Err(Error::DirectCreationNotAllowed.into());
    }
    if !valid_payload(&event_type.schema, &payload) {
        return Err(Error::PayloadDoesNotMatchSchema.into());
    }

    let protected = protect_sensitive_fields(&event_type.sensitive_fields, &payload, resolve_key);
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

/// `next_occurrence_after(schedule, instant)` - the single cron primitive
/// `create_system_event`'s own guards need: the first occurrence of
/// `schedule` strictly after `instant`. Unlike `scheduled_payload`, a real
/// black box (implemented here, not caller-supplied) in the same register
/// as `derive_tags`/`schema_is_backwards_compatible` - the expression
/// grammar is mechanism, the guard built on top of it is behaviour - and
/// deterministic in the ordinary way: the same schedule and instant always
/// resolve to the same occurrence, which is exactly why two instances
/// evaluating `create_system_event`'s guards independently reach the same
/// verdict.
///
/// `schedule` is a 7-field Quartz-style cron expression (`sec min hour
/// day-of-month month day-of-week year`, e.g. `"0 0 3 * * * *"` for daily
/// at 03:00) - the `cron` crate's own dialect, not the more familiar
/// 5-field POSIX one; `RegisterEventType`'s own `requires` doesn't
/// validate this string's own syntax (the spec never added a
/// `valid_schedule` black box the way it did for filters), so a
/// malformed expression here just means this function returns `None`
/// forever for it, the same as a schedule with no occurrence left at all
/// - `create_system_event` never gets to reach a due occurrence, and the
///   type is never raised.
pub fn next_occurrence_after(
    schedule: &str,
    instant: chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    use std::str::FromStr;
    cron::Schedule::from_str(schedule)
        .ok()?
        .after(&instant)
        .next()
}

/// See `rule CreateSystemEvent`. `occurrence_at` is the instant
/// `SystemTriggerDue` itself carries - what this event stands for, not
/// inferred from `now` (which only ever appears in its `Metadata.created_at`,
/// the wall clock of the write). `payload` is `scheduled_payload(event_type)`'s
/// own already-resolved output - a `FnOnce` so the caller (the scheduler's
/// own orchestration) only ever invokes that black box once eligibility is
/// confirmed, never for an occurrence this function is about to reject
/// anyway.
///
/// `None` when this occurrence must not produce an event - already
/// accounted for, a policy still waiting on a later occurrence, or the
/// type/context isn't eligible at all. Unlike `create_external_event`/
/// `create_direct_event`, this never returns `Err`: nothing external ever
/// calls `CreateSystemEvent` (see the note above the rule - the scheduler
/// presents no credential and faces no actor), so there is no caller for a
/// typed rejection to reach. `Some((event, occurrence_at))` pairs the
/// built event with the instant both `EventType.schedule_position` and
/// `EventType.last_fired_at` advance to - the caller's own job to persist,
/// atomically with the event itself (see `db::fire_system_event`).
#[allow(clippy::too_many_arguments)]
pub fn create_system_event(
    event_type: &EventType,
    occurrence_at: chrono::DateTime<chrono::Utc>,
    now: chrono::DateTime<chrono::Utc>,
    next_sequence: i64,
    resolve_payload: impl FnOnce() -> String,
    resolve_key: impl Fn(&str, &str) -> (EncryptionKey, DataKey),
) -> Option<(Event, chrono::DateTime<chrono::Utc>)> {
    if !event_type.system_triggered_allowed {
        return None;
    }
    if event_type.bounded_context.status != BoundedContextStatus::Active {
        return None;
    }
    let schedule = event_type.system_triggered_schedule.as_deref()?;
    let policy = event_type.missed_occurrence_policy?;
    let position = event_type.schedule_position?;

    // An occurrence is never fired before its own instant, and never
    // twice: `position` is the durable, shared record of how far this
    // type's schedule has been accounted for, so an occurrence at or
    // behind it has already been settled - by an earlier firing, by a
    // policy that passed it over, or by another instance a moment ago.
    // This is what makes two instances raising the same occurrence
    // harmless rather than a duplicate event.
    if occurrence_at > now || occurrence_at <= position {
        return None;
    }

    // no_gap: this occurrence is the very next one the position hasn't
    // accounted for yet. nothing_later_is_due: no occurrence after this
    // one has come due, so it is the whole of a backlog's tail. See the
    // three bullets in the note above `rule CreateSystemEvent`.
    let no_gap = next_occurrence_after(schedule, position) == Some(occurrence_at);
    let nothing_later_is_due =
        next_occurrence_after(schedule, occurrence_at).is_none_or(|next| next > now);

    let eligible = match policy {
        MissedOccurrencePolicy::ReplayBacklog => true,
        MissedOccurrencePolicy::FireOnce => nothing_later_is_due,
        MissedOccurrencePolicy::Skip => no_gap && nothing_later_is_due,
    };
    if !eligible {
        return None;
    }

    let payload = resolve_payload();
    let protected = protect_sensitive_fields(&event_type.sensitive_fields, &payload, resolve_key);
    let event = Event {
        bounded_context: event_type.bounded_context.clone(),
        event_type: event_type.clone(),
        payload: protected.payload,
        metadata: Metadata {
            r#type: event_type.name.clone(),
            version: event_type.schema_version,
            client_id: "system".to_string(),
            created_at: now,
        },
        sequence: next_sequence,
        tags: derive_tags(&event_type.tag_mappings, &payload),
        encryption_keys: protected.encryption_keys,
        origin: EventOrigin::SystemTriggered,
    };
    Some((event, occurrence_at))
}

/// See `rule SkipMissedOccurrences`. `None` when the resume is a no-op
/// (the position is already at or past `now`) - see that rule's own note
/// on why this makes a second instance resuming alongside the first
/// harmless: a live cluster's position is already at or past its last
/// occurrence, so nothing here moves it further. Like `create_system_event`,
/// no typed rejection - `SystemScheduleResumed` reaches no surface either.
pub fn skip_missed_occurrences(
    event_type: &EventType,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    if event_type.missed_occurrence_policy != Some(MissedOccurrencePolicy::Skip) {
        return None;
    }
    if event_type.bounded_context.status != BoundedContextStatus::Active {
        return None;
    }
    let position = event_type.schedule_position?;
    if position >= now {
        return None;
    }
    Some(now)
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
    if !valid_payload(&command_type.schema, &payload) {
        return Err(Error::PayloadDoesNotMatchSchema.into());
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
    if !valid_payload(&command_type.schema, &payload) {
        return Err(Error::PayloadDoesNotMatchSchema.into());
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
/// Nine parameters because this function's own scope is genuinely that
/// wide - it's `ProcessCommand`'s entire `let`/`ensures` body, not
/// something a smaller grouping would simplify without inventing a
/// struct that exists only to satisfy the lint. `resolve_key` is
/// `protect_sensitive_fields`'s own pre-resolution parameter, threaded
/// through unchanged to both of this function's own call sites below (the
/// command's payload, and each accepted event spec's) - a command and an
/// event naming the same subject resolve to the very same `EncryptionKey`
/// this way, exactly as the spec requires.
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
    resolve_key: impl Fn(&str, &str) -> (EncryptionKey, DataKey),
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

    let protected = protect_sensitive_fields(&command_type.sensitive_fields, payload, &resolve_key);
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
        let event_protected =
            protect_sensitive_fields(&event_type.sensitive_fields, &spec_payload, &resolve_key);
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
