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
    AccessLevel, CommandToken, DirectCreationToken, EventReadStartPosition, EventReadToken,
    ExternalEventToken, PrivateFieldGrant, Role, RoleAccessMapping, RoleStatus, TokenStatus,
};
use crate::encryption::DataKey;
use crate::error::SkiljRejection;
use crate::shared::{
    CommandDecision, Filter, FilterOperator, Metadata, PrivateField, PrivateFieldKind,
    SensitiveField, Tag, TagMapping,
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
/// propagating `BoundedContextCreation`/`BoundedContextDirectory` - the
/// `admin` default's own `created_at`/`created_by` are stamped for real
/// at startup by `bootstrap::stamp_admin_bounded_context` (see its own
/// doc comment, including the concurrent-startup race fix). The
/// relationship projections (`event_types`, `commands`,
/// ...) are omitted, the same "caller resolves it, not a stored field"
/// treatment every other relationship projection in this codebase gets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundedContext {
    pub name: String,
    pub status: BoundedContextStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub created_by: crate::bootstrap::ContextCreator,
    /// See `entity BoundedContext`'s own `template` field doc comment
    /// (Codeberg issue #13). `None` for every context `AddBoundedContext`
    /// makes and for the admin context - the ordinary case. Set once, at
    /// creation, by `CreateBoundedContextFromTemplate`, and never
    /// afterward - the one write it can still take is `DeleteBoundedContext`
    /// clearing it to `None` when the referenced template is deleted (the
    /// link ends with its target, it never moves to a different one).
    /// Invariant `TemplateIsNeverItselfTemplated`: a `BoundedContext` used
    /// as a template always has `template: None` itself, so this never
    /// recurses past one level.
    pub template: Option<Box<BoundedContext>>,
}

impl BoundedContext {
    /// Whether `other` is the same bounded context, by name. `==` compares
    /// two copies field by field, and a copy is a snapshot of when it was
    /// loaded: an event the event cache took in before its context was
    /// archived carries `status: Active` for good, and `==` against the
    /// archived context a reader holds now says it belongs to another one
    /// (docs/architecture.md §179).
    ///
    /// The name alone, on purpose. Every other field but `created_at` can
    /// change while the context stays the same one: `status` on archiving,
    /// `template` when it's set or its template is deleted, and
    /// `created_by` embeds a whole `Role` whose own `status` changes on
    /// revocation. `created_at` would tell two lives of a reused name
    /// apart, but deleting a context already removes its mappings and
    /// tokens and the event cache drops its events (§95). And a copy
    /// built in Rust holds nanoseconds where one read back from Postgres
    /// holds microseconds, so the same context could compare unequal.
    pub fn same_as(&self, other: &BoundedContext) -> bool {
        self.name == other.name
    }
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
    /// See `plugin::EventType::owner_tag_key`'s own doc comment - names
    /// which of `tag_mappings`' own keys is this type's "owner"
    /// dimension, or `None` for a type with no such notion (every type
    /// registered before this field existed). Validated at registration
    /// time by `valid_owner_tag_key` - `null`, or a key present in
    /// `tag_mappings` - never re-validated here. Read by
    /// `event_owner_scope_satisfied`, the cross-tenant read fix's own
    /// per-event predicate (docs/architecture.md's own write-up of these
    /// passes).
    pub owner_tag_key: Option<String>,
    pub sensitive_fields: Vec<SensitiveField>,
    /// See `value PrivateField`'s own doc comment - a plain read-time
    /// redaction rule standing beside `sensitive_fields`, not inside it.
    /// Unlike `sensitive_fields`, this is not a fact about how an event
    /// was stored (a private field is always stored in plaintext), so
    /// changing this declaration changes what every reader sees of this
    /// type's whole history immediately, not merely of events created
    /// next - the same live-declaration treatment `owner_tag_key` already
    /// gets, and for the same reason. Validated at registration by
    /// `valid_private_fields`.
    pub private_fields: Vec<PrivateField>,
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

impl EventType {
    /// Whether `other` is the same event type: same name, same bounded
    /// context. See [`BoundedContext::same_as`] - and a copy of an
    /// `EventType` goes stale sooner still, since `schedule_position`/
    /// `last_fired_at` move on every scheduled fire and a re-registration
    /// can change everything else. A re-registered type, new schema
    /// version included, is still the same type: that's how a schema
    /// evolves.
    pub fn same_as(&self, other: &EventType) -> bool {
        self.name == other.name && self.bounded_context.same_as(&other.bounded_context)
    }
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
    /// See `EventType.owner_tag_key`'s own doc comment - identical role,
    /// for `Command.consistency_tags` instead of `Event.tags`. Cross-tenant
    /// read fix (docs/architecture.md's own write-up of these passes).
    pub owner_tag_key: Option<String>,
    pub sensitive_fields: Vec<SensitiveField>,
    /// See `EventType.private_fields`'s own doc comment - identical role,
    /// for `Command.payload` instead of `Event.payload`.
    pub private_fields: Vec<PrivateField>,
    pub rest_trigger_allowed: bool,
}

impl CommandType {
    /// Whether `other` is the same command type - see
    /// [`EventType::same_as`].
    pub fn same_as(&self, other: &CommandType) -> bool {
        self.name == other.name && self.bounded_context.same_as(&other.bounded_context)
    }
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
    /// SkilJ's own internal identifier, generated at creation via
    /// `generate_token_id()` - the same "distinct, internal, not a
    /// credential" register `Role.id`/`AccessToken.id` already occupy
    /// (see their own doc comments). Added to close drift audit finding
    /// #12 (2026-08-20, see project memory `skilj-drift-audit-2026-08-20`):
    /// `fetch_commands`'s own `triggered_event` lookup used to match by
    /// whole-`Command` equality, reconstructing identity from content
    /// rather than having a real one to compare - a distinct concept from
    /// `commands.id`, this struct's own DB row's internal `BIGSERIAL`
    /// primary key (still unexposed here, purely an FK-linking detail -
    /// see `db::insert_command`'s own doc comment).
    pub id: String,
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
/// `SystemTriggeredClientIdIsSystem` for the `metadata.client_id`
/// constraint that comes with it - enforced, not just declared:
/// `create_system_event` stamps `client_id: "system"` unconditionally on
/// every occurrence it raises). `CommandTriggered`
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

/// The stable, composed string identifying an `Event` as a *cause*
/// (Codeberg issue #18) - `Event` deliberately has no synthetic id of
/// its own the way `Command.id` does (see the doc comment on `Command`'s
/// own `id` field for why only that entity needed one), only `sequence`,
/// which is unique per bounded context but not globally. Prefixing it
/// with the bounded context name is enough to make it globally stable
/// without adding a new column anywhere: used wherever an `Event` needs
/// to be named as a `Metadata.causation_id` value - today, exactly
/// `db::catch_up_cross_context_route`'s own routed-command construction.
pub fn event_causation_id(event: &Event) -> String {
    format!("{}:{}", event.bounded_context.name, event.sequence)
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

/// See `entity ReadCursor`. `checked_out_at` is `manual_ack` only - always
/// `None` for an `auto_advance` cursor, which has no equivalent gap to
/// close (see the field's own doc comment in the spec). Codeberg issue
/// #25's investigation found the gap this closes: docs/architecture.md
/// §53.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadCursor {
    pub token: EventReadToken,
    pub ack_mode: AckMode,
    pub sequence: i64,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub checked_out_at: Option<chrono::DateTime<chrono::Utc>>,
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

    #[error(
        "this filter is invalid: its field's declared type doesn't support the operator, \
         its value is longer than 4096 characters (an IS_LIKE pattern, 1024), or more \
         than 32 filters were given"
    )]
    InvalidFilter,

    /// More tags than `MAX_QUERY_TAGS` in one `queryEvents`/`countEvents`
    /// (docs/architecture.md §122).
    #[error("at most 32 tags may be given in one query")]
    TooManyTags,

    /// Drift audit finding #16 (2026-08-20, see project memory
    /// `skilj-drift-audit-2026-08-20`): a registered `schema` used to go
    /// unvalidated at registration time whenever a type declared no
    /// `tag_mappings` and no `sensitive_fields` - both `valid_tag_mappings`/
    /// `valid_sensitive_fields` short-circuit `true` on empty input,
    /// never calling `schema_properties`/`jsonschema::validator_for` at
    /// all in that case, so a genuinely malformed schema (unparseable
    /// JSON, or JSON that isn't a valid JSON Schema document) silently
    /// "succeeded" at registration and only ever surfaced later, on the
    /// first real write, via `PayloadDoesNotMatchSchema` - confusing at
    /// the point a caller would actually see it, and far from where the
    /// real mistake was made. `valid_schema` closes this unconditionally.
    #[error("this schema is not well-formed JSON Schema")]
    InvalidSchema,

    #[error("this tag mapping names a field the schema doesn't declare")]
    InvalidTagMapping,

    /// Cross-tenant read fix (docs/architecture.md's own write-up of
    /// these passes): `owner_tag_key` names something other than an
    /// existing `tag_mappings` key - see `valid_owner_tag_key`'s own doc
    /// comment.
    #[error("this owner_tag_key does not name a key present in tag_mappings")]
    InvalidOwnerTagKey,

    #[error("a TagMapping and a SensitiveField may not name the same field")]
    SensitiveFieldTagOverlap,

    #[error("this sensitive field names a field the schema doesn't declare")]
    InvalidSensitiveField,

    /// `valid_private_fields`'s own rejection - either `field`/
    /// `addressee_field` names something the schema doesn't declare, or
    /// the `team`/`addressee_field` presence doesn't match `kind` (see
    /// that function's own doc comment for the full contract).
    #[error(
        "this private field names a field the schema doesn't declare, or its team/addressee_field \
         presence doesn't match its kind"
    )]
    InvalidPrivateField,

    /// The identical leak this variant's `tag_mappings` sibling
    /// (`SensitiveFieldTagOverlap`) already guards against, one register
    /// over: a tag is visible to every reader with no per-field
    /// redaction at all, so a field that is also `tag_mappings`-derived
    /// or `sensitive_fields`-encrypted cannot also be `private_fields` -
    /// its plaintext value would leak through the tag/ciphertext
    /// unredacted regardless of what `private_fields` says.
    #[error("a private field may not also be a tag mapping or a sensitive field")]
    PrivateFieldOverlap,

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

    /// Rule `FetchCommands`' `after_command` requirement - where a page
    /// continues from must be a command of the grant's own bounded context.
    #[error("this after_command belongs to a different bounded context than the grant's")]
    AfterCommandNotInBoundedContext,

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

    /// Not spec-modeled: a security-review finding on `CrossContextRoute`
    /// ([docs/architecture.md §36](../../../docs/architecture.md#cross-context-route)) - the `idempotency_keys` table's own
    /// primary key is `(command_type_name, idempotency_key)` with no
    /// caller/client_id column at all (Codeberg issue #12's original
    /// design never needed one - every prior caller only ever collided
    /// with its own past submissions). `CrossContextRoute`'s background
    /// task is the first caller to place an *unauthenticated internal*
    /// idempotency key into that same shared namespace
    /// (`"{RESERVED_IDEMPOTENCY_KEY_PREFIX}{route_name}:{sequence}"`),
    /// which an ordinary Write-level caller could otherwise pre-plant
    /// via `submitCommand`/`Idempotency-Key` to silently swallow a real
    /// route delivery as a `Deduplicated` no-op - see
    /// `reject_reserved_idempotency_key`'s own doc comment for the fix.
    #[error(
        "this idempotency key uses a reserved prefix - {RESERVED_IDEMPOTENCY_KEY_PREFIX:?} \
         is reserved for skilj's own internal use"
    )]
    ReservedIdempotencyKeyPrefix,

    /// An idempotency key longer than `MAX_IDEMPOTENCY_KEY_CHARS` - one is
    /// stored per accepted command (docs/architecture.md §134).
    #[error("an idempotency key may be at most 255 characters")]
    IdempotencyKeyTooLong,

    /// An external event naming both a `dedupe` cursor and an
    /// `Idempotency-Key` - specs/skilj.allium's `rule CreateExternalEvent`
    /// takes one or the other (docs/architecture.md §175). Raised for a
    /// parked-delivery report naming both; `POST /v1/events/external`
    /// ignores the header when there is a cursor instead (§195).
    #[error("an external event takes either a dedupe cursor or an idempotency key, not both")]
    DedupeAndIdempotencyKey,

    /// A position read in another database epoch - before a standby was
    /// promoted, or the database restored (docs/architecture.md §176).
    /// The log may end before it, and its sequences may since name other
    /// events, so it can't be resumed from.
    #[error(
        "this position was read in database epoch {supplied}, but the database is now in \
         epoch {current} (a failover or restore): events after an earlier point may be gone \
         and their sequences reused - start again from a position read in the current epoch"
    )]
    EpochChanged { supplied: String, current: String },

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

    /// Codeberg issue #18 - `valid_correlation_id`'s own rejection,
    /// shared by `correlation_id` and `causation_id` alike (see that
    /// function's own doc comment for why the two get identical, looser-
    /// than-`valid_bounded_context_name` treatment: no charset
    /// restriction, only a length cap, since either is arbitrary
    /// caller/bridge-supplied trace data, not an identifier this
    /// codebase itself embeds anywhere).
    #[error("correlation_id/causation_id must be at most {CORRELATION_ID_MAX_LEN} characters")]
    CorrelationIdTooLong,

    /// Not spec-modeled (parked deliveries are Rust-only, Codeberg issue
    /// #21): a parked delivery's own `request` body doesn't have the wire
    /// shape its `kind`'s original REST route takes (`POST
    /// /v1/events/external`'s or `POST /v1/commands/trigger`'s body).
    /// Rejected up front by `POST /v1/parked-deliveries` itself, and
    /// raised again by `retryParkedDelivery` for a row stored before that
    /// check existed - either way an ordinary error, never a panic on
    /// bridge-supplied JSON.
    #[error("parked delivery request does not match its kind's request shape: {0}")]
    InvalidParkedDeliveryRequest(String),

    /// Not spec-modeled (docs/architecture.md §160): this event's type is
    /// consumed by a sync projection this instance doesn't declare - a
    /// newer version's, mid rolling deploy - so it can't fold the event in
    /// the same transaction. Refused rather than committed with the
    /// projection skipping it for good. Another instance, one that
    /// declares it, can take the write.
    #[error(
        "this instance does not declare sync projection {0:?}, which consumes this event type"
    )]
    SyncProjectionNotDeclared(String),
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::BoundedContextArchived => "bounded_context_archived",
            Error::SchemaIncompatible => "schema_incompatible",
            Error::MissingScheduleOrPolicy => "missing_schedule_or_policy",
            Error::InvalidFilter => "invalid_filter",
            Error::TooManyTags => "too_many_tags",
            Error::InvalidSchema => "invalid_schema",
            Error::InvalidTagMapping => "invalid_tag_mapping",
            Error::InvalidOwnerTagKey => "invalid_owner_tag_key",
            Error::SensitiveFieldTagOverlap => "sensitive_field_tag_overlap",
            Error::InvalidSensitiveField => "invalid_sensitive_field",
            Error::InvalidPrivateField => "invalid_private_field",
            Error::PrivateFieldOverlap => "private_field_overlap",
            Error::TagMappingKeyDropped => "tag_mapping_key_dropped",
            Error::EventTypeNotInBoundedContext => "event_type_not_in_bounded_context",
            Error::CommandTypeNotInBoundedContext => "command_type_not_in_bounded_context",
            Error::TriggeredEventNotInBoundedContext => "triggered_event_not_in_bounded_context",
            Error::AfterCommandNotInBoundedContext => "after_command_not_in_bounded_context",
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
            Error::ReservedIdempotencyKeyPrefix => "reserved_idempotency_key_prefix",
            Error::IdempotencyKeyTooLong => "idempotency_key_too_long",
            Error::DedupeAndIdempotencyKey => "dedupe_and_idempotency_key",
            Error::EpochChanged { .. } => "epoch_changed",
            Error::UnregisteredEventType(_) => "unregistered_event_type",
            Error::PayloadDecodeFailed(_) => "payload_decode_failed",
            Error::CorrelationIdTooLong => "correlation_id_too_long",
            Error::InvalidParkedDeliveryRequest(_) => "invalid_parked_delivery_request",
            Error::SyncProjectionNotDeclared(_) => "sync_projection_not_declared",
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
/// being schema-driven. Three more `format` values gate their own single
/// operator the same way: `"geo-point"` → `Near`, `"color"` →
/// `SimilarColor`, `"ip"` → `InSubnet` - each format-gated pairing has
/// its own real-parsing helper in `matches_one_filter` below, same
/// reasoning as the date/time case. `In` needs no `format` at all - valid
/// against any scalar leaf, same as `Equals`.
fn filter_operator_is_valid(kind: &FieldKind, operator: FilterOperator) -> bool {
    match kind {
        FieldKind::Scalar {
            json_type: "string",
            format,
        } => {
            matches!(
                operator,
                FilterOperator::Equals
                    | FilterOperator::Contains
                    | FilterOperator::IsLike
                    | FilterOperator::In
            ) || (matches!(
                format.as_deref(),
                Some("date-time" | "date" | "partial-date-time")
            ) && matches!(
                operator,
                FilterOperator::GreaterThan | FilterOperator::LessThan
            )) || (format.as_deref() == Some("geo-point") && operator == FilterOperator::Near)
                || (format.as_deref() == Some("color") && operator == FilterOperator::SimilarColor)
                || (format.as_deref() == Some("ip") && operator == FilterOperator::InSubnet)
        }
        FieldKind::Scalar {
            json_type: "integer" | "number",
            ..
        } => matches!(
            operator,
            FilterOperator::Equals
                | FilterOperator::GreaterThan
                | FilterOperator::LessThan
                | FilterOperator::In
        ),
        FieldKind::Scalar {
            json_type: "boolean",
            ..
        } => matches!(operator, FilterOperator::Equals | FilterOperator::In),
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
    if filters.len() > MAX_FILTERS
        || filters
            .iter()
            .any(|f| f.value.chars().count() > MAX_FILTER_VALUE_CHARS)
    {
        return false;
    }
    let Some(properties) = schema_properties(&event_type.schema) else {
        return false;
    };
    let definitions = schema_definitions(&event_type.schema);
    filters.iter().all(|f| {
        resolve_field_kind(&properties, definitions.as_ref(), &f.field)
            .is_some_and(|kind| filter_operator_is_valid(&kind, f.operator))
            && (f.operator != FilterOperator::IsLike
                || f.value.chars().count() <= MAX_LIKE_PATTERN_CHARS)
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
pub(crate) fn payload_field_value<'a>(
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
pub(crate) fn payload_field_value_mut<'a>(
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
pub(crate) fn json_scalar_to_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(_) | serde_json::Value::Bool(_) => Some(value.to_string()),
        _ => None,
    }
}

/// Black box (see the note above rule `RegisterEventType`): a registered
/// `schema` is well-formed JSON Schema - drift audit finding #16
/// (2026-08-20, see project memory `skilj-drift-audit-2026-08-20`).
/// Unconditional, unlike `valid_tag_mappings`/`valid_sensitive_fields`:
/// those only ever inspect `schema` as a side effect of checking a
/// non-empty `tag_mappings`/`sensitive_fields` list, so a type declaring
/// neither previously registered a garbage `schema` unrejected - this
/// closes that gap regardless of what else the registration declares.
/// The identical check `valid_payload` already runs against `schema`
/// itself (`serde_json::from_str` for well-formed JSON,
/// `jsonschema::validator_for` for well-formed JSON Schema specifically -
/// a schema that parses as JSON but isn't valid JSON Schema, e.g. a
/// `"type"` value that isn't a recognised keyword, is rejected here too).
pub fn valid_schema(schema: &str) -> bool {
    let Ok(schema_value) = serde_json::from_str::<serde_json::Value>(schema) else {
        return false;
    };
    jsonschema::validator_for(&schema_value).is_ok()
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

/// Black box (see the note above rule `RegisterEventType`) - cross-tenant
/// read fix (docs/architecture.md's own write-up of these passes):
/// `owner_tag_key` is `null` (no owner dimension declared - every type
/// registered before this field existed), or names a key present in
/// `tag_mappings`. Unlike `valid_tag_mappings`'/`valid_sensitive_fields`'
/// additive-only re-registration treatment (`RegisterEventType`'s own
/// `existing.tag_mappings.all(...)` check), `owner_tag_key` is re-validated
/// fresh on every registration rather than accumulated: it is a pointer
/// *into* `tag_mappings`, not itself additive state, so a later
/// registration is free to change or clear it as long as it still points
/// at a real key (or is null).
pub fn valid_owner_tag_key(tag_mappings: &[TagMapping], owner_tag_key: Option<&str>) -> bool {
    match owner_tag_key {
        None => true,
        Some(key) => tag_mappings.iter().any(|m| m.key == key),
    }
}

/// The one gate `Metadata.correlation_id`/`causation_id` (Codeberg issue
/// #18) each pass through, independently (`rule ProcessCommand`/
/// `CreateExternalEvent`/`CreateDirectEvent` all call this twice, once
/// per field). `None` or empty is vacuously valid - deliberately, the
/// same "absent input needs no further check" shape `valid_tag_mappings`/
/// `valid_sensitive_fields`/`valid_private_fields` above already share -
/// otherwise valid iff at most `CORRELATION_ID_MAX_LEN` characters.
///
/// A black box for the same reason `valid_bounded_context_name`
/// (`skilj-core/src/bootstrap/mod.rs`) is, but a deliberately looser
/// one, and the difference matters: a bounded context name is carried
/// through verbatim as a Postgres schema-name fragment by the layers
/// underneath this spec, which is why *that* black box also restricts
/// charset. A correlation/causation id is arbitrary caller- or
/// bridge-supplied trace data - a UUID, a W3C traceparent-shaped string,
/// an upstream broker's own message id - stored and compared for
/// equality only, never embedded as an identifier anywhere. So no
/// charset restriction here, only a length cap, purely as an abuse
/// guard - no real downstream constraint drives the exact number.
pub const CORRELATION_ID_MAX_LEN: usize = 200;

pub fn valid_correlation_id(id: Option<&str>) -> bool {
    match id {
        None => true,
        Some(id) => id.is_empty() || id.chars().count() <= CORRELATION_ID_MAX_LEN,
    }
}

/// Black box (see the note above rule `RegisterEventType`): the same
/// existence-and-shape check as `valid_tag_mappings` above for
/// `SensitiveField.subject_field` (any scalar or list-of-scalar leaf,
/// identically - `subject_field` is only ever read, never encrypted, so
/// nothing about its own declared type constrains what it can be).
/// `SensitiveField.field` - the leaf `protect_sensitive_fields` actually
/// replaces with ciphertext - is narrower: `FieldKind::Scalar { json_type:
/// "string", .. }` only, not any scalar and not `ListOfScalar`. This is
/// not an arbitrary restriction: ciphertext is fundamentally string-shaped
/// (`crate::encryption::encrypt_leaf`'s own output), so a leaf whose
/// declared type isn't already `"string"` cannot hold it and still
/// conform to its own schema - "a scalar keeps the same declared type
/// whether it holds plaintext or ciphertext" (the payload schema shape
/// note above `entity CommandType`) is only ever true for a leaf that was
/// a string to begin with. Encrypting a non-string leaf in place, or a
/// whole list at once (both accepted here before this fix), silently
/// broke every downstream typed decode instead.
pub fn valid_sensitive_fields(schema: &str, sensitive_fields: &[SensitiveField]) -> bool {
    if sensitive_fields.is_empty() {
        return true;
    }
    let Some(properties) = schema_properties(schema) else {
        return false;
    };
    let definitions = schema_definitions(schema);
    sensitive_fields.iter().all(|s| {
        matches!(
            resolve_field_kind(&properties, definitions.as_ref(), &s.field),
            Some(FieldKind::Scalar {
                json_type: "string",
                ..
            })
        ) && resolve_field_kind(&properties, definitions.as_ref(), &s.subject_field).is_some()
    })
}

/// Black box (see the note above rule `RegisterEventType`) - the
/// private-field mechanism's own registration-time check, in the same
/// register as `valid_tag_mappings`/`valid_sensitive_fields`: `field` (and,
/// for an `addressed`-kind entry, `addressee_field`) must be a real schema
/// leaf. Unlike `valid_sensitive_fields`, no `json_type: "string"`
/// restriction on `field` itself - a private field is redacted to `null`
/// at read time, not encrypted, so any scalar leaf works. The
/// kind-specific presence rule is `value PrivateField`'s own contract,
/// checked here rather than by the type system alone: `team` set and
/// `addressee_field` absent iff `kind = Team`; `addressee_field` set (and
/// itself a real schema leaf) and `team` absent iff `kind = Addressed`;
/// both absent iff `kind = Own`.
pub fn valid_private_fields(schema: &str, private_fields: &[PrivateField]) -> bool {
    if private_fields.is_empty() {
        return true;
    }
    let Some(properties) = schema_properties(schema) else {
        return false;
    };
    let definitions = schema_definitions(schema);
    private_fields.iter().all(|p| {
        if resolve_field_kind(&properties, definitions.as_ref(), &p.field).is_none() {
            return false;
        }
        match p.kind {
            PrivateFieldKind::Own => p.team.is_none() && p.addressee_field.is_none(),
            PrivateFieldKind::Team => p.team.is_some() && p.addressee_field.is_none(),
            PrivateFieldKind::Addressed => {
                p.team.is_none()
                    && p.addressee_field.as_deref().is_some_and(|af| {
                        resolve_field_kind(&properties, definitions.as_ref(), af).is_some()
                    })
            }
        }
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
    let Ok(payload_value) = serde_json::from_str::<serde_json::Value>(payload) else {
        return false;
    };
    let Some(validator) = compiled_schema(schema) else {
        return false;
    };
    validator.is_valid(&payload_value)
}

/// The most distinct schemas [`compiled_schema`] keeps compiled; past
/// this it starts over. Registered types' current schemas are all that is
/// normally in play, so this is only reached by schema churn.
const MAX_COMPILED_SCHEMAS: usize = 256;

/// `schema` compiled, from a process-wide cache keyed by the schema text
/// itself (docs/architecture.md §127): compiling cost about 60 µs for an
/// ordinary schema, against 0.2 µs to validate with the result, and ran
/// for every event and command a caller wrote. Keyed by content, so a
/// changed schema is simply another entry and a stale validator is never
/// served. `None` for a schema that doesn't parse or compile - not cached,
/// since registration refuses such schemas and they don't recur.
fn compiled_schema(schema: &str) -> Option<std::sync::Arc<jsonschema::Validator>> {
    static CACHE: std::sync::LazyLock<
        std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<jsonschema::Validator>>>,
    > = std::sync::LazyLock::new(Default::default);

    if let Some(validator) = CACHE
        .lock()
        .expect("the compiled-schema cache is never poisoned")
        .get(schema)
    {
        return Some(validator.clone());
    }
    let schema_value = serde_json::from_str::<serde_json::Value>(schema).ok()?;
    let validator = std::sync::Arc::new(jsonschema::validator_for(&schema_value).ok()?);
    let mut cache = CACHE
        .lock()
        .expect("the compiled-schema cache is never poisoned");
    if cache.len() >= MAX_COMPILED_SCHEMAS {
        cache.clear();
    }
    cache.insert(schema.to_string(), validator.clone());
    Some(validator)
}

/// The idempotency-key namespace `CrossContextRoute`'s own background
/// task (`db::catch_up_cross_context_route`, [docs/architecture.md §36](../../../docs/architecture.md#cross-context-route))
/// reserves for its own internally-derived keys
/// (`"{RESERVED_IDEMPOTENCY_KEY_PREFIX}{route_name}:{source_event_sequence}"`) -
/// see `Error::ReservedIdempotencyKeyPrefix`'s own doc comment for why
/// this needs to be reserved at all. Chosen to be something no ordinary
/// caller-meaningful idempotency key (a Temporal `run_id:activity_id`,
/// an app's own UUID, anything human-chosen) would plausibly start
/// with by accident - not a guarantee against a caller who deliberately
/// (or coincidentally) picks a key starting with this exact literal
/// prefix, who gets rejected same as an attacker would; low enough
/// odds in practice to accept as this mechanism's tradeoff.
///
/// **No longer load-bearing** since [docs/architecture.md §37](../../../docs/architecture.md#idempotency-keys-client-id-scoping)'s
/// `client_id`-scoped `idempotency_keys` (`db::ensure_idempotency_keys_table`'s
/// own doc comment) - no external caller's `client_id` is ever
/// caller-suppliable, so no external submission could land under
/// `CrossContextRoute`'s own partition regardless of what
/// `idempotency_key` string it used, prefix or not. Kept anyway as
/// harmless defense-in-depth (the same register the `Deduplicated`
/// warning in `catch_up_cross_context_route` already keeps for a
/// scenario the reservation itself is meant to make unreachable).
pub const RESERVED_IDEMPOTENCY_KEY_PREFIX: &str = "skilj-cross-context-route:";

/// Codeberg issue #20's own sibling reservation - the identical
/// defense-in-depth `RESERVED_IDEMPOTENCY_KEY_PREFIX` gives
/// `CrossContextRoute`, extended proactively to `db::fire_due_deadlines`'s
/// own internally-derived keys (`"{RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX}{deadline_id}"`,
/// `deadline_id` itself already `"{ScheduleDeadline::NAME}:{source_event_sequence}"`)
/// rather than after the fact the way [§36](../../../docs/architecture.md#cross-context-route)'s own version was. Not
/// load-bearing either, for the identical reason `RESERVED_IDEMPOTENCY_KEY_PREFIX`'s
/// own doc comment gives: `fire_due_deadlines`'s `client_id` ("deadline")
/// is server-derived, never caller-suppliable, so `idempotency_keys`'
/// own `client_id`-scoping ([§37](../../../docs/architecture.md#idempotency-keys-client-id-scoping)) already puts every key it writes in a
/// partition no external caller's own submission ever lands in. A
/// separate literal from `RESERVED_IDEMPOTENCY_KEY_PREFIX` rather than
/// sharing one, so each internal caller's own reserved namespace stays
/// legible on its own (an unexpected `Deduplicated` outcome logs which
/// namespace it collided with) rather than folding two unrelated
/// mechanisms under one name.
pub const RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX: &str = "skilj-deadline:";

/// `db::parked_delivery_redrive_identity`'s own per-row keys for a
/// `CommandTrigger`-kind redrive. That redrive runs under the bridge
/// token's own `client_id`, a partition the token's own REST calls also
/// write keys into, so the reservation keeps a caller-chosen key from
/// ever colliding with one (it could only ever affect that same token's
/// redrives - `idempotency_keys` is `client_id`-scoped).
pub const RESERVED_PARKED_DELIVERY_IDEMPOTENCY_KEY_PREFIX: &str = "skilj-parked-delivery:";

/// The longest caller-supplied idempotency key accepted, in characters -
/// see [`reject_reserved_idempotency_key`] (docs/architecture.md §134).
pub const MAX_IDEMPOTENCY_KEY_CHARS: usize = 255;
// `Error::IdempotencyKeyTooLong`'s message names it.
const _: () = assert!(MAX_IDEMPOTENCY_KEY_CHARS == 255);

/// Checked by `authorise_command_trigger`/`authorise_command_submission`'s
/// own two REST/GraphQL callers, immediately after either reads a
/// caller-supplied `idempotencyKey`/`Idempotency-Key` value - never
/// folded into those two functions themselves, since neither is on
/// `decide_and_submit_command`'s internal `CrossContextRoute` caller's
/// own path (that caller reaches `decide_and_submit_command` directly,
/// bypassing both), so centralising there would have been safe; kept
/// at the call site instead purely to match the "wire-boundary concern"
/// register `submitCommand`'s own `required_role` gate already uses
/// (see that resolver's own doc comment) rather than out of necessity.
/// `Ok(())` for `None` - omitting an idempotency key entirely is always
/// fine, the same "unchanged behaviour when absent" every other
/// idempotency-key caller already gets. See `RESERVED_IDEMPOTENCY_KEY_PREFIX`'s
/// own doc comment for why this check is no longer this mechanism's
/// real defense, just a harmless second layer on top of it.
///
/// Also refuses a key longer than [`MAX_IDEMPOTENCY_KEY_CHARS`]
/// (docs/architecture.md §134): one is stored with every accepted command
/// that carries it, and on a parked delivery, and nothing else bounded it
/// short of the request size - megabytes per command.
pub fn reject_reserved_idempotency_key(idempotency_key: Option<&str>) -> crate::error::Result<()> {
    if idempotency_key.is_some_and(|key| key.chars().count() > MAX_IDEMPOTENCY_KEY_CHARS) {
        return Err(Error::IdempotencyKeyTooLong.into());
    }
    match idempotency_key {
        Some(key)
            if key.starts_with(RESERVED_IDEMPOTENCY_KEY_PREFIX)
                || key.starts_with(RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX)
                || key.starts_with(RESERVED_PARKED_DELIVERY_IDEMPOTENCY_KEY_PREFIX) =>
        {
            Err(Error::ReservedIdempotencyKeyPrefix.into())
        }
        _ => Ok(()),
    }
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

/// The longest `FilterOperator::IsLike` pattern [`valid_filters`]
/// accepts, in characters (docs/architecture.md §121). Matching costs
/// time proportional to pattern length times the matched string's, for
/// every event a read examines.
pub const MAX_LIKE_PATTERN_CHARS: usize = 1024;
/// The most filters one read or subscription may carry, and the longest
/// value any of them may have, in characters (docs/architecture.md §122).
/// Every filter is evaluated against every event examined - for a
/// subscription, every event committed for as long as it lives.
pub const MAX_FILTERS: usize = 32;
pub const MAX_FILTER_VALUE_CHARS: usize = 4096;

/// The most tags one `queryEvents`/`countEvents` may give; each becomes
/// its own condition in the tag-index query (docs/architecture.md §122).
pub const MAX_QUERY_TAGS: usize = 32;

// `Error::InvalidFilter`'s and `Error::TooManyTags`' messages name these.
const _: () = assert!(
    MAX_LIKE_PATTERN_CHARS == 1024
        && MAX_FILTERS == 32
        && MAX_FILTER_VALUE_CHARS == 4096
        && MAX_QUERY_TAGS == 32
);

/// `Err(TooManyTags)` for more than [`MAX_QUERY_TAGS`] tags - checked by
/// `queryEvents`/`countEvents` before their tag-index query, and again by
/// the rules themselves.
pub fn valid_query_tags(tags: Option<&[Tag]>) -> crate::error::Result<()> {
    if tags.is_some_and(|tags| tags.len() > MAX_QUERY_TAGS) {
        return Err(Error::TooManyTags.into());
    }
    Ok(())
}

/// Classic SQL-LIKE matching for `FilterOperator::IsLike` - `%` matches
/// any run of characters (including none), `_` matches exactly one
/// character, everything else matches itself literally. Case-sensitive,
/// whole-string anchored (no implicit substring search - that's what
/// `Contains` is for). Over `char`s, for UTF-8 safety.
///
/// Wildcard-matching DP, bit-parallel (docs/architecture.md §121): the
/// row "the text read so far matches the pattern's first `j` characters"
/// is a bitset, advanced one text character at a time with a few word
/// operations per 64 pattern positions. The previous full-table DP
/// allocated pattern x text booleans - a gigabyte for a 10k-character
/// pattern against a 100k-character string - for every event a read
/// examined. Runs of `%` are collapsed to one first (same meaning), which
/// makes a `%` position depend only on the non-`%` position before it,
/// so one shift settles it. Stops once no prefix of the pattern matches,
/// since none can again.
fn like_matches(text: &str, pattern: &str) -> bool {
    let mut p: Vec<char> = Vec::new();
    for c in pattern.chars() {
        if !(c == '%' && p.last() == Some(&'%')) {
            p.push(c);
        }
    }
    // Bit j: the text read so far matches p[..j].
    let words = (p.len() + 1).div_ceil(64);
    let set = |bits: &mut [u64], j: usize| bits[j / 64] |= 1 << (j % 64);
    let mut percent = vec![0u64; words];
    let mut any_char = vec![0u64; words];
    let mut literal: std::collections::HashMap<char, Vec<u64>> = std::collections::HashMap::new();
    for (i, &c) in p.iter().enumerate() {
        match c {
            '%' => set(&mut percent, i + 1),
            '_' => set(&mut any_char, i + 1),
            c => set(literal.entry(c).or_insert_with(|| vec![0; words]), i + 1),
        }
    }
    let mut row = vec![0u64; words];
    set(&mut row, 0);
    if p.first() == Some(&'%') {
        set(&mut row, 1);
    }
    let mut next = vec![0u64; words];
    for c in text.chars() {
        let literal = literal.get(&c);
        // A non-`%` position j advances from j - 1 when it matches `c`;
        // a `%` position keeps what it had.
        let mut carry = 0;
        for w in 0..words {
            let shifted = (row[w] << 1) | carry;
            carry = row[w] >> 63;
            let matches = any_char[w] | literal.map_or(0, |l| l[w]);
            next[w] = (shifted & matches) | (row[w] & percent[w]);
        }
        // A `%` position also matches wherever the position before it
        // now does - never itself a `%`, so already final above.
        let mut carry = 0;
        let mut any = 0;
        for w in 0..words {
            let shifted = (next[w] << 1) | carry;
            carry = next[w] >> 63;
            next[w] |= shifted & percent[w];
            any |= next[w];
        }
        if any == 0 {
            return false;
        }
        std::mem::swap(&mut row, &mut next);
    }
    row[p.len() / 64] >> (p.len() % 64) & 1 == 1
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

/// `FilterOperator::Near` - `payload` is a stored `"lat,lng"` string,
/// `filter_value` is `"lat,lng,radius_meters"` (the query point plus how
/// close counts as "near"). Haversine great-circle distance, `f64`
/// throughout - plenty precise for anything this library's own filtering
/// needs (not a geodesy library). Any parse failure on either side (not
/// exactly two/three comma-separated numbers) just doesn't match, same
/// "reject gracefully" register as `string_ordering`.
fn geo_distance_within(payload: &str, filter_value: &str) -> bool {
    fn parse_point(s: &str) -> Option<(f64, f64)> {
        let mut parts = s.splitn(2, ',');
        let lat = parts.next()?.trim().parse::<f64>().ok()?;
        let lng = parts.next()?.trim().parse::<f64>().ok()?;
        Some((lat, lng))
    }
    let Some((lat1, lng1)) = parse_point(payload) else {
        return false;
    };
    let mut filter_parts = filter_value.splitn(3, ',');
    let (Some(lat2_str), Some(lng2_str), Some(radius_str)) = (
        filter_parts.next(),
        filter_parts.next(),
        filter_parts.next(),
    ) else {
        return false;
    };
    let (Ok(lat2), Ok(lng2), Ok(radius_meters)) = (
        lat2_str.trim().parse::<f64>(),
        lng2_str.trim().parse::<f64>(),
        radius_str.trim().parse::<f64>(),
    ) else {
        return false;
    };
    const EARTH_RADIUS_METERS: f64 = 6_371_000.0;
    let (phi1, phi2) = (lat1.to_radians(), lat2.to_radians());
    let d_phi = (lat2 - lat1).to_radians();
    let d_lambda = (lng2 - lng1).to_radians();
    let a = (d_phi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (d_lambda / 2.0).sin().powi(2);
    let distance_meters = EARTH_RADIUS_METERS * 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    distance_meters <= radius_meters
}

/// `FilterOperator::SimilarColor` - `payload` is a stored `"#RRGGBB"`
/// string, `filter_value` is `"#RRGGBB,max_distance"`. Plain Euclidean
/// distance over the three RGB channels (range `0.0..=441.67`, i.e.
/// `sqrt(255^2 * 3)`) - deliberately not a perceptual metric (CIE ΔE
/// would need a color-science dependency this doesn't warrant); the doc
/// comment on `FilterOperator::SimilarColor` says so too, so a caller
/// isn't misled about what "similar" means here.
fn color_similarity_within(payload: &str, filter_value: &str) -> bool {
    fn parse_hex(s: &str) -> Option<(f64, f64, f64)> {
        let s = s.trim().strip_prefix('#')?;
        if s.len() != 6 {
            return None;
        }
        let r = u8::from_str_radix(&s[0..2], 16).ok()? as f64;
        let g = u8::from_str_radix(&s[2..4], 16).ok()? as f64;
        let b = u8::from_str_radix(&s[4..6], 16).ok()? as f64;
        Some((r, g, b))
    }
    let Some((r1, g1, b1)) = parse_hex(payload) else {
        return false;
    };
    let mut filter_parts = filter_value.splitn(2, ',');
    let (Some(color_str), Some(max_distance_str)) = (filter_parts.next(), filter_parts.next())
    else {
        return false;
    };
    let Some((r2, g2, b2)) = parse_hex(color_str) else {
        return false;
    };
    let Ok(max_distance) = max_distance_str.trim().parse::<f64>() else {
        return false;
    };
    let distance = ((r1 - r2).powi(2) + (g1 - g2).powi(2) + (b1 - b2).powi(2)).sqrt();
    distance <= max_distance
}

/// `FilterOperator::InSubnet` - `payload` is a stored IPv4/IPv6 address
/// string, `filter_value` is a CIDR (e.g. `"192.168.1.0/24"`,
/// `"2001:db8::/32"`). Delegates the actual containment check to
/// `ipnet` rather than hand-rolled bitwise subnet math - IPv6 in
/// particular is easy to get subtly wrong by hand.
fn ip_in_subnet(payload: &str, filter_value: &str) -> bool {
    let Ok(addr) = payload.trim().parse::<std::net::IpAddr>() else {
        return false;
    };
    let Ok(subnet) = filter_value.trim().parse::<ipnet::IpNet>() else {
        return false;
    };
    subnet.contains(&addr)
}

/// `FilterOperator::In` - `filter_value` is a comma-separated list of
/// candidates, no escaping (same as `IsLike`'s `%`/`_` wildcards already
/// being unescaped). Valid against any scalar leaf; reuses
/// `json_scalar_to_string` (the same string representation the `Array`/
/// `Contains` arm below already uses) so `In` is exactly "equals one of",
/// not its own comparison logic.
fn matches_any_of(value: &serde_json::Value, filter_value: &str) -> bool {
    let Some(value_as_string) = json_scalar_to_string(value) else {
        return false;
    };
    filter_value
        .split(',')
        .any(|candidate| candidate == value_as_string)
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
            FilterOperator::Near => geo_distance_within(s, &filter.value),
            FilterOperator::SimilarColor => color_similarity_within(s, &filter.value),
            FilterOperator::InSubnet => ip_in_subnet(s, &filter.value),
            FilterOperator::In => matches_any_of(value, &filter.value),
        },
        serde_json::Value::Number(n) => {
            if filter.operator == FilterOperator::In {
                return matches_any_of(value, &filter.value);
            }
            let (Some(a), Ok(b)) = (n.as_f64(), filter.value.parse::<f64>()) else {
                return false;
            };
            match filter.operator {
                FilterOperator::Equals => a == b,
                FilterOperator::GreaterThan => a > b,
                FilterOperator::LessThan => a < b,
                FilterOperator::Contains
                | FilterOperator::IsLike
                | FilterOperator::Near
                | FilterOperator::SimilarColor
                | FilterOperator::InSubnet
                | FilterOperator::In => false,
            }
        }
        serde_json::Value::Bool(b) => match filter.operator {
            FilterOperator::Equals => b.to_string() == filter.value,
            FilterOperator::In => matches_any_of(value, &filter.value),
            _ => false,
        },
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

/// Cross-tenant read/write fix (docs/architecture.md's own write-up of
/// these passes) - the shared core `event_owner_scope_satisfied`/
/// `command_owner_scope_satisfied` below each delegate to, and the one
/// the write-side `authorise_command_submission`/`authorise_command_trigger`/
/// `create_external_event`/`create_direct_event` call directly, since
/// none of those have an `Event`/`Command` yet to read `.tags`/
/// `.event_type.owner_tag_key` off of at the point they need this check -
/// the record does not exist until after it passes. Takes the derived
/// `tags` and the type's own `owner_tag_key` as plain values instead, so
/// every caller - already-materialized or not-yet-created alike - feeds
/// it identically.
///
/// Holds - the record is visible/included, or in the write-side case,
/// permitted to be created - when `scope` is `None` (unrestricted, every
/// caller's behaviour before `scope` existed), or `owner_tag_key` is
/// `None` (this type declares no owner dimension, so no `scope` value
/// ever restricts it), or `tags` carries a tag whose key equals
/// `owner_tag_key` and whose value equals `scope`. Does not hold - fails
/// closed, the "affirmatively provable, not merely un-contradicted"
/// stance every caller of this shares - when `scope` is `Some`, the type
/// does declare `owner_tag_key`, and no tag carries that key with a
/// matching value (including no such tag at all, or one with a null
/// value - the "mapped field was absent" case, see `Tag.value` in the
/// spec).
pub fn tag_owner_scope_satisfied(
    tags: &[Tag],
    owner_tag_key: Option<&str>,
    scope: Option<&str>,
) -> bool {
    let Some(scope) = scope else {
        return true;
    };
    let Some(owner_tag_key) = owner_tag_key else {
        return true;
    };
    tags.iter()
        .any(|tag| tag.key == owner_tag_key && tag.value.as_deref() == Some(scope))
}

/// Cross-tenant read fix (docs/architecture.md's own write-up of these
/// passes) - the per-event sibling of `projections::query_projection`'s
/// own `tag_owner_scope_satisfied`, same idea applied to a raw `Event`
/// instead of a projection instance; delegates to `tag_owner_scope_satisfied`
/// above with `event.tags`/`event.event_type.owner_tag_key`. Takes a raw
/// `scope: Option<&str>` rather than a whole `RoleAccessMapping`, so both
/// the GraphQL track (`RoleAccessMapping.scope`) and the REST track
/// (`EventReadToken.scope`) feed it identically - `query_events`/
/// `count_events`/`deliver_to_subscriptions`/`inspect_event` call it with
/// the former, `fetch_events`/`consume_events` with the latter.
///
/// A multi-record surface (`query_events`/`count_events`/
/// `deliver_to_subscriptions`/`fetch_events`/`consume_events`) uses this
/// as a `.filter()`: a non-owned event is silently excluded, the call
/// still succeeds. A single-record surface (`inspect_event`) rejects
/// outright when it returns `false`, the same shape
/// `query_projection`'s own check has.
pub fn event_owner_scope_satisfied(event: &Event, scope: Option<&str>) -> bool {
    tag_owner_scope_satisfied(
        &event.tags,
        event.event_type.owner_tag_key.as_deref(),
        scope,
    )
}

/// `event_owner_scope_satisfied`'s own sibling for `Command`, read by
/// `fetch_commands` (`FetchCommands`' `command_owner_scope_satisfied`).
/// Identical contract - see that function's own doc comment for the full
/// three-holds/fails-closed reasoning, not restated here - delegating to
/// `tag_owner_scope_satisfied` above with
/// `command.consistency_tags`/`command.command_type.owner_tag_key` where
/// the event version delegates with `Event.tags`/`EventType.owner_tag_key`.
/// `consistency_tags` is the right field for this: it is always
/// `derive_tags(command_type, payload)`, unconditionally - a plain
/// `Vec<Tag>`, never absent - regardless of whether this particular
/// command actually used a consistency boundary; only `consistency_boundary`
/// itself goes missing for that case (see `Command.consistency_boundary`'s
/// own doc comment).
pub fn command_owner_scope_satisfied(command: &Command, scope: Option<&str>) -> bool {
    tag_owner_scope_satisfied(
        &command.consistency_tags,
        command.command_type.owner_tag_key.as_deref(),
        scope,
    )
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

/// The private-field mechanism's own accessor trait: `is_default_private_reader`
/// and the render-time redaction pass below read the same three things
/// off an `Event` or a `Command` alike rather than being duplicated per
/// type - the identical role `protect_sensitive_fields`'s own shared walk
/// already plays for `sensitive_fields`, just needing one more field read
/// (`private_fields`) than that walk does.
pub trait PrivateFieldRecord {
    fn private_fields(&self) -> &[PrivateField];
    fn client_id(&self) -> &str;
    fn payload(&self) -> &str;
}

impl PrivateFieldRecord for Event {
    fn private_fields(&self) -> &[PrivateField] {
        &self.event_type.private_fields
    }
    fn client_id(&self) -> &str {
        &self.metadata.client_id
    }
    fn payload(&self) -> &str {
        &self.payload
    }
}

impl PrivateFieldRecord for Command {
    fn private_fields(&self) -> &[PrivateField] {
        &self.command_type.private_fields
    }
    fn client_id(&self) -> &str {
        &self.metadata.client_id
    }
    fn payload(&self) -> &str {
        &self.payload
    }
}

/// `is_default_private_reader(record, access_mapping)` in the spec -
/// takes `role: &Role` rather than a whole `&RoleAccessMapping` here,
/// since nothing else off it is ever read: `RoleAccessMapping.status`/
/// `.level`/`.bounded_context` play no part in this specific question,
/// only `.role.id`/`.role.external_subject` do. That narrower signature
/// is what lets `PrivateFieldGrant.grantor` - a bare `Role`, no
/// `RoleAccessMapping` alongside it - be re-checked here directly at
/// render time (see the redaction pass below) without fabricating one.
///
/// Holds when either of two things is true, read off `record`'s own
/// type's `private_fields`:
///   - some `Own`-kind field is declared and `record`'s own `client_id`
///     equals `role.id` - the caller is the record's creator (`client_id`
///     is the Role's internal id for anything a GraphQL caller produced -
///     see `authorise_command_submission`), or
///   - some `Addressed`-kind field is declared and `role.external_subject`
///     equals the value that field's own `addressee_field` names in
///     `record`'s payload - the caller is the party the record itself
///     addressed, the identical match a sensitive field's subject already
///     gets in `render_event`/`render_command`, resolved against a
///     payload field rather than an `EncryptionKey`'s subject value.
///
/// Does not hold for a record whose type declares only `Team`-kind
/// private fields, or none at all - there is no default reader to be.
/// Generic over `Event`/`Command` alike via `PrivateFieldRecord` above -
/// used both here and at grant time (`grant_private_field_access_for_event`/
/// `grant_private_field_access_for_command`), one predicate, several call
/// sites, never two implementations of the same question.
pub fn is_default_private_reader<R: PrivateFieldRecord>(record: &R, role: &Role) -> bool {
    let private_fields = record.private_fields();
    if private_fields
        .iter()
        .any(|p| p.kind == PrivateFieldKind::Own)
        && record.client_id() == role.id
    {
        return true;
    }
    if !private_fields
        .iter()
        .any(|p| p.kind == PrivateFieldKind::Addressed)
    {
        return false;
    }
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(record.payload()) else {
        return false;
    };
    private_fields.iter().any(|p| {
        p.kind == PrivateFieldKind::Addressed
            && p.addressee_field
                .as_deref()
                .and_then(|af| payload_field_value(&parsed, af))
                .and_then(json_scalar_to_string)
                .is_some_and(|addressee| addressee == role.external_subject)
    })
}

/// Whether `access_mapping.role` may read one `PrivateField` entry on
/// `record` - the one decision the redaction pass below makes per field.
/// `Team`-kind needs no `record`/grant lookup at all: membership is the
/// grant. `Own`/`Addressed` hold when `is_default_private_reader` holds
/// directly, or an active `PrivateFieldGrant` reaches this caller -
/// either a per-record one `matches_this_record` identifies, or a
/// blanket one (naming neither an event nor a command) whose own
/// `grantor` independently satisfies `is_default_private_reader` for
/// `record` - re-derived here, against the record actually being read,
/// which is what keeps a blanket grant a convenience rather than a wider
/// trust boundary (see `entity PrivateFieldGrant`'s own doc comment).
fn entitled_to_read_private_field<R: PrivateFieldRecord>(
    record: &R,
    kind: PrivateFieldKind,
    team: Option<&str>,
    access_mapping: &RoleAccessMapping,
    grants: &[PrivateFieldGrant],
    matches_this_record: impl Fn(&PrivateFieldGrant) -> bool,
) -> bool {
    match kind {
        PrivateFieldKind::Team => {
            crate::access_control::role_matches_required_team(&access_mapping.role, team)
        }
        PrivateFieldKind::Own | PrivateFieldKind::Addressed => {
            is_default_private_reader(record, &access_mapping.role)
                || grants.iter().any(|g| {
                    g.status == TokenStatus::Active
                        && g.grantee == access_mapping.role
                        && (matches_this_record(g)
                            || (g.event_sequence.is_none()
                                && g.command_id.is_none()
                                && is_default_private_reader(record, &g.grantor)))
                })
        }
    }
}

/// Black box (see the note above rule `DeliverToSubscriptions`, reused by
/// `QueryEvents`/`CountEvents`/`InspectEvent` and by `crate::projections::
/// read_projection` too - see its own doc comment for its real, since
/// implemented, once-per-query treatment). Real now, both branches: per
/// `sensitive_fields` entry, `sensitive_field_is_granted`
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
/// `grants` is the caller's own already-loaded snapshot of active
/// `PrivateFieldGrant`s naming `access_mapping.role` as grantee, within
/// this bounded context - the identical "pure function, I/O resolved
/// before the call" split every other rule in this module already
/// follows for `resolve_data_key`. Empty for a caller with no such
/// grants, which is every caller before this mechanism existed - the
/// third pass below is then just `is_default_private_reader`/`Team`
/// membership, unchanged from a `private_fields`-free type's own
/// behaviour.
pub fn render_event(
    event: &Event,
    access_mapping: &RoleAccessMapping,
    resolve_data_key: &impl Fn(&str, &str) -> Option<DataKey>,
    grants: &[PrivateFieldGrant],
) -> String {
    let decrypted = decrypt_sensitive_fields(
        &event.event_type.sensitive_fields,
        &event.payload,
        access_mapping,
        resolve_data_key,
    );
    redact_unentitled_private_fields(event, &decrypted, access_mapping, grants, |g| {
        g.bounded_context.same_as(&event.bounded_context)
            && g.event_sequence == Some(event.sequence)
    })
}

/// See `render_event` above - same black box, same real decrypt/redact
/// logic, applied to `Command.payload`/`CommandType.sensitive_fields`/
/// `CommandType.private_fields` instead. See `rule FetchCommands`' own
/// `@guidance` for why this is a distinct function rather than
/// `render_event` reused: the tests are identical, but the value being
/// rendered is a `Command`, not an `Event`.
pub fn render_command(
    command: &Command,
    access_mapping: &RoleAccessMapping,
    resolve_data_key: &impl Fn(&str, &str) -> Option<DataKey>,
    grants: &[PrivateFieldGrant],
) -> String {
    let decrypted = decrypt_sensitive_fields(
        &command.command_type.sensitive_fields,
        &command.payload,
        access_mapping,
        resolve_data_key,
    );
    redact_unentitled_private_fields(command, &decrypted, access_mapping, grants, |g| {
        g.bounded_context.same_as(&command.bounded_context)
            && g.command_id.as_deref() == Some(command.id.as_str())
    })
}

/// An event a dry-run's `decide()` would emit (see `rule DryRunCommand`):
/// what `process_command` would build from one accepted `EventSpec`,
/// minus everything only a real submission has - a sequence, a
/// `Command` origin, encryption. `payload` is the spec's own, in
/// plaintext; render it with [`render_would_be_event`] before showing it.
#[derive(Debug, Clone, PartialEq)]
pub struct WouldBeEvent {
    pub event_type: EventType,
    pub payload: String,
    pub tags: Vec<Tag>,
    /// The submitter's, as `process_command` would stamp it.
    pub client_id: String,
}

impl PrivateFieldRecord for WouldBeEvent {
    fn private_fields(&self) -> &[PrivateField] {
        &self.event_type.private_fields
    }
    fn client_id(&self) -> &str {
        &self.client_id
    }
    fn payload(&self) -> &str {
        &self.payload
    }
}

/// `render_would_be_event(spec, access_mapping)` in the spec - what
/// [`render_event`] does for a stored event, for one that doesn't exist.
/// Nothing in it was encrypted, so a sensitive field the caller isn't
/// granted (`sensitive_field_is_granted`) is set to `null` rather than
/// left as ciphertext, and one it is granted is left as `decide()` wrote
/// it. Private fields are redacted as `render_event` redacts them, except
/// that no per-record `PrivateFieldGrant` can match: the event has no
/// sequence for one to name.
pub fn render_would_be_event(
    event: &WouldBeEvent,
    access_mapping: &RoleAccessMapping,
    grants: &[PrivateFieldGrant],
) -> String {
    let sensitive_fields = &event.event_type.sensitive_fields;
    let payload = if sensitive_fields.is_empty() {
        event.payload.clone()
    } else {
        let mut parsed: serde_json::Value = serde_json::from_str(&event.payload)
            .expect("render_would_be_event: a decided event's payload is serialized JSON");
        for sf in sensitive_fields {
            let granted = payload_field_value(&parsed, &sf.subject_field)
                .and_then(json_scalar_to_string)
                .is_some_and(|subject| sensitive_field_is_granted(access_mapping, &subject));
            if granted {
                continue;
            }
            if let Some(slot) = payload_field_value_mut(&mut parsed, &sf.field) {
                *slot = serde_json::Value::Null;
            }
        }
        serde_json::to_string(&parsed).expect("re-serialising a parsed JSON Value is infallible")
    };
    let record = WouldBeEvent {
        payload,
        ..event.clone()
    };
    redact_unentitled_private_fields(&record, &record.payload, access_mapping, grants, |_| false)
}

/// The redaction pass `render_event`/`render_command` both run after
/// decrypting sensitive fields - a plain visibility rule, not a
/// cryptographic one, with no `EncryptionKey`/`resolve_data_key`/
/// `ForgetSubject` involvement anywhere: a private field is stored in
/// plaintext exactly as written, and that absence is the whole point of
/// the mechanism (see `value PrivateField`). Per `PrivateField` entry,
/// `entitled_to_read_private_field` decides whether this caller may see
/// the leaf; a caller who isn't gets it set to `null` - not the enclosing
/// object, not the whole record withheld, and not ciphertext either,
/// since none was ever produced, exactly the shape and exactly the place
/// an unentitled caller already finds a sensitive field's leaf left as
/// stored ciphertext, one pass up.
fn redact_unentitled_private_fields<R: PrivateFieldRecord>(
    record: &R,
    payload: &str,
    access_mapping: &RoleAccessMapping,
    grants: &[PrivateFieldGrant],
    matches_this_record: impl Fn(&PrivateFieldGrant) -> bool,
) -> String {
    let private_fields = record.private_fields();
    if private_fields.is_empty() {
        return payload.to_string();
    }

    let mut parsed: serde_json::Value = serde_json::from_str(payload).expect(
        "redact_unentitled_private_fields: payload is always valid JSON by the time this is called",
    );
    for p in private_fields {
        if entitled_to_read_private_field(
            record,
            p.kind,
            p.team.as_deref(),
            access_mapping,
            grants,
            &matches_this_record,
        ) {
            continue;
        }
        if let Some(slot) = payload_field_value_mut(&mut parsed, &p.field) {
            *slot = serde_json::Value::Null;
        }
    }
    serde_json::to_string(&parsed).expect("re-serialising a parsed JSON Value is infallible")
}

/// `redact_private_fields(record)` in the spec - the REST track's
/// unconditional counterpart to the render-time pass above. No
/// `access_mapping`/`Role` argument at all, deliberately: an
/// `EventReadToken` is tied to no `Role`, so every `private_fields` leaf
/// is nulled for every caller alike, the same fail-closed-by-construction
/// shape `EventFetch`'s own `PrivateFieldsStayRedacted` guarantee states.
/// All three kinds fail closed structurally here, not by omission - a
/// token is nobody's `client_id`, carries no `Role.name`, carries no
/// `external_subject`, and no `PrivateFieldGrant` can name it as a
/// grantee (a grant's grantee is always a `Role`) - so no amount of
/// sharing on the GraphQL side ever opens a private field on this one.
/// Called by `fetch_events`/`consume_events` alone: `CommandQuery` is
/// GraphQL/`AdminAccess`-only and already routes through `render_command`,
/// and `CommandTrigger` reads nothing back, so there is no command-side
/// counterpart to this function.
pub fn redact_private_fields(event: &Event) -> Event {
    let private_fields = &event.event_type.private_fields;
    if private_fields.is_empty() {
        return event.clone();
    }
    let mut parsed: serde_json::Value = serde_json::from_str(&event.payload)
        .expect("redact_private_fields: payload is always valid JSON by the time this is called");
    for p in private_fields {
        if let Some(slot) = payload_field_value_mut(&mut parsed, &p.field) {
            *slot = serde_json::Value::Null;
        }
    }
    Event {
        payload: serde_json::to_string(&parsed)
            .expect("re-serialising a parsed JSON Value is infallible"),
        ..event.clone()
    }
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
    owner_tag_key: Option<String>,
    sensitive_fields: Vec<SensitiveField>,
    private_fields: Vec<PrivateField>,
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
    if !access_mapping.bounded_context.same_as(bounded_context) {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if bounded_context.status != BoundedContextStatus::Active {
        return Err(Error::BoundedContextArchived.into());
    }
    if !valid_schema(&schema) {
        return Err(Error::InvalidSchema.into());
    }
    if !valid_tag_mappings(&schema, &tag_mappings) {
        return Err(Error::InvalidTagMapping.into());
    }
    if !valid_owner_tag_key(&tag_mappings, owner_tag_key.as_deref()) {
        return Err(Error::InvalidOwnerTagKey.into());
    }
    if !valid_sensitive_fields(&schema, &sensitive_fields) {
        return Err(Error::InvalidSensitiveField.into());
    }
    if !valid_private_fields(&schema, &private_fields) {
        return Err(Error::InvalidPrivateField.into());
    }
    if tag_mappings
        .iter()
        .any(|m| sensitive_fields.iter().any(|s| s.field == m.field))
    {
        return Err(Error::SensitiveFieldTagOverlap.into());
    }
    // A private field may not also be a tag mapping or a sensitive
    // field - the identical leak `SensitiveFieldTagOverlap` above already
    // guards against, one register over (see `Error::PrivateFieldOverlap`'s
    // own doc comment).
    if tag_mappings
        .iter()
        .any(|m| private_fields.iter().any(|p| p.field == m.field))
        || sensitive_fields
            .iter()
            .any(|s| private_fields.iter().any(|p| p.field == s.field))
    {
        return Err(Error::PrivateFieldOverlap.into());
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
            owner_tag_key,
            sensitive_fields,
            private_fields,
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
        owner_tag_key,
        sensitive_fields,
        private_fields,
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
    owner_tag_key: Option<String>,
    sensitive_fields: Vec<SensitiveField>,
    private_fields: Vec<PrivateField>,
    rest_trigger_allowed: bool,
    existing: Option<&CommandType>,
) -> crate::error::Result<CommandTypeRegistration> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if !access_mapping.bounded_context.same_as(bounded_context) {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if bounded_context.status != BoundedContextStatus::Active {
        return Err(Error::BoundedContextArchived.into());
    }
    if !valid_schema(&schema) {
        return Err(Error::InvalidSchema.into());
    }
    if !valid_tag_mappings(&schema, &tag_mappings) {
        return Err(Error::InvalidTagMapping.into());
    }
    if !valid_owner_tag_key(&tag_mappings, owner_tag_key.as_deref()) {
        return Err(Error::InvalidOwnerTagKey.into());
    }
    if !valid_sensitive_fields(&schema, &sensitive_fields) {
        return Err(Error::InvalidSensitiveField.into());
    }
    if !valid_private_fields(&schema, &private_fields) {
        return Err(Error::InvalidPrivateField.into());
    }
    if tag_mappings
        .iter()
        .any(|m| sensitive_fields.iter().any(|s| s.field == m.field))
    {
        return Err(Error::SensitiveFieldTagOverlap.into());
    }
    // See the identical check in `register_event_type` - same reasoning.
    if tag_mappings
        .iter()
        .any(|m| private_fields.iter().any(|p| p.field == m.field))
        || sensitive_fields
            .iter()
            .any(|s| private_fields.iter().any(|p| p.field == s.field))
    {
        return Err(Error::PrivateFieldOverlap.into());
    }

    let Some(existing) = existing else {
        return Ok(CommandTypeRegistration::Created(CommandType {
            bounded_context: bounded_context.clone(),
            name,
            schema,
            schema_version: 1,
            tag_mappings,
            owner_tag_key,
            sensitive_fields,
            private_fields,
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
        owner_tag_key,
        sensitive_fields,
        private_fields,
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
    if !access_mapping.bounded_context.same_as(&key.bounded_context) {
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

/// `config.max_events_per_read`'s spec default: the most events one
/// `FetchEvents`/`ConsumeEvents`/`QueryEvents` serves - the first that
/// many matching, in sequence order. A caller pages on from the last
/// served sequence (`after_sequence`, or the consume cursor), so nothing
/// is skipped. `SkiljBuilder::max_events_per_read` overrides it; the
/// `*_page` variants of those three functions take the value explicitly.
pub const DEFAULT_MAX_EVENTS_PER_READ: usize = 1000;

/// See `rule QueryEvents`. `bounded_context_events` is every `Event`
/// matching what this call needs, the same full-snapshot shape
/// `consistency_boundary_and_matching_events` takes `Event`s in - real
/// callers source it via `db::list_events_for_bounded_context_cached`
/// (`crate::event_cache`'s own module doc comment has the full design),
/// not an unconditional Postgres load. An empty `event_types`/`tags` means
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
/// `skilj-graphql`'s `EventQuery` resolver ([§8](../../../docs/architecture.md#open-for-a-future-pass) item 5, Phase 3) - a
/// caller with only the rendered strings back could never page past the
/// first call.
#[allow(clippy::too_many_arguments)]
pub fn query_events(
    access_mapping: &RoleAccessMapping,
    event_types: &[EventType],
    tags: Option<&[Tag]>,
    after_sequence: Option<i64>,
    correlation_id: Option<&str>,
    bounded_context_events: &[Event],
    resolve_data_key: impl Fn(&str, &str) -> Option<DataKey>,
    private_field_grants: &[PrivateFieldGrant],
) -> crate::error::Result<Vec<(i64, String)>> {
    query_events_page(
        access_mapping,
        event_types,
        tags,
        after_sequence,
        correlation_id,
        bounded_context_events,
        resolve_data_key,
        private_field_grants,
        DEFAULT_MAX_EVENTS_PER_READ,
    )
}

/// [`query_events`] with an explicit `config.max_events_per_read`:
/// [`query_events_select`], then each selected event rendered.
#[allow(clippy::too_many_arguments)]
pub fn query_events_page(
    access_mapping: &RoleAccessMapping,
    event_types: &[EventType],
    tags: Option<&[Tag]>,
    after_sequence: Option<i64>,
    correlation_id: Option<&str>,
    bounded_context_events: &[Event],
    resolve_data_key: impl Fn(&str, &str) -> Option<DataKey>,
    private_field_grants: &[PrivateFieldGrant],
    max_events: usize,
) -> crate::error::Result<Vec<(i64, String)>> {
    Ok(query_events_select(
        access_mapping,
        event_types,
        tags,
        after_sequence,
        correlation_id,
        bounded_context_events,
        max_events,
    )?
    .iter()
    .map(|e| {
        (
            e.sequence,
            render_event(e, access_mapping, &resolve_data_key, private_field_grants),
        )
    })
    .collect())
}

/// `rule QueryEvents`' `requires` and its `candidates` - which events a
/// query serves, at most `max_events`, not yet rendered. Separate from
/// rendering so `skilj-graphql` can pick its page a chunk at a time and
/// then resolve decryption keys for just the events it serves.
pub fn query_events_select(
    access_mapping: &RoleAccessMapping,
    event_types: &[EventType],
    tags: Option<&[Tag]>,
    after_sequence: Option<i64>,
    correlation_id: Option<&str>,
    bounded_context_events: &[Event],
    max_events: usize,
) -> crate::error::Result<Vec<Event>> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if !event_types
        .iter()
        .all(|et| et.bounded_context.same_as(&access_mapping.bounded_context))
    {
        return Err(Error::EventTypeNotInBoundedContext.into());
    }
    valid_query_tags(tags)?;

    let after = after_sequence.unwrap_or(-1);
    Ok(bounded_context_events
        .iter()
        .filter(|e| e.bounded_context.same_as(&access_mapping.bounded_context))
        .filter(|e| {
            event_types.is_empty() || event_types.iter().any(|et| et.same_as(&e.event_type))
        })
        .filter(|e| tags.is_none_or(|wanted| wanted.iter().any(|t| e.tags.contains(t))))
        .filter(|e| e.sequence > after)
        // Codeberg issue #18 - "show me everything in this transaction",
        // the same `Option`-filter shape every other criterion here
        // already has: `None` matches everything, `Some(id)` requires an
        // exact match against this event's own (always-present, per the
        // spec's own invariant) correlation_id.
        .filter(|e| {
            correlation_id.is_none_or(|id| e.metadata.correlation_id.as_deref() == Some(id))
        })
        .filter(|e| event_owner_scope_satisfied(e, access_mapping.scope.as_deref()))
        .take(max_events)
        .cloned()
        .collect())
}

/// What a rejected command's submitter is shown of the events its
/// decision was made against (`submitCommand`'s `matchingEvents`, docs/
/// architecture.md §118): only those inside the caller's own owner scope,
/// as `query_events_select` serves, and at most `max_events` of them - the
/// most recent, nearest the conflict. `true` alongside when events within
/// scope were left out for the cap. The decision itself still saw every
/// matching event; this narrows only what is shown. Rendering (decrypting
/// granted sensitive fields, redacting unentitled private ones) is still
/// the caller's, per event, via [`render_event`].
pub fn visible_matching_events(
    access_mapping: &RoleAccessMapping,
    matching_events: &[Event],
    max_events: usize,
) -> (Vec<Event>, bool) {
    let in_scope: Vec<&Event> = matching_events
        .iter()
        .filter(|e| event_owner_scope_satisfied(e, access_mapping.scope.as_deref()))
        .collect();
    let truncated = in_scope.len() > max_events;
    let visible = in_scope[in_scope.len().saturating_sub(max_events)..]
        .iter()
        .map(|e| (*e).clone())
        .collect();
    (visible, truncated)
}

/// See `rule CountEvents`. Same shape and narrowing as `query_events`
/// above, minus the `after_sequence` cursor - a count is a live aggregate
/// over everything matching, not a page of it (see the note above the
/// rule).
pub fn count_events(
    access_mapping: &RoleAccessMapping,
    event_types: &[EventType],
    tags: Option<&[Tag]>,
    correlation_id: Option<&str>,
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
        .all(|et| et.bounded_context.same_as(&access_mapping.bounded_context))
    {
        return Err(Error::EventTypeNotInBoundedContext.into());
    }
    valid_query_tags(tags)?;

    Ok(bounded_context_events
        .iter()
        .filter(|e| e.bounded_context.same_as(&access_mapping.bounded_context))
        .filter(|e| {
            event_types.is_empty() || event_types.iter().any(|et| et.same_as(&e.event_type))
        })
        .filter(|e| tags.is_none_or(|wanted| wanted.iter().any(|t| e.tags.contains(t))))
        .filter(|e| {
            correlation_id.is_none_or(|id| e.metadata.correlation_id.as_deref() == Some(id))
        })
        .filter(|e| event_owner_scope_satisfied(e, access_mapping.scope.as_deref()))
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
    private_field_grants: &[PrivateFieldGrant],
) -> crate::error::Result<EventInspected> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if !access_mapping
        .bounded_context
        .same_as(&event.bounded_context)
    {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if !event_owner_scope_satisfied(event, access_mapping.scope.as_deref()) {
        return Err(crate::access_control::Error::GrantScopeMismatch.into());
    }

    // The originating command's payload is rendered as `fetchCommands`
    // renders it (docs/architecture.md §120) - it used to go out as
    // stored, private fields in plaintext.
    let mut shown = event.clone();
    if let EventOrigin::CommandTriggered { command } = &mut shown.origin {
        command.payload = render_command(
            command,
            access_mapping,
            &resolve_data_key,
            private_field_grants,
        );
    }
    Ok(EventInspected {
        event: shown,
        rendered_payload: render_event(
            event,
            access_mapping,
            &resolve_data_key,
            private_field_grants,
        ),
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
/// second collection required. Matched by `Command.id` specifically
/// (drift audit finding #12, 2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`) - not whole-struct equality, which
/// used to stand in for identity here before `Command` had a real one.
/// Returns rendered payloads only, the same "deliberately coarse on the
/// wire contract" choice `query_events` makes for `EventsQueried.events`
/// - see its own doc comment.
#[allow(clippy::too_many_arguments)]
pub fn fetch_commands(
    access_mapping: &RoleAccessMapping,
    command_types: &[CommandType],
    after: Option<chrono::DateTime<chrono::Utc>>,
    before: Option<chrono::DateTime<chrono::Utc>>,
    triggered_event: Option<&Event>,
    correlation_id: Option<&str>,
    bounded_context_commands: &[Command],
    resolve_data_key: impl Fn(&str, &str) -> Option<DataKey>,
    private_field_grants: &[PrivateFieldGrant],
) -> crate::error::Result<Vec<String>> {
    Ok(fetch_commands_select(
        access_mapping,
        command_types,
        after,
        before,
        triggered_event,
        correlation_id,
        None,
        bounded_context_commands,
        DEFAULT_MAX_EVENTS_PER_READ,
    )?
    .iter()
    .map(|c| render_command(c, access_mapping, &resolve_data_key, private_field_grants))
    .collect())
}

/// `rule FetchCommands`' `requires` and its `candidates`, not yet
/// rendered: at most `max_commands`, taken in the order given.
/// `bounded_context_commands` must already be in recording order and
/// only hold commands recorded after `after_command` - what
/// `db::collect_command_page` hands it - since recording order isn't
/// part of `Command` itself; `after_command` is checked here only for the
/// rule's own bounded-context requirement.
#[allow(clippy::too_many_arguments)]
pub fn fetch_commands_select(
    access_mapping: &RoleAccessMapping,
    command_types: &[CommandType],
    after: Option<chrono::DateTime<chrono::Utc>>,
    before: Option<chrono::DateTime<chrono::Utc>>,
    triggered_event: Option<&Event>,
    correlation_id: Option<&str>,
    after_command: Option<&Command>,
    bounded_context_commands: &[Command],
    max_commands: usize,
) -> crate::error::Result<Vec<Command>> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if !command_types
        .iter()
        .all(|ct| ct.bounded_context.same_as(&access_mapping.bounded_context))
    {
        return Err(Error::CommandTypeNotInBoundedContext.into());
    }
    if triggered_event
        .is_some_and(|te| !te.bounded_context.same_as(&access_mapping.bounded_context))
    {
        return Err(Error::TriggeredEventNotInBoundedContext.into());
    }
    if after_command.is_some_and(|c| !c.bounded_context.same_as(&access_mapping.bounded_context)) {
        return Err(Error::AfterCommandNotInBoundedContext.into());
    }

    Ok(bounded_context_commands
        .iter()
        .filter(|c| c.bounded_context.same_as(&access_mapping.bounded_context))
        .filter(|c| {
            command_types.is_empty() || command_types.iter().any(|ct| ct.same_as(&c.command_type))
        })
        .filter(|c| after.is_none_or(|a| c.metadata.created_at >= a))
        .filter(|c| before.is_none_or(|b| c.metadata.created_at <= b))
        .filter(|c| {
            triggered_event.is_none_or(|te| match &te.origin {
                EventOrigin::CommandTriggered { command } => command.id == c.id,
                _ => false,
            })
        })
        // Codeberg issue #18 - same "show me everything in this
        // transaction" filter `query_events`/`count_events` gained.
        .filter(|c| {
            correlation_id.is_none_or(|id| c.metadata.correlation_id.as_deref() == Some(id))
        })
        .filter(|c| command_owner_scope_satisfied(c, access_mapping.scope.as_deref()))
        .take(max_commands)
        .cloned()
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
        .all(|et| et.bounded_context.same_as(&access_mapping.bounded_context))
    {
        return Err(Error::EventTypeNotInBoundedContext.into());
    }

    let starting_point = from_sequence.unwrap_or_else(|| {
        bounded_context_events
            .iter()
            .filter(|e| e.bounded_context.same_as(&access_mapping.bounded_context))
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
    if !access_mapping
        .bounded_context
        .same_as(&event_type.bounded_context)
    {
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
            .filter(|e| e.bounded_context.same_as(&event_type.bounded_context))
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
/// `private_field_grants` is every active `PrivateFieldGrant` in
/// `event.bounded_context`, regardless of grantee - a single shared
/// snapshot passed to every subscriber's own `render_event` call below
/// rather than looked up per subscriber, since `render_event` already
/// filters a grant list down to `g.grantee == access_mapping.role`
/// internally. Different subscribers see different rendered payloads
/// from the identical input for exactly that reason: each is only ever
/// entitled by its own grants.
pub fn deliver_to_subscriptions(
    event: &Event,
    subscriptions: &[Subscription],
    resolve_data_key: impl Fn(&str, &str) -> Option<DataKey>,
    private_field_grants: &[PrivateFieldGrant],
) -> Vec<EventDelivered> {
    subscriptions
        .iter()
        .filter(|s| subscription_selects(s, event))
        .filter(|s| s.access_mapping().status == RoleStatus::Active)
        .filter(|s| event_owner_scope_satisfied(event, s.access_mapping().scope.as_deref()))
        .map(|s| EventDelivered {
            subscription: s.clone(),
            event: event.clone(),
            rendered_payload: render_event(
                event,
                s.access_mapping(),
                &resolve_data_key,
                private_field_grants,
            ),
        })
        .collect()
}

/// The part of `deliver_to_subscriptions`' match that doesn't depend on
/// the grant: same bounded context, past `from_sequence`, one of the
/// subscription's event types, and its filters. A caller holding a live
/// subscription checks this first, so an event the subscription could
/// never deliver costs no grant re-check, key resolution or grant reads
/// (docs/architecture.md §85); `deliver_to_subscriptions` still applies
/// it together with the grant's status and owner scope.
pub fn subscription_selects(subscription: &Subscription, event: &Event) -> bool {
    subscription
        .bounded_context()
        .same_as(&event.bounded_context)
        && subscription.starting_sequence() < event.sequence
        && match subscription {
            Subscription::AllEventsSubscription(a) => {
                a.event_types.is_empty()
                    || a.event_types.iter().any(|et| et.same_as(&event.event_type))
            }
            Subscription::EventTypeSubscription(e) => {
                e.event_type.same_as(&event.event_type) && matches_filters(event, &e.filters)
            }
        }
}

/// The real-time delivery mechanism `DeliverToSubscriptions`/
/// `EventSubscription` need, deliberately as small as possible - a
/// single-process, in-memory broadcast rather than anything itself
/// Postgres-`LISTEN`/`NOTIFY`- or broker-backed, since every local
/// `EventSubscription` subscriber already lives in this one process and
/// needs nothing more elaborate to be reached. Multi-instance delivery
/// (Codeberg issue #2; `crate::cross_instance`) builds on top of this
/// rather than replacing it: another instance's committed event still
/// arrives here, republished by this instance's own cross-instance
/// listener - see `instance_id`'s own doc comment for how a duplicate
/// republish of *this* instance's own writes is avoided.
///
/// `Clone`-cheap - `tokio::sync::broadcast::Sender` is already an `Arc`
/// around its own internal state, so every clone shares the same
/// channel, the same "hand out cheap clones from one shared thing"
/// treatment `db::Pool`/the two dispatchers already get. `instance_id` is
/// a plain `String`, but `Clone` still stays effectively free for what
/// this type is used for - it's cloned to hand out shared handles, not
/// in any per-event hot path.
#[derive(Clone)]
pub struct EventBroadcaster {
    sender: tokio::sync::broadcast::Sender<Event>,
    /// Bumped by [`signal_gap`](Self::signal_gap) - see there.
    gaps: std::sync::Arc<tokio::sync::watch::Sender<u64>>,
    instance_id: String,
}

impl EventBroadcaster {
    /// `capacity` is how many not-yet-delivered events a single slow
    /// subscriber may lag behind by before its next `subscribe()`d
    /// receiver starts reporting `Lagged` - see
    /// `SkiljBuilder::event_broadcast_capacity`'s own doc comment.
    pub fn new(capacity: usize) -> Self {
        // `broadcast::channel` panics on a zero capacity; one is the
        // smallest real buffer (a slow subscriber then lags sooner).
        let (sender, _receiver) = tokio::sync::broadcast::channel(capacity.max(1));
        let (gaps, _gaps_receiver) = tokio::sync::watch::channel(0);
        Self {
            sender,
            gaps: std::sync::Arc::new(gaps),
            instance_id: crate::shared::generate_token_id(),
        }
    }

    /// A random id generated once per broadcaster - in practice once per
    /// running `Skilj` instance, since `SkiljBuilder::build` is this
    /// type's only real construction site. `db::notify_event_appended`
    /// stamps every `NOTIFY` this instance sends with it; `skilj`'s own
    /// cross-instance dispatch loop compares an incoming notification's
    /// `origin_instance_id` against this to recognise "this is my own
    /// write echoing back" and skip it, rather than calling `publish`
    /// here a second time for an event this exact broadcaster already
    /// delivered once directly. Without that check, every locally
    /// submitted event reached every local subscriber twice - Postgres
    /// `NOTIFY` is delivered to every listening backend including ones
    /// opened by the same process that sent it, and this broadcaster is
    /// the identical shared destination both the direct `publish` call
    /// and the cross-instance republish write into.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// A fresh, independent receiver - `resolvers::event_subscription`'s
    /// own entry point, one call per live GraphQL subscription. Multiple
    /// receivers each see every event published after they were created,
    /// with no coordination needed between them - `broadcast`'s own
    /// native fan-out, not something this type manages by hand.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<Event> {
        self.sender.subscribe()
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
        let _ = self.sender.send(event.clone());
    }

    /// Tells every live subscriber that events may have been committed
    /// that this broadcaster never published - the cross-instance
    /// listener's connection dropped, and another instance's `NOTIFY`s
    /// sent meanwhile are gone (docs/architecture.md §83). A subscriber
    /// treats it exactly like its own receiver lagging: an event
    /// subscription ends with `subscription_lagged` (`DeliveryIsAtMostOnce`:
    /// no silent gaps), a projection subscription refetches.
    pub fn signal_gap(&self) {
        self.gaps.send_modify(|n| *n = n.wrapping_add(1));
    }

    /// A receiver for [`signal_gap`](Self::signal_gap), taken alongside
    /// [`subscribe`](Self::subscribe): its `changed()` resolves on the
    /// next gap signalled after this call.
    pub fn subscribe_gaps(&self) -> tokio::sync::watch::Receiver<u64> {
        self.gaps.subscribe()
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
    correlation_id: Option<&str>,
) -> crate::error::Result<Vec<Event>> {
    fetch_events_page(
        token,
        events,
        filters,
        after_sequence,
        correlation_id,
        DEFAULT_MAX_EVENTS_PER_READ,
    )
}

/// Every way [`fetch_events_page`] can refuse a request - none of which
/// depend on the events - so a caller can check them before loading any
/// (docs/architecture.md §140).
pub fn check_fetch_events(token: &EventReadToken, filters: &[Filter]) -> crate::error::Result<()> {
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
    Ok(())
}

/// [`fetch_events`] with an explicit `config.max_events_per_read`. Also
/// what `skilj-rest` calls once per chunk while paging through history
/// (see `db::collect_event_page`).
pub fn fetch_events_page(
    token: &EventReadToken,
    events: &[Event],
    filters: &[Filter],
    after_sequence: Option<i64>,
    correlation_id: Option<&str>,
    max_events: usize,
) -> crate::error::Result<Vec<Event>> {
    check_fetch_events(token, filters)?;
    let read_type = &token.event_type;

    let after = after_sequence.unwrap_or(-1);
    Ok(events
        .iter()
        .filter(|e| e.bounded_context.same_as(&read_type.bounded_context))
        .filter(|e| e.event_type.same_as(read_type))
        .filter(|e| e.sequence > after)
        .filter(|e| matches_filters(e, filters))
        // Codeberg issue #18 - the REST-side counterpart to
        // `query_events`/`count_events`'s own identical filter.
        .filter(|e| {
            correlation_id.is_none_or(|id| e.metadata.correlation_id.as_deref() == Some(id))
        })
        .filter(|e| event_owner_scope_satisfied(e, token.scope.as_deref()))
        .take(max_events)
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
    /// `manual_ack`, cursor not already claimed (or a stale claim just
    /// reclaimed) - `ReadCursor.checked_out_at` becomes `now`, `sequence`
    /// untouched (manual_ack never moves it here, claimed or not - only
    /// `AcknowledgeEvents` does). Codeberg issue #25's investigation
    /// (docs/architecture.md §53); see `ReadCursor.checked_out_at`'s own
    /// spec doc comment for why this exists.
    Claimed {
        checked_out_at: chrono::DateTime<chrono::Utc>,
    },
    /// `manual_ack`, cursor already claimed by a live (not stale) lease -
    /// nothing moves, `served` is empty (see `consume_events`'s own
    /// `is_leased` handling).
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
/// the caller's persistence concern, not this rule's. `checkout_lease` is
/// `config.read_cursor_checkout_lease` - the caller's own
/// `SkiljBuilder`-configured value, not a spec-time constant (Codeberg
/// issue #25's investigation, docs/architecture.md §53).
pub fn consume_events(
    token: &EventReadToken,
    existing_cursor: Option<&ReadCursor>,
    ack_mode: Option<AckMode>,
    events: &[Event],
    filters: &[Filter],
    now: chrono::DateTime<chrono::Utc>,
    checkout_lease: chrono::Duration,
) -> crate::error::Result<ConsumeEventsResult> {
    let position = existing_cursor.map_or_else(
        || initial_consume_position(token, events),
        |cursor| cursor.sequence,
    );
    // `events` is the whole history, so every event of the token's type
    // after `position` was examined.
    let scanned_through = events
        .iter()
        .filter(|e| e.bounded_context.same_as(&token.event_type.bounded_context))
        .filter(|e| e.event_type.same_as(&token.event_type))
        .filter(|e| e.sequence > position)
        .map(|e| e.sequence)
        .max();
    consume_events_page(
        token,
        existing_cursor,
        ack_mode,
        position,
        events,
        scanned_through,
        filters,
        now,
        checkout_lease,
        DEFAULT_MAX_EVENTS_PER_READ,
    )
}

/// Where a token's *new* `ReadCursor` starts - rule `ConsumeEvents`'
/// `position` for `is_new`, from `token.start_from`. `Latest`/`AtTime`
/// are the greatest qualifying sequence in `history`, so a caller can
/// scan history in chunks and keep the maximum of each chunk's result
/// (`-1` means none) instead of loading it all at once. `Latest`/`AtTime`
/// are scoped exactly as a served event is (an event outside this
/// token's own scope was never visible to it, so it can't count as
/// "already seen" either), but blind to any call's `filters`, which vary
/// call to call and must never move where a one-time seed lands.
/// `AtSequence` is the caller-chosen value, unvalidated.
pub fn initial_consume_position<'a>(
    token: &EventReadToken,
    history: impl IntoIterator<Item = &'a Event>,
) -> i64 {
    let read_type = &token.event_type;
    let qualifying = history
        .into_iter()
        .filter(|e| e.bounded_context.same_as(&read_type.bounded_context))
        .filter(|e| e.event_type.same_as(read_type))
        .filter(|e| event_owner_scope_satisfied(e, token.scope.as_deref()));
    match token.start_from {
        EventReadStartPosition::Beginning => -1,
        EventReadStartPosition::Latest => qualifying.map(|e| e.sequence).max().unwrap_or(-1),
        EventReadStartPosition::AtSequence => token.start_at_sequence.expect(
            "create_event_read_token guarantees start_at_sequence is Some when \
             start_from = AtSequence",
        ),
        EventReadStartPosition::AtTime => {
            let threshold = token.start_at_time.expect(
                "create_event_read_token guarantees start_at_time is Some when \
                 start_from = AtTime",
            );
            qualifying
                .filter(|e| e.metadata.created_at <= threshold)
                .map(|e| e.sequence)
                .max()
                .unwrap_or(-1)
        }
    }
}

/// Every way [`consume_events_page`] can refuse a request - none of which
/// depend on the events - so a caller can check them before loading any.
/// `skilj-rest` does, because resolving a new `Latest`/`AtTime` cursor's
/// position walks the event type's whole history: checked only after
/// that walk, a revoked token, a type closed to reads, an invalid filter
/// or a first call without a `mode` paid for a full scan on every request
/// before being refused (docs/architecture.md §140).
pub fn check_consume_events(
    token: &EventReadToken,
    existing_cursor: Option<&ReadCursor>,
    ack_mode: Option<AckMode>,
    filters: &[Filter],
) -> crate::error::Result<()> {
    check_fetch_events(token, filters)?;
    match (existing_cursor, ack_mode) {
        (None, None) => Err(Error::CursorAckModeMismatch.into()),
        (Some(cursor), Some(requested)) if requested != cursor.ack_mode => {
            Err(Error::CursorAckModeMismatch.into())
        }
        _ => Ok(()),
    }
}

/// [`consume_events`] with a new cursor's starting position already
/// resolved (`new_cursor_position`, from [`initial_consume_position`];
/// ignored when `existing_cursor` is `Some`) and an explicit
/// `config.max_events_per_read`. `events` then only needs to hold the
/// candidates after that position - which is what lets `skilj-rest` load
/// them a chunk at a time.
///
/// `scanned_through` is the highest sequence of the token's event type
/// after the cursor's position that the caller examined, every such
/// event up to it being in `events` (`None` if it examined none). When
/// fewer than `max_events` are served, nothing up to it was left
/// unserved that this call could serve, so the cursor moves there rather
/// than stopping at the last event served (docs/architecture.md §112).
#[allow(clippy::too_many_arguments)]
pub fn consume_events_page(
    token: &EventReadToken,
    existing_cursor: Option<&ReadCursor>,
    ack_mode: Option<AckMode>,
    new_cursor_position: i64,
    events: &[Event],
    scanned_through: Option<i64>,
    filters: &[Filter],
    now: chrono::DateTime<chrono::Utc>,
    checkout_lease: chrono::Duration,
    max_events: usize,
) -> crate::error::Result<ConsumeEventsResult> {
    check_consume_events(token, existing_cursor, ack_mode, filters)?;
    let read_type = &token.event_type;
    let is_new = existing_cursor.is_none();

    let mode = ack_mode
        .or_else(|| existing_cursor.map(|c| c.ack_mode))
        .expect("is_new implies ack_mode.is_some(), checked above");

    // See `initial_consume_position` for how a new cursor's position is
    // chosen; the caller resolved it already.
    let position = existing_cursor.map_or(new_cursor_position, |cursor| cursor.sequence);

    // `manual_ack` only, and always `false` when `is_new` (a cursor just
    // being provisioned was never claimed by anyone) - see
    // `ReadCursor.checked_out_at`'s own spec doc comment. A claim older
    // than `checkout_lease` is treated as abandoned rather than live, the
    // same "self-healing, not a hard failure" register
    // `fire_due_deadlines`'s own missed-occurrence handling already uses.
    let is_leased = mode == AckMode::ManualAck
        && !is_new
        && existing_cursor
            .and_then(|c| c.checked_out_at)
            .is_some_and(|checked_out_at| checked_out_at + checkout_lease > now);

    // `is_leased` overrides everything else to the empty set - a second
    // caller racing a live claim sees no candidates at all, not a
    // filtered or scope-narrowed view of them (see `rule ConsumeEvents`'
    // own identical comment).
    let served: Vec<Event> = if is_leased {
        Vec::new()
    } else {
        events
            .iter()
            .filter(|e| e.bounded_context.same_as(&read_type.bounded_context))
            .filter(|e| e.event_type.same_as(read_type))
            .filter(|e| e.sequence > position)
            .filter(|e| matches_filters(e, filters))
            .filter(|e| event_owner_scope_satisfied(e, token.scope.as_deref()))
            .take(max_events)
            .cloned()
            .collect()
    };

    // docs/architecture.md §112: a short page means every examined event
    // this call didn't serve failed its filters or scope, so the cursor
    // passes over them to `scanned_through` - otherwise a narrow filter
    // or scope walked the same non-matching events again on every poll.
    // A full page stops at its last event: what follows wasn't looked at.
    let caught_up = (served.len() < max_events).then(|| {
        position
            .max(highest_sequence(&served).unwrap_or(position))
            .max(scanned_through.unwrap_or(position))
    });
    let next_position = match mode {
        AckMode::AutoAdvance => {
            caught_up.unwrap_or_else(|| highest_sequence(&served).unwrap_or(position))
        }
        // Nothing served means nothing to acknowledge or redeliver, so a
        // manual cursor passes over the examined events too. With events
        // served, only an acknowledgement moves it.
        AckMode::ManualAck if !is_leased && served.is_empty() => caught_up.unwrap_or(position),
        AckMode::ManualAck => position,
    };

    let cursor_update = if is_new {
        CursorUpdate::Created(Box::new(ReadCursor {
            token: token.clone(),
            ack_mode: mode,
            sequence: next_position,
            // A claim protects a served batch; an empty page has none, and
            // claiming it would leave the next polls empty - new events
            // included - until the lease lapsed (§112).
            checked_out_at: (mode == AckMode::ManualAck && !served.is_empty()).then_some(now),
            updated_at: now,
        }))
    } else if mode == AckMode::AutoAdvance {
        CursorUpdate::Advanced {
            sequence: next_position,
            updated_at: now,
        }
    } else if !is_leased && served.is_empty() {
        if next_position == position {
            CursorUpdate::Unchanged
        } else {
            CursorUpdate::Advanced {
                sequence: next_position,
                updated_at: now,
            }
        }
    } else if !is_leased {
        // manual_ack, and either never claimed or a stale claim just
        // reclaimed - this call's own served batch is now this call's
        // claim to hold until it acknowledges or its own lease lapses in
        // turn.
        CursorUpdate::Claimed {
            checked_out_at: now,
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

/// Every `requires` of `rule CreateExternalEvent`, returning the event's
/// tags (derived for the scope check). None needs the database, so
/// `db::create_and_insert_external_event` checks them before it
/// provisions encryption keys, takes the sequence lock or consults the
/// dedupe watermark: checked only inside [`create_external_event`], a
/// revoked token or an invalid payload could create an `EncryptionKey`
/// for any subject value it named, and a revoked token's redelivery was
/// answered `redelivered` (docs/architecture.md §141).
pub fn check_create_external_event(
    adapter: &ExternalEventToken,
    payload: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
) -> crate::error::Result<Vec<Tag>> {
    if adapter.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    if !adapter.event_type.external_creation_allowed {
        return Err(Error::ExternalCreationNotAllowed.into());
    }
    check_event_creation(
        &adapter.event_type,
        adapter.scope.as_deref(),
        payload,
        correlation_id,
        causation_id,
    )
}

/// [`check_create_external_event`]'s counterpart for `rule
/// CreateDirectEvent`.
pub fn check_create_direct_event(
    adapter: &DirectCreationToken,
    payload: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
) -> crate::error::Result<Vec<Tag>> {
    if adapter.status != TokenStatus::Active {
        return Err(crate::access_control::Error::TokenNotActive.into());
    }
    if !adapter.event_type.direct_creation_allowed {
        return Err(Error::DirectCreationNotAllowed.into());
    }
    check_event_creation(
        &adapter.event_type,
        adapter.scope.as_deref(),
        payload,
        correlation_id,
        causation_id,
    )
}

/// The `requires` `CreateExternalEvent` and `CreateDirectEvent` share,
/// after each one's own token status and opt-in checks.
fn check_event_creation(
    event_type: &EventType,
    scope: Option<&str>,
    payload: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
) -> crate::error::Result<Vec<Tag>> {
    if event_type.bounded_context.status != BoundedContextStatus::Active {
        return Err(Error::BoundedContextArchived.into());
    }
    if !valid_payload(&event_type.schema, payload) {
        return Err(Error::PayloadDoesNotMatchSchema.into());
    }
    // Codeberg issue #18 - see `valid_correlation_id`'s own doc comment.
    // Neither field takes any part in the adjacent dedup mechanism
    // (`dedupe_partition_key`/`dedupe_sequence`, docs/architecture.md
    // §39): a redelivery with a fresh correlation_id is still the same
    // redelivery.
    if !valid_correlation_id(correlation_id) || !valid_correlation_id(causation_id) {
        return Err(Error::CorrelationIdTooLong.into());
    }
    // Cross-tenant write fix (docs/architecture.md's own write-up of
    // these passes) - the write-side counterpart to EventFetch's own
    // EventsScopedToOwnerWhenDeclared read guarantee: a token that names
    // a scope may create events only for the owner it names. Single-
    // record, so this rejects outright rather than filtering, the same
    // reasoning authorise_command_submission's own note gives. Vacuously
    // true for a token naming no scope or an event type declaring no
    // owner dimension.
    let tags = derive_tags(&event_type.tag_mappings, payload);
    if !tag_owner_scope_satisfied(&tags, event_type.owner_tag_key.as_deref(), scope) {
        return Err(crate::access_control::Error::GrantScopeMismatch.into());
    }
    Ok(tags)
}

/// See `rule CreateExternalEvent`. `next_sequence` is the value the
/// caller's own `next_sequence(bounded_context)` already allocated under
/// its Postgres row lock (see the note above the rules) - not this
/// function's to compute, the same pure/no-I/O split every other rule in
/// this module uses. Same "derived, not a separate parameter" treatment
/// as `fetch_events`' `read_type` for `adapter.event_type = event_type`:
/// there's no `event_type` argument to disagree with `adapter.event_type`.
#[allow(clippy::too_many_arguments)]
pub fn create_external_event(
    adapter: &ExternalEventToken,
    payload: String,
    source_content: String,
    source_context: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    next_sequence: i64,
    now: chrono::DateTime<chrono::Utc>,
    resolve_key: impl Fn(&str, &str) -> (EncryptionKey, DataKey),
) -> crate::error::Result<Event> {
    let tags = check_create_external_event(
        adapter,
        &payload,
        correlation_id.as_deref(),
        causation_id.as_deref(),
    )?;
    let event_type = &adapter.event_type;

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
            correlation_id: Some(
                correlation_id
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(crate::shared::generate_token_id),
            ),
            causation_id: causation_id.filter(|id| !id.is_empty()),
        },
        sequence: next_sequence,
        tags,
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
    correlation_id: Option<String>,
    causation_id: Option<String>,
    next_sequence: i64,
    now: chrono::DateTime<chrono::Utc>,
    resolve_key: impl Fn(&str, &str) -> (EncryptionKey, DataKey),
) -> crate::error::Result<Event> {
    let tags = check_create_direct_event(
        adapter,
        &payload,
        correlation_id.as_deref(),
        causation_id.as_deref(),
    )?;
    let event_type = &adapter.event_type;

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
            correlation_id: Some(
                correlation_id
                    .filter(|id| !id.is_empty())
                    .unwrap_or_else(crate::shared::generate_token_id),
            ),
            causation_id: causation_id.filter(|id| !id.is_empty()),
        },
        sequence: next_sequence,
        tags,
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

/// The latest occurrence of `schedule` at or before `instant` - the one a
/// `fire_once` backlog collapses into - found without walking the
/// backlog (docs/architecture.md §150). `cron`'s backward step gives an
/// occurrence just before `instant`; stepping forward from it while the
/// next one is still at or before `instant` makes the answer exact
/// whatever that step does at the boundary. `None` when the schedule
/// doesn't parse or has no occurrence at or before `instant`.
pub fn latest_occurrence_at_or_before(
    schedule: &str,
    instant: chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    use std::str::FromStr;
    let schedule = cron::Schedule::from_str(schedule).ok()?;
    let mut latest = schedule.after(&instant).next_back()?;
    while let Some(next) = schedule.after(&latest).next() {
        if next > instant {
            break;
        }
        latest = next;
    }
    (latest <= instant).then_some(latest)
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
            // Always a root - a scheduler tick has no caller and no
            // upstream cause to inherit (Codeberg issue #18).
            correlation_id: Some(crate::shared::generate_token_id()),
            causation_id: None,
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
    // Drift audit finding #10 (2026-08-20, see project memory
    // `skilj-drift-audit-2026-08-20`): `rule SkipMissedOccurrences`'s own
    // first `requires` clause, missing here until this fix. Not reachable
    // from the real scheduler today - `list_scheduled_event_types` already
    // filters to this flag before this function is ever called - but a
    // pure function should faithfully encode every one of its rule's
    // `requires` clauses regardless of what its current callers happen to
    // pre-filter, the same standard every other pure function in this
    // module is held to.
    if !event_type.system_triggered_allowed {
        return None;
    }
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
/// one command-processing pipeline both funnel into. `consistency_tags`
/// is a Rust-only addition beyond the spec's own `CommandAuthorised`
/// fact: both authorising functions already have to compute
/// `derive_tags(command_type, payload)` themselves to check
/// `tag_owner_scope_satisfied` before returning, and every caller needs the
/// identical value again immediately afterward (to fetch
/// `matching_events`) - carrying it here is one computation instead of
/// two, not a behaviour change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandAuthorised {
    pub command_type: CommandType,
    pub payload: String,
    pub client_id: String,
    pub consistency_tags: Vec<Tag>,
    /// The caller's own opaque value, threaded through unvalidated here -
    /// same footing `idempotency_key` already has at this layer.
    /// `process_command` (Codeberg issue #18) is where
    /// `valid_correlation_id` actually gates it and where an absent
    /// `correlation_id` gets generated.
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

/// See `rule AuthoriseCommandTrigger`. Same "derived, not a separate
/// parameter" treatment as `fetch_events`/`create_external_event` for
/// `token.command_type = command_type`. Reuses `Error::BoundedContextArchived`
/// for `command_type.bounded_context.status = active` - same rejection,
/// same meaning, whichever rule hits it.
pub fn authorise_command_trigger(
    token: &CommandToken,
    payload: String,
    correlation_id: Option<String>,
    causation_id: Option<String>,
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
    // Cross-tenant write fix (docs/architecture.md's own write-up of
    // these passes) - the write-side counterpart to CommandQuery's own
    // command_owner_scope_satisfied read check: a token that names a
    // scope may trigger commands only for the owner it names, checked
    // against its own scope rather than any minting admin's, exactly as
    // EventFetch's tokens are (see create_event_read_token's own doc
    // comment). Single-record, so this rejects outright rather than
    // filtering - there is exactly one command being authorised here.
    let consistency_tags = derive_tags(&command_type.tag_mappings, &payload);
    if !tag_owner_scope_satisfied(
        &consistency_tags,
        command_type.owner_tag_key.as_deref(),
        token.scope.as_deref(),
    ) {
        return Err(crate::access_control::Error::GrantScopeMismatch.into());
    }

    Ok(CommandAuthorised {
        command_type: command_type.clone(),
        payload,
        client_id: token.id.clone(),
        consistency_tags,
        correlation_id,
        causation_id,
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
    correlation_id: Option<String>,
    causation_id: Option<String>,
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
    if !access_mapping
        .bounded_context
        .same_as(&command_type.bounded_context)
    {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if command_type.bounded_context.status != BoundedContextStatus::Active {
        return Err(Error::BoundedContextArchived.into());
    }
    if !valid_payload(&command_type.schema, &payload) {
        return Err(Error::PayloadDoesNotMatchSchema.into());
    }
    // Cross-tenant write fix (docs/architecture.md's own write-up of
    // these passes) - the write-side counterpart to CommandQuery's own
    // CommandsScopedToOwnerWhenDeclared read guarantee: a grant that
    // names a scope may submit commands only for the owner it names,
    // checked against the same command_type.owner_tag_key/derived tags
    // the read side already reads back with command_owner_scope_satisfied,
    // just before the record exists rather than after. Single-record, so
    // this rejects outright rather than filtering, the same shape
    // InspectEvent/InspectSnapshot already have - there is exactly one
    // command being authorised here, so there is nothing to filter, only
    // one thing to refuse. Vacuously true, same as always, for a grant
    // naming no scope or a command type declaring no owner dimension.
    let consistency_tags = derive_tags(&command_type.tag_mappings, &payload);
    if !tag_owner_scope_satisfied(
        &consistency_tags,
        command_type.owner_tag_key.as_deref(),
        access_mapping.scope.as_deref(),
    ) {
        return Err(crate::access_control::Error::GrantScopeMismatch.into());
    }

    Ok(CommandAuthorised {
        command_type: command_type.clone(),
        payload,
        client_id: access_mapping.role.id.clone(),
        consistency_tags,
        correlation_id,
        causation_id,
    })
}

/// See `rule DryRunCommand`. `authorise_command_submission`'s checks with
/// the access level raised to admin, since a dry-run shows event content
/// (the would-be events and the matching events). The returned
/// `CommandAuthorised` carries no correlation or causation id: nothing is
/// stored for either to label.
pub fn authorise_command_dry_run(
    access_mapping: &RoleAccessMapping,
    command_type: &CommandType,
    payload: String,
) -> crate::error::Result<CommandAuthorised> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    authorise_command_submission(access_mapping, command_type, payload, None, None)
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
/// authorisation paths: `AuthoriseCommandTrigger` (REST/`CommandToken`)
/// and `AuthoriseCommandSubmission` (GraphQL/`RoleAccessMapping`) are both
/// fully implemented, each in `resolvers::command_submission`/
/// `skilj-rest`'s own `post_commands_trigger` respectively - this function
/// itself doesn't care which one produced its `command_type`/`payload`/
/// `client_id` input, and never has to know.
///
/// `decision` is already-computed, not called from inside this function:
/// invoking the right bounded context's own typed `CommandType::decide()`
/// crosses a type-erasure boundary this internal engine sits on the far
/// side of (raw JSON/`EventType`-by-name) - the spec itself says that
/// wiring "stays a prose contract here, not a formally specified
/// interface" (see the note above the rule), so resolving it isn't this
/// pure function's job. Same reasoning for `resolve_event_type`: turning
/// `decision`'s `EventSpec.event_type: String` names back into full
/// `EventType`s is a bounded-context-scoped registry lookup - real now,
/// `db::get_event_type` - supplied by the caller since resolving it is
/// I/O, not this pure function's own job. `next_sequence` is called once
/// per accepted event, in order - the same Postgres-lock-backed,
/// caller-supplied value as `create_external_event`/`create_direct_event`'s.
///
/// A dozen parameters (`correlation_id`/`causation_id`, Codeberg issue
/// #18, are the newest two) because this function's own scope is
/// genuinely that wide - it's `ProcessCommand`'s entire `let`/`ensures`
/// body, not something a smaller grouping would simplify without
/// inventing a struct that exists only to satisfy the lint. `resolve_key` is
/// `protect_sensitive_fields`'s own pre-resolution parameter, threaded
/// through unchanged to both of this function's own call sites below (the
/// command's payload, and each accepted event spec's) - a command and an
/// event naming the same subject resolve to the very same `EncryptionKey`
/// this way, exactly as the spec requires. `id` is `Command.id`'s own
/// already-resolved `generate_token_id()` output (drift audit finding
/// #12), caller-supplied the same way `next_sequence`'s own values are -
/// this function has no way to generate one itself, the same "black box
/// resolved outside, handed in" treatment every id-bearing entity's own
/// pure constructor gets (see `access_control::create_role`).
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        bounded_context = %command_type.bounded_context.name,
        command_type = %command_type.name,
    )
)]
pub fn process_command(
    id: String,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    bounded_context_events: &[Event],
    decision: CommandDecision,
    resolve_event_type: impl Fn(&str) -> Option<EventType>,
    mut next_sequence: impl FnMut() -> i64,
    now: chrono::DateTime<chrono::Utc>,
    resolve_key: impl Fn(&str, &str) -> (EncryptionKey, DataKey),
) -> crate::error::Result<ProcessCommandResult> {
    // Codeberg issue #18 - see `valid_correlation_id`'s own doc comment.
    // Checked here rather than only at the authorisation layer above
    // this function (docs/architecture.md §8 item 4's own "resolving the
    // wiring isn't this pure function's job" split notwithstanding):
    // `ProcessCommand`'s own `requires` names this guard directly, so it
    // belongs on the function that is this rule's actual implementation.
    if !valid_correlation_id(correlation_id) || !valid_correlation_id(causation_id) {
        return Err(Error::CorrelationIdTooLong.into());
    }
    // docs/architecture.md §96: archiving stops new commands and events.
    // The public surfaces refuse an archived context in their own
    // authorisation rules, but routes, deadlines and parked redrives
    // submit without one - so the rule every command goes through
    // enforces it for all of them.
    if command_type.bounded_context.status != BoundedContextStatus::Active {
        return Err(Error::BoundedContextArchived.into());
    }

    let event_specs = match decision {
        CommandDecision::Accepted { events } => events,
        CommandDecision::Rejected { reason, kind } => {
            return Err(crate::error::Error::CommandRejected { reason, kind });
        }
    };

    // Every stored Command ends up with a correlation_id - generated
    // here when the caller didn't supply one - per the spec's own
    // `CorrelationIdIsAlwaysRecorded` invariant. causation_id is never
    // generated: absent means "this is a root", a true statement for a
    // plain submission.
    let correlation_id = correlation_id
        .filter(|id| !id.is_empty())
        .map(str::to_string)
        .unwrap_or_else(crate::shared::generate_token_id);
    let causation_id = causation_id.filter(|id| !id.is_empty()).map(str::to_string);

    let consistency_tags = derive_tags(&command_type.tag_mappings, payload);
    let (consistency_boundary, _matching_events) =
        consistency_boundary_and_matching_events(bounded_context_events, &consistency_tags);

    let protected = protect_sensitive_fields(&command_type.sensitive_fields, payload, &resolve_key);
    let command = Command {
        id,
        bounded_context: command_type.bounded_context.clone(),
        command_type: command_type.clone(),
        payload: protected.payload,
        metadata: Metadata {
            r#type: command_type.name.clone(),
            version: command_type.schema_version,
            client_id: client_id.to_string(),
            created_at: now,
            correlation_id: Some(correlation_id),
            causation_id,
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
                // Every event a command triggers inherits that command's
                // correlation_id (always present by this point) and is
                // caused by it directly - Codeberg issue #18.
                correlation_id: command.metadata.correlation_id.clone(),
                causation_id: Some(command.id.clone()),
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
