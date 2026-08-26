//! The public plugin API - the one module every consuming application's
//! own code touches directly. See docs/architecture.md §1 for the full
//! reasoning behind this shape.

use crate::event_store::{Event, MissedOccurrencePolicy};
use crate::shared::{CommandDecision, SensitiveField, TagMapping};
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Serialize};

/// See `CommandType::required_role`'s own doc comment and docs/
/// architecture.md §1.3.1 - the one deliberate proc-macro exception to
/// this module's otherwise macro-free plugin API. Re-exported here so a
/// consumer only ever needs `skilj_core::plugin::{CommandType,
/// requires_role}`, not a direct dependency on `skilj-macros` itself.
pub use skilj_macros::requires_role;

/// The fallback every plugin trait's `BOUNDED_CONTEXT` associated const
/// carries - see that const's own doc comment on `EventType`/
/// `CommandType`/`Projection`. Also what `skilj::SkiljBuilder::builder()`
/// seeds `current_bounded_context` with, so a single-bounded-context app
/// never has to call `.bounded_context(...)` at all: every `.event_type::<T>()`/
/// `.command_type::<T>()`/`.projection::<T>()` call registers under this
/// name until something changes it.
pub const DEFAULT_BOUNDED_CONTEXT: &str = "default";

/// Bridges a raw, type-erased `event_store::Event` into the
/// strongly-typed, hand-written per-bounded-context event enum
/// `decide()`/`project()` pattern-match over (§1.4's `BankingEvent`, say),
/// implemented once per bounded context, on that enum. See docs/
/// architecture.md §1.6 for the full reasoning; §1.7's decider bridge is
/// the one caller of `try_from_event` in this crate.
///
/// ```ignore
/// impl BoundedContextEvent for BankingEvent {
///     fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
///         match event.event_type.name.as_str() {
///             "MoneyDeposited" => Some(serde_json::from_str(&event.payload).map(Self::MoneyDeposited)),
///             "MoneyWithdrawn" => Some(serde_json::from_str(&event.payload).map(Self::MoneyWithdrawn)),
///             _ => None,
///         }
///     }
/// }
/// ```
pub trait BoundedContextEvent: Sized {
    /// `None` when `event.event_type.name` doesn't match any variant this
    /// enum declares - never expected in practice (a caller only ever
    /// passes this bounded context's own events), so `None` is a
    /// defensive case, not a designed-for one. `Some(Err(..))` when the
    /// stored payload doesn't deserialize into the matched variant's
    /// payload type - narrower than it used to be (an externally- or
    /// directly-created event's own payload is now schema-checked before
    /// it's ever stored), but still reachable: a command-triggered or
    /// system-triggered event's payload came from `decide()`/
    /// `scheduled_payload`, which are never schema-checked at all (see
    /// the note above `entity CommandType`'s "Payload schema shape" in
    /// specs/skilj.allium), and even a schema-valid payload can still be
    /// stricter than the compiled Rust type expects.
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>>;
}

/// One command type a bounded context registers.
///
/// `decide()` is synchronous and pure: it receives only `payload` and
/// `matching_events`, nothing else - no database access, no I/O. That's
/// deliberate (§1.1): `ProcessCommand`'s optimistic-then-locked retry may
/// call `decide()` more than once per submission.
pub trait CommandType {
    /// The payload shape, and the source of its JSON Schema - derived via
    /// `schemars`, never hand-written (§1.2).
    type Payload: Serialize + DeserializeOwned + JsonSchema;

    /// The bounded context's own generated event enum, one variant per
    /// registered `EventType`. `matching_events` is typed against this so
    /// a missed match arm is a compile error, not a silently-ignored
    /// event type (§1.4). `: BoundedContextEvent` is what lets §1.7's
    /// decider bridge convert this bounded context's raw, stored `Event`s
    /// into it before calling `decide()`.
    type Event: BoundedContextEvent;

    const NAME: &'static str;

    /// Which bounded context `skilj::SkiljBuilder::auto_register()`
    /// registers this command type under - see `skilj_macros::auto_register`'s
    /// own doc comment for the `#[auto_register]` attribute that reads
    /// this. Irrelevant to manual `.bounded_context(name).command_type::<T>()`
    /// chaining, which ignores this const entirely and keeps working
    /// exactly as before.
    ///
    /// Defaults to `DEFAULT_BOUNDED_CONTEXT` ("default"), so a
    /// single-bounded-context app never has to override this at all. A
    /// multi-context app points it at its own module-level
    /// `BOUNDED_CONTEXT` const (the same one `skilj-demo`'s `banking`/
    /// `courses` modules already declare), e.g.
    /// `const BOUNDED_CONTEXT: &'static str = BOUNDED_CONTEXT;`.
    const BOUNDED_CONTEXT: &'static str = DEFAULT_BOUNDED_CONTEXT;

    fn tag_mappings() -> Vec<TagMapping> {
        Vec::new()
    }

    fn sensitive_fields() -> Vec<SensitiveField> {
        Vec::new()
    }

    /// Opt-in: submitting over GraphQL with a write-level grant is always
    /// permitted; triggering over REST with a `CommandToken` needs this.
    fn rest_trigger_allowed() -> bool {
        false
    }

    /// An extra caller-facing gate, on top of the ordinary write-level
    /// `RoleAccessMapping` check: `None` (the default) means no extra
    /// restriction. Set this by writing `#[requires_role("name")]`
    /// directly above the `impl CommandType for ...` block - see
    /// `requires_role`'s own doc comment - rather than overriding this
    /// method by hand; the attribute is what makes the requirement read
    /// as part of the command's own declaration.
    ///
    /// Not a spec-level concept, not persisted anywhere: `Role.name` has
    /// no uniqueness guarantee in `specs/skilj.allium` (see `entity
    /// Role`), so this is only as safe as the deployment's own discipline
    /// keeping role names meaningful and non-colliding - a `skilj-core`
    /// concern, not something the engine itself enforces. Checked
    /// exclusively by `skilj-graphql`'s own mutation resolver (docs/
    /// architecture.md §1.3.1, §8 item 5) - REST triggering is untouched,
    /// since `CommandToken` is already its own, separate per-token grant.
    fn required_role() -> Option<&'static str> {
        None
    }

    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision;
}

/// One event type a bounded context registers. Unlike `CommandType`/
/// `Projection`, there's no behaviour to plug in here - an `EventType` is
/// pure declaration (its schema, its tag/sensitivity mappings, which
/// origins may create it).
pub trait EventType {
    type Payload: Serialize + DeserializeOwned + JsonSchema;

    const NAME: &'static str;

    /// See `CommandType::BOUNDED_CONTEXT`'s own doc comment - identical
    /// role and default here, for `skilj::SkiljBuilder::auto_register()`'s
    /// `EventType` side.
    const BOUNDED_CONTEXT: &'static str = DEFAULT_BOUNDED_CONTEXT;

    fn tag_mappings() -> Vec<TagMapping> {
        Vec::new()
    }

    fn sensitive_fields() -> Vec<SensitiveField> {
        Vec::new()
    }

    fn external_creation_allowed() -> bool {
        false
    }

    fn direct_creation_allowed() -> bool {
        false
    }

    fn event_read_allowed() -> bool {
        false
    }

    /// Opt-in: `rule CreateSystemEvent`'s own gate, `EventType.
    /// system_triggered_allowed`. `false` by default, like every other
    /// creation-path flag on this trait - a type that never overrides
    /// this three plus `system_triggered_schedule`/`missed_occurrence_policy`
    /// is never scheduled at all, so `scheduled_payload` below is never
    /// called for it.
    fn system_triggered_allowed() -> bool {
        false
    }

    /// The 7-field Quartz-dialect cron expression (`cron` crate) the
    /// scheduler evaluates - required, and checked at registration time
    /// (see `Error::MissingScheduleOrPolicy`), when
    /// `system_triggered_allowed()` is `true`. `None` otherwise.
    fn system_triggered_schedule() -> Option<String> {
        None
    }

    /// See `enum MissedOccurrencePolicy`. Deliberately no default that
    /// silently picks one policy over another - the user's own call on
    /// this: "either way can be potentially dangerous", so a type opting
    /// into scheduling must say which it means, explicitly, by
    /// overriding this method; `register_event_type`'s own guard rejects
    /// registration outright if this is still `None` while
    /// `system_triggered_allowed()` is `true`.
    fn missed_occurrence_policy() -> Option<MissedOccurrencePolicy> {
        None
    }

    /// `scheduled_payload(event_type)` - the black box `rule
    /// CreateSystemEvent` names. Called at most once per eligible
    /// occurrence (see `event_store::create_system_event`'s own doc
    /// comment on why it's never called for one that's about to be
    /// rejected), never for a type that isn't `system_triggered_allowed`.
    /// Panics if reached without being overridden - a type opting into
    /// scheduling without ever producing a payload for it is a genuine
    /// programming error, not a runtime condition to handle gracefully.
    fn scheduled_payload() -> Self::Payload {
        unimplemented!(
            "{}::scheduled_payload() must be overridden - this event type is registered with \
             system_triggered_allowed() = true, so the scheduler needs a payload for every \
             occurrence it fires",
            Self::NAME
        )
    }
}

/// Type-erased access to a bounded context's own `EventType::
/// scheduled_payload()`, for the background scheduler in `db::
/// fire_system_event` - the `EventType` equivalent of `CommandDispatcher`/
/// `ProjectionDispatcher` above `Skilj` implements over its own compiled-in
/// registry.
pub trait EventDispatcher: Send + Sync {
    /// `scheduled_payload()`'s own output, JSON-serialised - `None` when
    /// no `(bounded_context, event_type)` pair matches anything
    /// registered in this process, the same "pair isn't registered at
    /// all" convention `CommandDispatcher::dispatch`/`ProjectionDispatcher::
    /// keys` already use. A genuine misconfiguration when it happens for
    /// a type the database itself marks `system_triggered_allowed` (see
    /// `@guarantee ScheduleStateIsShared`'s own expectation that every
    /// instance in a cluster registers the same scheduled types) -
    /// `db::fire_system_event`'s caller treats it as a skip, not a panic,
    /// the same tolerance `catch_up_bounded_context` already extends to
    /// an unregistered projection dispatcher.
    fn scheduled_payload(&self, bounded_context: &str, event_type: &str) -> Option<String>;
}

/// One projection a bounded context registers.
///
/// `project()` folds exactly one event into this projection's stored
/// state - synchronous, like `decide()`, and for the same reason (§1.1).
pub trait Projection {
    type State: Serialize + DeserializeOwned + JsonSchema + Default;

    /// Same generated per-bounded-context event enum `CommandType::Event`
    /// uses - a projection folds any event type it consumes, not just
    /// one. Same `: BoundedContextEvent` bound and reasoning.
    type Event: BoundedContextEvent;

    const NAME: &'static str;

    /// See `CommandType::BOUNDED_CONTEXT`'s own doc comment - identical
    /// role and default here, for `skilj::SkiljBuilder::auto_register()`'s
    /// `Projection` side.
    const BOUNDED_CONTEXT: &'static str = DEFAULT_BOUNDED_CONTEXT;

    /// The `EventType::NAME`s this projection actually folds - `RegisterProjection`'s
    /// own `consumed_event_types`. No default: unlike `tag_mappings()`/
    /// `sensitive_fields()`, there's no reasonable "consumes nothing"
    /// default for a projection to fall back to, and Rust has no way to
    /// infer this from `project()`'s own body (which variants of
    /// `Self::Event` it actually matches on isn't something the type
    /// system exposes) - so it's declared explicitly, the same
    /// "must specify" register `NAME` itself already uses.
    fn consumed_event_types() -> Vec<&'static str>;

    /// Whether this projection updates inline, in the same transaction as
    /// the events it consumes (`true`), or via a background consumer
    /// (`false`, the default) - see `Projection.sync` in the spec.
    fn sync() -> bool {
        false
    }

    /// Which instance(s) of this projection `event` updates - see the
    /// instance-data note on `entity Projection` in the spec. Defaults to
    /// one constant, unnamed key (`""`) - the "single shared value" case
    /// every projection built before this pass already is, needing no
    /// change at all to keep working exactly as it already does.
    /// Returning more than one key (a transfer event naming both the
    /// giver's and the receiver's own account id, say) folds this event
    /// into each of those instances independently, via its own call to
    /// `project()` - see that method's own doc comment for the `key`
    /// parameter it's handed each time.
    fn keys(_event: &Self::Event) -> Vec<String> {
        vec![String::new()]
    }

    /// `key` is which instance is currently being folded - one of
    /// `Self::keys(event)`'s own return values, handed back so `project()`
    /// can tell them apart when an event touches more than one (compare
    /// `key` against the event's own fields to decide, e.g., whether this
    /// call is crediting or debiting). Ignored entirely by a projection
    /// that never overrides `keys()` - it only ever sees the one constant
    /// `""` instance, the same single fold every existing projection
    /// already does.
    fn project(state: &mut Self::State, event: &Self::Event, key: &str);
}

/// Type-erased dispatch to a bounded context's own typed `decide()` -
/// what `CommandTrigger`'s REST handler and GraphQL's `CommandSubmission`
/// resolver both call through to actually process a command. Lives here,
/// as a trait, rather than `skilj-rest`/
/// `skilj-graphql` reaching into whichever crate happens to build the
/// concrete registry (`skilj`'s `SkiljBuilder`, today): both surface
/// crates already depend on `skilj-core`, so this is the natural shared
/// boundary - the same "opaque handle both sides can reach" reasoning
/// `db::Pool` already uses. See docs/architecture.md §1.7/§8 item 4.
///
/// A consumer using `skilj-core` + `skilj-rest` directly, without the
/// `skilj` facade's builder, implements this by hand instead - nothing
/// about it is facade-specific.
pub trait CommandDispatcher: Send + Sync {
    /// `None` when no `(bounded_context, command_type)` pair matches
    /// anything registered - the caller decides what that means for its
    /// own wire contract (an unrecognised-command rejection, a 404,
    /// whatever fits), since neither existing wire contract has needed
    /// to yet and this trait shouldn't invent one on their behalf.
    fn dispatch(
        &self,
        bounded_context: &str,
        command_type: &str,
        payload: &str,
        matching_events: &[Event],
    ) -> Option<crate::error::Result<CommandDecision>>;

    /// The registered command type's own `CommandType::required_role()` -
    /// see that method's own doc comment. Outer `None` mirrors
    /// `dispatch`'s "pair isn't registered at all" convention; inner
    /// `None` is a registered command type declaring no extra gate (the
    /// default). Meant to be checked by `skilj-graphql`'s mutation
    /// resolver *before* calling `dispatch` at all, so an unauthorised
    /// caller never reaches `decide()` - not folded into `dispatch`
    /// itself, since REST triggering never needs this check (`CommandToken`
    /// is its own, separate per-token grant) and shouldn't pay for it.
    fn required_role(
        &self,
        bounded_context: &str,
        command_type: &str,
    ) -> Option<Option<&'static str>>;
}

/// Type-erased dispatch to a bounded context's own typed `project()` -
/// `CommandDispatcher`'s own counterpart for the other black box §1.1
/// draws the pure-function line around. What the new transactional
/// `db::insert_event_and_update_sync_projections` calls through to
/// actually fold an event into a `sync` projection's stored state - see
/// docs/architecture.md's own write-up of this pass (§8 item 6).
///
/// Async (`sync: false`, the default) projections are folded by the
/// background consumer `db::catch_up_bounded_context` - see its own doc
/// comment and docs/architecture.md's write-up of that pass. Same
/// `project()` call either way; the consumer doesn't know or care whether
/// its caller is a request (the sync path) or a poll tick (this one).
pub trait ProjectionDispatcher: Send + Sync {
    /// `None` when no `(bounded_context, projection_name)` pair matches
    /// anything registered - the same "pair isn't registered at all"
    /// convention `dispatch`/`project` below already use. `Some(vec![])`
    /// when the pair *is* registered but `event`'s own type isn't one
    /// this projection actually consumes (`Projection::keys` is never
    /// called for it) - `caught_up_to` still advances, no instance is
    /// touched, the identical "position always advances, state only
    /// changes when consumed" treatment this trait's own `project`
    /// already documents. Otherwise, `Projection::keys(event)` verbatim -
    /// every instance this one event updates, one call to `project`
    /// below per key.
    fn keys(
        &self,
        bounded_context: &str,
        projection_name: &str,
        event: &Event,
    ) -> Option<Vec<String>>;

    /// `None` when no `(bounded_context, projection_name)` pair matches
    /// anything registered - the same "pair isn't registered at all"
    /// convention `CommandDispatcher::dispatch` uses. `Some(Ok(new_state))`
    /// is the projection's `state_json` after folding `event` into it,
    /// for the one instance named by `key` (one of `keys`'s own return
    /// values) - `BoundedContextEvent::try_from_event` succeeding is not
    /// by itself permission to call `project()`; the caller only reaches
    /// this once per key `keys` returned.
    fn project(
        &self,
        bounded_context: &str,
        projection_name: &str,
        state_json: &str,
        event: &Event,
        key: &str,
    ) -> Option<crate::error::Result<String>>;

    /// `T::State::default()`, JSON-serialised - `None` for the identical
    /// "pair isn't registered at all" case `project`/`dispatch` already
    /// have. The only source `db::catch_up_bounded_context` has for a
    /// `ProjectionRebuild`'s own starting state when it begins (or
    /// resumes after a restage) folding one: a rebuild's `caught_up_to`
    /// resets to `None` on every `db::upsert_projection_rebuild` call,
    /// including a restage of an already-`building` row (see that
    /// function's own doc comment), so whatever state a previous attempt
    /// had accumulated is stale the moment that happens - only the
    /// dispatcher, not whichever caller triggered the reset, knows the
    /// *current* correct starting point.
    fn default_state(&self, bounded_context: &str, projection_name: &str) -> Option<String>;
}
