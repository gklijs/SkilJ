//! The public plugin API - the one module every consuming application's
//! own code touches directly. See [docs/architecture.md §1](../../../docs/architecture.md#plugin-api-decide-project) for the full
//! reasoning behind this shape.

use crate::event_store::{Event, MissedOccurrencePolicy};
use crate::shared::{CommandDecision, PrivateField, SensitiveField, Tag, TagMapping};
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

/// One step in an `upcast_payload` chain - see that function's own doc
/// comment. `to_version` is the schema_version this step's `transform`
/// produces: a payload whose own `Metadata.version` is strictly less
/// than `to_version` needs this step applied before whichever step (or
/// final deserialization) comes next.
#[derive(Clone, Copy)]
pub struct UpcastStep {
    pub to_version: i64,
    pub transform: fn(serde_json::Value) -> serde_json::Value,
}

/// Sugar around the one pattern a hand-written `BoundedContextEvent::
/// try_from_event` already has every ingredient for: `event.metadata.version`
/// records the schema_version a payload was actually written under
/// (specs/skilj.allium's own note above, "that is what Metadata.version
/// is for"), and nothing stops a `try_from_event` arm from branching on
/// it before deserializing. This is that branch, generalised into a
/// declared chain of pure JSON transforms instead of a repeated
/// `match metadata.version { ... }` per event type. See
/// [docs/architecture.md §33](../../../docs/architecture.md#payload-upcasting) for the full "why not first-class
/// upcasting" investigation this is the one built piece of.
///
/// Not a replacement for `schema_is_backwards_compatible`'s additive-only
/// contract, and doesn't need to be: a genuinely reshaping change
/// (rename, retype, split/merge a field) still can't be registered as a
/// revision of an existing type - `Self::Payload`'s own derived JSON
/// Schema only ever has to describe the *current*, already-reshaped
/// value, because this runs entirely on the raw `serde_json::Value`
/// before `serde_json::from_value::<T>` ever sees it. The transform
/// chain lives in application code only; nothing about the registered
/// schema, `RegisterEventType`, or `RegisterCommandType` changes.
///
/// `chain` is walked once, in the order given, applying every step whose
/// `to_version` is strictly greater than `written_at_version` - a
/// payload written at version 1 runs every step in a `[..to 2, ..to 3]`
/// chain, one written at version 2 runs only the second, one written at
/// version 3 (or later) runs none. Steps are expected in ascending
/// `to_version` order; this does not sort them - an out-of-order chain
/// applies transforms in the wrong sequence, the same "caller's own
/// responsibility" register `consumed_event_types()` already trusts the
/// caller to get right.
///
/// ```ignore
/// // MoneyDeposited moved from a float-dollar `amount` (schema_version 1)
/// // to an integer-cent `amount_cents` (schema_version 2) - a genuine
/// // reshape schema_is_backwards_compatible would reject as a revision
/// // of the same type, handled here entirely in application code:
/// const MONEY_DEPOSITED_UPCASTS: &[UpcastStep] = &[UpcastStep {
///     to_version: 2,
///     transform: |mut v| {
///         if let Some(amount) = v.get("amount").and_then(|a| a.as_f64()) {
///             if let Some(obj) = v.as_object_mut() {
///                 obj.remove("amount");
///                 obj.insert("amount_cents".into(), ((amount * 100.0).round() as i64).into());
///             }
///         }
///         v
///     },
/// }];
///
/// "MoneyDeposited" => Some(
///     skilj_core::plugin::upcast_payload(
///         &event.payload,
///         event.metadata.version,
///         MONEY_DEPOSITED_UPCASTS,
///     )
///     .map(Self::MoneyDeposited),
/// ),
/// ```
pub fn upcast_payload<T: DeserializeOwned>(
    payload: &str,
    written_at_version: i64,
    chain: &[UpcastStep],
) -> Result<T, serde_json::Error> {
    let mut value: serde_json::Value = serde_json::from_str(payload)?;
    for step in chain {
        if written_at_version < step.to_version {
            value = (step.transform)(value);
        }
    }
    serde_json::from_value(value)
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

    /// See `plugin::EventType::owner_tag_key`'s own doc comment - identical
    /// role here: which of `tag_mappings()`'s own keys names this command
    /// type's "owner" dimension, `None` (the default) for one with no such
    /// notion. Registered and validated exactly the same way
    /// (`RegisterCommandType`'s own `valid_owner_tag_key`), and read by
    /// `event_store::command_owner_scope_satisfied` - `Command.consistency_tags`'
    /// own sibling check to `event_owner_scope_satisfied`'s `Event.tags`.
    /// Cross-tenant read fix (docs/architecture.md's own write-up of these
    /// passes).
    fn owner_tag_key() -> Option<&'static str> {
        None
    }

    fn sensitive_fields() -> Vec<SensitiveField> {
        Vec::new()
    }

    /// See `EventType::private_fields()`'s own doc comment - identical
    /// role, for `Command.payload` instead of `Event.payload`.
    fn private_fields() -> Vec<PrivateField> {
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
    /// architecture.md §1.3.1, [§8](../../../docs/architecture.md#open-for-a-future-pass) item 5) - REST triggering is untouched,
    /// since `CommandToken` is already its own, separate per-token grant.
    fn required_role() -> Option<&'static str> {
        None
    }

    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision;

    /// Opt-in accelerator for `decide()` against a large tag-scoped
    /// history ([docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 2") - names the
    /// `Snapshot::NAME` this command type's own decide() can fold
    /// forward from instead of replaying every matching event. `None`
    /// (the default) means no snapshot; every existing `CommandType`
    /// impl is completely unaffected.
    ///
    /// Only actually used when this command type derives exactly the
    /// one tag the named `Snapshot::TAG_KEY` is - a multi-tag command
    /// that names a snapshot anyway is a harmless misconfiguration,
    /// silently ignored (falls back to the ordinary `decide()` path),
    /// not a hard error - see `decide_from_snapshot`'s own doc comment
    /// for the *reachable* misconfiguration this trait can't catch for
    /// you.
    fn snapshot() -> Option<&'static str> {
        None
    }

    /// Called instead of `decide()` when `snapshot()` names a real,
    /// tag-matching `Snapshot` (see that method's own doc comment).
    /// `snapshot_state_json` is `Snapshot::State::default()`,
    /// JSON-encoded, when no snapshot has been written yet for this tag
    /// value, or when a stored one's own `snapshot_version` no longer
    /// matches `Snapshot::VERSION` - the model changed, so it's treated
    /// as absent rather than trusted; `events_since_snapshot` is then
    /// simply everything for this tag, the same set `decide()` would
    /// have seen. Correctness obligation: this must reach the identical
    /// `CommandDecision` `decide()` would over the *full* matching_events
    /// for the same payload - `snapshot_state_json` plus
    /// `events_since_snapshot` is meant to be exactly equivalent
    /// information, not an approximation.
    ///
    /// `snapshot_state_json` deliberately isn't a typed `Snapshot::State`,
    /// since linking `CommandType` to one specific `Snapshot` impl at
    /// the type level needs either a new required associated type
    /// (breaking every existing `CommandType` impl) or a *defaulted*
    /// one (`associated_type_defaults`, nightly-only). Deserialise it
    /// yourself, the same way `tag_mappings()`'s own `field` is already
    /// just a string, unchecked against `Payload` at compile time.
    ///
    /// The default implementation is a safe, non-panicking rejection -
    /// only ever reached if a `CommandType` overrides `snapshot()`
    /// without also overriding this: a real misconfiguration (unlike
    /// the multi-tag case above), but one this trait can still fail
    /// safely on rather than silently producing a wrong decision.
    fn decide_from_snapshot(
        _payload: &Self::Payload,
        _snapshot_state_json: &str,
        _events_since_snapshot: &[Self::Event],
    ) -> CommandDecision {
        CommandDecision::Rejected {
            reason: "this CommandType declares snapshot() but never overrides \
                     decide_from_snapshot()"
                .to_string(),
            kind: "snapshot_misconfigured".to_string(),
        }
    }
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

    /// Which of `tag_mappings()`'s own keys names this type's "owner"
    /// dimension, if it has one - `Some("company")` for a type whose
    /// events belong to different tenants sharing one bounded context,
    /// `None` (the default) for a type with no such notion. Unlike
    /// `plugin::Projection::OWNER_TAG_KEY` (deliberately Rust-only, no
    /// spec field), this one is registered and validated:
    /// `RegisterEventType`'s own `valid_owner_tag_key` requires it be
    /// null or name a real `tag_mappings()` key, since `EventType.tag_mappings`
    /// is already a real, spec'd, registered field this joins rather than
    /// a fresh Rust-only concept. Cross-tenant read fix
    /// (docs/architecture.md's own write-up of these passes).
    fn owner_tag_key() -> Option<&'static str> {
        None
    }

    fn sensitive_fields() -> Vec<SensitiveField> {
        Vec::new()
    }

    /// See `value PrivateField`'s own doc comment - a plain read-time
    /// redaction rule, opt-in per field, default empty like
    /// `sensitive_fields()` above. Private-field mechanism
    /// (docs/architecture.md's own write-up of this pass).
    fn private_fields() -> Vec<PrivateField> {
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

    /// The tag key (one of some consumed event type's own `tag_mappings`
    /// keys) that names this projection's "owner" dimension, if it has
    /// one - `Some("company")` for a projection whose instances belong to
    /// different tenants sharing one bounded context, `None` (the
    /// default) for a projection with no such notion, e.g. `keys()`
    /// already being a person's own id or the projection being unkeyed.
    /// Deliberately a Rust-only implementation detail with no spec entity
    /// field and no registration/admin-visible surface of its own - the
    /// identical treatment `Snapshot::TAG_KEY` already gets, and for the
    /// same reason: `RoleAccessMapping.scope` is what an admin actually
    /// grants and sees, not this.
    ///
    /// When set, each instance's own "owner" value is derived
    /// automatically at fold time from whichever event touched it: the
    /// value of the tag on `event.tags` whose key matches this one, if
    /// present. An event lacking that tag leaves an already-established
    /// owner untouched rather than clearing it. `query_projection`
    /// rejects a `scope`-restricted `RoleAccessMapping` whose `scope`
    /// does not match an instance's own derived owner - see that
    /// function's own doc comment and specs/skilj.allium's
    /// `owner_scope_satisfied`.
    const OWNER_TAG_KEY: Option<&'static str> = None;

    /// The `Role.name` required to query this projection at all, if any -
    /// `None` (the default, and every projection that predates this) for
    /// one with no such notion. Deliberately Rust-only, no spec entity
    /// field and no registration/admin-visible surface, the identical
    /// treatment `OWNER_TAG_KEY` just above gets and for the same reason.
    ///
    /// Unlike `OWNER_TAG_KEY`, which is derived per-instance from folded
    /// event tags, this is a fixed, whole-projection gate: it names no
    /// dimension to look up per instance, just one required team, so
    /// either every instance of this projection is reachable by a
    /// matching Role or none are - never a per-instance answer.
    /// Composes with `OWNER_TAG_KEY` rather than replacing it: a
    /// projection may declare both (staff-only *and* company-scoped),
    /// and `query_projection` checks both independently. See that
    /// function's own doc comment and specs/skilj.allium's
    /// `team_only_satisfied`.
    ///
    /// Genuinely different in kind from `private_fields`' own `Team`
    /// kind, even though the membership test is identical
    /// (`Role.name` equality) - `private_fields` redacts one field of
    /// one raw event/command, for one reader, traced back to who created
    /// that specific record; a projection's stored state is one shared
    /// value folded from possibly many events by `project()`, with no
    /// single record each field individually traces back to, so nothing
    /// short of a whole-instance gate generalizes to it (Codeberg issue
    /// #17's own finding, closing the gap left by `private_fields`
    /// itself: it protects `queryEvents`/`fetchCommands`, surfaces no
    /// `Write`-level Role can reach anyway, while `ProjectionQuery` - the
    /// one surface a `Write`-level Role *can* reach - stayed exactly as
    /// open as before).
    const TEAM_ONLY: Option<&'static str> = None;

    /// How many partitions this projection's own `keys()` values hash
    /// into for async catch-up (Codeberg issue #25,
    /// docs/architecture.md §51) - `1` (the default, and every
    /// projection that predates this) means every key hashes to the
    /// same partition, so `catch_up_bounded_context` takes its original,
    /// unpartitioned code path unchanged and order is exactly as it
    /// already was. A value greater than `1` lets multiple skilj
    /// instances each independently claim and fold a different slice of
    /// this projection's own keys concurrently, instead of every
    /// instance redundantly redoing all of it - see
    /// `catch_up_partitioned_projection`'s own doc comment for the
    /// mechanism. Deliberately Rust-only, no spec entity field and no
    /// registration/admin-visible surface, the identical treatment
    /// `OWNER_TAG_KEY`/`TEAM_ONLY` above already get: it's a pure
    /// work-distribution detail, invisible to what `project()` actually
    /// folds, so changing it between deploys needs no rebuild (unlike
    /// `schema`/`consumed_event_types`/`sync`, which change what gets
    /// folded - see `register_projection`'s own rebuild-trigger logic).
    /// Meaningful only for `sync() == false`; inert (never validated
    /// against, never rejected) for a sync one, the same stance
    /// `OWNER_TAG_KEY`/`TEAM_ONLY` already take toward combinations that
    /// don't make sense rather than erroring on them.
    const PARTITION_COUNT: u32 = 1;

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

/// A folded, tag-scoped accelerator for `CommandType::decide()` against
/// a large `matching_events` history ([docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s
/// "Problem 2") - `CommandType::snapshot()`'s own doc comment is where a
/// command type opts in.
///
/// Deliberately **not** `Projection`, even though the shape rhymes
/// (`fold` vs. `project`, `TAG_KEY` vs. `keys()`, `VERSION` vs. a
/// restage): `Projection`s are eventually-consistent, rebuildable/
/// restageable, Admin-exposed read-model infrastructure with no
/// obligation to the timing/correctness bar a DCB consistency check
/// needs - letting `decide()`'s own inputs depend on `Projection`
/// machinery would mean an operator rebuilding a projection for
/// ordinary read-model reasons could silently corrupt what `decide()`
/// sees. `Snapshot` is its own trait, own storage
/// (`{schema}.snapshots`/`{schema}.snapshot_progress`), own background
/// catch-up task, own inspection endpoint - no shared code path a
/// read-model change could destabilise.
///
/// Scoped to exactly one tag key, deliberately: `matching_events` is a
/// *union* across a command's own derived tags (`courses.rs`'s real
/// `EnrollStudentInCourse` unions `student` and `course` in one
/// `decide()` call), and only a single tag key has a stable, reusable
/// identity to snapshot against - a multi-tag command gets no benefit
/// here, out of scope for now (composing several single-tag snapshots
/// is feasible in principle, real unbuilt design work).
///
/// One `Snapshot` definition is shared by every `CommandType` that tags
/// on its `TAG_KEY` and opts in - `banking.rs`'s `DepositMoney`/
/// `WithdrawMoney` both tag on `account` and both want the same running
/// balance, so they share one `AccountBalanceSnapshot` rather than each
/// computing their own, free to drift apart.
pub trait Snapshot {
    type State: Serialize + DeserializeOwned + JsonSchema + Default;

    /// Same generated per-bounded-context event enum `CommandType::Event`/
    /// `Projection::Event` use - a snapshot folds any event type
    /// carrying its `TAG_KEY`, not just one.
    type Event: BoundedContextEvent;

    const NAME: &'static str;

    /// See `CommandType::BOUNDED_CONTEXT`'s own doc comment - identical
    /// role and default here, for `skilj::SkiljBuilder::auto_register()`'s
    /// `Snapshot` side.
    const BOUNDED_CONTEXT: &'static str = DEFAULT_BOUNDED_CONTEXT;

    /// The single tag key this snapshot is scoped to - see this trait's
    /// own doc comment for why only one. The background catch-up task
    /// derives which tag *value* an event belongs to generically, from
    /// `event.tags`, so there's no `keys()`-equivalent method to
    /// implement here the way `Projection` needs one.
    const TAG_KEY: &'static str;

    /// Which tag key names this snapshot's "owner" dimension, if it has
    /// one and it differs from `TAG_KEY` itself - `None` (the default)
    /// for a snapshot with no such notion. Deliberately Rust-only, no
    /// spec entity field and no registration surface, the identical
    /// treatment `TAG_KEY` itself already gets (`Snapshot` is compiled,
    /// deployed configuration throughout - see this trait's own doc
    /// comment). When set, each stored row's own owner value is derived
    /// at fold time from whichever event touched it: the value of the
    /// tag on `event.tags` whose key matches this one, if present - not
    /// necessarily the same tag `TAG_KEY` itself reads (a snapshot keyed
    /// by `"account"` might still need owner-scoping by `"company"`).
    /// `inspectSnapshot` rejects a `scope`-restricted `RoleAccessMapping`
    /// whose `scope` does not match a stored row's own derived owner -
    /// see `access_control::scope_matches_owner` and
    /// `snapshot_query::inspect_snapshot_field`'s own doc comment. Cross-
    /// tenant read fix (docs/architecture.md's own write-up of these
    /// passes).
    const OWNER_TAG_KEY: Option<&'static str> = None;

    /// Bumped by hand whenever `fold()`'s own logic or `State`'s shape
    /// changes - a stored row at an older version is treated as if it
    /// doesn't exist (see `CommandType::decide_from_snapshot`'s own doc
    /// comment), never trusted. Not inferred: Rust has no way to detect
    /// a fold's own semantic change, only a decider declaring one can.
    const VERSION: u64;

    /// How many partitions this snapshot's own tag values hash into for
    /// catch-up (Codeberg issue #25, docs/architecture.md §52 - the
    /// `Snapshot` twin of `Projection::PARTITION_COUNT`, see that
    /// const's own doc comment for the full reasoning, identical here
    /// with the derived tag *value* playing the role `Projection`'s own
    /// `keys()` return values play). `1` (the default) means every tag
    /// value hashes to the same partition, so `catch_up_snapshots` takes
    /// its original, unpartitioned code path unchanged.
    const PARTITION_COUNT: u32 = 1;

    /// Folds one event into `state`, in place - the identical shape
    /// `Projection::project` already has, minus the `key` parameter
    /// (`Snapshot` only ever folds one entity's own events into its own
    /// state, never several at once the way a multi-instance projection
    /// can).
    fn fold(state: &mut Self::State, event: &Self::Event);
}

/// Type-erased dispatch to a bounded context's own typed `decide()` -
/// what `CommandTrigger`'s REST handler and GraphQL's `CommandSubmission`
/// resolver both call through to actually process a command. Lives here,
/// as a trait, rather than `skilj-rest`/
/// `skilj-graphql` reaching into whichever crate happens to build the
/// concrete registry (`skilj`'s `SkiljBuilder`, today): both surface
/// crates already depend on `skilj-core`, so this is the natural shared
/// boundary - the same "opaque handle both sides can reach" reasoning
/// `db::Pool` already uses. See docs/architecture.md §1.7/[§8](../../../docs/architecture.md#open-for-a-future-pass) item 4.
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

    /// The registered command type's own `CommandType::snapshot()` -
    /// same outer/inner `Option` convention as `required_role`. Meant to
    /// be checked before `dispatch`, the same way `required_role` is:
    /// if this is `Some(Some(name))` and the command's own derived tags
    /// match `SnapshotDispatcher::tag_key(bc, name)` exactly, the caller
    /// should read that snapshot and call `dispatch_from_snapshot`
    /// instead of `dispatch` - see [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events).
    fn snapshot_name(
        &self,
        bounded_context: &str,
        command_type: &str,
    ) -> Option<Option<&'static str>>;

    /// `CommandType::decide_from_snapshot`'s own type-erased entry
    /// point - `dispatch`'s counterpart for the snapshot-accelerated
    /// path. `None` for the identical "pair isn't registered at all"
    /// case `dispatch` already has.
    fn dispatch_from_snapshot(
        &self,
        bounded_context: &str,
        command_type: &str,
        payload: &str,
        snapshot_state_json: &str,
        events_since_snapshot: &[Event],
    ) -> Option<crate::error::Result<CommandDecision>>;
}

/// Type-erased dispatch to a bounded context's own typed `project()` -
/// `CommandDispatcher`'s own counterpart for the other black box §1.1
/// draws the pure-function line around. What the new transactional
/// `db::insert_event_and_update_sync_projections` calls through to
/// actually fold an event into a `sync` projection's stored state - see
/// docs/architecture.md's own write-up of this pass ([§8](../../../docs/architecture.md#open-for-a-future-pass) item 6).
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

    /// The registered projection's own `Projection::OWNER_TAG_KEY` -
    /// `CommandDispatcher::snapshot_name`'s identical `Option<Option<_>>`
    /// shape: outer `None` for the same "pair isn't registered at all"
    /// case every method here already has, inner `None` when the
    /// projection is registered but declares no owner dimension (the
    /// default, and every projection that predates this).
    fn owner_tag_key(
        &self,
        bounded_context: &str,
        projection_name: &str,
    ) -> Option<Option<&'static str>>;

    /// The registered projection's own `Projection::TEAM_ONLY` - the
    /// identical `Option<Option<_>>` shape `owner_tag_key` just above
    /// already has, for the same reason. Defaulted to `None` (unlike
    /// `owner_tag_key`, which has no default) - every call site here
    /// only ever `.flatten()`s the result, for which the outer "pair
    /// isn't registered" `None` and the inner "registered, no team
    /// declared" `Some(None)` already read identically, so a dispatcher
    /// with nothing team-gated needs no explicit override at all.
    fn team_only(
        &self,
        _bounded_context: &str,
        _projection_name: &str,
    ) -> Option<Option<&'static str>> {
        None
    }

    /// The registered projection's own `Projection::PARTITION_COUNT`
    /// (Codeberg issue #25, docs/architecture.md §51) - `None` for the
    /// same "pair isn't registered at all" case every method here
    /// already has, treated by every real caller (`db::
    /// catch_up_bounded_context`) identically to `Some(1)`: unpartitioned.
    /// Defaulted (unlike `owner_tag_key`, which has no default) for the
    /// same reason `team_only` is - a dispatcher with nothing partitioned
    /// needs no explicit override at all.
    fn partition_count(&self, _bounded_context: &str, _projection_name: &str) -> Option<u32> {
        None
    }
}

/// Type-erased dispatch to a bounded context's own typed `Snapshot::fold` -
/// `CommandDispatcher`'s `dispatch_from_snapshot`/`snapshot_name`'s own
/// counterpart for the other half of the type-erasure boundary, and what
/// `db::catch_up_snapshots` (the background catch-up task -
/// [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)) calls through to actually fold an event
/// into a stored snapshot row. Same "outer `None` = not registered"
/// convention every other dispatcher trait in this module already uses.
pub trait SnapshotDispatcher: Send + Sync {
    /// Every `Snapshot::NAME` registered for `bounded_context` in this
    /// process - `db::catch_up_snapshots`' own discovery mechanism,
    /// since (deliberately - see `Snapshot`'s own doc comment) there is
    /// no `snapshots` metadata table to enumerate the way
    /// `list_projections_for_bounded_context` does for `Projection`.
    fn snapshot_names(&self, bounded_context: &str) -> Vec<&'static str>;

    /// The registered snapshot's own `Snapshot::TAG_KEY`.
    fn tag_key(&self, bounded_context: &str, snapshot_name: &str) -> Option<&'static str>;

    /// The registered snapshot's own `Snapshot::OWNER_TAG_KEY` -
    /// `ProjectionDispatcher::owner_tag_key`'s identical `Option<Option<_>>`
    /// shape: outer `None` for the same "pair isn't registered at all"
    /// case every method here already has, inner `None` when the
    /// snapshot is registered but declares no owner dimension (the
    /// default, and every snapshot that predates this).
    fn owner_tag_key(
        &self,
        bounded_context: &str,
        snapshot_name: &str,
    ) -> Option<Option<&'static str>>;

    /// The registered snapshot's own `Snapshot::VERSION` - compared
    /// against a stored row's `snapshot_version` to decide whether it's
    /// still trustworthy (see `CommandType::decide_from_snapshot`'s own
    /// doc comment).
    fn version(&self, bounded_context: &str, snapshot_name: &str) -> Option<u64>;

    /// `Some(Ok(new_state))` is `state_json` with `event` folded into it
    /// via `Snapshot::fold` - `None` for the identical "pair isn't
    /// registered at all" case every dispatcher here already has.
    /// `event`'s own `BoundedContextEvent::try_from_event` failing is a
    /// real `Err`, not a silent skip - a stored event this snapshot's
    /// own `TAG_KEY` names should always convert cleanly.
    fn fold(
        &self,
        bounded_context: &str,
        snapshot_name: &str,
        state_json: &str,
        event: &Event,
    ) -> Option<crate::error::Result<String>>;

    /// `T::State::default()`, JSON-serialised - the starting point for a
    /// tag value with no stored snapshot yet, or one whose stored
    /// `snapshot_version` no longer matches. `None` for the identical
    /// "pair isn't registered at all" case.
    fn default_state(&self, bounded_context: &str, snapshot_name: &str) -> Option<String>;

    /// The registered snapshot's own `Snapshot::PARTITION_COUNT`
    /// (Codeberg issue #25, docs/architecture.md §52) -
    /// `ProjectionDispatcher::partition_count`'s identical shape and
    /// defaulting: `None` for the same "pair isn't registered at all"
    /// case every method here already has, treated by every real caller
    /// (`db::catch_up_snapshots`) identically to `Some(1)`: unpartitioned.
    fn partition_count(&self, _bounded_context: &str, _snapshot_name: &str) -> Option<u32> {
        None
    }
}

/// A single-hop, stateless reaction: when `Source` commits in its own
/// bounded context, submit `Target`'s own command into *its* bounded
/// context - the answer to "make messages cross bounded contexts
/// without needing an external system like Temporal" (docs/architecture.md's
/// own write-up of this pass) for the case that never needed Temporal's
/// own durable-timer/retry/compensation machinery in the first place:
/// both bounded contexts already live in the same Postgres database,
/// often the same process, so relaying one committed fact into another
/// bounded context's own command needs nothing more than a durable
/// cursor and the same `submit_command` path every other caller already
/// goes through.
///
/// Deliberately **not** a Saga/process manager - see this trait's own
/// design note in docs/architecture.md: no multi-step state is tracked
/// across calls, no compensation, no retry beyond `submit_command`'s own
/// idempotency-key mechanism. One commit causes at most one more commit
/// elsewhere, the same bounded, single-hop shape `submit_command`'s own
/// DCB-conflict retry already has - not an open-ended state machine.
///
/// A `Target` command that gets rejected by its own `decide()` is a
/// legitimate business outcome here too (§5.4's own "a business
/// rejection is not an error" register), not a failure to retry -
/// `route()` translating `Source`'s payload into a `Target` payload
/// `Target::decide()` goes on to reject is exactly as valid an outcome
/// as one it accepts.
pub trait CrossContextRoute {
    /// The `EventType` this route reacts to, in its own bounded context.
    type Source: EventType;
    /// The `CommandType` this route submits, in *its* bounded context -
    /// deliberately a command, not a raw event: `Target`'s own
    /// `decide()` still gets to accept, reject, or reshape the outcome,
    /// the same as every other command submission. A route that skipped
    /// straight to creating an event would bypass whatever domain rules
    /// the target bounded context wants to enforce on its own history.
    type Target: CommandType;

    /// This route's own stable identity - names its durable cursor row
    /// (`{Source::BOUNDED_CONTEXT}.cross_context_route_cursors`) and the
    /// idempotency key derived for every command it submits
    /// (`"{NAME}:{source_event_sequence}"`), so a redelivered/retried
    /// catch-up tick is exactly as safe as any other idempotency-keyed
    /// submission already is.
    const NAME: &'static str;

    /// Where this route's own durable cursor starts the first time it is
    /// ever registered - `Beginning` (the default) replays every
    /// `Source` occurrence ever committed, exactly what every route
    /// registered before this const existed already does. A route whose
    /// job is a side effect that must never re-fire for old history (the
    /// canonical case: `UserRegistered -> SendWelcomeEmail`) overrides
    /// this to `Latest`, the `CrossContextRoute`-side counterpart to
    /// `EventReadStartPosition::Latest` on the spec-backed `EventReadToken`
    /// track (docs/architecture.md's own write-up of this pass) - a
    /// separate Rust-only type rather than reusing that one, since a
    /// route has no spec entity of its own to hang a shared type off of
    /// (§36's own "no spec entity" note). See
    /// `db::catch_up_cross_context_route`'s own doc comment for exactly
    /// when this is read - only on this route's first-ever catch-up
    /// tick, never again once a cursor row exists.
    const START_FROM: CrossContextRouteStartFrom = CrossContextRouteStartFrom::Beginning;

    /// `None` skips this occurrence of `Source` entirely - no command
    /// submitted, cursor still advances (retrying can never produce a
    /// target payload for an occurrence this route itself decided
    /// doesn't apply). `Some(payload)` submits `Target` with that
    /// payload, going through the identical `decide()`/`submit_command`
    /// path every other caller of `Target` already does.
    fn route(
        source_payload: &<Self::Source as EventType>::Payload,
    ) -> Option<<Self::Target as CommandType>::Payload>;
}

/// See `CrossContextRoute::START_FROM`. Deliberately not
/// `#[derive(Default)]`: `Beginning` is the trait const's own default,
/// spelled out explicitly at every use site (`CrossContextRouteInfo::
/// start_from`) rather than leaned on implicitly, so a reader never has
/// to check whether "the default" here means this type's `Default` impl
/// or the trait const's - there is only ever one to check.
///
/// `AtSequence`/`AtTime` are this trait's own counterpart to
/// `EventReadStartPosition`'s identically-named extension
/// (docs/architecture.md's own write-up of this pass) - a route's first
/// tick seeds its cursor at a caller-chosen sequence, or at the highest
/// sequence already committed at or before a given point in time,
/// exactly like an `EventReadToken` minted the same way would. `AtTime`
/// carries a plain Unix timestamp (seconds, UTC) rather than
/// `chrono::DateTime<Utc>` for one structural reason: `START_FROM` is a
/// trait *associated const*, so every value it can ever hold has to be
/// const-evaluable at the implementing type's own definition site, and
/// `DateTime<Utc>` has no `const fn` constructor to build one from - an
/// `i64` literal is. Converted to a real `DateTime<Utc>` only where it's
/// actually compared against event timestamps
/// (`db::catch_up_cross_context_route`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossContextRouteStartFrom {
    Beginning,
    Latest,
    AtSequence(i64),
    /// Unix timestamp, seconds, UTC - see this enum's own doc comment
    /// for why not `DateTime<Utc>`.
    AtTime(i64),
}

/// One registered route's own static identity - `CrossContextRouteDispatcher::routes()`'s
/// own element type, enumerated once at startup by the background
/// catch-up loop (`db::catch_up_cross_context_route`).
#[derive(Debug, Clone, Copy)]
pub struct CrossContextRouteInfo {
    pub name: &'static str,
    pub source_bounded_context: &'static str,
    pub source_event_type: &'static str,
    pub target_bounded_context: &'static str,
    pub target_command_type: &'static str,
    /// `CrossContextRoute::START_FROM`, carried through so
    /// `db::catch_up_cross_context_route` can act on it without needing
    /// the route's own concrete type.
    pub start_from: CrossContextRouteStartFrom,
}

/// Type-erased dispatch to a bounded context's own typed
/// `CrossContextRoute::route` - the same "outer `None` = not registered
/// at all" convention every other dispatcher trait in this module
/// already uses, one level deeper here since `route()` itself can also
/// legitimately produce nothing for a given occurrence.
pub trait CrossContextRouteDispatcher: Send + Sync {
    /// Every registered route, across every bounded context - routes
    /// aren't scoped to one bounded context the way `EventType`/
    /// `CommandType`/`Projection` are (a route by definition spans two),
    /// so unlike `ProjectionDispatcher::keys` and friends there is no
    /// `bounded_context` parameter to filter by here; the background
    /// catch-up loop calls this once and iterates the full list itself.
    fn routes(&self) -> Vec<CrossContextRouteInfo>;

    /// `route_name` names one of `routes()`'s own entries;
    /// `source_payload_json` is the triggering `Source` event's own
    /// stored payload. Outer `None` - `route_name` isn't registered at
    /// all (defensive; the caller only ever names one of its own
    /// `routes()`). `Some(Err(e))` - the stored payload didn't
    /// deserialize into `Source::Payload` (see `BoundedContextEvent::
    /// try_from_event`'s own doc comment on why this is still reachable
    /// even though payloads are schema-checked at write time). `Some(Ok(None))` -
    /// `route()` itself decided this occurrence doesn't apply. `Some(Ok(Some(payload)))` -
    /// the `Target` command's own JSON payload, ready to submit.
    fn route(
        &self,
        route_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<String>, serde_json::Error>>;
}

/// Codeberg issue #20's own "where does this cursor start" const, one per
/// `ScheduleDeadline`/`CancelDeadline` implementor - identical in shape to
/// `CrossContextRouteStartFrom` but its own type rather than a reuse of
/// it: a deadline reactor isn't a route, and giving it a name that says
/// what it's actually for beats leaning on a type whose own doc comment
/// is written entirely in terms of routes. See `CrossContextRouteStartFrom`'s
/// own doc comment for why `AtTime` carries a plain Unix timestamp rather
/// than `DateTime<Utc>` - the identical reasoning (a trait associated
/// const has to be const-evaluable) applies here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlinePollStartFrom {
    Beginning,
    Latest,
    AtSequence(i64),
    /// Unix timestamp, seconds, UTC.
    AtTime(i64),
}

/// `ScheduleDeadline::schedule`'s own `Some` output - everything a
/// one-shot, per-entity timer needs: when it fires (`fire_at`), what it's
/// scoped to for a later `CancelDeadline` lookup (`tags`), and what to
/// submit when it does (`payload`, `Self::Target`'s own payload type).
#[derive(Debug, Clone)]
pub struct DeadlineSpec<P> {
    pub fire_at: chrono::DateTime<chrono::Utc>,
    pub tags: Vec<Tag>,
    pub payload: P,
}

/// Codeberg issue #20: a native one-shot, per-entity deadline/timer -
/// "cancel this order if not paid within 30 minutes," without reaching
/// for `skilj-temporal` (docs/architecture.md §34) or hand-building it on
/// top of the recurring, `EventType`-wide scheduler (`rule
/// CreateSystemEvent`). Deliberately the same shape as `CrossContextRoute`
/// just above, for the same reason: `decide()`'s only output is
/// `CommandDecision` (`Accepted { events }` / `Rejected`) - it has no
/// side-effect channel to schedule a timer through, and none should be
/// added there. So scheduling one is itself modelled as a pure reaction
/// to a committed event, with a background poller doing the actual
/// durable write, exactly like a route reacting to one.
///
/// Fires `Target` as a real command, not a raw event - the same reasoning
/// `CrossContextRoute::Target` already gives: whether a deadline is still
/// relevant is a decision `Target::decide()` gets to make against
/// *current* state at fire time, not one baked in back when the timer was
/// scheduled.
pub trait ScheduleDeadline {
    /// The event that, on commit, may schedule a new deadline.
    type Source: EventType;
    /// The command submitted when the deadline fires.
    type Target: CommandType;

    /// This schedule's own stable identity - names its durable cursor row
    /// and, combined with the triggering `Source` event's own sequence,
    /// the deterministic id of every row it inserts
    /// (`"{NAME}:{source_event_sequence}"`) - so a redelivered/retried
    /// catch-up tick re-inserts the identical row rather than a duplicate
    /// (`ON CONFLICT (id) DO NOTHING`), the same property a route's own
    /// idempotency key gives its command submissions, applied here to the
    /// row insert itself.
    const NAME: &'static str;

    /// See `DeadlinePollStartFrom`. `Beginning` (the default) replays
    /// every `Source` occurrence ever committed - the same default
    /// `CrossContextRoute::START_FROM` has, and for the same reason: it's
    /// what every schedule registered before this const existed already
    /// does.
    const START_FROM: DeadlinePollStartFrom = DeadlinePollStartFrom::Beginning;

    /// `None` skips this occurrence of `Source` entirely - no deadline
    /// scheduled, cursor still advances. `Some(spec)` inserts a `pending`
    /// row, fired the next time `spec.fire_at` comes due (or immediately,
    /// if it's already in the past by the time this runs - a deadline is
    /// a one-shot, so there's no missed-occurrence policy to choose
    /// between the way the recurring scheduler needs one).
    fn schedule(
        source_payload: &<Self::Source as EventType>::Payload,
    ) -> Option<DeadlineSpec<<Self::Target as CommandType>::Payload>>;
}

/// Codeberg issue #20's own cancellation half - "cancellable if the
/// awaited thing happens first." Bound to a specific `ScheduleDeadline`
/// via `Deadline` (not just any deadline sharing a tag) so two unrelated
/// schedules that happen to reuse a tag key never cross-cancel each
/// other's rows.
///
/// Cancels **by tag**, not by id: the cancelling event (say, `OrderPaid`)
/// only ever carries its own payload/tags, never the opaque id a
/// `ScheduleDeadline` implementor generated deep inside its own row
/// insert - tag-based lookup is what actually lets "whichever happens
/// first" work without threading an id back out through some side
/// channel. Reuses the same tag-containment query
/// `list_events_for_bounded_context_matching_tags` already uses for
/// events (docs/architecture.md §19 Problem 1), applied to the
/// `deadlines` table's own `tags` column.
///
/// **Adding one to an existing schedule in a rolling deploy.** A due
/// deadline waits until every cancel targeting its schedule has processed
/// its source events up to the deadline's `fire_at` - but an instance only
/// knows the cancels declared in its own code. While instances without
/// the new cancel are still running, one of them can fire a deadline whose
/// cancelling event had already committed but not yet been processed.
/// Where that matters, deploy the new cancel while no such deadline can
/// come due, or roll out completely before relying on it
/// (docs/architecture.md §164).
pub trait CancelDeadline {
    /// The event that, on commit, may cancel one or more pending
    /// deadlines.
    type Source: EventType;
    /// Which `ScheduleDeadline`'s own pending rows this targets.
    type Deadline: ScheduleDeadline;

    /// This cancel reactor's own stable identity - names its durable
    /// cursor row, independent of `Self::Deadline::NAME`'s own cursor (a
    /// schedule and its cancel counterpart advance independently, each
    /// walking its own `Source` event stream).
    const NAME: &'static str;

    /// See `DeadlinePollStartFrom`; same default and reasoning as
    /// `ScheduleDeadline::START_FROM`.
    const START_FROM: DeadlinePollStartFrom = DeadlinePollStartFrom::Beginning;

    /// `None` skips this occurrence entirely - nothing cancelled, cursor
    /// still advances. `Some(tags)` cancels every still-`pending` row
    /// `Self::Deadline::NAME` owns whose own `tags` contain these -
    /// cancelling zero, one, or several rows is all a legitimate outcome
    /// here (a deadline that already fired, was already cancelled, or was
    /// never scheduled in the first place is not an error), the same
    /// register `CrossContextRoute::route` returning `None` already is.
    fn cancel_tags(source_payload: &<Self::Source as EventType>::Payload) -> Option<Vec<Tag>>;
}

/// One registered `ScheduleDeadline`'s own static identity -
/// `ScheduleDeadlineDispatcher::schedules()`'s own element type, the
/// `ScheduleDeadline`/`CancelDeadline` pair's counterpart to
/// `CrossContextRouteInfo`.
#[derive(Debug, Clone, Copy)]
pub struct ScheduleDeadlineInfo {
    pub name: &'static str,
    pub source_bounded_context: &'static str,
    pub source_event_type: &'static str,
    pub target_bounded_context: &'static str,
    pub target_command_type: &'static str,
    pub start_from: DeadlinePollStartFrom,
}

/// One registered `CancelDeadline`'s own static identity.
#[derive(Debug, Clone, Copy)]
pub struct CancelDeadlineInfo {
    pub name: &'static str,
    pub source_bounded_context: &'static str,
    pub source_event_type: &'static str,
    pub deadline_schedule_name: &'static str,
    /// `Self::Deadline::Source::BOUNDED_CONTEXT` - where the paired
    /// `ScheduleDeadline`'s own `deadlines`/`deadline_cursors` rows
    /// physically live (mirroring `CrossContextRoute`'s own
    /// `source_bounded_context` placement choice for its cursor table),
    /// which can differ from this cancel reactor's own
    /// `source_bounded_context` above - the cancelling event and the
    /// deadline it cancels aren't required to live in the same bounded
    /// context, the same generality `CrossContextRoute::Source`/`Target`
    /// already has.
    pub deadline_schedule_bounded_context: &'static str,
    /// `Self::Deadline::Source::NAME` - the event type the paired
    /// `ScheduleDeadline` schedules from. A cancel waits until that
    /// schedule has processed every such event that precedes the
    /// cancelling one (docs/architecture.md §130).
    pub deadline_schedule_source_event_type: &'static str,
    pub start_from: DeadlinePollStartFrom,
}

/// Type-erased dispatch to a bounded context's own typed
/// `ScheduleDeadline::schedule` - `CrossContextRouteDispatcher`'s own
/// counterpart for this trait.
pub trait ScheduleDeadlineDispatcher: Send + Sync {
    /// Every registered schedule, across every bounded context - not
    /// scoped to one bounded context, the same reasoning
    /// `CrossContextRouteDispatcher::routes` already gives.
    fn schedules(&self) -> Vec<ScheduleDeadlineInfo>;

    /// `schedule_name` names one of `schedules()`'s own entries;
    /// `source_payload_json` is the triggering `Source` event's own
    /// stored payload. Outer `None` - not registered at all. `Some(Err(e))` -
    /// the stored payload didn't deserialize into `Source::Payload`.
    /// `Some(Ok(None))` - `schedule()` itself decided this occurrence
    /// doesn't apply. `Some(Ok(Some(spec)))` - ready to insert as a
    /// `pending` row, `Target`'s own JSON payload already resolved.
    fn schedule(
        &self,
        schedule_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<ErasedDeadlineSpec>, serde_json::Error>>;
}

/// `ScheduleDeadlineDispatcher::schedule`'s own type-erased output -
/// `DeadlineSpec<P>` with `P` resolved to its already-serialised JSON,
/// the same "erase the payload's own concrete type at the dispatcher
/// boundary" treatment every other dispatcher trait in this module
/// already gives its own typed payloads.
#[derive(Debug, Clone)]
pub struct ErasedDeadlineSpec {
    pub fire_at: chrono::DateTime<chrono::Utc>,
    pub tags: Vec<Tag>,
    pub payload_json: String,
}

/// Type-erased dispatch to a bounded context's own typed
/// `CancelDeadline::cancel_tags`.
pub trait CancelDeadlineDispatcher: Send + Sync {
    /// Every registered cancel reactor, across every bounded context.
    fn cancels(&self) -> Vec<CancelDeadlineInfo>;

    /// `cancel_name` names one of `cancels()`'s own entries;
    /// `source_payload_json` is the triggering `Source` event's own
    /// stored payload. Outer `None` - not registered at all. `Some(Err(e))` -
    /// the stored payload didn't deserialize into `Source::Payload`.
    /// `Some(Ok(None))` - `cancel_tags()` itself decided this occurrence
    /// doesn't apply. `Some(Ok(Some(tags)))` - cancel every still-`pending`
    /// row the paired `ScheduleDeadline` owns whose own tags contain
    /// these.
    fn cancel_tags(
        &self,
        cancel_name: &str,
        source_payload_json: &str,
    ) -> Option<Result<Option<Vec<Tag>>, serde_json::Error>>;
}
