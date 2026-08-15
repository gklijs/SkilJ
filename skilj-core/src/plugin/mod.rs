//! The public plugin API - the one module every consuming application's
//! own code touches directly. See docs/architecture.md §1 for the full
//! reasoning behind this shape.

use crate::shared::{CommandDecision, SensitiveField, TagMapping};
use schemars::JsonSchema;
use serde::{de::DeserializeOwned, Serialize};

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
    /// event type (§1.4).
    type Event;

    const NAME: &'static str;

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

    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision;
}

/// One event type a bounded context registers. Unlike `CommandType`/
/// `Projection`, there's no behaviour to plug in here - an `EventType` is
/// pure declaration (its schema, its tag/sensitivity mappings, which
/// origins may create it).
pub trait EventType {
    type Payload: Serialize + DeserializeOwned + JsonSchema;

    const NAME: &'static str;

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
}

/// One projection a bounded context registers.
///
/// `project()` folds exactly one event into this projection's stored
/// state - synchronous, like `decide()`, and for the same reason (§1.1).
pub trait Projection {
    type State: Serialize + DeserializeOwned + JsonSchema + Default;

    /// Same generated per-bounded-context event enum `CommandType::Event`
    /// uses - a projection folds any event type it consumes, not just one.
    type Event;

    const NAME: &'static str;

    /// Whether this projection updates inline, in the same transaction as
    /// the events it consumes (`true`), or via a background consumer
    /// (`false`, the default) - see `Projection.sync` in the spec.
    fn sync() -> bool {
        false
    }

    fn project(state: &mut Self::State, event: &Self::Event);
}
