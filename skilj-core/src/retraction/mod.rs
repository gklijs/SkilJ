//! Retractions and corrections (docs/architecture.md §235, issue #133).
//!
//! Events and their tags are immutable, so a wrong event stays in the
//! journal. What can change is whether it is still endorsed: a command
//! appends an [`EventRetracted`] naming the wrong event's position, and the
//! store copies that event's tags onto it. A retraction therefore can't miss
//! a tag: every query that found the original finds the retraction too, a
//! blind tag included, without the plaintext being needed.
//!
//! A correction is a retraction and the event that should have been recorded,
//! in one decision: see [`corrected`]. Reads fold around retractions with
//! [`Retractable`] and [`standing`], and a uniqueness rule with
//! [`ValueClaims`].
//!
//! A snapshot's state is kept to its standing events by the store: a snapshot
//! never folds a retracted event or a retraction. A projection isn't: it lists
//! [`RETRACTED_EVENT_TYPE`] among its consumed event types and takes the
//! contribution out itself.
//!
//! A retraction doesn't erase anything. The original stays in the journal and
//! readable, which is what `forgetSubject` is for instead, and a reason is
//! stored in the clear, so it shouldn't hold personal data.

use crate::event_store::Event;
use crate::plugin::{BoundedContextEvent, EventType};
use crate::shared::{CommandDecision, EventSpec, QueryItemMapping, Tag, TagMapping};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// The name [`EventRetracted`] registers under. Reserved: a type of this name
/// with any other shape is refused at registration.
pub const RETRACTED_EVENT_TYPE: &str = "EventRetracted";

/// The tag the store adds to a retraction, its value the retracted position.
/// Finds what retracted an event, and is how the store refuses a second one.
pub const RETRACTS_TAG_KEY: &str = "skilj_retracts";

/// Why a claim is no longer endorsed. Erasure is not among them: a request to
/// be forgotten or an expired retention period is `forgetSubject`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RetractionGround {
    /// The data submitted was wrong.
    WrongDataSubmitted,
    /// The data was right, and was processed wrongly.
    ProcessedWrongly,
    /// A new legal or policy interpretation no longer supports it.
    Reinterpretation,
}

/// The payload of an [`EventRetracted`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RetractionPayload {
    /// The sequence of the event no longer endorsed.
    pub position: i64,
    /// Why it isn't.
    pub ground: RetractionGround,
    /// A human-readable explanation, stored in the clear.
    pub reason: String,
    /// The type of the retracted event. Filled in by the store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retracted_event_type: Option<String>,
    /// The retracted event's owner tag value, when the type is registered
    /// with `retractions_owned_by`. Filled in by the store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

/// The built-in retraction event. Only a command's decision can emit one, with
/// [`retract`] or [`corrected`]; registered per bounded context by
/// `SkiljBuilder::retractions`.
pub struct EventRetracted;

impl EventType for EventRetracted {
    type Payload = RetractionPayload;
    const NAME: &'static str = RETRACTED_EVENT_TYPE;

    /// A reader who can follow an event has to be able to learn it was
    /// retracted.
    fn event_read_allowed() -> bool {
        true
    }
}

/// The schema every [`EventRetracted`] registers with.
pub fn retraction_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(RetractionPayload))
        .expect("JSON Schema serialization is infallible")
}

/// The position `event` retracts, when it is a retraction.
pub fn retracted_position(event: &Event) -> Option<i64> {
    if event.event_type.name != RETRACTED_EVENT_TYPE {
        return None;
    }
    serde_json::from_str::<RetractionPayload>(&event.payload)
        .ok()
        .map(|retraction| retraction.position)
}

/// The event retracting `position`, for a decision to emit.
pub fn retract(position: i64, ground: RetractionGround, reason: impl Into<String>) -> EventSpec {
    EventSpec {
        event_type: RETRACTED_EVENT_TYPE.to_string(),
        payload: serde_json::json!({
            "position": position,
            "ground": ground,
            "reason": reason.into(),
        }),
    }
}

/// A correction: `position` retracted and `replacement` recorded, atomically.
/// The retraction comes first, so a replacement with the same tag value as the
/// original leaves that value claimed. Both are caused by the same command.
pub fn corrected(
    position: i64,
    ground: RetractionGround,
    reason: impl Into<String>,
    replacement: impl IntoIterator<Item = EventSpec>,
) -> CommandDecision {
    let mut events = vec![retract(position, ground, reason)];
    events.extend(replacement);
    CommandDecision::Accepted { events }
}

/// The consistency query of a command correcting a unique value: what the
/// identity tag has done, and what has happened to the new value. A concurrent
/// correction of the same identity conflicts on the first item, a concurrent
/// claim of the new value on the second. Both mappings must be among the
/// command type's `tag_mappings()`; `build()` checks it. Every match is read,
/// as [`standing`] needs: a late retraction of an old position would otherwise
/// be the latest event of a value someone holds.
pub fn correction_query(
    event_types: &[&str],
    identity: TagMapping,
    value: TagMapping,
) -> Vec<QueryItemMapping> {
    let types: Vec<&str> = event_types
        .iter()
        .copied()
        .chain([RETRACTED_EVENT_TYPE])
        .collect();
    vec![
        QueryItemMapping::types(&types).tagged(vec![identity]),
        QueryItemMapping::types(&types).tagged(vec![value]),
    ]
}

/// [`correction_query`]'s one-item form for a command claiming a new value: its
/// claims, releases and retractions, every match.
pub fn claim_query(event_types: &[&str], value: TagMapping) -> Vec<QueryItemMapping> {
    let types: Vec<&str> = event_types
        .iter()
        .copied()
        .chain([RETRACTED_EVENT_TYPE])
        .collect();
    vec![QueryItemMapping::types(&types).tagged(vec![value])]
}

/// An event as read by a decision or a projection, with what it takes to fold
/// around retractions: use as `type Event = Retractable<MyEvent>`. `E` decides
/// which events it understands, exactly as it does on its own.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Retractable<E> {
    /// An event `E` understands.
    Event {
        /// Its sequence, what a retraction names.
        position: i64,
        /// Its tags.
        tags: Vec<Tag>,
        /// The event.
        event: E,
    },
    /// A retraction.
    Retraction {
        /// The retraction's own sequence.
        position: i64,
        /// Its tags, copied from the retracted event.
        tags: Vec<Tag>,
        /// Its payload, naming the position it retracts.
        retraction: RetractionPayload,
    },
}

impl<E> Retractable<E> {
    /// An event, for a test of a `decide()`, which never sees the store.
    pub fn event(position: i64, tags: Vec<Tag>, event: E) -> Self {
        Self::Event {
            position,
            tags,
            event,
        }
    }

    /// A retraction of `retracted`, for a test of a `decide()`.
    pub fn retraction(position: i64, tags: Vec<Tag>, retracted: i64) -> Self {
        Self::Retraction {
            position,
            tags,
            retraction: RetractionPayload {
                position: retracted,
                ground: RetractionGround::WrongDataSubmitted,
                reason: String::new(),
                retracted_event_type: None,
                owner: None,
            },
        }
    }
}

impl<E: BoundedContextEvent> BoundedContextEvent for Retractable<E> {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        if event.event_type.name == RETRACTED_EVENT_TYPE {
            return Some(
                serde_json::from_str(&event.payload).map(|retraction| Self::Retraction {
                    position: event.sequence,
                    tags: event.tags.clone(),
                    retraction,
                }),
            );
        }
        E::try_from_event(event).map(|converted| {
            converted.map(|inner| Self::Event {
                position: event.sequence,
                tags: event.tags.clone(),
                event: inner,
            })
        })
    }
}

/// An event no retraction in the read names.
#[derive(Debug)]
pub struct Standing<'a, E> {
    /// Its sequence.
    pub position: i64,
    /// Its tags.
    pub tags: &'a [Tag],
    /// The event.
    pub event: &'a E,
}

// Not derived: `E` itself needn't be `Clone` or `Copy`, only the reference is.
impl<E> Clone for Standing<'_, E> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<E> Copy for Standing<'_, E> {}

impl<E> Standing<'_, E> {
    /// Whether it carries this plain tag.
    pub fn is_tagged(&self, key: &str, value: &str) -> bool {
        self.tags
            .iter()
            .any(|tag| tag.key == key && tag.value.as_deref() == Some(value))
    }
}

/// The events of `events` still endorsed, in order. Needs every retraction of
/// them in the read, so no `.latest()` on a query that folds with this.
pub fn standing<E>(events: &[Retractable<E>]) -> Vec<Standing<'_, E>> {
    let retracted: HashSet<i64> = events
        .iter()
        .filter_map(|event| match event {
            Retractable::Retraction { retraction, .. } => Some(retraction.position),
            Retractable::Event { .. } => None,
        })
        .collect();
    events
        .iter()
        .filter_map(|event| match event {
            Retractable::Event {
                position,
                tags,
                event,
            } if !retracted.contains(position) => Some(Standing {
                position: *position,
                tags,
                event,
            }),
            _ => None,
        })
        .collect()
}

/// What an event does to a unique value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ValueEffect {
    /// The value is taken.
    Claims,
    /// The value is free.
    Releases,
    /// Nothing.
    Neither,
}

/// Who holds which value, from a read of claims, releases and retractions: per
/// value of the tag, the last standing event that claims or releases it. Works
/// on tags alone, so it needs no plaintext and a blind value is no different.
#[derive(Debug)]
pub struct ValueClaims<'a, E> {
    held: Vec<Standing<'a, E>>,
}

impl<'a, E> ValueClaims<'a, E> {
    /// Folds `events`, whose values are the `value_key` tag's.
    pub fn fold(
        events: &'a [Retractable<E>],
        value_key: &str,
        effect: impl Fn(&E) -> ValueEffect,
    ) -> Self {
        let mut latest: Vec<(&str, Standing<'a, E>, ValueEffect)> = Vec::new();
        for event in standing(events) {
            let event_effect = effect(event.event);
            if event_effect == ValueEffect::Neither {
                continue;
            }
            for tag in event.tags.iter().filter(|tag| tag.key == value_key) {
                let Some(value) = tag.value.as_deref() else {
                    continue;
                };
                match latest.iter_mut().find(|(known, _, _)| *known == value) {
                    Some(entry) => {
                        entry.1 = event;
                        entry.2 = event_effect;
                    }
                    None => latest.push((value, event, event_effect)),
                }
            }
        }
        let held = latest
            .into_iter()
            .filter(|(_, _, effect)| *effect == ValueEffect::Claims)
            .map(|(_, event, _)| event)
            .collect();
        Self { held }
    }

    /// The standing claim of every value held.
    pub fn held(&self) -> &[Standing<'a, E>] {
        &self.held
    }

    /// The claim `identity_value` holds, the one a correction retracts.
    pub fn held_by(&self, identity_key: &str, identity_value: &str) -> Option<Standing<'a, E>> {
        self.held
            .iter()
            .rev()
            .find(|claim| claim.is_tagged(identity_key, identity_value))
            .copied()
    }

    /// A claim held by anyone but `identity_value`. In a [`correction_query`]
    /// read, that can only be the new value: another identity's events are read
    /// only through it.
    pub fn held_by_another(
        &self,
        identity_key: &str,
        identity_value: &str,
    ) -> Option<Standing<'a, E>> {
        self.held
            .iter()
            .find(|claim| !claim.is_tagged(identity_key, identity_value))
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    enum Value {
        Claimed,
        Released,
    }

    fn tags(customer: &str, email: &str) -> Vec<Tag> {
        [("customer", customer), ("email", email)]
            .map(|(key, value)| Tag {
                key: key.to_string(),
                value: Some(value.to_string()),
            })
            .to_vec()
    }

    fn claim(position: i64, customer: &str, email: &str) -> Retractable<Value> {
        Retractable::event(position, tags(customer, email), Value::Claimed)
    }

    fn release(position: i64, customer: &str, email: &str) -> Retractable<Value> {
        Retractable::event(position, tags(customer, email), Value::Released)
    }

    fn fold(events: &[Retractable<Value>]) -> ValueClaims<'_, Value> {
        ValueClaims::fold(events, "email", |event| match event {
            Value::Claimed => ValueEffect::Claims,
            Value::Released => ValueEffect::Releases,
        })
    }

    #[test]
    fn a_retracted_event_no_longer_stands() {
        let events = [
            claim(1, "a", "x"),
            claim(2, "a", "y"),
            Retractable::retraction(3, tags("a", "x"), 1),
        ];
        let standing = standing(&events);
        assert_eq!(standing.iter().map(|e| e.position).collect::<Vec<_>>(), [2]);
    }

    #[test]
    fn a_retraction_frees_the_value_it_claimed() {
        let events = [
            claim(1, "a", "x"),
            Retractable::retraction(2, tags("a", "x"), 1),
        ];
        assert!(fold(&events).held().is_empty());
    }

    #[test]
    fn a_late_retraction_of_an_old_claim_leaves_the_current_holder() {
        // Being the latest event of the value is what `.latest()` would go by.
        let events = [
            claim(1, "a", "x"),
            release(2, "a", "x"),
            claim(3, "b", "x"),
            Retractable::retraction(4, tags("a", "x"), 1),
        ];
        let claims = fold(&events);
        assert_eq!(claims.held_by("customer", "b").unwrap().position, 3);
        assert!(claims.held_by("customer", "a").is_none());
    }

    #[test]
    fn retracting_a_release_restores_the_claim() {
        let events = [
            claim(1, "a", "x"),
            release(2, "a", "x"),
            Retractable::retraction(3, tags("a", "x"), 2),
        ];
        assert_eq!(fold(&events).held_by("customer", "a").unwrap().position, 1);
    }

    #[test]
    fn a_correction_sees_its_own_claim_and_another_holder_apart() {
        let events = [claim(1, "a", "old"), claim(2, "b", "new")];
        let claims = fold(&events);
        assert_eq!(claims.held_by("customer", "a").unwrap().position, 1);
        assert_eq!(claims.held_by_another("customer", "a").unwrap().position, 2);
    }

    #[test]
    fn a_correction_lists_the_retraction_first() {
        let replacement = EventSpec {
            event_type: "Claimed".to_string(),
            payload: serde_json::json!({}),
        };
        let CommandDecision::Accepted { events } = corrected(
            7,
            RetractionGround::WrongDataSubmitted,
            "typo",
            [replacement],
        ) else {
            panic!("a correction is accepted");
        };
        assert_eq!(events[0].event_type, RETRACTED_EVENT_TYPE);
        assert_eq!(events[0].payload["position"], 7);
        assert_eq!(events[0].payload["ground"], "wrong_data_submitted");
        assert_eq!(events[1].event_type, "Claimed");
    }

    #[test]
    fn the_query_reads_retractions_and_every_match() {
        let query = correction_query(
            &["Claimed"],
            TagMapping::new("customer", "customer_id"),
            TagMapping::new("email", "email"),
        );
        assert_eq!(query.len(), 2);
        for item in &query {
            assert!(item.event_types.contains(&RETRACTED_EVENT_TYPE.to_string()));
            assert_eq!(item.latest, None);
        }
    }
}
