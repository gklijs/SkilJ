//! Tests for `rule CreateSystemEvent`/`rule SkipMissedOccurrences` and the
//! `next_occurrence_after` black box - the real scheduler mechanism
//! implementing the drift audit's #7 finding (see project memory
//! `skilj-drift-audit-2026-08-18`), and the missed-occurrence policy the
//! user resolved directly (no default, ever - see that memory's own
//! writeup). Pure functions only, real Postgres not needed - the actual
//! scheduler orchestration (the background task walking due occurrences,
//! locking the EventType row for multi-instance safety) is `db`/`skilj`
//! layer, tested separately.

use chrono::{TimeZone, Utc};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, EventType, MissedOccurrencePolicy,
};
use skilj_core::shared::Tag;

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn bounded_context(status: BoundedContextStatus) -> BoundedContext {
    BoundedContext {
        name: "banking".into(),
        status,
        created_at: timestamp(0),
        created_by: ContextCreator::SystemCreator,
        template: None,
    }
}

/// Fires every second on the clock - `"0/1 * * * * * *"` isn't valid in
/// this crate's own dialect (seconds are a plain field, not a step
/// range starting at a literal), so `"* * * * * * *"` (every second) is
/// used instead, giving tests a schedule dense enough that "the next
/// occurrence" is always just one second away, easy to reason about by
/// hand.
fn every_second() -> String {
    "* * * * * * *".to_string()
}

fn scheduled_event_type(
    policy: MissedOccurrencePolicy,
    schedule_position: chrono::DateTime<Utc>,
    last_fired_at: Option<chrono::DateTime<Utc>>,
) -> EventType {
    EventType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: "HeartBeat".into(),
        schema: r#"{"properties":{}}"#.into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: false,
        direct_creation_allowed: false,
        system_triggered_allowed: true,
        system_triggered_schedule: Some(every_second()),
        missed_occurrence_policy: Some(policy),
        schedule_position: Some(schedule_position),
        last_fired_at,
        event_read_allowed: false,
    }
}

fn no_key(
    _subject_key: &str,
    _subject_value: &str,
) -> (event_store::EncryptionKey, skilj_core::encryption::DataKey) {
    unreachable!("this event type has no sensitive_fields, so resolve_key is never called")
}

// ---------------------------------------------------------------------
// next_occurrence_after
// ---------------------------------------------------------------------

#[test]
fn next_occurrence_after_is_strictly_after_the_given_instant() {
    let next = event_store::next_occurrence_after(&every_second(), timestamp(1000)).unwrap();
    assert_eq!(next, timestamp(1001));
    // Never the instant itself, even though it's on-schedule.
    assert_ne!(next, timestamp(1000));
}

#[test]
fn next_occurrence_after_is_none_for_an_unparseable_schedule() {
    assert_eq!(
        event_store::next_occurrence_after("not a cron expression", timestamp(1000)),
        None
    );
}

// ---------------------------------------------------------------------
// latest_occurrence_at_or_before (docs/architecture.md §150)
// ---------------------------------------------------------------------

#[test]
fn latest_occurrence_at_or_before_includes_an_instant_on_the_schedule() {
    assert_eq!(
        event_store::latest_occurrence_at_or_before(&every_second(), timestamp(1000)),
        Some(timestamp(1000))
    );
}

#[test]
fn latest_occurrence_at_or_before_rounds_a_sub_second_instant_down() {
    let instant = timestamp(1000) + chrono::Duration::milliseconds(500);
    assert_eq!(
        event_store::latest_occurrence_at_or_before(&every_second(), instant),
        Some(timestamp(1000))
    );
}

#[test]
fn latest_occurrence_at_or_before_finds_the_last_of_a_sparse_schedule() {
    // Top of every hour; 5h02m after the epoch - the 5h occurrence.
    assert_eq!(
        event_store::latest_occurrence_at_or_before("0 0 * * * * *", timestamp(5 * 3600 + 120)),
        Some(timestamp(5 * 3600))
    );
    // Exactly on one.
    assert_eq!(
        event_store::latest_occurrence_at_or_before("0 0 * * * * *", timestamp(5 * 3600)),
        Some(timestamp(5 * 3600))
    );
}

#[test]
fn latest_occurrence_at_or_before_is_none_without_an_earlier_occurrence() {
    // Only ever fires in 2099.
    assert_eq!(
        event_store::latest_occurrence_at_or_before("0 0 0 1 1 * 2099", timestamp(1000)),
        None
    );
    assert_eq!(
        event_store::latest_occurrence_at_or_before("not a cron expression", timestamp(1000)),
        None
    );
}

// ---------------------------------------------------------------------
// create_system_event
// ---------------------------------------------------------------------

#[test]
fn create_system_event_fires_a_due_occurrence_under_replay_backlog() {
    let et = scheduled_event_type(MissedOccurrencePolicy::ReplayBacklog, timestamp(1000), None);
    let mut payload_calls = 0;
    let result = event_store::create_system_event(
        &et,
        timestamp(1001),
        timestamp(1001),
        7,
        || {
            payload_calls += 1;
            "{}".to_string()
        },
        no_key,
    );
    let (event, new_position) = result.expect("a due occurrence must fire");
    assert_eq!(new_position, timestamp(1001));
    assert_eq!(event.sequence, 7);
    assert_eq!(event.metadata.client_id, "system");
    assert_eq!(event.metadata.created_at, timestamp(1001));
    assert!(matches!(
        event.origin,
        event_store::EventOrigin::SystemTriggered
    ));
    assert_eq!(payload_calls, 1);
}

#[test]
fn create_system_event_never_calls_resolve_payload_for_an_ineligible_occurrence() {
    // Not due yet - occurrence_at is in the future relative to now.
    let et = scheduled_event_type(MissedOccurrencePolicy::ReplayBacklog, timestamp(1000), None);
    let mut called = false;
    let result = event_store::create_system_event(
        &et,
        timestamp(2000),
        timestamp(1001), // now - well before the occurrence
        1,
        || {
            called = true;
            "{}".to_string()
        },
        no_key,
    );
    assert!(result.is_none());
    assert!(
        !called,
        "resolve_payload must not run for an occurrence that won't fire"
    );
}

#[test]
fn create_system_event_rejects_an_occurrence_already_accounted_for() {
    let et = scheduled_event_type(MissedOccurrencePolicy::ReplayBacklog, timestamp(1000), None);
    // occurrence_at at or before schedule_position - already settled.
    let result = event_store::create_system_event(
        &et,
        timestamp(1000),
        timestamp(2000),
        1,
        || "{}".to_string(),
        no_key,
    );
    assert!(result.is_none());
}

#[test]
fn create_system_event_rejects_an_inactive_bounded_context() {
    let et = EventType {
        bounded_context: bounded_context(BoundedContextStatus::Archived),
        ..scheduled_event_type(MissedOccurrencePolicy::ReplayBacklog, timestamp(1000), None)
    };
    let result = event_store::create_system_event(
        &et,
        timestamp(1001),
        timestamp(1001),
        1,
        || "{}".to_string(),
        no_key,
    );
    assert!(result.is_none());
}

/// Three occurrences overdue (1001, 1002, 1003), evaluated one at a time
/// in ascending order, threading the newly-advanced position through
/// each call - the same thing the real scheduler loop does. Under
/// replay_backlog every one of them fires.
#[test]
fn create_system_event_replay_backlog_fires_every_occurrence_in_a_backlog() {
    let mut position = timestamp(1000);
    let mut fired = Vec::new();
    for occurrence in [timestamp(1001), timestamp(1002), timestamp(1003)] {
        let et = scheduled_event_type(MissedOccurrencePolicy::ReplayBacklog, position, None);
        let (event, new_position) = event_store::create_system_event(
            &et,
            occurrence,
            timestamp(1003),
            fired.len() as i64,
            || "{}".to_string(),
            no_key,
        )
        .unwrap_or_else(|| panic!("occurrence {occurrence:?} must fire under replay_backlog"));
        fired.push(event.sequence);
        position = new_position;
    }
    assert_eq!(fired, vec![0, 1, 2]);
    assert_eq!(position, timestamp(1003));
}

/// Same backlog, under fire_once: only the last occurrence in the
/// backlog fires - the intermediate ones are rejected (something later
/// is due), and firing the last one advances the position past all of
/// them at once.
#[test]
fn create_system_event_fire_once_collapses_a_backlog_to_its_tail() {
    let position = timestamp(1000);
    let now = timestamp(1003);
    let mut fired = Vec::new();
    for occurrence in [timestamp(1001), timestamp(1002), timestamp(1003)] {
        let et = scheduled_event_type(MissedOccurrencePolicy::FireOnce, position, None);
        let result =
            event_store::create_system_event(&et, occurrence, now, 0, || "{}".to_string(), no_key);
        if let Some((event, new_position)) = result {
            fired.push((event.sequence, new_position));
        }
    }
    assert_eq!(fired, vec![(0, timestamp(1003))]);
}

/// Same backlog, under skip: nothing fires via create_system_event at
/// all (the backlog is only ever closed by `skip_missed_occurrences`
/// below) - every occurrence is rejected since `no_gap` is false for
/// anything but the very next unaccounted-for one, and that one is
/// itself rejected too since something later is already due.
#[test]
fn create_system_event_skip_fires_nothing_for_a_backlog() {
    let position = timestamp(1000);
    let now = timestamp(1003);
    for occurrence in [timestamp(1001), timestamp(1002), timestamp(1003)] {
        let et = scheduled_event_type(MissedOccurrencePolicy::Skip, position, None);
        let result =
            event_store::create_system_event(&et, occurrence, now, 0, || "{}".to_string(), no_key);
        assert!(
            result.is_none(),
            "occurrence {occurrence:?} must not fire under skip"
        );
    }
}

/// Steady state - skip fires normally when there is no gap and nothing
/// later is due, exactly like the other two policies.
#[test]
fn create_system_event_skip_fires_a_single_occurrence_in_steady_state() {
    let et = scheduled_event_type(MissedOccurrencePolicy::Skip, timestamp(1000), None);
    let result = event_store::create_system_event(
        &et,
        timestamp(1001),
        timestamp(1001),
        0,
        || "{}".to_string(),
        no_key,
    );
    assert!(result.is_some());
}

#[test]
fn create_system_event_stamps_tags_from_the_payload_it_resolves() {
    let et = EventType {
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "kind".into(),
            field: "kind".into(),
        }],
        ..scheduled_event_type(MissedOccurrencePolicy::ReplayBacklog, timestamp(1000), None)
    };
    let (event, _) = event_store::create_system_event(
        &et,
        timestamp(1001),
        timestamp(1001),
        0,
        || r#"{"kind":"ping"}"#.to_string(),
        no_key,
    )
    .unwrap();
    assert_eq!(
        event.tags,
        vec![Tag {
            key: "kind".into(),
            value: Some("ping".into())
        }]
    );
}

// ---------------------------------------------------------------------
// skip_missed_occurrences
// ---------------------------------------------------------------------

#[test]
fn skip_missed_occurrences_advances_the_position_to_now() {
    let et = scheduled_event_type(MissedOccurrencePolicy::Skip, timestamp(1000), None);
    let new_position = event_store::skip_missed_occurrences(&et, timestamp(5000)).unwrap();
    assert_eq!(new_position, timestamp(5000));
}

#[test]
fn skip_missed_occurrences_is_a_no_op_once_the_cluster_is_already_caught_up() {
    let et = scheduled_event_type(MissedOccurrencePolicy::Skip, timestamp(5000), None);
    // A second instance resuming alongside the first, against a position
    // already at (or past) now - must not move anything.
    assert_eq!(
        event_store::skip_missed_occurrences(&et, timestamp(5000)),
        None
    );
    assert_eq!(
        event_store::skip_missed_occurrences(&et, timestamp(4000)),
        None
    );
}

#[test]
fn skip_missed_occurrences_only_ever_applies_to_the_skip_policy() {
    let et = scheduled_event_type(MissedOccurrencePolicy::FireOnce, timestamp(1000), None);
    assert_eq!(
        event_store::skip_missed_occurrences(&et, timestamp(5000)),
        None
    );
    let et = scheduled_event_type(MissedOccurrencePolicy::ReplayBacklog, timestamp(1000), None);
    assert_eq!(
        event_store::skip_missed_occurrences(&et, timestamp(5000)),
        None
    );
}

#[test]
fn skip_missed_occurrences_rejects_an_inactive_bounded_context() {
    let et = EventType {
        bounded_context: bounded_context(BoundedContextStatus::Archived),
        ..scheduled_event_type(MissedOccurrencePolicy::Skip, timestamp(1000), None)
    };
    assert_eq!(
        event_store::skip_missed_occurrences(&et, timestamp(5000)),
        None
    );
}

/// Drift audit finding #10 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): `rule SkipMissedOccurrences`'s own
/// first `requires` clause - not reachable from the real scheduler today
/// (`list_scheduled_event_types` already filters to this flag before
/// `skip_missed_occurrences_for_event_type` is ever called), but the pure
/// function itself must still encode it, the same standard every other
/// rule's own pure function in this module is held to.
#[test]
fn skip_missed_occurrences_rejects_an_event_type_not_opted_into_scheduling() {
    let et = EventType {
        system_triggered_allowed: false,
        ..scheduled_event_type(MissedOccurrencePolicy::Skip, timestamp(1000), None)
    };
    assert_eq!(
        event_store::skip_missed_occurrences(&et, timestamp(5000)),
        None
    );
}
