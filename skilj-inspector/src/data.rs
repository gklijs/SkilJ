//! The read-only data layer - every function here does exactly one
//! `skilj_core::db` read call (or, for [`load_bounded_context_data`], an
//! existence check and four reads in sequence) and nothing else. See this crate's own root doc comment for
//! the "never a write path, never decryption" constraints every function
//! here holds to.

use skilj_core::db::Pool;
use skilj_core::event_store::{BoundedContext, CommandType, Event, EventType};
use skilj_core::projections::Projection;

/// How many of a bounded context's most recent events to load into the
/// Events tab - a bounded, cheap-to-refresh window rather than the whole
/// history (`db::list_events_for_bounded_context` loads genuinely
/// everything, the shape `submit_command`'s own consistency-boundary
/// resolution needs, not what an operator skimming recent activity
/// wants). Refreshable on demand (`r` in the running app), not paginated,
/// since there's no "events before sequence N" read to page backwards
/// with today; `db::list_recent_events_for_bounded_context` is the
/// closest fit and already ordered newest-first.
pub const RECENT_EVENTS_LIMIT: usize = 200;

/// Every registered `BoundedContext`, ordered however
/// `db::list_bounded_contexts` returns them.
pub async fn load_bounded_contexts(pool: &Pool) -> skilj_core::error::Result<Vec<BoundedContext>> {
    skilj_core::db::list_bounded_contexts(pool).await
}

/// Everything the tabs behind a selected bounded context show, loaded
/// together - one call per tab's own `db::` read, all against the same
/// `bounded_context.name`.
pub struct BoundedContextData {
    pub event_types: Vec<EventType>,
    pub command_types: Vec<CommandType>,
    pub projections: Vec<Projection>,
    pub recent_events: Vec<Event>,
    /// What has been said about those events, retracted or in doubt
    /// (docs/architecture.md §235, §236). By position, for those it applies to.
    pub event_stances: std::collections::HashMap<i64, skilj_core::retraction::EventStance>,
}

pub async fn load_bounded_context_data(
    pool: &Pool,
    bounded_context: &str,
) -> skilj_core::error::Result<BoundedContextData> {
    // `db::list_recent_events_for_bounded_context` (unlike the three
    // list functions below, each of which already guards this itself)
    // assumes its caller already knows the bounded context exists -
    // it returns a not-found error rather than `[]`. This crate's own
    // whole premise is "let an operator look something up without
    // already knowing it's there", so that assumption doesn't hold here
    // - check first, short-circuit to the same all-empty shape the other
    // three give an unregistered context.
    if skilj_core::db::get_bounded_context(pool, bounded_context)
        .await?
        .is_none()
    {
        return Ok(BoundedContextData {
            event_types: Vec::new(),
            command_types: Vec::new(),
            projections: Vec::new(),
            recent_events: Vec::new(),
            event_stances: Default::default(),
        });
    }

    let event_types =
        skilj_core::db::list_event_types_for_bounded_context(pool, bounded_context).await?;
    let command_types =
        skilj_core::db::list_command_types_for_bounded_context(pool, bounded_context).await?;
    let projections =
        skilj_core::db::list_projections_for_bounded_context(pool, bounded_context).await?;
    let recent_events = skilj_core::db::list_recent_events_for_bounded_context(
        pool,
        bounded_context,
        RECENT_EVENTS_LIMIT,
    )
    .await?;
    let sequences: Vec<i64> = recent_events.iter().map(|event| event.sequence).collect();
    let event_stances = skilj_core::db::event_stances(pool, bounded_context, &sequences).await?;
    Ok(BoundedContextData {
        event_types,
        command_types,
        projections,
        recent_events,
        event_stances,
    })
}

/// What the event list shows next to an event something was said about: a
/// retraction ends a doubt, so never both.
pub fn stance_marker(stance: &skilj_core::retraction::EventStance) -> String {
    match (stance.retracted_by, stance.doubted_by) {
        (Some(retraction), _) => format!(" [retracted by #{retraction}]"),
        (None, Some(doubt)) => format!(" [in doubt by #{doubt}]"),
        (None, None) => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skilj_core::retraction::EventStance;

    #[test]
    fn the_marker_says_what_was_said() {
        let mut stance = EventStance::default();
        assert_eq!(stance_marker(&stance), "");
        stance.doubted_by = Some(8);
        assert_eq!(stance_marker(&stance), " [in doubt by #8]");
        stance.retracted_by = Some(9);
        assert_eq!(stance_marker(&stance), " [retracted by #9]");
    }
}
