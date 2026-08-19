//! The in-memory per-bounded-context event cache the "Payload schema
//! shape" note's sibling passages describe throughout specs/skilj.allium
//! (search the spec for "in-memory event cache") - drift audit finding
//! #8 (project memory `skilj-drift-audit-2026-08-18`), closed here.
//! Named by `FetchEvents`/`ConsumeEvents`/`AcknowledgeEvents`,
//! `ProcessCommand`'s own DCB `matching_events` pre-check, and
//! `QueryEvents`/`CountEvents`/`InspectEvent` (`EventQuery`) as what
//! serves them - see each rule's own doc comment at its call site for
//! why those four groups, and only those four, are wired to this.
//!
//! **Postgres remains the source of truth throughout; this is purely a
//! read-path optimisation and never a limit on what is readable** - the
//! spec's own framing, taken literally: every read this cache can't
//! prove it fully covers falls back to an unmodified Postgres query
//! (`db::list_events_for_bounded_context`/`_from`, `db::list_events`,
//! `db::get_event_by_sequence` - all untouched by this module), and
//! every read it *can* answer returns exactly the same events a fresh
//! Postgres query would.
//!
//! **Design, in one pass**: one [`VecDeque<Event>`] per bounded context,
//! ascending by `sequence`, capped at a configured `capacity` (the
//! spec's own "a configurable count that defaults to 1000" - a
//! process-start knob, the identical register `SkiljBuilder::
//! scheduler_poll_interval`/`async_projection_poll_interval` already
//! live in, not a field on `BoundedContext`). Warmed once per bounded
//! context at startup ([`EventCache::warm`]) by fetching the most recent
//! `capacity` events; appended to as events commit
//! ([`EventCache::append`], called at the exact post-commit choke point
//! `EventBroadcaster::publish` already is - see that type's own doc
//! comment).
//!
//! **Multi-instance freshness, deliberately real** - the spec's own
//! top-level scope header excludes "Multi-instance / distributed
//! deployment", but this session already built real multi-instance-safe
//! mechanisms elsewhere (the admin `BoundedContext` seeding race fix,
//! the scheduler's own `@guarantee ScheduleStateIsShared`) at the user's
//! own explicit request, so the same bar applies here: a plain "trust
//! whatever this process appended to its own copy" cache would silently
//! violate `LatestEventsAlwaysAvailable` the moment a second instance
//! commits an event this process never observed. Instead, the only two
//! read entry points, [`EventCache::try_events_after`]/
//! [`EventCache::try_event_by_sequence`], compare this window's own
//! highest known `sequence` against a fresh `db::latest_sequence` read
//! every time, and if it's behind, fetch the missing delta via the
//! existing `db::list_events_for_bounded_context_from` (built for
//! `catch_up_bounded_context`, reused unchanged here) before answering.
//! One small, indexed query per cache-served read, never a full reload,
//! is the price of that guarantee holding for real, not just under a
//! single instance.
//!
//! **Coverage, not correctness, is what a request can fail**: once
//! freshened, a window only ever holds events it has proven are
//! complete back to `front().sequence` (nothing between that point and
//! the true beginning was ever evicted without first being loaded, and
//! nothing has been missed since). A request needing anything older than
//! that returns `None` - a coverage miss, not a wrong answer - and its
//! caller falls back to Postgres for the *whole* request, never a
//! partial merge (see the note above `rule ProcessCommand`'s own call
//! site in db/mod.rs for why a partial, tag-scoped completion was
//! considered and deliberately not built).

use crate::db::Pool;
use crate::event_store::Event;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// One bounded context's own recent-events window.
struct ContextWindow {
    /// Ascending by `sequence`, length capped at `EventCache::capacity`.
    events: VecDeque<Event>,
}

impl ContextWindow {
    fn highest_known_sequence(&self) -> Option<i64> {
        self.events.back().map(|e| e.sequence)
    }

    /// The earliest instant this window can vouch for completeness from -
    /// `None` when the window is empty (nothing evicted, either a
    /// genuinely empty bounded context or not yet warmed/touched).
    fn covers_from(&self) -> Option<i64> {
        self.events.front().map(|e| e.sequence)
    }

    fn push(&mut self, event: Event, capacity: usize) {
        self.events.push_back(event);
        while self.events.len() > capacity {
            self.events.pop_front();
        }
    }
}

/// See this module's own doc comment for the full design. `Clone` is
/// cheap - `Arc`-wrapped internals, the identical "hand out a cheap
/// clone, not an `Arc<Skilj>`" reasoning `EventBroadcaster` already
/// uses (its own `tokio::sync::broadcast::Sender` is internally `Arc`'d
/// the same way).
#[derive(Clone)]
pub struct EventCache {
    capacity: usize,
    contexts: Arc<tokio::sync::RwLock<HashMap<String, ContextWindow>>>,
}

impl EventCache {
    /// `capacity` is both the warm-up fetch size and the steady-state
    /// cap every bounded context's own window is held to afterward -
    /// see this module's own doc comment for why those are the same
    /// number, not two separate knobs.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            contexts: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    /// Seeds `bounded_context`'s own window from Postgres - the most
    /// recent `capacity` events, in ascending order. Called once per
    /// bounded context in `SkiljBuilder::build()`, before the first
    /// request can be served; safe to call again later (a fresh
    /// re-warm, replacing whatever the window held) though nothing does
    /// today - a newly created `BoundedContext` starts with an empty,
    /// correctly-covers-from-the-beginning window the first
    /// `try_events_after`/`append` call finds and treats identically to
    /// one this function warmed.
    pub async fn warm(&self, pool: &Pool, bounded_context: &str) -> crate::error::Result<()> {
        let recent =
            crate::db::list_recent_events_for_bounded_context(pool, bounded_context, self.capacity)
                .await?;
        let mut contexts = self.contexts.write().await;
        contexts.insert(
            bounded_context.to_string(),
            ContextWindow {
                events: recent.into(),
            },
        );
        Ok(())
    }

    /// The post-commit hook - called at the identical choke point
    /// `EventBroadcaster::publish` already is, once per committed event,
    /// regardless of origin. A bounded context this process has never
    /// warmed or touched before (a race with warm-up, or a context
    /// created after `.build()` returned) starts its own window here,
    /// from empty - correctly covering from the beginning, the same as
    /// a freshly `warm`ed one for a context with fewer than `capacity`
    /// events total.
    pub async fn append(&self, event: &Event) {
        let mut contexts = self.contexts.write().await;
        let window = contexts
            .entry(event.bounded_context.name.clone())
            .or_insert_with(|| ContextWindow {
                events: VecDeque::new(),
            });
        window.push(event.clone(), self.capacity);
    }

    /// Freshens `bounded_context`'s own window against Postgres (see
    /// this module's own doc comment on why every call does this, not
    /// just a cold one) and returns whether it now covers `sequence`.
    async fn freshen(&self, pool: &Pool, bounded_context: &str) -> crate::error::Result<()> {
        let known = {
            let contexts = self.contexts.read().await;
            contexts
                .get(bounded_context)
                .and_then(|w| w.highest_known_sequence())
        };
        let latest = crate::db::latest_sequence(pool, bounded_context).await?;
        if known == latest {
            return Ok(());
        }
        let delta = crate::db::list_events_for_bounded_context_from(
            pool,
            bounded_context,
            known.unwrap_or(-1),
        )
        .await?;
        let mut contexts = self.contexts.write().await;
        let window = contexts
            .entry(bounded_context.to_string())
            .or_insert_with(|| ContextWindow {
                events: VecDeque::new(),
            });
        for event in delta {
            window.push(event, self.capacity);
        }
        Ok(())
    }

    /// The one read entry point every cache-served call site uses (see
    /// this module's own doc comment for the full list). `after_sequence:
    /// -1` asks for full history - `ProcessCommand`'s own DCB pre-check
    /// and `CountEvents` both need this, since neither takes an
    /// `after_sequence` of its own. `Ok(None)` is a coverage miss, not
    /// an error or "nothing found" - the caller falls back to its own
    /// unmodified Postgres query for the whole request.
    pub async fn try_events_after(
        &self,
        pool: &Pool,
        bounded_context: &str,
        after_sequence: i64,
    ) -> crate::error::Result<Option<Vec<Event>>> {
        self.freshen(pool, bounded_context).await?;
        let contexts = self.contexts.read().await;
        let Some(window) = contexts.get(bounded_context) else {
            // Freshened and still nothing - a genuinely empty bounded
            // context, which trivially covers everything from the
            // beginning.
            return Ok(Some(Vec::new()));
        };
        match window.covers_from() {
            None => Ok(Some(Vec::new())), // freshened, still empty - see above
            Some(covers_from) if after_sequence >= covers_from - 1 => Ok(Some(
                window
                    .events
                    .iter()
                    .filter(|e| e.sequence > after_sequence)
                    .cloned()
                    .collect(),
            )),
            Some(_) => Ok(None), // after_sequence reaches further back than this window can prove
        }
    }

    /// `InspectEvent`'s own single-row lookup. `Ok(None)` is a cache
    /// miss (this sequence isn't in the window right now - may still
    /// exist further back in Postgres, or not exist at all), not
    /// "doesn't exist" - the caller falls back to
    /// `db::get_event_by_sequence` either way.
    pub async fn try_event_by_sequence(
        &self,
        pool: &Pool,
        bounded_context: &str,
        sequence: i64,
    ) -> crate::error::Result<Option<Event>> {
        self.freshen(pool, bounded_context).await?;
        let contexts = self.contexts.read().await;
        let Some(window) = contexts.get(bounded_context) else {
            return Ok(None);
        };
        Ok(window
            .events
            .iter()
            .find(|e| e.sequence == sequence)
            .cloned())
    }
}
