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
//!
//! **Declarations are served current**: an event holds a copy of its
//! type, and a type's declarations (`private_fields`, `owner_tag_key`)
//! apply to its whole history the moment they change. So every event
//! leaves a window with its bounded context's current registrations,
//! reloaded whenever `freshen`'s stamp of them changes - see
//! `Registrations` (docs/architecture.md §180).

use crate::db::Pool;
use crate::event_store::{Command, CommandType, Event, EventOrigin, EventType};
use crate::shared::Tag;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// One bounded context's own recent-events window.
struct ContextWindow {
    /// Ascending by `sequence`, length capped at `EventCache::capacity`.
    events: VecDeque<Event>,
    /// The `events` table these came from (`db::events_table_identity`),
    /// or `None` for a window only `append` has touched. A bounded context
    /// hard-deleted and recreated under the same name has a new table, so
    /// a mismatch means these events belong to its predecessor and the
    /// window is refilled rather than served (docs/architecture.md §95).
    /// The same goes for a new database epoch (§176): after a promotion the
    /// table is the same but its log may end before events this window
    /// holds, and later sequences are reused for different events.
    table: Option<crate::db::TableIdentity>,
    /// The bounded context's registrations as of `stamp`, or `None` until
    /// the next `freshen` loads them. Every event leaves this window with
    /// these copies of its type rather than the one it came in with
    /// (docs/architecture.md §180).
    registrations: Option<Registrations>,
}

/// The current `EventType`s and `CommandType`s of one bounded context. A
/// declaration like `private_fields` or `owner_tag_key` applies to a
/// type's whole history the moment it changes, but an event holds a copy
/// of its type from when it was read or committed - so a window that kept
/// handing those copies out rendered and scoped its events by an old
/// registration until they were evicted, while the same read served from
/// Postgres got the new one (docs/architecture.md §180).
struct Registrations {
    stamp: crate::db::RegistrationsStamp,
    event_types: HashMap<String, EventType>,
    command_types: HashMap<String, CommandType>,
}

impl Registrations {
    /// `event` with its own type, and its originating command's, replaced
    /// by the current registration. A name not registered here (a command
    /// from another bounded context) keeps the copy it has.
    ///
    /// Built field by field, so only the copy that is served gets cloned:
    /// cloning the whole event first and then overwriting its type cloned
    /// every type twice, on every read the cache serves
    /// (docs/architecture.md §195).
    fn current(&self, event: &Event) -> Event {
        let origin = match &event.origin {
            EventOrigin::CommandTriggered { command }
                if command
                    .command_type
                    .bounded_context
                    .same_as(&event.bounded_context) =>
            {
                match self.command_types.get(&command.command_type.name) {
                    Some(ct) => EventOrigin::CommandTriggered {
                        command: Box::new(Command {
                            id: command.id.clone(),
                            bounded_context: command.bounded_context.clone(),
                            command_type: ct.clone(),
                            payload: command.payload.clone(),
                            metadata: command.metadata.clone(),
                            encryption_keys: command.encryption_keys.clone(),
                            consistency_tags: command.consistency_tags.clone(),
                            consistency_query: command.consistency_query.clone(),
                            consistency_boundary: command.consistency_boundary,
                        }),
                    },
                    None => event.origin.clone(),
                }
            }
            origin => origin.clone(),
        };
        Event {
            bounded_context: event.bounded_context.clone(),
            event_type: self
                .event_types
                .get(&event.event_type.name)
                .unwrap_or(&event.event_type)
                .clone(),
            payload: event.payload.clone(),
            metadata: event.metadata.clone(),
            sequence: event.sequence,
            tags: event.tags.clone(),
            encryption_keys: event.encryption_keys.clone(),
            origin,
        }
    }
}

impl ContextWindow {
    /// How `event` is served out of this window - see [`Registrations`].
    fn serve(&self, event: &Event) -> Event {
        match &self.registrations {
            Some(r) => r.current(event),
            None => event.clone(),
        }
    }

    fn highest_known_sequence(&self) -> Option<i64> {
        self.events.back().map(|e| e.sequence)
    }

    /// The earliest instant this window can vouch for completeness from -
    /// `None` when the window is empty (nothing evicted, either a
    /// genuinely empty bounded context or not yet warmed/touched).
    fn covers_from(&self) -> Option<i64> {
        self.events.front().map(|e| e.sequence)
    }

    /// Adds an event read from Postgres as part of a complete range (a
    /// `freshen` delta): anything past the highest known sequence. One
    /// already here - a concurrent `append` or `freshen` got to it first -
    /// is skipped rather than held twice (docs/architecture.md §89).
    fn push_from_database(&mut self, event: Event, capacity: usize) {
        if self
            .highest_known_sequence()
            .is_some_and(|highest| event.sequence <= highest)
        {
            return;
        }
        self.push_unchecked(event, capacity);
    }

    /// Adds an event this process just committed, only if it extends the
    /// window without a hole: exactly one past the highest known
    /// sequence (or into an empty window, which vouches for nothing
    /// before its first event). Commits don't reach this in order - a
    /// concurrent commit's append, or another instance's event this cache
    /// never hears of, can sit in between - so an event further ahead is
    /// dropped instead of claiming the window is complete up to it; the
    /// next read's `freshen` fills the range from Postgres. One at or
    /// below the highest is already here (§89).
    fn append_committed(&mut self, event: Event, capacity: usize) {
        match self.highest_known_sequence() {
            Some(highest) if event.sequence != highest + 1 => {}
            _ => self.push_unchecked(event, capacity),
        }
    }

    fn push_unchecked(&mut self, event: Event, capacity: usize) {
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
    /// A zero `capacity` is a cache that's off: it holds nothing, and
    /// every lookup misses (`Ok(None)`), so callers read Postgres. It used
    /// to hold nothing yet report every bounded context as empty.
    fn is_disabled(&self) -> bool {
        self.capacity == 0
    }

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
        if self.is_disabled() {
            return Ok(());
        }
        let state = crate::db::events_table_identity(pool, bounded_context).await?;
        self.refill(pool, bounded_context, state.table).await?;
        self.refresh_registrations(pool, bounded_context, state.registrations)
            .await
    }

    /// Loads `bounded_context`'s registrations into its window unless the
    /// window already holds them as of `stamp`. `stamp` is read before
    /// the registrations, so a re-registration racing this leaves a window
    /// stamped older than what it holds, which the next `freshen` reloads
    /// - never one stamped current while holding an old registration.
    async fn refresh_registrations(
        &self,
        pool: &Pool,
        bounded_context: &str,
        stamp: crate::db::RegistrationsStamp,
    ) -> crate::error::Result<()> {
        let current = {
            let contexts = self.contexts.read().await;
            match contexts.get(bounded_context) {
                Some(w) => w.registrations.as_ref().is_some_and(|r| r.stamp == stamp),
                // Nothing to serve, so nothing to refresh.
                None => true,
            }
        };
        if current {
            return Ok(());
        }
        let event_types =
            crate::db::list_event_types_for_bounded_context(pool, bounded_context).await?;
        let command_types =
            crate::db::list_command_types_for_bounded_context(pool, bounded_context).await?;
        let mut contexts = self.contexts.write().await;
        if let Some(window) = contexts.get_mut(bounded_context) {
            window.registrations = Some(Registrations {
                stamp,
                event_types: event_types
                    .into_iter()
                    .map(|et| (et.name.clone(), et))
                    .collect(),
                command_types: command_types
                    .into_iter()
                    .map(|ct| (ct.name.clone(), ct))
                    .collect(),
            });
        }
        Ok(())
    }

    /// Replaces `bounded_context`'s window with its most recent `capacity`
    /// events, stamped with `table` - read by the caller *before* the
    /// events, so a delete-and-recreate racing this leaves a stale stamp
    /// the next `freshen` notices, never a fresh stamp on old events.
    async fn refill(
        &self,
        pool: &Pool,
        bounded_context: &str,
        table: crate::db::TableIdentity,
    ) -> crate::error::Result<()> {
        let recent =
            crate::db::list_recent_events_for_bounded_context(pool, bounded_context, self.capacity)
                .await?;
        let mut contexts = self.contexts.write().await;
        contexts.insert(
            bounded_context.to_string(),
            ContextWindow {
                events: recent.into(),
                table: Some(table),
                registrations: None,
            },
        );
        Ok(())
    }

    /// The post-commit hook - called at the identical choke point
    /// `EventBroadcaster::publish` already is, once per event this process
    /// committed. It only ever extends the window contiguously (see
    /// `ContextWindow::append_committed`): anything else is left for the
    /// next read's `freshen` to load from Postgres. A bounded context this
    /// process has never warmed or touched before (a race with warm-up, or
    /// a context created after `.build()` returned) starts its own window
    /// here, vouching only for this event onward.
    pub async fn append(&self, event: &Event) {
        if self.is_disabled() {
            return;
        }
        let mut contexts = self.contexts.write().await;
        let window = contexts
            .entry(event.bounded_context.name.clone())
            .or_insert_with(|| ContextWindow {
                events: VecDeque::new(),
                table: None,
                registrations: None,
            });
        window.append_committed(event.clone(), self.capacity);
    }

    /// Freshens `bounded_context`'s own window against Postgres (see
    /// this module's own doc comment on why every call does this, not
    /// just a cold one) and returns whether it now covers `sequence`.
    async fn freshen(&self, pool: &Pool, bounded_context: &str) -> crate::error::Result<()> {
        let state = crate::db::events_table_identity(pool, bounded_context).await?;
        self.freshen_events(pool, bounded_context, state.table, state.latest)
            .await?;
        self.refresh_registrations(pool, bounded_context, state.registrations)
            .await
    }

    /// `freshen`'s events half: brings the window up to `latest`.
    async fn freshen_events(
        &self,
        pool: &Pool,
        bounded_context: &str,
        table: crate::db::TableIdentity,
        latest: Option<i64>,
    ) -> crate::error::Result<()> {
        let (known, known_table) = {
            let contexts = self.contexts.read().await;
            match contexts.get(bounded_context) {
                Some(w) => (w.highest_known_sequence(), w.table.clone()),
                None => (None, None),
            }
        };
        // docs/architecture.md §95: a window from another incarnation of
        // this name - or one only `append` has started, which can't vouch
        // for its table - is replaced wholesale.
        if known_table.as_ref().is_some_and(|t| *t != table)
            || (known_table.is_none() && known.is_some())
        {
            return self.refill(pool, bounded_context, table).await;
        }
        if known == latest {
            return Ok(());
        }
        // A cold window (never warmed - e.g. a bounded context added at
        // runtime) or one further behind than it can hold is refilled
        // from the recent tail, as `warm` does, rather than loading every
        // event since `known` (the whole history, when cold) only to keep
        // the last `capacity` of them (docs/architecture.md §79).
        let behind_by = latest.map(|l| l - known.unwrap_or(-1)).unwrap_or(0);
        if known.is_none() || behind_by > i64::try_from(self.capacity).unwrap_or(i64::MAX) {
            return self.refill(pool, bounded_context, table).await;
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
                table: Some(table),
                registrations: None,
            });
        for event in delta {
            window.push_from_database(event, self.capacity);
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
        Ok(self
            .try_events_where(pool, bounded_context, after_sequence, |_| true)
            .await?
            .map(|(events, _)| events))
    }

    /// `try_events_after`, keeping only the events `keep` accepts, plus the
    /// window's highest sequence (`-1` when it holds none). `keep` runs
    /// before an event is served, so what it turns away is never cloned -
    /// a tag read used to clone the whole window to return a handful of
    /// it (docs/architecture.md §187).
    async fn try_events_where(
        &self,
        pool: &Pool,
        bounded_context: &str,
        after_sequence: i64,
        keep: impl Fn(&Event) -> bool,
    ) -> crate::error::Result<Option<(Vec<Event>, i64)>> {
        if self.is_disabled() {
            return Ok(None);
        }
        self.freshen(pool, bounded_context).await?;
        let contexts = self.contexts.read().await;
        let Some(window) = contexts.get(bounded_context) else {
            // Freshened and still nothing - a genuinely empty bounded
            // context, which trivially covers everything from the
            // beginning.
            return Ok(Some((Vec::new(), -1)));
        };
        match window.covers_from() {
            None => Ok(Some((Vec::new(), -1))), // freshened, still empty - see above
            Some(covers_from) if after_sequence >= covers_from - 1 => {
                let from = window
                    .events
                    .partition_point(|e| e.sequence <= after_sequence);
                Ok(Some((
                    window
                        .events
                        .range(from..)
                        .filter(|e| keep(e))
                        .map(|e| window.serve(e))
                        .collect(),
                    window.highest_known_sequence().unwrap_or(-1),
                )))
            }
            Some(_) => Ok(None), // after_sequence reaches further back than this window can prove
        }
    }

    /// Whether `bounded_context`'s window has already let go of the events
    /// just after `after_sequence`, so it can't serve a read from there
    /// however it is freshened: freshening only appends and evicts, and a
    /// refill takes the most recent `capacity` events of a log already
    /// longer than that, so the window's first event only ever moves on.
    /// The exception is a refill onto another table (§95/§176), which needs
    /// a window stamped with one, so a window only `append` has touched
    /// never counts.
    async fn has_evicted_past(&self, bounded_context: &str, after_sequence: i64) -> bool {
        let contexts = self.contexts.read().await;
        contexts.get(bounded_context).is_some_and(|w| {
            w.table.is_some()
                && w.covers_from()
                    .is_some_and(|front| front - 1 > after_sequence)
        })
    }

    /// [docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 1" fix -
    /// `db::list_events_for_bounded_context_matching_tags_cached`'s own
    /// cache-first half: the events after `after_sequence` carrying any
    /// of `tags` (the same union semantics
    /// `consistency_boundary_and_matching_events` already uses), so a
    /// caller gets the same already-tag-scoped shape whether this was
    /// served from cache or fell through to the tag-indexed Postgres
    /// query. `after_sequence` is `-1` for a command's whole history, or
    /// its snapshot's `as_of_sequence`: a window that no longer reaches
    /// the start can still serve the events since a recent snapshot
    /// (docs/architecture.md §188). `Ok(None)` is a coverage miss,
    /// identical convention to every other method here - the caller falls
    /// back to `db::list_events_for_bounded_context_matching_tags`.
    ///
    /// Alongside the events, the position they are complete through: the
    /// highest sequence the window held, matching or not (`-1` when there
    /// is none) - every event at or below it was looked at. A command's
    /// re-check under the lock only needs what came after it
    /// (docs/architecture.md §178).
    pub async fn try_events_matching_tags(
        &self,
        pool: &Pool,
        bounded_context: &str,
        tags: &[Tag],
        after_sequence: i64,
    ) -> crate::error::Result<Option<(Vec<Event>, i64)>> {
        // A window that has already evicted what this read needs misses
        // whatever `freshen` does, so its round trip is skipped. Only a
        // miss can come of skipping it: a window that ends up stale this
        // way (its bounded context recreated) keeps sending these reads to
        // Postgres until any other read freshens it (docs/architecture.md
        // §187).
        if self.has_evicted_past(bounded_context, after_sequence).await {
            return Ok(None);
        }
        self.try_events_where(pool, bounded_context, after_sequence, |e| {
            tags.iter().any(|t| e.tags.contains(t))
        })
        .await
    }

    /// [`Self::try_events_matching_tags`] for a consistency query
    /// (docs/architecture.md §198): every event in the window after
    /// `after_sequence` matching any item of `query`, however many of its
    /// matches an item keeps - the caller selects the last ones from it.
    pub async fn try_events_matching_query(
        &self,
        pool: &Pool,
        bounded_context: &str,
        query: &[crate::shared::QueryItem],
        after_sequence: i64,
    ) -> crate::error::Result<Option<(Vec<Event>, i64)>> {
        if self.has_evicted_past(bounded_context, after_sequence).await {
            return Ok(None);
        }
        self.try_events_where(pool, bounded_context, after_sequence, |e| {
            crate::event_store::query_matches(query, e)
        })
        .await
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
        if self.is_disabled() {
            return Ok(None);
        }
        self.freshen(pool, bounded_context).await?;
        let contexts = self.contexts.read().await;
        let Some(window) = contexts.get(bounded_context) else {
            return Ok(None);
        };
        Ok(window
            .events
            .binary_search_by_key(&sequence, |e| e.sequence)
            .ok()
            .map(|i| window.serve(&window.events[i])))
    }
}
