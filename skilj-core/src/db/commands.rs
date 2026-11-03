//! Persistence for commands rule groups.

use super::*;

/// What `submit_command` below settles on - either governs a real insert
/// (`Accepted`, already persisted by the time this returns) or a
/// legitimate business rejection (`Rejected`, nothing persisted at all).
/// `ProcessCommand`'s own two `CommandDecision` outcomes, but carrying
/// the *actual* result - which may differ from the caller's own
/// optimistic `dispatch` call if a DCB conflict forced a retry, see
/// `submit_command`'s own doc comment.
#[derive(Debug)]
#[non_exhaustive]
pub enum SubmitCommandOutcome {
    Accepted {
        // Boxed for the same reason `EventOrigin::CommandTriggered`'s own
        // `command` field is - `Command` is large enough next to
        // `Rejected`'s two `String`s that clippy's `large_enum_variant`
        // flags it otherwise.
        command: Box<Command>,
        events: Vec<Event>,
    },
    Rejected {
        reason: String,
        kind: String,
        // issue #7's DCB conflict visualizer: the tag-scoped
        // event set `decide()` actually evaluated against for *this*
        // rejection - the caller-supplied `matching_events` parameter
        // below for an ordinary rejection, or the freshly-recomputed
        // set from this function's own DCB-conflict redispatch branch
        // if that's what produced it (never the caller's now-stale
        // pre-lock one in that case) - see this function's own doc
        // comment.
        matching_events: Vec<Event>,
    },
    /// A duplicate submission bearing an `idempotency_key` that already
    /// produced a real `Accepted` outcome (issue #12) - not a
    /// new decision, the *original* one, verbatim. `decide()` never ran
    /// again; no event was inserted again. Rejected outcomes are never
    /// deduplicated - they have zero side effects, so a duplicate one is
    /// simply re-decided fresh, correctly, every time (see
    /// `submit_command`'s own doc comment for why).
    Deduplicated { triggered_event_sequences: Vec<i64> },
}

/// A `submit_command` call's own snapshot-acceleration context
/// ([docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)) - `Some` when the caller took the
/// snapshot-accelerated path (`CommandDispatcher::dispatch_from_snapshot`)
/// instead of the ordinary one (`dispatch`) for its own `initial_decision`.
/// `state_json` is handed to `dispatch_from_snapshot` again on a
/// DCB-conflict redispatch (made fresh, here, exactly like the ordinary
/// path's own `dispatch` re-call); `as_of_sequence` is the snapshot's
/// own high-water mark - the floor `original_highest` needs when
/// `bounded_context_events` (the caller's own `events_since_snapshot`)
/// is empty, since a plain `.unwrap_or(-1)` there would wrongly treat
/// "nothing changed since the snapshot" as "nothing has ever happened".
///
/// **Known limitation, narrow and audit-only**: `Command.consistency_boundary`
/// can under-report as `None` for a snapshot-accelerated command whose
/// own `events_since_snapshot` is empty, even though the snapshot's own
/// folded prefix represents real prior history -
/// `consistency_boundary_and_matching_events` has no way to know about
/// `as_of_sequence`, only about `bounded_context_events`. This never
/// affects `decide_from_snapshot`'s own inputs or the decision it
/// produces - only that one audit field - so it's left as a documented
/// gap for this pass rather than widening `process_command`'s own
/// signature too.
pub struct SnapshotContext<'a> {
    pub state_json: &'a str,
    pub as_of_sequence: i64,
}

/// The shared "locked half" of `ProcessCommand` - `skilj-rest`'s
/// `post_commands_trigger` and `skilj-graphql`'s `submitCommand` both
/// delegate to this once authorisation is done and `initial_decision` -
/// `dispatch`'s own first, optimistic, unlocked call against
/// `bounded_context_events` - is already in hand. Implements the
/// optimistic-then-locked pattern the note above the rules in
/// specs/skilj.allium describes: "Only the final re-check and insert
/// need the lock; reading matching events and running decide()
/// beforehand does not."
///
/// Opens one transaction and locks `bounded_context`'s own `sequence`
/// row up front - `SELECT ... FOR UPDATE`, peeking its current value
/// rather than `next_sequence`'s own increment-and-return, since how
/// many sequence numbers this submission ends up needing isn't known
/// until the decision that finally governs it is. If that peek shows
/// more has been committed than `bounded_context_events` (the caller's
/// own optimistic read - already tag-scoped to `consistency_tags` by
/// every real call site, [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 1" fix)
/// already reflected, and at least one of those new arrivals matches
/// `consistency_tags`, that is the DCB conflict this lock exists to
/// catch (see `DynamicConsistencyBoundaryHonoured`):
/// `dispatch` is called again, now with a `matching_events` set that
/// includes the new arrival, and the fresh decision it returns supersedes
/// `initial_decision`. At most one such retry is ever needed - once the
/// lock is held, nothing else can commit to this bounded context until
/// this transaction ends (see the note above the rules: "no concurrent
/// committer for the same bounded context can be interleaved while the
/// row is locked"), so there is nothing further this call could miss.
///
/// Whichever decision ends up governing, `event_store::process_command`,
/// `insert_command`, and `insert_event_and_update_sync_projections_in_tx`
/// (once per triggered event) all run inside that same transaction and
/// share its one commit; a rejection (initial or retried) instead leaves
/// `tx` uncommitted - dropped with nothing but the read-only peek lock
/// ever taken, the same implicit-rollback-on-drop every early `?` return
/// elsewhere in this module already relies on. Either way,
/// `SequenceIsGaplessPerBoundedContext` holds for real: any failure
/// anywhere in this function (a rejection, an unregistered event type, a
/// database error) rolls the whole transaction back, sequence allocation
/// included, rather than burning a sequence number on a write that never
/// lands - the actual bug this function exists to close.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        bounded_context = %command_type.bounded_context.name,
        command_type = %command_type.name,
    )
)]
pub async fn submit_command(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    bounded_context_events: &[Event],
    consistency_tags: &[Tag],
    matching_events: &[Event],
    initial_decision: crate::shared::CommandDecision,
    key_service: Option<&KeyService>,
    now: DateTime<Utc>,
    snapshot: Option<SnapshotContext<'_>>,
    snapshot_states: &[crate::plugin::SnapshotState],
    idempotency_key: Option<&str>,
    covered_through: Option<i64>,
    context: &crate::plugin::DecisionContext,
) -> crate::error::Result<SubmitCommandOutcome> {
    let bounded_context_name = command_type.bounded_context.name.clone();

    // issue #32: a pre-lock warm-up for the common, no-DCB-conflict
    // path - mirrors `create_and_insert_external_event`/
    // `create_and_insert_direct_event`'s own "resolve encryption keys
    // before opening the transaction" shape, extended here to event-type
    // lookups too. See `warm_up_event_types_and_encryption_keys`'s own
    // doc comment for why this is always safe, never just an
    // optimisation this function's own correctness depends on.
    let (event_types_by_name, resolved) = warm_up_event_types_and_encryption_keys(
        pool,
        &bounded_context_name,
        command_type,
        payload,
        &initial_decision,
        key_service,
    )
    .await?;

    let mut tx = pool.begin().await?;
    let (locked_highest, limits) =
        lock_bounded_context_sequence_with_limits(&mut tx, &bounded_context_name).await?;
    let payload_limits = limits.apply(projection_dispatcher.payload_limits());
    // Fetched once for the whole submission, not once per event (
    // issue #32) - and only now, after the lock above is already held,
    // which is what keeps this read race-free against
    // `promote_projection_rebuild` (see
    // `insert_event_and_update_sync_projections_in_tx`'s own doc comment).
    let sync_projections = sync_projection_names(&mut tx, &bounded_context_name).await?;

    let outcome = submit_one_command_in_tx(
        &mut tx,
        dispatcher,
        projection_dispatcher,
        command_type,
        payload,
        client_id,
        correlation_id,
        causation_id,
        bounded_context_events,
        consistency_tags,
        matching_events,
        initial_decision,
        key_service,
        now,
        snapshot,
        snapshot_states,
        idempotency_key,
        locked_highest,
        &sync_projections,
        &[],
        event_types_by_name,
        resolved,
        covered_through,
        context,
        &payload_limits,
    )
    .await?;

    tx.commit().await?;
    broadcast_appended_events(pool, broadcaster, event_cache, &outcome).await;

    Ok(outcome)
}

/// The pre-lock warm-up `submit_command`/`command_batcher::CommandBatcher::submit`
/// both run before ever opening a transaction: every `EventType` an
/// accepted `initial_decision`'s own event_specs names, and every
/// `EncryptionKey` those events' (and the command's own) sensitive
/// fields need. A no-op (both maps come back empty) for a `Rejected`
/// `initial_decision` - nothing to warm up for a command that was never
/// going to write anything.
///
/// **Always safe, never a correctness dependency**: `submit_one_command_in_tx`'s
/// own post-lock code re-runs the identical resolution unconditionally
/// against whatever `event_specs` actually governs by the time it runs
/// (the redispatched ones, if a DCB conflict forced a retry) - its
/// `Entry::Vacant`/`resolved.contains_key` guards make every call here a
/// no-op the second time, and tolerate this warm-up being partial, stale,
/// or skipped entirely just as well as they tolerate it being complete.
/// This function exists purely to shorten the bounded-context lock's own
/// hold span (`submit_command`) or to move that work fully in parallel,
/// before a request ever joins a shared batch queue at all
/// (`CommandBatcher::submit`) - not to change what's correct.
pub async fn warm_up_event_types_and_encryption_keys(
    pool: &Pool,
    bounded_context_name: &str,
    command_type: &CommandType,
    payload: &str,
    initial_decision: &crate::shared::CommandDecision,
    key_service: Option<&KeyService>,
) -> crate::error::Result<(
    std::collections::HashMap<String, EventType>,
    std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
)> {
    let mut event_types_by_name: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut resolved = std::collections::HashMap::new();
    if let crate::shared::CommandDecision::Accepted { events } = initial_decision {
        for spec in events {
            if let std::collections::hash_map::Entry::Vacant(entry) =
                event_types_by_name.entry(spec.event_type.clone())
            {
                // `command_type` already carries the bounded context's
                // row, read moments ago (docs/architecture.md §196).
                if let Some(et) =
                    get_event_type_with_bc(pool, &command_type.bounded_context, &spec.event_type)
                        .await?
                {
                    entry.insert(et);
                }
            }
        }
        resolve_encryption_keys(
            pool,
            bounded_context_name,
            &command_type.sensitive_fields,
            payload,
            key_service,
            &mut resolved,
        )
        .await?;
        for spec in events {
            if let Some(event_type) = event_types_by_name.get(&spec.event_type) {
                let spec_payload = spec.payload.to_string();
                resolve_encryption_keys(
                    pool,
                    bounded_context_name,
                    &event_type.sensitive_fields,
                    &spec_payload,
                    key_service,
                    &mut resolved,
                )
                .await?;
            }
        }
    }
    Ok((event_types_by_name, resolved))
}

/// The `SELECT ... FOR UPDATE` peek `submit_command`'s own doc comment
/// describes, pulled out so `submit_command_batch`'s leader can take it
/// exactly once per batch too, instead of once per command - the actual
/// mechanism this whole pass (issue #32, round two) exists to
/// amortise across as many concurrently-arriving commands as possible.
pub(super) async fn lock_bounded_context_sequence(
    tx: &mut Transaction<'_, Postgres>,
    bounded_context: &str,
) -> crate::error::Result<i64> {
    let schema = schema_ident(bounded_context);
    let (locked_highest,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT next_value FROM {schema}.sequence FOR UPDATE"
    )))
    .fetch_one(&mut **tx)
    .await?;
    Ok(locked_highest)
}

/// [`lock_bounded_context_sequence`], also returning the bounded context's
/// payload-limit override from the row it locks (docs/architecture.md
/// §238) - in the same statement, so it costs no round trip.
pub(super) async fn lock_bounded_context_sequence_with_limits(
    tx: &mut Transaction<'_, Postgres>,
    bounded_context: &str,
) -> crate::error::Result<(i64, PayloadLimitsOverride)> {
    let schema = schema_ident(bounded_context);
    let (locked_highest, max_payload_bytes, warn_payload_bytes): (i64, Option<i64>, Option<i64>) =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT next_value, {PAYLOAD_LIMIT_COLUMNS} FROM {schema}.sequence s FOR UPDATE"
        )))
        .fetch_one(&mut **tx)
        .await?;
    Ok((
        locked_highest,
        PayloadLimitsOverride {
            max_payload_bytes,
            warn_payload_bytes,
        },
    ))
}

/// `EventSubscription`'s own real-time delivery - after the commit, not
/// before, the same rule `insert_event_and_update_sync_projections`
/// itself already follows. Shared by `submit_command`'s own single-request
/// path and `command_batcher::CommandBatcher`'s batched one - both commit
/// once, then need every event that commit produced broadcast exactly
/// this way; a no-op for `Rejected`/`Deduplicated` outcomes, which
/// produced nothing to broadcast.
pub async fn broadcast_appended_events(
    pool: &Pool,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    outcome: &SubmitCommandOutcome,
) {
    broadcast_appended_batch(pool, broadcaster, event_cache, std::iter::once(outcome)).await;
}

/// [`broadcast_appended_events`] for every outcome of a committed batch at
/// once: each event is published and cached as before, and the other
/// instances are notified of all of them in one statement rather than one
/// round trip per event, which the batch's callers used to wait out
/// before getting their answers (docs/architecture.md §196).
pub async fn broadcast_appended_batch<'a>(
    pool: &Pool,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    outcomes: impl IntoIterator<Item = &'a SubmitCommandOutcome>,
) {
    let events: Vec<&Event> = outcomes
        .into_iter()
        .flat_map(|outcome| match outcome {
            SubmitCommandOutcome::Accepted { events, .. } => events.as_slice(),
            _ => &[],
        })
        .collect();
    for event in &events {
        broadcaster.publish(event);
        record_event_appended(event);
        event_cache.append(event).await;
    }
    notify_events_appended(pool, &events, broadcaster.instance_id()).await;
}

/// Where a command's re-check under the lock starts: where its optimistic
/// read is complete through, when the caller knows that, else the highest
/// event the read holds (or the snapshot's position, when it holds none) -
/// the larger of the two, since a read from Postgres can hold an event
/// above the position it reports (docs/architecture.md §178).
pub(super) fn recheck_from(
    bounded_context_events: &[Event],
    snapshot_as_of: Option<i64>,
    covered_through: Option<i64>,
) -> i64 {
    let original_highest = bounded_context_events
        .iter()
        .map(|e| e.sequence)
        .max()
        .unwrap_or_else(|| snapshot_as_of.unwrap_or(-1));
    covered_through.map_or(original_highest, |c| c.max(original_highest))
}

/// The re-checks of every command in `batch` whose read is older than
/// the lock, as one tag query (docs/architecture.md §196): over the union
/// of their consistency tags, from the lowest position any of them starts
/// at, up to `locked_highest`. Each command then keeps its own part in
/// memory - the events past its own position matching its own tags -
/// which is exactly what its own query would have returned, through the
/// same tag index. `None` when no command needs a re-check. A command
/// whose query reads nothing needs none (docs/architecture.md §201), and
/// its tags would only widen the query.
///
/// The tag list is padded, repeating its last tag, to a power of two: the
/// query's text has one `OR` term per tag, and a batch's union varies in
/// size, so unpadded it would be a new prepared statement for nearly
/// every batch.
pub(super) async fn batch_recheck_delta(
    conn: &mut sqlx::PgConnection,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    batch: &[BatchedCommand],
    locked_highest: i64,
) -> crate::error::Result<Option<Vec<Event>>> {
    let mut from: Option<i64> = None;
    let mut tags: Vec<Tag> = Vec::new();
    for item in batch {
        let covered = recheck_from(
            &item.bounded_context_events,
            item.snapshot.as_ref().map(|s| s.as_of_sequence),
            item.covered_through,
        );
        let query = command_consistency_query(dispatcher, &item.command_type, &item.payload)?;
        if covered >= locked_highest || crate::event_store::query_reads_nothing(&query) {
            continue;
        }
        for tag in item
            .consistency_tags
            .iter()
            .filter(|tag| !query.is_empty() || tag.value.is_some())
        {
            from = Some(from.map_or(covered, |f| f.min(covered)));
            if !tags.contains(tag) {
                tags.push(tag.clone());
            }
        }
    }
    let (Some(from), Some(first)) = (from, batch.first()) else {
        return Ok(None);
    };
    let padded = tags.len().next_power_of_two();
    let last = tags
        .last()
        .cloned()
        .expect("a command needing a re-check has tags");
    tags.resize(padded, last);
    let mut known = std::collections::HashMap::new();
    for item in batch {
        for (name, event_type) in &item.event_types_by_name {
            known
                .entry(name.clone())
                .or_insert_with(|| event_type.clone());
        }
    }
    let started = std::time::Instant::now();
    let mut events = list_events_for_bounded_context_matching_tags_with_bc(
        conn,
        &first.command_type.bounded_context,
        &tags,
        Some(from),
        Some(&known),
        None,
    )
    .await?;
    events.retain(|e| e.sequence <= locked_highest);
    tracing::debug!(
        bounded_context = %first.command_type.bounded_context.name,
        delta_query_us = started.elapsed().as_micros(),
        delta_rows = events.len(),
        "command decide delta query"
    );
    Ok(Some(events))
}

/// The highest position any of `states` holds its value's events
/// through: a snapshot item's part of a command's consistency boundary
/// (docs/architecture.md §202). `None` when there are no states, or none
/// holds an event.
pub(super) fn states_boundary(states: &[crate::plugin::SnapshotState]) -> Option<i64> {
    states
        .iter()
        .map(|state| state.folded_through)
        .filter(|position| *position >= 0)
        .max()
}

/// Idempotency keys claimed by commands earlier in the same batch, by
/// `(command type, client id, key)`, with the sequences their events got
/// - see [`decide_command_in_tx`].
pub(super) type BatchIdempotencyKeys =
    std::collections::HashMap<(String, String, String), Vec<i64>>;

/// What [`decide_command_in_tx`] settles on for a command whose decision
/// was `Accepted` - everything [`finish_accepted_command_in_tx`] needs to
/// actually write it, once sequence numbers are available. Kept as its
/// own struct, rather than inlining decide's tail into finish, is what
/// lets `commit_command_batch`'s own loop insert a [`SequencePool`] draw
/// in between the two calls.
pub(super) struct AcceptedDecision {
    pub(super) event_specs: Vec<crate::shared::EventSpec>,
    /// The command's consistency boundary: the highest sequence among the
    /// events its final decision saw that match its consistency tags
    /// (docs/architecture.md §196).
    pub(super) consistency_boundary: Option<i64>,
    /// The command type's own query, derived from the payload - recorded
    /// on the command (docs/architecture.md §198).
    pub(super) consistency_query: Vec<QueryItem>,
    pub(super) event_types_by_name: std::collections::HashMap<String, EventType>,
    /// What the command's events' blind tags are hashed with - the
    /// dispatcher's, carried to where they're derived.
    pub(super) blind: crate::blind::BlindIndex,
    /// The tags of the retractions and doubts among `event_specs`, by index
    /// (docs/architecture.md §235, §236), replacing what their mappings derive.
    pub(super) stance_tags: StanceTags,
}

/// Which of a decision's events say something about an earlier event, and the
/// tags each gets.
pub(super) type StanceTags = Vec<(usize, Vec<Tag>)>;

/// Why a decision's retraction or doubt can't be appended, a typed rejection
/// like any other: `(reason, kind)`.
type StanceRefusal = (String, &'static str);

/// A retraction, a doubt or a lift, as found among a decision's events.
enum Stance {
    Retraction(crate::retraction::RetractionPayload),
    Doubt(crate::retraction::DoubtPayload),
    Lift(crate::retraction::DoubtPayload),
}

impl Stance {
    /// `Some` for one of the three built-in types: `Ok` when its payload parses.
    fn of(spec: &crate::shared::EventSpec) -> Option<Result<Self, StanceRefusal>> {
        use crate::retraction::{
            DOUBTED_EVENT_TYPE, DOUBT_LIFTED_EVENT_TYPE, RETRACTED_EVENT_TYPE,
        };
        let payload = spec.payload.clone();
        match spec.event_type.as_str() {
            RETRACTED_EVENT_TYPE => Some(
                serde_json::from_value(payload)
                    .map(Stance::Retraction)
                    .map_err(|error| (format!("not a retraction: {error}"), "invalid_retraction")),
            ),
            DOUBTED_EVENT_TYPE => Some(
                serde_json::from_value(payload)
                    .map(Stance::Doubt)
                    .map_err(|error| (format!("not a doubt: {error}"), "invalid_doubt")),
            ),
            DOUBT_LIFTED_EVENT_TYPE => Some(
                serde_json::from_value(payload)
                    .map(Stance::Lift)
                    .map_err(|error| (format!("not a doubt: {error}"), "invalid_doubt")),
            ),
            _ => None,
        }
    }

    fn position(&self) -> i64 {
        match self {
            Stance::Retraction(retraction) => retraction.position,
            Stance::Doubt(doubt) | Stance::Lift(doubt) => doubt.position,
        }
    }

    fn is_retraction(&self) -> bool {
        matches!(self, Stance::Retraction(_))
    }
}

/// Whether `position` has been retracted: earlier in this decision, in this
/// batch, or in the store.
async fn is_retracted(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    extra_committed_events: &[Event],
    retracted_here: &std::collections::HashSet<i64>,
    position: i64,
) -> crate::error::Result<bool> {
    use crate::retraction::{RETRACTED_EVENT_TYPE, RETRACTS_TAG_KEY};
    if retracted_here.contains(&position) {
        return Ok(true);
    }
    let position_tag = position.to_string();
    if extra_committed_events.iter().any(|event| {
        event.event_type.name == RETRACTED_EVENT_TYPE
            && event.tags.iter().any(|tag| {
                tag.key == RETRACTS_TAG_KEY && tag.value.as_deref() == Some(position_tag.as_str())
            })
    }) {
        return Ok(true);
    }
    Ok(sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(format!(
        "SELECT EXISTS (SELECT 1 FROM {schema}.event_tags \
         WHERE tag_key = $1 AND tag_value = $2 AND event_type_name = $3)"
    )))
    .bind(RETRACTS_TAG_KEY)
    .bind(&position_tag)
    .bind(RETRACTED_EVENT_TYPE)
    .fetch_one(&mut *conn)
    .await?)
}

/// Whether a doubt about `position` is open: what happened last, in this
/// decision, in this batch, or in the store. Both doubt types carry the
/// position in their [`DOUBTS_TAG_KEY`](crate::retraction::DOUBTS_TAG_KEY) tag.
async fn doubt_is_open(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    extra_committed_events: &[Event],
    doubt_here: &std::collections::HashMap<i64, bool>,
    position: i64,
) -> crate::error::Result<bool> {
    use crate::retraction::{DOUBTED_EVENT_TYPE, DOUBTS_TAG_KEY};
    if let Some(open) = doubt_here.get(&position) {
        return Ok(*open);
    }
    let position_tag = position.to_string();
    let in_batch = extra_committed_events.iter().rev().find(|event| {
        event.tags.iter().any(|tag| {
            tag.key == DOUBTS_TAG_KEY && tag.value.as_deref() == Some(position_tag.as_str())
        })
    });
    if let Some(event) = in_batch {
        return Ok(event.event_type.name == DOUBTED_EVENT_TYPE);
    }
    let latest = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name FROM {schema}.event_tags \
         WHERE tag_key = $1 AND tag_value = $2 ORDER BY sequence DESC LIMIT 1"
    )))
    .bind(DOUBTS_TAG_KEY)
    .bind(&position_tag)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(latest.as_deref() == Some(DOUBTED_EVENT_TYPE))
}

/// Checks the retractions and doubts among `event_specs` and settles what the
/// store adds to each (docs/architecture.md §235, §236): the tags of the event
/// it's about, and that event's type and owner in its payload. A read on the
/// lock holder's connection, so nothing can change an event's standing between
/// this and the append; `extra_committed_events` holds what an earlier command
/// in the batch decided and hasn't written yet. Runs on a dry run too, without
/// the lock, where it says what would have happened.
async fn resolve_stances(
    conn: &mut sqlx::PgConnection,
    command_type: &CommandType,
    consistency_tags: &[Tag],
    event_specs: &mut [crate::shared::EventSpec],
    event_types_by_name: &std::collections::HashMap<String, EventType>,
    extra_committed_events: &[Event],
) -> crate::error::Result<Result<StanceTags, StanceRefusal>> {
    use crate::retraction::{
        DOUBTED_EVENT_TYPE, DOUBTS_TAG_KEY, DOUBT_LIFTED_EVENT_TYPE, RETRACTED_EVENT_TYPE,
        RETRACTS_TAG_KEY,
    };
    let mut resolved: StanceTags = Vec::new();
    let schema = schema_ident(&command_type.bounded_context.name);
    let mut retracted_here = std::collections::HashSet::new();
    let mut doubt_here = std::collections::HashMap::new();
    for (index, spec) in event_specs.iter_mut().enumerate() {
        let Some(stance) = Stance::of(spec) else {
            continue;
        };
        // Unregistered: `process_command_at_boundary` refuses it, as any.
        let Some(stance_type) = event_types_by_name.get(&spec.event_type) else {
            continue;
        };
        let mut stance = match stance {
            Ok(stance) => stance,
            Err(refusal) => return Ok(Err(refusal)),
        };
        let position = stance.position();
        let position_tag = position.to_string();
        let target = match extra_committed_events
            .iter()
            .find(|event| event.sequence == position)
        {
            Some(event) => Some((event.event_type.name.clone(), event.tags.clone())),
            None => sqlx::query_as::<_, (String, Json<Vec<Tag>>)>(sqlx::AssertSqlSafe(format!(
                "SELECT event_type_name, tags FROM {schema}.events WHERE sequence = $1"
            )))
            .bind(position)
            .fetch_optional(&mut *conn)
            .await?
            .map(|(name, Json(tags))| (name, tags)),
        };
        let Some((target_type, target_tags)) = target else {
            return Ok(Err(if stance.is_retraction() {
                (
                    format!("no event at position {position} to retract"),
                    "retraction_target_not_found",
                )
            } else {
                (
                    format!("no event at position {position} to doubt"),
                    "doubt_target_not_found",
                )
            }));
        };
        if stance.is_retraction() && target_type == RETRACTED_EVENT_TYPE {
            return Ok(Err((
                format!(
                    "position {position} is a retraction; record the event again instead of \
                     retracting the retraction"
                ),
                "retraction_of_retraction",
            )));
        }
        if stance.is_retraction()
            && (target_type == DOUBTED_EVENT_TYPE || target_type == DOUBT_LIFTED_EVENT_TYPE)
        {
            return Ok(Err((
                format!("position {position} is a doubt; lift it instead of retracting it"),
                "retraction_of_doubt",
            )));
        }
        if !stance.is_retraction()
            && (target_type == RETRACTED_EVENT_TYPE
                || target_type == DOUBTED_EVENT_TYPE
                || target_type == DOUBT_LIFTED_EVENT_TYPE)
        {
            return Ok(Err((
                format!(
                    "position {position} says something about another event; it can't be doubted"
                ),
                "doubt_of_stance",
            )));
        }
        // A command scoped to an owner acts on that owner's events only: the
        // position is the caller's to choose, and a retraction frees a value.
        if let Some(owner_key) = &command_type.owner_tag_key {
            let in_scope = consistency_tags
                .iter()
                .filter(|tag| &tag.key == owner_key)
                .any(|tag| target_tags.contains(tag));
            if !in_scope {
                return Ok(Err((
                    format!("the event at position {position} belongs to another {owner_key}"),
                    if stance.is_retraction() {
                        "retraction_target_out_of_scope"
                    } else {
                        "doubt_target_out_of_scope"
                    },
                )));
            }
        }
        let retracted = is_retracted(
            &mut *conn,
            &schema,
            extra_committed_events,
            &retracted_here,
            position,
        )
        .await?;
        if retracted {
            return Ok(Err(if stance.is_retraction() {
                (
                    format!("the event at position {position} is already retracted"),
                    "already_retracted",
                )
            } else {
                (
                    format!("the event at position {position} is retracted, which ended any doubt"),
                    "doubt_target_retracted",
                )
            }));
        }
        // The one owner tag an owner-scoped type is read by. An event without
        // it stays hidden from every scoped reader, failing closed rather than
        // showing a scoped reader someone else's tags.
        let owner = stance_type.owner_tag_key.as_ref().and_then(|key| {
            target_tags
                .iter()
                .find(|tag| &tag.key == key)
                .and_then(|tag| tag.value.clone())
        });
        let tag_key = match &stance {
            Stance::Retraction(_) => RETRACTS_TAG_KEY,
            Stance::Doubt(_) | Stance::Lift(_) => DOUBTS_TAG_KEY,
        };
        let payload = match &mut stance {
            Stance::Retraction(retraction) => {
                retracted_here.insert(position);
                retraction.owner = owner;
                retraction.retracted_event_type = Some(target_type);
                serde_json::to_value(&*retraction)
            }
            Stance::Doubt(doubt) | Stance::Lift(doubt) => {
                doubt.owner = owner;
                doubt.doubted_event_type = Some(target_type);
                serde_json::to_value(&*doubt)
            }
        };
        if !stance.is_retraction() {
            let open = doubt_is_open(
                &mut *conn,
                &schema,
                extra_committed_events,
                &doubt_here,
                position,
            )
            .await?;
            let opening = matches!(stance, Stance::Doubt(_));
            if opening && open {
                return Ok(Err((
                    format!("the event at position {position} is already in doubt"),
                    "already_doubted",
                )));
            }
            if !opening && !open {
                return Ok(Err((
                    format!("the event at position {position} isn't in doubt"),
                    "no_open_doubt",
                )));
            }
            doubt_here.insert(position, opening);
        }
        spec.payload = payload
            .map_err(|error| crate::event_store::Error::PayloadDecodeFailed(error.to_string()))?;
        let mut tags = target_tags;
        tags.push(Tag {
            key: tag_key.to_string(),
            value: Some(position_tag),
        });
        resolved.push((index, tags));
    }
    Ok(Ok(resolved))
}

/// Gives a command's events the tags [`resolve_stances`] settled on. Events are
/// built one per spec, in order.
fn apply_stance_tags(events: &mut [Event], stance_tags: StanceTags) {
    for (index, tags) in stance_tags {
        if let Some(event) = events.get_mut(index) {
            event.tags = tags;
        }
    }
}

/// [`decide_command_in_tx`]'s own return type - either a terminal outcome
/// (`Rejected`/`Deduplicated`) that never needs a sequence number at all,
/// or an [`AcceptedDecision`] still needing [`finish_accepted_command_in_tx`]
/// to actually persist it.
pub(super) enum DecideOutcome {
    Terminal(SubmitCommandOutcome),
    Accepted(AcceptedDecision),
}

/// The read-only, no-sequence-number-needed half of what used to be one
/// `submit_one_command_in_tx` - split out (issue #32, round
/// three: shrinking a batch's own per-command round trips further, after
/// round two's group commit already amortised the lock acquisition
/// itself) so `commit_command_batch`'s loop can run this directly
/// against the *leader's own* `tx`, not a per-command `SAVEPOINT`. That
/// matters for exactly one reason: it lets a [`SequencePool`] reservation -
/// acquired against that same `tx`, in the gap between this call and
/// [`finish_accepted_command_in_tx`]'s own `SAVEPOINT` - survive a later
/// command's real failure instead of being rolled back with it, which is
/// what makes pooling sequence numbers across several commands safe at
/// all. See `SequencePool`'s own doc comment for the full reasoning.
///
/// Never touches `next_sequence_batch`, `resolve_encryption_keys`,
/// `process_command`, or any insert - everything here is either a pure
/// function or a read on `conn` - the lock holder's own connection, never
/// a second pooled one (docs/architecture.md §117) - so running it
/// directly on a long-lived `tx` shared by many commands, rather than
/// inside its own disposable transaction, changes nothing about what it
/// can safely see or do. A decider error is a Rust `Err` and stays
/// per-command; a failing read means the connection itself is in
/// trouble, which fails the batch - see `commit_command_batch`'s own
/// comment.
///
/// `event_types_by_name` arrives already warmed up (see
/// `warm_up_event_types_and_encryption_keys`) and is grown in place for
/// whatever a redispatch still needs - identical to how `submit_command`
/// itself used to do this inline.
#[allow(clippy::too_many_arguments)]
pub(super) async fn decide_command_in_tx(
    conn: &mut sqlx::PgConnection,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    bounded_context_events: &[Event],
    consistency_tags: &[Tag],
    matching_events: &[Event],
    initial_decision: crate::shared::CommandDecision,
    snapshot: Option<SnapshotContext<'_>>,
    snapshot_states: &[crate::plugin::SnapshotState],
    idempotency_key: Option<&str>,
    locked_highest: i64,
    extra_committed_events: &[Event],
    mut event_types_by_name: std::collections::HashMap<String, EventType>,
    covered_through: Option<i64>,
    batch_idempotency_keys: Option<&BatchIdempotencyKeys>,
    batch_delta: Option<&[Event]>,
    slow: Option<&crate::slow_command::SlowCommandWarnings>,
    context: &crate::plugin::DecisionContext,
) -> crate::error::Result<DecideOutcome> {
    let bounded_context_name = command_type.bounded_context.name.clone();
    let schema = schema_ident(&bounded_context_name);

    // issue #12: a cached prior answer, not a new decision -
    // checked as early as possible, right after the lock that makes this
    // race-free (see `lookup_idempotency_key`'s own doc comment).
    // `initial_decision`/the redispatch logic below never runs on a hit -
    // nothing is written, and this command never reaches
    // `finish_accepted_command_in_tx` at all.
    //
    // Read on the lock holder's own connection (docs/architecture.md
    // §117), so it also sees a key an earlier command in the same,
    // still-uncommitted batch claimed: that command's savepoint was
    // released into this same transaction.
    if let Some(key) = idempotency_key {
        // A key an earlier command in this batch claimed, when the batch
        // decides everything before writing anything (docs/architecture.md
        // §196) - not in the table yet.
        let claimed = batch_idempotency_keys.and_then(|claimed| {
            claimed
                .get(&(
                    command_type.name.clone(),
                    client_id.to_string(),
                    key.to_string(),
                ))
                .cloned()
        });
        let triggered = match claimed {
            Some(sequences) => Some(sequences),
            None => {
                lookup_idempotency_key(&mut *conn, &schema, &command_type.name, client_id, key)
                    .await?
            }
        };
        if let Some(triggered_event_sequences) = triggered {
            return Ok(DecideOutcome::Terminal(
                SubmitCommandOutcome::Deduplicated {
                    triggered_event_sequences,
                },
            ));
        }
    }

    // docs/architecture.md §209: the command's tags, and the history read
    // with them, were derived before the lock. If a key rotation of this
    // bounded context committed since, they are of the old generation and
    // match nothing stored: derive them again now that nothing can rotate,
    // and send the command round once more if they changed. Compared per
    // command, not per refresh - an earlier command, in this batch or on
    // another task, may have refreshed the generations already. A query's
    // blind keys are among the command's own tags (`build()` checks it), so
    // these cover it too.
    let blind = dispatcher.blind_index();
    if blind.declares_any(&bounded_context_name) {
        refresh_blind_generations(&mut *conn, &bounded_context_name, &blind).await?;
        let current = crate::event_store::derive_tags(
            &command_type.tag_mappings,
            payload,
            &blind,
            &bounded_context_name,
        )?;
        if current != consistency_tags {
            return Err(crate::blind::Error::Rotated {
                bounded_context: bounded_context_name,
            }
            .into());
        }
    }

    let mut final_decision = initial_decision;
    // docs/architecture.md §198: what the command reads and conflicts on.
    // Without a query of its own, one item per consistency tag - every
    // event carrying any of them, as before.
    let consistency_query = command_consistency_query(dispatcher, command_type, payload)?;
    let query = crate::event_store::effective_query(&consistency_query, consistency_tags);
    // docs/architecture.md §196: no copy of the history unless a conflict
    // below needs an extended one. The boundary is a plain maximum, raised
    // by the delta's.
    let mut consistency_boundary =
        crate::event_store::query_boundary(&query, bounded_context_events)
            .max(states_boundary(snapshot_states));
    // issue #7: `None` is the caller's own pre-lock set,
    // replaced below only if a DCB conflict forced a redispatch -
    // whichever one actually produced `final_decision` is the one a
    // rejection reports (see `SubmitCommandOutcome::Rejected`'s own doc
    // comment). Copied only for a rejection.
    let mut final_matching_events: Option<Vec<Event>> = None;

    // Something committed between the caller's own optimistic read and
    // this lock - either before the lock (`locked_highest >
    // original_highest`, queried up to `locked_highest`) or by an earlier
    // command in this same batch (`extra_committed_events`, kept in
    // memory so a batch doesn't query once per command for it). Only a
    // match on our own consistency_tags is an actual DCB conflict; see
    // docs/architecture.md §19's "Problem 1" fix for why the query is
    // tag-indexed rather than an unfiltered range scan.
    //
    // It starts where the optimistic read is complete through, when the
    // caller knows that, else at the highest event the read holds. Not
    // just the latter: for a tag with no recent events that is far back,
    // and the query below walked the bounded context's history from there
    // for every command, under the lock (docs/architecture.md §178). The
    // larger of the two: a read from Postgres can hold an event above the
    // position it reports (it saw a later commit), and nothing it holds
    // must come back as new.
    let covered = recheck_from(
        bounded_context_events,
        snapshot.as_ref().map(|s| s.as_of_sequence),
        covered_through,
    );
    // The batch's tag query is over the commands' consistency tags, from
    // the command type as stored. It holds every event matching an item
    // only when the item has one of those tags: an event matching the item
    // carries all of the item's tags. An item with no tags, or with tags
    // the stored type no longer maps - a newer instance re-registered it
    // during a rolling deploy, or an admin did - isn't covered, and such a
    // command re-checks with a query of its own (docs/architecture.md
    // §200).
    let batch_delta = batch_delta.filter(|_| {
        query
            .iter()
            .all(|item| item.tags.iter().any(|tag| consistency_tags.contains(tag)))
    });
    let mut delta = if let Some(batch_delta) = batch_delta {
        // docs/architecture.md §196: the batch read every command's
        // re-check range in one query, over all their tags - this
        // command's part of it.
        batch_delta
            .iter()
            .filter(|e| {
                e.sequence > covered
                    && e.sequence <= locked_highest
                    && crate::event_store::query_matches(&query, e)
            })
            .cloned()
            .collect()
    } else if crate::event_store::query_reads_nothing(&consistency_query) {
        // docs/architecture.md §201: no event can match, so none conflicts.
        Vec::new()
    } else if locked_highest > covered && !consistency_query.is_empty() {
        // Every event matching any item counts, however many matches the
        // item keeps: a newer one displaces the oldest the command
        // decided on.
        let every_match: Vec<QueryItem> = query
            .iter()
            .map(|item| QueryItem {
                latest: None,
                ..item.clone()
            })
            .collect();
        let mut result = list_events_matching_query_with_bc(
            &mut *conn,
            &command_type.bounded_context,
            &every_match,
            Some(covered),
            Some(&event_types_by_name),
        )
        .await?;
        result.retain(|e| e.sequence <= locked_highest);
        result
    } else if locked_highest > covered {
        // `command_type.bounded_context` is already this exact row -
        // every command a `CommandBatcher` batch ever holds shares one
        // bounded context (the queue is keyed on it), so there is never
        // a fresher copy to fetch here. See
        // `list_events_for_bounded_context_matching_tags_with_bc`'s own
        // doc comment for why re-fetching it per command was pure
        // round-trip waste inside the batch leader's held lock.
        //
        // Timed separately (issue #32, round four investigation)
        // - `commit_command_batch`'s own per-batch `decide_us` total was
        // far higher than CPU-only decide work should cost; this pins
        // down whether this specific query, firing whenever a busy
        // batch's fixed `locked_highest` has outrun some individual
        // command's own pre-lock snapshot (which real concurrent load
        // makes the common case, not the rare-conflict case this branch
        // was written for), is where that time actually goes.
        let delta_query_started = std::time::Instant::now();
        let mut result = list_events_for_bounded_context_matching_tags_with_bc(
            &mut *conn,
            &command_type.bounded_context,
            &consistency_tags
                .iter()
                .filter(|tag| tag.value.is_some())
                .cloned()
                .collect::<Vec<_>>(),
            Some(covered),
            Some(&event_types_by_name),
            None,
        )
        .await?;
        // On the lock holder's own connection this also returns what an
        // earlier command in this batch inserted (above `locked_highest`);
        // those come from `extra_committed_events` below instead.
        result.retain(|e| e.sequence <= locked_highest);
        // `debug`: one line per command was `info` (docs/architecture.md §178).
        tracing::debug!(
            bounded_context = %bounded_context_name,
            delta_query_us = delta_query_started.elapsed().as_micros(),
            delta_rows = result.len(),
            "command decide delta query"
        );
        result
    } else {
        Vec::new()
    };
    delta.extend(
        extra_committed_events
            .iter()
            .filter(|e| e.sequence > covered && crate::event_store::query_matches(&query, e))
            .cloned(),
    );

    if delta.is_empty() {
        decision_history(
            &mut Db::Conn(conn),
            matching_events,
            dispatcher.key_service(),
        )
        .await?;
    }
    if !delta.is_empty() {
        let mut extended = Vec::with_capacity(bounded_context_events.len() + delta.len());
        extended.extend_from_slice(bounded_context_events);
        extended.extend(delta);
        extended.sort_by_key(|e| e.sequence);
        // The query selects again over the extended history: a newer
        // match displaces an item's oldest kept one when it keeps a count.
        // A snapshot item's new events are folded into its state instead
        // (docs/architecture.md §202).
        let redispatch_matching_events = crate::event_store::select_matching_events(
            &decided_on(&consistency_query, consistency_tags),
            &extended,
        );
        let mut redispatch_states = snapshot_states.to_vec();
        if !redispatch_states.is_empty() {
            let snapshots = dispatcher
                .snapshots()
                .ok_or_else(|| unregistered_snapshot(&redispatch_states[0].snapshot))?;
            // A retraction committed since the read can name an event already
            // in a state. Rebuilding it takes the value's whole history, and
            // here every other writer to the bounded context would wait for
            // that, so the command goes back to its read, before the lock,
            // which does it for nobody's time but its own
            // (docs/architecture.md §235).
            if !states_holding_retracted_events(&consistency_query, &redispatch_states, &extended)
                .is_empty()
            {
                return Err(crate::event_store::Error::SnapshotRetractedMeanwhile {
                    bounded_context: bounded_context_name,
                }
                .into());
            }
            fold_into_snapshot_states(
                snapshots,
                &bounded_context_name,
                &consistency_query,
                &mut redispatch_states,
                &extended,
            )?;
        }
        consistency_boundary = crate::event_store::query_boundary(&query, &extended)
            .max(states_boundary(&redispatch_states));
        // docs/architecture.md §19: a snapshot-accelerated initial
        // decision redispatches through `dispatch_from_snapshot`
        // again too, not the ordinary `dispatch` - the snapshot's
        // own folded prefix (`snapshot.state_json`) still represents
        // real history `redispatch_matching_events` alone doesn't
        // (it's the tag-indexed delta since the snapshot, same as
        // `bounded_context_events` already was); calling the
        // ordinary path here would silently drop everything the
        // snapshot had already folded.
        let history = decision_history(
            &mut Db::Conn(conn),
            &redispatch_matching_events,
            dispatcher.key_service(),
        )
        .await?;
        let redecide_started = std::time::Instant::now();
        final_decision = match &snapshot {
            Some(ctx) => match dispatcher.dispatch_from_snapshot(
                &bounded_context_name,
                &command_type.name,
                payload,
                ctx.state_json,
                &history,
                context,
            ) {
                None => return Err(crate::error::Error::NoDeciderRegistered),
                Some(Err(e)) => return Err(e),
                Some(Ok(d)) => d,
            },
            None => match dispatcher.dispatch_with(
                &bounded_context_name,
                &command_type.name,
                payload,
                &history,
                &redispatch_states,
                context,
            ) {
                None => return Err(crate::error::Error::NoDeciderRegistered),
                Some(Err(e)) => return Err(e),
                Some(Ok(d)) => d,
            },
        };
        // docs/architecture.md §203: this one holds up every other
        // command in the bounded context.
        let redecide_time = redecide_started.elapsed();
        tracing::debug!(
            bounded_context = %bounded_context_name,
            command_type = %command_type.name,
            events_decided_on = redispatch_matching_events.len(),
            decide_us = redecide_time.as_micros(),
            "command decide under lock"
        );
        if let Some(slow) = slow {
            slow.note_decide(
                command_type,
                redecide_time,
                redispatch_matching_events.len(),
                true,
            );
        }
        final_matching_events = Some(redispatch_matching_events);
    }

    let event_specs = match final_decision {
        crate::shared::CommandDecision::Rejected { reason, kind } => {
            COMMANDS_PROCESSED.add(
                1,
                &[
                    KeyValue::new("bounded_context", command_type.bounded_context.name.clone()),
                    KeyValue::new("command_type", command_type.name.clone()),
                    KeyValue::new("outcome", "rejected"),
                ],
            );
            return Ok(DecideOutcome::Terminal(SubmitCommandOutcome::Rejected {
                reason,
                kind,
                matching_events: final_matching_events.unwrap_or_else(|| matching_events.to_vec()),
            }));
        }
        crate::shared::CommandDecision::Accepted { events } => events,
    };
    let mut event_specs = event_specs;

    // process_command's own resolve_event_type stays a plain sync closure
    // (decide() and everything downstream is I/O-free per §1.1) - every
    // EventType lookup this call will need happens against the *final*
    // event_specs (the redispatched ones, if a retry happened above).
    // `event_types_by_name` arrived already warmed up for `initial_decision`'s
    // own specs; this loop is what a redispatch's different specs still
    // need, and its `Entry::Vacant` guard means it costs nothing extra
    // when nothing changed.
    for spec in &event_specs {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            event_types_by_name.entry(spec.event_type.clone())
        {
            if let Some(et) =
                get_event_type_with_bc(&mut *conn, &command_type.bounded_context, &spec.event_type)
                    .await?
            {
                entry.insert(et);
            }
        }
    }

    let stance_tags = match resolve_stances(
        &mut *conn,
        command_type,
        consistency_tags,
        &mut event_specs,
        &event_types_by_name,
        extra_committed_events,
    )
    .await?
    {
        Ok(tags) => tags,
        Err((reason, kind)) => {
            COMMANDS_PROCESSED.add(
                1,
                &[
                    KeyValue::new("bounded_context", command_type.bounded_context.name.clone()),
                    KeyValue::new("command_type", command_type.name.clone()),
                    KeyValue::new("outcome", "rejected"),
                ],
            );
            return Ok(DecideOutcome::Terminal(SubmitCommandOutcome::Rejected {
                reason,
                kind: kind.to_string(),
                matching_events: final_matching_events.unwrap_or_else(|| matching_events.to_vec()),
            }));
        }
    };

    Ok(DecideOutcome::Accepted(AcceptedDecision {
        event_specs,
        consistency_boundary,
        consistency_query,
        event_types_by_name,
        blind: dispatcher.blind_index(),
        stance_tags,
    }))
}

/// protect_sensitive_fields' own pre-resolution step, for the command's
/// own payload *and* every final event spec's - see
/// `resolve_encryption_keys`'s own doc comment. `resolved` arrived already
/// warmed up, before the lock, for `initial_decision`'s own payloads;
/// what's left is only what a redispatch newly needs. That is provisioned
/// on `conn`, the lock holder's own connection, not the pool
/// (docs/architecture.md §117): this transaction holds the bounded
/// context's `sequence` lock, and every other writer to it waits on that
/// lock holding a pooled connection.
///
/// Every distinct `(subject_key, subject_value)` still needed is gathered
/// up front and deduped (against itself and `resolved`), so no subject is
/// provisioned twice.
pub(super) async fn provision_missing_encryption_keys(
    conn: &mut sqlx::PgConnection,
    command_type: &CommandType,
    payload: &str,
    event_specs: &[crate::shared::EventSpec],
    event_types_by_name: &std::collections::HashMap<String, EventType>,
    resolved: &mut std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
    key_service: Option<&KeyService>,
) -> crate::error::Result<()> {
    let needed_subjects = missing_encryption_subjects(
        command_type,
        payload,
        event_specs,
        event_types_by_name,
        resolved,
    )?;
    provision_encryption_subjects(conn, command_type, needed_subjects, resolved, key_service).await
}

/// The `(subject_key, subject_value)` pairs `command_type`'s payload and
/// `event_specs` need a key for that `resolved` doesn't have yet - what
/// [`provision_missing_encryption_keys`] provisions.
pub(super) fn missing_encryption_subjects(
    command_type: &CommandType,
    payload: &str,
    event_specs: &[crate::shared::EventSpec],
    event_types_by_name: &std::collections::HashMap<String, EventType>,
    resolved: &std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
) -> crate::error::Result<Vec<(String, String)>> {
    let mut needed_subjects: Vec<(String, String)> =
        crate::event_store::try_sensitive_field_subjects(&command_type.sensitive_fields, payload)?;
    for spec in event_specs {
        if let Some(event_type) = event_types_by_name.get(&spec.event_type) {
            needed_subjects.extend(crate::event_store::try_sensitive_field_subjects(
                &event_type.sensitive_fields,
                &spec.payload.to_string(),
            )?);
        }
    }
    needed_subjects.retain(|pair| !resolved.contains_key(pair));
    needed_subjects.sort();
    needed_subjects.dedup();
    Ok(needed_subjects)
}

/// Provisions a key for each of `needed_subjects` on `conn` into `resolved`.
pub(super) async fn provision_encryption_subjects(
    conn: &mut sqlx::PgConnection,
    command_type: &CommandType,
    needed_subjects: Vec<(String, String)>,
    resolved: &mut std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
    key_service: Option<&KeyService>,
) -> crate::error::Result<()> {
    if !needed_subjects.is_empty() {
        let key_service = key_service.ok_or(encryption::Error::MasterKeyNotConfigured)?;
        for subject in needed_subjects {
            let provisioned = get_or_create_encryption_key_on(
                &mut *conn,
                command_type.bounded_context.clone(),
                &subject.0,
                &subject.1,
                key_service,
            )
            .await?;
            resolved.insert(subject, provisioned);
        }
    }
    Ok(())
}

/// The write half of what used to be one `submit_one_command_in_tx` -
/// everything [`decide_command_in_tx`] couldn't do without real sequence
/// numbers in hand. `sequences` is exactly `decided.event_specs.len()`
/// numbers, already allocated by the caller (a plain `next_sequence_batch`
/// call for `submit_command`'s own batch-of-one path, a shared
/// [`SequencePool`] draw for `commit_command_batch`'s multi-command one) -
/// this function itself never touches `{schema}.sequence` at all.
#[allow(clippy::too_many_arguments)]
pub(super) async fn finish_accepted_command_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    key_service: Option<&KeyService>,
    now: DateTime<Utc>,
    idempotency_key: Option<&str>,
    sync_projections: &[String],
    sequences: Vec<i64>,
    decided: AcceptedDecision,
    mut resolved: std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
    payload_limits: &crate::payload_limits::PayloadLimits,
) -> crate::error::Result<SubmitCommandOutcome> {
    let bounded_context_name = command_type.bounded_context.name.clone();
    let schema = schema_ident(&bounded_context_name);
    let AcceptedDecision {
        event_specs,
        consistency_boundary,
        consistency_query,
        event_types_by_name,
        blind,
        stance_tags,
    } = decided;
    let mut sequences = sequences.into_iter();

    provision_missing_encryption_keys(
        tx,
        command_type,
        payload,
        &event_specs,
        &event_types_by_name,
        &mut resolved,
        key_service,
    )
    .await?;

    let mut result = crate::event_store::process_command_at_boundary(
        crate::shared::try_generate_token_id()?,
        command_type,
        payload,
        client_id,
        correlation_id,
        causation_id,
        consistency_query,
        consistency_boundary,
        crate::shared::CommandDecision::Accepted {
            events: event_specs,
        },
        |name| event_types_by_name.get(name).cloned(),
        || {
            sequences.next().expect(
                "process_command called next_sequence more times than there are accepted events",
            )
        },
        now,
        |subject_key, subject_value| {
            let (key, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect(
                    "resolve_encryption_keys pre-resolved every subject sensitive_field_subjects \
                     named",
                );
            (key.clone(), data_key.clone())
        },
        &blind,
    )?;
    apply_stance_tags(&mut result.events, stance_tags);
    // As stored - after encryption - and before anything is written
    // (docs/architecture.md §238).
    payload_limits.check_command(&result.command, &result.events)?;
    // Every blob reference is to a blob this bounded context holds as
    // referenced, before anything is written (§239).
    let blob_claims = command_blob_claims(&result.command, &result.events)?;
    check_blob_claims(tx, &bounded_context_name, &blob_claims).await?;

    // Command and every one of its triggered events, in the one
    // (savepoint-scoped, in the batched path) transaction `tx` has held
    // since the lock above - DynamicConsistencyBoundaryHonoured's actual
    // enforcement: a failure partway through this loop rolls the command
    // insert back too, rather than leaving a persisted Command with only
    // some of its events.
    let command_key_ids = encryption_key_ids(&result.command.encryption_keys, &resolved);
    let command_id = insert_command(tx, &result.command, &command_key_ids).await?;
    for event in &result.events {
        let event_key_ids = encryption_key_ids(&event.encryption_keys, &resolved);
        insert_event_and_update_sync_projections_in_tx(
            tx,
            event,
            Some(command_id),
            projection_dispatcher,
            &event_key_ids,
            sync_projections,
        )
        .await?;
    }

    record_blob_claims(
        tx,
        &bounded_context_name,
        &blob_claims,
        Some(command_id),
        now,
    )
    .await?;

    if let Some(key) = idempotency_key {
        let triggered_event_sequences: Vec<i64> =
            result.events.iter().map(|e| e.sequence).collect();
        insert_idempotency_key(
            &mut **tx,
            &schema,
            &command_type.name,
            client_id,
            key,
            &triggered_event_sequences,
            now,
        )
        .await?;
    }

    COMMANDS_PROCESSED.add(
        1,
        &[
            KeyValue::new("bounded_context", command_type.bounded_context.name.clone()),
            KeyValue::new("command_type", command_type.name.clone()),
            KeyValue::new("outcome", "accepted"),
        ],
    );

    Ok(SubmitCommandOutcome::Accepted {
        command: Box::new(result.command),
        events: result.events,
    })
}

/// `submit_command`'s own batch-of-one composition of [`decide_command_in_tx`]
/// and [`finish_accepted_command_in_tx`], with a plain per-call
/// `next_sequence_batch` in between. Unlike `commit_command_batch`'s own
/// loop, there is only ever one command here, so there is nothing to
/// pool sequence numbers *across* - both halves run on the same flat
/// `tx` `submit_command` opened, with no per-command `SAVEPOINT` layered
/// on top of it at all, so a real failure in either half rolls back that
/// one, whole transaction, exactly as before this function was split in
/// two.
#[allow(clippy::too_many_arguments)]
pub(super) async fn submit_one_command_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    bounded_context_events: &[Event],
    consistency_tags: &[Tag],
    matching_events: &[Event],
    initial_decision: crate::shared::CommandDecision,
    key_service: Option<&KeyService>,
    now: DateTime<Utc>,
    snapshot: Option<SnapshotContext<'_>>,
    snapshot_states: &[crate::plugin::SnapshotState],
    idempotency_key: Option<&str>,
    locked_highest: i64,
    sync_projections: &[String],
    extra_committed_events: &[Event],
    event_types_by_name: std::collections::HashMap<String, EventType>,
    resolved: std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
    covered_through: Option<i64>,
    context: &crate::plugin::DecisionContext,
    payload_limits: &crate::payload_limits::PayloadLimits,
) -> crate::error::Result<SubmitCommandOutcome> {
    let bounded_context_name = command_type.bounded_context.name.clone();
    let decided = match decide_command_in_tx(
        tx,
        dispatcher,
        command_type,
        payload,
        client_id,
        bounded_context_events,
        consistency_tags,
        matching_events,
        initial_decision,
        snapshot,
        snapshot_states,
        idempotency_key,
        locked_highest,
        extra_committed_events,
        event_types_by_name,
        covered_through,
        None,
        None,
        None,
        context,
    )
    .await?
    {
        DecideOutcome::Terminal(outcome) => return Ok(outcome),
        DecideOutcome::Accepted(decided) => decided,
    };

    // Allocated inside `tx`, after the lock above, in one round trip for
    // every event this command decided - `next_sequence_batch`, not
    // `event_specs.len()` separate `next_sequence` calls (issue
    // #32). A failure anywhere below (an unregistered event type,
    // encryption resolution, the inserts themselves) rolls the whole
    // allocation back with the rest of this command's own work - the
    // whole transaction, since this path has no per-command `SAVEPOINT`
    // of its own.
    let sequences = next_sequence_batch(
        &mut **tx,
        &bounded_context_name,
        decided.event_specs.len() as i64,
    )
    .await?;

    finish_accepted_command_in_tx(
        tx,
        projection_dispatcher,
        command_type,
        payload,
        client_id,
        correlation_id,
        causation_id,
        key_service,
        now,
        idempotency_key,
        sync_projections,
        sequences,
        decided,
        resolved,
        payload_limits,
    )
    .await
}

/// The owned equivalent of [`SnapshotContext`] - that type borrows
/// `state_json`, fine for a caller whose own stack frame outlives the
/// call it's passed into, but a [`BatchedCommand`] has to survive being
/// handed to a *different* task (`command_batcher::CommandBatcher`'s
/// batch leader, which may be a different concurrently-running request's
/// own task entirely) - see `BatchedCommand`'s own doc comment.
#[derive(Debug, Clone)]
pub struct OwnedSnapshotContext {
    pub state_json: String,
    pub as_of_sequence: i64,
}

/// One command's worth of everything `submit_one_command_in_tx` needs,
/// entirely owned rather than borrowed - what a request becomes once it
/// has to survive being queued for, and processed by, some other task
/// entirely (`command_batcher::CommandBatcher`'s batch leader is
/// whichever concurrently-submitting caller happened to arrive first;
/// every other command in its batch is, from the leader's own stack
/// frame's perspective, a different task's data). The shared collaborators
/// [`submit_command`]'s own signature also takes - `pool`, `dispatcher`,
/// `projection_dispatcher`, `key_service` - are deliberately
/// *not* part of this struct: within one process they are the same
/// `Arc`/reference for every command a `CommandBatcher` ever processes
/// (all ultimately sourced from the one `Skilj` instance), so the batch
/// leader already has its own copy and simply reuses it for every command
/// in the batch rather than each one carrying a redundant copy.
pub struct BatchedCommand {
    pub command_type: CommandType,
    pub payload: String,
    pub client_id: String,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub bounded_context_events: Vec<Event>,
    /// See `ResolvedCommandSubmission::covered_through`; `None` for a
    /// caller that doesn't know it, whose re-check then starts from the
    /// highest event it does hold.
    pub covered_through: Option<i64>,
    pub consistency_tags: Vec<Tag>,
    pub matching_events: Vec<Event>,
    pub initial_decision: crate::shared::CommandDecision,
    pub now: DateTime<Utc>,
    pub snapshot: Option<OwnedSnapshotContext>,
    /// See `ResolvedCommandSubmission::snapshot_states`
    /// (docs/architecture.md §202).
    pub snapshot_states: Vec<crate::plugin::SnapshotState>,
    pub idempotency_key: Option<String>,
    /// Pre-lock-warmed by this command's own original caller, via
    /// `warm_up_event_types_and_encryption_keys`, before it ever joined a
    /// batch queue - see that function's own doc comment for why this is
    /// always safe and never a correctness dependency.
    pub event_types_by_name: std::collections::HashMap<String, EventType>,
    pub resolved: std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
}

/// What one command in a [`submit_command_batch`] call settles on - the
/// same [`SubmitCommandOutcome`] a standalone `submit_command` call would
/// have produced for it, had it run alone; `Err` only for a real failure
/// (an unregistered event type, a database error) that rolled back just
/// this one command's own savepoint, not its batch-mates'.
pub type BatchedCommandResult = crate::error::Result<SubmitCommandOutcome>;

/// What [`begin_command_batch_leader_tx`] hands to [`commit_command_batch`] -
/// see each of their own doc comments for why acquiring the lock and
/// running the batch are split into two functions at all (issue
/// #36).
pub struct CommandBatchLeaderTx {
    pub(super) tx: Transaction<'static, Postgres>,
    pub(super) bounded_context_name: String,
    pub(super) locked_highest: i64,
    pub(super) sync_projections: Vec<String>,
    /// Read with the lock (docs/architecture.md §238).
    pub(super) payload_limits: PayloadLimitsOverride,
}

/// The lock-acquiring half of [`submit_command_batch`], pulled out
/// (issue #36) so `command_batcher::CommandBatcher::run_as_leader`
/// can await this - the one part of the batch's own processing whose
/// duration is meant to grow the batch, per this module's own doc comment
/// on the self-tuning design - *before* draining the queue, not after.
/// The previous, unsplit `submit_command_batch` made `run_as_leader` drain
/// first and lock second, which defeated that self-tuning window (a batch
/// almost never grew past whoever raced to become leader) and, worse,
/// meant that if this step alone hung, the caller had no chance to
/// give every already-queued follower a real answer, since draining
/// happened somewhere the caller no longer controlled once this
/// combined function was already inside it.
///
/// `idle_in_transaction_session_timeout`, when `Some`, is issued as a
/// `SET LOCAL` on this transaction's own connection right after opening
/// it, before the lock wait - a defense-in-depth backstop (issue
/// #36's own recommendation #3): if this transaction's connection ever
/// does stall indefinitely - a stuck decider, a hung downstream await,
/// anything that leaves it holding the bounded-context lock without
/// making forward progress - Postgres itself kills the idle session
/// after the timeout, releasing the lock so a later batch can proceed
/// instead of every writer to this bounded context queueing forever
/// behind a wedge no automatic mechanism ever clears. `SET LOCAL` scopes
/// the change to this one transaction; it's gone the moment `tx` commits,
/// rolls back, or (if the timeout itself fires) Postgres closes it, so it
/// never leaks onto whatever this pooled connection is handed to next.
///
/// What the timeout does and doesn't measure: Postgres's idle clock only
/// runs *between* statements on this session, and restarts after each
/// one - so it bounds the gap between two consecutive statements (one
/// command's in-memory decide, or a run of consecutive rejected commands
/// that issue none), never the whole batch. The blocked
/// `SELECT ... FOR UPDATE` lock wait is an *active* statement, so it is
/// not covered (nor meant to be): the timeout only protects the phase
/// after the lock is held.
pub async fn begin_command_batch_leader_tx(
    pool: &Pool,
    bounded_context_name: &str,
    idle_in_transaction_session_timeout: Option<std::time::Duration>,
) -> crate::error::Result<CommandBatchLeaderTx> {
    let mut tx = pool.begin().await?;
    // The timeout rides on the lock statement itself, as `set_config(...,
    // true)` - `SET LOCAL`'s function form - rather than a statement of
    // its own (docs/architecture.md §196). Evaluated as the row is read,
    // possibly before the lock wait ends, which changes nothing: the
    // timeout only counts time the session is idle, never a statement
    // waiting on a lock.
    let schema = schema_ident(bounded_context_name);
    let (locked_highest, payload_limits) = match idle_in_transaction_session_timeout {
        Some(timeout) => {
            let (locked_highest, max_payload_bytes, warn_payload_bytes): (
                i64,
                Option<i64>,
                Option<i64>,
            ) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT next_value, {PAYLOAD_LIMIT_COLUMNS} FROM {schema}.sequence s \
                 WHERE set_config('idle_in_transaction_session_timeout', $1, true) IS NOT NULL \
                 FOR UPDATE"
            )))
            .bind(format!("{}ms", timeout.as_millis()))
            .fetch_one(&mut *tx)
            .await?;
            (
                locked_highest,
                PayloadLimitsOverride {
                    max_payload_bytes,
                    warn_payload_bytes,
                },
            )
        }
        None => lock_bounded_context_sequence_with_limits(&mut tx, bounded_context_name).await?,
    };
    let sync_projections = sync_projection_names(&mut tx, bounded_context_name).await?;
    Ok(CommandBatchLeaderTx {
        tx,
        bounded_context_name: bounded_context_name.to_string(),
        locked_highest,
        sync_projections,
        payload_limits,
    })
}

/// A prefetched, batch-local block of not-yet-consumed sequence numbers -
/// `commit_command_batch`'s own answer to `next_sequence_batch` being
/// called once per command instead of once per batch (issue #32,
/// round three). Reserves numbers in chunks against the *leader's own*
/// `tx`, never a per-command `SAVEPOINT` - specifically so a reservation
/// survives a later command's real failure instead of being rolled back
/// with it: today's per-command `next_sequence_batch` call is what
/// currently makes gapless sequencing safe for a command that fails
/// partway through (its own `SAVEPOINT` rollback un-reserves exactly what
/// it asked for); pooling numbers across several commands means a
/// command can end up holding numbers it never uses (a *later* command in
/// the same batch is the one that fails, or the pool simply over-reserved
/// as a heuristic), and those must still never be lost or duplicated.
///
/// `SequenceIsGaplessPerBoundedContext` stays honoured by two rules,
/// together: (1) a draw's numbers are only ever permanently discarded
/// ([`Self::confirm_last_draw`]) once the command that used them has
/// actually committed; (2) a draw that instead fails
/// ([`Self::return_last_draw`]) goes back onto the *front* of the queue,
/// in its original order, so it is the very next thing handed to
/// whichever command asks next - recycled, never wasted mid-batch. Only
/// genuinely unused numbers - ones nobody ever drew, or ones returned and
/// never redrawn because the batch ended first - are corrected back out
/// of `{schema}.sequence` in one shot ([`Self::shrink_back`]), right
/// before `tx.commit()`. Because every draw is either confirmed or
/// returned before the next one starts (this pool is only ever driven by
/// `commit_command_batch`'s own strictly sequential loop, never
/// concurrently), the persisted `next_value` this leaves behind is always
/// identical to what calling `next_sequence_batch` once per command,
/// inside each one's own `SAVEPOINT`, would have left - only the number
/// of round trips to get there differs.
pub(super) struct SequencePool {
    pub(super) reserved: std::collections::VecDeque<i64>,
    pub(super) last_draw: Vec<i64>,
}

impl SequencePool {
    pub(super) fn new() -> Self {
        Self {
            reserved: std::collections::VecDeque::new(),
            last_draw: Vec::new(),
        }
    }

    /// Pops `count` sequence numbers, refilling from `tx` first if the
    /// pool doesn't already have enough. `refill_hint` (`commit_command_batch`
    /// passes "how many commands are still left in this batch, this one
    /// included") sizes that refill generously so a run of small,
    /// single-event commands shares one round trip instead of paying for
    /// one each; `refill_hint.max(count)` guarantees the refill is always
    /// big enough for the command that triggered it, regardless of how
    /// small a hint the caller passed. Remembers exactly what it handed
    /// out as `last_draw`, so the caller can later call exactly one of
    /// [`Self::confirm_last_draw`] or [`Self::return_last_draw`] once
    /// that command's own outcome is known - never both, never neither.
    pub(super) async fn take(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
        bounded_context: &str,
        count: i64,
        refill_hint: i64,
    ) -> crate::error::Result<Vec<i64>> {
        if (self.reserved.len() as i64) < count {
            let refill =
                next_sequence_batch(&mut **tx, bounded_context, refill_hint.max(count)).await?;
            self.reserved.extend(refill);
        }
        let drawn: Vec<i64> = (0..count)
            .map(|_| {
                self.reserved
                    .pop_front()
                    .expect("just topped up the pool to at least `count`")
            })
            .collect();
        self.last_draw = drawn.clone();
        Ok(drawn)
    }

    /// The command that drew `last_draw` committed successfully - those
    /// numbers are truly spent, never returned to the pool.
    pub(super) fn confirm_last_draw(&mut self) {
        self.last_draw.clear();
    }

    /// The command that drew `last_draw` failed - those numbers were
    /// never actually used by any inserted event, so they go back onto
    /// the *front* of the queue (preserving their original order) for
    /// whichever command draws next, instead of being permanently lost.
    pub(super) fn return_last_draw(&mut self) {
        for value in self.last_draw.drain(..).rev() {
            self.reserved.push_front(value);
        }
    }

    /// Gives back whatever's left reserved-but-never-confirmed once the
    /// batch's own loop is done - see this struct's own doc comment for
    /// why this one corrective `UPDATE`, run once per batch, is all
    /// `SequenceIsGaplessPerBoundedContext` needs, regardless of how many
    /// refills happened or how many draws were returned and re-drawn
    /// along the way.
    pub(super) async fn shrink_back(
        self,
        tx: &mut Transaction<'_, Postgres>,
        bounded_context: &str,
    ) -> crate::error::Result<()> {
        debug_assert!(
            self.last_draw.is_empty(),
            "every draw must be confirmed or returned before shrink_back runs"
        );
        let leftover = self.reserved.len() as i64;
        if leftover > 0 {
            let schema = schema_ident(bounded_context);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.sequence SET next_value = next_value - $1"
            )))
            .bind(leftover)
            .execute(&mut **tx)
            .await?;
        }
        Ok(())
    }
}

/// The batched form of the "locked half" of `ProcessCommand` -
/// `submit_command`'s own doc comment describes the single-command
/// shape this generalises. Takes the already-locked transaction
/// [`begin_command_batch_leader_tx`] produced and runs every command in
/// `batch`, *in order*, each inside its own nested transaction
/// (`tx.begin()`, which sqlx backs with a real Postgres `SAVEPOINT` for a
/// `Transaction` that is itself already inside one) - so one command's
/// real failure (as opposed to an ordinary `Rejected` decision, which is
/// never an `Err` at all) rolls back only its own work, `ROLLBACK TO
/// SAVEPOINT`, not the whole batch's. Every command that succeeds is
/// `RELEASE SAVEPOINT`d immediately, keeping its writes visible to
/// whichever command in the batch runs next (see `extra_committed_events`
/// below) without making them durable yet - that's still `tx.commit()`,
/// called exactly once, after the whole batch has been processed.
///
/// This is what makes batching commands from *different*, concurrently-
/// submitting callers into one lock acquisition possible at all
/// (issue #32, round two - see `command_batcher`'s own module
/// doc comment for the throughput problem this exists to close): the
/// lock `leader_tx` already holds is held for the combined work of every
/// command in `batch`, not re-acquired per command, so N commands that
/// would previously have queued for N separate lock acquisitions instead
/// share one.
///
/// `extra_committed_events` threading: `submit_one_command_in_tx`'s own
/// DCB-conflict redispatch check needs to see events *this same batch*
/// already produced, not just ones committed before the lock. This loop
/// accumulates every accepted command's own `events` in memory as it
/// goes and hands the running total to each subsequent command, so the
/// check needs no query of its own for them.
///
/// Returns one [`BatchedCommandResult`] per input command, same order,
/// only once `tx.commit()` has actually succeeded - nothing here is
/// reported back to any caller as durable before it truly is. A failure
/// committing at the end is returned as the outer `Err` instead - at that
/// point no per-command outcome is trustworthy (a failed commit could
/// mean anything committed or nothing did), so the caller
/// (`command_batcher::CommandBatcher`) is expected to treat every command
/// in the batch as failed identically, not to guess from whatever this
/// function got partway through computing.
#[tracing::instrument(skip_all, fields(batch_size = batch.len()))]
pub async fn commit_command_batch(
    leader_tx: CommandBatchLeaderTx,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    key_service: Option<&KeyService>,
    batch: Vec<BatchedCommand>,
    slow: Option<&crate::slow_command::SlowCommandWarnings>,
) -> crate::error::Result<Vec<BatchedCommandResult>> {
    let CommandBatchLeaderTx {
        mut tx,
        bounded_context_name,
        locked_highest,
        sync_projections,
        payload_limits,
    } = leader_tx;
    let payload_limits = payload_limits.apply(projection_dispatcher.payload_limits());

    COMMAND_BATCH_SIZE.record(
        batch.len() as u64,
        &[KeyValue::new(
            "bounded_context",
            bounded_context_name.clone(),
        )],
    );
    // A plain log line alongside the histogram above - purely so a load
    // test can grep achieved batch sizes straight out of server logs
    // without standing up an OTLP receiver, per
    // docs/load-test-report-2026-09-18*.md's own "skipped OTel/Grafana"
    // reasoning. The histogram is the lasting instrument; this is the
    // zero-infra way to read it for one investigation.
    tracing::debug!(
        bounded_context = %bounded_context_name,
        batch_size = batch.len(),
        "command batch committed"
    );

    let batch_len = batch.len();
    // docs/architecture.md §196: decided and written as a set first, in a
    // savepoint. Anything failing there - a statement, or a sync
    // projection refusing an event - rolls the savepoint back and the
    // batch is replayed one command at a time, each in its own savepoint,
    // which isolates the failure to its command as before.
    let mut savepoint = AsSetSavepoint::default();
    let as_set = match write_command_batch_as_set(
        &mut tx,
        &mut savepoint,
        dispatcher,
        projection_dispatcher,
        key_service,
        &batch,
        locked_highest,
        &sync_projections,
        slow,
        &payload_limits,
    )
    .await
    {
        Ok(written) => Some(written),
        Err(e) => {
            tracing::debug!(
                bounded_context = %bounded_context_name,
                batch_size = batch_len,
                error = %e,
                "command batch not written as a set - replaying it one command at a time"
            );
            if savepoint.open {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "ROLLBACK TO SAVEPOINT {AS_SET_SAVEPOINT}"
                )))
                .execute(&mut *tx)
                .await?;
            }
            None
        }
    };
    let (results, timing) = match as_set {
        Some(written) => written,
        None => {
            commit_command_batch_per_command(
                &mut tx,
                dispatcher,
                projection_dispatcher,
                key_service,
                &bounded_context_name,
                locked_highest,
                &sync_projections,
                batch,
                slow,
                &payload_limits,
            )
            .await?
        }
    };

    let commit_started = std::time::Instant::now();
    tx.commit().await?;
    let commit_elapsed = commit_started.elapsed();

    tracing::debug!(
        bounded_context = %bounded_context_name,
        batch_size = batch_len,
        as_set = timing.as_set,
        decide_us = timing.decide.as_micros(),
        sequence_us = timing.sequence.as_micros(),
        persist_us = timing.persist.as_micros(),
        commit_us = commit_elapsed.as_micros(),
        "command batch phase timing"
    );

    Ok(results)
}

/// Where a batch's time under the lock went, for `commit_command_batch`'s
/// one timing line per batch (issue #32, round four).
#[derive(Default)]
pub(super) struct BatchPhaseTiming {
    /// Whether the batch was written as a set (docs/architecture.md §196)
    /// rather than one command at a time.
    pub(super) as_set: bool,
    pub(super) decide: std::time::Duration,
    pub(super) sequence: std::time::Duration,
    pub(super) persist: std::time::Duration,
}

/// The savepoint the set path writes under, so a failed write rolls back
/// to before it and the batch is replayed one command at a time.
pub(super) const AS_SET_SAVEPOINT: &str = "skilj_batch_as_set";

/// Whether the set path has opened [`AS_SET_SAVEPOINT`] yet. It is opened
/// with the first write, in the same round trip, and never released:
/// `COMMIT` keeps what was written under it, and a batch that writes
/// nothing - every command rejected or deduplicated - never opens it
/// (docs/architecture.md §196).
#[derive(Default)]
pub(super) struct AsSetSavepoint {
    pub(super) open: bool,
}

impl AsSetSavepoint {
    /// `statement` - a plain statement with no parameters, or none - run in
    /// the same round trip as opening the savepoint, if it isn't open yet.
    pub(super) async fn write(
        &mut self,
        tx: &mut Transaction<'_, Postgres>,
        statement: Option<String>,
    ) -> crate::error::Result<()> {
        let sql = match (self.open, statement) {
            (true, None) => return Ok(()),
            (true, Some(statement)) => statement,
            (false, None) => format!("SAVEPOINT {AS_SET_SAVEPOINT}"),
            (false, Some(statement)) => format!("SAVEPOINT {AS_SET_SAVEPOINT}; {statement}"),
        };
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(&mut **tx)
            .await?;
        self.open = true;
        Ok(())
    }
}

/// One accepted command of a batch written as a set, built in memory
/// before anything is written (docs/architecture.md §196).
pub(super) struct PlannedCommand {
    /// Its position in the batch, where its result goes.
    pub(super) index: usize,
    pub(super) command: Command,
    pub(super) events: Vec<Event>,
    pub(super) command_key_ids: Vec<i64>,
    pub(super) event_key_ids: Vec<Vec<i64>>,
    pub(super) idempotency_key: Option<String>,
    pub(super) now: DateTime<Utc>,
    /// Its blob references, already checked (docs/architecture.md §239).
    pub(super) blob_claims: Vec<BlobClaim>,
}

/// `commit_command_batch`'s set path (docs/architecture.md §196): decides
/// every command in `batch` first, in order, without writing anything
/// but the encryption keys a redispatch newly needs, then writes every
/// accepted command with a handful of multi-row statements instead of
/// several statements per command.
///
/// Deciding first needs nothing written: a later command sees an earlier
/// one's events through `extra_committed_events`, as in the per-command
/// path, and its idempotency key through `claimed` instead of the table.
/// The events get their sequence numbers in memory, from right after
/// `locked_highest` - the lock is held, so nothing else takes one - and
/// `{schema}.sequence` moves once by however many were used. A command
/// whose decision or construction fails takes none, so the sequence stays
/// gapless with no [`SequencePool`] to recycle draws.
///
/// `Err` means the batch couldn't be written this way; the caller rolls
/// back the savepoint `tx` is and replays the batch one command at a time.
/// A command's own failure to decide is not that: it is that command's
/// result, as it would be in the per-command path.
#[allow(clippy::too_many_arguments)]
pub(super) async fn write_command_batch_as_set(
    tx: &mut Transaction<'_, Postgres>,
    savepoint: &mut AsSetSavepoint,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    key_service: Option<&KeyService>,
    batch: &[BatchedCommand],
    locked_highest: i64,
    sync_projections: &[String],
    slow: Option<&crate::slow_command::SlowCommandWarnings>,
    payload_limits: &crate::payload_limits::PayloadLimits,
) -> crate::error::Result<(Vec<BatchedCommandResult>, BatchPhaseTiming)> {
    let mut timing = BatchPhaseTiming {
        as_set: true,
        ..BatchPhaseTiming::default()
    };
    let mut results: Vec<Option<BatchedCommandResult>> = Vec::with_capacity(batch.len());
    let mut planned: Vec<PlannedCommand> = Vec::new();
    let mut extra_committed_events: Vec<Event> = Vec::new();
    let mut claimed: BatchIdempotencyKeys = std::collections::HashMap::new();
    let mut highest_assigned = locked_highest;

    let decide_started = std::time::Instant::now();
    let batch_delta = batch_recheck_delta(tx, dispatcher, batch, locked_highest).await?;
    for (index, item) in batch.iter().enumerate() {
        let context = crate::plugin::DecisionContext::at(item.now);
        let decided = decide_command_in_tx(
            tx,
            dispatcher,
            &item.command_type,
            &item.payload,
            &item.client_id,
            &item.bounded_context_events,
            &item.consistency_tags,
            &item.matching_events,
            item.initial_decision.clone(),
            item.snapshot.as_ref().map(|s| SnapshotContext {
                state_json: &s.state_json,
                as_of_sequence: s.as_of_sequence,
            }),
            &item.snapshot_states,
            item.idempotency_key.as_deref(),
            locked_highest,
            &extra_committed_events,
            item.event_types_by_name.clone(),
            item.covered_through,
            Some(&claimed),
            Some(batch_delta.as_deref().unwrap_or(&[])),
            slow,
            &context,
        )
        .await;
        let AcceptedDecision {
            event_specs,
            consistency_boundary,
            consistency_query,
            event_types_by_name,
            blind,
            stance_tags,
        } = match decided {
            Ok(DecideOutcome::Terminal(outcome)) => {
                results.push(Some(Ok(outcome)));
                continue;
            }
            Ok(DecideOutcome::Accepted(decided)) => decided,
            Err(e) => {
                results.push(Some(Err(e)));
                continue;
            }
        };

        let mut resolved = item.resolved.clone();
        let needed_subjects = match missing_encryption_subjects(
            &item.command_type,
            &item.payload,
            &event_specs,
            &event_types_by_name,
            &resolved,
        ) {
            Ok(subjects) => subjects,
            Err(error) => {
                results.push(Some(Err(error)));
                continue;
            }
        };
        if !needed_subjects.is_empty() {
            savepoint.write(tx, None).await?;
            provision_encryption_subjects(
                tx,
                &item.command_type,
                needed_subjects,
                &mut resolved,
                key_service,
            )
            .await?;
        }

        let command_id = match crate::shared::try_generate_token_id() {
            Ok(id) => id,
            Err(error) => {
                results.push(Some(Err(error.into())));
                continue;
            }
        };
        let mut assigned = highest_assigned;
        let processed = crate::event_store::process_command_at_boundary(
            command_id,
            &item.command_type,
            &item.payload,
            &item.client_id,
            item.correlation_id.as_deref(),
            item.causation_id.as_deref(),
            consistency_query,
            consistency_boundary,
            crate::shared::CommandDecision::Accepted {
                events: event_specs,
            },
            |name| event_types_by_name.get(name).cloned(),
            || {
                assigned += 1;
                assigned
            },
            item.now,
            |subject_key, subject_value| {
                let (key, _, data_key) = resolved
                    .get(&(subject_key.to_string(), subject_value.to_string()))
                    .expect(
                        "provision_missing_encryption_keys resolved every subject \
                         sensitive_field_subjects named",
                    );
                (key.clone(), data_key.clone())
            },
            &blind,
        );
        let mut result = match processed {
            Ok(result) => result,
            Err(e) => {
                results.push(Some(Err(e)));
                continue;
            }
        };
        apply_stance_tags(&mut result.events, stance_tags);
        // Refused alone, before its sequence numbers are taken
        // (docs/architecture.md §238).
        if let Err(e) = payload_limits.check_command(&result.command, &result.events) {
            results.push(Some(Err(e)));
            continue;
        }
        // Checked here, refused alone; recorded with the writes below
        // (docs/architecture.md §239). The savepoint first, so a failed
        // read rolls back like a failed write and the batch is replayed.
        let blob_claims = match command_blob_claims(&result.command, &result.events) {
            Ok(claims) => claims,
            Err(e) => {
                results.push(Some(Err(e)));
                continue;
            }
        };
        if !blob_claims.is_empty() {
            savepoint.write(tx, None).await?;
            if let Err(e) =
                check_blob_claims(tx, &item.command_type.bounded_context.name, &blob_claims).await
            {
                if matches!(e, crate::error::Error::Database(_)) {
                    return Err(e);
                }
                results.push(Some(Err(e)));
                continue;
            }
        }
        highest_assigned = assigned;

        if let Some(key) = &item.idempotency_key {
            claimed.insert(
                (
                    item.command_type.name.clone(),
                    item.client_id.clone(),
                    key.clone(),
                ),
                result.events.iter().map(|e| e.sequence).collect(),
            );
        }
        extra_committed_events.extend(result.events.iter().cloned());
        results.push(None);
        planned.push(PlannedCommand {
            index,
            command_key_ids: encryption_key_ids(&result.command.encryption_keys, &resolved),
            event_key_ids: result
                .events
                .iter()
                .map(|event| encryption_key_ids(&event.encryption_keys, &resolved))
                .collect(),
            command: result.command,
            events: result.events,
            idempotency_key: item.idempotency_key.clone(),
            now: item.now,
            blob_claims,
        });
    }
    timing.decide = decide_started.elapsed();

    let persist_started = std::time::Instant::now();
    if let Some(first) = batch.first() {
        write_planned_commands(
            tx,
            savepoint,
            &first.command_type.bounded_context.name,
            &planned,
            highest_assigned - locked_highest,
            projection_dispatcher,
            sync_projections,
        )
        .await?;
    }
    timing.persist = persist_started.elapsed();

    for planned in planned {
        COMMANDS_PROCESSED.add(
            1,
            &[
                KeyValue::new(
                    "bounded_context",
                    planned.command.bounded_context.name.clone(),
                ),
                KeyValue::new("command_type", planned.command.command_type.name.clone()),
                KeyValue::new("outcome", "accepted"),
            ],
        );
        results[planned.index] = Some(Ok(SubmitCommandOutcome::Accepted {
            command: Box::new(planned.command),
            events: planned.events,
        }));
    }
    Ok((
        results
            .into_iter()
            .map(|result| result.expect("every accepted command was written above"))
            .collect(),
        timing,
    ))
}

/// Writes every command `write_command_batch_as_set` planned: the
/// sequence, then one multi-row `INSERT` each for the commands, their
/// events, both encryption-key link tables and the idempotency keys,
/// then the sync projections, event by event in sequence order. A few
/// round trips per batch, where the per-command path makes several per
/// command (docs/architecture.md §196).
pub(super) async fn write_planned_commands(
    tx: &mut Transaction<'_, Postgres>,
    savepoint: &mut AsSetSavepoint,
    bounded_context: &str,
    planned: &[PlannedCommand],
    sequences_used: i64,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    sync_projections: &[String],
) -> crate::error::Result<()> {
    if planned.is_empty() {
        return Ok(());
    }
    let schema = schema_ident(bounded_context);
    // The savepoint opens in the same round trip as the first write. A
    // plain statement for that, so `sequences_used` - a count this
    // function computed, never caller input - is inlined.
    savepoint
        .write(
            tx,
            (sequences_used > 0).then(|| {
                format!("UPDATE {schema}.sequence SET next_value = next_value + {sequences_used}")
            }),
        )
        .await?;

    let commands: Vec<&Command> = planned.iter().map(|p| &p.command).collect();
    let rows: Vec<(i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.commands ({COMMAND_COLUMNS}) \
         SELECT external_id, command_type_name, payload, metadata_type, metadata_version, \
                client_id, created_at, correlation_id, causation_id, consistency_tags::jsonb, \
                consistency_boundary, consistency_query::jsonb \
         FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bigint[], $6::text[], \
                     $7::timestamptz[], $8::text[], $9::text[], $10::text[], $11::bigint[], \
                     $12::text[]) \
              AS u(external_id, command_type_name, payload, metadata_type, metadata_version, \
                   client_id, created_at, correlation_id, causation_id, consistency_tags, \
                   consistency_boundary, consistency_query) \
         RETURNING id, external_id"
    )))
    .bind(commands.iter().map(|c| c.id.as_str()).collect::<Vec<_>>())
    .bind(
        commands
            .iter()
            .map(|c| c.command_type.name.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| c.payload.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| c.metadata.r#type.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| c.metadata.version)
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| c.metadata.client_id.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| c.metadata.created_at)
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| c.metadata.correlation_id.as_deref())
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| c.metadata.causation_id.as_deref())
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| {
                serde_json::to_string(&c.consistency_tags).expect("Tag serialisation is infallible")
            })
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| c.consistency_boundary)
            .collect::<Vec<_>>(),
    )
    .bind(
        commands
            .iter()
            .map(|c| {
                serde_json::to_string(&c.consistency_query)
                    .expect("QueryItem serialisation is infallible")
            })
            .collect::<Vec<_>>(),
    )
    .fetch_all(&mut **tx)
    .await?;
    let ids: std::collections::HashMap<String, i64> = rows
        .into_iter()
        .map(|(id, external_id)| (external_id, id))
        .collect();
    let command_id = |command: &Command| {
        *ids.get(&command.id)
            .expect("every inserted command came back from RETURNING")
    };

    let events: Vec<(&Event, i64)> = planned
        .iter()
        .flat_map(|p| {
            let id = command_id(&p.command);
            p.events.iter().map(move |event| (event, id))
        })
        .collect();
    if !events.is_empty() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.events (sequence, event_type_name, payload, metadata_type, \
             metadata_version, metadata_client_id, metadata_created_at, metadata_correlation_id, \
             metadata_causation_id, tags, origin_kind, origin_source_content, \
             origin_source_context, origin_command_id) \
             SELECT sequence, event_type_name, payload, metadata_type, metadata_version, \
                    client_id, created_at, correlation_id, causation_id, tags::jsonb, \
                    'command_triggered', NULL, NULL, command_id \
             FROM unnest($1::bigint[], $2::text[], $3::text[], $4::text[], $5::bigint[], \
                         $6::text[], $7::timestamptz[], $8::text[], $9::text[], $10::text[], \
                         $11::bigint[]) \
                  AS u(sequence, event_type_name, payload, metadata_type, metadata_version, \
                       client_id, created_at, correlation_id, causation_id, tags, command_id)"
        )))
        .bind(events.iter().map(|(e, _)| e.sequence).collect::<Vec<_>>())
        .bind(
            events
                .iter()
                .map(|(e, _)| e.event_type.name.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(
            events
                .iter()
                .map(|(e, _)| e.payload.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(
            events
                .iter()
                .map(|(e, _)| e.metadata.r#type.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(
            events
                .iter()
                .map(|(e, _)| e.metadata.version)
                .collect::<Vec<_>>(),
        )
        .bind(
            events
                .iter()
                .map(|(e, _)| e.metadata.client_id.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(
            events
                .iter()
                .map(|(e, _)| e.metadata.created_at)
                .collect::<Vec<_>>(),
        )
        .bind(
            events
                .iter()
                .map(|(e, _)| e.metadata.correlation_id.as_deref())
                .collect::<Vec<_>>(),
        )
        .bind(
            events
                .iter()
                .map(|(e, _)| e.metadata.causation_id.as_deref())
                .collect::<Vec<_>>(),
        )
        .bind(
            events
                .iter()
                .map(|(e, _)| {
                    serde_json::to_string(&e.tags).expect("Tag serialisation is infallible")
                })
                .collect::<Vec<_>>(),
        )
        .bind(events.iter().map(|(_, id)| *id).collect::<Vec<_>>())
        .execute(&mut **tx)
        .await?;
    }

    let (key_commands, command_keys): (Vec<i64>, Vec<i64>) = planned
        .iter()
        .flat_map(|p| {
            let id = command_id(&p.command);
            p.command_key_ids.iter().map(move |key| (id, *key))
        })
        .unzip();
    if !key_commands.is_empty() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.command_encryption_keys (command_id, encryption_key_id) \
             SELECT * FROM unnest($1::bigint[], $2::bigint[])"
        )))
        .bind(&key_commands)
        .bind(&command_keys)
        .execute(&mut **tx)
        .await?;
    }
    let (key_events, event_keys): (Vec<i64>, Vec<i64>) = planned
        .iter()
        .flat_map(|p| {
            p.events
                .iter()
                .zip(&p.event_key_ids)
                .flat_map(|(event, keys)| keys.iter().map(move |key| (event.sequence, *key)))
        })
        .unzip();
    if !key_events.is_empty() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.event_encryption_keys (event_sequence, encryption_key_id) \
             SELECT * FROM unnest($1::bigint[], $2::bigint[])"
        )))
        .bind(&key_events)
        .bind(&event_keys)
        .execute(&mut **tx)
        .await?;
    }

    let keyed: Vec<(&PlannedCommand, &str)> = planned
        .iter()
        .filter_map(|p| p.idempotency_key.as_deref().map(|key| (p, key)))
        .collect();
    if !keyed.is_empty() {
        // `triggered_event_sequences` is an array per row, which `unnest`
        // can't take as an array of arrays of different lengths - so each
        // row's goes as a JSON array.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.idempotency_keys \
             (command_type_name, client_id, idempotency_key, triggered_event_sequences, created_at) \
             SELECT command_type_name, client_id, idempotency_key, \
                    ARRAY(SELECT jsonb_array_elements_text(sequences::jsonb)::bigint), created_at \
             FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[]) \
                  AS u(command_type_name, client_id, idempotency_key, sequences, created_at)"
        )))
        .bind(
            keyed
                .iter()
                .map(|(p, _)| p.command.command_type.name.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(
            keyed
                .iter()
                .map(|(p, _)| p.command.metadata.client_id.as_str())
                .collect::<Vec<_>>(),
        )
        .bind(keyed.iter().map(|(_, key)| *key).collect::<Vec<_>>())
        .bind(
            keyed
                .iter()
                .map(|(p, _)| {
                    serde_json::to_string(
                        &p.events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
                    )
                    .expect("a list of numbers serialises")
                })
                .collect::<Vec<_>>(),
        )
        .bind(keyed.iter().map(|(p, _)| p.now).collect::<Vec<_>>())
        .execute(&mut **tx)
        .await?;
    }

    let blob_claims: Vec<(&BlobClaim, Option<i64>)> = planned
        .iter()
        .flat_map(|p| {
            let id = command_id(&p.command);
            p.blob_claims.iter().map(move |claim| (claim, Some(id)))
        })
        .collect();
    if let Some(first) = planned.first() {
        record_blob_claims_by_command(tx, bounded_context, &blob_claims, first.now).await?;
    }

    if !sync_projections.is_empty() {
        let events: Vec<&Event> = events.iter().map(|(event, _)| *event).collect();
        fold_events_into_sync_projections_in_tx(
            tx,
            bounded_context,
            &events,
            projection_dispatcher,
            sync_projections,
        )
        .await?;
    }
    Ok(())
}

/// Folds a batch's `events`, already inserted on `tx` and in sequence
/// order, into every one of `sync_projections` at once - what folding
/// them one by one with [`fold_event_into_sync_projections_in_tx`] does,
/// with a few statements per projection instead of two per key per event
/// (docs/architecture.md §196), the way the async catch-up folds a chunk
/// (§182): every row the events touch is read and locked in one
/// statement, the events are folded into them in memory, in order, each
/// row is written back in one statement, and `caught_up_to` moves once.
pub(super) async fn fold_events_into_sync_projections_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    bounded_context: &str,
    events: &[&Event],
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    sync_projections: &[String],
) -> crate::error::Result<()> {
    let Some(last) = events.last() else {
        return Ok(());
    };
    let schema = schema_ident(bounded_context);
    for projection_name in sync_projections {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT 1 FROM {schema}.projections WHERE name = $1 FOR NO KEY UPDATE"
        )))
        .bind(projection_name)
        .execute(&mut **tx)
        .await?;
        if live_fold_paused(tx, bounded_context, projection_name, dispatcher).await? {
            return Err(crate::projections::Error::LiveFoldPaused(projection_name.clone()).into());
        }
        // As in `fold_event_into_sync_projections_in_tx`: a projection
        // this instance doesn't declare can't be folded here, and a write
        // that it consumes is refused (docs/architecture.md §160).
        let Some(default_state_json) = dispatcher.default_state(bounded_context, projection_name)
        else {
            let mut types: Vec<&str> = events.iter().map(|e| e.event_type.name.as_str()).collect();
            types.sort_unstable();
            types.dedup();
            let consumes: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT EXISTS (SELECT 1 FROM {schema}.projection_consumed_event_types \
                 WHERE projection_name = $1 AND event_type_name = ANY($2))"
            )))
            .bind(projection_name)
            .bind(&types)
            .fetch_one(&mut **tx)
            .await?;
            if consumes {
                return Err(crate::event_store::Error::SyncProjectionNotDeclared(
                    projection_name.clone(),
                )
                .into());
            }
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.projections SET caught_up_to = $1 WHERE name = $2"
            )))
            .bind(last.sequence)
            .bind(projection_name)
            .execute(&mut **tx)
            .await?;
            continue;
        };
        let owner_tag_key = dispatcher
            .owner_tag_key(bounded_context, projection_name)
            .flatten();

        let mut touched: Vec<(&Event, Vec<String>)> = Vec::new();
        let mut keys: Vec<String> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for event in events {
            let event_keys = dispatcher
                .keys(bounded_context, projection_name, event)
                .unwrap_or_default();
            for key in &event_keys {
                if seen.insert(key.clone()) {
                    keys.push(key.clone());
                }
            }
            if !event_keys.is_empty() {
                touched.push((event, event_keys));
            }
        }

        if !keys.is_empty() {
            // Get-or-create-with-lock, every key at once - see
            // `get_or_create_projection_state_for_update`.
            let rows: Vec<(String, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {schema}.projection_state (projection_name, key, state, updated_at) \
                 SELECT $1, key, $3, now() FROM unnest($2::text[]) AS key \
                 ON CONFLICT (projection_name, key) DO UPDATE SET \
                 state = {schema}.projection_state.state \
                 RETURNING key, as_of_sequence, state"
            )))
            .bind(projection_name)
            .bind(&keys)
            .bind(&default_state_json)
            .fetch_all(&mut **tx)
            .await?;
            let mut states: std::collections::HashMap<String, CatchUpChunkState> = rows
                .into_iter()
                .map(|(key, as_of_sequence, state)| {
                    (
                        key,
                        CatchUpChunkState {
                            as_of_sequence,
                            state,
                            owner: None,
                            folded: false,
                        },
                    )
                })
                .collect();

            for (event, event_keys) in &touched {
                let owner = event_owner(owner_tag_key, event);
                for key in event_keys {
                    let entry = states
                        .get_mut(key)
                        .expect("every touched key's row was just read");
                    if entry.as_of_sequence >= event.sequence {
                        continue;
                    }
                    if let Some(result) = dispatcher.project(
                        bounded_context,
                        projection_name,
                        &entry.state,
                        event,
                        key,
                    ) {
                        entry.state = result?;
                    }
                    entry.as_of_sequence = event.sequence;
                    if owner.is_some() {
                        entry.owner.clone_from(&owner);
                    }
                    entry.folded = true;
                }
            }

            let mut folded_keys = Vec::new();
            let mut folded_states = Vec::new();
            let mut owners = Vec::new();
            let mut as_of_sequences = Vec::new();
            for (key, folded) in states.iter().filter(|(_, s)| s.folded) {
                folded_keys.push(key.as_str());
                folded_states.push(folded.state.as_str());
                owners.push(folded.owner.as_deref());
                as_of_sequences.push(folded.as_of_sequence);
            }
            if !folded_keys.is_empty() {
                // A `NULL` owner leaves the stored one as it is - see
                // `apply_projection_fold_update`.
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {schema}.projection_state AS s SET state = u.state, \
                     owner = COALESCE(u.owner, s.owner), as_of_sequence = u.as_of_sequence, \
                     updated_at = now() \
                     FROM unnest($2::text[], $3::text[], $4::text[], $5::bigint[]) \
                     AS u(key, state, owner, as_of_sequence) \
                     WHERE s.projection_name = $1 AND s.key = u.key"
                )))
                .bind(projection_name)
                .bind(&folded_keys)
                .bind(&folded_states)
                .bind(&owners)
                .bind(&as_of_sequences)
                .execute(&mut **tx)
                .await?;
            }
        }

        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.projections SET caught_up_to = $1 WHERE name = $2"
        )))
        .bind(last.sequence)
        .bind(projection_name)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

/// `commit_command_batch`'s per-command path: every command in `batch`, in
/// order, decided and written in its own savepoint, so a command that
/// fails rolls back alone. Since docs/architecture.md §196 it is the
/// fallback for a batch that couldn't be written as a set.
#[allow(clippy::too_many_arguments)]
pub(super) async fn commit_command_batch_per_command(
    tx: &mut Transaction<'static, Postgres>,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    key_service: Option<&KeyService>,
    bounded_context_name: &str,
    locked_highest: i64,
    sync_projections: &[String],
    batch: Vec<BatchedCommand>,
    slow: Option<&crate::slow_command::SlowCommandWarnings>,
    payload_limits: &crate::payload_limits::PayloadLimits,
) -> crate::error::Result<(Vec<BatchedCommandResult>, BatchPhaseTiming)> {
    // Per-phase wall-clock accumulators (issue #32,
    // round four investigation) - the batch-size histogram above already
    // ruled out "batches aren't forming large enough to amortise" as the
    // explanation for the flat ~27/s ceiling; this is the next
    // measurement, not more reasoning from round-trip counting alone:
    // which of decide/sequence-draw/persist actually eats the wall-clock
    // time inside the held lock. One log line per batch, greppable, same
    // zero-infra reasoning as the batch-size line above.
    let mut decide_total = std::time::Duration::ZERO;
    let mut sequence_total = std::time::Duration::ZERO;
    let mut persist_total = std::time::Duration::ZERO;

    let mut results = Vec::with_capacity(batch.len());
    let mut extra_committed_events: Vec<Event> = Vec::new();
    let mut sequence_pool = SequencePool::new();
    let batch_len = batch.len();

    for (idx, item) in batch.into_iter().enumerate() {
        // Not inside a `SAVEPOINT`: `decide_command_in_tx` only reads (on
        // `tx`, since it runs under the batch's lock - docs/architecture.md
        // §117), and a failing read means the connection itself is in
        // trouble, which no savepoint would contain. A decider's own
        // error is a Rust `Err`, not a statement, and stays per-command.
        let decide_started = std::time::Instant::now();
        let context = crate::plugin::DecisionContext::at(item.now);
        let decide_result = decide_command_in_tx(
            tx,
            dispatcher,
            &item.command_type,
            &item.payload,
            &item.client_id,
            &item.bounded_context_events,
            &item.consistency_tags,
            &item.matching_events,
            item.initial_decision,
            item.snapshot.as_ref().map(|s| SnapshotContext {
                state_json: &s.state_json,
                as_of_sequence: s.as_of_sequence,
            }),
            &item.snapshot_states,
            item.idempotency_key.as_deref(),
            locked_highest,
            &extra_committed_events,
            item.event_types_by_name,
            item.covered_through,
            None,
            None,
            slow,
            &context,
        )
        .await;
        decide_total += decide_started.elapsed();

        let decided = match decide_result {
            Ok(DecideOutcome::Terminal(outcome)) => {
                results.push(Ok(outcome));
                continue;
            }
            Ok(DecideOutcome::Accepted(decided)) => decided,
            Err(e) => {
                results.push(Err(e));
                continue;
            }
        };

        // Reserved against `tx` itself, not the `nested` `SAVEPOINT`
        // opened below - see `SequencePool`'s own doc comment for why
        // that's what lets a later command's failure recycle these
        // instead of burning a permanent gap. `refill_hint` is "how many
        // commands (including this one) are still left to process" - an
        // upper bound on how many single-event commands could still
        // share whatever this refill provisions.
        let refill_hint = (batch_len - idx) as i64;
        let sequence_started = std::time::Instant::now();
        let sequence_result = sequence_pool
            .take(
                tx,
                bounded_context_name,
                decided.event_specs.len() as i64,
                refill_hint,
            )
            .await;
        sequence_total += sequence_started.elapsed();
        let sequences = match sequence_result {
            Ok(sequences) => sequences,
            Err(e) => {
                // A failure bumping `{schema}.sequence` on `tx` itself -
                // not a per-command problem any `SAVEPOINT` isolates,
                // since this call deliberately runs outside one. Same
                // treatment as a final `tx.commit()` failure below:
                // nothing in this batch is trustworthy from here, so
                // every command - this one and everything still
                // queued - gets the caller's own uniform `BatchFailed`
                // treatment instead of a partial `results`.
                return Err(e);
            }
        };

        let persist_started = std::time::Instant::now();
        let mut nested = tx.begin().await?;
        let outcome = finish_accepted_command_in_tx(
            &mut nested,
            projection_dispatcher,
            &item.command_type,
            &item.payload,
            &item.client_id,
            item.correlation_id.as_deref(),
            item.causation_id.as_deref(),
            key_service,
            item.now,
            item.idempotency_key.as_deref(),
            sync_projections,
            sequences,
            decided,
            item.resolved,
            payload_limits,
        )
        .await;

        match outcome {
            Ok(outcome) => {
                // A failure of this savepoint statement itself (as with
                // `tx.begin()`/`nested.rollback()` below) means the shared
                // connection is unusable, not that this one command is bad,
                // so it deliberately aborts the whole batch: the
                // transaction can no longer be committed, and the earlier
                // commands' writes live only in it. Per-command failures
                // (`finish_accepted_command_in_tx` returning `Err`) are
                // the ones isolated by the savepoint - see the `Err` arm.
                //
                // Releases this command's own savepoint - its writes stay
                // in `tx`, visible to every subsequent command in this
                // same batch, but still no more durable than the rest of
                // `tx` until the one `tx.commit()` below succeeds.
                nested.commit().await?;
                persist_total += persist_started.elapsed();
                sequence_pool.confirm_last_draw();
                if let SubmitCommandOutcome::Accepted { ref events, .. } = outcome {
                    extra_committed_events.extend(events.iter().cloned());
                }
                results.push(Ok(outcome));
            }
            Err(e) => {
                // `ROLLBACK TO SAVEPOINT`, awaited explicitly rather than
                // left to `nested`'s own `Drop` - both end up issuing the
                // same statement, but every subsequent command in this
                // loop shares `tx`'s one underlying connection, so the
                // rollback must be known-complete before the next
                // `tx.begin()` reuses it, not merely queued by a
                // fire-and-forget `Drop`.
                nested.rollback().await?;
                persist_total += persist_started.elapsed();
                sequence_pool.return_last_draw();
                results.push(Err(e));
            }
        }
    }

    sequence_pool.shrink_back(tx, bounded_context_name).await?;

    Ok((
        results,
        BatchPhaseTiming {
            as_set: false,
            decide: decide_total,
            sequence: sequence_total,
            persist: persist_total,
        },
    ))
}

/// [`begin_command_batch_leader_tx`] immediately followed by
/// [`commit_command_batch`], with no `idle_in_transaction_session_timeout`
/// and nothing drained from a shared queue in between - the whole-batch-
/// in-one-call shape every direct caller (this crate's own
/// `submit_command.rs` tests, which hand-build a batch up front) wants.
/// `command_batcher::CommandBatcher::run_as_leader` calls the two halves
/// separately instead, draining its own queue in the gap between them -
/// see that function's own doc comment for why.
pub async fn submit_command_batch(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    key_service: Option<&KeyService>,
    bounded_context_name: &str,
    batch: Vec<BatchedCommand>,
) -> crate::error::Result<Vec<BatchedCommandResult>> {
    let leader_tx = begin_command_batch_leader_tx(pool, bounded_context_name, None).await?;
    commit_command_batch(
        leader_tx,
        dispatcher,
        projection_dispatcher,
        key_service,
        batch,
        None,
    )
    .await
}

/// What [`resolve_command_submission`] settles on - everything both
/// [`decide_and_submit_command`] and `command_batcher::CommandBatcher::decide_and_submit`
/// need to hand off to their own "locked half" ([`submit_command`] or
/// [`CommandBatcher::submit`](crate::command_batcher::CommandBatcher::submit)
/// respectively), computed identically by both.
pub struct ResolvedCommandSubmission {
    pub bounded_context_events: Vec<Event>,
    /// The position `bounded_context_events` is complete through - see
    /// `list_events_for_bounded_context_matching_tags_cached`.
    pub covered_through: i64,
    pub consistency_tags: Vec<Tag>,
    /// The command type's own consistency query, derived from the payload
    /// - empty for one that declares none (docs/architecture.md §198).
    pub consistency_query: Vec<QueryItem>,
    pub matching_events: Vec<Event>,
    pub decision: crate::shared::CommandDecision,
    pub snapshot_context: Option<ResolvedSnapshot>,
    /// The states of the query's snapshot items, as of the read
    /// (docs/architecture.md §202) - empty for a query without any.
    pub snapshot_states: Vec<crate::plugin::SnapshotState>,
    /// How long reading the history took, snapshot rows included
    /// (docs/architecture.md §203).
    pub read_time: std::time::Duration,
    /// How long `decide()` took on what was read.
    pub decide_time: std::time::Duration,
}

/// `CommandType::consistency_query`, as `dispatcher` holds it for
/// `command_type`, derived from `payload` - empty for a command type that
/// declares none (docs/architecture.md §198). Derived wherever it's
/// needed rather than carried: it's a pure function of the two, and the
/// compiled query is what the compiled `decide()` was written against.
pub(super) fn command_consistency_query(
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    command_type: &CommandType,
    payload: &str,
) -> crate::error::Result<Vec<QueryItem>> {
    let bounded_context = &command_type.bounded_context.name;
    match dispatcher.consistency_query(bounded_context, &command_type.name) {
        Some(mappings) => crate::event_store::derive_query(
            &mappings,
            payload,
            &dispatcher.blind_index(),
            bounded_context,
        ),
        None => Ok(Vec::new()),
    }
}

/// The optimistic, unlocked half of `ProcessCommand`'s own "optimistic
/// decide, then locked submit" sequence - derive consistency tags,
/// resolve a snapshot context if one applies (`resolve_snapshot_context`),
/// fetch matching events (tag-indexed - [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s
/// own "Problem 1" fix), `dispatch()` once optimistically. Shared by
/// [`decide_and_submit_command`] (hands the result to [`submit_command`])
/// and `command_batcher::CommandBatcher::decide_and_submit` (hands it to
/// [`CommandBatcher::submit`](crate::command_batcher::CommandBatcher::submit)
/// instead) - both need the identical resolution, only what happens with
/// it afterward differs.
pub async fn resolve_command_submission(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    event_cache: &crate::event_cache::EventCache,
    command_type: &CommandType,
    payload: &str,
    context: &crate::plugin::DecisionContext,
) -> crate::error::Result<ResolvedCommandSubmission> {
    // docs/architecture.md §200: a bounded context an older instance added
    // during a rolling deploy has no `consistency_query` column for the
    // command to be written with. Once per process per bounded context.
    ensure_bounded_context_current(pool, &command_type.bounded_context).await?;
    let bounded_context_name = command_type.bounded_context.name.clone();
    let consistency_tags = crate::event_store::derive_tags(
        &command_type.tag_mappings,
        payload,
        &dispatcher.blind_index(),
        &bounded_context_name,
    )?;
    let consistency_query = command_consistency_query(dispatcher, command_type, payload)?;
    let default_tags: Vec<_> = consistency_tags
        .iter()
        .filter(|tag| tag.value.is_some())
        .cloned()
        .collect();

    // `build()` refuses a snapshot together with a query; a dispatcher
    // built by hand that has both gets the query.
    let snapshot_context = match dispatcher.snapshot_name(&bounded_context_name, &command_type.name)
    {
        Some(Some(snapshot_name)) if consistency_query.is_empty() => {
            resolve_snapshot_context(
                pool,
                &bounded_context_name,
                snapshot_dispatcher,
                snapshot_name,
                &consistency_tags,
            )
            .await?
        }
        _ => None,
    };

    let read_started = std::time::Instant::now();
    let mut snapshot_states = Vec::new();
    let (bounded_context_events, covered_through) =
        if crate::event_store::query_reads_nothing(&consistency_query) {
            // docs/architecture.md §201: no history to read, and the re-check
            // under the lock skips it too, so the position it would start from
            // doesn't matter.
            (Vec::new(), -1)
        } else if crate::event_store::query_has_snapshots(&consistency_query) {
            let (events, covered_through, states) = read_query_with_snapshots(
                pool,
                event_cache,
                dispatcher,
                &bounded_context_name,
                &consistency_query,
            )
            .await?;
            snapshot_states = states;
            (events, covered_through)
        } else if consistency_query.is_empty() {
            list_events_for_bounded_context_matching_tags_cached(
                pool,
                event_cache,
                &bounded_context_name,
                &default_tags,
                snapshot_context.as_ref().map(|ctx| ctx.as_of_sequence),
            )
            .await?
        } else {
            list_events_matching_query_cached(
                pool,
                event_cache,
                &bounded_context_name,
                &consistency_query,
                None,
            )
            .await?
        };
    let matching_events = crate::event_store::select_matching_events(
        &decided_on(&consistency_query, &consistency_tags),
        &bounded_context_events,
    );
    let read_time = read_started.elapsed();

    let history = decision_history(
        &mut Db::Pool(pool),
        &matching_events,
        dispatcher.key_service(),
    )
    .await?;
    let decide_started = std::time::Instant::now();
    let decision = match &snapshot_context {
        Some(ctx) => dispatcher
            .dispatch_from_snapshot(
                &bounded_context_name,
                &command_type.name,
                payload,
                &ctx.state_json,
                &history,
                context,
            )
            .ok_or(crate::error::Error::NoDeciderRegistered)??,
        None => dispatcher
            .dispatch_with(
                &bounded_context_name,
                &command_type.name,
                payload,
                &history,
                &snapshot_states,
                context,
            )
            .ok_or(crate::error::Error::NoDeciderRegistered)??,
    };
    let decide_time = decide_started.elapsed();
    tracing::debug!(
        bounded_context = %bounded_context_name,
        command_type = %command_type.name,
        events_read = bounded_context_events.len(),
        read_us = read_time.as_micros(),
        decide_us = decide_time.as_micros(),
        "command optimistic read"
    );

    Ok(ResolvedCommandSubmission {
        bounded_context_events,
        covered_through,
        consistency_tags,
        consistency_query,
        matching_events,
        decision,
        snapshot_context,
        snapshot_states,
        read_time,
        decide_time,
    })
}

/// The items whose events `decide()` is handed: the command's query
/// without its snapshot items, whose events are folded into their states
/// instead (docs/architecture.md §202), or the default, one item per
/// consistency tag, for a command type that declares no query.
pub(super) fn decided_on(
    consistency_query: &[QueryItem],
    consistency_tags: &[Tag],
) -> Vec<QueryItem> {
    if consistency_query.is_empty() {
        crate::event_store::effective_query(consistency_query, consistency_tags)
    } else {
        crate::event_store::event_items(consistency_query)
    }
}

/// What [`dry_run_command`] found: `decide()`'s answer, and the matching
/// events it decided from.
#[derive(Debug)]
pub struct CommandDryRun {
    pub decision: DryRunDecision,
    /// Unrendered and unscoped, every event `decide()` saw. A surface that
    /// shows them narrows and renders them first, as `submitCommand` does
    /// for a rejection's (`event_store::visible_matching_events`).
    pub matching_events: Vec<Event>,
}

#[derive(Debug)]
#[non_exhaustive]
pub enum DryRunDecision {
    /// Unrendered: plaintext, as `decide()` emitted them. See
    /// `event_store::render_would_be_event`.
    Accepted {
        events: Vec<WouldBeEvent>,
    },
    Rejected {
        reason: String,
        kind: String,
    },
}

/// `rule DryRunCommand`/`rule DryRunTriggerCommand` past authorisation
/// (issue #47): what a submission of `payload` would decide
/// right now. It is [`resolve_command_submission`] - the same snapshot
/// resolution, the same tag-indexed read, the same `decide()` - and
/// nothing after it. No lock is taken, nothing is written, no sequence
/// is used up, no encryption key is provisioned, and no idempotency key
/// is looked up or recorded. Nor does it re-check under a lock, so a
/// commit racing it can make the real submission decide differently.
///
/// An accepted decision fails where the real submission would before
/// writing: with `UnregisteredEventType` for an event type this bounded
/// context hasn't registered, and with `MasterKeyNotConfigured` when the
/// command or a would-be event has a sensitive field to encrypt and no
/// `key_service` is configured. The key itself is never
/// looked up or provisioned. `client_id` is stamped on each would-be
/// event, as
/// `process_command` stamps it: it is what an `Own` private field's
/// default reader is decided by.
#[allow(clippy::too_many_arguments)]
pub async fn dry_run_command(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    event_cache: &crate::event_cache::EventCache,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    key_service: Option<&KeyService>,
    now: DateTime<Utc>,
) -> crate::error::Result<CommandDryRun> {
    let context = crate::plugin::DecisionContext::at(now);
    let resolved = resolve_command_submission(
        pool,
        dispatcher,
        snapshot_dispatcher,
        event_cache,
        command_type,
        payload,
        &context,
    )
    .await?;
    let decision = match resolved.decision {
        crate::shared::CommandDecision::Rejected { reason, kind } => {
            DryRunDecision::Rejected { reason, kind }
        }
        crate::shared::CommandDecision::Accepted { events: mut specs } => {
            let mut event_types: std::collections::HashMap<String, EventType> =
                std::collections::HashMap::new();
            for spec in &specs {
                if !event_types.contains_key(&spec.event_type) {
                    let event_type =
                        get_event_type(pool, &command_type.bounded_context.name, &spec.event_type)
                            .await?
                            .ok_or_else(|| {
                                crate::event_store::Error::UnregisteredEventType(
                                    spec.event_type.clone(),
                                )
                            })?;
                    event_types.insert(spec.event_type.clone(), event_type);
                }
            }
            let consistency_tags = crate::event_store::derive_tags(
                &command_type.tag_mappings,
                payload,
                &dispatcher.blind_index(),
                &command_type.bounded_context.name,
            )?;
            let stance_tags = {
                let mut conn = pool.acquire().await?;
                match resolve_stances(
                    &mut conn,
                    command_type,
                    &consistency_tags,
                    &mut specs,
                    &event_types,
                    &[],
                )
                .await?
                {
                    Ok(tags) => tags,
                    Err((reason, kind)) => {
                        return Ok(CommandDryRun {
                            decision: DryRunDecision::Rejected {
                                reason,
                                kind: kind.to_string(),
                            },
                            matching_events: resolved.matching_events,
                        });
                    }
                }
            };
            let mut events = Vec::with_capacity(specs.len());
            for spec in specs {
                let event_type = event_types[&spec.event_type].clone();
                let payload = spec.payload.to_string();
                events.push(WouldBeEvent {
                    tags: crate::event_store::derive_tags(
                        &event_type.tag_mappings,
                        &payload,
                        &dispatcher.blind_index(),
                        &command_type.bounded_context.name,
                    )?,
                    event_type,
                    payload,
                    client_id: client_id.to_string(),
                });
            }
            for (index, tags) in stance_tags {
                if let Some(event) = events.get_mut(index) {
                    event.tags = tags;
                }
            }
            let mut needs_a_key = !crate::event_store::try_sensitive_field_subjects(
                &command_type.sensitive_fields,
                payload,
            )?
            .is_empty();
            for event in &events {
                needs_a_key |= !crate::event_store::try_sensitive_field_subjects(
                    &event.event_type.sensitive_fields,
                    &event.payload,
                )?
                .is_empty();
            }
            if needs_a_key && key_service.is_none() {
                return Err(encryption::Error::MasterKeyNotConfigured.into());
            }
            DryRunDecision::Accepted { events }
        }
    };
    Ok(CommandDryRun {
        decision,
        matching_events: resolved.matching_events,
    })
}

/// The full "optimistic decide, then locked submit" sequence
/// `ProcessCommand` describes end to end, for a caller that already has
/// a resolved `CommandType` and a JSON payload in hand - see
/// [`resolve_command_submission`] for the optimistic half this hands off
/// to [`submit_command`] for the real, locked recheck-and-retry.
///
/// Previously this exact sequence was independently duplicated by
/// `skilj-rest`'s `post_commands_trigger` and `skilj-graphql`'s
/// `submitCommand` resolver (each one's own comments cross-referenced
/// the other as "the identical branch") - both now call through here
/// instead, and it is also what `SkiljBuilder`'s cross-context event
/// router (docs/architecture.md's own write-up of that pass) uses to
/// submit a routed command in-process, a third caller with no REST/
/// GraphQL wire concerns of its own to keep separate from this. `None`
/// from `dispatch`/`dispatch_from_snapshot` (no decider registered for
/// this `(bounded_context, command_type)` pair - `CommandDispatcher::
/// dispatch`'s own doc comment on why that's reachable in principle)
/// surfaces as `Error::NoDeciderRegistered`, the same variant every
/// caller already converts into its own wire error today.
///
/// **Not routed through `command_batcher::CommandBatcher`** - this stays
/// the direct, unbatched path, still exactly what it always was. High-
/// volume, externally-triggered submission (REST/GraphQL command
/// submission, parked-delivery redrive) uses `CommandBatcher::decide_and_submit`
/// instead (issue #32, round two); the lower-volume, periodic
/// internal ones (the cross-context event router's own tick,
/// `fire_due_deadlines`) still call through here, deliberately - batching
/// buys the least where a caller is already ticking on its own schedule
/// rather than arriving as a burst of concurrent external requests.
#[allow(clippy::too_many_arguments)]
pub async fn decide_and_submit_command(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    key_service: Option<&KeyService>,
    now: DateTime<Utc>,
    idempotency_key: Option<&str>,
    deadline: Option<crate::plugin::DeadlineContext>,
) -> crate::error::Result<SubmitCommandOutcome> {
    let mut retried = false;
    loop {
        let outcome = decide_and_submit_command_once(
            pool,
            dispatcher,
            projection_dispatcher,
            snapshot_dispatcher,
            broadcaster,
            event_cache,
            command_type,
            payload,
            client_id,
            correlation_id,
            causation_id,
            key_service,
            now,
            idempotency_key,
            deadline,
        )
        .await;
        match outcome {
            Err(err) if back_to_the_read(&err) && !retried => retried = true,
            outcome => return outcome,
        }
    }
}

/// Whether `err` is a command sent back to its read before the lock - retried
/// once by the submission functions: its blind tags were derived before a key
/// rotation (docs/architecture.md §209), or a retraction committed since the
/// read names an event already in a snapshot state (§235).
pub fn back_to_the_read(err: &crate::error::Error) -> bool {
    matches!(
        err,
        crate::error::Error::Blind(crate::blind::Error::Rotated { .. })
            | crate::error::Error::EventStore(
                crate::event_store::Error::SnapshotRetractedMeanwhile { .. }
            )
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn decide_and_submit_command_once(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    key_service: Option<&KeyService>,
    now: DateTime<Utc>,
    idempotency_key: Option<&str>,
    deadline: Option<crate::plugin::DeadlineContext>,
) -> crate::error::Result<SubmitCommandOutcome> {
    let context = match deadline {
        Some(deadline) => crate::plugin::DecisionContext {
            now,
            deadline: Some(deadline),
        },
        None => crate::plugin::DecisionContext::at(now),
    };
    let resolved = resolve_command_submission(
        pool,
        dispatcher,
        snapshot_dispatcher,
        event_cache,
        command_type,
        payload,
        &context,
    )
    .await?;

    submit_command(
        pool,
        dispatcher,
        projection_dispatcher,
        broadcaster,
        event_cache,
        command_type,
        payload,
        client_id,
        correlation_id,
        causation_id,
        &resolved.bounded_context_events,
        &resolved.consistency_tags,
        &resolved.matching_events,
        resolved.decision,
        key_service,
        now,
        resolved
            .snapshot_context
            .as_ref()
            .map(|ctx| SnapshotContext {
                state_json: &ctx.state_json,
                as_of_sequence: ctx.as_of_sequence,
            }),
        &resolved.snapshot_states,
        idempotency_key,
        Some(resolved.covered_through),
        &context,
    )
    .await
}
