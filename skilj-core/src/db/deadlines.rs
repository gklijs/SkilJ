//! Persistence for deadlines rule groups.

use super::*;

// --- issue #20: native one-shot, per-entity deadlines ---
//
// `catch_up_schedule_deadline`/`catch_up_cancel_deadline` are
// `catch_up_cross_context_route`'s own shape, applied to
// `ScheduleDeadline`/`CancelDeadline` instead of `CrossContextRoute` -
// walk one registered reactor's own `Source` event stream after its
// cursor, react to every occurrence, advance the cursor past it either
// way. `fire_due_deadlines` has no `CrossContextRoute` analogue: it
// isn't tied to any one registered type, it scans the `deadlines` table
// itself by `fire_at`, the same register `scheduler_tick_for_bounded_context`'s
// own due-occurrence scan already is.

/// `deadline_cursors`' own read - `get_cross_context_route_cursor`'s
/// analogue, `cursor_owner` a `ScheduleDeadline::NAME`/`CancelDeadline::NAME`.
pub(super) async fn get_deadline_cursor(
    pool: &Pool,
    bounded_context: &str,
    cursor_owner: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT last_dispatched_sequence FROM {schema}.deadline_cursors WHERE cursor_owner = $1"
    )))
    .bind(cursor_owner)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(seq,)| seq))
}

pub(super) async fn update_deadline_cursor(
    pool: &Pool,
    bounded_context: &str,
    cursor_owner: &str,
    sequence: i64,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.deadline_cursors \
         (cursor_owner, last_dispatched_sequence, updated_at) VALUES ($1, $2, $3) \
         ON CONFLICT (cursor_owner) DO UPDATE SET \
         last_dispatched_sequence = EXCLUDED.last_dispatched_sequence, \
         updated_at = EXCLUDED.updated_at"
    )))
    .bind(cursor_owner)
    .bind(sequence)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// One registered `ScheduleDeadline`'s own catch-up tick - see this
/// section's own header comment, and `catch_up_cross_context_route`'s
/// doc comment for the full "first-ever tick seeding" reasoning
/// `start_from != Beginning` shares with it verbatim.
pub async fn catch_up_schedule_deadline(
    pool: &Pool,
    schedule: &crate::plugin::ScheduleDeadlineInfo,
    dispatcher: &dyn crate::plugin::ScheduleDeadlineDispatcher,
    event_cache: &crate::event_cache::EventCache,
) -> crate::error::Result<()> {
    let existing_cursor =
        get_deadline_cursor(pool, schedule.source_bounded_context, schedule.name).await?;
    let cursor = existing_cursor.unwrap_or(-1);
    if existing_cursor.is_none()
        && schedule.start_from != crate::plugin::DeadlinePollStartFrom::Beginning
    {
        // First-ever tick: seed the cursor and dispatch nothing. The
        // seed is one `MAX(sequence)`, not a scan of the loaded history.
        let seed = match schedule.start_from {
            crate::plugin::DeadlinePollStartFrom::Beginning => {
                unreachable!("excluded by this branch's own condition above")
            }
            crate::plugin::DeadlinePollStartFrom::Latest => {
                highest_sequence_of_type(
                    pool,
                    schedule.source_bounded_context,
                    schedule.source_event_type,
                    None,
                )
                .await?
            }
            crate::plugin::DeadlinePollStartFrom::AtSequence(n) => n,
            crate::plugin::DeadlinePollStartFrom::AtTime(unix_secs) => {
                let threshold = DateTime::from_timestamp(unix_secs, 0).unwrap_or(Utc::now());
                highest_sequence_of_type(
                    pool,
                    schedule.source_bounded_context,
                    schedule.source_event_type,
                    Some(threshold),
                )
                .await?
            }
        };
        update_deadline_cursor(
            pool,
            schedule.source_bounded_context,
            schedule.name,
            seed,
            Utc::now(),
        )
        .await?;
        return Ok(());
    }

    // At most `MAX_EVENTS_PER_CATCH_UP_TICK` per tick - the cursor
    // persists per processed event, so the next tick continues exactly
    // where this one stopped, and a long backlog is never loaded whole.
    let events = list_events_cached_limited(
        pool,
        event_cache,
        schedule.source_bounded_context,
        schedule.source_event_type,
        cursor,
        MAX_EVENTS_PER_CATCH_UP_TICK,
    )
    .await?;

    let schema = schema_ident(schedule.source_bounded_context);
    for event in &events {
        match dispatcher.schedule(schedule.name, &event.payload) {
            None => {
                // Defensive only - `schedule.name` came from this same
                // dispatcher's own `schedules()` list.
                tracing::warn!(
                    sequence = event.sequence,
                    "schedule deadline not found in its own dispatcher - skipping"
                );
            }
            Some(Err(e)) => {
                tracing::warn!(
                    sequence = event.sequence,
                    error = %e,
                    "schedule deadline: source payload did not deserialize - skipping"
                );
            }
            Some(Ok(None)) => {
                // `schedule()` itself decided this occurrence doesn't
                // apply - a real, expected outcome, not an error.
            }
            Some(Ok(Some(spec))) => {
                // issue #95 - a recurring spec's own initial
                // `fire_at` must lie inside its own validity window, or
                // the chain would emit a row that is already expired.
                // Skipped with a warning, cursor still advances - a caller
                // bug in `schedule()`, not a condition to retry.
                let out_of_window = match &spec.recurrence {
                    Some(recurrence) => {
                        spec.fire_at
                            < recurrence
                                .start_at
                                .unwrap_or(chrono::DateTime::<Utc>::MIN_UTC)
                            || spec.fire_at
                                > recurrence
                                    .end_at
                                    .unwrap_or(chrono::DateTime::<Utc>::MAX_UTC)
                            || spec.fire_at
                                > recurrence
                                    .valid_until
                                    .unwrap_or(chrono::DateTime::<Utc>::MAX_UTC)
                    }
                    None => false,
                };
                if out_of_window {
                    tracing::warn!(
                        sequence = event.sequence,
                        fire_at = %spec.fire_at,
                        "schedule deadline: initial fire_at outside its own recurrence window - skipping"
                    );
                } else {
                    // Deterministic id - see `ensure_deadlines_table`'s
                    // own doc comment for why `ON CONFLICT (id) DO
                    // NOTHING` makes a redelivered tick a safe no-op.
                    // The first row names itself its own chain parent at
                    // sequence 0; `fire_due_deadlines` derives every later
                    // row's id from those. A one-shot stores no recurrence
                    // columns at all. A bad cron expression degrades to a
                    // one-shot (a warning here, not a refusal) - the chain
                    // simply ends after the first firing.
                    let id = format!("{}:{}", schedule.name, event.sequence);
                    let (recurrence_schedule, recurrence_policy, recurrence_start_at) =
                        match &spec.recurrence {
                            Some(recurrence) => (
                                Some(recurrence.schedule.clone()),
                                Some(recurrence_policy_to_str(recurrence.policy)),
                                recurrence.start_at,
                            ),
                            None => (None, None, None),
                        };
                    let (recurrence_end_at, recurrence_valid_until) = match &spec.recurrence {
                        Some(recurrence) => (recurrence.end_at, recurrence.valid_until),
                        None => (None, None),
                    };
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "INSERT INTO {schema}.deadlines \
                         (id, schedule_name, fire_at, expires_at, tags, correlation_id, \
                          target_bounded_context, target_command_type, payload, status, created_at, \
                          recurrence_schedule, recurrence_policy, recurrence_start_at, \
                          recurrence_end_at, recurrence_valid_until, recurrence_parent_id, \
                          recurrence_sequence) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', $10, $11, $12, $13, \
                                 $14, $15, $16, $17) \
                         ON CONFLICT (id) DO NOTHING"
                    )))
                    .bind(&id)
                    .bind(schedule.name)
                    .bind(spec.fire_at)
                    .bind(spec.expires_at)
                    .bind(Json(&spec.tags))
                    .bind(event.metadata.correlation_id.as_deref())
                    .bind(schedule.target_bounded_context)
                    .bind(schedule.target_command_type)
                    .bind(&spec.payload_json)
                    .bind(Utc::now())
                    .bind(recurrence_schedule)
                    .bind(recurrence_policy)
                    .bind(recurrence_start_at)
                    .bind(recurrence_end_at)
                    .bind(recurrence_valid_until)
                    .bind(spec.recurrence.as_ref().map(|_| id.clone()))
                    .bind(spec.recurrence.as_ref().map(|_| 0_i64))
                    .execute(pool)
                    .await?;
                }
            }
        }
        update_deadline_cursor(
            pool,
            schedule.source_bounded_context,
            schedule.name,
            event.sequence,
            Utc::now(),
        )
        .await?;
    }
    Ok(())
}

/// When the first of `cancel`'s source events it hasn't processed yet was
/// created, or `None` when it has processed every one there is
/// (docs/architecture.md §131). A cancel that has never run has processed
/// nothing.
pub(super) async fn cancel_backlog_start(
    pool: &Pool,
    cancel: &crate::plugin::CancelDeadlineInfo,
) -> crate::error::Result<Option<DateTime<Utc>>> {
    // A cancel whose source bounded context was hard-deleted can never
    // cancel anything again, so there is nothing to wait for. Querying its
    // dropped schema instead failed every fire tick of the bounded context
    // holding the deadlines - none of them fired (docs/architecture.md §133).
    let source_exists: Option<i32> =
        sqlx::query_scalar("SELECT 1 FROM bounded_contexts WHERE name = $1")
            .bind(cancel.source_bounded_context)
            .fetch_optional(pool)
            .await?;
    if source_exists.is_none() {
        return Ok(None);
    }
    let cursor = get_deadline_cursor(pool, cancel.source_bounded_context, cancel.name)
        .await?
        .unwrap_or(-1);
    let schema = schema_ident(cancel.source_bounded_context);
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT metadata_created_at FROM {schema}.events \
         WHERE event_type_name = $1 AND sequence > $2 ORDER BY sequence LIMIT 1"
    )))
    .bind(cancel.source_event_type)
    .bind(cursor)
    .fetch_optional(pool)
    .await?)
}

/// Where a `CancelDeadline`'s schedule still has work: the first of the
/// schedule's source events it hasn't processed. Compared by sequence
/// when the cancelling events share its bounded context - exact, since
/// events become visible in sequence order - and by creation time across
/// bounded contexts, where sequences aren't comparable.
pub(super) enum ScheduleBacklogStart {
    Sequence(i64),
    Time(DateTime<Utc>),
}

/// The start of `cancel`'s schedule's unprocessed backlog, or `None` when
/// it has processed every source event there is (docs/architecture.md
/// §130). A schedule that has never run has processed nothing.
pub(super) async fn deadline_schedule_backlog_start(
    pool: &Pool,
    cancel: &crate::plugin::CancelDeadlineInfo,
) -> crate::error::Result<Option<ScheduleBacklogStart>> {
    let cursor = get_deadline_cursor(
        pool,
        cancel.deadline_schedule_bounded_context,
        cancel.deadline_schedule_name,
    )
    .await?
    .unwrap_or(-1);
    let schema = schema_ident(cancel.deadline_schedule_bounded_context);
    let first: Option<(i64, DateTime<Utc>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT sequence, metadata_created_at FROM {schema}.events \
         WHERE event_type_name = $1 AND sequence > $2 ORDER BY sequence LIMIT 1"
    )))
    .bind(cancel.deadline_schedule_source_event_type)
    .bind(cursor)
    .fetch_optional(pool)
    .await?;
    Ok(first.map(|(sequence, created_at)| {
        if cancel.deadline_schedule_bounded_context == cancel.source_bounded_context {
            ScheduleBacklogStart::Sequence(sequence)
        } else {
            ScheduleBacklogStart::Time(created_at)
        }
    }))
}

/// One registered `CancelDeadline`'s own catch-up tick - same shape as
/// `catch_up_schedule_deadline` just above, reacting by cancelling
/// pending rows instead of inserting one. Writes into
/// `cancel.deadline_schedule_bounded_context`'s own `deadlines` table,
/// which can differ from `cancel.source_bounded_context` - see
/// `CancelDeadlineInfo::deadline_schedule_bounded_context`'s own doc
/// comment.
pub async fn catch_up_cancel_deadline(
    pool: &Pool,
    cancel: &crate::plugin::CancelDeadlineInfo,
    dispatcher: &dyn crate::plugin::CancelDeadlineDispatcher,
    event_cache: &crate::event_cache::EventCache,
) -> crate::error::Result<()> {
    let existing_cursor =
        get_deadline_cursor(pool, cancel.source_bounded_context, cancel.name).await?;
    let cursor = existing_cursor.unwrap_or(-1);
    if existing_cursor.is_none()
        && cancel.start_from != crate::plugin::DeadlinePollStartFrom::Beginning
    {
        // First-ever tick: seed the cursor and dispatch nothing. The
        // seed is one `MAX(sequence)`, not a scan of the loaded history.
        let seed = match cancel.start_from {
            crate::plugin::DeadlinePollStartFrom::Beginning => {
                unreachable!("excluded by this branch's own condition above")
            }
            crate::plugin::DeadlinePollStartFrom::Latest => {
                highest_sequence_of_type(
                    pool,
                    cancel.source_bounded_context,
                    cancel.source_event_type,
                    None,
                )
                .await?
            }
            crate::plugin::DeadlinePollStartFrom::AtSequence(n) => n,
            crate::plugin::DeadlinePollStartFrom::AtTime(unix_secs) => {
                let threshold = DateTime::from_timestamp(unix_secs, 0).unwrap_or(Utc::now());
                highest_sequence_of_type(
                    pool,
                    cancel.source_bounded_context,
                    cancel.source_event_type,
                    Some(threshold),
                )
                .await?
            }
        };
        update_deadline_cursor(
            pool,
            cancel.source_bounded_context,
            cancel.name,
            seed,
            Utc::now(),
        )
        .await?;
        return Ok(());
    }

    // At most `MAX_EVENTS_PER_CATCH_UP_TICK` per tick - the cursor
    // persists per processed event, so the next tick continues exactly
    // where this one stopped, and a long backlog is never loaded whole.
    let events = list_events_cached_limited(
        pool,
        event_cache,
        cancel.source_bounded_context,
        cancel.source_event_type,
        cursor,
        MAX_EVENTS_PER_CATCH_UP_TICK,
    )
    .await?;

    // docs/architecture.md §130: a cancel only applies to deadlines its
    // schedule has already created. The two run independently, so a
    // cancelling event is held back until the schedule has processed
    // every source event that precedes it - otherwise it matches nothing,
    // the deadline is created a moment later, and fires regardless.
    let before = deadline_schedule_backlog_start(pool, cancel).await?;
    let target_schema = schema_ident(cancel.deadline_schedule_bounded_context);
    for event in &events {
        let scheduled_past = match &before {
            None => true,
            Some(ScheduleBacklogStart::Sequence(first_unscheduled)) => {
                event.sequence < *first_unscheduled
            }
            Some(ScheduleBacklogStart::Time(first_unscheduled)) => {
                event.metadata.created_at < *first_unscheduled
            }
        };
        if !scheduled_past {
            // The rest waits for the schedule; the cursor stays on the
            // last event handled, and a later tick resumes from there.
            break;
        }
        match dispatcher.cancel_tags(cancel.name, &event.payload) {
            None => {
                tracing::warn!(
                    sequence = event.sequence,
                    "cancel deadline not found in its own dispatcher - skipping"
                );
            }
            Some(Err(e)) => {
                tracing::warn!(
                    sequence = event.sequence,
                    error = %e,
                    "cancel deadline: source payload did not deserialize - skipping"
                );
            }
            Some(Ok(None)) => {
                // `cancel_tags()` itself decided this occurrence doesn't
                // apply.
            }
            Some(Ok(Some(tags))) if tags.is_empty() => {
                // An empty tag list matches nothing - the identical
                // treatment `list_events_for_bounded_context_matching_tags`'s
                // own early return already gives, applied here rather
                // than running a `WHERE` clause with no tag condition at
                // all (which would match every still-pending row this
                // schedule owns, not none).
            }
            Some(Ok(Some(tags))) => {
                // Tag-containment, one `tags @> $n::jsonb` clause per
                // wanted tag, ORed together - the identical shape
                // `list_events_for_bounded_context_matching_tags` already
                // uses against the `events` table's own `tags` column
                // (docs/architecture.md §19 Problem 1), applied here to
                // `deadlines.tags` instead. Cancels every still-`pending`
                // row this schedule owns that shares at least one of
                // these tags - zero, one, or several rows, all a
                // legitimate outcome (see `CancelDeadline::cancel_tags`'s
                // own doc comment).
                let tag_literals: Vec<String> = tags
                    .iter()
                    .map(|t| {
                        serde_json::to_string(std::slice::from_ref(t))
                            .expect("Tag serialisation is infallible")
                    })
                    .collect();
                let tag_clause = (0..tag_literals.len())
                    .map(|i| format!("tags @> ${}::jsonb", i + 3))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                let mut query = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {target_schema}.deadlines SET status = 'cancelled', resolved_at = $1, \
                     payload = '{{}}' \
                     WHERE schedule_name = $2 AND status = 'pending' AND ({tag_clause})"
                )))
                .bind(Utc::now())
                .bind(cancel.deadline_schedule_name);
                for literal in tag_literals {
                    query = query.bind(literal);
                }
                query.execute(pool).await?;
            }
        }
        update_deadline_cursor(
            pool,
            cancel.source_bounded_context,
            cancel.name,
            event.sequence,
            Utc::now(),
        )
        .await?;
    }
    Ok(())
}

/// `fire_due_deadlines`'s own per-tick cap - the identical "recent
/// window, not a hard limit on correctness" register
/// `scheduler_tick`'s own `MAX_OCCURRENCES_PER_TICK` already is: a
/// backlog bigger than this just takes more ticks to drain, each one
/// picking up wherever the last left off (`fire_at` order, never
/// re-offering an already-`fired`/`cancelled` row).
pub(super) const MAX_DUE_DEADLINES_PER_TICK: i64 = 1000;

/// How long a claimed (`'firing'`) row is left alone before a future
/// tick is allowed to reclaim it - long enough that a live instance's
/// own `decide_and_submit_command` call (a handful of DB round trips)
/// never comes close, short enough that a genuinely crashed claim (the
/// only way a row is still `'firing'` this long after `firing_at`) gets
/// retried on a human-visible timescale rather than parked forever. The
/// retry itself is safe even if the original attempt actually did
/// commit its command before crashing: `RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX`
/// makes the resubmission a harmless `Deduplicated`, the same
/// defense-in-depth `fire_due_deadlines`'s own doc comment already
/// describes for the fire-vs-fire case this claim mechanism otherwise
/// closes off entirely.
pub(super) fn deadline_firing_claim_stale_after() -> chrono::Duration {
    chrono::Duration::minutes(5)
}

#[derive(sqlx::FromRow)]
pub(super) struct DueDeadlineRow {
    pub(super) id: String,
    pub(super) schedule_name: String,
    pub(super) fire_at: DateTime<Utc>,
    pub(super) expires_at: Option<DateTime<Utc>>,
    pub(super) tags: Json<Vec<Tag>>,
    pub(super) correlation_id: Option<String>,
    pub(super) target_bounded_context: String,
    pub(super) target_command_type: String,
    pub(super) payload: String,
    pub(super) attempt_count: i32,
    pub(super) first_failed_at: Option<DateTime<Utc>>,
    pub(super) recurrence_schedule: Option<String>,
    pub(super) recurrence_policy: Option<String>,
    pub(super) recurrence_start_at: Option<DateTime<Utc>>,
    pub(super) recurrence_end_at: Option<DateTime<Utc>>,
    pub(super) recurrence_valid_until: Option<DateTime<Utc>>,
    pub(super) recurrence_parent_id: Option<String>,
    pub(super) recurrence_sequence: Option<i64>,
}

/// issue #95 - one due row's own recurrence state, reconstituted
/// from its columns. `None` for a one-shot (`recurrence_schedule` is
/// `NULL`) or for a row whose stored policy no known writer could have
/// stored (defensive; the `CHECK` constraint already refuses those) - a
/// chain that can't be understood is simply not advanced.
pub(super) struct Recurrence {
    pub(super) schedule: String,
    pub(super) policy: crate::plugin::MissedRunPolicy,
    pub(super) start_at: Option<DateTime<Utc>>,
    pub(super) end_at: Option<DateTime<Utc>>,
    pub(super) valid_until: Option<DateTime<Utc>>,
    pub(super) parent_id: Option<String>,
    pub(super) sequence: Option<i64>,
}

impl DueDeadlineRow {
    pub(super) fn recurrence(&self) -> Option<Recurrence> {
        Some(Recurrence {
            schedule: self.recurrence_schedule.clone()?,
            policy: recurrence_policy_from_str(self.recurrence_policy.as_deref())?,
            start_at: self.recurrence_start_at,
            end_at: self.recurrence_end_at,
            valid_until: self.recurrence_valid_until,
            parent_id: self.recurrence_parent_id.clone(),
            sequence: self.recurrence_sequence,
        })
    }
}

/// A `Deadline`-kind parked row's `source` is this prefix plus the
/// `ScheduleDeadline` that created it; its `identifier` is the deadline's
/// id (docs/architecture.md §115).
pub const DEADLINE_PARKED_SOURCE_PREFIX: &str = "deadline:";

/// The `client_id` every deadline's command is submitted under.
pub const DEADLINE_CLIENT_ID: &str = "deadline";

/// One bounded context's own share of the firing scan - **not** tied to
/// any one registered `ScheduleDeadline`, unlike the two catch-up
/// functions above: it scans `{schema}.deadlines` itself, so a row fires
/// regardless of whether the `ScheduleDeadline` that created it is still
/// registered in this process (the same "every instance does the same
/// redundant, idempotent work" register [docs/architecture.md §22](../../../docs/architecture.md#background-polling-and-startup-scaling)
/// already established.
///
/// No `FOR UPDATE SKIP LOCKED` on the initial `SELECT` - two instances
/// racing to fire the same row both submit under the identical
/// idempotency key, so the second is a harmless `Deduplicated`. But
/// unlike that fire-vs-fire race, a fire-vs-*cancel* race is not
/// harmless: `catch_up_cancel_deadline` can flip this same row to
/// `'cancelled'` concurrently, and if that landed between this
/// function's own `SELECT` and its call to `decide_and_submit_command`,
/// the row would end up recorded `'cancelled'` while the target command
/// had already been submitted - see [docs/architecture.md §55](../../../docs/architecture.md#55-closing-the-canceldeadline-fire-vs-cancel-race).
/// Each row is therefore atomically claimed (`'pending'` -> `'firing'`)
/// immediately before submitting, via the same `UPDATE ... WHERE
/// status = 'pending' RETURNING` shape `mark_deadline_resolved` already
/// uses to make its own write idempotent. `catch_up_cancel_deadline`'s
/// own `UPDATE ... WHERE status = 'pending'` then naturally loses the
/// race once a row is `'firing'` - no change needed there, only a
/// dedicated test proving it (`skilj/tests/deadlines.rs`).
///
/// A due row whose own `target_command_type` isn't registered at all is
/// marked `fired` without ever calling `decide_and_submit_command` -
/// logged as a warning, not retried forever, the identical stance
/// `catch_up_cross_context_route` already takes for its own "target
/// `CommandType` isn't registered" case. No claim needed on that path:
/// no side effect has happened yet, so losing a race with a concurrent
/// cancel there is just ordinary "cancelled before it fired" - the
/// correct outcome, not a bug - so `mark_deadline_resolved` is called
/// directly against the still-`'pending'` row. A `Target` command that
/// *is* submitted but gets rejected by its own `decide()` is marked
/// `fired` too - a legitimate business outcome (`ScheduleDeadline`'s own
/// doc comment), not a reason to retry.
///
/// A submission that fails with an *error* is retried per
/// `retry_policy` - the row goes back to `pending` with `next_attempt_at`
/// set, so a cancel in the meantime still wins - and once the policy is
/// exhausted, parked as a `Deadline`-kind [`ParkedDelivery`] in the
/// target bounded context and marked `parked`, where an operator can
/// retry or discard it like a parked route delivery. Either way the rest
/// of the tick carries on (docs/architecture.md §115).
///
/// issue #95 - a row carrying recurrence state is the head of a
/// chain, and a firing advances it: the next row is inserted *before*
/// this one is marked `fired` (deterministic id, `ON CONFLICT DO
/// NOTHING`), so a crash between the two re-fires idempotently under the
/// same `skilj-deadline:` key and re-inserts the same next row as a
/// no-op - no gap, no duplicate. An `Accepted`, `Rejected` (per-fire
/// business, like a one-shot's) or `Deduplicated` outcome advances the
/// chain; a parked outcome advances it too, so one poisoned occurrence
/// doesn't stall every later one. A retryable error or an
/// `another_instance` defer leaves the row `pending` and begets nothing;
/// an unregistered target, an archived target, an unparseable schedule,
/// or a window the next occurrence falls outside of ends the chain - the
/// row is simply marked `fired`. A pending row already past
/// `valid_until` never submits at all: marked `fired`, nothing begotten.
/// `Cancelled` rows never reach here, so a `CancelDeadline` matching the
/// head ends the chain the same way - nothing further is ever inserted.
#[allow(clippy::too_many_arguments)]
pub async fn fire_due_deadlines(
    pool: &Pool,
    command_dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    now: DateTime<Utc>,
    key_service: Option<&KeyService>,
    retry_policy: &skilj_retry::RetryPolicy,
    cancels: &[crate::plugin::CancelDeadlineInfo],
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let stale_cutoff = now - deadline_firing_claim_stale_after();
    // docs/architecture.md §131: per cancel that can reach this bounded
    // context's deadlines, when its first unprocessed source event was
    // created - looked up once per tick, on first need.
    let mut cancel_backlogs: std::collections::HashMap<&str, Option<DateTime<Utc>>> =
        std::collections::HashMap::new();
    let rows: Vec<DueDeadlineRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, schedule_name, fire_at, expires_at, tags, correlation_id, \
         target_bounded_context, \
         target_command_type, payload, attempt_count, first_failed_at, recurrence_schedule, \
         recurrence_policy, recurrence_start_at, recurrence_end_at, recurrence_valid_until, \
         recurrence_parent_id, recurrence_sequence \
         FROM {schema}.deadlines \
         WHERE (status = 'pending' OR (status = 'firing' AND firing_at <= $1)) AND fire_at <= $2 \
         AND (next_attempt_at IS NULL OR next_attempt_at <= $2) \
         ORDER BY fire_at LIMIT {MAX_DUE_DEADLINES_PER_TICK}"
    )))
    .bind(stale_cutoff)
    .bind(now)
    .fetch_all(pool)
    .await?;

    'rows: for row in rows {
        // docs/architecture.md §131: a deadline only fires once every
        // cancel that can reach it has processed its source events up to
        // the deadline's `fire_at` - one committed before then, but not
        // yet processed by a lagging cancel loop, would otherwise lose to
        // the fire. Held rows stay `pending` for a later tick.
        for cancel in cancels.iter().filter(|c| {
            c.deadline_schedule_bounded_context == bounded_context
                && c.deadline_schedule_name == row.schedule_name
        }) {
            let backlog = match cancel_backlogs.get(cancel.name) {
                Some(backlog) => *backlog,
                None => {
                    let backlog = cancel_backlog_start(pool, cancel).await?;
                    cancel_backlogs.insert(cancel.name, backlog);
                    backlog
                }
            };
            if backlog.is_some_and(|first_unprocessed| first_unprocessed <= row.fire_at) {
                continue 'rows;
            }
        }
        // issue #95 - the chain's kill-switch: a pending row
        // already past its own `valid_until` never submits and begets
        // nothing, marked `fired` like any other chain end. The claim
        // below is skipped deliberately - no side effect has happened, so
        // a concurrent cancel racing this tick is just ordinary
        // "cancelled before it fired", the same register the
        // unregistered-target path already is.
        let recurrence = row.recurrence();
        if recurrence.as_ref().is_some_and(|recurrence| {
            now > recurrence
                .valid_until
                .unwrap_or(chrono::DateTime::<Utc>::MAX_UTC)
        }) {
            mark_deadline_resolved(pool, &schema, &row.id, "fired", now).await?;
            continue;
        }
        let Some(target_command_type) =
            get_command_type(pool, &row.target_bounded_context, &row.target_command_type).await?
        else {
            tracing::warn!(
                deadline_id = %row.id,
                target_bounded_context = %row.target_bounded_context,
                target_command_type = %row.target_command_type,
                "deadline's own target CommandType isn't registered - marking fired without submitting"
            );
            mark_deadline_resolved(pool, &schema, &row.id, "fired", now).await?;
            continue;
        };
        // docs/architecture.md §96: an archived target refuses the command
        // (`process_command`), and would keep refusing it - left as an
        // error, the claim would go stale and be retried every few minutes
        // forever. Resolved the same way as an unregistered target.
        if target_command_type.bounded_context.status
            != crate::event_store::BoundedContextStatus::Active
        {
            tracing::warn!(
                deadline_id = %row.id,
                target_bounded_context = %row.target_bounded_context,
                target_command_type = %row.target_command_type,
                "deadline's own target bounded context is archived - marking fired without submitting"
            );
            mark_deadline_resolved(pool, &schema, &row.id, "fired", now).await?;
            continue;
        }
        // Claims a `'pending'` row outright, or reclaims a `'firing'` one
        // whose own `firing_at` is already past `stale_cutoff` - the
        // identical condition the `SELECT` above already filtered on,
        // re-checked here under the `UPDATE`'s own row lock so a second
        // instance racing to reclaim the same stale row always loses
        // (Postgres re-evaluates `WHERE` against the just-committed row
        // once the first `UPDATE` releases it, and by then `firing_at`
        // is `now`, no longer `<= stale_cutoff`).
        let claimed: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.deadlines SET status = 'firing', firing_at = $1 \
             WHERE id = $2 AND (status = 'pending' OR (status = 'firing' AND firing_at <= $3)) \
             AND (next_attempt_at IS NULL OR next_attempt_at <= $1) \
             RETURNING id"
        )))
        .bind(now)
        .bind(&row.id)
        .bind(stale_cutoff)
        .fetch_optional(pool)
        .await?;
        if claimed.is_none() {
            // Lost the claim to a concurrent cancel, or to a concurrent
            // instance's own fire/reclaim attempt on the same row -
            // either way, correct to skip: this instance must not submit
            // the target command.
            continue;
        }
        // Prefixed with `RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX` - see
        // that constant's own doc comment for why this is
        // defense-in-depth, not load-bearing, now that `idempotency_keys`
        // is `client_id`-scoped and this call's own `client_id` below
        // ("deadline") is never externally suppliable.
        let idempotency_key = format!(
            "{}{}",
            crate::event_store::RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX,
            row.id
        );
        // issue #18: carries the scheduling event's own
        // correlation_id forward (stored on the row at schedule time) -
        // no `causation_id` of its own, since there's no `Event` this
        // firing directly descends from the way a `CrossContextRoute`'s
        // own submission descends from the `Source` event that triggered
        // it (`event_causation_id`); a fired deadline's real cause is a
        // clock, not a prior commit.
        // issue #97: the target's `decide_with` gets the deadline
        // this command descends from - when it was scheduled to fire
        // (`fire_at`), its optional `expires_at`, and whether this firing
        // is past it - so it can refuse an action it fired too late for.
        let deadline_context =
            crate::plugin::DeadlineContext::new(row.fire_at, row.expires_at, now);
        // issue #98: an approval-required target becomes a
        // proposal, never a direct submission - otherwise whoever can cause
        // the scheduling event bypasses the approver. `Ok(None)` is
        // "proposed"; errors take the same retry/park path as a failed
        // submission.
        let outcome = match propose_if_approval_required(
            pool,
            &target_command_type,
            &row.payload,
            DEADLINE_CLIENT_ID,
            row.correlation_id.as_deref(),
            None,
            &idempotency_key,
            key_service,
            now,
        )
        .await
        {
            Ok(true) => Ok(None),
            Ok(false) => decide_and_submit_command(
                pool,
                command_dispatcher,
                projection_dispatcher,
                snapshot_dispatcher,
                broadcaster,
                event_cache,
                &target_command_type,
                &row.payload,
                DEADLINE_CLIENT_ID,
                row.correlation_id.as_deref(),
                None,
                key_service,
                now,
                Some(&idempotency_key),
                Some(deadline_context),
            )
            .await
            .map(Some),
            Err(e) => Err(e),
        };
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(e) if e.another_instance_can_do_it() => {
                defer_deadline_to_another_instance(pool, &schema, &row, &e, now).await?;
                continue;
            }
            Err(e) => {
                record_deadline_failure(pool, &schema, &row, &e, retry_policy, now).await?;
                let parked: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT id FROM {schema}.deadlines WHERE id = $1 AND status = 'parked'"
                )))
                .bind(&row.id)
                .fetch_optional(pool)
                .await?;
                if parked.is_some() {
                    advance_deadline_chain(pool, &schema, &row, recurrence.as_ref(), now).await?;
                }
                continue;
            }
        };
        if matches!(outcome, Some(SubmitCommandOutcome::Deduplicated { .. })) {
            tracing::warn!(
                deadline_id = %row.id,
                "deadline's own idempotency key was already present - expected after a \
                 crash/restart between a prior fire attempt and marking it resolved; \
                 unexpected otherwise, since the key space is reserved"
            );
        }
        // issue #95 - the next row lands before this one is
        // marked `fired`, so a crash between the two re-fires idempotently
        // and re-inserts the same next id as a no-op.
        advance_deadline_chain(pool, &schema, &row, recurrence.as_ref(), now).await?;
        mark_deadline_resolved(pool, &schema, &row.id, "fired", now).await?;
    }
    Ok(())
}

/// issue #95 - advances one recurring chain past the firing of
/// `row`: derives the next `fire_at` from the recurrence math and inserts
/// the next pending row (deterministic id, `ON CONFLICT DO NOTHING`),
/// carrying the chain's tags, payload, correlation, target and recurrence
/// state forward. A `None` derivation ends the chain silently - warned
/// once only for an unparseable schedule, since a windowed end is the
/// ordinary, expected way a chain stops. The sequence counts up from the
/// parent's 0, so ids (`"{parent_id}#r{sequence}"`) can never collide with
/// the parent's own `"{schedule}:{sequence}"` shape or with each other.
pub(super) async fn advance_deadline_chain(
    pool: &Pool,
    schema: &str,
    row: &DueDeadlineRow,
    recurrence: Option<&Recurrence>,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let Some(recurrence) = recurrence else {
        return Ok(());
    };
    let Some(next_fire_at) = crate::plugin::next_recurrence_fire_at(
        &crate::plugin::RecurrenceSpec {
            schedule: recurrence.schedule.clone(),
            policy: recurrence.policy,
            start_at: recurrence.start_at,
            end_at: recurrence.end_at,
            valid_until: recurrence.valid_until,
        },
        row.fire_at,
        now,
    ) else {
        if crate::event_store::next_occurrence_after(&recurrence.schedule, row.fire_at).is_none()
            && crate::event_store::latest_occurrence_at_or_before(&recurrence.schedule, now)
                .is_none()
        {
            tracing::warn!(
                deadline_id = %row.id,
                schedule = %recurrence.schedule,
                "deadline's own recurrence schedule does not parse - ending the chain"
            );
        }
        return Ok(());
    };
    let parent_id = recurrence.parent_id.as_deref().unwrap_or(&row.id);
    let next_sequence = recurrence
        .sequence
        .map(|sequence| sequence + 1)
        .unwrap_or(1);
    let next_id = format!("{parent_id}#r{next_sequence}");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.deadlines \
         (id, schedule_name, fire_at, expires_at, tags, correlation_id, target_bounded_context, \
          target_command_type, payload, status, created_at, recurrence_schedule, \
          recurrence_policy, recurrence_start_at, recurrence_end_at, \
          recurrence_valid_until, recurrence_parent_id, recurrence_sequence) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', $10, $11, $12, $13, $14, \
                 $15, $16, $17) \
         ON CONFLICT (id) DO NOTHING"
    )))
    .bind(&next_id)
    .bind(&row.schedule_name)
    .bind(next_fire_at)
    .bind(row.expires_at)
    .bind(&row.tags)
    .bind(row.correlation_id.as_deref())
    .bind(&row.target_bounded_context)
    .bind(&row.target_command_type)
    .bind(&row.payload)
    .bind(now)
    .bind(&recurrence.schedule)
    .bind(recurrence_policy_to_str(recurrence.policy))
    .bind(recurrence.start_at)
    .bind(recurrence.end_at)
    .bind(recurrence.valid_until)
    .bind(parent_id)
    .bind(next_sequence)
    .execute(pool)
    .await?;
    Ok(())
}

/// `WHERE status IN ('pending', 'firing')` - called both directly
/// against a still-`'pending'` row (the "target `CommandType` isn't
/// registered" path, no claim taken) and against a row this same tick
/// already claimed into `'firing'` (the normal fire path) - either way
/// idempotent: a status that's already terminal (`'fired'`/`'cancelled'`)
/// never matches, so a redelivered/duplicate call is a safe no-op.
/// A deadline whose command submission failed with an error: back to
/// `pending` until its next attempt, or - once `retry_policy` is
/// exhausted - parked in the target bounded context and marked `parked`
/// (docs/architecture.md §115). Only this instance's own claim is
/// updated (`status = 'firing'`): a reclaim racing it leaves it alone.
/// How long work this instance can't do, but another can
/// ([`crate::error::Error::another_instance_can_do_it`]), waits before
/// being tried again - by whichever instance claims it then
/// (docs/architecture.md §161).
pub const ANOTHER_INSTANCE_RETRY_DELAY: std::time::Duration =
    skilj_retry::ANOTHER_INSTANCE_RETRY_DELAY;

/// Releases a claimed deadline this instance can't fire, for another
/// instance to take after [`ANOTHER_INSTANCE_RETRY_DELAY`]. Unlike
/// [`record_deadline_failure`] it spends no attempt and never parks.
pub(super) async fn defer_deadline_to_another_instance(
    pool: &Pool,
    schema: &str,
    row: &DueDeadlineRow,
    error: &crate::error::Error,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let next_attempt_at = now
        + chrono::Duration::from_std(ANOTHER_INSTANCE_RETRY_DELAY)
            .expect("a ten-second delay fits a chrono::Duration");
    tracing::warn!(
        deadline_id = %row.id,
        error = %error,
        next_attempt_at = %next_attempt_at,
        "deadline: this instance can't fire it, another can - retrying later"
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.deadlines SET status = 'pending', firing_at = NULL, \
         next_attempt_at = $1 WHERE id = $2 AND status = 'firing'"
    )))
    .bind(next_attempt_at)
    .bind(&row.id)
    .execute(pool)
    .await?;
    Ok(())
}

pub(super) async fn record_deadline_failure(
    pool: &Pool,
    schema: &str,
    row: &DueDeadlineRow,
    error: &crate::error::Error,
    retry_policy: &skilj_retry::RetryPolicy,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let attempt_count = row.attempt_count + 1;
    let first_failed_at = row.first_failed_at.unwrap_or(now);
    let elapsed = (now - first_failed_at).to_std().unwrap_or_default();
    let message = error.to_string();
    if retry_policy.is_exhausted(attempt_count as u32, elapsed) {
        tracing::error!(
            deadline_id = %row.id,
            error = %message,
            attempt = attempt_count,
            "deadline: target command submission failed repeatedly - parking it"
        );
        let request_json = serde_json::from_str(&row.payload)
            .unwrap_or_else(|_| serde_json::Value::String(row.payload.clone()));
        insert_parked_delivery(
            pool,
            &row.target_bounded_context,
            &format!("{DEADLINE_PARKED_SOURCE_PREFIX}{}", row.schedule_name),
            ParkedDeliveryKind::Deadline,
            &row.id,
            None,
            Some(&row.target_bounded_context),
            Some(&row.target_command_type),
            &request_json,
            &message,
            attempt_count,
            first_failed_at,
            now,
        )
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.deadlines SET status = 'parked', resolved_at = $1, \
             attempt_count = $2, last_error = $3, payload = '{{}}' \
             WHERE id = $4 AND status = 'firing'"
        )))
        .bind(now)
        .bind(attempt_count)
        .bind(&message)
        .bind(&row.id)
        .execute(pool)
        .await?;
        return Ok(());
    }
    let next_attempt_at = retry_policy.next_attempt_at(now, attempt_count as u32);
    tracing::warn!(
        deadline_id = %row.id,
        error = %message,
        attempt = attempt_count,
        next_attempt_at = %next_attempt_at,
        "deadline: target command submission failed - will retry"
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.deadlines SET status = 'pending', firing_at = NULL, \
         attempt_count = $1, first_failed_at = $2, next_attempt_at = $3, last_error = $4 \
         WHERE id = $5 AND status = 'firing'"
    )))
    .bind(attempt_count)
    .bind(first_failed_at)
    .bind(next_attempt_at)
    .bind(&message)
    .bind(&row.id)
    .execute(pool)
    .await?;
    Ok(())
}

pub(super) async fn mark_deadline_resolved(
    pool: &Pool,
    schema: &str,
    id: &str,
    status: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.deadlines SET status = $1, resolved_at = $2, payload = '{{}}' \
         WHERE id = $3 AND status IN ('pending', 'firing')"
    )))
    .bind(status)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}
