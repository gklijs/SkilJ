//! Persistence for projection consumers rule groups.

use super::*;

/// issue #15: the batched replacement for looping every
/// registered projection through `get_projection_rebuild` - that path
/// cost up to `~2P` queries per tick for `P` projections even when
/// nothing was building (`get_projection_rebuild` alone is 3 queries:
/// `get_projection`, the rebuild row, `rebuild_consumed_event_types` -
/// and that last one has its own inner N+1, one `get_event_type` call
/// per consumed event type name). This does the same job in a small
/// constant number of queries regardless of `P` or how many rebuilds
/// are actually building: one query for every `building` row in the
/// bc's `projection_rebuilds` table, one for every row in
/// `projection_rebuild_consumed_event_types` (grouped by
/// `projection_name` in memory), one for every `EventType` in the bc
/// (`list_event_types_for_bounded_context`, already a single query),
/// and zero further queries for the live `Projection` each rebuild
/// belongs to - `all_projections` is data the caller
/// (`catch_up_bounded_context`) already fetched, handed in here instead
/// of re-fetched via `get_project` per row the way `get_projection_rebuild`
/// does for its own single-row callers (`RebuildProjection`/
/// `DiscardProjectionRebuild`'s resolvers - untouched, a single lookup
/// is the right shape there).
pub(super) async fn list_building_projection_rebuilds_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
    all_projections: &[Projection],
) -> crate::error::Result<Vec<ProjectionRebuild>> {
    let schema = schema_ident(bounded_context);

    let rebuild_rows: Vec<ProjectionRebuildRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_REBUILD_COLUMNS} FROM {schema}.projection_rebuilds \
         WHERE status = 'building'"
    )))
    .fetch_all(pool)
    .await?;
    if rebuild_rows.is_empty() {
        return Ok(Vec::new());
    }

    let consumed_rows: Vec<(String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT projection_name, event_type_name \
         FROM {schema}.projection_rebuild_consumed_event_types WHERE status = 'building'"
    )))
    .fetch_all(pool)
    .await?;
    let mut consumed_by_projection: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for (projection_name, event_type_name) in consumed_rows {
        consumed_by_projection
            .entry(projection_name)
            .or_default()
            .push(event_type_name);
    }

    let all_event_types = list_event_types_for_bounded_context(pool, bounded_context).await?;
    let event_types_by_name: std::collections::HashMap<&str, &EventType> = all_event_types
        .iter()
        .map(|et| (et.name.as_str(), et))
        .collect();

    let projections_by_name: std::collections::HashMap<&str, &Projection> = all_projections
        .iter()
        .map(|p| (p.name.as_str(), p))
        .collect();

    let mut rebuilds = Vec::with_capacity(rebuild_rows.len());
    for row in rebuild_rows {
        let Some(projection) = projections_by_name.get(row.projection_name.as_str()) else {
            continue;
        };
        let consumed_event_types = consumed_by_projection
            .remove(&row.projection_name)
            .unwrap_or_default()
            .into_iter()
            .map(|name| {
                event_types_by_name
                    .get(name.as_str())
                    .copied()
                    .cloned()
                    .expect(
                        "projection_rebuild_consumed_event_types join row references an \
                         event_types row that no longer exists",
                    )
            })
            .collect();
        rebuilds.push(ProjectionRebuild {
            projection: (*projection).clone(),
            schema: row.schema,
            schema_version: row.schema_version,
            consumed_event_types,
            sync: row.sync,
            caught_up_to: row.caught_up_to,
            status: projection_rebuild_status_from_str(&row.status),
        });
    }
    Ok(rebuilds)
}

/// The background half of [§8](../../../docs/architecture.md#open-for-a-future-pass) item 6: one poll tick, for one bounded
/// context - folds every committed event not yet reflected in that
/// context's `sync = false` `Projection`s, and separately walks any
/// `building` `ProjectionRebuild`s toward completion, promoting each one
/// automatically once it catches up to the same high-water mark. Called
/// in a loop, on a timer, by the single shared background task
/// `SkiljBuilder::build()` spawns - see that function's own doc comment
/// for why one shared task rather than one per bounded context.
///
/// Uses the `-1` "nothing yet" sentinel `next_sequence`'s own `sequence`
/// row is seeded with throughout, rather than threading `Option<i64>`
/// through every comparison - `caught_up_to.unwrap_or(-1)` and
/// `latest_sequence(...).unwrap_or(-1)` compare directly, and a
/// building rebuild in a bounded context with no events at all (`latest
/// = -1`) is then trivially already caught up (`caught_up_to.unwrap_or(-1)
/// == -1`) and promotes immediately, with no separate "nothing committed
/// yet" special case needed.
///
/// A rebuild whose `caught_up_to` is `None` - freshly staged, or
/// restarted while `building` (a `rebuildProjection` arriving mid-build,
/// see `transition_projection_rebuild_to_building`) - has its
/// `projection_rebuild_state` rows deleted in the transaction that folds
/// its first event, under the rebuild row's lock: `ProjectionDispatcher::
/// default_state` resolved fresh, right before the first event actually
/// gets folded, is the only correct starting point once that reset has
/// happened; a row left over from a build attempt this same reset just
/// invalidated would otherwise be silently reused as if it were still
/// current. Each fold re-reads the rebuild's `caught_up_to` under that
/// lock and goes ahead only if it's exactly the previous event's
/// sequence, so an instance working from a stale snapshot never folds a
/// restarted rebuild, and never deletes what another instance just folded
/// (docs/architecture.md §151). A rebuild
/// this dispatcher can't resolve at all (no compiled type registered in
/// this process for that name) still advances - `state` frozen at `"{}"`,
/// a neutral placeholder never actually deserialised by anything, since
/// `dispatcher.project()` returns `None` for the same lookup key
/// `default_state()` did - the same "position always advances, state
/// only changes when the dispatcher can" treatment sync projections
/// already get from `insert_event_and_update_sync_projections` above.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn catch_up_bounded_context(
    pool: &Pool,
    bounded_context: &str,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let latest = latest_sequence(pool, bounded_context).await?.unwrap_or(-1);

    // docs/architecture.md §160: only the projections this instance's
    // dispatcher knows. One it doesn't - declared by a newer version
    // during a rolling deploy, or no longer declared at all - has no keys
    // and no fold here, and walking it anyway advanced its `caught_up_to`
    // past events nothing folded. It's left to an instance that knows it.
    let all_projections: Vec<Projection> =
        list_projections_for_bounded_context(pool, bounded_context)
            .await?
            .into_iter()
            .filter(|p| dispatcher.default_state(bounded_context, &p.name).is_some())
            .collect();
    // issue #25 (docs/architecture.md §51) - every async
    // projection is split here by its own `Projection::PARTITION_COUNT`
    // (via the dispatcher, since that's a Rust-only config the domain
    // `Projection` struct itself never carries). `unpartitioned_async_projections`
    // (`<= 1`, the default, and every projection that predates this
    // feature) feeds the exact same loop this function has always had,
    // completely unchanged below - order is kept simply by never
    // entering the new code path. `.max(1)` guards a misconfigured
    // `PARTITION_COUNT = 0` against a later divide-by-zero.
    let unpartitioned_async_projections: Vec<_> = all_projections
        .iter()
        .filter(|p| {
            !p.sync
                && dispatcher
                    .partition_count(bounded_context, &p.name)
                    .unwrap_or(1)
                    .max(1)
                    <= 1
        })
        .collect();
    let partitioned_async_projections: Vec<(&Projection, u32)> = all_projections
        .iter()
        .filter(|p| !p.sync)
        .filter_map(|p| {
            let partition_count = dispatcher
                .partition_count(bounded_context, &p.name)
                .unwrap_or(1)
                .max(1);
            (partition_count > 1).then_some((p, partition_count))
        })
        .collect();
    let building_rebuilds = list_building_projection_rebuilds_for_bounded_context(
        pool,
        bounded_context,
        &all_projections,
    )
    .await?;

    if unpartitioned_async_projections.is_empty()
        && partitioned_async_projections.is_empty()
        && building_rebuilds.is_empty()
    {
        return Ok(());
    }

    let min_caught_up = unpartitioned_async_projections
        .iter()
        .map(|p| p.caught_up_to.unwrap_or(-1))
        .chain(
            building_rebuilds
                .iter()
                .map(|r| r.caught_up_to.unwrap_or(-1)),
        )
        .min()
        .unwrap_or(latest);

    let events = if min_caught_up >= latest {
        Vec::new()
    } else {
        // At most `MAX_EVENTS_PER_CATCH_UP_TICK`: progress persists per
        // event, so a new projection/rebuild/snapshot over a long history
        // catches up over several ticks instead of loading it whole.
        list_events_for_bounded_context_from_limited(
            pool,
            bounded_context,
            min_caught_up,
            MAX_EVENTS_PER_CATCH_UP_TICK,
        )
        .await?
    };

    // The sequence of the bounded context's event just before the chunk
    // being folded: `events` is every event after `min_caught_up`, in
    // order, so a rebuild may fold an event only if it has processed
    // exactly up to the one before it (docs/architecture.md §151). A chunk
    // of events per transaction, not one (§182).
    let mut previous = min_caught_up;
    for chunk in events.chunks(CATCH_UP_EVENTS_PER_TRANSACTION) {
        let folded = fold_catch_up_chunk(
            pool,
            bounded_context,
            &schema,
            dispatcher,
            &unpartitioned_async_projections,
            &building_rebuilds,
            chunk,
            previous,
        )
        .await;
        if let Err(failure) = folded {
            // A failing event doesn't take the events before it in its
            // chunk down with it: those commit on their own, as they did
            // with one transaction per event, and the error is returned.
            // The failure blamed is not always the earliest: a shorter
            // run can fail again earlier, and is then shortened to that
            // (docs/architecture.md §200).
            let mut end = failure.folded_before;
            while end > 0 {
                match fold_catch_up_chunk(
                    pool,
                    bounded_context,
                    &schema,
                    dispatcher,
                    &unpartitioned_async_projections,
                    &building_rebuilds,
                    &chunk[..end],
                    previous,
                )
                .await
                {
                    Ok(()) => break,
                    Err(retry) if retry.folded_before < end => end = retry.folded_before,
                    Err(retry) => return Err(retry.error),
                }
            }
            return Err(failure.error);
        }
        previous = chunk.last().expect("chunks are never empty").sequence;
    }

    for rebuild in &building_rebuilds {
        // It can have vanished since the fold loop above: another
        // instance's overlapping tick may have promoted it (deleting the
        // building row), or a `hard_delete_bounded_context` may have
        // landed. Either way there's nothing left to promote - and a panic
        // here would take down the one background task every bounded
        // context's catch-up runs on, not just this tick.
        let Some(current) = get_projection_rebuild(
            pool,
            bounded_context,
            &rebuild.projection.name,
            ProjectionRebuildStatus::Building,
        )
        .await?
        else {
            continue;
        };
        if current.caught_up_to.unwrap_or(-1) == latest {
            // A cheap fast-path filter, not the authoritative check
            // anymore (drift audit finding #6) - `promote_projection_rebuild`
            // re-verifies for real under its own lock, so a `false`
            // return here is an expected, self-correcting outcome (a new
            // event landed in the gap between this snapshot and that
            // lock) rather than something this caller needs to react to;
            // the next tick retries.
            let _ =
                promote_projection_rebuild(pool, bounded_context, &rebuild.projection.name).await?;
        }
    }

    // issue #25 (docs/architecture.md §51) - sequential across
    // partitioned projections, not `for_each_concurrent`: the real
    // scale-out value comes from separate *instances* racing for
    // partitions, not from one instance's own intra-tick fan-out, and
    // stacking more concurrency here on top of this function's own
    // already-concurrent per-bounded-context caller
    // (`BACKGROUND_TASK_CONCURRENCY` in `skilj/src/lib.rs`) against a
    // pool whose default cap is modest (docs/architecture.md §35) risks
    // exhausting it for little gain.
    for (projection, partition_count) in partitioned_async_projections {
        catch_up_partitioned_projection(
            pool,
            bounded_context,
            &schema,
            dispatcher,
            projection,
            partition_count,
            latest,
        )
        .await?;
    }

    Ok(())
}

/// How many events `catch_up_bounded_context` folds per transaction
/// (docs/architecture.md §182). One per event paid, per event, a commit,
/// a lock and re-read of every projection row, and a read and a write of
/// every key it touched; a chunk pays them once, with each projection's
/// keys read and written in one statement. Bounded because the chunk's
/// transaction holds each folded projection's row lock throughout, which
/// a registration or promotion of that projection waits on.
pub(super) const CATCH_UP_EVENTS_PER_TRANSACTION: usize = 100;

/// Why [`fold_catch_up_chunk`] failed, and how many of the chunk's events
/// came before the one it failed on - `0` when the failure isn't one
/// event's (the transaction itself, or reading or writing the chunk's
/// rows).
pub(super) struct CatchUpChunkFailure {
    pub(super) folded_before: usize,
    pub(super) error: crate::error::Error,
}

/// One key's state while a chunk is folded: read and locked before the
/// chunk's events are folded into it, written back once after.
pub(super) struct CatchUpChunkState {
    pub(super) as_of_sequence: i64,
    pub(super) state: String,
    pub(super) owner: Option<String>,
    pub(super) folded: bool,
}

/// One projection or `building` rebuild a chunk folds into.
pub(super) struct CatchUpChunkTarget<'a> {
    pub(super) name: &'a str,
    pub(super) rebuild: bool,
    /// `caught_up_to` as read under the row's lock.
    pub(super) locked: Option<i64>,
    /// `caught_up_to` once the chunk is folded.
    pub(super) position: Option<i64>,
    /// Every key the chunk's events touch, in the order first touched.
    pub(super) keys: Vec<String>,
    pub(super) states: std::collections::HashMap<String, CatchUpChunkState>,
}

/// Folds `events` - a contiguous run of the bounded context's events,
/// the first one right after `previous` - into the unpartitioned async
/// `projections` and the `building` `rebuilds`, in one transaction.
/// Exactly what folding them one transaction per event did, only
/// committed together: every check the per-event loop made against a
/// re-read row is made once, under that row's lock, which the transaction
/// then holds to the end - so it can't change in between.
///
/// Locks are taken in the order the per-event loop took them and
/// promotion takes them (docs/architecture.md §156): every projection row
/// first, then every rebuild row, each by name so two instances' chunks
/// can't take two of them in opposite orders, then their state rows.
///
/// `FOR NO KEY UPDATE`, not `FOR UPDATE` (docs/architecture.md §195): it
/// still excludes another chunk and a promotion, but not the `KEY SHARE`
/// a sync projection's state insert takes on its projection row through
/// the foreign key. A row the caller saw as async may have turned sync
/// by the time it is locked, and a `FOR UPDATE` kept on it to the end of
/// the chunk stalled every write folding into it, under the sequence
/// lock. Promotion doesn't wait for a chunk either; see
/// [`promote_projection_rebuild`].
#[allow(clippy::too_many_arguments)]
pub(super) async fn fold_catch_up_chunk(
    pool: &Pool,
    bounded_context: &str,
    schema: &str,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    projections: &[&Projection],
    rebuilds: &[ProjectionRebuild],
    events: &[Event],
    previous: i64,
) -> Result<(), CatchUpChunkFailure> {
    let mut reached = 0;
    fold_catch_up_chunk_in_tx(
        pool,
        bounded_context,
        schema,
        dispatcher,
        projections,
        rebuilds,
        events,
        previous,
        &mut reached,
    )
    .await
    .map_err(|error| CatchUpChunkFailure {
        folded_before: reached,
        error,
    })
}

/// [`fold_catch_up_chunk`]'s body - `reached` is the index of the event
/// being folded, `0` outside the fold itself.
#[allow(clippy::too_many_arguments)]
pub(super) async fn fold_catch_up_chunk_in_tx(
    pool: &Pool,
    bounded_context: &str,
    schema: &str,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    projections: &[&Projection],
    rebuilds: &[ProjectionRebuild],
    events: &[Event],
    previous: i64,
    reached: &mut usize,
) -> crate::error::Result<()> {
    let Some(chunk_end) = events.last().map(|event| event.sequence) else {
        return Ok(());
    };
    let mut tx = pool.begin().await?;
    let mut targets: Vec<CatchUpChunkTarget> = Vec::new();

    // Re-read under the lock, a projection already past the chunk, turned
    // sync or gone since the caller's snapshot is left alone.
    let mut candidates: Vec<&Projection> = projections
        .iter()
        .copied()
        .filter(|p| p.caught_up_to.unwrap_or(-1) < chunk_end)
        .collect();
    candidates.sort_by(|a, b| a.name.cmp(&b.name));
    for projection in candidates {
        let current: Option<(Option<i64>, bool)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT caught_up_to, sync FROM {schema}.projections WHERE name = $1 \
             FOR NO KEY UPDATE"
        )))
        .bind(&projection.name)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((caught_up_to, false)) = current {
            if live_fold_paused(&mut tx, bounded_context, &projection.name, dispatcher).await? {
                continue;
            }
            targets.push(CatchUpChunkTarget {
                name: &projection.name,
                rebuild: false,
                locked: caught_up_to,
                position: caught_up_to,
                keys: Vec::new(),
                states: std::collections::HashMap::new(),
            });
        }
    }

    // docs/architecture.md §151: a rebuild promoted, gone or restarted
    // since the caller's snapshot is seen here, under its lock.
    let mut candidates: Vec<&ProjectionRebuild> = rebuilds
        .iter()
        .filter(|r| r.caught_up_to.unwrap_or(-1) < chunk_end)
        .collect();
    candidates.sort_by(|a, b| a.projection.name.cmp(&b.projection.name));
    for rebuild in candidates {
        let current: Option<(Option<i64>,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT caught_up_to FROM {schema}.projection_rebuilds \
             WHERE projection_name = $1 AND status = 'building' FOR NO KEY UPDATE"
        )))
        .bind(&rebuild.projection.name)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some((caught_up_to,)) = current {
            targets.push(CatchUpChunkTarget {
                name: &rebuild.projection.name,
                rebuild: true,
                locked: caught_up_to,
                position: caught_up_to,
                keys: Vec::new(),
                states: std::collections::HashMap::new(),
            });
        }
    }

    // Which target folds which event, and with which keys, in the order
    // the per-event loop folded them. A live projection folds every event
    // past its position; a rebuild only an event right after what it has
    // processed (§151) - and from then on every one, since it then has.
    let mut planned: Vec<(usize, usize, Vec<String>)> = Vec::new();
    let mut seen: Vec<std::collections::HashSet<String>> =
        vec![std::collections::HashSet::new(); targets.len()];
    let mut before = previous;
    for (index, event) in events.iter().enumerate() {
        for (slot, target) in targets.iter_mut().enumerate() {
            let folds = if target.rebuild {
                target.position.unwrap_or(-1) == before
            } else {
                target.position.unwrap_or(-1) < event.sequence
            };
            if !folds {
                continue;
            }
            // `Some(vec![])`/`None` both mean zero instances touched -
            // see `insert_event_and_update_sync_projections`'s own
            // identical comment.
            let keys = dispatcher
                .keys(bounded_context, target.name, event)
                .unwrap_or_default();
            for key in &keys {
                if seen[slot].insert(key.clone()) {
                    target.keys.push(key.clone());
                }
            }
            planned.push((index, slot, keys));
            target.position = Some(event.sequence);
        }
        before = event.sequence;
    }

    for target in &mut targets {
        if target.rebuild && target.locked.is_none() && target.position.is_some() {
            // The first fold of this build: state left by a build it
            // restarted is discarded - under the lock, so no instance can
            // be folding into it meanwhile.
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {schema}.projection_rebuild_state \
                 WHERE projection_name = $1 AND status = 'building'"
            )))
            .bind(target.name)
            .execute(&mut *tx)
            .await?;
        }
        if target.keys.is_empty() {
            continue;
        }
        let default_state_json = dispatcher
            .default_state(bounded_context, target.name)
            .unwrap_or_default();
        target.states = lock_chunk_states(
            &mut tx,
            schema,
            target.rebuild,
            target.name,
            &target.keys,
            &default_state_json,
        )
        .await?;
    }

    for (index, slot, keys) in &planned {
        *reached = *index;
        let event = &events[*index];
        let target = &mut targets[*slot];
        let owner = event_owner(
            dispatcher
                .owner_tag_key(bounded_context, target.name)
                .flatten(),
            event,
        );
        fold_into_chunk_states(
            dispatcher,
            bounded_context,
            target.name,
            &mut target.states,
            keys,
            event,
            &owner,
        )?;
    }
    *reached = 0;

    for target in &targets {
        write_chunk_states(&mut tx, schema, target.rebuild, target.name, &target.states).await?;

        if target.position == target.locked {
            continue;
        }
        // Never backwards: another instance's concurrent catch-up may
        // already have moved it further (docs/architecture.md §113). `AND
        // status = 'building'`: a coexisting pending row shares the
        // `projection_name`, and must keep its `caught_up_to` `None` until
        // it's promoted or discarded.
        let query = if target.rebuild {
            format!(
                "UPDATE {schema}.projection_rebuilds SET caught_up_to = $1 \
                 WHERE projection_name = $2 AND status = 'building' \
                 AND (caught_up_to IS NULL OR caught_up_to < $1)"
            )
        } else {
            format!(
                "UPDATE {schema}.projections SET caught_up_to = $1 \
                 WHERE name = $2 AND (caught_up_to IS NULL OR caught_up_to < $1)"
            )
        };
        sqlx::query(sqlx::AssertSqlSafe(query))
            .bind(target.position)
            .bind(target.name)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Get-or-create-with-lock of every one of `keys` at once, in the order
/// given - see `get_or_create_projection_state_for_update`. `rebuild`
/// picks the `building` rebuild's state table instead of the live one.
pub(super) async fn lock_chunk_states(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    rebuild: bool,
    projection_name: &str,
    keys: &[String],
    default_state_json: &str,
) -> crate::error::Result<std::collections::HashMap<String, CatchUpChunkState>> {
    let rows: Vec<(String, i64, String)> = if rebuild {
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.projection_rebuild_state \
             (projection_name, status, key, state, updated_at) \
             SELECT $1, 'building', key, $3, now() FROM unnest($2::text[]) AS key \
             ON CONFLICT (projection_name, status, key) DO UPDATE SET \
             state = {schema}.projection_rebuild_state.state \
             RETURNING key, as_of_sequence, state"
        )))
    } else {
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.projection_state (projection_name, key, state, updated_at) \
             SELECT $1, key, $3, now() FROM unnest($2::text[]) AS key \
             ON CONFLICT (projection_name, key) DO UPDATE SET \
             state = {schema}.projection_state.state \
             RETURNING key, as_of_sequence, state"
        )))
    }
    .bind(projection_name)
    .bind(keys)
    .bind(default_state_json)
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(key, as_of_sequence, state)| {
            let folded = CatchUpChunkState {
                as_of_sequence,
                state,
                owner: None,
                folded: false,
            };
            (key, folded)
        })
        .collect())
}

/// Folds `event` into each of `keys`' states, keeping the
/// `as_of_sequence` guard (issue #25): another instance may have
/// folded a row past `event` already. `owner` is the one `event` gives
/// (`event_owner`).
pub(super) fn fold_into_chunk_states(
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    bounded_context: &str,
    projection_name: &str,
    states: &mut std::collections::HashMap<String, CatchUpChunkState>,
    keys: &[String],
    event: &Event,
    owner: &Option<String>,
) -> crate::error::Result<()> {
    for key in keys {
        let entry = states
            .get_mut(key)
            .expect("every planned key's row was just read");
        if entry.as_of_sequence >= event.sequence {
            continue;
        }
        if let Some(result) =
            dispatcher.project(bounded_context, projection_name, &entry.state, event, key)
        {
            entry.state = result?;
        }
        entry.as_of_sequence = event.sequence;
        if owner.is_some() {
            entry.owner.clone_from(owner);
        }
        entry.folded = true;
    }
    Ok(())
}

/// Writes every folded one of `states` back in one statement. A `NULL`
/// owner leaves the stored one as it is - see
/// `apply_projection_fold_update`.
pub(super) async fn write_chunk_states(
    conn: &mut sqlx::PgConnection,
    schema: &str,
    rebuild: bool,
    projection_name: &str,
    states: &std::collections::HashMap<String, CatchUpChunkState>,
) -> crate::error::Result<()> {
    let mut keys = Vec::new();
    let mut folded_states = Vec::new();
    let mut owners = Vec::new();
    let mut as_of_sequences = Vec::new();
    for (key, folded) in states.iter().filter(|(_, s)| s.folded) {
        keys.push(key.as_str());
        folded_states.push(folded.state.as_str());
        owners.push(folded.owner.as_deref());
        as_of_sequences.push(folded.as_of_sequence);
    }
    if keys.is_empty() {
        return Ok(());
    }
    let (table, extra_where) = if rebuild {
        ("projection_rebuild_state", " AND s.status = 'building'")
    } else {
        ("projection_state", "")
    };
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.{table} AS s SET state = u.state, \
         owner = COALESCE(u.owner, s.owner), as_of_sequence = u.as_of_sequence, \
         updated_at = now() \
         FROM unnest($2::text[], $3::text[], $4::text[], $5::bigint[]) \
         AS u(key, state, owner, as_of_sequence) \
         WHERE s.projection_name = $1 AND s.key = u.key{extra_where}"
    )))
    .bind(projection_name)
    .bind(&keys)
    .bind(&folded_states)
    .bind(&owners)
    .bind(&as_of_sequences)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// `catch_up_bounded_context`'s own per-tick body for one async
/// `Projection` whose `Projection::PARTITION_COUNT > 1` (issue
/// #25's investigation, docs/architecture.md §50/§51 - see that
/// section for the full design writeup). Reuses `keys()` itself as the
/// partitioning input - no separate "partition key" concept - hashing
/// each key `dispatcher.keys()` returns into one of `partition_count`
/// buckets via `partition_for_key` (a hand-written, cross-version-stable
/// FNV-1a, not `std::collections::hash_map::DefaultHasher`, which is
/// explicitly not guaranteed stable across Rust versions and would risk
/// two instances silently disagreeing on a key's own bucket).
///
/// Claiming is a non-blocking, per-tick race for each partition's own
/// `pg_try_advisory_xact_lock`, the identical primitive
/// `migrate_idempotency_keys_client_id_scoping` already establishes -
/// whichever instance wins a given partition this tick folds every
/// pending key in that bucket, inside the one transaction that holds
/// the lock; a losing instance simply skips that partition this tick,
/// with no lease/heartbeat/expiry machinery needed since a dead
/// instance just stops winning locks. **This lock is a pure
/// work-avoidance optimization, not a correctness mechanism**: every
/// fold below still goes through the exact same
/// `get_or_create_projection_state_for_update` → `as_of_sequence` guard
/// → `dispatcher.project()` → `apply_projection_fold_update` sequence
/// the unpartitioned loop above uses, so even a hash disagreement
/// between instances (a theoretical `hashtext` collision, or a rolling
/// deploy briefly running two different `PARTITION_COUNT` values) could
/// only ever cause wasted duplicate work, never a double-fold.
///
/// One transaction per `(partition, tick)`, not per-event: a
/// non-blocking lock re-acquired per event would force stopping at the
/// first lost race anyway, since a partition's own progress can only
/// advance contiguously in sequence order - far more lock round trips
/// for no real benefit. The real cost this trades away, relative to the
/// unpartitioned loop's own per-event transaction granularity, is
/// durability: a poison event anywhere in a partition's own batch this
/// tick rolls back that whole partition's tick, not just the offending
/// event - `MAX_EVENTS_PER_PARTITION_TICK` bounds the blast radius, and
/// the next tick's fresh, non-blocking race is self-healing regardless
/// of which instance (if any) previously held that partition.
///
/// Deliberately excludes a `ProjectionRebuild`'s own `building` fold and
/// `fold_history_into_new_sync_projection` - both stay single-instance,
/// exactly as before `PARTITION_COUNT` existed. Rebuilds are one-time,
/// bounded events (only `schema_changed`/`consumed_change_has_history`/
/// `becoming_sync` in `register_projection` ever stage one), not the
/// sustained-throughput concern issue #25 is actually about.
///
/// **Known accepted limitation**: after a rebuild promotes
/// (`promote_projection_rebuild`), this projection's own
/// `projection_partition_progress` rows can be stale relative to the
/// freshly-jumped `projections.caught_up_to` the rebuild just set - the
/// next tick(s) will re-scan the gap (capped, self-healing over a few
/// ticks by `MAX_EVENTS_PER_PARTITION_TICK`) purely as wasted work,
/// never a correctness risk, since `as_of_sequence` still skips every
/// already-folded key/event pair it re-scans. Not worth promotion-side
/// bookkeeping to close for what is a rare, self-correcting cost.
#[allow(clippy::too_many_arguments)]
pub(super) async fn catch_up_partitioned_projection(
    pool: &Pool,
    bounded_context: &str,
    schema: &str,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    projection: &Projection,
    partition_count: u32,
    latest: i64,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_partition_progress \
         (projection_name, partition_index, caught_up_to, updated_at) \
         SELECT $1, gs, -1, now() FROM generate_series(0, $2::int - 1) AS gs \
         ON CONFLICT (projection_name, partition_index) DO NOTHING"
    )))
    .bind(&projection.name)
    .bind(partition_count as i32)
    .execute(pool)
    .await?;

    let mut progress = list_projection_partition_progress(pool, schema, &projection.name).await?;
    let min_caught_up = (0..partition_count)
        .map(|p| progress.get(&p).copied().unwrap_or(-1))
        .min()
        .unwrap_or(latest);

    let events = if min_caught_up >= latest {
        Vec::new()
    } else {
        list_events_for_bounded_context_from_limited(
            pool,
            bounded_context,
            min_caught_up,
            MAX_EVENTS_PER_PARTITION_TICK,
        )
        .await?
    };

    if !events.is_empty() {
        let batch_end = events
            .last()
            .expect("just checked events is non-empty")
            .sequence;
        let default_state_json = dispatcher
            .default_state(bounded_context, &projection.name)
            .unwrap_or_default();
        let owner_tag_key = dispatcher
            .owner_tag_key(bounded_context, &projection.name)
            .flatten();

        for partition_index in 0..partition_count {
            if progress.get(&partition_index).copied().unwrap_or(-1) >= batch_end {
                continue;
            }

            let mut tx = pool.begin().await?;
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "SELECT 1 FROM {schema}.projections WHERE name = $1 FOR NO KEY UPDATE"
            )))
            .bind(&projection.name)
            .execute(&mut *tx)
            .await?;
            if live_fold_paused(&mut tx, bounded_context, &projection.name, dispatcher).await? {
                return Ok(());
            }

            let locked: bool =
                sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1)::bigint)")
                    .bind(partition_lock_key(
                        bounded_context,
                        &projection.name,
                        partition_index,
                    ))
                    .fetch_one(&mut *tx)
                    .await?;
            if !locked {
                // Nothing was written under this transaction - dropping
                // it is a no-op rollback, not a partial commit to worry
                // about. Another instance (or this one, next tick) owns
                // this partition for now.
                continue;
            }

            // Every key of this partition read and locked once, folded
            // in memory, and written once (docs/architecture.md §197), as
            // the unpartitioned chunk does (§182). Keys are locked in
            // sorted order. The transaction was already all or nothing.
            let after = progress.get(&partition_index).copied().unwrap_or(-1);
            let mut planned: Vec<(&Event, Vec<String>)> = Vec::new();
            let mut keys: Vec<String> = Vec::new();
            for event in events.iter().filter(|e| e.sequence > after) {
                let event_keys: Vec<String> = dispatcher
                    .keys(bounded_context, &projection.name, event)
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|key| partition_for_key(key, partition_count) == partition_index)
                    .collect();
                if !event_keys.is_empty() {
                    keys.extend(event_keys.iter().cloned());
                    planned.push((event, event_keys));
                }
            }
            keys.sort_unstable();
            keys.dedup();
            if !keys.is_empty() {
                let mut states = lock_chunk_states(
                    &mut tx,
                    schema,
                    false,
                    &projection.name,
                    &keys,
                    &default_state_json,
                )
                .await?;
                for (event, event_keys) in &planned {
                    fold_into_chunk_states(
                        dispatcher,
                        bounded_context,
                        &projection.name,
                        &mut states,
                        event_keys,
                        event,
                        &event_owner(owner_tag_key, event),
                    )?;
                }
                write_chunk_states(&mut tx, schema, false, &projection.name, &states).await?;
            }
            let this_partition_progress = after.max(batch_end);

            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {schema}.projection_partition_progress \
                 (projection_name, partition_index, caught_up_to, updated_at) \
                 VALUES ($1, $2, $3, now()) \
                 ON CONFLICT (projection_name, partition_index) DO UPDATE SET \
                    caught_up_to = GREATEST(\
                        {schema}.projection_partition_progress.caught_up_to, EXCLUDED.caught_up_to), \
                    updated_at = now()"
            )))
            .bind(&projection.name)
            .bind(partition_index as i32)
            .bind(this_partition_progress)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            progress.insert(partition_index, this_partition_progress);
        }
    }

    // Rolled up regardless of which partitions (if any) this instance
    // won this tick - any instance computing this is harmless, idempotent
    // work, and it's what keeps `projections.caught_up_to` (the single
    // external source of truth GraphQL's `caughtUpTo`/`wait_until_caught_up`
    // depend on) meaning exactly what it always has: every key of this
    // projection reflects every event up to this sequence. The
    // `partition_index < $2` filter excludes any row left over from a
    // since-decreased `PARTITION_COUNT`, so it can never wedge the
    // rollup on a stale high-index row nothing advances anymore.
    let rolled_up: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT MIN(caught_up_to) FROM {schema}.projection_partition_progress \
         WHERE projection_name = $1 AND partition_index < $2"
    )))
    .bind(&projection.name)
    .bind(partition_count as i32)
    .fetch_one(pool)
    .await?;
    if let Some(rolled_up) = rolled_up {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.projections SET caught_up_to = $1 \
             WHERE name = $2 AND (caught_up_to IS NULL OR caught_up_to < $1)"
        )))
        .bind(rolled_up)
        .bind(&projection.name)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// A generous bound on how many events one partitioned projection's own
/// per-partition-per-tick transaction (`catch_up_partitioned_projection`)
/// spans - not a correctness requirement, but for a different reason
/// than `MAX_DUE_DEADLINES_PER_TICK`/`MAX_OCCURRENCES_PER_TICK`'s own
/// "don't starve other work sharing this tick": a partitioned
/// projection's own tick is a *single all-or-nothing transaction* per
/// partition (see `catch_up_partitioned_projection`'s own doc comment
/// for why), so without a cap, a poison event anywhere in a truly
/// enormous post-outage backlog would roll back that entire backlog's
/// worth of progress for that partition, not just the offending event -
/// the unpartitioned path's own per-event transaction granularity has no
/// such exposure. Events beyond this cap are simply left for the next
/// tick, which resumes from wherever this partition's own progress
/// landed - the same "recent window, not a hard limit on correctness"
/// register those two constants are already in.
pub(super) const MAX_EVENTS_PER_PARTITION_TICK: i64 = 1000;

/// `catch_up_partitioned_projection`'s own key→partition assignment -
/// hand-written 64-bit FNV-1a rather than
/// `std::collections::hash_map::DefaultHasher` (explicitly not
/// guaranteed stable across Rust versions/std/build flags - unsuitable
/// for a scheme that must agree across every instance in a fleet,
/// possibly running slightly different builds during a rolling deploy)
/// or a new crate dependency (not judged worth it for a few lines of
/// well-known, public-domain algorithm). Computed in Rust, not pushed
/// into Postgres via `hashtext()`, since partition membership must be
/// known *before* deciding which partitions are even worth a lock
/// attempt this tick - `hashtext()` is still used, unrelated, for the
/// advisory lock key itself (`partition_lock_key` below).
pub(super) fn partition_for_key(key: &str, partition_count: u32) -> u32 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash % u64::from(partition_count)) as u32
}

/// The `pg_try_advisory_xact_lock` key `catch_up_partitioned_projection`
/// races for one `(bounded_context, projection, partition)` triple -
/// bound via `hashtext($1)::bigint`, the same pattern
/// `migrate_idempotency_keys_client_id_scoping` already establishes. A
/// `hashtext` collision with an unrelated lock key is theoretically
/// possible (32-bit output) but harmless here: it would only ever cause
/// a spurious, self-healing missed attempt this tick - exactly why this
/// function's own caller uses the non-blocking `pg_try_advisory_xact_lock`
/// form rather than the blocking `pg_advisory_xact_lock` the migration
/// guard uses.
pub(super) fn partition_lock_key(
    bounded_context: &str,
    projection_name: &str,
    partition_index: u32,
) -> String {
    format!("projection_partition:{bounded_context}:{projection_name}:{partition_index}")
}

/// `catch_up_partitioned_projection`'s own progress read - every
/// `projection_partition_progress` row for one projection, as a
/// `partition_index -> caught_up_to` map. A partition index with no row
/// yet (shouldn't happen once that function's own seed step has run,
/// but not assumed) is simply absent from the map - every caller already
/// treats a missing entry as `-1` via `.unwrap_or(-1)`, the same
/// "nothing folded yet" convention `projection_state.as_of_sequence`'s
/// own default uses.
pub(super) async fn list_projection_partition_progress(
    pool: &Pool,
    schema: &str,
    projection_name: &str,
) -> crate::error::Result<std::collections::HashMap<u32, i64>> {
    let rows: Vec<(i32, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT partition_index, caught_up_to \
         FROM {schema}.projection_partition_progress WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(idx, seq)| (idx as u32, seq))
        .collect())
}

/// The values of `tag_key` `event` carries, each once, in the order it
/// carries them: the snapshot rows it folds into (docs/architecture.md
/// §202). A tag with no value has no row.
pub(super) fn snapshot_tag_values<'e>(event: &'e Event, tag_key: &str) -> Vec<&'e str> {
    let mut values: Vec<&str> = Vec::new();
    for tag in &event.tags {
        if tag.key != tag_key {
            continue;
        }
        if let Some(value) = tag.value.as_deref() {
            if !values.contains(&value) {
                values.push(value);
            }
        }
    }
    values
}

/// One snapshot row's state while a chunk is folded: read and locked
/// before the chunk's events are folded into it, written back once after
/// (docs/architecture.md §197).
pub(super) struct SnapshotChunkRow {
    pub(super) as_of_sequence: i64,
    pub(super) state: String,
    pub(super) owner: Option<String>,
    /// The position its tag's history was refolded through, for a row
    /// created or reset by this chunk - see [`refold_snapshot_histories`].
    /// The chunk's events up to it are already in `state`.
    pub(super) history_through: Option<i64>,
}

/// Folds the events of `events` past `after` into snapshot `name`'s rows,
/// on `conn` - the caller's transaction, which it commits. `partition`,
/// `(index, count)`, keeps only the tag values that hash to that
/// partition. Exactly what folding them one event at a time did, with
/// each row read and locked once and written once (docs/architecture.md
/// §197):
///
/// 1. Every tag value the events touch is locked, or created, in one
///    statement, sorted, so two instances' chunks lock them in the same
///    order. A row at an older `Snapshot::VERSION` is reset as part of it,
///    atomically, to the default state at `as_of_sequence` -1 - the
///    "model changed" case `CommandType::decide_from_snapshot`'s own doc
///    comment describes.
/// 2. A row created or reset here (`as_of_sequence` -1) is refolded from
///    its tag's whole history through its last event in the chunk - one
///    paged read for every such row at once ([`refold_snapshot_histories`]).
///    That's what folding the history at its first event and then each
///    later event did.
/// 3. The rest are folded in memory, keeping the `as_of_sequence` guard.
/// 4. Every folded row is written back in one `UPDATE ... FROM unnest`.
///    A row's owner is the last owner tag a folded event carried; one no
///    event gave an owner keeps the stored one.
///
/// `reached` is set to the index in `events` of the event a failing fold
/// was folding, and left alone otherwise - see
/// [`CatchUpChunkFailure`].
#[allow(clippy::too_many_arguments)]
pub(super) async fn fold_snapshot_chunk_in_tx(
    conn: &mut sqlx::PgConnection,
    bc: &BoundedContext,
    schema: &str,
    dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    name: &str,
    events: &[Event],
    after: i64,
    partition: Option<(u32, u32)>,
    reached: &mut usize,
) -> crate::error::Result<()> {
    let bounded_context = bc.name.as_str();
    let Some(tag_key) = dispatcher.tag_key(bounded_context, name) else {
        return Ok(());
    };
    let version = stored_snapshot_version(dispatcher.version(bounded_context, name).unwrap_or(0));
    let default_state_json = dispatcher
        .default_state(bounded_context, name)
        .unwrap_or_default();
    let owner_tag_key = dispatcher.owner_tag_key(bounded_context, name).flatten();

    // The events this snapshot folds, with the tag value each one folds
    // into: every value of the key it carries (docs/architecture.md
    // §202), each once.
    let mut planned: Vec<(usize, &str)> = Vec::new();
    let mut values: Vec<&str> = Vec::new();
    for (index, event) in events.iter().enumerate() {
        if event.sequence <= after {
            continue;
        }
        for value in snapshot_tag_values(event, tag_key) {
            if let Some((partition_index, partition_count)) = partition {
                if partition_for_key(value, partition_count) != partition_index {
                    continue;
                }
            }
            planned.push((index, value));
            values.push(value);
        }
    }
    if planned.is_empty() {
        return Ok(());
    }
    values.sort_unstable();
    values.dedup();

    let locked: Vec<(String, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.snapshots \
            (snapshot_name, tag_key, tag_value, snapshot_version, as_of_sequence, state, updated_at) \
         SELECT $1, $2, value, $4, -1, $5::jsonb, now() FROM unnest($3::text[]) AS value \
         ON CONFLICT (snapshot_name, tag_key, tag_value) DO UPDATE SET \
            snapshot_version = $4, \
            as_of_sequence = CASE WHEN {schema}.snapshots.snapshot_version = $4 \
                THEN {schema}.snapshots.as_of_sequence ELSE -1 END, \
            state = CASE WHEN {schema}.snapshots.snapshot_version = $4 \
                THEN {schema}.snapshots.state ELSE $5::jsonb END \
         RETURNING tag_value, as_of_sequence, state::text"
    )))
    .bind(name)
    .bind(tag_key)
    .bind(&values)
    .bind(version)
    .bind(&default_state_json)
    .fetch_all(&mut *conn)
    .await?;
    let mut rows: std::collections::HashMap<String, SnapshotChunkRow> = locked
        .into_iter()
        .map(|(value, as_of_sequence, state)| {
            let row = SnapshotChunkRow {
                as_of_sequence,
                state,
                owner: None,
                history_through: None,
            };
            (value, row)
        })
        .collect();

    // A retraction this row hasn't seen yet: what it retracts may be in the
    // state, and a fold can't be undone, so the row starts over like a reset
    // one, without the retracted event (docs/architecture.md §235).
    for (index, value) in &planned {
        let Some(event) = events.get(*index) else {
            continue;
        };
        if crate::retraction::retracted_position(event).is_none() {
            continue;
        }
        if let Some(row) = rows.get_mut(*value) {
            if row.as_of_sequence < event.sequence {
                row.as_of_sequence = -1;
            }
        }
    }

    // A row at -1 is refolded through its last event in the chunk. Each
    // keeps its events' `(sequence, index)`, in order, to say which event
    // a failing fold belongs to.
    let mut refold: std::collections::HashMap<&str, Vec<(i64, usize)>> =
        std::collections::HashMap::new();
    for (index, value) in &planned {
        if rows[*value].as_of_sequence == -1 {
            refold
                .entry(*value)
                .or_default()
                .push((events[*index].sequence, *index));
        }
    }
    if !refold.is_empty() {
        refold_snapshot_histories(
            conn,
            bc,
            dispatcher,
            name,
            tag_key,
            &default_state_json,
            &refold,
            &mut rows,
            reached,
        )
        .await?;
    }

    for (index, value) in &planned {
        let event = &events[*index];
        let row = rows
            .get_mut(*value)
            .expect("every planned value's row was just read");
        let refolded = row.history_through.is_some_and(|h| event.sequence <= h);
        if !refolded {
            // `as_of_sequence` guard (issue #25): another
            // instance may have folded this row past `event` already.
            if row.as_of_sequence >= event.sequence {
                continue;
            }
            // A doubt carries its target's tags and leaves the state as it is
            // (docs/architecture.md §236).
            if !crate::retraction::is_doubt_event(event) {
                if let Some(result) =
                    dispatcher.fold_value(bounded_context, name, value, &row.state, event)
                {
                    row.state = result.inspect_err(|_| *reached = *index)?;
                }
            }
            row.as_of_sequence = event.sequence;
        }
        if let Some(owner) = event_owner(owner_tag_key, event) {
            row.owner = Some(owner);
        }
    }

    let mut written_values = Vec::new();
    let mut states = Vec::new();
    let mut owners = Vec::new();
    let mut as_of_sequences = Vec::new();
    for (value, row) in &rows {
        written_values.push(value.as_str());
        states.push(row.state.as_str());
        owners.push(row.owner.as_deref());
        as_of_sequences.push(row.as_of_sequence);
    }
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.snapshots AS s SET snapshot_version = $3, state = u.state::jsonb, \
         owner = COALESCE(u.owner, s.owner), as_of_sequence = u.as_of_sequence, \
         updated_at = now() \
         FROM unnest($4::text[], $5::text[], $6::text[], $7::bigint[]) \
         AS u(tag_value, state, owner, as_of_sequence) \
         WHERE s.snapshot_name = $1 AND s.tag_key = $2 AND s.tag_value = u.tag_value"
    )))
    .bind(name)
    .bind(tag_key)
    .bind(version)
    .bind(&written_values)
    .bind(&states)
    .bind(&owners)
    .bind(&as_of_sequences)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Folds `events` - a contiguous run of the bounded context's events -
/// into every unpartitioned snapshot in `names` that `progress` puts
/// before its end, and moves their progress to its end, in one
/// transaction (docs/architecture.md §197). Snapshots go by name, so two
/// instances' chunks lock their rows in the same order.
#[allow(clippy::too_many_arguments)]
pub(super) async fn fold_snapshots_chunk(
    pool: &Pool,
    bc: &BoundedContext,
    schema: &str,
    dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    names: &[&str],
    progress: &std::collections::HashMap<&str, i64>,
    events: &[Event],
) -> Result<(), CatchUpChunkFailure> {
    let mut reached = 0;
    let folded: crate::error::Result<()> = async {
        let Some(chunk_end) = events.last().map(|event| event.sequence) else {
            return Ok(());
        };
        let mut sorted: Vec<&str> = names.to_vec();
        sorted.sort_unstable();
        let mut tx = pool.begin().await?;
        for name in sorted {
            let after = progress.get(name).copied().unwrap_or(-1);
            if after >= chunk_end {
                continue;
            }
            fold_snapshot_chunk_in_tx(
                &mut tx,
                bc,
                schema,
                dispatcher,
                name,
                events,
                after,
                None,
                &mut reached,
            )
            .await?;
            upsert_snapshot_progress(&mut *tx, schema, name, chunk_end).await?;
        }
        tx.commit().await?;
        Ok(())
    }
    .await;
    folded.map_err(|error| CatchUpChunkFailure {
        folded_before: reached,
        error,
    })
}

/// Refolds each row in `refold` - tag value to its chunk events'
/// `(sequence, index)`, in order - from nothing over every event carrying
/// that tag value up to and including its last chunk event, into `rows`
/// (docs/architecture.md §152, §197). One paged read covers every value
/// at once, instead of one read per value, through `event_tags` (§198):
/// its btree hands back each value's sequences in order, where the GIN
/// index on `events.tags` (since dropped, §210) rebuilt and sorted every value's whole bitmap
/// for each page - about four times slower for 100 values of 500 events.
/// The page is of distinct sequences: `event_tags` has a row per tag, so
/// an event carrying two of the values has two, and a limit on rows
/// returned a short page before the end, which ended the paging early.
/// A
/// fold that fails sets `reached` to the first of the value's chunk
/// events at or after the failing one: the event at which the per-event
/// loop would have reached it, and failed - the first chunk event for
/// history from before the chunk.
#[allow(clippy::too_many_arguments)]
pub(super) async fn refold_snapshot_histories(
    conn: &mut sqlx::PgConnection,
    bc: &BoundedContext,
    dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    name: &str,
    tag_key: &str,
    default_state_json: &str,
    refold: &std::collections::HashMap<&str, Vec<(i64, usize)>>,
    rows: &mut std::collections::HashMap<String, SnapshotChunkRow>,
    reached: &mut usize,
) -> crate::error::Result<()> {
    const PAGE: i64 = 1000;
    let schema = schema_ident(&bc.name);
    let through_of = |planned: &[(i64, usize)]| {
        planned
            .last()
            .expect("a refolded value has a chunk event")
            .0
    };
    let through_all = refold
        .values()
        .map(|planned| through_of(planned))
        .max()
        .expect("refold is non-empty");
    let mut values: Vec<&str> = refold.keys().copied().collect();
    values.sort_unstable();
    // Up front: a retraction can be pages past what it retracts. A retraction
    // carries its target's tags, so it is found by the same value.
    let retracted: std::collections::HashSet<i64> =
        sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(format!(
            "SELECT payload FROM {schema}.events \
             WHERE sequence IN (SELECT sequence FROM {schema}.event_tags \
                 WHERE tag_key = $1 AND tag_value = ANY($2) AND event_type_name = $3 \
                 AND sequence <= $4)"
        )))
        .bind(tag_key)
        .bind(&values)
        .bind(crate::retraction::RETRACTED_EVENT_TYPE)
        .bind(through_all)
        .fetch_all(&mut *conn)
        .await?
        .iter()
        .filter_map(|payload| {
            serde_json::from_str::<crate::retraction::RetractionPayload>(payload).ok()
        })
        .map(|retraction| retraction.position)
        .collect();
    for (value, planned) in refold {
        let row = rows.get_mut(*value).expect("every refolded row was read");
        row.state = default_state_json.to_string();
        row.as_of_sequence = through_of(planned);
        row.history_through = Some(through_of(planned));
    }

    let mut after = -1;
    loop {
        let page: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
             metadata_client_id, metadata_created_at, metadata_correlation_id, \
             metadata_causation_id, tags, origin_kind, origin_source_content, \
             origin_source_context, origin_command_id FROM {schema}.events \
             WHERE sequence IN (SELECT DISTINCT sequence FROM {schema}.event_tags \
                 WHERE tag_key = $1 AND tag_value = ANY($2) \
                 AND sequence > $3 AND sequence <= $4 ORDER BY sequence LIMIT $5) \
             ORDER BY sequence"
        )))
        .bind(tag_key)
        .bind(&values)
        .bind(after)
        .bind(through_all)
        .bind(PAGE)
        .fetch_all(&mut *conn)
        .await?;
        let full = page.len() as i64 == PAGE;
        let page = events_from_rows(&mut *conn, bc, page, None).await?;
        for event in &page {
            if retracted.contains(&event.sequence)
                || crate::retraction::retracted_position(event).is_some()
                || crate::retraction::is_doubt_event(event)
            {
                continue;
            }
            for value in snapshot_tag_values(event, tag_key) {
                let Some(planned) = refold.get(value) else {
                    continue;
                };
                let Some(&(_, index)) = planned.iter().find(|(seq, _)| *seq >= event.sequence)
                else {
                    continue;
                };
                let row = rows.get_mut(value).expect("every refolded row was read");
                if let Some(result) =
                    dispatcher.fold_value(&bc.name, name, value, &row.state, event)
                {
                    row.state = result.inspect_err(|_| *reached = index)?;
                }
            }
        }
        match page.last() {
            Some(last) if full => after = last.sequence,
            _ => return Ok(()),
        }
    }
}

/// [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 2" - the background half of
/// `Snapshot`: one poll tick, for one bounded context, folding every
/// committed event not yet reflected in any registered snapshot.
/// Deliberately its own function, not folded into `catch_up_bounded_context`
/// above even though the shape closely mirrors it - see `Snapshot`'s own
/// doc comment for why the two stay structurally separate. Called in a
/// loop, on a timer, by its own shared background task
/// `SkiljBuilder::build()` spawns.
///
/// Unlike `Projection` (discovered via the `projections` metadata
/// table), which snapshots exist is asked of `dispatcher` directly -
/// `SnapshotDispatcher::snapshot_names` - since there is deliberately no
/// metadata table for `Snapshot` (see that trait's own doc comment).
/// Progress is tracked per `snapshot_name` in `snapshot_progress`
/// (`Projection`'s own `caught_up_to` column isn't reused, for the same
/// separation reason), and, per event, at most one tag *value*'s own row
/// is touched - `Snapshot::TAG_KEY` names a single tag key, so `event.tags`
/// either does or doesn't carry it, no `keys()`-equivalent fan-out the
/// way a multi-instance `Projection` needs.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn catch_up_snapshots(
    pool: &Pool,
    bounded_context: &str,
    dispatcher: &dyn crate::plugin::SnapshotDispatcher,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let latest = latest_sequence(pool, bounded_context).await?.unwrap_or(-1);

    let all_snapshot_names = dispatcher.snapshot_names(bounded_context);
    // issue #25 (docs/architecture.md §52) - split by
    // `Snapshot::PARTITION_COUNT` (via the dispatcher, the only place
    // this Rust-only config lives, exactly like `Projection`'s own
    // split in `catch_up_bounded_context`). `unpartitioned_snapshot_names`
    // (`<= 1`, the default, and every snapshot that predates this
    // feature) feeds the exact same loop this function has always had,
    // completely unchanged below.
    let unpartitioned_snapshot_names: Vec<&str> = all_snapshot_names
        .iter()
        .filter(|name| {
            dispatcher
                .partition_count(bounded_context, name)
                .unwrap_or(1)
                .max(1)
                <= 1
        })
        .copied()
        .collect();
    let partitioned_snapshot_names: Vec<(&str, u32)> = all_snapshot_names
        .iter()
        .filter_map(|name| {
            let partition_count = dispatcher
                .partition_count(bounded_context, name)
                .unwrap_or(1)
                .max(1);
            (partition_count > 1).then_some((*name, partition_count))
        })
        .collect();
    if unpartitioned_snapshot_names.is_empty() && partitioned_snapshot_names.is_empty() {
        return Ok(());
    }

    let progress_from_db =
        list_snapshot_progress_for_bounded_context(pool, bounded_context).await?;
    let mut progress: std::collections::HashMap<&str, i64> = unpartitioned_snapshot_names
        .iter()
        .map(|name| (*name, progress_from_db.get(*name).copied().unwrap_or(-1)))
        .collect();

    let min_caught_up = progress.values().copied().min().unwrap_or(latest);
    let events = if unpartitioned_snapshot_names.is_empty() || min_caught_up >= latest {
        Vec::new()
    } else {
        // At most `MAX_EVENTS_PER_CATCH_UP_TICK`: progress persists per
        // event, so a new projection/rebuild/snapshot over a long history
        // catches up over several ticks instead of loading it whole.
        list_events_for_bounded_context_from_limited(
            pool,
            bounded_context,
            min_caught_up,
            MAX_EVENTS_PER_CATCH_UP_TICK,
        )
        .await?
    };

    // A chunk of events per transaction, not one (docs/architecture.md
    // §197), as projection catch-up does (§182).
    if let Some(first) = events.first() {
        let bc = &first.bounded_context;
        for chunk in events.chunks(CATCH_UP_EVENTS_PER_TRANSACTION) {
            let folded = fold_snapshots_chunk(
                pool,
                bc,
                &schema,
                dispatcher,
                &unpartitioned_snapshot_names,
                &progress,
                chunk,
            )
            .await;
            if let Err(failure) = folded {
                // A failing event doesn't take the events before it in
                // its chunk down with it - see `catch_up_bounded_context`.
                // Shortened again if a shorter run fails earlier - a
                // refolded row's history, or another snapshot, can fail
                // before the event blamed (docs/architecture.md §200).
                let mut end = failure.folded_before;
                while end > 0 {
                    match fold_snapshots_chunk(
                        pool,
                        bc,
                        &schema,
                        dispatcher,
                        &unpartitioned_snapshot_names,
                        &progress,
                        &chunk[..end],
                    )
                    .await
                    {
                        Ok(()) => break,
                        Err(retry) if retry.folded_before < end => end = retry.folded_before,
                        Err(retry) => return Err(retry.error),
                    }
                }
                return Err(failure.error);
            }
            let chunk_end = chunk.last().expect("chunks are never empty").sequence;
            for position in progress.values_mut() {
                *position = (*position).max(chunk_end);
            }
        }
    }

    // issue #25 (docs/architecture.md §52) - sequential across
    // partitioned snapshots, not `for_each_concurrent`, the identical
    // reasoning `catch_up_bounded_context`'s own partitioned-projection
    // loop already documents (§51): the real scale-out value comes from
    // separate *instances* racing for partitions, not from one
    // instance's own intra-tick fan-out.
    for (name, partition_count) in partitioned_snapshot_names {
        catch_up_partitioned_snapshot(
            pool,
            bounded_context,
            &schema,
            dispatcher,
            name,
            partition_count,
            latest,
        )
        .await?;
    }

    Ok(())
}

/// `catch_up_snapshots`'s own per-tick body for one `Snapshot` whose
/// `Snapshot::PARTITION_COUNT > 1` (issue #25, docs/architecture.md
/// §52 - the `Snapshot` twin of `catch_up_partitioned_projection`, see
/// that function's own doc comment for the full design writeup, not
/// repeated in full here). The one structural difference: `Snapshot`
/// derives at most *one* tag value per event (there's no `keys()`-style
/// fan-out - see `Snapshot`'s own doc comment), so partitioning hashes
/// that single derived tag value instead of iterating several keys per
/// event.
///
/// Same primitives throughout: `partition_for_key` (the identical
/// FNV-1a §51 established), a non-blocking
/// `pg_try_advisory_xact_lock` per `(snapshot_name, partition)` per
/// tick (`snapshot_partition_lock_key`), one transaction per
/// `(partition, tick)`, `MAX_EVENTS_PER_PARTITION_TICK`-capped batches
/// via the existing `list_events_for_bounded_context_from_limited`, and
/// a monotonic-only rollup into `snapshot_progress.caught_up_to` (the
/// single external source of truth, unchanged in shape - `Snapshot` has
/// no GraphQL-exposed `caughtUpTo` field the way `Projection` does, but
/// `resolve_snapshot_context`'s own correctness already depends on
/// `snapshots.as_of_sequence` per row, untouched by any of this).
#[allow(clippy::too_many_arguments)]
pub(super) async fn catch_up_partitioned_snapshot(
    pool: &Pool,
    bounded_context: &str,
    schema: &str,
    dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    name: &str,
    partition_count: u32,
    latest: i64,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.snapshot_partition_progress \
         (snapshot_name, partition_index, caught_up_to, updated_at) \
         SELECT $1, gs, -1, now() FROM generate_series(0, $2::int - 1) AS gs \
         ON CONFLICT (snapshot_name, partition_index) DO NOTHING"
    )))
    .bind(name)
    .bind(partition_count as i32)
    .execute(pool)
    .await?;

    let mut progress = list_snapshot_partition_progress(pool, schema, name).await?;
    let min_caught_up = (0..partition_count)
        .map(|p| progress.get(&p).copied().unwrap_or(-1))
        .min()
        .unwrap_or(latest);

    let events = if min_caught_up >= latest {
        Vec::new()
    } else {
        list_events_for_bounded_context_from_limited(
            pool,
            bounded_context,
            min_caught_up,
            MAX_EVENTS_PER_PARTITION_TICK,
        )
        .await?
    };

    if !events.is_empty() {
        let batch_end = events
            .last()
            .expect("just checked events is non-empty")
            .sequence;
        if dispatcher.tag_key(bounded_context, name).is_none() {
            return Ok(());
        }
        let bc = require_bounded_context(pool, bounded_context).await?;

        for partition_index in 0..partition_count {
            if progress.get(&partition_index).copied().unwrap_or(-1) >= batch_end {
                continue;
            }

            let mut tx = pool.begin().await?;
            let locked: bool =
                sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1)::bigint)")
                    .bind(snapshot_partition_lock_key(
                        bounded_context,
                        name,
                        partition_index,
                    ))
                    .fetch_one(&mut *tx)
                    .await?;
            if !locked {
                // Nothing was written under this transaction - dropping
                // it is a no-op rollback. Another instance (or this
                // one, next tick) owns this partition for now.
                continue;
            }

            // Every event of this partition in one fold, each row read
            // and written once (docs/architecture.md §197). The
            // transaction was already all or nothing.
            let after = progress.get(&partition_index).copied().unwrap_or(-1);
            let mut reached = 0;
            fold_snapshot_chunk_in_tx(
                &mut tx,
                &bc,
                schema,
                dispatcher,
                name,
                &events,
                after,
                Some((partition_index, partition_count)),
                &mut reached,
            )
            .await?;
            let this_partition_progress = after.max(batch_end);

            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {schema}.snapshot_partition_progress \
                 (snapshot_name, partition_index, caught_up_to, updated_at) \
                 VALUES ($1, $2, $3, now()) \
                 ON CONFLICT (snapshot_name, partition_index) DO UPDATE SET \
                    caught_up_to = GREATEST(\
                        {schema}.snapshot_partition_progress.caught_up_to, EXCLUDED.caught_up_to), \
                    updated_at = now()"
            )))
            .bind(name)
            .bind(partition_index as i32)
            .bind(this_partition_progress)
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;
            progress.insert(partition_index, this_partition_progress);
        }
    }

    // Rolled up regardless of which partitions (if any) this instance
    // won this tick - see `catch_up_partitioned_projection`'s own
    // identical comment. `partition_index < $2` excludes any row left
    // over from a since-decreased `PARTITION_COUNT`.
    let rolled_up: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT MIN(caught_up_to) FROM {schema}.snapshot_partition_progress \
         WHERE snapshot_name = $1 AND partition_index < $2"
    )))
    .bind(name)
    .bind(partition_count as i32)
    .fetch_one(pool)
    .await?;
    if let Some(rolled_up) = rolled_up {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.snapshot_progress (snapshot_name, caught_up_to) \
             VALUES ($1, $2) \
             ON CONFLICT (snapshot_name) DO UPDATE SET \
                caught_up_to = GREATEST({schema}.snapshot_progress.caught_up_to, EXCLUDED.caught_up_to)"
        )))
        .bind(name)
        .bind(rolled_up)
        .execute(pool)
        .await?;
    }

    Ok(())
}

/// `catch_up_partitioned_snapshot`'s own lock key - `partition_lock_key`'s
/// identical `hashtext(...)::bigint` pattern, namespaced by `"snapshot_partition"`
/// rather than `"projection_partition"` so the two families can never
/// collide with each other even if a projection and a snapshot happened
/// to share a name.
pub(super) fn snapshot_partition_lock_key(
    bounded_context: &str,
    snapshot_name: &str,
    partition_index: u32,
) -> String {
    format!("snapshot_partition:{bounded_context}:{snapshot_name}:{partition_index}")
}

/// `catch_up_partitioned_snapshot`'s own progress read - `list_projection_partition_progress`'s
/// identical shape, for `snapshot_partition_progress` instead.
pub(super) async fn list_snapshot_partition_progress(
    pool: &Pool,
    schema: &str,
    snapshot_name: &str,
) -> crate::error::Result<std::collections::HashMap<u32, i64>> {
    let rows: Vec<(i32, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT partition_index, caught_up_to \
         FROM {schema}.snapshot_partition_progress WHERE snapshot_name = $1"
    )))
    .bind(snapshot_name)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(idx, seq)| (idx as u32, seq))
        .collect())
}

/// `RegisterProjection`'s own synchronous counterpart to
/// `catch_up_bounded_context`'s async walk - drift audit finding #3
/// (2026-08-20, see project memory `skilj-drift-audit-2026-08-20` and
/// `ProjectionRegistration::Created`'s own `needs_history_fold` doc
/// comment). Called exactly once, immediately after `upsert_projection`,
/// whenever `register_projection` reports `needs_history_fold: true` -
/// never for a re-registration (that path already goes through
/// `RebuildStaged`/`promote_projection_rebuild` instead) and never for an
/// async projection (`catch_up_bounded_context`'s own periodic walk
/// already backfills those correctly, `caught_up_to.unwrap_or(-1)`
/// treating a freshly created row exactly like a stale one).
///
/// Walks every event the bounded context has ever committed, not just
/// ones matching `projection.consumed_event_types` - the same "let the
/// dispatcher decide" shape `catch_up_bounded_context` itself uses, so
/// this can never disagree with what the async path would have produced
/// had the projection been async instead. One transaction per event,
/// same as `catch_up_bounded_context`'s own per-event loop; this only
/// ever runs once, at first-time registration, so the cost is paid once
/// per projection, not on every poll tick.
///
/// Returns the given `Projection` with `caught_up_to` set to the highest
/// sequence folded - always `Some` when this is called at all, since the
/// caller only calls it when `needs_history_fold` was true, which itself
/// requires at least one matching event to exist.
#[tracing::instrument(skip_all)]
pub async fn fold_history_into_new_sync_projection(
    pool: &Pool,
    projection: &Projection,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
) -> crate::error::Result<Projection> {
    let bounded_context = &projection.bounded_context.name;
    let default_state_json = dispatcher
        .default_state(bounded_context, &projection.name)
        .unwrap_or_default();
    let owner_tag_key = dispatcher
        .owner_tag_key(bounded_context, &projection.name)
        .flatten();

    let mut caught_up_to = None;
    // History a chunk at a time rather than all at once; each event still
    // commits in its own transaction, exactly as before.
    let mut after = -1;
    loop {
        let events = list_events_for_bounded_context_from_limited(
            pool,
            bounded_context,
            after,
            MAX_EVENTS_PER_CATCH_UP_TICK,
        )
        .await?;
        let exhausted = (events.len() as i64) < MAX_EVENTS_PER_CATCH_UP_TICK;
        if let Some(last) = events.last() {
            after = last.sequence;
        }
        for event in &events {
            let mut tx = pool.begin().await?;
            fold_event_into_new_projection(
                &mut tx,
                projection,
                event,
                dispatcher,
                &default_state_json,
                owner_tag_key,
            )
            .await?;
            tx.commit().await?;
            caught_up_to = Some(event.sequence);
        }
        if exhausted {
            break;
        }
    }

    Ok(Projection {
        caught_up_to,
        ..projection.clone()
    })
}

/// Folds one event into a projection being brought up to date by
/// [`fold_history_into_new_sync_projection`] or
/// [`register_new_sync_projection`]'s locked tail, inside the caller's
/// transaction, and advances its `caught_up_to` (never backwards).
pub(super) async fn fold_event_into_new_projection(
    tx: &mut Transaction<'_, Postgres>,
    projection: &Projection,
    event: &Event,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    default_state_json: &str,
    owner_tag_key: Option<&str>,
) -> crate::error::Result<()> {
    let bounded_context = &projection.bounded_context.name;
    let schema = schema_ident(bounded_context);
    // docs/architecture.md §156: the projection row before any of its
    // state rows, the order catch-up and promotion take them in. The
    // projection is stored async while its history is folded, so
    // catch-up folds it at the same time; taking a state row first could
    // deadlock against a catch-up chunk holding the projection row
    // (§182).
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT 1 FROM {schema}.projections WHERE name = $1 FOR UPDATE"
    )))
    .bind(&projection.name)
    .execute(&mut **tx)
    .await?;
    let keys = dispatcher
        .keys(bounded_context, &projection.name, event)
        .unwrap_or_default();
    for key in &keys {
        // `as_of_sequence` guard - see `catch_up_bounded_context`'s
        // identical comment. Two instances both registering the same
        // brand-new projection (`register_projection`'s `existing = None`
        // read-then-decide has no claim mechanism) fold the same history
        // at once: whichever commits a key's row first, the other's own
        // `RETURNING` here sees `as_of_sequence` already at
        // `event.sequence` and skips instead of folding again.
        let (as_of_sequence, current_state) = get_or_create_projection_state_for_update(
            &mut **tx,
            &schema,
            &projection.name,
            key,
            default_state_json,
        )
        .await?;
        if as_of_sequence >= event.sequence {
            continue;
        }

        let new_state = match dispatcher.project(
            bounded_context,
            &projection.name,
            &current_state,
            event,
            key,
        ) {
            Some(result) => result?,
            None => current_state,
        };

        apply_projection_fold_update(
            &mut **tx,
            &schema,
            "projection_state",
            "",
            &projection.name,
            key,
            &new_state,
            owner_tag_key,
            event,
        )
        .await?;
    }

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.projections SET caught_up_to = $1 \
         WHERE name = $2 AND (caught_up_to IS NULL OR caught_up_to < $1)"
    )))
    .bind(event.sequence)
    .bind(&projection.name)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

/// Persists a `ProjectionRegistration::Created` - every caller's one
/// entry point for it. An async projection is just stored:
/// `catch_up_bounded_context` backfills it. A sync one goes through
/// [`register_new_sync_projection`].
pub async fn create_projection(
    pool: &Pool,
    projection: &Projection,
    needs_history_fold: bool,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
) -> crate::error::Result<Projection> {
    if !projection.sync {
        upsert_projection(pool, projection).await?;
        return Ok(projection.clone());
    }
    register_new_sync_projection(pool, projection, needs_history_fold, dispatcher).await
}

/// Registers a brand-new sync projection so that no event is folded out
/// of order or missed (docs/architecture.md §113).
///
/// Live writes fold every event into each *sync* projection they find
/// once they hold the bounded context's sequence lock. Storing the
/// projection as sync and then folding its history left two races:
///
/// - a write landing before the history fold reached a key created that
///   key's row at the new event's sequence, and the fold's
///   `as_of_sequence` guard then skipped every earlier event for it - the
///   key's history silently lost;
/// - a write that had already read the projection list before the row
///   appeared committed a matching event the projection never saw.
///
/// So when there is history to fold, the projection is first stored as
/// *async* - which live writes ignore - and its history folded without
/// holding any lock, one event per transaction as before. Then one
/// transaction takes the sequence lock (the one `promote_projection_rebuild`
/// takes for the same reason), folds the consumed-type events committed
/// since, and stores the projection as sync. Every event at or below the
/// locked sequence is folded by then, and every later write sees the
/// projection. With no history to fold, only that locked step runs; its
/// tail is whatever matching event slipped in after the caller's history
/// check - normally nothing.
pub async fn register_new_sync_projection(
    pool: &Pool,
    projection: &Projection,
    needs_history_fold: bool,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
) -> crate::error::Result<Projection> {
    let bounded_context = &projection.bounded_context.name;
    let mut folded_through = None;
    if needs_history_fold {
        let hidden = Projection {
            sync: false,
            caught_up_to: None,
            ..projection.clone()
        };
        upsert_projection(pool, &hidden).await?;
        folded_through = fold_history_into_new_sync_projection(pool, &hidden, dispatcher)
            .await?
            .caught_up_to;
    }

    let default_state_json = dispatcher
        .default_state(bounded_context, &projection.name)
        .unwrap_or_default();
    let owner_tag_key = dispatcher
        .owner_tag_key(bounded_context, &projection.name)
        .flatten();

    let mut tx = pool.begin().await?;
    if needs_history_fold {
        // docs/architecture.md §195: the hidden async row is one a catch-up
        // chunk folds into and holds for a whole chunk. Wait for it before
        // the sequence lock, not under it, where every write to the
        // bounded context would wait too. Nothing takes the sequence lock
        // and then waits on this row: live writes fold only sync
        // projections, and promotion doesn't wait.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT 1 FROM {}.projections WHERE name = $1 FOR NO KEY UPDATE",
            schema_ident(bounded_context)
        )))
        .bind(&projection.name)
        .execute(&mut *tx)
        .await?;
    }
    let locked_highest = lock_bounded_context_sequence(&mut tx, bounded_context).await?;
    // With the lock held no event can commit, so this sees the complete
    // tail. Read on `tx` itself: every writer to this bounded context now
    // waits on the lock holding a pooled connection, so a second one might
    // never come (docs/architecture.md §117).
    let consumed_types: std::collections::HashMap<String, EventType> = projection
        .consumed_event_types
        .iter()
        .map(|et| (et.name.clone(), et.clone()))
        .collect();
    let consumed_names: Vec<&str> = consumed_types.keys().map(String::as_str).collect();
    let schema = schema_ident(bounded_context);
    let mut tail = Vec::new();
    let mut after = folded_through.unwrap_or(-1);
    loop {
        let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
             metadata_client_id, metadata_created_at, metadata_correlation_id, \
             metadata_causation_id, tags, origin_kind, origin_source_content, \
             origin_source_context, origin_command_id FROM {schema}.events \
             WHERE event_type_name = ANY($1) AND sequence > $2 ORDER BY sequence LIMIT $3"
        )))
        .bind(&consumed_names)
        .bind(after)
        .bind(MAX_EVENTS_PER_CATCH_UP_TICK)
        .fetch_all(&mut *tx)
        .await?;
        let exhausted = (rows.len() as i64) < MAX_EVENTS_PER_CATCH_UP_TICK;
        let chunk = events_from_rows(
            &mut tx,
            &projection.bounded_context,
            rows,
            Some(&consumed_types),
        )
        .await?;
        if let Some(last) = chunk.last() {
            after = last.sequence;
        }
        tail.extend(chunk);
        if exhausted {
            break;
        }
    }

    // Every event up to the locked sequence is accounted for once the
    // tail is folded. Left unset when nothing was ever folded, as a new
    // projection with no history always was.
    let stored = Projection {
        sync: true,
        caught_up_to: (folded_through.is_some() || !tail.is_empty() || needs_history_fold)
            .then_some(locked_highest),
        ..projection.clone()
    };
    if !needs_history_fold {
        // The row doesn't exist yet, and `projection_state` rows need it.
        upsert_projection_in_tx(&mut tx, &stored).await?;
    }
    for event in &tail {
        fold_event_into_new_projection(
            &mut tx,
            &stored,
            event,
            dispatcher,
            &default_state_json,
            owner_tag_key,
        )
        .await?;
    }
    upsert_projection_in_tx(&mut tx, &stored).await?;
    tx.commit().await?;
    notify_registration_changed(pool).await;
    Ok(stored)
}

/// SQLSTATE `lock_not_available`: a `NOWAIT` lock found its row taken.
pub(super) const LOCK_NOT_AVAILABLE: &str = "55P03";

/// Promotes a `building` `ProjectionRebuild` that has caught up to its
/// bounded context's latest committed sequence - `catch_up_bounded_context`'s
/// own last step for each rebuild it walks. One transaction: the rebuild's
/// `schema`/`schema_version`/`sync`/`caught_up_to` replace the live
/// `Projection` row's own, `projection_consumed_event_types` is replaced
/// wholesale from `projection_rebuild_consumed_event_types`, and
/// `projection_rebuild_state`'s content replaces `projection_state`'s
/// (the live projection's old state is discarded - the note above
/// `RegisterProjection`) - then every rebuild-side row is deleted. A
/// rebuild the dispatcher never once resolved (see `catch_up_bounded_context`'s
/// own doc comment) still promotes on schedule; its `projection_rebuild_state`
/// row, if one was ever written, is copied as-is - `"{}"` included.
///
/// Drift audit finding #6 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): the caller's own `caught_up_to ==
/// latest` pre-check is only a snapshot taken before this call, not a
/// lock - without one, a new event could commit in the gap between that
/// check and this function's own writes, invisible to both sides
/// forever after (never folded into the rebuild, which this promotes
/// away from `building` before it can be; never folded inline either,
/// since the live projection was still `sync = false` when it arrived).
/// Closed the same way `submit_command`'s own DCB re-check is: locking
/// the bounded context's `{schema}.sequence` row first
/// (`SELECT ... FOR UPDATE`, the identical query, since `next_value`
/// already *is* the latest committed sequence per
/// `SequenceIsGaplessPerBoundedContext` - no separate lookup needed),
/// then re-verifying eligibility under that lock rather than trusting
/// the pre-lock snapshot. This is also what makes
/// `insert_event_and_update_sync_projections_in_tx`'s own
/// `sync_projections` read safe to leave running on the bare pool,
/// outside its own `tx` - see that function's doc comment: every
/// event-insert path already takes this exact lock before reaching that
/// read, so once promotion takes it too, the two can never interleave.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn promote_projection_rebuild(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
) -> crate::error::Result<bool> {
    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;

    // The lock this whole fix hinges on - see this function's own doc
    // comment. A read-only peek, not `next_sequence`'s increment: this
    // never allocates a sequence number of its own.
    let (locked_highest,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT next_value FROM {schema}.sequence FOR UPDATE"
    )))
    .fetch_one(&mut *tx)
    .await?;

    // Every rebuild-side statement below is scoped to `status = 'building'`
    // - promotion only ever ends the building row. A coexisting pending
    // row (the deliberate case `UniqueRebuildPerProjectionAndStatus`
    // names - a non-trivial registration that arrived mid-build) must
    // survive this call untouched: it is still waiting on its own future
    // `RebuildProjection` trigger, unrelated to whichever build just
    // finished.
    let building = projection_rebuild_status_to_str(ProjectionRebuildStatus::Building);

    // docs/architecture.md §195: a catch-up chunk holds these rows for up
    // to `CATCH_UP_EVENTS_PER_TRANSACTION` events. Waiting for it here,
    // with the sequence lock held, stalled every write to the bounded
    // context for that long. So take them without waiting, in the
    // chunk's order, and defer to the next tick when one is busy - the
    // same `false` as a rebuild that isn't caught up yet.
    for lock in [
        format!("SELECT 1 FROM {schema}.projections WHERE name = $1 FOR UPDATE NOWAIT"),
        format!(
            "SELECT 1 FROM {schema}.projection_rebuilds \
             WHERE projection_name = $1 AND status = 'building' FOR UPDATE NOWAIT"
        ),
    ] {
        match sqlx::query(sqlx::AssertSqlSafe(lock))
            .bind(projection_name)
            .execute(&mut *tx)
            .await
        {
            Ok(_) => {}
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some(LOCK_NOT_AVAILABLE) => {
                return Ok(false);
            }
            Err(e) => return Err(e.into()),
        }
    }

    let rebuild_row: ProjectionRebuildRow = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_REBUILD_COLUMNS} FROM {schema}.projection_rebuilds \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .fetch_one(&mut *tx)
    .await?;

    if rebuild_row.caught_up_to.unwrap_or(-1) != locked_highest {
        // A new event committed after this bounded context's last fold
        // pass but before this lock was acquired - promoting now would
        // strand it. Defer: the next catch_up_bounded_context tick folds
        // it into this still-building rebuild and retries promotion
        // then. `tx` drops here, rolling back the no-op peek lock -
        // nothing was written, nothing to undo.
        return Ok(false);
    }

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.projections SET schema = $1, schema_version = $2, sync = $3, \
         caught_up_to = $4 WHERE name = $5"
    )))
    .bind(&rebuild_row.schema)
    .bind(rebuild_row.schema_version)
    .bind(rebuild_row.sync)
    .bind(rebuild_row.caught_up_to)
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_consumed_event_types WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_consumed_event_types (projection_name, event_type_name) \
         SELECT projection_name, event_type_name \
         FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;

    // Every instance the rebuild accumulated replaces every live one -
    // not just one row anymore (§9's "keyed / multi-row Projections"
    // pass): delete the live set for this projection wholesale, then
    // copy the rebuild's own set across in its place, the identical
    // "delete then bulk `INSERT ... SELECT`" shape
    // `projection_consumed_event_types` just above already uses. A
    // rebuild whose dispatcher never resolved even once (see this
    // function's own doc comment) has no rebuild-side rows to copy at
    // all - the live set is simply left empty, not clobbered with
    // nothing pretending to be something.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_state WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .execute(&mut *tx)
    .await?;
    // `owner` carried across too - a promoted rebuild keeps whatever
    // ownership it derived while building, exactly as `state` does
    // (cross-tenant projection read fix, docs/architecture.md's own
    // write-up of this pass). `as_of_sequence` carried across for the
    // identical reason (issue #25's investigation finding) -
    // and load-bearing here, not just consistency: leaving it out would
    // have every promoted row's `as_of_sequence` default back to `-1`
    // while `state` already reflects the rebuild's full replay, so the
    // very next `catch_up_bounded_context` tick would see "never folded"
    // and replay every historical event into already-folded state a
    // second time - the exact bug this whole pass fixes, reintroduced
    // right here if this column were dropped from the copy.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_state \
         (projection_name, key, state, owner, as_of_sequence, updated_at) \
         SELECT projection_name, key, state, owner, as_of_sequence, updated_at \
         FROM {schema}.projection_rebuild_state WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_state WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuilds WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(building)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    // The live `schema` just changed, so every instance's GraphQL type
    // for this projection is stale - this one's included (#69).
    notify_registration_changed(pool).await;
    Ok(true)
}
