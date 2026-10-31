//! Persistence for projection records rule groups.

use super::*;

// --- Projection / ProjectionRebuild ---

#[derive(sqlx::FromRow)]
pub(super) struct ProjectionRow {
    pub(super) name: String,
    pub(super) schema: String,
    pub(super) schema_version: i64,
    pub(super) sync: bool,
    pub(super) caught_up_to: Option<i64>,
    pub(super) requires_capability: Option<String>,
    pub(super) owner_tag_key: Option<String>,
}

/// Every `EventType` a `projection_consumed_event_types`-style join table
/// names for one projection - shared by `Projection.consumed_event_types`
/// and `ProjectionRebuild`'s own field of the same name, which read from
/// the two structurally identical join tables `join_table` names.
pub(super) async fn consumed_event_types(
    pool: &Pool,
    bounded_context: &str,
    join_table: &str,
    projection_name: &str,
) -> crate::error::Result<Vec<EventType>> {
    let schema = schema_ident(bounded_context);
    let names: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name FROM {schema}.{join_table} WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .fetch_all(pool)
    .await?;

    let mut event_types = Vec::with_capacity(names.len());
    for (event_type_name,) in names {
        // `require_event_type`, not `.expect()`: a concurrent
        // `hard_delete_bounded_context` committing between the join
        // query above and this lookup makes `get_event_type` come back
        // `None` (its own `get_bounded_context` misses), not a real
        // dangling join row - see `require_bounded_context`.
        event_types.push(require_event_type(pool, bounded_context, &event_type_name).await?);
    }
    Ok(event_types)
}

pub(super) async fn replace_consumed_event_types(
    executor: &mut sqlx::PgConnection,
    bounded_context: &str,
    join_table: &str,
    projection_name: &str,
    event_types: &[EventType],
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.{join_table} WHERE projection_name = $1"
    )))
    .bind(projection_name)
    .execute(&mut *executor)
    .await?;
    for et in event_types {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.{join_table} (projection_name, event_type_name) VALUES ($1,$2)"
        )))
        .bind(projection_name)
        .bind(&et.name)
        .execute(&mut *executor)
        .await?;
    }
    Ok(())
}

pub(super) const PROJECTION_COLUMNS: &str =
    "name, schema, schema_version, sync, caught_up_to, requires_capability, owner_tag_key";

/// See `upsert_event_type` above - same upsert shape, plus replacing this
/// projection's `projection_consumed_event_types` join rows wholesale
/// (simpler and plenty fast enough for a small, admin-managed list than
/// diffing old vs. new membership).
///
/// Both statements run inside one transaction, not back-to-back on the
/// bare pool: two concurrent `upsert_projection` calls for the same
/// `projection.name` (e.g. a rolling deploy's two instances reconciling
/// at once, or - what actually surfaced this, see `skilj-demo`'s test
/// suite - two `#[test]`s racing to register the same projection) used
/// to each run their own `DELETE` then `INSERT` directly against `pool`
/// with no lock between them, so both could pass the `DELETE` before
/// either reached its `INSERT`, then both try to insert the identical
/// `(projection_name, event_type_name)` row and one loses to
/// `projection_consumed_event_types_pkey`. Wrapping the row upsert and
/// the join-table replace in a single transaction fixes this for free:
/// the `INSERT ... ON CONFLICT (name) DO UPDATE` against `projections`
/// takes (and, on the losing side, waits on) that row's lock for the
/// rest of the transaction, so a second concurrent call can't reach its
/// own `DELETE`/`INSERT` pair until the first has committed.
#[tracing::instrument(skip_all)]
pub async fn upsert_projection(pool: &Pool, projection: &Projection) -> crate::error::Result<()> {
    let mut tx = pool.begin().await?;
    upsert_projection_in_tx(&mut tx, projection).await?;
    tx.commit().await?;
    notify_registration_changed(pool).await;
    Ok(())
}

/// [`upsert_projection`] inside the caller's transaction, without the
/// commit or the registration-changed notification.
pub(super) async fn upsert_projection_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    projection: &Projection,
) -> crate::error::Result<()> {
    let schema = schema_ident(&projection.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projections ({PROJECTION_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7) \
         ON CONFLICT (name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            sync = EXCLUDED.sync, caught_up_to = EXCLUDED.caught_up_to, \
            requires_capability = EXCLUDED.requires_capability, owner_tag_key = EXCLUDED.owner_tag_key"
    )))
    .bind(&projection.name)
    .bind(&projection.schema)
    .bind(projection.schema_version)
    .bind(projection.sync)
    .bind(projection.caught_up_to)
    .bind(&projection.requires_capability)
    .bind(&projection.owner_tag_key)
    .execute(&mut **tx)
    .await?;
    replace_consumed_event_types(
        tx,
        &projection.bounded_context.name,
        "projection_consumed_event_types",
        &projection.name,
        &projection.consumed_event_types,
    )
    .await
}

/// Get-or-create-with-lock for one projection instance's own
/// `projection_state` row, keyed by `(projection_name, key)` - the
/// insert-or-update `insert_event_and_update_sync_projections`/
/// `catch_up_bounded_context` both need now that instances are created
/// lazily, on first touch, rather than pre-seeded at registration
/// ([§9](../../../docs/architecture.md#next-steps)'s "keyed / multi-row Projections" pass). `ON CONFLICT ... DO
/// UPDATE SET state = {schema}.projection_state.state` is a no-op write
/// on the already-exists path - it exists purely so Postgres still
/// acquires the row lock there too (the identical guarantee a plain
/// `SELECT ... FOR UPDATE` gave the old always-pre-seeded schema),
/// without ever overwriting real accumulated state with
/// `default_state_json`. Generic over `impl sqlx::PgExecutor<'_>` (the
/// same generalisation `insert_event` itself already has) so both
/// callers can run this inside their own already-open transaction.
///
/// Also returns the row's own `as_of_sequence` (issue #25's
/// investigation finding - see `apply_projection_fold_update`'s own doc
/// comment) - every caller needs it immediately after to decide whether
/// this specific row has already folded the event it's about to fold,
/// the same shape `fold_snapshot_chunk_in_tx`'s own row lock returns for
/// `Snapshot`.
pub(super) async fn get_or_create_projection_state_for_update(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    projection_name: &str,
    key: &str,
    default_state_json: &str,
) -> crate::error::Result<(i64, String)> {
    let (as_of_sequence, state): (i64, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_state (projection_name, key, state, updated_at) \
         VALUES ($1, $2, $3, now()) \
         ON CONFLICT (projection_name, key) DO UPDATE SET state = {schema}.projection_state.state \
         RETURNING as_of_sequence, state"
    )))
    .bind(projection_name)
    .bind(key)
    .bind(default_state_json)
    .fetch_one(executor)
    .await?;
    Ok((as_of_sequence, state))
}

/// Applies one projection fold's `UPDATE ... SET state = ...` - shared by
/// every "fold one event, persist the new state" call site
/// (`insert_event_and_update_sync_projections_in_tx`, the partitioned
/// catch-up, and `fold_history_into_new_sync_projection`). The
/// unpartitioned catch-up folds a chunk of events at once and writes each
/// key back in one statement of its own (docs/architecture.md §182).
/// `table` is `"projection_state"` or `"projection_rebuild_state"`;
/// `extra_where` is appended to the `WHERE` clause verbatim - `""` for the
/// live table, `" AND status = 'building'"` for the rebuild one.
///
/// Also derives and persists this instance's own `owner` column
/// alongside `state` - cross-tenant projection read fix
/// (docs/architecture.md's own write-up of this pass). When
/// `owner_tag_key` is `Some` and `event.tags` carries a tag with that
/// key *and* a non-null value, that value becomes this row's own
/// `owner`; otherwise `owner` is left exactly as already stored - an
/// event lacking the tag (or carrying it with a null value, the "mapped
/// field was absent" case - see `Tag.value` in the spec) never clears an
/// already-established owner. See `plugin::Projection::OWNER_TAG_KEY`'s
/// own doc comment for the full contract.
///
/// Also advances `as_of_sequence` to `event.sequence` (issue
/// #25's investigation finding) - every call site's own `event.sequence`
/// only ever increases within its own walk, so a plain unconditional
/// `SET` is correct here without needing a `GREATEST(...)` guard; what
/// actually makes this safe under two instances racing the same row is
/// each call site's own new pre-fold check against the value
/// `get_or_create_projection_state_for_update` just returned, *before* `dispatcher.project()` is ever called - by the
/// time this function runs, the caller has already decided this event
/// genuinely hasn't been folded into this row yet.
#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_projection_fold_update(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    table: &str,
    extra_where: &str,
    projection_name: &str,
    key: &str,
    new_state: &str,
    owner_tag_key: Option<&str>,
    event: &Event,
) -> crate::error::Result<()> {
    match event_owner(owner_tag_key, event) {
        Some(owner) => {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.{table} SET state = $1, owner = $2, as_of_sequence = $3, \
                 updated_at = now() WHERE projection_name = $4 AND key = $5{extra_where}"
            )))
            .bind(new_state)
            .bind(owner)
            .bind(event.sequence)
            .bind(projection_name)
            .bind(key)
            .execute(executor)
            .await?;
        }
        None => {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.{table} SET state = $1, as_of_sequence = $2, updated_at = now() \
                 WHERE projection_name = $3 AND key = $4{extra_where}"
            )))
            .bind(new_state)
            .bind(event.sequence)
            .bind(projection_name)
            .bind(key)
            .execute(executor)
            .await?;
        }
    }
    Ok(())
}

/// The owner `event` gives a projection row: the value of its tag keyed
/// `owner_tag_key`, when it has one with a non-null value - see
/// [`apply_projection_fold_update`]. Shared with `catch_up_bounded_context`'s
/// chunked fold (docs/architecture.md §182).
pub(super) fn event_owner(owner_tag_key: Option<&str>, event: &Event) -> Option<String> {
    owner_tag_key.and_then(|owner_tag_key| {
        event
            .tags
            .iter()
            .find(|tag| tag.key == owner_tag_key)
            .and_then(|tag| tag.value.clone())
    })
}

/// Set in every `snapshots.snapshot_version` this version writes: its
/// rows fold an event into every value of the tag key the event carries
/// (docs/architecture.md §202), where earlier versions folded it into the
/// first only. A row without it - written before the upgrade, or by an
/// older instance during a rolling deploy - never matches, so it is
/// treated as at an older `Snapshot::VERSION`: absent to a command, and
/// reset and refolded by catch-up when an event next touches its value.
pub(super) const SNAPSHOT_EVERY_VALUE_FOLD: i64 = 1 << 40;

/// The `snapshots.snapshot_version` a row folded by this version at
/// `Snapshot::VERSION` `version` carries.
pub(super) fn stored_snapshot_version(version: u64) -> i64 {
    (version as i64) | SNAPSHOT_EVERY_VALUE_FOLD
}

/// The `Snapshot::VERSION` a stored `snapshot_version` was folded at, as
/// the read functions report it: 0 for a row from before
/// [`SNAPSHOT_EVERY_VALUE_FOLD`], which no current `VERSION` trusts.
pub(super) fn reported_snapshot_version(stored: i64) -> u64 {
    if stored & SNAPSHOT_EVERY_VALUE_FOLD == 0 {
        0
    } else {
        (stored & !SNAPSHOT_EVERY_VALUE_FOLD) as u64
    }
}

/// `(snapshot_version, as_of_sequence, state, updated_at)` -
/// `get_snapshot_state`'s own return shape, named to satisfy
/// `clippy::type_complexity` rather than because anything else reuses
/// it.
pub(super) type SnapshotStateRow = (u64, i64, String, DateTime<Utc>);

/// One snapshot's own materialised state, JSON-encoded, alongside the
/// `snapshot_version` it was written at, the `sequence` it's folded up
/// to, and when that last happened - `None` when nothing has ever been
/// written for this `(snapshot_name, tag_key, tag_value)` triple. Every
/// caller (the GraphQL inspection endpoint, and `submit_command`'s own
/// snapshot-accelerated `decide_from_snapshot` path) treats both
/// "nothing yet" and "a stored `snapshot_version` that no longer
/// matches the currently-registered `Snapshot::VERSION`" identically -
/// a cold start, not an error - see `CommandType::decide_from_snapshot`'s
/// own doc comment. The version is reported without the every-value
/// flag the stored column carries, and as 0 for a row folded before
/// docs/architecture.md §202.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_snapshot_state(
    pool: &Pool,
    bounded_context: &str,
    snapshot_name: &str,
    tag_key: &str,
    tag_value: &str,
) -> crate::error::Result<Option<SnapshotStateRow>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(i64, i64, String, DateTime<Utc>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT snapshot_version, as_of_sequence, state::text, updated_at FROM {schema}.snapshots \
         WHERE snapshot_name = $1 AND tag_key = $2 AND tag_value = $3"
    )))
        .bind(snapshot_name)
        .bind(tag_key)
        .bind(tag_value)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|(version, as_of_sequence, state, updated_at)| {
        (
            reported_snapshot_version(version),
            as_of_sequence,
            state,
            updated_at,
        )
    }))
}

/// `get_snapshot_state`'s own sibling, also returning the row's `owner`
/// column - cross-tenant read fix (docs/architecture.md's own write-up
/// of these passes). A separate function rather than changing
/// `get_snapshot_state`'s own return shape: that function's other
/// caller, `submit_command`'s snapshot-accelerated `decide_from_snapshot`
/// path, is a write-path internal accelerator, not a caller read -
/// deliberately untouched here, the same "`ProcessCommand`'s
/// `matching_events` is deliberately untouched" principle the raw-event
/// pass already stated. Only `snapshot_query::inspect_snapshot_field`,
/// which needs `owner` to enforce `RoleAccessMapping.scope`, calls this
/// one instead.
pub async fn get_snapshot_state_and_owner(
    pool: &Pool,
    bounded_context: &str,
    snapshot_name: &str,
    tag_key: &str,
    tag_value: &str,
) -> crate::error::Result<Option<(SnapshotStateRow, Option<String>)>> {
    type RawSnapshotStateAndOwnerRow = (i64, i64, String, DateTime<Utc>, Option<String>);
    let schema = schema_ident(bounded_context);
    let row: Option<RawSnapshotStateAndOwnerRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT snapshot_version, as_of_sequence, state::text, updated_at, owner \
         FROM {schema}.snapshots WHERE snapshot_name = $1 AND tag_key = $2 AND tag_value = $3"
    )))
    .bind(snapshot_name)
    .bind(tag_key)
    .bind(tag_value)
    .fetch_optional(pool)
    .await?;
    Ok(
        row.map(|(version, as_of_sequence, state, updated_at, owner)| {
            (
                (
                    reported_snapshot_version(version),
                    as_of_sequence,
                    state,
                    updated_at,
                ),
                owner,
            )
        }),
    )
}

/// For each `(snapshot_name, tag_key, tag_value, Snapshot::VERSION)`, in
/// order, the stored row's `(as_of_sequence, state)` when it is current -
/// folded at that `VERSION` under the every-value rule
/// ([`stored_snapshot_version`]) - and `None` when there is none or it
/// isn't. One statement for all of them (docs/architecture.md §202).
pub(super) async fn get_current_snapshot_rows(
    pool: &Pool,
    bounded_context: &str,
    wanted: &[(&str, &str, &str, u64)],
) -> crate::error::Result<Vec<Option<(i64, String)>>> {
    if wanted.is_empty() {
        return Ok(Vec::new());
    }
    let schema = schema_ident(bounded_context);
    let names: Vec<&str> = wanted.iter().map(|w| w.0).collect();
    let keys: Vec<&str> = wanted.iter().map(|w| w.1).collect();
    let values: Vec<&str> = wanted.iter().map(|w| w.2).collect();
    let versions: Vec<i64> = wanted
        .iter()
        .map(|w| stored_snapshot_version(w.3))
        .collect();
    let rows: Vec<(i32, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT w.ord::int, s.as_of_sequence, s.state::text          FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[])              WITH ORDINALITY AS w(snapshot_name, tag_key, tag_value, snapshot_version, ord)          JOIN {schema}.snapshots s ON s.snapshot_name = w.snapshot_name              AND s.tag_key = w.tag_key AND s.tag_value = w.tag_value              AND s.snapshot_version = w.snapshot_version"
    )))
    .bind(&names)
    .bind(&keys)
    .bind(&values)
    .bind(&versions)
    .fetch_all(pool)
    .await?;
    let mut found = vec![None; wanted.len()];
    for (ord, as_of_sequence, state) in rows {
        found[ord as usize - 1] = Some((as_of_sequence, state));
    }
    Ok(found)
}

/// What `submit_command`'s own snapshot-accelerated `decide_from_snapshot`
/// call needs, resolved from a stored `snapshots` row (or a cold start).
pub struct ResolvedSnapshot {
    pub state_json: String,
    pub as_of_sequence: i64,
}

/// Shared by `skilj-graphql`'s `submitCommand` resolver and `skilj-rest`'s
/// `post_commands_trigger` route ([docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)), so the "does
/// this command take the snapshot-accelerated path" decision lives once,
/// not duplicated per surface the way plenty of resolver-local logic
/// legitimately is elsewhere - this one has real, non-trivial rules
/// worth keeping in exactly one place.
///
/// `None` when `snapshot_name` isn't actually registered
/// (`SnapshotDispatcher::tag_key` returning `None`), or when
/// `consistency_tags` isn't *exactly* the snapshot's own single
/// `TAG_KEY` - `CommandType::snapshot()`'s own doc comment on why a
/// multi-tag command opting in is a silent fallback, not an error.
/// `Some` otherwise: a fresh/cold snapshot (nothing stored yet, or a
/// stored `snapshot_version` that no longer matches the currently-
/// registered one - "model changed", `CommandType::decide_from_snapshot`'s
/// own doc comment) resolves to `Snapshot::State::default()`,
/// JSON-encoded, at `as_of_sequence: -1` - the same cold-start shape
/// `fold_snapshot_chunk_in_tx` gives the background catch-up task.
pub async fn resolve_snapshot_context(
    pool: &Pool,
    bounded_context: &str,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    snapshot_name: &str,
    consistency_tags: &[Tag],
) -> crate::error::Result<Option<ResolvedSnapshot>> {
    let Some(tag_key) = snapshot_dispatcher.tag_key(bounded_context, snapshot_name) else {
        return Ok(None);
    };
    let [only_tag] = consistency_tags else {
        return Ok(None);
    };
    if only_tag.key != tag_key {
        return Ok(None);
    }
    let Some(tag_value) = &only_tag.value else {
        return Ok(None);
    };

    let version = snapshot_dispatcher
        .version(bounded_context, snapshot_name)
        .unwrap_or(0);
    let stored = get_current_snapshot_rows(
        pool,
        bounded_context,
        &[(snapshot_name, tag_key, tag_value.as_str(), version)],
    )
    .await?
    .pop()
    .flatten();
    let (as_of_sequence, state_json) = match stored {
        Some((as_of_sequence, state_json)) => (as_of_sequence, state_json),
        _ => (
            -1,
            snapshot_dispatcher
                .default_state(bounded_context, snapshot_name)
                .unwrap_or_default(),
        ),
    };
    Ok(Some(ResolvedSnapshot {
        state_json,
        as_of_sequence,
    }))
}

/// The background catch-up task's own progress marker for every
/// registered snapshot in one bounded context, in one query -
/// issue #15's batched replacement for what used to be a `get_snapshot_progress`
/// call per snapshot name (one query each). A name absent from the
/// returned map has never had a catch-up tick yet - `catch_up_snapshots`
/// treats that the same as this function's own predecessor did
/// (`unwrap_or(-1)` at the call site), just without a per-name round
/// trip to discover it. Plain read, no lock: only used to compute
/// `catch_up_snapshots`' own starting point for the *next* tick, never
/// to decide whether a specific write is safe - that's
/// `fold_snapshot_chunk_in_tx`'s own job, per row.
pub(super) async fn list_snapshot_progress_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<std::collections::HashMap<String, i64>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT snapshot_name, caught_up_to FROM {schema}.snapshot_progress"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().collect())
}

/// Advances (or seeds) one snapshot's own progress marker - called once
/// per event `catch_up_snapshots` processes, for every registered
/// snapshot, regardless of whether that particular event actually
/// touched any of its rows - the identical "position always advances,
/// state only changes when consumed" treatment `Projection`'s own
/// `caught_up_to` already gets.
pub(super) async fn upsert_snapshot_progress(
    executor: impl sqlx::PgExecutor<'_>,
    schema: &str,
    snapshot_name: &str,
    caught_up_to: i64,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.snapshot_progress (snapshot_name, caught_up_to) VALUES ($1, $2) \
         ON CONFLICT (snapshot_name) DO UPDATE SET caught_up_to = $2"
    )))
    .bind(snapshot_name)
    .bind(caught_up_to)
    .execute(executor)
    .await?;
    Ok(())
}

/// One projection instance's own materialised state, JSON-encoded -
/// `None` when nothing has touched this `(projection_name, key)` pair
/// yet (a legitimate, common state now that instances are created
/// lazily - a customer with no purchase history yet, say - not an
/// error; `resolvers::projection_query` falls back to
/// `ProjectionDispatcher::default_state` for it, the same value a fresh
/// instance would lazily start from).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_projection_state(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    key: &str,
) -> crate::error::Result<Option<String>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT state FROM {schema}.projection_state WHERE projection_name = $1 AND key = $2"
    )))
    .bind(projection_name)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(state,)| state))
}

/// `get_projection_state`'s own twin, also returning the row's `owner`
/// column - cross-tenant projection read fix (docs/architecture.md's own
/// write-up of this pass). A separate function rather than changing
/// `get_projection_state`'s own return shape: that function has many
/// existing callers (tests included) uninterested in `owner` at all: only
/// `projection_query`'s resolver, which needs `owner` to enforce
/// `RoleAccessMapping.scope` (see `projections::query_projection`), calls
/// this one instead. `None` for "no row at all" (an untouched key) is not
/// distinguished from `Some((state, None))` at the SQL level by this
/// function - only the caller decides what "no proven owner" versus "no
/// row yet" means for its own enforcement.
pub async fn get_projection_state_and_owner(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    key: &str,
) -> crate::error::Result<Option<(String, Option<String>)>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(String, Option<String>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT state, owner FROM {schema}.projection_state \
         WHERE projection_name = $1 AND key = $2"
    )))
    .bind(projection_name)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// [`get_projection_state_and_owner`] for many `keys` of one projection in
/// one statement - federation's `_entities` resolves a router's whole page
/// of instances at once (docs/architecture.md §199). Keys with no row are
/// absent from the map.
pub async fn get_projection_states_and_owners(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    keys: &[String],
) -> crate::error::Result<std::collections::HashMap<String, (String, Option<String>)>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT key, state, owner FROM {schema}.projection_state \
         WHERE projection_name = $1 AND key = ANY($2)"
    )))
    .bind(projection_name)
    .bind(keys)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(key, state, owner)| (key, (state, owner)))
        .collect())
}

/// A building `ProjectionRebuild`'s own materialised state, JSON-encoded,
/// for one instance/`key`. `None` when either nothing is staged at all
/// or a staged rebuild hasn't reached this key yet (`catch_up_bounded_context`
/// hasn't reached it in a poll tick yet, or the dispatcher has never
/// recognised it - see `ProjectionDispatcher::default_state`). Mirrors
/// `get_projection_state` above - for tests and anything else wanting to
/// read this row directly outside the consumer's own row-locked
/// transaction.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_projection_rebuild_state(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    key: &str,
) -> crate::error::Result<Option<String>> {
    let schema = schema_ident(bounded_context);
    // Always the `'building'` row: a pending rebuild is never folded, so
    // it has no state of its own.
    let row: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT state FROM {schema}.projection_rebuild_state \
         WHERE projection_name = $1 AND status = 'building' AND key = $2"
    )))
    .bind(projection_name)
    .bind(key)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(state,)| state))
}

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
/// Just `name`'s `caught_up_to` - `None` if no such projection, `Some(None)`
/// if it has folded nothing yet. The one column `ProjectionQuery`'s
/// `waitForSequence` loop polls, in one query rather than
/// [`get_projection`]'s three (docs/architecture.md §88).
pub async fn projection_caught_up_to(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<Option<i64>>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(Option<i64>,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT caught_up_to FROM {schema}.projections WHERE name = $1"
    )))
    .bind(name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(caught_up_to,)| caught_up_to))
}

pub async fn get_projection(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<Projection>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let Some(row): Option<ProjectionRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_COLUMNS} FROM {schema}.projections WHERE name = $1"
    )))
    .bind(name)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    let consumed = consumed_event_types(
        pool,
        bounded_context,
        "projection_consumed_event_types",
        name,
    )
    .await?;
    Ok(Some(Projection {
        bounded_context: bc,
        name: row.name,
        schema: row.schema,
        schema_version: row.schema_version,
        consumed_event_types: consumed,
        sync: row.sync,
        caught_up_to: row.caught_up_to,
        owner_tag_key: row.owner_tag_key,
        requires_capability: row.requires_capability,
    }))
}

/// Every `Projection` registered in one bounded context - backs
/// `TypeRegistration`'s own `exposes: for projection in
/// bounded_context.projections`. Same per-row `consumed_event_types`
/// resolution `get_projection` does, just for every row in the context's
/// own `projections` table rather than one name.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_projections_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Projection>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<ProjectionRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_COLUMNS} FROM {schema}.projections"
    )))
    .fetch_all(pool)
    .await?;

    let mut projections = Vec::with_capacity(rows.len());
    for row in rows {
        let consumed = consumed_event_types(
            pool,
            bounded_context,
            "projection_consumed_event_types",
            &row.name,
        )
        .await?;
        projections.push(Projection {
            bounded_context: bc.clone(),
            name: row.name,
            schema: row.schema,
            schema_version: row.schema_version,
            consumed_event_types: consumed,
            sync: row.sync,
            caught_up_to: row.caught_up_to,
            owner_tag_key: row.owner_tag_key,
            requires_capability: row.requires_capability,
        });
    }
    // Name order, and part of the contract for the same reason
    // `list_bounded_contexts`'s is: `skilj-graphql`'s schema build walks
    // these in order and resolves generated type-name collisions
    // first-wins, so whichever projection survives a collision is decided
    // by the order it walks them in. Two contexts colliding needs names
    // like `a_b` + `c` against `a` + `b_c`, but one bounded context can
    // collide with itself too - a nested `{parent}_{field}` type name can
    // equal another of its own top-level names - and then the row order
    // Postgres happens to return is what decides it.
    projections.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(projections)
}

/// The names of `bounded_context`'s `sync` projections - all
/// `insert_event_and_update_sync_projections_in_tx` needs of them (the
/// `ProjectionDispatcher` answers everything else by name). Every call
/// site fetches it once per commit and passes the same slice to every
/// event that commit inserts (issue #32).
///
/// Read on the caller's own transaction, after it took this bounded
/// context's `sequence` row lock: `promote_projection_rebuild` - the one
/// place a projection's `sync` flag can flip mid-flight - takes that same
/// lock before it promotes (drift audit finding #6), so the two can't
/// interleave. It must not be a separate pooled read (docs/architecture.md
/// §116): every other writer to this bounded context waits on that lock
/// holding a connection, so with a pool's worth of them waiting the
/// holder could never get a second one, and all of them failed at the
/// acquire timeout.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn sync_projection_names(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
) -> crate::error::Result<Vec<String>> {
    let schema = schema_ident(bounded_context);
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT name FROM {schema}.projections WHERE sync ORDER BY name"
    )))
    .fetch_all(conn)
    .await?)
}

#[derive(sqlx::FromRow)]
pub(super) struct ProjectionRebuildRow {
    pub(super) projection_name: String,
    pub(super) schema: String,
    pub(super) schema_version: i64,
    pub(super) sync: bool,
    pub(super) caught_up_to: Option<i64>,
    pub(super) status: String,
}

pub(super) fn projection_rebuild_status_to_str(status: ProjectionRebuildStatus) -> &'static str {
    match status {
        ProjectionRebuildStatus::Pending => "pending",
        ProjectionRebuildStatus::Building => "building",
    }
}

pub(super) fn projection_rebuild_status_from_str(s: &str) -> ProjectionRebuildStatus {
    match s {
        "building" => ProjectionRebuildStatus::Building,
        _ => ProjectionRebuildStatus::Pending,
    }
}

pub(super) const PROJECTION_REBUILD_COLUMNS: &str =
    "projection_name, schema, schema_version, sync, caught_up_to, status";

/// `consumed_event_types`/`replace_consumed_event_types` above's own twin
/// for `projection_rebuild_consumed_event_types` - not built on those
/// generic helpers directly, since that table now carries a `status`
/// column the plain `projections`/`projection_consumed_event_types` pair
/// never needed (see `provision_bounded_context_schema`'s own doc comment
/// on why: a pending row and a building row each need their own
/// consumed-types set, never mixed up).
pub(super) async fn rebuild_consumed_event_types(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    status: ProjectionRebuildStatus,
) -> crate::error::Result<Vec<EventType>> {
    let schema = schema_ident(bounded_context);
    let names: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(projection_rebuild_status_to_str(status))
    .fetch_all(pool)
    .await?;

    let mut event_types = Vec::with_capacity(names.len());
    for (event_type_name,) in names {
        // Same race as `consumed_event_types` above: a concurrent
        // `hard_delete_bounded_context` makes this a `RowNotFound`
        // error, not a dangling join row.
        event_types.push(require_event_type(pool, bounded_context, &event_type_name).await?);
    }
    Ok(event_types)
}

pub(super) async fn replace_rebuild_consumed_event_types(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
    projection_name: &str,
    status: ProjectionRebuildStatus,
    event_types: &[EventType],
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let status = projection_rebuild_status_to_str(status);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(status)
    .execute(&mut *conn)
    .await?;
    for et in event_types {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.projection_rebuild_consumed_event_types \
             (projection_name, status, event_type_name) VALUES ($1,$2,$3)"
        )))
        .bind(projection_name)
        .bind(status)
        .bind(&et.name)
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// Whether a rebuild prevents this process from updating the live generation.
/// Call with the projection row locked, so staging cannot race the check.
pub(super) async fn live_fold_paused(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
    name: &str,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
) -> crate::error::Result<bool> {
    if dispatcher.live_fold_compatible(bounded_context, name) {
        return Ok(false);
    }
    let schema = schema_ident(bounded_context);
    let (live_schema, rebuilding): (String, bool) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT schema, EXISTS (SELECT 1 FROM {schema}.projection_rebuilds WHERE projection_name = $1) \
         FROM {schema}.projections WHERE name = $1"
    ))).bind(name).fetch_one(conn).await?;
    Ok(rebuilding
        || dispatcher
            .state_schema(bounded_context, name)
            .is_some_and(|compiled| compiled != live_schema))
}

/// Upsert **within `rebuild.status`'s own row** - matching
/// `RegisterProjection`'s own "restaging replaces the pending row"
/// semantics (`ON CONFLICT (projection_name, status)`, not
/// `projection_name` alone - see `provision_bounded_context_schema`'s own
/// doc comment on why one projection can have both a pending and a
/// building row at once). Never call this to persist a *status
/// transition* (pending becoming building) - that would leave the old
/// row behind as a stale duplicate under its own now-wrong status instead
/// of replacing it; see `transition_projection_rebuild_to_building` for
/// that case, the only one `rebuild_projection`'s own output is ever fed
/// into.
///
/// Note that `register_projection`'s own struct-update construction
/// (`ProjectionRebuild { caught_up_to: None, ..staged.clone() }`) sets
/// `caught_up_to` back to `None` on every restage - any progress
/// `catch_up_bounded_context` had already made toward the old
/// `schema`/`consumed_event_types` is invalidated the moment this is
/// called. This function doesn't touch `projection_rebuild_state` itself,
/// since `catch_up_bounded_context` is what notices `caught_up_to = None`
/// and resets that row, using whatever the *current* dispatcher considers
/// the default (see `ProjectionDispatcher::default_state`'s own doc
/// comment for why it, not this function, has to be the one deciding
/// that value).
#[tracing::instrument(skip_all)]
pub async fn upsert_projection_rebuild(
    pool: &Pool,
    rebuild: &ProjectionRebuild,
) -> crate::error::Result<()> {
    let schema = schema_ident(&rebuild.projection.bounded_context.name);
    let mut tx = pool.begin().await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT 1 FROM {schema}.sequence FOR UPDATE"
    )))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT 1 FROM {schema}.projections WHERE name = $1 FOR NO KEY UPDATE"
    )))
    .bind(&rebuild.projection.name)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_rebuilds ({PROJECTION_REBUILD_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6) \
         ON CONFLICT (projection_name, status) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            sync = EXCLUDED.sync, caught_up_to = EXCLUDED.caught_up_to"
    )))
    .bind(&rebuild.projection.name)
    .bind(&rebuild.schema)
    .bind(rebuild.schema_version)
    .bind(rebuild.sync)
    .bind(rebuild.caught_up_to)
    .bind(projection_rebuild_status_to_str(rebuild.status))
    .execute(&mut *tx)
    .await?;
    replace_rebuild_consumed_event_types(
        &mut tx,
        &rebuild.projection.bounded_context.name,
        &rebuild.projection.name,
        rebuild.status,
        &rebuild.consumed_event_types,
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Persists `rebuild_projection`'s own output - a pending row transitioning
/// to building, per `rule RebuildProjection`'s `ensures: staged.status =
/// building`. One transaction: the old `(projection_name, 'pending')` row
/// and its own join rows are deleted, and the new `(projection_name,
/// 'building')` row (`rebuild`, already carrying `status: Building`) is
/// inserted in their place - never both left behind as if this were two
/// independent rows, and never a window in which neither exists if this
/// fails partway. `ON CONFLICT (projection_name, status) DO UPDATE` on the
/// insert half is defensive, not the expected path: `RebuildProjection`'s
/// own `requires: staged.status = pending` doesn't check for an
/// *already*-building row, so a caller triggering it while one is still
/// replaying re-stages the build under the freshly toggled row rather
/// than erroring - still at most one building row afterward, satisfying
/// `UniqueRebuildPerProjectionAndStatus` either way.
#[tracing::instrument(skip_all)]
pub async fn transition_projection_rebuild_to_building(
    pool: &Pool,
    rebuild: &ProjectionRebuild,
) -> crate::error::Result<()> {
    debug_assert_eq!(
        rebuild.status,
        ProjectionRebuildStatus::Building,
        "transition_projection_rebuild_to_building's own rebuild must already carry status: \
         Building - rebuild_projection's own output does"
    );
    let mut tx = pool.begin().await?;
    let schema = schema_ident(&rebuild.projection.bounded_context.name);
    let pending = projection_rebuild_status_to_str(ProjectionRebuildStatus::Pending);

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(&rebuild.projection.name)
    .bind(pending)
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuilds WHERE projection_name = $1 AND status = $2"
    )))
    .bind(&rebuild.projection.name)
    .bind(pending)
    .execute(&mut *tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.projection_rebuilds ({PROJECTION_REBUILD_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6) \
         ON CONFLICT (projection_name, status) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            sync = EXCLUDED.sync, caught_up_to = EXCLUDED.caught_up_to"
    )))
    .bind(&rebuild.projection.name)
    .bind(&rebuild.schema)
    .bind(rebuild.schema_version)
    .bind(rebuild.sync)
    .bind(rebuild.caught_up_to)
    .bind(projection_rebuild_status_to_str(rebuild.status))
    .execute(&mut *tx)
    .await?;
    let building = projection_rebuild_status_to_str(rebuild.status);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(&rebuild.projection.name)
    .bind(building)
    .execute(&mut *tx)
    .await?;
    for et in &rebuild.consumed_event_types {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.projection_rebuild_consumed_event_types \
             (projection_name, status, event_type_name) VALUES ($1,$2,$3)"
        )))
        .bind(&rebuild.projection.name)
        .bind(building)
        .bind(&et.name)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// `None` when this projection has no rebuild staged *at this `status`* -
/// the `staged` parameter `register_projection`/`rebuild_projection`/
/// `discard_projection_rebuild` each expect "as already looked up by the
/// caller" (see their own doc comments) is always the *pending* row per
/// the spec's own `let staged = ProjectionRebuild{projection: ...,
/// status: pending}` in every one of those three rules -
/// `catch_up_bounded_context` is the one caller that asks for `Building`
/// instead.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_projection_rebuild(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    status: ProjectionRebuildStatus,
) -> crate::error::Result<Option<ProjectionRebuild>> {
    let Some(projection) = get_projection(pool, bounded_context, projection_name).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let Some(row): Option<ProjectionRebuildRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROJECTION_REBUILD_COLUMNS} FROM {schema}.projection_rebuilds \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(projection_rebuild_status_to_str(status))
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };
    debug_assert_eq!(
        row.projection_name, projection_name,
        "projection_rebuilds row matched the WHERE clause but its own projection_name column disagrees"
    );
    let consumed =
        rebuild_consumed_event_types(pool, bounded_context, projection_name, status).await?;
    Ok(Some(ProjectionRebuild {
        projection,
        schema: row.schema,
        schema_version: row.schema_version,
        consumed_event_types: consumed,
        sync: row.sync,
        caught_up_to: row.caught_up_to,
        status: projection_rebuild_status_from_str(&row.status),
    }))
}

/// Persists `discard_projection_rebuild`'s outcome - the spec's own
/// `ensures` is a deletion (`not exists staged`), which that pure
/// function can't perform itself (see its own doc comment); this is the
/// caller-side deletion it hands back to. `status` is always `Pending` in
/// practice - `DiscardProjectionRebuild`'s own `staged` is a pending row
/// by definition (see `rule DiscardProjectionRebuild`'s `let`) - but a
/// real parameter rather than hardcoded, matching `get_projection_rebuild`'s
/// own shape, since the caller (not this function) is what already knows
/// which row `discard_projection_rebuild`'s own `staged` argument was.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn delete_projection_rebuild(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    status: ProjectionRebuildStatus,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let status = projection_rebuild_status_to_str(status);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuild_consumed_event_types \
         WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(status)
    .execute(pool)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.projection_rebuilds WHERE projection_name = $1 AND status = $2"
    )))
    .bind(projection_name)
    .bind(status)
    .execute(pool)
    .await?;
    Ok(())
}
