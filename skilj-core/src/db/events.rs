//! Persistence for events rule groups.

use super::*;

// --- next_sequence ---

/// Backs the `next_sequence` black box (see the note above rule
/// `CreateExternalEvent` and `event_store::create_external_event`'s own
/// doc comment): allocates the next `Event.sequence` for a bounded
/// context, serialised via the row lock the `UPDATE ... RETURNING`
/// acquires - concurrent callers block on it rather than racing to the
/// same value. The context's own `{schema}.sequence` row is seeded once,
/// at provisioning time (see `provision_bounded_context_schema`), so this
/// is a plain `UPDATE` - no more insert-if-missing step needed now that a
/// bounded context can't exist at all without one.
///
/// Generic over `sqlx::PgExecutor` rather than `&Pool` specifically -
/// every real call site now passes `&mut *tx` from an already-open
/// transaction, so this row's own lock is held for that whole
/// transaction rather than released the instant this one statement's
/// implicit autocommit finishes (see the note above the rules: "locked
/// ... as part of the same transaction that inserts the new Command/
/// Event rows, then incremented and released on commit" - the gaplessness
/// guarantee this gives depends on that, not on this function alone). A
/// bare `&Pool` still works too (autocommit, the pre-fix behaviour) -
/// every direct `db::next_sequence(&pool, ...)` test call keeps compiling
/// unchanged.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn next_sequence<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<i64> {
    let schema = schema_ident(bounded_context);
    let (next,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.sequence SET next_value = next_value + 1 RETURNING next_value"
    )))
    .fetch_one(executor)
    .await?;
    Ok(next)
}

/// `next_sequence`'s own batch form - one command triggering several
/// events (`CommandDecision::Accepted { events }` with more than one
/// `EventSpec`) used to call `next_sequence` once per event, each its own
/// round trip to Postgres, all of them while `submit_command`'s own
/// bounded-context lock is held (issue #32: that lock hold
/// duration, not pool size, is what caps real-world write throughput).
/// This claims the whole range in one `UPDATE ... RETURNING`, exactly the
/// same row-lock-based serialisation as the single-value form (already
/// held by the `SELECT ... FOR UPDATE` every real caller takes first), so
/// the semantics are identical - just one round trip for `count` sequence
/// numbers instead of `count` of them. `count == 0` short-circuits without
/// touching the row at all, since `next_value + 0` would still be a wasted
/// round trip for a command whose decision produced no events.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn next_sequence_batch<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
    count: i64,
) -> crate::error::Result<Vec<i64>> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let schema = schema_ident(bounded_context);
    let (last,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.sequence SET next_value = next_value + $1 RETURNING next_value"
    )))
    .bind(count)
    .fetch_one(executor)
    .await?;
    Ok(((last - count + 1)..=last).collect())
}

/// A bounded context's override of the builder's payload limits, as its
/// `sequence` row stores it - see [`crate::payload_limits`]
/// (docs/architecture.md §238). Read by the same statement that locks the
/// row for a write, so it costs no round trip and is never stale.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PayloadLimitsOverride {
    /// Replaces `max_payload_bytes` when set.
    pub max_payload_bytes: Option<i64>,
    /// Replaces `warn_payload_bytes` when set.
    pub warn_payload_bytes: Option<i64>,
}

impl PayloadLimitsOverride {
    /// `base` - the builder's limits - with this override applied.
    pub fn apply(
        self,
        base: crate::payload_limits::PayloadLimits,
    ) -> crate::payload_limits::PayloadLimits {
        base.overridden(self.max_payload_bytes, self.warn_payload_bytes)
    }
}

/// The override columns of `{schema}.sequence s`, read through `to_jsonb`
/// rather than by name: a bounded context an older instance provisioned
/// mid rolling deploy has no such columns until a newer instance patches
/// it (`ensure_payload_limit_columns`), and naming a missing column would
/// fail every write to it. Absent reads as null - no override.
pub(super) const PAYLOAD_LIMIT_COLUMNS: &str = "(to_jsonb(s) ->> 'max_payload_bytes')::bigint, \
     (to_jsonb(s) ->> 'warn_payload_bytes')::bigint";

/// [`next_sequence`], also returning the bounded context's payload-limit
/// override from the row it locks.
pub(super) async fn next_sequence_with_limits<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<(i64, PayloadLimitsOverride)> {
    let schema = schema_ident(bounded_context);
    let (next, max_payload_bytes, warn_payload_bytes): (i64, Option<i64>, Option<i64>) =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.sequence s SET next_value = next_value + 1 \
             RETURNING next_value, {PAYLOAD_LIMIT_COLUMNS}"
        )))
        .fetch_one(executor)
        .await?;
    Ok((
        next,
        PayloadLimitsOverride {
            max_payload_bytes,
            warn_payload_bytes,
        },
    ))
}

/// A bounded context's payload-limit override, read on its own - for a
/// caller that doesn't hold the sequence lock and only needs to know the
/// limits (`skilj`'s read surfaces, the admin console).
pub async fn payload_limits_override(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<PayloadLimitsOverride> {
    let schema = schema_ident(bounded_context);
    let (max_payload_bytes, warn_payload_bytes): (Option<i64>, Option<i64>) =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {PAYLOAD_LIMIT_COLUMNS} FROM {schema}.sequence s"
        )))
        .fetch_one(pool)
        .await?;
    Ok(PayloadLimitsOverride {
        max_payload_bytes,
        warn_payload_bytes,
    })
}

/// Stores a bounded context's payload-limit override; `None` clears that
/// half of it, back to the builder's value. Takes the sequence row's lock,
/// so a write already holding it finishes under the old limits and the
/// next one sees the new ones. Superadmin-only: the caller decides that
/// with [`crate::bootstrap::set_payload_limits`] first.
pub async fn set_payload_limits(
    pool: &Pool,
    bounded_context: &str,
    limits: PayloadLimitsOverride,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    // A bounded context provisioned before the columns existed.
    ensure_payload_limit_columns(pool, bounded_context).await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.sequence SET max_payload_bytes = $1, warn_payload_bytes = $2"
    )))
    .bind(limits.max_payload_bytes)
    .bind(limits.warn_payload_bytes)
    .execute(pool)
    .await?;
    Ok(())
}

/// The identity of `bounded_context`'s `events` table (its OID) together
/// with [`latest_sequence`], in one round trip. Hard-deleting a bounded
/// context drops its schema and frees the name for reuse, and recreating
/// it creates a new table with a new OID - so anything keyed by the *name*
/// (the event cache's windows) can tell a successor from its predecessor,
/// which sequences alone can't (docs/architecture.md §95).
///
/// The identity includes the database's [`Epoch`] too (docs/architecture.md
/// §176): a promoted standby keeps the table's OID, but its log can end
/// before the old primary's did, and the sequences past that end are
/// reused for different events.
///
/// The same round trip also stamps the bounded context's type
/// registrations ([`RegistrationsStamp`]), so the event cache notices a
/// re-registration - by any instance - before serving events that carry
/// copies of the old one (docs/architecture.md §180). The stamp is
/// `registrations_generation`'s counter (§189); a bounded context without
/// one - provisioned by an older instance mid rolling deploy, and not yet
/// patched by a restart - is stamped by hashing its registrations instead.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn events_table_identity(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<EventsTableState> {
    let schema = schema_ident(bounded_context);
    let identity = |registrations: String| {
        format!(
            "SELECT '{schema}.events'::regclass::oid::bigint, \
                    (SELECT MAX(sequence) FROM {schema}.events), {EPOCH_SQL}, ({registrations})"
        )
    };
    let counted = sqlx::query_as(sqlx::AssertSqlSafe(identity(format!(
        "SELECT 'g' || generation FROM {schema}.registrations_generation"
    ))))
    .fetch_one(pool)
    .await;
    let (oid, max, epoch, registrations): (i64, Option<i64>, String, String) = match counted {
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => {
            sqlx::query_as(sqlx::AssertSqlSafe(identity(format!(
                "SELECT (SELECT md5(coalesce(string_agg((to_jsonb(t) - 'schema' \
                    - 'schedule_position' - 'last_fired_at')::text, ',' ORDER BY t.name), '')) \
                 FROM {schema}.event_types t) \
                || (SELECT md5(coalesce(string_agg((to_jsonb(c) - 'schema')::text, ',' \
                    ORDER BY c.name), '')) FROM {schema}.command_types c)"
            ))))
            .fetch_one(pool)
            .await?
        }
        counted => counted?,
    };
    Ok(EventsTableState {
        table: TableIdentity {
            oid,
            epoch: Epoch(epoch),
        },
        latest: max,
        registrations: RegistrationsStamp(registrations),
    })
}

/// What [`events_table_identity`] reads in its one round trip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventsTableState {
    pub table: TableIdentity,
    /// [`latest_sequence`].
    pub latest: Option<i64>,
    pub registrations: RegistrationsStamp,
}

/// A fingerprint of a bounded context's `EventType`/`CommandType`
/// registrations: it changes whenever a declaration does
/// (`private_fields`, `owner_tag_key`, `tag_mappings`, ...). A scheduled
/// fire moves only `schedule_position`/`last_fired_at`, which don't count,
/// so firing doesn't look like a re-registration (docs/architecture.md
/// §180). Normally `registrations_generation`'s counter (§189), else a hash
/// of the registrations - see [`events_table_identity`]. Compared for
/// equality, never parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrationsStamp(String);

/// Which `events` table, on which history, a set of events came from -
/// see [`events_table_identity`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableIdentity {
    pub oid: i64,
    pub epoch: Epoch,
}

/// The database's epoch: its system identifier and current timeline,
/// `"{system_identifier}-{timeline}"` (docs/architecture.md §176). A
/// promoted standby keeps the system identifier but starts a new
/// timeline, and so does a point-in-time restore; a fresh cluster (a
/// dump restored elsewhere) has a new system identifier. In each case the
/// log may end before positions held outside the database - a client's
/// `after`, a bridge's consumed batch, a broker's acknowledged offset -
/// and later sequences are reused for different events, so a position is
/// only meaningful together with the epoch it was read in. Opaque to
/// clients: compared for equality, never parsed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Epoch(String);

impl Epoch {
    /// An epoch a client handed back, to compare against [`current_epoch`].
    pub fn from_client(epoch: impl Into<String>) -> Self {
        Self(epoch.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Epoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The SQL expression for [`Epoch`]. Both functions are executable by
/// every role by default. The timeline comes from the current WAL file's
/// name: it changes the moment a standby is promoted, where
/// `pg_control_checkpoint()`'s only changes at the next checkpoint. Valid
/// on a primary only, which is the only server skilj talks to.
///
/// `pg_control_system()` reads `global/pg_control` under `ControlFileLock`
/// on every call, and this runs on every event-cache freshen, so the
/// system identifier is kept in a session setting the first time and read
/// from there after (docs/architecture.md §195): it can't change for one
/// session, which is always connected to the same server. `nullif` because
/// `DISCARD ALL` (a pooler's reset) leaves the setting empty, not unset.
pub(super) const EPOCH_SQL: &str =
    "coalesce(nullif(current_setting('skilj.system_identifier', true), ''), \
     set_config('skilj.system_identifier', \
         (SELECT system_identifier::text FROM pg_control_system()), false)) \
     || '-' || substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8)";

/// The database's current [`Epoch`] - on a transaction's connection, the
/// epoch of the server that transaction commits on.
pub async fn current_epoch<'e>(executor: impl sqlx::PgExecutor<'e>) -> crate::error::Result<Epoch> {
    let (epoch,): (String,) = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT {EPOCH_SQL}")))
        .fetch_one(executor)
        .await?;
    Ok(Epoch(epoch))
}

/// Refuses `supplied` - an epoch a client read a position in - unless it
/// is `current` (docs/architecture.md §176). `None` passes: a client that
/// sends no epoch is served as before.
pub fn require_epoch(supplied: Option<&str>, current: &Epoch) -> crate::error::Result<()> {
    match supplied {
        Some(supplied) if supplied != current.as_str() => {
            Err(crate::event_store::Error::EpochChanged {
                supplied: supplied.to_string(),
                current: current.to_string(),
            }
            .into())
        }
        _ => Ok(()),
    }
}

/// [`latest_sequence`] and [`current_epoch`] in one round trip, so both
/// come from the same server: what a client-supplied position is checked
/// against (docs/architecture.md §148, §176).
pub async fn latest_sequence_and_epoch(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<(Option<i64>, Epoch)> {
    let schema = schema_ident(bounded_context);
    let (max, epoch): (Option<i64>, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT (SELECT MAX(sequence) FROM {schema}.events), {EPOCH_SQL}"
    )))
    .fetch_one(pool)
    .await?;
    Ok((max, Epoch(epoch)))
}

/// Records `epoch` as the last one seen and returns the one recorded
/// before it, when it differs - `None` the first time and while it's
/// unchanged. Of several instances noticing the same change, only the
/// first gets it back, so it's reported once (docs/architecture.md §176).
pub async fn record_epoch(
    pool: &Pool,
    epoch: &Epoch,
    now: DateTime<Utc>,
) -> crate::error::Result<Option<Epoch>> {
    let previous: Option<(Option<String>,)> = sqlx::query_as(
        "WITH previous AS (SELECT epoch FROM skilj_epoch WHERE id = 1) \
         INSERT INTO skilj_epoch (id, epoch, recorded_at) VALUES (1, $1, $2) \
         ON CONFLICT (id) DO UPDATE SET epoch = EXCLUDED.epoch, recorded_at = EXCLUDED.recorded_at \
         WHERE skilj_epoch.epoch <> EXCLUDED.epoch \
         RETURNING (SELECT epoch FROM previous)",
    )
    .bind(epoch.as_str())
    .bind(now)
    .fetch_optional(pool)
    .await?;
    Ok(previous.and_then(|(previous,)| previous).map(Epoch))
}

/// The highest `sequence` currently committed in a bounded context -
/// `None` when it has no events at all yet. `catch_up_bounded_context`'s
/// own cheap first check every poll tick, so a quiet context (nothing
/// since the last tick) costs one small aggregate query, not a full event
/// reload - `list_events_for_bounded_context_from` only ever runs once
/// this comes back higher than everything that still needs catching up.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn latest_sequence(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(bounded_context);
    let (max,): (Option<i64>,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT MAX(sequence) FROM {schema}.events"
    )))
    .fetch_one(pool)
    .await?;
    Ok(max)
}

// --- Event ---

/// One row's origin, for `get_event_by_sequence`'s single-row read -
/// reads of many rows resolve origins in one batch through
/// `events_from_rows` instead (docs/architecture.md §125). Async, unlike
/// every other `*_from_row`/`*_to_str` helper in this module, because
/// `command_triggered` needs a second query - `EventOrigin::CommandTriggered`
/// embeds a whole `Command` by value (see the migration's own doc
/// comment on `commands`), not just an id.
pub(super) async fn event_origin_from_row(
    pool: &Pool,
    bounded_context: &str,
    kind: &str,
    source_content: Option<String>,
    source_context: Option<String>,
    command_id: Option<i64>,
) -> crate::error::Result<EventOrigin> {
    Ok(match kind {
        "external_triggered" => EventOrigin::ExternalTriggered {
            source_content: source_content
                .expect("external_triggered event row without origin_source_content"),
            source_context,
        },
        "directly_created" => EventOrigin::DirectlyCreated,
        "system_triggered" => EventOrigin::SystemTriggered,
        "skilj_recorded" => EventOrigin::SkiljRecorded,
        "command_triggered" => {
            let command_id =
                command_id.expect("command_triggered event row without origin_command_id");
            let command = require_command(pool, bounded_context, command_id).await?;
            EventOrigin::CommandTriggered {
                command: Box::new(command),
            }
        }
        other => EventOrigin::Unknown {
            kind: other.to_string(),
        },
    })
}

/// Every `Event` currently stored for one `(bounded_context, event_type)`
/// pair, ordered by `sequence` - the full-snapshot parameter
/// `fetch_events`/`consume_events` each expect (see their own doc
/// comments). `list_events_cached` below is what their own real REST
/// call sites use instead (`crate::event_cache`'s own module doc
/// comment); this function is its unconditional-load fallback, and
/// still used directly wherever the whole type's history is genuinely
/// wanted regardless of recency.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events(
    pool: &Pool,
    bounded_context: &str,
    event_type_name: &str,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;
    let et = require_event_type(pool, bounded_context, event_type_name).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, metadata_client_id, \
         metadata_created_at, metadata_correlation_id, metadata_causation_id, tags, \
         origin_kind, origin_source_content, origin_source_context, \
         origin_command_id FROM {schema}.events WHERE event_type_name = $1 ORDER BY sequence"
    )))
    .bind(event_type_name)
    .fetch_all(pool)
    .await?;

    let known = std::collections::HashMap::from([(et.name.clone(), et)]);
    events_from_rows(&mut *pool.acquire().await?, &bc, rows, Some(&known)).await
}

#[derive(sqlx::FromRow)]
pub(super) struct EventRowAnyType {
    pub(super) event_type_name: String,
    pub(super) sequence: i64,
    pub(super) payload: String,
    pub(super) metadata_type: String,
    pub(super) metadata_version: i64,
    pub(super) metadata_client_id: String,
    pub(super) metadata_created_at: DateTime<Utc>,
    // issue #18 - `None` only for a row written before these
    // columns existed.
    pub(super) metadata_correlation_id: Option<String>,
    pub(super) metadata_causation_id: Option<String>,
    pub(super) tags: Json<Vec<Tag>>,
    pub(super) origin_kind: String,
    pub(super) origin_source_content: Option<String>,
    pub(super) origin_source_context: Option<String>,
    pub(super) origin_command_id: Option<i64>,
}

/// Every `Event` currently stored for a whole bounded context, across
/// every `EventType`, ordered by `sequence` - the full-snapshot parameter
/// `consistency_boundary_and_matching_events` (and so
/// `event_store::process_command`) expects for `bounded_context_events`,
/// per §1.7's own persistence note. Unlike `list_events` above, a given
/// row's `EventType` isn't known ahead of the query, so each distinct
/// `event_type_name` seen is resolved once and cached for the rest of
/// this call rather than reloaded per row.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events ORDER BY sequence"
    )))
    .fetch_all(pool)
    .await?;

    events_from_rows(&mut *pool.acquire().await?, &bc, rows, None).await
}

/// Every `Event` committed after `after_sequence`, ordered by `sequence`.
/// `catch_up_bounded_context`'s own bounded load, unlike
/// `list_events_for_bounded_context` above's "always reload everything"
/// shape. Same per-row resolution as that function, just with a `WHERE`
/// clause and a starting point.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context_from(
    pool: &Pool,
    bounded_context: &str,
    after_sequence: i64,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         WHERE sequence > $1 ORDER BY sequence"
    )))
    .bind(after_sequence)
    .fetch_all(pool)
    .await?;

    events_from_rows(&mut *pool.acquire().await?, &bc, rows, None).await
}

/// `list_events_for_bounded_context_from`'s own capped sibling -
/// issue #25's `catch_up_partitioned_projection` is the one
/// caller (`MAX_EVENTS_PER_PARTITION_TICK`'s own doc comment explains
/// why it needs a cap that function's other caller, `catch_up_snapshots`,
/// doesn't). A separate function rather than an optional `limit`
/// parameter added to `list_events_for_bounded_context_from` itself, so
/// that function's existing, unrelated caller stays untouched.
pub async fn list_events_for_bounded_context_from_limited(
    pool: &Pool,
    bounded_context: &str,
    after_sequence: i64,
    limit: i64,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         WHERE sequence > $1 ORDER BY sequence LIMIT $2"
    )))
    .bind(after_sequence)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    events_from_rows(&mut *pool.acquire().await?, &bc, rows, None).await
}

/// Tag-indexed sibling of `list_events_for_bounded_context`/`_from` -
/// [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 1" fix. Those two always fetch
/// the *whole* bounded context and leave tag-matching to the caller's
/// own in-memory filter (`consistency_boundary_and_matching_events`'s
/// own union semantics) - fine while a bounded context is small, but a
/// full unfiltered scan on every command submission once it isn't. This
/// pushes the same union-of-tags matching down into the query itself,
/// through `event_tags` (docs/architecture.md §210; §19's GIN index on
/// `events.tags` before it) - one sub-select per wanted tag, their union,
/// optionally further bounded by `sequence > after_sequence` (folds in
/// both `queryEvents`' own pagination and `submit_command`'s
/// DCB-conflict redispatch check, which needs the identical "tag-scoped
/// and newer than X" shape - see that function's own call site).
///
/// An empty `tags` slice returns `Ok(vec![])` without ever touching
/// Postgres - mirrors `consistency_boundary_and_matching_events`'s own
/// "no `tag_mappings` declared" case exactly, so a `CommandType`/
/// `EventType` with no tags sees no behavioural change at all.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context_matching_tags(
    pool: &Pool,
    bounded_context: &str,
    tags: &[Tag],
    after_sequence: Option<i64>,
) -> crate::error::Result<Vec<Event>> {
    if tags.is_empty() {
        return Ok(Vec::new());
    }

    let bc = require_bounded_context(pool, bounded_context).await?;
    list_events_for_bounded_context_matching_tags_with_bc(
        &mut *pool.acquire().await?,
        &bc,
        tags,
        after_sequence,
        None,
        None,
    )
    .await
}

/// The same query [`list_events_for_bounded_context_matching_tags`] runs,
/// for a caller that already has the `BoundedContext` row in hand and
/// would otherwise be re-fetching it redundantly - `submit_one_command_in_tx`'s
/// own per-command DCB-conflict delta check is exactly that caller: every
/// command in a `CommandBatcher` batch shares the identical bounded
/// context (that's what the batch's own queue is keyed on), and
/// `command_type.bounded_context` is already that same row, loaded once
/// when the command type itself was resolved - re-fetching it (plus,
/// transitively, `get_role` for its `created_by_role_id`) once per
/// command in a large, self-tuning batch was pure round-trip waste sitting
/// inside the batch leader's own held lock.
pub(super) async fn list_events_for_bounded_context_matching_tags_with_bc(
    conn: &mut sqlx::PgConnection,
    bc: &BoundedContext,
    tags: &[Tag],
    after_sequence: Option<i64>,
    known_event_types: Option<&std::collections::HashMap<String, EventType>>,
    limit: Option<i64>,
) -> crate::error::Result<Vec<Event>> {
    if tags.is_empty() {
        return Ok(Vec::new());
    }

    let schema = schema_ident(&bc.name);
    // One `event_tags` sub-select per wanted tag, their union the events'
    // sequences (docs/architecture.md §210). Parameters: each tag's key and
    // value, then the position and the limit.
    let mut binds: Vec<Option<String>> = Vec::new();
    let types = all_event_types(&schema);
    let after_param = 2 * tags.len() + 1;
    let limit_param = after_param + usize::from(after_sequence.is_some());
    let filter = match after_sequence {
        Some(_) => format!(" AND t.sequence > ${after_param}::bigint"),
        None => String::new(),
    };
    let selects: Vec<String> = tags
        .iter()
        .map(|tag| {
            binds.push(Some(tag.key.clone()));
            let key = format!("${}", binds.len());
            let value = match &tag.value {
                Some(value) => {
                    binds.push(Some(value.clone()));
                    format!("= ${}", binds.len())
                }
                None => {
                    // Keeps the parameter numbering fixed.
                    binds.push(None);
                    format!("IS NULL AND ${}::text IS NULL", binds.len())
                }
            };
            event_tags_select(
                &schema,
                &key,
                &value,
                &types,
                &filter,
                limit.map(|_| (TagOrder::Oldest, format!("${limit_param}::bigint"))),
            )
        })
        .collect();
    let limit_clause = match limit {
        Some(_) => format!(" LIMIT ${limit_param}::bigint"),
        None => String::new(),
    };
    let sql = format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         WHERE sequence = ANY(ARRAY({})) ORDER BY sequence{limit_clause}",
        selects.join(" UNION ")
    );
    let mut query = sqlx::query_as::<_, EventRowAnyType>(sqlx::AssertSqlSafe(sql));
    for bind in binds {
        query = query.bind(bind);
    }
    if let Some(after_sequence) = after_sequence {
        query = query.bind(after_sequence);
    }
    if let Some(limit) = limit {
        query = query.bind(limit);
    }
    let rows: Vec<EventRowAnyType> = query.fetch_all(&mut *conn).await?;

    events_from_rows(conn, bc, rows, known_event_types).await
}

/// The statements built from [`event_tags_select`] read the events as
/// `sequence = ANY(ARRAY(<sub-selects>))`, not `sequence IN (...)`
/// (docs/architecture.md §210). With `IN`, Postgres chose a join from row
/// estimates, and the generic plan it settles on after a few executions
/// of a statement - planned for a tag with a few events - took 2.5 times
/// as long on a long-lived tag. An array leaves it one plan: primary-key
/// lookups, in sequence order.
///
/// Which end of a tag's matches [`event_tags_select`] keeps, when it keeps
/// only some.
#[derive(Clone, Copy)]
pub(super) enum TagOrder {
    /// The first matches - a chunk of a read in sequence order.
    Oldest,
    /// The last matches - an item with `latest`.
    Newest,
}

/// Every event type `schema`'s bounded context registers, as a SQL array:
/// what a tag read with no types of its own probes `event_tags` for. A
/// superset of the type of any event, which `events.event_type_name`
/// references (docs/architecture.md §210). Evaluated once per statement.
pub(super) fn all_event_types(schema: &str) -> String {
    format!("ARRAY(SELECT name FROM {schema}.event_types)")
}

/// A sub-select of the sequences in `event_tags` carrying the tag `key`
/// (`value_condition` on its value) with one of `types`, and `filter` -
/// `AND` conditions on `t`, its position and any further tags - in no
/// particular order (docs/architecture.md §210).
///
/// The dictionary resolves the exact tag ID in the same SQL snapshot. The
/// reference index is `(tag_id, event_type_name, sequence)`, so a
/// tag's events are in sequence order within each type, not across them.
/// Without `limit` that doesn't matter: one index range per type. With
/// it - an item's `latest`, or a chunk - each type's range is read from
/// the wanted end, `limit` entries at most, through a lateral join, and
/// the per-type results are cut to `limit` again. Ordering the whole
/// tag instead would read every entry of a long-lived tag to keep one.
pub(super) fn event_tags_select(
    schema: &str,
    key: &str,
    value_condition: &str,
    types: &str,
    filter: &str,
    limit: Option<(TagOrder, String)>,
) -> String {
    // Resolve the exact string identity once, before scanning any event
    // references. An absent dictionary entry selects nothing. The lookup
    // and history share one statement snapshot, including explicit nulls.
    let tag_id = format!(
        "(SELECT id FROM {schema}.event_tag_values \
         WHERE tag_key = {key} AND tag_value {value_condition})"
    );
    match limit {
        None => format!(
            "(SELECT t.sequence FROM {schema}.event_tag_refs t \
             WHERE t.tag_id = {tag_id} \
             AND t.event_type_name = ANY({types}){filter})"
        ),
        Some((order, limit)) => {
            let direction = match order {
                TagOrder::Oldest => "",
                TagOrder::Newest => " DESC",
            };
            format!(
                "(SELECT l.sequence FROM unnest({types}) AS ty(name) \
                 CROSS JOIN LATERAL (SELECT t.sequence FROM {schema}.event_tag_refs t \
                    WHERE t.tag_id = {tag_id} \
                    AND t.event_type_name = ty.name{filter} \
                    ORDER BY t.sequence{direction} LIMIT {limit}) l \
                 ORDER BY l.sequence{direction} LIMIT {limit})"
            )
        }
    }
}

/// The events of `bc` matching `query`, after `after_sequence` when given,
/// in sequence order - an item with `latest` contributes only that many
/// of its matches, the newest (docs/architecture.md §198). One statement, so one snapshot: the
/// union of one sub-select per item.
///
/// An item with tags is read through `event_tags`: an index probe on its
/// first tag and each of its types - every registered type when it names
/// none - each other tag checked on the event it finds. That is what keeps
/// "the latest lifecycle event of this company" one probe per type however
/// many other events carry the company's tag (docs/architecture.md §210).
/// One with types and no tags goes through `events_by_type`.
///
/// The statement's text depends only on the shape of `query` - how many
/// items, which kinds, how many tags, and whether a tag's value is null -
/// so a command type always sends the same few to the statement cache.
pub(super) async fn list_events_matching_query_with_bc(
    conn: &mut sqlx::PgConnection,
    bc: &BoundedContext,
    query: &[QueryItem],
    after_sequence: Option<i64>,
    known_event_types: Option<&std::collections::HashMap<String, EventType>>,
) -> crate::error::Result<Vec<Event>> {
    list_events_matching_items_with_bc(
        conn,
        bc,
        query,
        &vec![after_sequence; query.len()],
        known_event_types,
    )
    .await
}

/// [`list_events_matching_query_with_bc`] with a position of its own per
/// item: `afters[i]` for `query[i]`. A snapshot item reads its tag value's
/// events after the stored row's `as_of_sequence`, the other items their
/// whole history, in the one statement (docs/architecture.md §202). The
/// result is every event some item matched past that item's position.
pub(super) async fn list_events_matching_items_with_bc(
    conn: &mut sqlx::PgConnection,
    bc: &BoundedContext,
    query: &[QueryItem],
    afters: &[Option<i64>],
    known_event_types: Option<&std::collections::HashMap<String, EventType>>,
) -> crate::error::Result<Vec<Event>> {
    if query.is_empty() {
        return Ok(Vec::new());
    }
    let schema = schema_ident(&bc.name);
    enum Bind {
        Text(Option<String>),
        Texts(Vec<String>),
        Json(String),
        Sequence(i64),
    }
    let mut binds: Vec<Bind> = Vec::new();
    let param = |binds: &mut Vec<Bind>, bind: Bind| {
        binds.push(bind);
        format!("${}", binds.len())
    };
    // One parameter per distinct position, so a query with one position
    // for every item sends the statement it always did.
    let mut positions: Vec<(i64, String)> = Vec::new();
    let mut selects = Vec::with_capacity(query.len());
    for (item, item_after) in query.iter().zip(afters) {
        let after = item_after.map(|after| {
            match positions.iter().find(|(position, _)| *position == after) {
                Some((_, placeholder)) => placeholder.clone(),
                None => {
                    let placeholder =
                        format!("{}::bigint", param(&mut binds, Bind::Sequence(after)));
                    positions.push((after, placeholder.clone()));
                    placeholder
                }
            }
        });
        let after_clause = |column: &str| match &after {
            Some(after) => format!(" AND {column} > {after}"),
            None => String::new(),
        };
        let latest = |binds: &mut Vec<Bind>, column: &str| match item.latest {
            Some(count) => format!(
                " ORDER BY {column} DESC LIMIT {}",
                param(binds, Bind::Sequence(i64::from(count)))
            ),
            None => String::new(),
        };
        let select = match (item.event_types.is_empty(), item.tags.split_first()) {
            (no_types, Some((first, rest))) => {
                let key = param(&mut binds, Bind::Text(Some(first.key.clone())));
                let value = match &first.value {
                    Some(value) => {
                        format!("= {}", param(&mut binds, Bind::Text(Some(value.clone()))))
                    }
                    None => "IS NULL".to_string(),
                };
                // Without types, every type the bounded context registers:
                // a superset of any event's, which `events.event_type_name`
                // references (docs/architecture.md §210).
                let types = if no_types {
                    all_event_types(&schema)
                } else {
                    format!(
                        "{}::text[]",
                        param(&mut binds, Bind::Texts(item.event_types.clone()))
                    )
                };
                let rest = if rest.is_empty() {
                    String::new()
                } else {
                    let tags = param(
                        &mut binds,
                        Bind::Json(
                            serde_json::to_string(rest).expect("Tag serialisation is infallible"),
                        ),
                    );
                    format!(
                        " AND EXISTS (SELECT 1 FROM {schema}.events r \
                         WHERE r.sequence = t.sequence AND r.tags @> {tags}::jsonb)"
                    )
                };
                let latest = item.latest.map(|count| {
                    (
                        TagOrder::Newest,
                        param(&mut binds, Bind::Sequence(i64::from(count))),
                    )
                });
                event_tags_select(
                    &schema,
                    &key,
                    &value,
                    &types,
                    &format!("{}{rest}", after_clause("t.sequence")),
                    latest,
                )
            }
            (false, None) => {
                let types = param(&mut binds, Bind::Texts(item.event_types.clone()));
                format!(
                    "(SELECT sequence FROM {schema}.events \
                     WHERE event_type_name = ANY({types}::text[]){}{})",
                    after_clause("sequence"),
                    latest(&mut binds, "sequence"),
                )
            }
            // Matches nothing (`QueryItem::matches`): refused by `build()`,
            // and derived only for an item no event can match.
            (true, None) => continue,
        };
        selects.push(select);
    }
    if selects.is_empty() {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         WHERE sequence = ANY(ARRAY({})) ORDER BY sequence",
        selects.join(" UNION ")
    );
    let mut statement = sqlx::query_as::<_, EventRowAnyType>(sqlx::AssertSqlSafe(sql));
    for bind in binds {
        statement = match bind {
            Bind::Text(text) => statement.bind(text),
            Bind::Texts(texts) => statement.bind(texts),
            Bind::Json(json) => statement.bind(json),
            Bind::Sequence(sequence) => statement.bind(sequence),
        };
    }
    let rows: Vec<EventRowAnyType> = statement.fetch_all(&mut *conn).await?;
    events_from_rows(conn, bc, rows, known_event_types).await
}

/// The snapshot state `item` reads, when it is a snapshot item whose tag
/// derived a value: the entry of `states` for its snapshot and value.
pub(super) fn snapshot_state_of<'s>(
    item: &QueryItem,
    states: &'s [crate::plugin::SnapshotState],
) -> Option<&'s crate::plugin::SnapshotState> {
    let (Some(name), [tag]) = (&item.snapshot, item.tags.as_slice()) else {
        return None;
    };
    let value = tag.value.as_deref()?;
    states
        .iter()
        .find(|state| state.snapshot == *name && state.tag_value == value)
}

/// Whether `event` belongs to `state`: it matches a snapshot item of `query`
/// for that snapshot and value.
fn event_belongs_to_state(
    query: &[QueryItem],
    state: &crate::plugin::SnapshotState,
    event: &Event,
) -> bool {
    query.iter().any(|item| {
        item.snapshot.as_deref() == Some(state.snapshot.as_str())
            && item.tags.first().and_then(|t| t.value.as_deref()) == Some(state.tag_value.as_str())
            && item.matches(event)
    })
}

/// Folds every event of `events` (in sequence order) past a state's
/// `folded_through` into each of `states` it belongs to - an event
/// matching a snapshot item of `query` for that snapshot and value - with
/// the snapshot's own fold for that value (docs/architecture.md §202). A
/// retraction and what it retracts are passed over (§235), and so is a doubt
/// (§236), its target staying in; a retraction whose target is already in a
/// state is [`states_holding_retracted_events`]' to deal with.
pub(super) fn fold_into_snapshot_states(
    snapshots: &dyn crate::plugin::SnapshotDispatcher,
    bounded_context: &str,
    query: &[QueryItem],
    states: &mut [crate::plugin::SnapshotState],
    events: &[Event],
) -> crate::error::Result<()> {
    let retracted: std::collections::HashSet<i64> = events
        .iter()
        .filter_map(crate::retraction::retracted_position)
        .collect();
    for event in events {
        let passed_over = retracted.contains(&event.sequence)
            || crate::retraction::retracted_position(event).is_some()
            || crate::retraction::is_doubt_event(event);
        for state in states.iter_mut() {
            if event.sequence <= state.folded_through
                || !event_belongs_to_state(query, state, event)
            {
                continue;
            }
            if !passed_over {
                state.state_json = snapshots
                    .fold_value(
                        bounded_context,
                        &state.snapshot,
                        &state.tag_value,
                        &state.state_json,
                        event,
                    )
                    .ok_or_else(|| unregistered_snapshot(&state.snapshot))??;
            }
            state.folded_through = event.sequence;
        }
    }
    Ok(())
}

/// What has been said about each of `sequences` (docs/architecture.md §235,
/// §236): an entry for those something was said about. Goes by the tags the
/// store adds, so finding a retraction pages past its target costs the same.
/// Not under the lock, so not something to decide on.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn event_stances(
    pool: &Pool,
    bounded_context: &str,
    sequences: &[i64],
) -> crate::error::Result<std::collections::HashMap<i64, crate::retraction::EventStance>> {
    let schema = schema_ident(bounded_context);
    let positions: Vec<String> = sequences.iter().map(i64::to_string).collect();
    let rows: Vec<(String, i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT tag_value, sequence, event_type_name FROM {schema}.event_tags \
         WHERE tag_key = ANY($1) AND tag_value = ANY($2) ORDER BY sequence"
    )))
    .bind([
        crate::retraction::RETRACTS_TAG_KEY,
        crate::retraction::DOUBTS_TAG_KEY,
    ])
    .bind(&positions)
    .fetch_all(pool)
    .await?;
    let mut stances = std::collections::HashMap::new();
    for (position, sequence, event_type) in rows {
        if let Ok(position) = position.parse::<i64>() {
            stances
                .entry(position)
                .or_insert_with(crate::retraction::EventStance::default)
                .apply(&event_type, sequence);
        }
    }
    Ok(stances)
}

/// The indexes of `states` a retraction among `events` makes wrong: it is
/// past the state's position and retracts something at or before it, so the
/// event's contribution is already in the state and can't be folded out
/// (docs/architecture.md §235). Such a state starts over from its value's whole
/// history; see [`reset_snapshot_state`].
pub(super) fn states_holding_retracted_events(
    query: &[QueryItem],
    states: &[crate::plugin::SnapshotState],
    events: &[Event],
) -> Vec<usize> {
    states
        .iter()
        .enumerate()
        .filter(|(_, state)| {
            events.iter().any(|event| {
                event.sequence > state.folded_through
                    && crate::retraction::retracted_position(event)
                        .is_some_and(|target| target <= state.folded_through)
                    && event_belongs_to_state(query, state, event)
            })
        })
        .map(|(index, _)| index)
        .collect()
}

/// Puts `state` back to the snapshot's default state, before every event.
pub(super) fn reset_snapshot_state(
    snapshots: &dyn crate::plugin::SnapshotDispatcher,
    bounded_context: &str,
    state: &mut crate::plugin::SnapshotState,
) -> crate::error::Result<()> {
    state.state_json = snapshots
        .default_state(bounded_context, &state.snapshot)
        .ok_or_else(|| unregistered_snapshot(&state.snapshot))?;
    state.folded_through = -1;
    Ok(())
}

pub(super) fn unregistered_snapshot(name: &str) -> crate::error::Error {
    crate::error::Error::configuration(format!(
        "a consistency_query() item names snapshot {name}, which this instance doesn't register"
    ))
}

/// A command's read before the lock when its query has snapshot items
/// (docs/architecture.md §202). The stored rows of every snapshot and tag
/// value the items derive come first, in one statement; a row that is
/// missing, or at another `Snapshot::VERSION`, is the default state at -1.
/// Then one read of every item: a snapshot item past its row's
/// `as_of_sequence`, the others their whole history - from the event
/// cache when its window holds everything after the lowest of those
/// positions, else from Postgres. The events past each state's position
/// are folded into it. Returns the events read, the position the read is
/// complete through, and the states.
pub(super) async fn read_query_with_snapshots(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    bounded_context: &str,
    query: &[QueryItem],
) -> crate::error::Result<(Vec<Event>, i64, Vec<crate::plugin::SnapshotState>)> {
    let Some(snapshots) = dispatcher.snapshots() else {
        let name = query
            .iter()
            .find_map(|item| item.snapshot.as_deref())
            .unwrap_or_default();
        return Err(unregistered_snapshot(name));
    };
    let mut wanted: Vec<(&str, &str, &str, u64)> = Vec::new();
    for item in query {
        let (Some(name), [tag]) = (&item.snapshot, item.tags.as_slice()) else {
            continue;
        };
        let Some(value) = tag.value.as_deref() else {
            continue;
        };
        if wanted.iter().any(|w| w.0 == name && w.2 == value) {
            continue;
        }
        let version = snapshots
            .version(bounded_context, name)
            .ok_or_else(|| unregistered_snapshot(name))?;
        wanted.push((name, tag.key.as_str(), value, version));
    }
    let rows = get_current_snapshot_rows(pool, bounded_context, &wanted).await?;
    let mut states = Vec::with_capacity(wanted.len());
    for ((name, _, value, _), row) in wanted.iter().zip(rows) {
        let (folded_through, state_json) = match row {
            Some(row) => row,
            None => (
                -1,
                snapshots
                    .default_state(bounded_context, name)
                    .ok_or_else(|| unregistered_snapshot(name))?,
            ),
        };
        states.push(crate::plugin::SnapshotState {
            snapshot: name.to_string(),
            tag_value: value.to_string(),
            state_json,
            folded_through,
        });
    }

    // A stored row can hold an event retracted since it was folded. Each pass
    // resets at least one state, and a state at -1 can't hold anything, so this
    // ends (docs/architecture.md §235).
    let (events, covered_through) = loop {
        let (events, covered_through) =
            read_query_past_snapshot_states(pool, cache, bounded_context, query, &states).await?;
        let stale = states_holding_retracted_events(query, &states, &events);
        if stale.is_empty() {
            break (events, covered_through);
        }
        for index in stale {
            if let Some(state) = states.get_mut(index) {
                reset_snapshot_state(snapshots, bounded_context, state)?;
            }
        }
    };
    fold_into_snapshot_states(snapshots, bounded_context, query, &mut states, &events)?;
    Ok((events, covered_through, states))
}

/// One read of every item of `query`: a snapshot item past its state's
/// position, the others their whole history - from the event cache when its
/// window holds everything after the lowest of those positions, else from
/// Postgres. Returns the events and the position the read is complete through.
async fn read_query_past_snapshot_states(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    query: &[QueryItem],
    states: &[crate::plugin::SnapshotState],
) -> crate::error::Result<(Vec<Event>, i64)> {
    let afters: Vec<Option<i64>> = query
        .iter()
        .map(|item| snapshot_state_of(item, states).map(|state| state.folded_through))
        .collect();
    let lowest = afters.iter().map(|a| a.unwrap_or(-1)).min().unwrap_or(-1);
    let past_its_item = |event: &Event| {
        query
            .iter()
            .zip(&afters)
            .any(|(item, after)| item.matches(event) && event.sequence > after.unwrap_or(-1))
    };
    match cache
        .try_events_matching_query(pool, bounded_context, query, lowest)
        .await?
    {
        Some((events, covered_through)) => Ok((
            events.into_iter().filter(|e| past_its_item(e)).collect(),
            covered_through,
        )),
        None => {
            let (covered_through, bc) = futures_util::try_join!(
                latest_sequence(pool, bounded_context),
                require_bounded_context(pool, bounded_context),
            )?;
            let events = list_events_matching_items_with_bc(
                &mut *pool.acquire().await?,
                &bc,
                query,
                &afters,
                None,
            )
            .await?;
            Ok((events, covered_through.unwrap_or(-1)))
        }
    }
}

/// `list_events_matching_query_with_bc` for a command's read before the
/// lock: cache first, then Postgres, returning the position the read is
/// complete through, as [`list_events_for_bounded_context_matching_tags_cached`]
/// does. The cache serves it only from a window that holds everything
/// after `after_sequence`, and selects the last matches of items with
/// `latest` from it the
/// same way.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_matching_query_cached(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    query: &[QueryItem],
    after_sequence: Option<i64>,
) -> crate::error::Result<(Vec<Event>, i64)> {
    match cache
        .try_events_matching_query(pool, bounded_context, query, after_sequence.unwrap_or(-1))
        .await?
    {
        Some((events, covered_through)) => Ok((
            crate::event_store::select_matching_events(query, &events),
            covered_through,
        )),
        None => {
            let (covered_through, bc) = futures_util::try_join!(
                latest_sequence(pool, bounded_context),
                require_bounded_context(pool, bounded_context),
            )?;
            let events = list_events_matching_query_with_bc(
                &mut *pool.acquire().await?,
                &bc,
                query,
                after_sequence,
                None,
            )
            .await?;
            Ok((events, covered_through.unwrap_or(-1)))
        }
    }
}

/// Turns `events` rows of any type from `bc` into [`Event`]s on `conn`,
/// batching the lookups they need: one query for every event type
/// `known_event_types` doesn't already answer, and one (plus one for
/// their command types) for every originating command a
/// `command_triggered` row names. Every read of many events goes through
/// it (docs/architecture.md §125) - they used to resolve each row's
/// originating command one at a time, several queries each. The DCB
/// delta query and a new sync projection's locked tail read run it on the
/// transaction holding the bounded context's `sequence` lock
/// (docs/architecture.md §117); the others on a connection of their own.
pub(super) async fn events_from_rows(
    conn: &mut sqlx::PgConnection,
    bc: &BoundedContext,
    rows: Vec<EventRowAnyType>,
    known_event_types: Option<&std::collections::HashMap<String, EventType>>,
) -> crate::error::Result<Vec<Event>> {
    let row_count = rows.len();
    let row_loop_started = std::time::Instant::now();

    // Every distinct event type this result set needs that
    // `known_event_types` (a caller's already-warmed map, e.g.
    // `decide_command_in_tx`'s own) doesn't already answer, batched into
    // one `WHERE name = ANY($1)` round trip - `get_commands_by_ids_with_bc`'s
    // own precedent below, same reasoning: a batched query, not a
    // concurrent burst of small ones (a first, concurrent attempt at
    // fixing this whole row-processing loop measured worse overall, by
    // starving the next batch's own lock acquisition - see this
    // function's own doc comment for the full story).
    let mut event_types: std::collections::HashMap<String, EventType> =
        std::collections::HashMap::new();
    let mut missing_type_names: Vec<&str> = Vec::new();
    for row in &rows {
        if event_types.contains_key(&row.event_type_name) {
            continue;
        }
        match known_event_types.and_then(|m| m.get(&row.event_type_name)) {
            Some(et) => {
                event_types.insert(row.event_type_name.clone(), et.clone());
            }
            None => {
                if !missing_type_names.contains(&row.event_type_name.as_str()) {
                    missing_type_names.push(&row.event_type_name);
                }
            }
        }
    }
    let event_type_lookup_started = std::time::Instant::now();
    event_types
        .extend(get_event_types_by_names_with_bc(&mut *conn, bc, &missing_type_names).await?);
    let event_type_lookup_elapsed = event_type_lookup_started.elapsed();

    // The real cost this whole investigation found (docs/architecture.md
    // §61's round four): a `command_triggered` row's own origin needs the
    // *entire* originating `Command` (`get_command_by_id`, itself several
    // round trips deep - `get_command_type` → `get_bounded_context` →
    // `get_role`). A first attempt fetched these concurrently, one
    // `event_origin_from_row` future per row - measured *worse*, not
    // better: bursting many simultaneous connection requests out of a
    // shared, already-contended pool starved the *next* batch's own lock
    // acquisition (`command batch leader lock wait` climbed to ~1.7s mean
    // in that measurement). This instead batches the distinct
    // `origin_command_id`s this result set actually needs into one
    // `WHERE id = ANY($1)` query (`get_commands_by_ids_with_bc`, itself
    // internally batching the distinct `command_type_name`s the same
    // way) - one or two round trips total for the whole row set, not one
    // per row, concurrent or not.
    let origin_lookup_started = std::time::Instant::now();
    let mut command_ids: Vec<i64> = Vec::new();
    for row in &rows {
        if row.origin_kind == "command_triggered" {
            if let Some(id) = row.origin_command_id {
                if !command_ids.contains(&id) {
                    command_ids.push(id);
                }
            }
        }
    }
    let commands_by_id = get_commands_by_ids_with_bc(conn, bc, &command_ids).await?;
    let mut origins = Vec::with_capacity(rows.len());
    for row in &rows {
        origins.push(match row.origin_kind.as_str() {
            "external_triggered" => EventOrigin::ExternalTriggered {
                source_content: row
                    .origin_source_content
                    .clone()
                    .expect("external_triggered event row without origin_source_content"),
                source_context: row.origin_source_context.clone(),
            },
            "directly_created" => EventOrigin::DirectlyCreated,
            "system_triggered" => EventOrigin::SystemTriggered,
            "skilj_recorded" => EventOrigin::SkiljRecorded,
            "command_triggered" => {
                let command_id = row
                    .origin_command_id
                    .expect("command_triggered event row without origin_command_id");
                // `commands_by_id` came from `get_commands_by_ids_with_bc`
                // (see its own sibling note in `get_commands_by_ids_with_bc`
                // above) - a `bc`-in-hand, no-recheck batch query, so a
                // `hard_delete_bounded_context` race surfaces as a real
                // `Err` before this lookup, not a silent miss. A genuine
                // miss here is a real bug (an `events` row outliving the
                // `commands` row it names), not a race.
                let command = commands_by_id
                    .get(&command_id)
                    .expect("events row references a commands row that no longer exists");
                EventOrigin::CommandTriggered {
                    command: Box::new(command.clone()),
                }
            }
            other => EventOrigin::Unknown {
                kind: other.to_string(),
            },
        });
    }
    let origin_lookup_elapsed = origin_lookup_started.elapsed();

    let mut events = Vec::with_capacity(rows.len());
    for (row, origin) in rows.into_iter().zip(origins) {
        events.push(Event {
            bounded_context: bc.clone(),
            event_type: event_types[&row.event_type_name].clone(),
            payload: row.payload,
            metadata: Metadata {
                r#type: row.metadata_type,
                version: row.metadata_version,
                client_id: row.metadata_client_id,
                created_at: row.metadata_created_at,
                correlation_id: row.metadata_correlation_id,
                causation_id: row.metadata_causation_id,
            },
            sequence: row.sequence,
            tags: row.tags.0,
            encryption_keys: Vec::new(),
            origin,
        });
    }
    tracing::debug!(
        row_count,
        row_loop_us = row_loop_started.elapsed().as_micros(),
        event_type_lookup_us = event_type_lookup_elapsed.as_micros(),
        origin_lookup_us = origin_lookup_elapsed.as_micros(),
        "event rows resolved"
    );
    Ok(events)
}

/// One `Event` by its own `sequence` - `InspectEvent`'s own lookup key
/// (`context event: Event`), added propagating `skilj-graphql`'s
/// `EventQuery` resolver (Phase 3). `None` when no event in this bounded
/// context has that sequence.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_event_by_sequence(
    pool: &Pool,
    bounded_context: &str,
    sequence: i64,
) -> crate::error::Result<Option<Event>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };

    let schema = schema_ident(bounded_context);
    let Some(row): Option<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events WHERE sequence = $1"
    )))
    .bind(sequence)
    .fetch_optional(pool)
    .await?
    else {
        return Ok(None);
    };

    let et = require_event_type(pool, bounded_context, &row.event_type_name).await?;
    let origin = event_origin_from_row(
        pool,
        bounded_context,
        &row.origin_kind,
        row.origin_source_content,
        row.origin_source_context,
        row.origin_command_id,
    )
    .await?;
    Ok(Some(Event {
        bounded_context: bc,
        event_type: et,
        payload: row.payload,
        metadata: Metadata {
            r#type: row.metadata_type,
            version: row.metadata_version,
            client_id: row.metadata_client_id,
            created_at: row.metadata_created_at,
            correlation_id: row.metadata_correlation_id,
            causation_id: row.metadata_causation_id,
        },
        sequence: row.sequence,
        tags: row.tags.0,
        encryption_keys: Vec::new(),
        origin,
    }))
}

/// The most recent `limit` events of a whole bounded context, ascending
/// by `sequence` - `event_cache::EventCache::warm`'s own fetch, and the
/// only place this module issues a `DESC ... LIMIT` query at all (every
/// other listing function loads a range in ascending order directly).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_recent_events_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
    limit: usize,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, \
         metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, origin_kind, origin_source_content, \
         origin_source_context, origin_command_id FROM {schema}.events \
         ORDER BY sequence DESC LIMIT $1"
    )))
    .bind(limit as i64)
    .fetch_all(pool)
    .await?;

    let mut events = events_from_rows(&mut *pool.acquire().await?, &bc, rows, None).await?;
    // The query above fetched newest-first to make LIMIT cheap - reverse
    // back to sequence order.
    events.reverse();
    Ok(events)
}

/// The type-scoped, `after_sequence`-bounded twin of `list_events` above -
/// `list_events_cached`'s own fallback path, so falling back to Postgres
/// doesn't itself load more than the request actually needs.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_from(
    pool: &Pool,
    bounded_context: &str,
    event_type_name: &str,
    after_sequence: i64,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;
    let et = require_event_type(pool, bounded_context, event_type_name).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, metadata_client_id, \
         metadata_created_at, metadata_correlation_id, metadata_causation_id, tags, \
         origin_kind, origin_source_content, origin_source_context, \
         origin_command_id FROM {schema}.events WHERE event_type_name = $1 AND sequence > $2 \
         ORDER BY sequence"
    )))
    .bind(event_type_name)
    .bind(after_sequence)
    .fetch_all(pool)
    .await?;

    let known = std::collections::HashMap::from([(et.name.clone(), et)]);
    events_from_rows(&mut *pool.acquire().await?, &bc, rows, Some(&known)).await
}

/// At most one event of each of `event_types`, in `bounded_context` -
/// what `projections::register_projection` needs for its only use of
/// history: whether *any* event of a consumed type exists yet (so a new
/// sync projection needs a history fold). One witness per type answers
/// that exactly as the full history would, without loading it - which
/// the `registerProjection` mutation, every projection's startup
/// reconciliation and template resync all used to do.
pub async fn witness_events_of_types(
    pool: &Pool,
    bounded_context: &str,
    event_types: &[EventType],
) -> crate::error::Result<Vec<Event>> {
    let mut witnesses = Vec::new();
    for event_type in event_types {
        witnesses.extend(
            list_events_from_limited(pool, bounded_context, &event_type.name, -1, 1).await?,
        );
    }
    Ok(witnesses)
}

/// How many events one background catch-up tick loads at most - for a
/// `CrossContextRoute`/`ScheduleDeadline`/`CancelDeadline` source, an
/// async projection or rebuild (`catch_up_bounded_context`) or a snapshot
/// (`catch_up_snapshots`). The rest wait for the next tick, which picks
/// up from the persisted cursor/`caught_up_to`/snapshot progress.
pub const MAX_EVENTS_PER_CATCH_UP_TICK: i64 = 1000;

/// [`list_events_cached`], at most `limit` events.
pub(super) async fn list_events_cached_limited(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    event_type_name: &str,
    after_sequence: i64,
    limit: i64,
) -> crate::error::Result<Vec<Event>> {
    match cache
        .try_events_after(pool, bounded_context, after_sequence)
        .await?
    {
        Some(events) => Ok(events
            .into_iter()
            .filter(|e| e.event_type.name == event_type_name)
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .collect()),
        None => {
            list_events_from_limited(
                pool,
                bounded_context,
                event_type_name,
                after_sequence,
                limit,
            )
            .await
        }
    }
}

/// The highest sequence of `event_type_name` in `bounded_context`
/// (only among events created at or before `created_at_or_before`, when
/// given), or `-1` - a new catch-up cursor's `Latest`/`AtTime` seed, as
/// one aggregate query.
pub(super) async fn highest_sequence_of_type(
    pool: &Pool,
    bounded_context: &str,
    event_type_name: &str,
    created_at_or_before: Option<DateTime<Utc>>,
) -> crate::error::Result<i64> {
    let schema = schema_ident(bounded_context);
    let (max,): (Option<i64>,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT MAX(sequence) FROM {schema}.events WHERE event_type_name = $1 \
         AND ($2::timestamptz IS NULL OR metadata_created_at <= $2)"
    )))
    .bind(event_type_name)
    .bind(created_at_or_before)
    .fetch_one(pool)
    .await?;
    Ok(max.unwrap_or(-1))
}

/// [`list_events_from`] capped at `limit` events - [`for_each_event_chunk`]'s
/// Postgres fallback for a type-scoped read.
pub async fn list_events_from_limited(
    pool: &Pool,
    bounded_context: &str,
    event_type_name: &str,
    after_sequence: i64,
    limit: i64,
) -> crate::error::Result<Vec<Event>> {
    let bc = require_bounded_context(pool, bounded_context).await?;
    let et = require_event_type(pool, bounded_context, event_type_name).await?;

    let schema = schema_ident(bounded_context);
    let rows: Vec<EventRowAnyType> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_type_name, sequence, payload, metadata_type, metadata_version, metadata_client_id, \
         metadata_created_at, metadata_correlation_id, metadata_causation_id, tags, \
         origin_kind, origin_source_content, origin_source_context, \
         origin_command_id FROM {schema}.events WHERE event_type_name = $1 AND sequence > $2 \
         ORDER BY sequence LIMIT $3"
    )))
    .bind(event_type_name)
    .bind(after_sequence)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let known = std::collections::HashMap::from([(et.name.clone(), et)]);
    events_from_rows(&mut *pool.acquire().await?, &bc, rows, Some(&known)).await
}

/// `FetchEvents`/`ConsumeEvents`'s own real read path - see
/// `crate::event_cache`'s own module doc comment for the full design.
/// Tries the cache first; `list_events_from` above is the fallback, so a
/// coverage miss still only loads what the request actually needs, not
/// the whole type's history.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_cached(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    event_type_name: &str,
    after_sequence: i64,
) -> crate::error::Result<Vec<Event>> {
    match cache
        .try_events_after(pool, bounded_context, after_sequence)
        .await?
    {
        Some(events) => Ok(events
            .into_iter()
            .filter(|e| e.event_type.name == event_type_name)
            .collect()),
        None => list_events_from(pool, bounded_context, event_type_name, after_sequence).await,
    }
}

/// Walks a bounded context's events after `after_sequence`, in sequence
/// order, `chunk_size` at a time - only `event_type_name`'s when given -
/// handing each chunk to `visit` until it returns `false` or
/// history runs out. Cache-first per chunk, like `list_events_cached`;
/// the Postgres fallback is `LIMIT`ed, so a long history is never loaded
/// whole. This is how the `config.max_events_per_read`-bounded reads
/// (`FetchEvents`, `ConsumeEvents`, `QueryEvents`) find their page without
/// holding more than a chunk plus that page in memory - see
/// [`collect_event_page`].
pub async fn for_each_event_chunk(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    event_type_name: Option<&str>,
    mut after_sequence: i64,
    chunk_size: usize,
    mut visit: impl FnMut(&[Event]) -> crate::error::Result<bool>,
) -> crate::error::Result<()> {
    let chunk_size = chunk_size.max(1);
    let limit = i64::try_from(chunk_size).unwrap_or(i64::MAX);
    loop {
        let chunk = match cache
            .try_events_after(pool, bounded_context, after_sequence)
            .await?
        {
            Some(events) => {
                // The window is complete from `after_sequence` on, so a
                // short type-filtered result here is the real end.
                let chunk: Vec<Event> = events
                    .into_iter()
                    .filter(|e| event_type_name.is_none_or(|name| e.event_type.name == name))
                    .take(chunk_size)
                    .collect();
                let exhausted = chunk.len() < chunk_size;
                (chunk, exhausted)
            }
            None => {
                let chunk = match event_type_name {
                    Some(name) => {
                        list_events_from_limited(pool, bounded_context, name, after_sequence, limit)
                            .await?
                    }
                    None => {
                        list_events_for_bounded_context_from_limited(
                            pool,
                            bounded_context,
                            after_sequence,
                            limit,
                        )
                        .await?
                    }
                };
                let exhausted = chunk.len() < chunk_size;
                (chunk, exhausted)
            }
        };
        let (chunk, exhausted) = chunk;
        if let Some(last) = chunk.last() {
            after_sequence = last.sequence;
        }
        if !visit(&chunk)? || exhausted {
            return Ok(());
        }
    }
}

/// One bounded page: walks [`for_each_event_chunk`], `max_events` at a
/// time (so memory stays at about a chunk plus the page), and asks `select`
/// for the events to serve out of each chunk (given how many more fit),
/// stopping once `max_events` are selected or history ends. `select` is
/// the rule's own pure function (`event_store::fetch_events_page` and
/// friends), so the page is exactly the first `max_events` it would have
/// served over the whole history. It runs at least once, even on an
/// empty history, so its own validation errors still surface. `up_to` is an
/// as-of read (docs/architecture.md §237): nothing past that sequence is
/// served, and the walk stops there rather than going on to the end.
#[allow(clippy::too_many_arguments)]
pub async fn collect_event_page(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    event_type_name: Option<&str>,
    after_sequence: i64,
    up_to: Option<i64>,
    max_events: usize,
    select: impl FnMut(&[Event], usize) -> crate::error::Result<Vec<Event>>,
) -> crate::error::Result<Vec<Event>> {
    Ok(collect_scanned_event_page(
        pool,
        cache,
        bounded_context,
        event_type_name,
        after_sequence,
        up_to,
        max_events,
        select,
    )
    .await?
    .events)
}

/// A page from [`collect_scanned_event_page`]: the events served, and the
/// highest sequence the walk examined (`None` if it examined nothing).
#[derive(Debug, Clone, PartialEq)]
pub struct ScannedEventPage {
    pub events: Vec<Event>,
    pub scanned_through: Option<i64>,
}

/// The part of `chunk`, which is in sequence order, at or below `up_to`.
fn chunk_up_to(chunk: &[Event], up_to: Option<i64>) -> &[Event] {
    match up_to {
        Some(up_to) => {
            let end = chunk.partition_point(|event| event.sequence <= up_to);
            chunk.get(..end).unwrap_or_default()
        }
        None => chunk,
    }
}

/// [`collect_event_page`], also reporting how far it walked. When the
/// page came back short of `max_events`, the walk reached the end of
/// history, and `select` passed over every examined event it didn't
/// serve - so a reader's cursor can move to `scanned_through` rather than
/// the last event served, and the next poll doesn't walk the same
/// non-matching events again (docs/architecture.md §112). Safe because
/// events become visible in sequence order: `next_sequence`'s row lock
/// is held until its transaction commits, so nothing can later appear
/// below a sequence already read.
#[allow(clippy::too_many_arguments)]
pub async fn collect_scanned_event_page(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    event_type_name: Option<&str>,
    after_sequence: i64,
    up_to: Option<i64>,
    max_events: usize,
    mut select: impl FnMut(&[Event], usize) -> crate::error::Result<Vec<Event>>,
) -> crate::error::Result<ScannedEventPage> {
    let mut page = Vec::new();
    let mut scanned_through = None;
    for_each_event_chunk(
        pool,
        cache,
        bounded_context,
        event_type_name,
        after_sequence,
        max_events,
        |chunk| {
            let wanted = chunk_up_to(chunk, up_to);
            if let Some(last) = wanted.last() {
                scanned_through = Some(last.sequence);
            }
            page.extend(select(wanted, max_events - page.len())?);
            Ok(page.len() < max_events && wanted.len() == chunk.len())
        },
    )
    .await?;
    Ok(ScannedEventPage {
        events: page,
        scanned_through,
    })
}

/// The tag-filtered counterpart of [`for_each_event_chunk`]
/// (docs/architecture.md §108): walks the events carrying any of `tags`,
/// after `after_sequence`, in sequence order, `chunk_size` at a time
/// through the tag index, calling `f` per chunk until it returns `false`
/// or the matches run out. So a tag shared by a large part of a bounded
/// context's history is never loaded whole. `f` runs at least once, even
/// with no matches, so the caller's own validation still happens.
pub async fn for_each_tagged_event_chunk(
    pool: &Pool,
    bounded_context: &str,
    tags: &[Tag],
    after_sequence: i64,
    chunk_size: usize,
    mut f: impl FnMut(&[Event]) -> crate::error::Result<bool>,
) -> crate::error::Result<()> {
    let bc = require_bounded_context(pool, bounded_context).await?;
    let chunk_size = chunk_size.max(1);
    let limit = i64::try_from(chunk_size).unwrap_or(i64::MAX);
    let mut after = after_sequence;
    loop {
        let chunk = list_events_for_bounded_context_matching_tags_with_bc(
            &mut *pool.acquire().await?,
            &bc,
            tags,
            Some(after),
            None,
            Some(limit),
        )
        .await?;
        if !f(&chunk)? || chunk.len() < chunk_size {
            return Ok(());
        }
        after = chunk.last().map_or(after, |e| e.sequence);
    }
}

/// [`collect_event_page`] over [`for_each_tagged_event_chunk`]: the first
/// `max_events` events `select` serves out of those carrying any of
/// `tags` after `after_sequence`, read a chunk at a time. `up_to` as there.
#[allow(clippy::too_many_arguments)]
pub async fn collect_tagged_event_page(
    pool: &Pool,
    bounded_context: &str,
    tags: &[Tag],
    after_sequence: i64,
    up_to: Option<i64>,
    max_events: usize,
    mut select: impl FnMut(&[Event], usize) -> crate::error::Result<Vec<Event>>,
) -> crate::error::Result<Vec<Event>> {
    let mut page = Vec::new();
    for_each_tagged_event_chunk(
        pool,
        bounded_context,
        tags,
        after_sequence,
        max_events,
        |chunk| {
            let wanted = chunk_up_to(chunk, up_to);
            page.extend(select(wanted, max_events - page.len())?);
            Ok(page.len() < max_events && wanted.len() == chunk.len())
        },
    )
    .await?;
    Ok(page)
}

/// `QueryEvents`/`CountEvents`'s own real read path when no `tags`
/// filter is given - see `crate::event_cache`'s own module doc comment
/// for the full design. `after_sequence: -1` asks for full history
/// (`CountEvents`, which has no `after_sequence` of its own);
/// `list_events_for_bounded_context`/`list_events_for_bounded_context_from`
/// (both already existed, reused unchanged) are the fallback for either
/// case respectively. `ProcessCommand`'s own DCB pre-check, and
/// `QueryEvents`/`CountEvents` when a `tags` filter *is* given, use
/// `list_events_for_bounded_context_matching_tags_cached` below instead -
/// [docs/architecture.md §19](../../../docs/architecture.md#optional-snapshotting-matching-events)'s "Problem 1" fix, avoiding exactly the
/// full-bounded-context fetch this function's own fallback still does.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context_cached(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    after_sequence: i64,
) -> crate::error::Result<Vec<Event>> {
    match cache
        .try_events_after(pool, bounded_context, after_sequence)
        .await?
    {
        Some(events) => Ok(events),
        None if after_sequence < 0 => list_events_for_bounded_context(pool, bounded_context).await,
        None => list_events_for_bounded_context_from(pool, bounded_context, after_sequence).await,
    }
}

/// `ProcessCommand`'s own DCB pre-check, and `QueryEvents`/`CountEvents`
/// when a `tags` filter is supplied - the cached counterpart to
/// `list_events_for_bounded_context_matching_tags` above, same
/// cache-first/Postgres-fallback shape `list_events_for_bounded_context_cached`
/// already has, via `EventCache::try_events_matching_tags` instead of
/// `try_events_after`. Already tag-scoped either way - see that
/// function's own doc comment.
///
/// Also returns the position the read is complete through - every
/// matching event at or below it is in the result - which is where a
/// command's re-check under the lock starts (docs/architecture.md §178).
/// From the cache, the window's highest sequence. From Postgres, the
/// latest sequence read *before* the tag query: events commit in sequence
/// order under the bounded context's lock, so everything up to it is
/// visible to the query that follows. A newer event the query also
/// happened to see is above the position; the re-check starts above the
/// highest event the read holds anyway, so it doesn't come back.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_events_for_bounded_context_matching_tags_cached(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    tags: &[Tag],
    after_sequence: Option<i64>,
) -> crate::error::Result<(Vec<Event>, i64)> {
    match cache
        .try_events_matching_tags(pool, bounded_context, tags, after_sequence.unwrap_or(-1))
        .await?
    {
        Some(served) => Ok(served),
        None => {
            // Both before the tag query, alongside each other: the latest
            // sequence used to cost a round trip of its own ahead of the
            // bounded context's lookup, on every miss (docs/architecture.md
            // §195).
            let (covered_through, bc) = futures_util::try_join!(
                latest_sequence(pool, bounded_context),
                require_bounded_context(pool, bounded_context),
            )?;
            let events = list_events_for_bounded_context_matching_tags_with_bc(
                &mut *pool.acquire().await?,
                &bc,
                tags,
                after_sequence,
                None,
                None,
            )
            .await?;
            Ok((events, covered_through.unwrap_or(-1)))
        }
    }
}

/// `InspectEvent`'s own real read path - see `crate::event_cache`'s own
/// module doc comment for the full design.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_event_by_sequence_cached(
    pool: &Pool,
    cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    sequence: i64,
) -> crate::error::Result<Option<Event>> {
    match cache
        .try_event_by_sequence(pool, bounded_context, sequence)
        .await?
    {
        Some(event) => Ok(Some(event)),
        None => get_event_by_sequence(pool, bounded_context, sequence).await,
    }
}

/// `command_id` must be `Some` exactly when `event.origin` is
/// `CommandTriggered`, and `None` otherwise - the caller's own
/// `insert_command(pool, &result.command)` (returning the new row's id)
/// runs first for a `ProcessCommandResult`, then this is called once per
/// produced `Event` with that same id.
#[tracing::instrument(skip_all)]
pub async fn insert_event<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    event: &Event,
    command_id: Option<i64>,
) -> crate::error::Result<()> {
    let (origin_kind, source_content, source_context) = match &event.origin {
        EventOrigin::ExternalTriggered {
            source_content,
            source_context,
        } => {
            debug_assert!(command_id.is_none());
            (
                "external_triggered",
                Some(source_content.clone()),
                source_context.clone(),
            )
        }
        EventOrigin::DirectlyCreated => {
            debug_assert!(command_id.is_none());
            ("directly_created", None, None)
        }
        EventOrigin::SystemTriggered => {
            debug_assert!(command_id.is_none());
            ("system_triggered", None, None)
        }
        EventOrigin::SkiljRecorded => {
            debug_assert!(command_id.is_none());
            ("skilj_recorded", None, None)
        }
        EventOrigin::CommandTriggered { .. } => {
            debug_assert!(
                command_id.is_some(),
                "insert_event: a CommandTriggered origin needs command_id - call \
                 insert_command(pool, &result.command) first and pass its returned id"
            );
            ("command_triggered", None, None)
        }
        // Only ever read back from a newer version's row; this build has
        // no way to write the per-kind columns it would need.
        EventOrigin::Unknown { .. } => {
            return Err(crate::error::Error::Unsupported {
                what: "EventOrigin",
            })
        }
    };

    let schema = schema_ident(&event.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.events (sequence, event_type_name, payload, metadata_type, \
         metadata_version, metadata_client_id, metadata_created_at, metadata_correlation_id, \
         metadata_causation_id, tags, \
         origin_kind, origin_source_content, origin_source_context, origin_command_id) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)"
    )))
    .bind(event.sequence)
    .bind(&event.event_type.name)
    .bind(&event.payload)
    .bind(&event.metadata.r#type)
    .bind(event.metadata.version)
    .bind(&event.metadata.client_id)
    .bind(event.metadata.created_at)
    .bind(&event.metadata.correlation_id)
    .bind(&event.metadata.causation_id)
    .bind(Json(&event.tags))
    .bind(origin_kind)
    .bind(source_content)
    .bind(source_context)
    .bind(command_id)
    .execute(executor)
    .await?;
    Ok(())
}

/// The real, transactional half of [§8](../../../docs/architecture.md#open-for-a-future-pass) item 6: inserts `event` and, in
/// the *same* transaction, folds it into every `sync = true` `Projection`
/// in its bounded context via `dispatcher` - "commits or fails with the
/// write" (the note above the rules), the whole reason sync projections
/// exist as a distinct, deliberately-friction-laden opt-in over the
/// async default. Every event-creation call site (`CreateExternalEvent`/
/// `CreateDirectEvent`'s REST handlers, `ProcessCommand`'s REST and
/// GraphQL handlers alike) calls this instead of the plain `insert_event`
/// above - which stays exactly as it is, still the right shape for a
/// bounded context with no sync projections at all (every event-writing
/// test fixture so far) and for `insert_event`'s own use inside this
/// function.
///
/// `SELECT ... FOR UPDATE` on each sync projection's own `projection_state`
/// row is what makes this safe under concurrent writers to the same
/// projection - the identical row-lock-based serialisation
/// `next_sequence` already relies on for sequence assignment, just one
/// row per sync projection instead of one row per bounded context.
/// `caught_up_to` advances for every sync projection in the context
/// regardless of whether it actually consumes this event's type (the
/// note above the rules: "advances that projection's own caught_up_to...
/// either way") - `dispatcher.project()` itself is what decides whether
/// the *state* changes; this function always bumps the position.
#[tracing::instrument(skip_all)]
pub async fn insert_event_and_update_sync_projections(
    pool: &Pool,
    event: &Event,
    command_id: Option<i64>,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    encryption_key_ids: &[i64],
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
) -> crate::error::Result<()> {
    let mut tx = pool.begin().await?;
    let sync_projections = sync_projection_names(&mut tx, &event.bounded_context.name).await?;
    insert_event_and_update_sync_projections_in_tx(
        &mut tx,
        event,
        command_id,
        dispatcher,
        encryption_key_ids,
        &sync_projections,
    )
    .await?;
    tx.commit().await?;

    // EventSubscription's own real-time delivery - after the commit, not
    // before: a subscriber must never see an event that could still have
    // rolled back (see EventBroadcaster's own doc comment for why this
    // is the one choke point every event-creation call site already
    // shares).
    broadcaster.publish(event);
    record_event_appended(event);
    notify_event_appended(pool, event, broadcaster.instance_id()).await;
    event_cache.append(event).await;

    Ok(())
}

/// The transactional core of `insert_event_and_update_sync_projections`
/// above, split out so `next_sequence`'s own lock, this event's insert,
/// and (for `ProcessCommand`) the triggering `Command`'s own insert can
/// all share one already-open transaction instead of each getting their
/// own - see the note above the rules in specs/skilj.allium: "a single
/// row per bounded context... is locked... as part of the same
/// transaction that inserts the new Command/Event rows, then incremented
/// and released on commit." Neither commits `tx` nor broadcasts - both
/// stay the caller's job, exactly once, after every event in a single
/// submission (a `ProcessCommand` call can trigger several) has been
/// folded in.
///
/// `sync_projections` - the names of the bounded context's `sync`
/// projections - is the caller's job (issue #32): fetched once
/// per commit via [`sync_projection_names`], on this same transaction
/// after its `sequence` row lock is held - see that function's doc
/// comment for why both halves of that matter.
#[tracing::instrument(skip_all)]
pub async fn insert_event_and_update_sync_projections_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
    command_id: Option<i64>,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    encryption_key_ids: &[i64],
    sync_projections: &[String],
) -> crate::error::Result<()> {
    let bounded_context = &event.bounded_context.name;
    let schema = schema_ident(bounded_context);

    insert_event(&mut **tx, event, command_id).await?;

    // `event.encryption_keys`' own `id`s aren't carried on the domain
    // struct (it has none, matching `entity EncryptionKey` itself) -
    // `encryption_key_ids` is what `get_or_create_encryption_key` handed
    // the caller back alongside it, threaded through here so this
    // transaction can also link the join rows atomically with the event
    // insert they belong to.
    // Same `unnest`-batched insert as `insert_command`'s own
    // `command_encryption_keys` above, same reasoning - one round trip
    // for however many keys this event's payload needed, not one per
    // key.
    if !encryption_key_ids.is_empty() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.event_encryption_keys (event_sequence, encryption_key_id) \
             SELECT $1, unnest($2::bigint[])"
        )))
        .bind(event.sequence)
        .bind(encryption_key_ids)
        .execute(&mut **tx)
        .await?;
    }

    fold_event_into_sync_projections_in_tx(tx, event, dispatcher, sync_projections).await
}

/// The sync-projection half of [`insert_event_and_update_sync_projections_in_tx`]:
/// folds `event`, already inserted on `tx`, into every one of
/// `sync_projections`. Split out so a batch written as a set
/// (docs/architecture.md §196) can insert its events first and fold them
/// after, in sequence order.
pub(super) async fn fold_event_into_sync_projections_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    event: &Event,
    dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    sync_projections: &[String],
) -> crate::error::Result<()> {
    let bounded_context = &event.bounded_context.name;
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
        // `Some(vec![])` - registered, but this event's type isn't
        // consumed - falls through to an empty loop below: zero instances
        // touched, `caught_up_to` still advancing after it (§9's "keyed /
        // multi-row Projections" pass). `None` - this instance doesn't
        // declare the projection - can't be folded here at all. If it
        // consumes this event the write is refused: committed, the
        // projection would skip the event for good (docs/architecture.md
        // §160).
        if dispatcher
            .default_state(bounded_context, projection_name)
            .is_none()
        {
            let consumes: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT EXISTS (SELECT 1 FROM {schema}.projection_consumed_event_types \
                 WHERE projection_name = $1 AND event_type_name = $2)"
            )))
            .bind(projection_name)
            .bind(&event.event_type.name)
            .fetch_one(&mut **tx)
            .await?;
            if consumes {
                return Err(crate::event_store::Error::SyncProjectionNotDeclared(
                    projection_name.clone(),
                )
                .into());
            }
        }
        let keys = dispatcher
            .keys(bounded_context, projection_name, event)
            .unwrap_or_default();
        let default_state_json = dispatcher
            .default_state(bounded_context, projection_name)
            .unwrap_or_default();
        let owner_tag_key = dispatcher
            .owner_tag_key(bounded_context, projection_name)
            .flatten();

        for key in &keys {
            // `as_of_sequence` guard (issue #25's investigation
            // finding) - unreachable in practice on this particular path
            // (every call here shares the one transaction that also
            // holds `insert_event`'s own bounded-context `sequence` row
            // lock, already fully serializing concurrent instances for
            // the whole bc, per `insert_event_via_the_locked_path`'s own
            // doc comment), kept anyway so this call site stays
            // structurally identical to the two genuinely-concurrent
            // ones below rather than being the one exception mid-fix.
            let (as_of_sequence, current_state) = get_or_create_projection_state_for_update(
                &mut **tx,
                &schema,
                projection_name,
                key,
                &default_state_json,
            )
            .await?;
            if as_of_sequence >= event.sequence {
                continue;
            }

            let new_state = match dispatcher.project(
                bounded_context,
                projection_name,
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
                projection_name,
                key,
                &new_state,
                owner_tag_key,
                event,
            )
            .await?;
        }

        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.projections SET caught_up_to = $1 WHERE name = $2"
        )))
        .bind(event.sequence)
        .bind(projection_name)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

/// The `dedupe_partition_key`/`dedupe_sequence` pair from
/// specs/skilj.allium's own `SubmitExternalEvent` trigger
/// (`rule CreateExternalEvent`, [docs/architecture.md §39](../../../docs/architecture.md#external-message-dedup-create-external-event)) - always
/// supplied together or not at all. That "both or neither" `requires`
/// guard in the spec is satisfied structurally here, not by a separate
/// runtime check: there is no way to construct one of these fields
/// without the other through this type (or, at the REST wire boundary,
/// through `ExternalEventRequest`'s own `dedupe: Option<DedupeRequest>`
/// shape) - a caller supplying exactly one on the wire fails ordinary
/// JSON deserialization before this type is ever built, the same
/// "malformed request, not this crate's rejection to model" register
/// any other structurally-invalid request body already gets.
#[derive(Debug, Clone, Copy)]
pub struct DedupeCursor<'a> {
    /// Names the partition, shard, or stream this message came from -
    /// e.g. `"{topic}:{partition}"` for Kafka, a shard id for Kinesis.
    pub partition_key: &'a str,
    /// This message's own position within `partition_key` - must be
    /// strictly increasing per partition for the caller's own external
    /// source, the property this whole mechanism relies on and cannot
    /// verify itself (see `highest_dedupe_sequence`'s own doc comment).
    pub sequence: i64,
}

/// How `create_and_insert_external_event` recognises a redelivery -
/// specs/skilj.allium's `rule CreateExternalEvent` takes either the
/// dedupe pair or an `idempotency_key`, never both, and an enum makes
/// "both" unrepresentable the way [`DedupeCursor`] makes "only one of the
/// pair" unrepresentable.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ExternalEventDedupe<'a> {
    /// The per-partition watermark (docs/architecture.md §39): for a
    /// source that replays a partition in order from a committed position.
    Watermark(DedupeCursor<'a>),
    /// One record per message, in `external_message_keys`
    /// (docs/architecture.md §175): for a source that redelivers a message
    /// after later ones - NATS JetStream, AMQP - where a watermark would
    /// drop the redelivery as already seen.
    Key(&'a str),
}

/// What `create_and_insert_external_event` settles on - either a real
/// insert (`Created`, already persisted by the time this returns) or a
/// redelivery this rule recognises and accepts without creating anything
/// (`Redelivered`). Not `SubmitCommandOutcome`'s two-way split
/// (`Accepted`/`Deduplicated`) reused, deliberately: a genuine business
/// rejection has no equivalent here (`create_external_event`'s own
/// failure modes - `TokenNotActive`, `ExternalCreationNotAllowed`, a bad
/// payload - are real errors, returned as `Err`, not a third outcome
/// variant), and calling this case "Deduplicated" would invite comparing
/// it to `SubmitCommandOutcome::Deduplicated`, which returns the
/// original outcome's own triggered sequences - this outcome carries
/// nothing, on purpose, since a watermark remembers only the highest
/// sequence seen, not which event any particular past message produced.
/// See specs/skilj.allium's own `ExternalEventIngestion.ARedeliveryProducesNoEventAndNoOutcome`.
///
/// A key hit (`ExternalEventDedupe::Key`) does know which event the
/// message became, since its record names it, and says so in `sequence`;
/// a watermark hit leaves it `None`.
#[derive(Debug)]
#[non_exhaustive]
pub enum CreateExternalEventOutcome {
    Created(Box<Event>),
    Redelivered { sequence: Option<i64> },
}

/// `highest_dedupe_sequence(adapter, dedupe_partition_key)` from
/// specs/skilj.allium's own `rule CreateExternalEvent` - a black box in
/// the same register as `next_sequence`/`recorded_acceptance`: durable
/// state this library owns, whose storage shape the spec doesn't reach
/// into. `None` when nothing has been recorded yet for this
/// `(adapter_id, partition_key)` pair - the spec's own "or when the
/// partition key is null" clause is handled by the caller never calling
/// this at all when `dedupe` is `None`, not by this function.
///
/// Must be called after the bounded context's own `sequence` row lock is
/// already held (`next_sequence`, inside `create_and_insert_external_event`'s
/// own transaction) - the identical "no lock of its own, rides on the
/// one already held" register `lookup_idempotency_key` already uses, for
/// the same reason: every write into this bounded context is already
/// serialised by that lock, so a plain, unlocked `SELECT` here is
/// race-free.
pub(super) async fn highest_dedupe_sequence<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    schema: &str,
    adapter_id: &str,
    partition_key: &str,
) -> crate::error::Result<Option<i64>> {
    let row: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT last_sequence FROM {schema}.external_message_cursors \
         WHERE adapter_id = $1 AND partition_key = $2"
    )))
    .bind(adapter_id)
    .bind(partition_key)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|(seq,)| seq))
}

/// `recorded_external_event(adapter, idempotency_key)` from
/// specs/skilj.allium's own `rule CreateExternalEvent`: the sequence of
/// the event this adapter created for `key`, or `None`. Same locking
/// story as `highest_dedupe_sequence`: called with the bounded context's
/// `sequence` row lock already held, so a plain `SELECT` is race-free.
pub(super) async fn recorded_external_event<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    schema: &str,
    adapter_id: &str,
    key: &str,
) -> crate::error::Result<Option<i64>> {
    let row: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT event_sequence FROM {schema}.external_message_keys \
         WHERE adapter_id = $1 AND message_key = $2"
    )))
    .bind(adapter_id)
    .bind(key)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|(seq,)| seq))
}

/// Records `key` against the event it created, in the event's own
/// transaction. A plain insert, like `insert_idempotency_key`: the
/// sequence lock rules out a concurrent duplicate getting this far.
pub(super) async fn record_external_message_key<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    schema: &str,
    adapter_id: &str,
    key: &str,
    event_sequence: i64,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.external_message_keys \
         (adapter_id, message_key, event_sequence, created_at) VALUES ($1, $2, $3, $4)"
    )))
    .bind(adapter_id)
    .bind(key)
    .bind(event_sequence)
    .bind(now)
    .execute(executor)
    .await?;
    Ok(())
}

/// Records `sequence` as the new watermark for `(adapter_id,
/// partition_key)`, inside the same transaction as the `Event` it
/// stands for - a watermark advances exactly when the event it
/// represents is durable, never before and never without it (see
/// `create_and_insert_external_event`'s own doc comment). `ON CONFLICT
/// ... DO UPDATE`, not a plain insert the way `insert_idempotency_key`
/// is - unlike that table, a given `(adapter_id, partition_key)` pair is
/// expected to be written many times over its life, once per message
/// from a real partition, not once ever.
pub(super) async fn advance_dedupe_watermark<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    schema: &str,
    adapter_id: &str,
    partition_key: &str,
    sequence: i64,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.external_message_cursors \
         (adapter_id, partition_key, last_sequence, updated_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (adapter_id, partition_key) \
         DO UPDATE SET last_sequence = EXCLUDED.last_sequence, updated_at = EXCLUDED.updated_at"
    )))
    .bind(adapter_id)
    .bind(partition_key)
    .bind(sequence)
    .bind(now)
    .execute(executor)
    .await?;
    Ok(())
}

/// `CreateExternalEvent`'s own atomic whole: `next_sequence`'s row lock,
/// `event_store::create_external_event`'s pure construction (which needs
/// that lock's own allocated sequence baked into the `Event` it builds),
/// and the insert itself, all inside one transaction - `skilj-rest`'s
/// `post_events_external` handler used to run these three steps
/// unlocked/separately-transacted; a rejection from
/// `create_external_event` (`TokenNotActive`/`ExternalCreationNotAllowed`)
/// after `next_sequence` had already run on the bare pool used to burn a
/// sequence number for an event that was never written -
/// `SequenceIsGaplessPerBoundedContext`'s actual bug. Here, that same
/// rejection instead rolls `tx` back before it ever commits, so the
/// allocation never happened as far as any other reader can tell.
/// `resolve_encryption_keys` still runs on `pool`, before `tx` opens -
/// deliberately, not a gap: see the note above the rules in
/// specs/skilj.allium for why `EncryptionKey` provisioning is resolved
/// before a write's own transaction rather than inside it (real work -
/// a master-key wrap, a round trip - that shouldn't extend how long a
/// row lock like `next_sequence`'s own is held). Not callable from
/// anywhere but `skilj-rest` today (no
/// GraphQL surface offers `CreateExternalEvent`), but lives here rather
/// than in that crate per docs/architecture.md §3.1/§3.2: `skilj-core`
/// is the only crate that owns the database driver, so no other crate
/// ever opens a `Transaction` itself.
///
/// **`dedupe`** ([docs/architecture.md §39](../../../docs/architecture.md#external-message-dedup-create-external-event), specs/skilj.allium's own
/// `rule CreateExternalEvent`): `None` reproduces every existing
/// caller's own behaviour exactly, byte for byte - no lookup, no write
/// to `external_message_cursors`, an event created every single time,
/// the same "omitting it changes nothing" guarantee `submit_command`'s
/// own `idempotency_key: None` already gives (see
/// `ExternalEventIngestion.OmittingTheDedupePairChangesNothing`).
/// `Some(cursor)`: checked against `highest_dedupe_sequence` right after
/// `next_sequence`'s own lock is acquired - the earliest point that's
/// race-free, mirroring exactly where `submit_command`'s own idempotency
/// check runs relative to that same lock. A watermark hit rolls `tx`
/// back (dropped, nothing else was written) and returns
/// `CreateExternalEventOutcome::Redelivered` without ever calling
/// `event_store::create_external_event` at all; a miss proceeds exactly
/// as `None` would, plus one more write in the same transaction -
/// `advance_dedupe_watermark` - before `tx.commit()`.
///
/// `Some(ExternalEventDedupe::Key(key))` is checked at the same point
/// against `external_message_keys` instead (docs/architecture.md §175): a
/// hit is `Redelivered` with the sequence of the event the key created,
/// a miss records the key against the new event before the commit.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all)]
pub async fn create_and_insert_external_event(
    pool: &Pool,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    adapter: &ExternalEventToken,
    payload: String,
    source_content: String,
    source_context: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    dedupe: Option<ExternalEventDedupe<'_>>,
    now: DateTime<Utc>,
    key_service: Option<&KeyService>,
    blind: &crate::blind::BlindIndex,
) -> crate::error::Result<CreateExternalEventOutcome> {
    // Every `requires` first, as the spec orders them: before any key is
    // provisioned, the sequence lock taken, or a redelivery recognised
    // (docs/architecture.md §141).
    crate::event_store::check_create_external_event(
        adapter,
        &payload,
        correlation_id.as_deref(),
        causation_id.as_deref(),
        blind,
    )?;
    let bounded_context_name = adapter.event_type.bounded_context.name.clone();
    let schema = schema_ident(&bounded_context_name);

    let mut resolved = std::collections::HashMap::new();
    resolve_encryption_keys(
        pool,
        &bounded_context_name,
        &adapter.event_type.sensitive_fields,
        &payload,
        key_service,
        &mut resolved,
    )
    .await?;

    let mut tx = pool.begin().await?;
    let (next_seq, limits) = next_sequence_with_limits(&mut *tx, &bounded_context_name).await?;
    let payload_limits = limits.apply(projection_dispatcher.payload_limits());
    // docs/architecture.md §209: tags are derived below, under the lock a
    // rotation holds - of the generation stored now.
    refresh_blind_generations(&mut tx, &bounded_context_name, blind).await?;

    // On a hit: implicit rollback - nothing else was written, the same
    // "a hit is a cached prior answer, not a new decision, tx is simply
    // dropped" treatment `submit_command`'s own idempotency-key check
    // already uses.
    match &dedupe {
        Some(ExternalEventDedupe::Watermark(cursor)) => {
            let watermark =
                highest_dedupe_sequence(&mut *tx, &schema, &adapter.id, cursor.partition_key)
                    .await?;
            if watermark.is_some_and(|w| cursor.sequence <= w) {
                return Ok(CreateExternalEventOutcome::Redelivered { sequence: None });
            }
        }
        Some(ExternalEventDedupe::Key(key)) => {
            if let Some(sequence) =
                recorded_external_event(&mut *tx, &schema, &adapter.id, key).await?
            {
                return Ok(CreateExternalEventOutcome::Redelivered {
                    sequence: Some(sequence),
                });
            }
        }
        None => {}
    }

    let event = crate::event_store::create_external_event(
        adapter,
        payload,
        source_content,
        source_context,
        correlation_id,
        causation_id,
        next_seq,
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
        blind,
    )?;
    // As stored, after encryption (docs/architecture.md §238). A
    // redelivery above was answered before this: it was stored once.
    payload_limits.check_event(&event)?;
    let blob_claims = event_blob_claims(projection_dispatcher, &event)?;
    check_blob_claims(&mut tx, &bounded_context_name, &blob_claims).await?;
    let encryption_key_ids = encryption_key_ids(&event.encryption_keys, &resolved);
    // Fetched only now - after `next_sequence` above already took this
    // bounded context's own lock - see `insert_event_and_update_sync_projections_in_tx`'s
    // own doc comment on why that ordering, not "as early as possible", is
    // what keeps this read race-free against `promote_projection_rebuild`.
    let sync_projections = sync_projection_names(&mut tx, &bounded_context_name).await?;
    insert_event_and_update_sync_projections_in_tx(
        &mut tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
        &sync_projections,
    )
    .await?;
    record_blob_claims(&mut tx, &bounded_context_name, &blob_claims, None, now).await?;
    match &dedupe {
        Some(ExternalEventDedupe::Watermark(cursor)) => {
            advance_dedupe_watermark(
                &mut *tx,
                &schema,
                &adapter.id,
                cursor.partition_key,
                cursor.sequence,
                now,
            )
            .await?;
        }
        Some(ExternalEventDedupe::Key(key)) => {
            record_external_message_key(&mut *tx, &schema, &adapter.id, key, event.sequence, now)
                .await?;
        }
        None => {}
    }
    tx.commit().await?;
    broadcaster.publish(&event);
    record_event_appended(&event);
    notify_event_appended(pool, &event, broadcaster.instance_id()).await;
    event_cache.append(&event).await;

    Ok(CreateExternalEventOutcome::Created(Box::new(event)))
}

/// The dedupe cursor or key of one item of
/// [`create_and_insert_external_events`]: an owned
/// [`ExternalEventDedupe`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ExternalEventBatchDedupe {
    /// See [`ExternalEventDedupe::Watermark`].
    Watermark {
        /// The source's partition.
        partition_key: String,
        /// The message's position in it.
        sequence: i64,
    },
    /// See [`ExternalEventDedupe::Key`].
    Key(String),
}

impl ExternalEventBatchDedupe {
    pub(super) fn as_dedupe(&self) -> ExternalEventDedupe<'_> {
        match self {
            ExternalEventBatchDedupe::Watermark {
                partition_key,
                sequence,
            } => ExternalEventDedupe::Watermark(DedupeCursor {
                partition_key,
                sequence: *sequence,
            }),
            ExternalEventBatchDedupe::Key(key) => ExternalEventDedupe::Key(key),
        }
    }
}

/// One external event of [`create_and_insert_external_events`]: what
/// [`create_and_insert_external_event`] takes for one event, apart from
/// the token, which the batch shares.
#[derive(Debug, Clone)]
pub struct ExternalEventBatchItem {
    /// The payload, as JSON.
    pub payload: String,
    /// The message as the source sent it.
    pub source_content: String,
    /// Where it came from, if the adapter says.
    pub source_context: Option<String>,
    /// The caller's correlation id, or `None` to have one generated.
    pub correlation_id: Option<String>,
    /// The caller's causation id, if any.
    pub causation_id: Option<String>,
    /// The cursor or key that recognises a redelivery, if any.
    pub dedupe: Option<ExternalEventBatchDedupe>,
}

/// `rule CreateExternalEventBatch` (docs/architecture.md §227): several
/// external events through one token, created or recognised as
/// redeliveries in the order given, each with the outcome
/// [`create_and_insert_external_event`] would give it alone, right after
/// the one before it. One result per item, in order.
///
/// One transaction takes the bounded context's sequence lock once and
/// commits once for the whole batch. Each item runs in its own savepoint:
/// an item that fails - its payload off the schema, an error inserting it
/// - is rolled back alone, sequence number included, and the items after
/// it still run - unless `stop_at_first_failure`, when every item after
/// the first that fails answers [`crate::error::Error::NotProcessed`] and
/// is not processed, so a source that relies on its order (a dedupe
/// watermark, a partition's sequence) can send them again after dealing
/// with the failed one. Because they share the transaction, an item's dedupe
/// check sees the watermarks and keys recorded by the items before it, so
/// a batch that repeats a message answers the repeat as redelivered. An
/// error taking the lock or committing fails every item.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(events = items.len()))]
pub async fn create_and_insert_external_events(
    pool: &Pool,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    adapter: &ExternalEventToken,
    items: Vec<ExternalEventBatchItem>,
    key_service: Option<&KeyService>,
    blind: &crate::blind::BlindIndex,
    stop_at_first_failure: bool,
) -> Vec<crate::error::Result<CreateExternalEventOutcome>> {
    let bounded_context_name = adapter.event_type.bounded_context.name.clone();
    let schema = schema_ident(&bounded_context_name);

    // Every `requires`, and every encryption key, before the lock - as
    // `create_and_insert_external_event` does for one (§141).
    let mut results: Vec<Option<crate::error::Result<CreateExternalEventOutcome>>> =
        Vec::with_capacity(items.len());
    let mut resolved = std::collections::HashMap::new();
    let mut failed = false;
    for item in &items {
        if failed && stop_at_first_failure {
            results.push(Some(Err(crate::error::Error::NotProcessed)));
            continue;
        }
        let checked = crate::event_store::check_create_external_event(
            adapter,
            &item.payload,
            item.correlation_id.as_deref(),
            item.causation_id.as_deref(),
            blind,
        );
        let checked = match checked {
            Ok(_) => {
                resolve_encryption_keys(
                    pool,
                    &bounded_context_name,
                    &adapter.event_type.sensitive_fields,
                    &item.payload,
                    key_service,
                    &mut resolved,
                )
                .await
            }
            Err(e) => Err(e),
        };
        failed |= checked.is_err();
        results.push(checked.err().map(Err));
    }
    if results.iter().all(Option::is_some) {
        return results.into_iter().flatten().collect();
    }

    let created = async {
        let mut tx = pool.begin().await?;
        let (_, limits) =
            lock_bounded_context_sequence_with_limits(&mut tx, &bounded_context_name).await?;
        let payload_limits = limits.apply(projection_dispatcher.payload_limits());
        // docs/architecture.md §209: tags are derived below, under the lock
        // a rotation holds - of the generation stored now.
        refresh_blind_generations(&mut tx, &bounded_context_name, blind).await?;
        // After the lock - see `create_and_insert_external_event`.
        let sync_projections = sync_projection_names(&mut tx, &bounded_context_name).await?;
        let mut outcomes = Vec::new();
        let mut failed = false;
        for (item, result) in items.into_iter().zip(&results) {
            if result.is_some() {
                continue;
            }
            if failed && stop_at_first_failure {
                outcomes.push(Err(crate::error::Error::NotProcessed));
                continue;
            }
            // Each item its own time, as if sent alone.
            let now = Utc::now();
            let dedupe = item.dedupe.clone();
            let mut savepoint = tx.begin().await?;
            let outcome = insert_external_event_in_tx(
                &mut savepoint,
                &schema,
                projection_dispatcher,
                adapter,
                item,
                dedupe.as_ref().map(ExternalEventBatchDedupe::as_dedupe),
                now,
                &resolved,
                &sync_projections,
                blind,
                &payload_limits,
            )
            .await;
            match &outcome {
                Ok(CreateExternalEventOutcome::Created(_)) => savepoint.commit().await?,
                // A redelivery wrote nothing; a failure is undone alone.
                // Awaited, not left to `Drop`: the next item reuses the
                // connection.
                _ => savepoint.rollback().await?,
            }
            failed |= outcome.is_err();
            outcomes.push(outcome);
        }
        tx.commit().await?;
        Ok::<_, crate::error::Error>(outcomes)
    }
    .await;

    match created {
        Ok(outcomes) => {
            let events: Vec<&Event> = outcomes
                .iter()
                .filter_map(|outcome| match outcome {
                    Ok(CreateExternalEventOutcome::Created(event)) => Some(event.as_ref()),
                    _ => None,
                })
                .collect();
            for event in &events {
                broadcaster.publish(event);
                record_event_appended(event);
                event_cache.append(event).await;
            }
            notify_events_appended(pool, &events, broadcaster.instance_id()).await;
            let mut outcomes = outcomes.into_iter();
            results
                .into_iter()
                .map(|result| {
                    result.unwrap_or_else(|| outcomes.next().expect("one outcome per item"))
                })
                .collect()
        }
        // Nothing in the transaction committed: every item that reached
        // it gets the failure. `Error` isn't `Clone`.
        Err(e) => results
            .into_iter()
            .map(|result| result.unwrap_or_else(|| Err(crate::error::Error::batch_failed(&e))))
            .collect(),
    }
}

/// One item of [`create_and_insert_external_events`], on the batch's
/// transaction (a savepoint of it), which already holds the sequence lock:
/// the dedupe check, the sequence number, the event, its insert and sync
/// projections, and the watermark or key - what
/// `create_and_insert_external_event` does for one event under the lock.
#[allow(clippy::too_many_arguments)]
pub(super) async fn insert_external_event_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    schema: &str,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    adapter: &ExternalEventToken,
    item: ExternalEventBatchItem,
    dedupe: Option<ExternalEventDedupe<'_>>,
    now: DateTime<Utc>,
    resolved: &std::collections::HashMap<(String, String), (EncryptionKey, i64, DataKey)>,
    sync_projections: &[String],
    blind: &crate::blind::BlindIndex,
    payload_limits: &crate::payload_limits::PayloadLimits,
) -> crate::error::Result<CreateExternalEventOutcome> {
    match &dedupe {
        Some(ExternalEventDedupe::Watermark(cursor)) => {
            let watermark =
                highest_dedupe_sequence(&mut **tx, schema, &adapter.id, cursor.partition_key)
                    .await?;
            if watermark.is_some_and(|w| cursor.sequence <= w) {
                return Ok(CreateExternalEventOutcome::Redelivered { sequence: None });
            }
        }
        Some(ExternalEventDedupe::Key(key)) => {
            if let Some(sequence) =
                recorded_external_event(&mut **tx, schema, &adapter.id, key).await?
            {
                return Ok(CreateExternalEventOutcome::Redelivered {
                    sequence: Some(sequence),
                });
            }
        }
        None => {}
    }

    let next_seq = next_sequence(&mut **tx, &adapter.event_type.bounded_context.name).await?;
    let event = crate::event_store::create_external_event(
        adapter,
        item.payload,
        item.source_content,
        item.source_context,
        item.correlation_id,
        item.causation_id,
        next_seq,
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
        blind,
    )?;
    // Refused alone: the savepoint takes the sequence number back
    // (docs/architecture.md §238, §239).
    payload_limits.check_event(&event)?;
    let blob_claims = event_blob_claims(projection_dispatcher, &event)?;
    let schema_name = &adapter.event_type.bounded_context.name;
    check_blob_claims(tx, schema_name, &blob_claims).await?;
    let encryption_key_ids = encryption_key_ids(&event.encryption_keys, resolved);
    insert_event_and_update_sync_projections_in_tx(
        tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
        sync_projections,
    )
    .await?;
    record_blob_claims(tx, schema_name, &blob_claims, None, now).await?;
    match &dedupe {
        Some(ExternalEventDedupe::Watermark(cursor)) => {
            advance_dedupe_watermark(
                &mut **tx,
                schema,
                &adapter.id,
                cursor.partition_key,
                cursor.sequence,
                now,
            )
            .await?;
        }
        Some(ExternalEventDedupe::Key(key)) => {
            record_external_message_key(&mut **tx, schema, &adapter.id, key, event.sequence, now)
                .await?;
        }
        None => {}
    }
    Ok(CreateExternalEventOutcome::Created(Box::new(event)))
}

/// `CreateDirectEvent`'s own twin of `create_and_insert_external_event`
/// above - same reasoning, same fix, only the adapter type and the
/// absent `source_content`/`source_context` differ.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all)]
pub async fn create_and_insert_direct_event(
    pool: &Pool,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    adapter: &DirectCreationToken,
    payload: String,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    now: DateTime<Utc>,
    key_service: Option<&KeyService>,
    blind: &crate::blind::BlindIndex,
) -> crate::error::Result<Event> {
    // See `create_and_insert_external_event`'s identical check.
    crate::event_store::check_create_direct_event(
        adapter,
        &payload,
        correlation_id.as_deref(),
        causation_id.as_deref(),
        blind,
    )?;
    let bounded_context_name = adapter.event_type.bounded_context.name.clone();

    let mut resolved = std::collections::HashMap::new();
    resolve_encryption_keys(
        pool,
        &bounded_context_name,
        &adapter.event_type.sensitive_fields,
        &payload,
        key_service,
        &mut resolved,
    )
    .await?;

    let mut tx = pool.begin().await?;
    let (next_seq, limits) = next_sequence_with_limits(&mut *tx, &bounded_context_name).await?;
    // docs/architecture.md §209: tags are derived below, under the lock a
    // rotation holds - of the generation stored now.
    refresh_blind_generations(&mut tx, &bounded_context_name, blind).await?;
    let event = crate::event_store::create_direct_event(
        adapter,
        payload,
        correlation_id,
        causation_id,
        next_seq,
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
        blind,
    )?;
    // As stored, after encryption (docs/architecture.md §238, §239).
    limits
        .apply(projection_dispatcher.payload_limits())
        .check_event(&event)?;
    let blob_claims = event_blob_claims(projection_dispatcher, &event)?;
    check_blob_claims(&mut tx, &bounded_context_name, &blob_claims).await?;
    let encryption_key_ids = encryption_key_ids(&event.encryption_keys, &resolved);
    // Fetched only now - after `next_sequence` above already took this
    // bounded context's own lock - see `insert_event_and_update_sync_projections_in_tx`'s
    // own doc comment on why that ordering, not "as early as possible", is
    // what keeps this read race-free against `promote_projection_rebuild`.
    let sync_projections = sync_projection_names(&mut tx, &bounded_context_name).await?;
    insert_event_and_update_sync_projections_in_tx(
        &mut tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
        &sync_projections,
    )
    .await?;
    record_blob_claims(&mut tx, &bounded_context_name, &blob_claims, None, now).await?;
    tx.commit().await?;
    broadcaster.publish(&event);
    record_event_appended(&event);
    notify_event_appended(pool, &event, broadcaster.instance_id()).await;
    event_cache.append(&event).await;

    Ok(event)
}
