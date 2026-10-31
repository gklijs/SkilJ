//! Persistence for routes rule groups.

use super::*;

/// `cross_context_route_cursors`'s own read - `None` when no row exists
/// at all, which means this route has never had a single catch-up tick
/// run for it since it was registered (a row, once written, is never
/// deleted - see `update_cross_context_route_cursor`). Distinct from a
/// row genuinely holding `-1` (a `CrossContextRouteStartFrom::Latest`
/// route whose very first tick found no `Source` occurrences at all yet
/// to seed past - see `catch_up_cross_context_route`'s own doc comment):
/// that case has already had its one-time seeding tick, so it must never
/// be seeded again, which is exactly why this returns `Option<i64>`
/// rather than collapsing both into the same `-1` sentinel
/// `sequence`/`caught_up_to` use elsewhere.
pub(super) async fn get_cross_context_route_cursor(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(source_bounded_context);
    let row: Option<(i64,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT last_dispatched_sequence FROM {schema}.cross_context_route_cursors \
         WHERE route_name = $1"
    )))
    .bind(route_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(seq,)| seq))
}

/// Also unconditionally clears any retry backoff state (issue
/// #21) - every call site advances the cursor past an occurrence that is
/// now *done* one way or another (skipped, rejected, accepted, or
/// parked after retries were exhausted), so whatever retry state applied
/// to it is stale for the next occurrence regardless of which of those
/// outcomes this one was. See `catch_up_cross_context_route`'s own doc
/// comment.
pub(super) async fn update_cross_context_route_cursor(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
    sequence: i64,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(source_bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.cross_context_route_cursors \
         (route_name, last_dispatched_sequence, updated_at) VALUES ($1, $2, $3) \
         ON CONFLICT (route_name) DO UPDATE SET \
         last_dispatched_sequence = EXCLUDED.last_dispatched_sequence, \
         updated_at = EXCLUDED.updated_at, \
         retry_attempt_count = 0, \
         retry_first_failed_at = NULL, \
         retry_next_attempt_at = NULL"
    )))
    .bind(route_name)
    .bind(sequence)
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// `(retry_attempt_count, retry_first_failed_at, retry_next_attempt_at)` -
/// `get_cross_context_route_retry_state`'s own return shape, named only
/// to keep that signature (and clippy) happy, not used anywhere else.
pub(super) type CrossContextRouteRetryState = (i32, Option<DateTime<Utc>>, Option<DateTime<Utc>>);

/// `catch_up_cross_context_route`'s own read of the retry backoff state
/// `record_cross_context_route_retry_failure` writes - `(0, None, None)`
/// when no row exists yet (this route has never ticked, or every prior
/// occurrence succeeded outright), the same "no row = nothing recorded"
/// convention `get_cross_context_route_cursor` already uses one level up.
pub(super) async fn get_cross_context_route_retry_state(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
) -> crate::error::Result<CrossContextRouteRetryState> {
    let schema = schema_ident(source_bounded_context);
    let row: Option<CrossContextRouteRetryState> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT retry_attempt_count, retry_first_failed_at, retry_next_attempt_at \
             FROM {schema}.cross_context_route_cursors WHERE route_name = $1"
    )))
    .bind(route_name)
    .fetch_optional(pool)
    .await?;
    Ok(row.unwrap_or((0, None, None)))
}

/// Persists one more failed attempt at the route's own blocked
/// head-of-line occurrence - `current_cursor` is `last_dispatched_sequence`'s
/// own current value (unchanged by this call; only present so the
/// `INSERT` branch of `ON CONFLICT` has a value to write for a route
/// whose very first-ever tick is already failing, before
/// `update_cross_context_route_cursor` has ever run for it). Doesn't
/// touch `updated_at` on conflict - that column tracks the cursor's own
/// last *advance*, not the last retry attempt.
#[allow(clippy::too_many_arguments)]
pub(super) async fn record_cross_context_route_retry_failure(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
    current_cursor: i64,
    attempt_count: i32,
    first_failed_at: DateTime<Utc>,
    next_attempt_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(source_bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.cross_context_route_cursors \
         (route_name, last_dispatched_sequence, updated_at, retry_attempt_count, \
          retry_first_failed_at, retry_next_attempt_at) \
         VALUES ($1, $2, $3, $4, $5, $6) \
         ON CONFLICT (route_name) DO UPDATE SET \
         retry_attempt_count = EXCLUDED.retry_attempt_count, \
         retry_first_failed_at = EXCLUDED.retry_first_failed_at, \
         retry_next_attempt_at = EXCLUDED.retry_next_attempt_at"
    )))
    .bind(route_name)
    .bind(current_cursor)
    .bind(now)
    .bind(attempt_count)
    .bind(first_failed_at)
    .bind(next_attempt_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Holds a route back until `next_attempt_at` without touching its
/// attempt count or first failure (docs/architecture.md §161) - unlike
/// [`record_cross_context_route_retry_failure`].
pub(super) async fn defer_cross_context_route(
    pool: &Pool,
    source_bounded_context: &str,
    route_name: &str,
    current_cursor: i64,
    next_attempt_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(source_bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.cross_context_route_cursors \
         (route_name, last_dispatched_sequence, updated_at, retry_next_attempt_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (route_name) DO UPDATE SET \
         retry_next_attempt_at = EXCLUDED.retry_next_attempt_at"
    )))
    .bind(route_name)
    .bind(current_cursor)
    .bind(Utc::now())
    .bind(next_attempt_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// issue #21 - which family of thing a [`ParkedDelivery`]
/// originally was, and therefore how `retryParkedDelivery` redrives it.
/// `target_bounded_context`/`target_command_type` are populated only for
/// `CrossContextRoute` and `Deadline`; `ExternalEvent`/
/// `CommandTrigger` populate `access_token_id` instead (see
/// `ParkedDelivery`'s own doc comment for the full column-by-kind story).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParkedDeliveryKind {
    CrossContextRoute,
    ExternalEvent,
    CommandTrigger,
    /// A deadline whose command kept failing (docs/architecture.md §115).
    Deadline,
}

impl ParkedDeliveryKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            ParkedDeliveryKind::CrossContextRoute => "cross_context_route",
            ParkedDeliveryKind::ExternalEvent => "external_event",
            ParkedDeliveryKind::CommandTrigger => "command_trigger",
            ParkedDeliveryKind::Deadline => "deadline",
        }
    }
}

pub(super) fn parked_delivery_kind_from_str(s: &str) -> ParkedDeliveryKind {
    match s {
        "external_event" => ParkedDeliveryKind::ExternalEvent,
        "command_trigger" => ParkedDeliveryKind::CommandTrigger,
        "deadline" => ParkedDeliveryKind::Deadline,
        _ => ParkedDeliveryKind::CrossContextRoute,
    }
}

/// issue #21's own generic "parked delivery" record - see
/// `docs/architecture.md`'s parked-deliveries section for the full
/// design. `request_json` always carries enough to redrive a retry on
/// its own:
/// - `CrossContextRoute`: the `Target` command's own already-translated
///   JSON payload (`target_bounded_context`/`target_command_type` name
///   where to submit it - `access_token_id` is `None`).
/// - `ExternalEvent`/`CommandTrigger`: the bridge's own original
///   `ExternalEventRequest`/`CommandTriggerRequest` body, verbatim
///   (`target_bounded_context`/`target_command_type` are `None`;
///   `access_token_id` names which `ExternalEventToken`/`CommandToken`
///   to re-resolve and redrive through - the bridge's own credential,
///   never the token's secret, which is never stored here).
///
/// Lives in the same bounded-context schema the failing delivery itself
/// targeted - for `ExternalEvent`/`CommandTrigger`, that's
/// `access_token_id`'s own token's bounded context, resolved once at
/// ingestion time by `skilj-rest`'s `POST /v1/parked-deliveries` handler
/// (the same capability-based resolution every other REST route already
/// does), not a caller-supplied path/argument.
#[derive(Debug, Clone, PartialEq)]
pub struct ParkedDelivery {
    pub id: String,
    pub source: String,
    pub kind: ParkedDeliveryKind,
    pub identifier: String,
    pub access_token_id: Option<String>,
    pub target_bounded_context: Option<String>,
    pub target_command_type: Option<String>,
    pub request_json: serde_json::Value,
    pub error: String,
    pub attempt_count: i32,
    pub first_failed_at: DateTime<Utc>,
    pub last_failed_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
pub(super) struct ParkedDeliveryRow {
    pub(super) id: String,
    pub(super) source: String,
    pub(super) kind: String,
    pub(super) identifier: String,
    pub(super) access_token_id: Option<String>,
    pub(super) target_bounded_context: Option<String>,
    pub(super) target_command_type: Option<String>,
    pub(super) request_json: Json<serde_json::Value>,
    pub(super) error: String,
    pub(super) attempt_count: i32,
    pub(super) first_failed_at: DateTime<Utc>,
    pub(super) last_failed_at: DateTime<Utc>,
}

impl From<ParkedDeliveryRow> for ParkedDelivery {
    fn from(row: ParkedDeliveryRow) -> Self {
        ParkedDelivery {
            id: row.id,
            source: row.source,
            kind: parked_delivery_kind_from_str(&row.kind),
            identifier: row.identifier,
            access_token_id: row.access_token_id,
            target_bounded_context: row.target_bounded_context,
            target_command_type: row.target_command_type,
            request_json: row.request_json.0,
            error: row.error,
            attempt_count: row.attempt_count,
            first_failed_at: row.first_failed_at,
            last_failed_at: row.last_failed_at,
        }
    }
}

pub(super) const PARKED_DELIVERY_COLUMNS: &str = "id, source, kind, identifier, access_token_id, \
    target_bounded_context, target_command_type, request_json, error, attempt_count, \
    first_failed_at, last_failed_at";

/// issue #21 - `parked_deliveries`, one row per persistently-
/// failed delivery. A brand-new table, so (unlike
/// `cross_context_route_cursors`'s own retry columns above) no
/// already-provisioned bounded context needs a separate `ALTER TABLE`
/// patch - `CREATE TABLE IF NOT EXISTS`, called from both
/// `provision_bounded_context_schema` and every `build()`'s startup
/// loop, is the whole story, the same "brand-new table needs no
/// migration dance" register `ensure_external_message_cursors_table`'s
/// own doc comment already uses.
#[tracing::instrument(skip_all)]
pub async fn ensure_parked_deliveries_table<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let index = create_index_patch(
        &schema,
        "parked_deliveries_page_order",
        format!(
            "CREATE INDEX IF NOT EXISTS parked_deliveries_page_order \
             ON {schema}.parked_deliveries (first_failed_at DESC, id DESC)"
        ),
    );
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.parked_deliveries (
            id TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            kind TEXT NOT NULL,
            identifier TEXT NOT NULL,
            access_token_id TEXT,
            target_bounded_context TEXT,
            target_command_type TEXT,
            request_json JSONB NOT NULL,
            error TEXT NOT NULL,
            attempt_count INT NOT NULL,
            first_failed_at TIMESTAMPTZ NOT NULL,
            last_failed_at TIMESTAMPTZ NOT NULL
        );
        {index}"
    )))
    .execute(executor)
    .await?;
    Ok(())
}

/// The index this module's first parked-delivery migration created -
/// `(source, kind, identifier)` alone. Replaced by
/// [`PARKED_DELIVERIES_OCCURRENCE_INDEX`]; kept only so
/// `migrate_parked_deliveries_dedup_and_unique_index` can drop it.
pub(super) const PARKED_DELIVERIES_LEGACY_DEDUP_INDEX: &str =
    "parked_deliveries_source_kind_identifier_key";

pub(super) const PARKED_DELIVERIES_OCCURRENCE_INDEX: &str = "parked_deliveries_occurrence_key";

/// The `UNIQUE` index `insert_parked_delivery`'s own `ON CONFLICT` infers
/// against - one row per `(source, kind, identifier)` *per access token*.
/// The token is part of the key because it's what `retryParkedDelivery`
/// redrives under: with it left out, a second bridge credential
/// reporting a (guessable) identifier another credential already parked
/// would upsert onto that row, swapping its own `request_json` in under
/// the first token's authority. `COALESCE` rather than `NULLS NOT
/// DISTINCT` (Postgres 15+ only) so `CrossContextRoute`'s own
/// always-`NULL` rows still dedupe against each other; a real token id is
/// never empty, so `''` can't collide with one.
pub(super) fn parked_deliveries_occurrence_index_ddl(schema: &str) -> String {
    format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS {PARKED_DELIVERIES_OCCURRENCE_INDEX} \
         ON {schema}.parked_deliveries (source, kind, identifier, (COALESCE(access_token_id, '')))"
    )
}

/// issue #25 review (docs/architecture.md §56): belt-and-suspenders
/// for `catch_up_cross_context_route`'s own advisory-lock fix above -
/// a `UNIQUE` index identifying one real failed occurrence (see
/// `parked_deliveries_occurrence_index_ddl` for its exact key) so
/// `insert_parked_delivery`'s own `ON CONFLICT` can turn any remaining
/// duplicate-insert path, from this bug or a future one, into a harmless
/// upsert instead of a second row.
///
/// A live bounded context that already hit the race this fixes can have
/// real duplicate rows sitting in `parked_deliveries` already - creating
/// the index directly would fail outright against those, turning a
/// silent duplication bug into a hard startup error. So this runs as a
/// real migration, `migrate_idempotency_keys_client_id_scoping`'s own
/// shape: a dedicated transaction, a `pg_advisory_xact_lock` on the
/// schema name (serializing concurrent instances migrating the same
/// bounded context at startup), an idempotency check against
/// `pg_indexes` so an already-migrated schema is a cheap no-op, then the
/// actual work - here, deleting every duplicate but the newest
/// (`(last_failed_at, id)` descending; `id` only to break an exact tie,
/// never itself meaningful) before creating the index. A duplicate row
/// this deletes was never independently actionable - both were always
/// the identical occurrence records under `retryParkedDelivery`'s own
/// terms - so discarding all but one loses no real operator-facing
/// information, unlike `idempotency_keys`' own pre-migration rows.
///
/// A schema migrated under the earlier, narrower
/// `(source, kind, identifier)` index converges here too: that index is
/// strictly stricter, so its rows never violate the wider one, and it's
/// dropped once the wider one exists. A bounded context provisioned
/// after this existed already has the wider index from
/// `provision_bounded_context_schema` itself, and is a no-op here.
pub async fn migrate_parked_deliveries_dedup_and_unique_index(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    let raw_schema = format!("bc_{bounded_context}");

    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(&raw_schema)
        .execute(&mut *tx)
        .await?;

    let (has_current, has_legacy): (bool, bool) = sqlx::query_as(
        "SELECT \
            EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2), \
            EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $3)",
    )
    .bind(&raw_schema)
    .bind(PARKED_DELIVERIES_OCCURRENCE_INDEX)
    .bind(PARKED_DELIVERIES_LEGACY_DEDUP_INDEX)
    .fetch_one(&mut *tx)
    .await?;

    if has_current && !has_legacy {
        tx.commit().await?;
        return Ok(());
    }

    if !has_current {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {schema}.parked_deliveries a USING {schema}.parked_deliveries b \
             WHERE a.source = b.source AND a.kind = b.kind AND a.identifier = b.identifier \
             AND a.access_token_id IS NOT DISTINCT FROM b.access_token_id \
             AND (a.last_failed_at, a.id) < (b.last_failed_at, b.id)"
        )))
        .execute(&mut *tx)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(parked_deliveries_occurrence_index_ddl(
            &schema,
        )))
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP INDEX IF EXISTS {schema}.{PARKED_DELIVERIES_LEGACY_DEDUP_INDEX}"
    )))
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// `id` is generated here (`shared::generate_token_id`, the same opaque-
/// id generator every other synthetic identifier in this codebase
/// already uses - see its own doc comment) rather than left to the
/// caller, since nothing about a parked delivery's own identity needs to
/// be caller-chosen or caller-visible before this call returns it.
/// `ON CONFLICT` on `parked_deliveries_occurrence_index_ddl`'s key
/// (docs/architecture.md §56) turns a duplicate-occurrence insert into an
/// upsert that refreshes the failure details onto the existing row
/// rather than a second one -
/// `RETURNING` so the id/fields this returns always describe the row
/// that actually exists afterward, real either way (the fresh `id` this
/// generated, or the winning row's own from an earlier insert).
#[allow(clippy::too_many_arguments)]
pub async fn insert_parked_delivery(
    pool: &Pool,
    bounded_context: &str,
    source: &str,
    kind: ParkedDeliveryKind,
    identifier: &str,
    access_token_id: Option<&str>,
    target_bounded_context: Option<&str>,
    target_command_type: Option<&str>,
    request_json: &serde_json::Value,
    error: &str,
    attempt_count: i32,
    first_failed_at: DateTime<Utc>,
    last_failed_at: DateTime<Utc>,
) -> crate::error::Result<ParkedDelivery> {
    let schema = schema_ident(bounded_context);
    let id = crate::shared::generate_token_id();
    let row: ParkedDeliveryRow = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.parked_deliveries \
         (id, source, kind, identifier, access_token_id, target_bounded_context, \
          target_command_type, request_json, error, attempt_count, first_failed_at, \
          last_failed_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
         ON CONFLICT (source, kind, identifier, (COALESCE(access_token_id, ''))) DO UPDATE SET \
         request_json = EXCLUDED.request_json, \
         error = EXCLUDED.error, \
         attempt_count = EXCLUDED.attempt_count, \
         last_failed_at = EXCLUDED.last_failed_at \
         RETURNING {PARKED_DELIVERY_COLUMNS}"
    )))
    .bind(&id)
    .bind(source)
    .bind(kind.as_str())
    .bind(identifier)
    .bind(access_token_id)
    .bind(target_bounded_context)
    .bind(target_command_type)
    .bind(Json(request_json))
    .bind(error)
    .bind(attempt_count)
    .bind(first_failed_at)
    .bind(last_failed_at)
    .fetch_one(pool)
    .await?;
    Ok(row.into())
}

/// Every parked delivery in `bounded_context`, in page order (see
/// [`list_parked_deliveries_page`]). Unbounded - for tests and internal
/// checks; `parkedDeliveries` pages.
pub async fn list_parked_deliveries(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<ParkedDelivery>> {
    list_parked_deliveries_page(pool, bounded_context, None, i64::MAX).await
}

/// `AdminAccess`-gated `parkedDeliveries(boundedContext:, after:)`'s own
/// read (docs/architecture.md §86): at most `limit` rows, newest parked
/// first, strictly after `after` in that order. The order is
/// `(first_failed_at DESC, id DESC)`, not `last_failed_at`: a retry
/// that fails again updates `last_failed_at`, which would move the row
/// across a page boundary mid-listing and skip or repeat it, while
/// `first_failed_at` never changes for a row.
pub async fn list_parked_deliveries_page(
    pool: &Pool,
    bounded_context: &str,
    after: Option<&ParkedDeliveryCursor>,
    limit: i64,
) -> crate::error::Result<Vec<ParkedDelivery>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<ParkedDeliveryRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PARKED_DELIVERY_COLUMNS} FROM {schema}.parked_deliveries \
         WHERE $1::timestamptz IS NULL OR (first_failed_at, id) < ($1, $2) \
         ORDER BY first_failed_at DESC, id DESC LIMIT $3"
    )))
    .bind(after.map(|c| c.first_failed_at))
    .bind(after.map(|c| c.id.clone()))
    .bind(limit)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(ParkedDelivery::from).collect())
}

/// A position in [`list_parked_deliveries_page`]'s order. It carries the
/// position itself rather than naming a row, so it stays valid when that
/// row is retried or discarded between pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParkedDeliveryCursor {
    pub first_failed_at: DateTime<Utc>,
    pub id: String,
}

impl ParkedDeliveryCursor {
    pub fn of(delivery: &ParkedDelivery) -> Self {
        Self {
            first_failed_at: delivery.first_failed_at,
            id: delivery.id.clone(),
        }
    }

    /// `{microseconds since the epoch}:{id}` - opaque to clients.
    pub fn encode(&self) -> String {
        format!("{}:{}", self.first_failed_at.timestamp_micros(), self.id)
    }

    pub fn decode(cursor: &str) -> Option<Self> {
        let (micros, id) = cursor.split_once(':')?;
        Some(Self {
            first_failed_at: DateTime::from_timestamp_micros(micros.parse().ok()?)?,
            id: id.to_string(),
        })
    }
}

pub async fn get_parked_delivery(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<ParkedDelivery>> {
    let schema = schema_ident(bounded_context);
    let row: Option<ParkedDeliveryRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PARKED_DELIVERY_COLUMNS} FROM {schema}.parked_deliveries WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(ParkedDelivery::from))
}

/// The `client_id` every `CrossContextRoute` submission runs under -
/// server-derived, never caller-suppliable (see
/// `RESERVED_IDEMPOTENCY_KEY_PREFIX`'s own doc comment).
pub const CROSS_CONTEXT_ROUTE_CLIENT_ID: &str = "cross-context-route";

/// A `CrossContextRoute`-kind parked row's `source` is this prefix plus
/// the route's name; its `identifier` is the source event's sequence.
pub(super) const CROSS_CONTEXT_ROUTE_PARKED_SOURCE_PREFIX: &str = "cross-context-route:";

/// The idempotency key one route's submission for one source event runs
/// under - shared by `catch_up_cross_context_route` and
/// [`parked_delivery_redrive_identity`], so a redrive of a parked
/// occurrence dedupes against the original attempt if that attempt did
/// commit after all.
pub(super) fn cross_context_route_idempotency_key(
    route_name: &str,
    source_sequence: &str,
) -> String {
    format!(
        "{}{route_name}:{source_sequence}",
        crate::event_store::RESERVED_IDEMPOTENCY_KEY_PREFIX
    )
}

/// The `(client_id, idempotency_key)` `retryParkedDelivery` redrives a
/// command-kind [`ParkedDelivery`] under, so that however many times it
/// is redriven (a retry whose submission committed but whose row delete
/// then failed, or one racing a lost lock) the command lands once:
///
/// - `CrossContextRoute`: the route's own client id and the exact key its
///   original attempt used. An attempt that failed on the caller's side
///   but committed anyway (a dropped connection after `COMMIT`) is then
///   a `Deduplicated` no-op on redrive, not a second command.
/// - `CommandTrigger`: the token's own client id (`caller_client_id`)
///   and `original_idempotency_key` - the `Idempotency-Key` the bridge's
///   original request carried, when it reported one - so an original
///   attempt that committed after all dedupes too. Without one, a key
///   reserved to this row (`RESERVED_PARKED_DELIVERY_IDEMPOTENCY_KEY_PREFIX`),
///   which only dedupes across redrives.
///
/// - `Deadline`: the deadline's own client id and reserved key, exactly
///   as `fire_due_deadlines` submitted it (docs/architecture.md §115).
///
/// `None` for `ExternalEvent`, which has no idempotency key - see
/// [`parked_delivery_redrive_dedupe_partition_key`] for its equivalent.
pub fn parked_delivery_redrive_identity(
    delivery: &ParkedDelivery,
    caller_client_id: &str,
    original_idempotency_key: Option<&str>,
) -> Option<(String, String)> {
    match delivery.kind {
        ParkedDeliveryKind::CrossContextRoute => {
            match delivery
                .source
                .strip_prefix(CROSS_CONTEXT_ROUTE_PARKED_SOURCE_PREFIX)
            {
                Some(route_name) => Some((
                    CROSS_CONTEXT_ROUTE_CLIENT_ID.to_string(),
                    cross_context_route_idempotency_key(route_name, &delivery.identifier),
                )),
                // Only `catch_up_cross_context_route` writes this kind,
                // always with that prefix; kept total rather than
                // panicking on a hand-edited row.
                None => Some((
                    CROSS_CONTEXT_ROUTE_CLIENT_ID.to_string(),
                    format!(
                        "{}{}",
                        crate::event_store::RESERVED_PARKED_DELIVERY_IDEMPOTENCY_KEY_PREFIX,
                        delivery.id
                    ),
                )),
            }
        }
        ParkedDeliveryKind::CommandTrigger => Some((
            caller_client_id.to_string(),
            match original_idempotency_key {
                Some(key) => key.to_string(),
                None => format!(
                    "{}{}",
                    crate::event_store::RESERVED_PARKED_DELIVERY_IDEMPOTENCY_KEY_PREFIX,
                    delivery.id
                ),
            },
        )),
        // The deadline's own reserved key: an attempt that failed on this
        // side but committed anyway dedupes, and so do repeated redrives.
        ParkedDeliveryKind::Deadline => Some((
            DEADLINE_CLIENT_ID.to_string(),
            format!(
                "{}{}",
                crate::event_store::RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX,
                delivery.identifier
            ),
        )),
        ParkedDeliveryKind::ExternalEvent => None,
    }
}

/// The `dedupe` partition every `ExternalEvent`-kind redrive uses,
/// always at sequence `1`: one partition per parked row, under the row's
/// own token (the watermark's `adapter_id`). The first redrive lands and
/// sets the watermark; any later redrive of the same row - one whose
/// event committed but whose row delete then failed - is `Redelivered`,
/// not a second event. The `ExternalEvent` counterpart of
/// [`parked_delivery_redrive_identity`]'s idempotency keys. Leaves one
/// `external_message_cursors` row per redrive, which is bounded by how
/// often operators retry parked deliveries.
///
/// Used even when the bridge's request carried a cursor of its own
/// (issue #58). By the time a parked message is retried, the
/// bridge has committed past it and kept recording that partition, so
/// the partition's watermark is usually beyond the parked sequence, and
/// redriving under the original cursor was deduped away - nothing
/// created, the row deleted as a success. The cost: an original attempt
/// that committed without the bridge hearing back, then got parked, is
/// created a second time on retry. The watermark keeps only the highest
/// sequence per partition, so that case can't be told apart from a
/// message later ones overtook (docs/architecture.md §173).
pub fn parked_delivery_redrive_dedupe_partition_key(delivery: &ParkedDelivery) -> String {
    format!(
        "{}{}",
        crate::event_store::RESERVED_PARKED_DELIVERY_IDEMPOTENCY_KEY_PREFIX,
        delivery.id
    )
}

/// `retryParkedDelivery`'s serialization: a lock-only transaction holding
/// a `pg_try_advisory_xact_lock` on this one parked row, the same
/// lock-only-transaction shape `catch_up_cross_context_route` uses per
/// route (docs/architecture.md §56). The caller re-reads the row *after*
/// this returns and keeps the transaction alive until its redrive and the
/// row's delete (or failure record) are done; dropping or committing it
/// releases the lock. Without it, two retries of one row both read it,
/// both redrive, and the delivery lands twice.
///
/// `None` when another retry of the same row holds the lock. Non-blocking
/// on purpose: a waiter would pin a pooled connection while the holder
/// still needs connections for its own redrive, and for an operator
/// action "already being retried" is a better answer than a queue.
pub async fn try_lock_parked_delivery_for_retry(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<Transaction<'static, Postgres>>> {
    let mut lock_tx = pool.begin().await?;
    let acquired: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1)::bigint)")
            .bind(format!("parked_delivery:{bounded_context}:{id}"))
            .fetch_one(&mut *lock_tx)
            .await?;
    Ok(acquired.then_some(lock_tx))
}

/// `retryParkedDelivery`'s own failed-again path - the row stays parked,
/// but its own `error`/`attempt_count`/`last_failed_at` reflect this
/// latest attempt rather than only the original one, so an operator
/// looking at the list sees it was actually retried, not just left
/// alone.
pub async fn record_parked_delivery_retry_failure(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
    error: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.parked_deliveries \
         SET error = $1, attempt_count = attempt_count + 1, last_failed_at = $2 \
         WHERE id = $3"
    )))
    .bind(error)
    .bind(now)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// `forgetSubject`'s parked-delivery half (docs/architecture.md §91):
/// deletes every parked delivery in `bounded_context` whose request
/// references `(subject_key, subject_value)`, returning how many went. A
/// parked `request_json` is the plaintext request as it was submitted -
/// never encrypted - so destroying the subject's key leaves it readable to
/// any admin, and `retryParkedDelivery` would re-create the subject's data
/// under a fresh key. Called *before* the key is destroyed: if this failed
/// afterwards, `forgetSubject` couldn't be retried (the key is no longer
/// active) and the rows would stay.
///
/// A row references the subject when the payload its redrive would submit
/// names it through the target type's own sensitive fields - exactly the
/// subjects encryption would key it under (`sensitive_field_subjects`).
/// A row whose target type can't be resolved (its token or bounded
/// context gone) or whose request isn't the redrive's shape can't be
/// redriven anyway; it is deleted if the subject value appears anywhere in
/// its request, erring toward erasure.
pub async fn delete_parked_deliveries_for_subject(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
) -> crate::error::Result<u64> {
    let mut deleted = 0;
    let mut after: Option<ParkedDeliveryCursor> = None;
    let mut rules_cache = ParkedPayloadRulesCache::default();
    loop {
        let page = list_parked_deliveries_page(pool, bounded_context, after.as_ref(), 1000).await?;
        let Some(last) = page.last() else {
            return Ok(deleted);
        };
        after = Some(ParkedDeliveryCursor::of(last));
        for delivery in &page {
            if parked_delivery_references_subject(
                pool,
                &mut rules_cache,
                delivery,
                subject_key,
                subject_value,
            )
            .await?
                && delete_parked_delivery(pool, bounded_context, &delivery.id)
                    .await?
                    .is_some()
            {
                deleted += 1;
            }
        }
    }
}

pub(super) async fn parked_delivery_references_subject(
    pool: &Pool,
    rules_cache: &mut ParkedPayloadRulesCache,
    delivery: &ParkedDelivery,
    subject_key: &str,
    subject_value: &str,
) -> crate::error::Result<bool> {
    let rules = rules_cache.get(pool, delivery).await?;
    Ok(match (rules, parked_delivery_payload(delivery)) {
        (Some(rules), Some(payload)) => {
            let payload = serde_json::to_string(payload)
                .expect("serde_json::Value serialization is infallible");
            crate::event_store::sensitive_field_subjects(&rules.sensitive_fields, &payload)
                .iter()
                .any(|(key, value)| key == subject_key && value == subject_value)
        }
        _ => json_mentions(&delivery.request_json, subject_value),
    })
}

/// What governs a parked delivery's payload: the declarations of the type
/// its redrive would submit it as - the target `CommandType` of a route
/// or deadline, the token's `EventType` for an external event, the
/// token's `CommandType` for a trigger.
#[derive(Debug, Clone)]
pub struct ParkedPayloadRules {
    pub sensitive_fields: Vec<SensitiveField>,
    pub private_fields: Vec<PrivateField>,
    pub tag_mappings: Vec<TagMapping>,
    pub owner_tag_key: Option<String>,
}

/// [`ParkedPayloadRules`] for `delivery`, or `None` when its type can't be
/// resolved any more (its token, bounded context or type is gone) - such
/// a row can't be redriven either.
pub async fn parked_delivery_payload_rules(
    pool: &Pool,
    delivery: &ParkedDelivery,
) -> crate::error::Result<Option<ParkedPayloadRules>> {
    let from_command = |ct: CommandType| ParkedPayloadRules {
        sensitive_fields: ct.sensitive_fields,
        private_fields: ct.private_fields,
        tag_mappings: ct.tag_mappings,
        owner_tag_key: ct.owner_tag_key,
    };
    Ok(match delivery.kind {
        ParkedDeliveryKind::CrossContextRoute | ParkedDeliveryKind::Deadline => match (
            &delivery.target_bounded_context,
            &delivery.target_command_type,
        ) {
            (Some(bc), Some(name)) => get_command_type(pool, bc, name).await?.map(from_command),
            _ => None,
        },
        ParkedDeliveryKind::ExternalEvent => match &delivery.access_token_id {
            Some(id) => get_external_event_token(pool, id).await?.map(|token| {
                let et = token.event_type;
                ParkedPayloadRules {
                    sensitive_fields: et.sensitive_fields,
                    private_fields: et.private_fields,
                    tag_mappings: et.tag_mappings,
                    owner_tag_key: et.owner_tag_key,
                }
            }),
            None => None,
        },
        ParkedDeliveryKind::CommandTrigger => match &delivery.access_token_id {
            Some(id) => get_command_token(pool, id)
                .await?
                .map(|token| from_command(token.command_type)),
            None => None,
        },
    })
}

/// [`parked_delivery_payload_rules`], memoized for the length of one
/// scan (docs/architecture.md §153). The rules depend only on what a row
/// targets - its route's or deadline's command type, or its bridge's
/// token - and the rows one bridge or route parks share that, so a scan
/// over thousands of rows needs a handful of lookups rather than a few
/// queries per row. `parkedDeliveries` filling a page for a scoped grant,
/// and `forgetSubject` sweeping every row, both walk arbitrarily many.
#[derive(Default)]
pub struct ParkedPayloadRulesCache {
    pub(super) rules: std::collections::HashMap<String, Option<ParkedPayloadRules>>,
}

impl ParkedPayloadRulesCache {
    pub async fn get(
        &mut self,
        pool: &Pool,
        delivery: &ParkedDelivery,
    ) -> crate::error::Result<Option<ParkedPayloadRules>> {
        let key = match delivery.kind {
            ParkedDeliveryKind::CrossContextRoute | ParkedDeliveryKind::Deadline => format!(
                "type:{:?}:{:?}",
                delivery.target_bounded_context, delivery.target_command_type
            ),
            ParkedDeliveryKind::ExternalEvent => {
                format!("event-token:{:?}", delivery.access_token_id)
            }
            ParkedDeliveryKind::CommandTrigger => {
                format!("command-token:{:?}", delivery.access_token_id)
            }
        };
        if let Some(rules) = self.rules.get(&key) {
            return Ok(rules.clone());
        }
        let rules = parked_delivery_payload_rules(pool, delivery).await?;
        self.rules.insert(key, rules.clone());
        Ok(rules)
    }
}

/// The payload inside a parked delivery's request: the request itself for
/// a route or deadline, its `payload` field for an external event or a
/// trigger.
pub fn parked_delivery_payload(delivery: &ParkedDelivery) -> Option<&serde_json::Value> {
    match delivery.kind {
        ParkedDeliveryKind::CrossContextRoute | ParkedDeliveryKind::Deadline => {
            Some(&delivery.request_json)
        }
        ParkedDeliveryKind::ExternalEvent | ParkedDeliveryKind::CommandTrigger => {
            delivery.request_json.get("payload")
        }
    }
}

/// Whether `access_mapping` may see and handle `delivery`
/// (docs/architecture.md §119): always for an unscoped grant; for a
/// scoped one, only when the payload's owner - its owner tag, derived as
/// a stored record's would be - is the grant's scope. A type declaring no
/// owner tag is unowned and visible, as its events are to `queryEvents`.
/// A row whose type can't be resolved (`rules` is `None`) or whose payload
/// is missing fails closed.
pub fn parked_delivery_visible_to(
    delivery: &ParkedDelivery,
    rules: Option<&ParkedPayloadRules>,
    access_mapping: &RoleAccessMapping,
) -> bool {
    if access_mapping.scope.is_none() {
        return true;
    }
    let (Some(rules), Some(payload)) = (rules, parked_delivery_payload(delivery)) else {
        return false;
    };
    let payload =
        serde_json::to_string(payload).expect("serde_json::Value serialization is infallible");
    crate::event_store::tag_owner_scope_satisfied(
        &crate::event_store::derive_plain_tags(&rules.tag_mappings, &payload),
        rules.owner_tag_key.as_deref(),
        access_mapping.scope.as_deref(),
    )
}

/// `delivery` as `access_mapping` is shown it (docs/architecture.md
/// §119). The stored request is never encrypted - the write it carries
/// failed - so its protected leaves are masked here instead: a sensitive
/// field is `null` unless `sensitive_field_is_granted` for its subject,
/// and a private field is always `null`, since a parked request has no
/// author Role to be entitled to it (REST's fail-closed
/// `redact_private_fields` treatment). With its type unresolvable, the
/// whole payload is `null`: nothing says which leaves are protected. The
/// stored row itself is untouched; a redrive submits it in full.
pub fn render_parked_delivery(
    delivery: &ParkedDelivery,
    rules: Option<&ParkedPayloadRules>,
    access_mapping: &RoleAccessMapping,
) -> ParkedDelivery {
    let mut rendered = delivery.clone();
    let payload = match rendered.kind {
        ParkedDeliveryKind::CrossContextRoute | ParkedDeliveryKind::Deadline => {
            Some(&mut rendered.request_json)
        }
        ParkedDeliveryKind::ExternalEvent | ParkedDeliveryKind::CommandTrigger => {
            rendered.request_json.get_mut("payload")
        }
    };
    let Some(payload) = payload else {
        return rendered;
    };
    let Some(rules) = rules else {
        *payload = serde_json::Value::Null;
        return rendered;
    };
    let original = payload.clone();
    for sf in &rules.sensitive_fields {
        let subject = crate::event_store::payload_field_value(&original, &sf.subject_field)
            .and_then(crate::event_store::json_scalar_to_string);
        let granted = subject.is_some_and(|subject| {
            crate::event_store::sensitive_field_is_granted(access_mapping, &subject)
        });
        if !granted {
            if let Some(leaf) = crate::event_store::payload_field_value_mut(payload, &sf.field) {
                *leaf = serde_json::Value::Null;
            }
        }
    }
    for pf in &rules.private_fields {
        if let Some(leaf) = crate::event_store::payload_field_value_mut(payload, &pf.field) {
            *leaf = serde_json::Value::Null;
        }
    }
    rendered
}

/// `forgetSubject`'s deadline half (docs/architecture.md §132): every
/// still-`pending` deadline targeting `bounded_context` whose payload
/// names the subject is resolved as `forgotten`, its payload cleared. A
/// deadline stores its target command's payload in plaintext until it
/// fires - outside any key's reach - and firing after the subject was
/// forgotten would submit that data again under a fresh key. Deadlines
/// live with their schedule's source bounded context, not their target,
/// so every bounded context's `deadlines` table is walked, a page at a
/// time. A row names the subject when its payload does through the
/// target command type's sensitive fields (`sensitive_field_subjects`),
/// or - when that type is gone, or the payload doesn't parse - when the
/// subject value appears anywhere in it, erring toward erasure as
/// [`delete_parked_deliveries_for_subject`] does. A row already `firing`
/// is past stopping and left alone; its payload is cleared when it
/// resolves. The resolved row stays, as a trace that a deadline was
/// forgotten rather than fired or cancelled. A row resolved (fired,
/// cancelled, parked) before resolution cleared its payload still holds
/// it, and has it cleared here too, keeping its status
/// (docs/architecture.md §162). Returns how many pending deadlines were
/// resolved as `forgotten`.
pub async fn forget_subject_in_deadlines(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<u64> {
    let mut forgotten = 0;
    let mut sensitive_fields = std::collections::HashMap::new();
    for holder in list_bounded_contexts(pool).await? {
        match forget_subject_in_holder_deadlines(
            pool,
            &holder.name,
            bounded_context,
            subject_key,
            subject_value,
            now,
            &mut sensitive_fields,
        )
        .await
        {
            Ok(n) => forgotten += n,
            // docs/architecture.md §157: deleted since the listing - its
            // deadlines went with it.
            Err(_) if get_bounded_context(pool, &holder.name).await?.is_none() => {}
            Err(err) => return Err(err),
        }
    }
    Ok(forgotten)
}

/// [`forget_subject_in_deadlines`] for the deadlines one bounded context,
/// `holder`, stores.
pub(super) async fn forget_subject_in_holder_deadlines(
    pool: &Pool,
    holder: &str,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    now: DateTime<Utc>,
    sensitive_fields: &mut std::collections::HashMap<String, Option<Vec<SensitiveField>>>,
) -> crate::error::Result<u64> {
    const PAGE: i64 = 1000;
    let schema = schema_ident(holder);
    let mut forgotten = 0;
    let mut after = String::new();
    loop {
        // Pending rows, and resolved ones still holding a payload - a row
        // resolved before resolution cleared it (docs/architecture.md §162).
        let rows: Vec<(String, String, String, String)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT id, target_command_type, payload, status FROM {schema}.deadlines \
                 WHERE target_bounded_context = $1 AND id > $2 \
                 AND (status = 'pending' OR (status <> 'firing' AND payload <> '{{}}')) \
                 ORDER BY id LIMIT $3"
            )))
            .bind(bounded_context)
            .bind(&after)
            .bind(PAGE)
            .fetch_all(pool)
            .await?;
        let Some((last_id, _, _, _)) = rows.last() else {
            break;
        };
        after = last_id.clone();
        for (id, command_type, payload, status) in &rows {
            if !sensitive_fields.contains_key(command_type) {
                let fields = get_command_type(pool, bounded_context, command_type)
                    .await?
                    .map(|ct| ct.sensitive_fields);
                sensitive_fields.insert(command_type.clone(), fields);
            }
            let parsed = serde_json::from_str::<serde_json::Value>(payload).ok();
            let names_subject = match (&sensitive_fields[command_type], &parsed) {
                (Some(fields), Some(_)) => {
                    crate::event_store::sensitive_field_subjects(fields, payload)
                        .iter()
                        .any(|(key, value)| key == subject_key && value == subject_value)
                }
                (_, Some(json)) => json_mentions(json, subject_value),
                (_, None) => payload.contains(subject_value),
            };
            if names_subject && status == "pending" {
                forgotten += sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {schema}.deadlines \
                     SET status = 'forgotten', payload = '{{}}', resolved_at = $1 \
                     WHERE id = $2 AND status = 'pending'"
                )))
                .bind(now)
                .bind(id)
                .execute(pool)
                .await?
                .rows_affected();
            } else if names_subject {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {schema}.deadlines SET payload = '{{}}' \
                     WHERE id = $1 AND status NOT IN ('pending', 'firing')"
                )))
                .bind(id)
                .execute(pool)
                .await?;
            }
        }
        if (rows.len() as i64) < PAGE {
            break;
        }
    }
    Ok(forgotten)
}

/// Whether `value` appears as a string leaf, or a number/bool rendered as
/// a string, anywhere in `json`.
pub(super) fn json_mentions(json: &serde_json::Value, value: &str) -> bool {
    match json {
        serde_json::Value::String(s) => s == value,
        serde_json::Value::Number(n) => n.to_string() == value,
        serde_json::Value::Bool(b) => b.to_string() == value,
        serde_json::Value::Array(items) => items.iter().any(|v| json_mentions(v, value)),
        serde_json::Value::Object(fields) => fields.values().any(|v| json_mentions(v, value)),
        serde_json::Value::Null => false,
    }
}

/// `discardParkedDelivery`'s own write, and `retryParkedDelivery`'s own
/// success path (a delivery that finally landed is no longer "stuck",
/// so it leaves this table the same way a resolved
/// `discardProjectionRebuild` row does - no separate `resolved`/
/// `discarded` status to track, `delete_projection_rebuild`'s own
/// register). Returns the row as it was immediately before deletion, the
/// same "hand back what's gone" treatment `discard_projection_rebuild`'s
/// own doc comment gives for the identical reason: the caller (a GraphQL
/// mutation) still needs to render it in its response.
pub async fn delete_parked_delivery(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<ParkedDelivery>> {
    let existing = get_parked_delivery(pool, bounded_context, id).await?;
    if existing.is_none() {
        return Ok(None);
    }
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {schema}.parked_deliveries WHERE id = $1"
    )))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(existing)
}

/// One catch-up tick for one registered [`crate::plugin::CrossContextRoute`] -
/// called in a loop, on a timer, by the single shared background task
/// `SkiljBuilder::build()` spawns (docs/architecture.md's own write-up
/// of this pass), the same "one shared task, not one per route" register
/// `catch_up_bounded_context`/`catch_up_snapshots` already use.
///
/// Fetches every `Source` occurrence since this route's own cursor
/// (`list_events_cached`, already filtered to `Source`'s own event type -
/// the identical read path `EventReadToken`-based REST consumption
/// already uses), and for each one, in sequence order: asks the
/// type-erased dispatcher to translate the payload
/// (`CrossContextRouteDispatcher::route`), submits `Target`'s own
/// command through [`decide_and_submit_command`] when it produces one,
/// then advances the cursor to that occurrence's own sequence
/// regardless of the outcome - a skip, a rejection, and an accepted
/// submission are all equally "this occurrence is done", only a real
/// `Err` (a transient failure worth retrying next tick) leaves the
/// cursor where it was, stopping this route's own catch-up for this
/// tick without losing anything (the next tick re-fetches from the same
/// cursor).
///
/// **First-ever tick, `CrossContextRoute::START_FROM != Beginning`**:
/// when `get_cross_context_route_cursor` comes back `None` (no row -
/// this route has never ticked before) and the route asks to start
/// anywhere other than `Beginning`, this tick does no dispatching at
/// all. It instead loads `Source`'s own occurrences once (the identical
/// `list_events_cached` call the normal path below makes, from `-1`) -
/// needed to compute `Latest`'s/`AtTime`'s own seed, paid unconditionally
/// even for `AtSequence` (which doesn't need it) for one shared code
/// path rather than a fourth special case - purely to find the seed
/// sequence, writes the cursor row there (or leaves it at `-1`,
/// unchanged, if there's nothing to seed past), and returns - so every
/// occurrence that already existed at registration time is treated as
/// "already seen" without a single `route()` call or `Target` submission
/// for any of them:
/// - `Latest`: the highest sequence among every loaded occurrence.
/// - `AtSequence(n)`: `n` directly - no need to look at `events` at all,
///   the same "opaque, unvalidated value" treatment
///   `EventReadToken.start_at_sequence` gets.
/// - `AtTime(unix_secs)`: the highest sequence among occurrences whose
///   own `metadata.created_at` is at or before that moment - the
///   identical `at_time_position` computation `event_store::consume_events`
///   makes, minus the scope filter (`CrossContextRoute` has no scope
///   concept to filter by).
///
/// This is the mechanism that stops a brand-new route like
/// `UserRegistered -> SendWelcomeEmail` from emailing every user who has
/// ever registered: exactly one full history load, exactly once, ever,
/// for this route - every later tick reads the real cursor row this
/// seeded and only ever sees genuinely new occurrences, the same as a
/// `Beginning` route always has.
///
/// **issue #21**: `decide_and_submit_command`'s own `Err` used
/// to propagate straight out of this function via a bare `?`, which
/// stopped this tick immediately without advancing the cursor - correct
/// as far as it went (a crash-safe "the next tick retries the identical
/// occurrence"), but with no cap: a persistently-failing occurrence
/// blocked this route's cursor forever, retried on every single poll
/// tick, unthrottled. Replaced with `retry_policy` (`skilj_retry::
/// RetryPolicy`): each failure grows the backoff before this route's
/// blocked head-of-line occurrence is attempted again (the "not yet
/// time" early return below, backed by `cross_context_route_cursors`'
/// own new `retry_*` columns), and once the policy is exhausted, the
/// occurrence is recorded as a [`ParkedDelivery`] (`source:
/// "cross-context-route:{route.name}"`, `kind: CrossContextRoute`) and
/// the cursor finally advances past it - the route un-blocks, and an
/// operator gets a visible, retryable/discardable record instead of a
/// route silently stuck forever.
///
/// **issue #25 review (docs/architecture.md §56)**: this whole
/// tick - reading `retry_attempt_count`, deciding whether the policy is
/// exhausted, and either recording another backoff or parking - is a
/// plain read-then-write with no lock of its own, so two instances
/// polling the same route concurrently could both read the same
/// almost-exhausted retry state, both independently exhaust it, and both
/// call `insert_parked_delivery` for the identical occurrence: two rows
/// for one real failure. Closed by serializing the *entire* tick per
/// `route_name` with a blocking `pg_advisory_xact_lock` - the identical
/// primitive `lock_read_cursor_for_consume` already uses for its own
/// claim race, just held on a dedicated lock-only transaction here
/// rather than the transaction the protected work itself runs in: this
/// function's own work already spans many independent pooled calls
/// (`decide_and_submit_command` alone opens and commits its own nested
/// transaction), so threading one shared transaction through all of it
/// would be a far larger change than this fix calls for. A lock held on
/// its own connection for the tick's whole duration serializes callers
/// just as effectively, since what needs protecting is "only one
/// instance runs this route's tick at a time," not any single row.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(route = %route.name))]
pub async fn catch_up_cross_context_route(
    pool: &Pool,
    route: &crate::plugin::CrossContextRouteInfo,
    route_dispatcher: &dyn crate::plugin::CrossContextRouteDispatcher,
    command_dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    key_service: Option<&KeyService>,
    retry_policy: &skilj_retry::RetryPolicy,
) -> crate::error::Result<()> {
    // Held for this whole call's duration on a dedicated transaction
    // that carries no application writes of its own - `pg_advisory_xact_lock`
    // releases automatically when it commits (the success path) or rolls
    // back (dropped on an early `?` return), so there's no separate
    // unlock call to forget on any exit path. See this function's own
    // doc comment above for why the lock's connection is deliberately
    // not the same one the tick's own reads/writes use.
    let mut lock_tx = pool.begin().await?;
    let lock_key = format!(
        "cross_context_route:{}:{}",
        route.source_bounded_context, route.name
    );
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1)::bigint)")
        .bind(&lock_key)
        .execute(&mut *lock_tx)
        .await?;
    let result = catch_up_cross_context_route_locked(
        pool,
        route,
        route_dispatcher,
        command_dispatcher,
        projection_dispatcher,
        snapshot_dispatcher,
        broadcaster,
        event_cache,
        key_service,
        retry_policy,
    )
    .await;
    lock_tx.commit().await?;
    result
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn catch_up_cross_context_route_locked(
    pool: &Pool,
    route: &crate::plugin::CrossContextRouteInfo,
    route_dispatcher: &dyn crate::plugin::CrossContextRouteDispatcher,
    command_dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    key_service: Option<&KeyService>,
    retry_policy: &skilj_retry::RetryPolicy,
) -> crate::error::Result<()> {
    let existing_cursor =
        get_cross_context_route_cursor(pool, route.source_bounded_context, route.name).await?;
    let cursor = existing_cursor.unwrap_or(-1);
    if existing_cursor.is_none()
        && route.start_from != crate::plugin::CrossContextRouteStartFrom::Beginning
    {
        // First-ever tick: seed the cursor and dispatch nothing. The
        // seed is one `MAX(sequence)`, not a scan of the loaded history.
        let seed = match route.start_from {
            crate::plugin::CrossContextRouteStartFrom::Beginning => {
                unreachable!("excluded by this branch's own condition above")
            }
            crate::plugin::CrossContextRouteStartFrom::Latest => {
                highest_sequence_of_type(
                    pool,
                    route.source_bounded_context,
                    route.source_event_type,
                    None,
                )
                .await?
            }
            crate::plugin::CrossContextRouteStartFrom::AtSequence(n) => n,
            crate::plugin::CrossContextRouteStartFrom::AtTime(unix_secs) => {
                let threshold = DateTime::from_timestamp(unix_secs, 0).unwrap_or(Utc::now());
                highest_sequence_of_type(
                    pool,
                    route.source_bounded_context,
                    route.source_event_type,
                    Some(threshold),
                )
                .await?
            }
        };
        update_cross_context_route_cursor(
            pool,
            route.source_bounded_context,
            route.name,
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
        route.source_bounded_context,
        route.source_event_type,
        cursor,
        MAX_EVENTS_PER_CATCH_UP_TICK,
    )
    .await?;

    // issue #21 - see this function's own doc comment. Read
    // once per tick, ahead of the loop: only the loop's own first
    // occurrence can ever be a retry of a previously-failed one, since
    // the cursor never advances past a blocked occurrence - the same
    // invariant `cross_context_route_cursors` needing only one row's
    // worth of retry state (not one per occurrence) already relies on.
    let (mut retry_attempt, mut retry_first_failed_at, retry_next_attempt_at) =
        get_cross_context_route_retry_state(pool, route.source_bounded_context, route.name).await?;
    // Not gated on `retry_attempt > 0`: a deferral to another instance
    // (docs/architecture.md §161) sets a next attempt without spending
    // one. Advancing the cursor clears it.
    if let Some(next_attempt_at) = retry_next_attempt_at {
        if Utc::now() < next_attempt_at {
            // Not yet time - this route's own blocked occurrence is
            // still in backoff. Skip this tick entirely rather than
            // re-attempting the identical failing submission on
            // every single poll.
            return Ok(());
        }
    }

    for event in &events {
        match route_dispatcher.route(route.name, &event.payload) {
            None => {
                // Defensive only - `route.name` came from this same
                // dispatcher's own `routes()` list, so this should never
                // actually happen. Still advances the cursor below
                // rather than looping on it forever.
                tracing::warn!(
                    sequence = event.sequence,
                    "cross-context route not found in its own dispatcher - skipping"
                );
            }
            Some(Err(e)) => {
                // The stored payload didn't deserialize into `Source::
                // Payload` - see `BoundedContextEvent::try_from_event`'s
                // own doc comment on why this is reachable. Retrying can
                // never fix a payload the event itself was stored with,
                // so this is logged and skipped, not retried forever.
                tracing::warn!(
                    sequence = event.sequence,
                    error = %e,
                    "cross-context route: source payload did not deserialize - skipping"
                );
            }
            Some(Ok(None)) => {
                // `route()` itself decided this occurrence doesn't
                // apply - a real, expected outcome, not an error.
            }
            Some(Ok(Some(target_payload))) => {
                let Some(target_command_type) = get_command_type(
                    pool,
                    route.target_bounded_context,
                    route.target_command_type,
                )
                .await?
                else {
                    tracing::warn!(
                        sequence = event.sequence,
                        target_bounded_context = route.target_bounded_context,
                        target_command_type = route.target_command_type,
                        "cross-context route's own target CommandType isn't registered - \
                         skipping this occurrence, cursor still advances"
                    );
                    update_cross_context_route_cursor(
                        pool,
                        route.source_bounded_context,
                        route.name,
                        event.sequence,
                        Utc::now(),
                    )
                    .await?;
                    retry_attempt = 0;
                    retry_first_failed_at = None;
                    continue;
                };
                // Prefixed with `RESERVED_IDEMPOTENCY_KEY_PREFIX` - a
                // Historical note (docs/architecture.md §36/§37): this
                // prefix originally existed because `idempotency_keys`
                // had no caller/client_id column at all, so an ordinary
                // Write-level caller could pre-plant `"{route.name}:{sequence}"`
                // via `submitCommand`/`Idempotency-Key` ahead of time and
                // silently swallow this submission as a `Deduplicated`
                // no-op. `idempotency_keys` is `client_id`-scoped now (this
                // call's own `client_id` below is always `"cross-context-route"`,
                // never externally suppliable), which closes that gap
                // structurally - this reservation is kept as a harmless
                // second layer, not the load-bearing defense it was.
                let idempotency_key =
                    cross_context_route_idempotency_key(route.name, &event.sequence.to_string());
                // issue #18: the routed command finally gets a
                // real answer to "what caused this" - forward-carrying
                // the source event's own correlation_id (always present
                // by this point) and naming the source event itself as
                // the direct cause, via `event_causation_id`'s composed
                // `{bounded_context}:{sequence}` string (`Event` has no
                // synthetic id of its own to use instead - see that
                // helper's own doc comment).
                // issue #98: an approval-required target becomes a
                // proposal here too, never a direct submission - otherwise
                // whoever can cause the source event bypasses the approver.
                // `Ok(None)` is "proposed"; errors take the same retry/park
                // path as a failed submission.
                let causation_id = crate::event_store::event_causation_id(event);
                let outcome = match propose_if_approval_required(
                    pool,
                    &target_command_type,
                    &target_payload,
                    CROSS_CONTEXT_ROUTE_CLIENT_ID,
                    event.metadata.correlation_id.as_deref(),
                    Some(&causation_id),
                    &idempotency_key,
                    key_service,
                    Utc::now(),
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
                        &target_payload,
                        CROSS_CONTEXT_ROUTE_CLIENT_ID,
                        event.metadata.correlation_id.as_deref(),
                        Some(&causation_id),
                        key_service,
                        Utc::now(),
                        Some(&idempotency_key),
                        None,
                    )
                    .await
                    .map(Some),
                    Err(e) => Err(e),
                };
                // issue #21 - see this function's own doc
                // comment. `Err` no longer propagates straight out via
                // `?`; it's retried with backoff, up to `retry_policy`,
                // before this occurrence is parked.
                let outcome = match outcome {
                    Ok(outcome) => outcome,
                    Err(e) if e.another_instance_can_do_it() => {
                        // docs/architecture.md §161: no attempt spent, never
                        // parked - the route waits for an instance that can.
                        let next_attempt_at = Utc::now()
                            + chrono::Duration::from_std(ANOTHER_INSTANCE_RETRY_DELAY)
                                .expect("a ten-second delay fits a chrono::Duration");
                        tracing::warn!(
                            sequence = event.sequence,
                            error = %e,
                            next_attempt_at = %next_attempt_at,
                            "cross-context route: this instance can't submit the target \
                             command, another can - retrying later; route blocked until then"
                        );
                        defer_cross_context_route(
                            pool,
                            route.source_bounded_context,
                            route.name,
                            cursor,
                            next_attempt_at,
                        )
                        .await?;
                        return Ok(());
                    }
                    Err(e) => {
                        retry_attempt += 1;
                        // Every path out of this `Err` arm either
                        // `continue`s (after resetting `retry_attempt`/
                        // `retry_first_failed_at` to their defaults - the
                        // occurrence is done, parked) or `return`s (the
                        // function exits before the outer
                        // `retry_first_failed_at` would ever be read
                        // again) - so unlike `retry_attempt`, there's no
                        // corresponding outer reassignment needed for
                        // `first_failed_at` here, only this local,
                        // already-resolved value.
                        let first_failed_at = retry_first_failed_at.unwrap_or_else(Utc::now);
                        let now = Utc::now();
                        let elapsed = (now - first_failed_at).to_std().unwrap_or_default();
                        if retry_policy.is_exhausted(retry_attempt as u32, elapsed) {
                            tracing::error!(
                                sequence = event.sequence,
                                error = %e,
                                attempt = retry_attempt,
                                "cross-context route: target command submission failed \
                                 repeatedly - parking and skipping this occurrence"
                            );
                            let request_json = serde_json::from_str(&target_payload)
                                .unwrap_or(serde_json::Value::String(target_payload));
                            insert_parked_delivery(
                                pool,
                                route.target_bounded_context,
                                &format!(
                                    "{CROSS_CONTEXT_ROUTE_PARKED_SOURCE_PREFIX}{}",
                                    route.name
                                ),
                                ParkedDeliveryKind::CrossContextRoute,
                                &event.sequence.to_string(),
                                None,
                                Some(route.target_bounded_context),
                                Some(route.target_command_type),
                                &request_json,
                                &e.to_string(),
                                retry_attempt,
                                first_failed_at,
                                now,
                            )
                            .await?;
                            update_cross_context_route_cursor(
                                pool,
                                route.source_bounded_context,
                                route.name,
                                event.sequence,
                                now,
                            )
                            .await?;
                            retry_attempt = 0;
                            retry_first_failed_at = None;
                            continue;
                        }
                        let next_attempt_at =
                            retry_policy.next_attempt_at(now, retry_attempt as u32);
                        tracing::warn!(
                            sequence = event.sequence,
                            error = %e,
                            attempt = retry_attempt,
                            next_attempt_at = %next_attempt_at,
                            "cross-context route: target command submission failed - will \
                             retry with backoff; route blocked until then"
                        );
                        record_cross_context_route_retry_failure(
                            pool,
                            route.source_bounded_context,
                            route.name,
                            cursor,
                            retry_attempt,
                            first_failed_at,
                            next_attempt_at,
                            now,
                        )
                        .await?;
                        return Ok(());
                    }
                };
                // Defence in depth, not solely a bypass signal: with the
                // prefix reserved, nothing *else* can write into this
                // namespace, but this route's own past attempt at this
                // exact occurrence can - a process crash/restart between
                // this submission committing and the cursor advancing
                // below leaves the cursor pointing at this same source
                // event, so the next tick re-derives the identical key
                // and correctly lands here. That's ordinary, benign
                // crash recovery, not an anomaly - warned rather than
                // errored either way, since the cursor still correctly
                // advances past this occurrence.
                if matches!(outcome, Some(SubmitCommandOutcome::Deduplicated { .. })) {
                    tracing::warn!(
                        sequence = event.sequence,
                        "cross-context route's own idempotency key was already present - \
                         expected after a crash/restart between a prior submission and its \
                         cursor advance; unexpected otherwise, since the key space is reserved"
                    );
                }
            }
        }
        update_cross_context_route_cursor(
            pool,
            route.source_bounded_context,
            route.name,
            event.sequence,
            Utc::now(),
        )
        .await?;
        retry_attempt = 0;
        retry_first_failed_at = None;
    }
    Ok(())
}
