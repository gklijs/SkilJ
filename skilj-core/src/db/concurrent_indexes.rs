//! Indexes on tables that grow with traffic, built and dropped with
//! `CREATE/DROP INDEX CONCURRENTLY` so that writes go on while they do
//! (docs/architecture.md §256).
//!
//! A startup patch's plain `CREATE INDEX` ([`super::create_index_patch`])
//! holds a `SHARE` lock, which blocks every write to the table while the
//! index builds over its rows. That is fine for a table of configuration
//! rows and not for one that grows with traffic. An index added to such a
//! table goes in [`MANAGED_INDEXES`] instead, and is built here: in
//! default mode by startup and `migrate`, after the per-bounded-context
//! patches, and in both modes by the `concurrent_indexes` background task.
//!
//! The catalog is read by columns only (`pg_index`, `pg_am`,
//! `pg_attribute`): `pg_get_indexdef`/`pg_get_expr` take a lock on the
//! table and wait behind any `ACCESS EXCLUSIVE` holder. The background
//! discovery uses only catalog shape. Approval index readiness checks its
//! predicate separately because command submission needs it as an arbiter.

use super::{schema_ident, Pool};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// An index that is built, or dropped, concurrently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexSpec {
    /// The table it is on.
    pub table: &'static str,
    /// Its name.
    pub name: &'static str,
    /// Its key columns, in index order.
    pub key_columns: &'static [&'static str],
    /// Whether the index is unique.
    pub unique: bool,
    /// Its `WHERE` clause.
    pub predicate: &'static str,
}

/// The index the deadline poll claims due deadlines through: its
/// `pending OR stale firing` predicate is implied by this one, which
/// `deadlines_due`'s `pending` alone was not (issue #218).
pub(crate) const DEADLINES_CLAIMABLE: IndexSpec = IndexSpec {
    table: "deadlines",
    name: "deadlines_claimable",
    key_columns: &["fire_at"],
    unique: false,
    predicate: "status IN ('pending', 'firing')",
};

/// The index `deadlines_claimable` replaces, as releases up to 0.4
/// created it.
pub(crate) const DEADLINES_DUE: IndexSpec = IndexSpec {
    table: "deadlines",
    name: "deadlines_due",
    key_columns: &["fire_at"],
    unique: false,
    predicate: "status = 'pending'",
};

pub(super) const OPEN_PROPOSAL_KEY_PREDICATE: &str =
    "status IN ('pending', 'approving') AND idempotency_key IS NOT NULL";

pub(super) const OPEN_PROPOSAL_INDEX: IndexSpec = IndexSpec {
    table: "command_proposals",
    name: "command_proposals_open_idempotency",
    key_columns: &["command_type_name", "metadata_client_id", "idempotency_key"],
    unique: true,
    predicate: OPEN_PROPOSAL_KEY_PREDICATE,
};

const PENDING_PROPOSAL_INDEX: IndexSpec = IndexSpec {
    name: "command_proposals_pending_idempotency",
    predicate: "status = 'pending' AND idempotency_key IS NOT NULL",
    ..OPEN_PROPOSAL_INDEX
};

/// PostgreSQL's expression deparse for the predicate we provision, checked
/// on the supported server majors.
const OPEN_PROPOSAL_INDEX_EXPRESSION: &str =
    "((status = ANY (ARRAY['pending'::text, 'approving'::text])) AND (idempotency_key IS NOT NULL))";

/// Indexes built concurrently, each with the index it replaces, dropped
/// concurrently once the new one is valid. A changed definition needs a
/// new name: an index found under a managed name is never rebuilt for
/// having another predicate.
pub(crate) const MANAGED_INDEXES: &[(IndexSpec, Option<IndexSpec>)] = &[
    (OPEN_PROPOSAL_INDEX, Some(PENDING_PROPOSAL_INDEX)),
    (DEADLINES_CLAIMABLE, Some(DEADLINES_DUE)),
];

impl IndexSpec {
    /// `CREATE INDEX` for it in `schema` (an already-quoted identifier),
    /// concurrently or plainly (provisioning, on an empty table, in a
    /// transaction).
    pub(crate) fn create_sql(&self, schema: &str, concurrently: bool) -> String {
        format!(
            "CREATE {}INDEX {}IF NOT EXISTS {} ON {schema}.{} ({}) WHERE {}",
            if self.unique { "UNIQUE " } else { "" },
            if concurrently { "CONCURRENTLY " } else { "" },
            self.name,
            self.table,
            self.key_columns.join(", "),
            self.predicate
        )
    }
}

/// What the catalog holds under an index's name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IndexState {
    /// Nothing.
    Absent,
    /// The index, usable.
    Valid,
    /// The index, left invalid by an interrupted concurrent build.
    Invalid,
    /// Something else: not an index, on another table, or another shape.
    /// Only a manual change puts it there; it is left alone.
    Foreign,
}

/// One relation under a managed or replaced index name, by catalog columns.
#[derive(Debug, sqlx::FromRow)]
struct IndexRow {
    schema: String,
    name: String,
    relkind: String,
    table_name: Option<String>,
    valid: Option<bool>,
    is_unique: Option<bool>,
    amname: Option<String>,
    natts: Option<i32>,
    no_exprs: Option<bool>,
    partial: Option<bool>,
    columns: Vec<String>,
}

impl IndexRow {
    fn state(&self, spec: &IndexSpec) -> IndexState {
        let shaped = self.relkind == "i"
            && self.table_name.as_deref() == Some(spec.table)
            && self.amname.as_deref() == Some("btree")
            && self.natts == i32::try_from(spec.key_columns.len()).ok()
            && self.no_exprs == Some(true)
            && self.partial == Some(true)
            && self.is_unique == Some(spec.unique)
            && self.columns == spec.key_columns;
        match (shaped, self.valid) {
            (true, Some(true)) => IndexState::Valid,
            (true, _) => IndexState::Invalid,
            (false, _) => IndexState::Foreign,
        }
    }
}

/// Every relation named like a managed or replaced index in `schemas` (raw
/// schema names, `bc_<name>`), read from catalog columns alone, so the
/// read takes no lock on any table.
async fn index_rows<'e>(
    executor: impl sqlx::PgExecutor<'e>,
    schemas: &[String],
) -> crate::error::Result<Vec<IndexRow>> {
    let names: Vec<&str> = MANAGED_INDEXES
        .iter()
        .flat_map(|(managed, replaced)| {
            std::iter::once(managed.name).chain(replaced.map(|r| r.name))
        })
        .collect();
    Ok(sqlx::query_as(
        "SELECT n.nspname::text AS schema, i.relname::text AS name, i.relkind::text AS relkind, \
                t.relname::text AS table_name, x.indisvalid AS valid, x.indisunique AS is_unique, \
                am.amname::text AS amname, x.indnatts::int4 AS natts, \
                (x.indexprs IS NULL) AS no_exprs, (x.indpred IS NOT NULL) AS partial, \
                ARRAY(SELECT a.attname::text \
                        FROM unnest(x.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) \
                        JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = k.attnum \
                       ORDER BY k.ord) AS columns \
           FROM pg_class i \
           JOIN pg_namespace n ON n.oid = i.relnamespace \
           LEFT JOIN pg_index x ON x.indexrelid = i.oid \
           LEFT JOIN pg_class t ON t.oid = x.indrelid \
           LEFT JOIN pg_am am ON am.oid = i.relam \
          WHERE n.nspname = ANY($1) AND i.relname = ANY($2)",
    )
    .bind(schemas)
    .bind(&names)
    .fetch_all(executor)
    .await?)
}

/// The state of every managed and replaced index in each of `schemas`.
#[derive(Debug, Default)]
pub(crate) struct IndexStates(BTreeMap<(String, &'static str), IndexState>);

impl IndexStates {
    async fn read<'e>(
        executor: impl sqlx::PgExecutor<'e>,
        schemas: &[String],
    ) -> crate::error::Result<Self> {
        let rows = index_rows(executor, schemas).await?;
        let mut states = BTreeMap::new();
        for (managed, replaced) in MANAGED_INDEXES {
            for spec in std::iter::once(managed).chain(replaced.iter()) {
                for row in rows.iter().filter(|r| r.name == spec.name) {
                    states.insert((row.schema.clone(), spec.name), row.state(spec));
                }
            }
        }
        Ok(Self(states))
    }

    /// One bounded context's, by raw schema name.
    pub(crate) async fn for_schema(
        conn: &mut sqlx::PgConnection,
        raw_schema: &str,
    ) -> crate::error::Result<Self> {
        Self::read(&mut *conn, &[raw_schema.to_string()]).await
    }

    pub(crate) fn get(&self, raw_schema: &str, spec: &IndexSpec) -> IndexState {
        self.0
            .get(&(raw_schema.to_string(), spec.name))
            .copied()
            .unwrap_or(IndexState::Absent)
    }
}

/// Whether a managed index and the index it replaces leave work to do:
/// build or rebuild the first, or, once it is valid, drop the second. A
/// foreign object under either name is left alone, and so is the replaced
/// index while the managed one isn't valid.
fn needs_work(managed: IndexState, replaced: IndexState) -> bool {
    match managed {
        IndexState::Absent | IndexState::Invalid => true,
        IndexState::Valid => matches!(replaced, IndexState::Valid | IndexState::Invalid),
        IndexState::Foreign => false,
    }
}

/// The bounded contexts with concurrent index work left: a managed index
/// missing or invalid, or a replaced one still there. One catalog query
/// over the schemas of the bounded contexts in `bounded_contexts` - never
/// another schema, whatever its name.
#[tracing::instrument(skip_all)]
pub async fn pending_concurrent_index_work(pool: &Pool) -> crate::error::Result<Vec<String>> {
    let names: Vec<(String, bool, bool)> = sqlx::query_as(
        "SELECT bc.name, d.oid IS NOT NULL, p.oid IS NOT NULL FROM bounded_contexts bc \
           JOIN pg_namespace n ON n.nspname = 'bc_' || bc.name \
           LEFT JOIN pg_class d ON d.relnamespace = n.oid AND d.relname = 'deadlines' \
                AND d.relkind IN ('r', 'p') \
           LEFT JOIN pg_class p ON p.relnamespace = n.oid AND p.relname = 'command_proposals' \
                AND p.relkind IN ('r', 'p') \
          ORDER BY bc.name",
    )
    .fetch_all(pool)
    .await?;
    let schemas: Vec<String> = names
        .iter()
        .map(|(name, _, _)| format!("bc_{name}"))
        .collect();
    let states = IndexStates::read(pool, &schemas).await?;
    Ok(names
        .into_iter()
        .filter(|(name, deadlines, proposals)| {
            let raw = format!("bc_{name}");
            MANAGED_INDEXES.iter().any(|(managed, replaced)| {
                let present = if *managed == OPEN_PROPOSAL_INDEX {
                    *proposals
                } else {
                    *deadlines
                };
                present
                    && needs_work(
                        states.get(&raw, managed),
                        replaced.map_or(IndexState::Absent, |r| states.get(&raw, &r)),
                    )
            })
        })
        .map(|(name, _, _)| name)
        .collect())
}

/// What [`build_concurrent_indexes_until`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConcurrentIndexOutcome {
    /// Nothing was left to do.
    UpToDate,
    /// Indexes were built, rebuilt or dropped.
    Changed,
    /// A foreign object sits under a managed name; it was left alone.
    Skipped,
    /// The stop future resolved while waiting for another process's build.
    Stopped,
}

/// The application name of the connection a concurrent build runs on, so
/// `pg_stat_activity` tells it apart.
pub const CONCURRENT_INDEX_APPLICATION_NAME: &str = "skilj-concurrent-index";

/// How long to wait between attempts at another process's build lock.
const LOCK_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Warns about a foreign object under a managed name once per process.
fn warn_foreign_once(bounded_context: &str, name: &str) {
    static WARNED: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();
    let first = WARNED
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert((bounded_context.to_string(), name.to_string()));
    if first {
        tracing::warn!(
            bounded_context,
            index = name,
            "an object this release didn't create sits under the name of an index it manages; \
             leaving it, and the index it would replace, alone"
        );
    }
}

/// [`build_concurrent_indexes_until`], never stopped early.
pub async fn build_concurrent_indexes(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<ConcurrentIndexOutcome> {
    build_concurrent_indexes_until(pool, bounded_context, std::future::pending()).await
}

/// Builds the bounded context's managed indexes concurrently and drops
/// the indexes they replace, once each new one is valid
/// (docs/architecture.md §256).
///
/// When nothing is left to do, a catalog read on the pool is all it costs:
/// no lock on any table. Otherwise it takes one connection from the pool
/// and detaches it, so the pool's `after_connect` hook has run on it and
/// the pool replaces it, and does everything on it, outside any
/// transaction: wait for the build lock other processes building the same
/// index hold, drop an index an interrupted build left invalid, build,
/// check the result, drop the replaced index. A drop therefore never waits
/// on a pooled connection.
///
/// The build lock is an advisory lock taken with `pg_try_advisory_lock`,
/// polled: a session waiting in a blocking `pg_advisory_lock` holds a
/// snapshot that the build holding the lock waits for, and Postgres
/// resolves that as a deadlock against the build. With a `lock_timeout`
/// set, the wait ends after that long with one last attempt that fails
/// with `55P03` like any other lock timeout; with none, it ends when the
/// lock is free or `stop` resolves. `lock_timeout` then bounds the
/// build's and the drop's own waits, which end with `55P03` too.
///
/// What waits: the build and the drop wait for transactions already
/// writing the table, and the build also for every transaction with an
/// older snapshot anywhere in the database. Writes started meanwhile don't
/// wait for them, unless a conflicting DDL statement (a hard delete's
/// `DROP SCHEMA`) queues behind them and they queue behind it. A build
/// interrupted - a timeout, a cancel, a crash - leaves an invalid index,
/// which the next run drops and builds again. A client that goes away
/// doesn't stop a build already running: the server finishes it and holds
/// the build lock until then.
#[tracing::instrument(skip(pool, stop))]
pub async fn build_concurrent_indexes_until(
    pool: &Pool,
    bounded_context: &str,
    stop: impl std::future::Future<Output = ()>,
) -> crate::error::Result<ConcurrentIndexOutcome> {
    let raw = format!("bc_{bounded_context}");
    let schema = schema_ident(bounded_context);
    let states = IndexStates::read(pool, std::slice::from_ref(&raw)).await?;
    let pending: Vec<_> = MANAGED_INDEXES
        .iter()
        .filter(|(managed, replaced)| {
            if states.get(&raw, managed) == IndexState::Foreign {
                warn_foreign_once(bounded_context, managed.name);
            }
            needs_work(
                states.get(&raw, managed),
                replaced.map_or(IndexState::Absent, |r| states.get(&raw, &r)),
            )
        })
        .collect();
    if pending.is_empty() {
        return Ok(
            if MANAGED_INDEXES
                .iter()
                .any(|(m, _)| states.get(&raw, m) == IndexState::Foreign)
            {
                ConcurrentIndexOutcome::Skipped
            } else {
                ConcurrentIndexOutcome::UpToDate
            },
        );
    }

    let mut conn = pool.acquire().await?.detach();
    let outcome = build_on(&mut conn, bounded_context, &raw, &schema, &pending, stop).await;
    // The server releases the build lock with the session either way.
    let _ = sqlx::Connection::close(conn).await;
    outcome
}

async fn build_on(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
    raw: &str,
    schema: &str,
    pending: &[&(IndexSpec, Option<IndexSpec>)],
    stop: impl std::future::Future<Output = ()>,
) -> crate::error::Result<ConcurrentIndexOutcome> {
    sqlx::query("SELECT set_config('application_name', $1, false)")
        .bind(CONCURRENT_INDEX_APPLICATION_NAME)
        .execute(&mut *conn)
        .await?;
    // A statement timeout would cancel a long build every time, leaving
    // an invalid index for the next attempt to drop and build again.
    sqlx::query("SET statement_timeout = 0")
        .execute(&mut *conn)
        .await?;
    let (lock_timeout, lock_timeout_ms): (String, f64) = sqlx::query_as(
        "SELECT current_setting('lock_timeout'), \
                (extract(epoch FROM current_setting('lock_timeout')::interval) * 1000)::float8",
    )
    .fetch_one(&mut *conn)
    .await?;
    // A timeout too large to represent waits as long as none would.
    let lock_wait = (lock_timeout_ms > 0.0)
        .then(|| Duration::try_from_secs_f64(lock_timeout_ms / 1000.0).ok())
        .flatten();
    let mut stop = std::pin::pin!(stop);
    let mut changed = false;

    for (managed, replaced) in pending {
        if !table_present(conn, raw, managed.table).await? {
            continue;
        }
        let deadline = lock_wait.and_then(|wait| Instant::now().checked_add(wait));
        let key = format!("skilj_concurrent_index:{schema}.{}", managed.name);
        loop {
            let (got,): (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock(hashtext($1))")
                .bind(&key)
                .fetch_one(&mut *conn)
                .await?;
            if got {
                break;
            }
            let wait = match deadline {
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        // One blocking attempt, bounded so the deadlock
                        // detector (deadlock_timeout, 1 s by default) never
                        // runs: it either fails with 55P03, as any lock
                        // timeout does, or gets the lock.
                        sqlx::query("SET lock_timeout = '1ms'")
                            .execute(&mut *conn)
                            .await?;
                        sqlx::query("SELECT pg_advisory_lock(hashtext($1))")
                            .bind(&key)
                            .execute(&mut *conn)
                            .await?;
                        break;
                    }
                    left.min(LOCK_POLL_INTERVAL)
                }
                None => LOCK_POLL_INTERVAL,
            };
            tokio::select! {
                () = &mut stop => return Ok(ConcurrentIndexOutcome::Stopped),
                () = tokio::time::sleep(wait) => {}
            }
        }
        // Whichever way the lock was won, what follows waits under the
        // configured timeout, not the last attempt's.
        sqlx::query("SELECT set_config('lock_timeout', $1, false)")
            .bind(&lock_timeout)
            .execute(&mut *conn)
            .await?;

        let states = IndexStates::for_schema(conn, raw).await?;
        let state = states.get(raw, managed);
        if *managed == OPEN_PROPOSAL_INDEX
            && (state == IndexState::Foreign
                || (matches!(state, IndexState::Valid | IndexState::Invalid)
                    && !approval_predicate_matches(conn, raw).await?))
        {
            return Err(approval_index_error(
                bounded_context,
                "the replacement index has an unexpected definition",
            ));
        }
        match state {
            IndexState::Foreign => {
                warn_foreign_once(bounded_context, managed.name);
                continue;
            }
            IndexState::Invalid => {
                sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "DROP INDEX CONCURRENTLY IF EXISTS {schema}.{}",
                    managed.name
                )))
                .execute(&mut *conn)
                .await?;
                changed = true;
            }
            IndexState::Absent | IndexState::Valid => {}
        }
        if states.get(raw, managed) != IndexState::Valid {
            let build = sqlx::raw_sql(sqlx::AssertSqlSafe(managed.create_sql(schema, true)))
                .execute(&mut *conn)
                .await;
            if let Err(err) = build {
                if *managed == OPEN_PROPOSAL_INDEX
                    && err.as_database_error().and_then(|e| e.code()).as_deref() == Some("23505")
                {
                    let groups = duplicate_proposal_groups(conn, bounded_context).await?;
                    let detail = duplicate_message(&groups);
                    return Err(approval_index_error(bounded_context, &detail));
                }
                return Err(err.into());
            }
            changed = true;
            let state = IndexStates::for_schema(conn, raw).await?.get(raw, managed);
            if state != IndexState::Valid {
                return Err(crate::error::Error::Database(sqlx::Error::Protocol(
                    format!(
                        "index {}.{} is {state:?} after CREATE INDEX CONCURRENTLY",
                        raw, managed.name
                    ),
                )));
            }
        }
        if let Some(replaced) = replaced {
            match IndexStates::for_schema(conn, raw).await?.get(raw, replaced) {
                IndexState::Valid | IndexState::Invalid => {
                    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                        "DROP INDEX CONCURRENTLY IF EXISTS {schema}.{}",
                        replaced.name
                    )))
                    .execute(&mut *conn)
                    .await?;
                    changed = true;
                }
                IndexState::Foreign => warn_foreign_once(bounded_context, replaced.name),
                IndexState::Absent => {}
            }
        }
    }
    Ok(if changed {
        ConcurrentIndexOutcome::Changed
    } else {
        ConcurrentIndexOutcome::UpToDate
    })
}

async fn table_present(
    conn: &mut sqlx::PgConnection,
    raw_schema: &str,
    table: &str,
) -> crate::error::Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p'))",
    )
    .bind(raw_schema)
    .bind(table)
    .fetch_one(conn)
    .await?)
}

async fn approval_predicate_matches(
    conn: &mut sqlx::PgConnection,
    raw_schema: &str,
) -> crate::error::Result<bool> {
    let matches = sqlx::query_scalar(
        "SELECT pg_get_expr(x.indpred, x.indrelid, false) = $3
         FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
         JOIN pg_namespace n ON n.oid = i.relnamespace
         WHERE n.nspname = $1 AND i.relname = $2",
    )
    .bind(raw_schema)
    .bind(OPEN_PROPOSAL_INDEX.name)
    .bind(OPEN_PROPOSAL_INDEX_EXPRESSION)
    .fetch_optional(conn)
    .await?;
    Ok(matches.unwrap_or(false))
}

async fn duplicate_proposal_groups(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
) -> crate::error::Result<Vec<Vec<String>>> {
    let schema = schema_ident(bounded_context);
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT array_agg(id || ' (' || status || ')' ORDER BY id)
         FROM {schema}.command_proposals
         WHERE {OPEN_PROPOSAL_KEY_PREDICATE}
         GROUP BY command_type_name, metadata_client_id, idempotency_key
         HAVING count(*) > 1
         ORDER BY min(id)"
    )))
    .fetch_all(conn)
    .await?)
}

fn duplicate_message(groups: &[Vec<String>]) -> String {
    let groups: Vec<_> = groups
        .iter()
        .map(|ids| format!("[{}]", ids.join(", ")))
        .collect();
    format!(
        "duplicate open proposals block the upgrade: {}. Settle the proposals and retry (docs/deployment.md)",
        groups.join(", ")
    )
}

fn approval_index_error(bounded_context: &str, detail: &str) -> crate::error::Error {
    crate::error::Error::SchemaUpgradeRequired {
        pending: format!("bounded context {bounded_context}: {detail}"),
    }
}

pub(super) async fn approval_index_status(
    conn: &mut sqlx::PgConnection,
    bounded_context: &str,
) -> crate::error::Result<(IndexState, Option<String>)> {
    let raw = format!("bc_{bounded_context}");
    if !table_present(conn, &raw, OPEN_PROPOSAL_INDEX.table).await? {
        return Ok((IndexState::Absent, None));
    }
    let state = IndexStates::for_schema(conn, &raw)
        .await?
        .get(&raw, &OPEN_PROPOSAL_INDEX);
    if state == IndexState::Foreign
        || (matches!(state, IndexState::Valid | IndexState::Invalid)
            && !approval_predicate_matches(conn, &raw).await?)
    {
        return Ok((
            IndexState::Foreign,
            Some(format!(
                "bounded context {bounded_context}: {} has an unexpected definition",
                OPEN_PROPOSAL_INDEX.name
            )),
        ));
    }
    if state != IndexState::Valid {
        let groups = duplicate_proposal_groups(conn, bounded_context).await?;
        if !groups.is_empty() {
            return Ok((
                state,
                Some(format!(
                    "bounded context {bounded_context}: {}",
                    duplicate_message(&groups)
                )),
            ));
        }
    }
    Ok((state, None))
}

/// Checks all provisioned approval tables before and after the concurrent
/// upgrade. Before it, duplicates and foreign indexes must stop the swap.
/// After it, every approval table must have a valid arbiter before serving.
pub async fn check_required_approval_indexes(pool: &Pool, ready: bool) -> crate::error::Result<()> {
    let mut conn = pool.acquire().await?;
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT bc.name FROM bounded_contexts bc
         JOIN pg_namespace n ON n.nspname = 'bc_' || bc.name
         JOIN pg_class p ON p.relnamespace = n.oid AND p.relname = 'command_proposals'
             AND p.relkind IN ('r', 'p')
         ORDER BY bc.name",
    )
    .fetch_all(&mut *conn)
    .await?;
    for name in names {
        match approval_index_status(&mut conn, &name).await {
            Ok((state, detail)) => {
                if let Some(pending) = detail {
                    return Err(crate::error::Error::SchemaUpgradeRequired { pending });
                }
                if ready
                    && state != IndexState::Valid
                    && table_present(&mut conn, &format!("bc_{name}"), OPEN_PROPOSAL_INDEX.table)
                        .await?
                {
                    return Err(approval_index_error(
                        &name,
                        "the open proposal index is missing or invalid",
                    ));
                }
            }
            Err(err) => {
                if table_present(&mut conn, &format!("bc_{name}"), OPEN_PROPOSAL_INDEX.table)
                    .await?
                {
                    return Err(err);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(valid: bool) -> IndexRow {
        IndexRow {
            schema: "bc_x".into(),
            name: "deadlines_claimable".into(),
            relkind: "i".into(),
            table_name: Some("deadlines".into()),
            valid: Some(valid),
            is_unique: Some(false),
            amname: Some("btree".into()),
            natts: Some(1),
            no_exprs: Some(true),
            partial: Some(true),
            columns: vec!["fire_at".into()],
        }
    }

    #[test]
    fn a_shaped_index_is_valid_or_invalid() {
        assert_eq!(row(true).state(&DEADLINES_CLAIMABLE), IndexState::Valid);
        assert_eq!(row(false).state(&DEADLINES_CLAIMABLE), IndexState::Invalid);
    }

    #[test]
    fn anything_else_under_the_name_is_foreign() {
        let mut full = row(true);
        full.partial = Some(false);
        let mut unique = row(true);
        unique.is_unique = Some(true);
        let mut other_table = row(true);
        other_table.table_name = Some("events".into());
        let mut not_an_index = row(true);
        not_an_index.relkind = "r".into();
        let mut gin = row(true);
        gin.amname = Some("gin".into());
        for r in [full, unique, other_table, not_an_index, gin] {
            assert_eq!(r.state(&DEADLINES_CLAIMABLE), IndexState::Foreign);
        }
    }

    #[test]
    fn work_is_left_until_the_replacement_is_valid_and_the_old_one_gone() {
        use IndexState::*;
        assert!(needs_work(Absent, Valid));
        assert!(needs_work(Invalid, Absent));
        assert!(needs_work(Valid, Valid));
        assert!(needs_work(Valid, Invalid));
        assert!(!needs_work(Valid, Absent));
        assert!(!needs_work(Valid, Foreign));
        assert!(!needs_work(Foreign, Valid));
    }

    #[test]
    fn the_definition_matches_the_poll() {
        assert_eq!(
            DEADLINES_CLAIMABLE.create_sql("\"bc_x\"", true),
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS deadlines_claimable ON \"bc_x\".deadlines \
             (fire_at) WHERE status IN ('pending', 'firing')"
        );
    }
}
