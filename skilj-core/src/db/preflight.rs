//! The database side of [`crate::preflight`]: the read-only checks behind
//! `inspect`, the schema compatibility guard and record, and the shape of
//! a bounded context's schema (docs/architecture.md §249).

use super::routes::PARKED_DELIVERIES_OCCURRENCE_INDEX;
use super::{schema_ident, Pool};
use crate::access_trail::{access_event_type, fold_access_events, AccessEventKind};
use crate::bootstrap::{ContextCreator, ADMIN_BOUNDED_CONTEXT_NAME};
use crate::encryption::trail::{master_key_event_type, MasterKeyEventKind};
use crate::event_store::{BoundedContext, BoundedContextStatus};
use crate::preflight::{
    AdminContext, BoundedContextReport, Compatibility, EpochState, GlobalMigrations,
    InspectOptions, PendingMigration, PreflightReport, RecordedLevel, Refusal, Rewrite, RowCount,
    TableLock, Unknown, Verdict, MIN_COMPATIBLE_SCHEMA_LEVEL, REPORT_FORMAT, SCHEMA_LEVEL,
};
use chrono::{DateTime, Utc};
use sqlx::migrate::Migrator;
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet};

/// The embedded global migrations. `ignore_missing`: a migration this
/// binary doesn't know - one a newer release applied - is no longer
/// refused by sqlx itself; [`check_schema_compatibility`] decides instead,
/// from what that newer release recorded (docs/architecture.md §249).
pub(super) fn migrator() -> Migrator {
    let mut migrator = sqlx::migrate!("./migrations");
    migrator.set_ignore_missing(true);
    migrator
}

/// The shape a bounded context's schema has once startup has patched it,
/// as [`bounded_context_shape`] describes it, recorded from a freshly
/// provisioned one. `skilj/tests/startup_bounded_contexts.rs` checks it
/// against a new bounded context, and re-records it with
/// `SKILJ_RECORD_SHAPE=1`.
pub const EXPECTED_BOUNDED_CONTEXT_SHAPE: &str = include_str!("sql/bounded_context_shape.txt");

/// Columns startup leaves to their first use: `registered_by_version` is
/// added by the first registration from a versioned instance (§104).
fn is_lazy(line: &str) -> bool {
    [
        "column event_types.registered_by_version ",
        "column command_types.registered_by_version ",
        "column projections.registered_by_version ",
    ]
    .iter()
    .any(|prefix| line.starts_with(prefix))
}

/// One line per table, view, column, index, constraint, trigger and
/// function in the bounded context's schema, sorted, with nothing in them
/// that differs between two bounded contexts or between the supported
/// Postgres majors: names, column types and nullability, constraint kinds
/// (`NOT NULL` constraints, catalogued from Postgres 18, left out), and a
/// function's body as a hash with the schema's name taken out. Empty when
/// the schema doesn't exist.
pub async fn bounded_context_shape(
    conn: &mut PgConnection,
    bounded_context: &str,
) -> crate::error::Result<Vec<String>> {
    let raw = format!("bc_{bounded_context}");
    let quoted = schema_ident(bounded_context);
    let lines: Vec<(String,)> = sqlx::query_as(
        "WITH ns AS (SELECT oid FROM pg_namespace WHERE nspname = $1) \
         SELECT CASE c.relkind WHEN 'v' THEN 'view ' ELSE 'table ' END || c.relname \
           FROM pg_class c WHERE c.relnamespace = (SELECT oid FROM ns) \
           AND c.relkind IN ('r', 'p', 'v') \
         UNION ALL \
         SELECT 'column ' || c.relname || '.' || a.attname || ' ' \
             || format_type(a.atttypid, a.atttypmod) \
             || CASE WHEN a.attnotnull THEN ' not null' ELSE ' null' END \
           FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid \
           WHERE c.relnamespace = (SELECT oid FROM ns) AND c.relkind IN ('r', 'p', 'v') \
           AND a.attnum > 0 AND NOT a.attisdropped \
         UNION ALL \
         SELECT 'index ' || i.relname || ' on ' || t.relname \
           FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid \
           JOIN pg_class t ON t.oid = x.indrelid \
           WHERE t.relnamespace = (SELECT oid FROM ns) \
         UNION ALL \
         SELECT 'constraint ' || con.conname || ' on ' || c.relname || ' (' || con.contype::text || ')' \
           FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid \
           WHERE c.relnamespace = (SELECT oid FROM ns) AND con.contype <> 'n' \
         UNION ALL \
         SELECT 'trigger ' || t.tgname || ' on ' || c.relname \
           FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
           WHERE c.relnamespace = (SELECT oid FROM ns) AND NOT t.tgisinternal \
         UNION ALL \
         SELECT 'function ' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) \
             || ') ' || left(md5(replace(replace(p.prosrc, $2, '@SCHEMA@'), $1, '@SCHEMA@')), 12) \
           FROM pg_proc p WHERE p.pronamespace = (SELECT oid FROM ns)",
    )
    .bind(&raw)
    .bind(&quoted)
    .fetch_all(&mut *conn)
    .await?;
    let mut lines: Vec<String> = lines.into_iter().map(|(line,)| line).collect();
    lines.sort();
    Ok(lines)
}

/// The key of a shape line: a function's line without its body hash, so a
/// changed body is told apart from a missing function.
fn shape_key(line: &str) -> &str {
    if line.starts_with("function ") {
        line.rsplit_once(' ').map_or(line, |(key, _)| key)
    } else {
        line
    }
}

/// What a bounded context's actual shape lacks, has in addition, or has
/// differently, against the expected one.
pub(crate) struct ShapeDiff {
    pub missing: Vec<String>,
    pub outdated: Vec<String>,
    pub unexpected: Vec<String>,
    pub drops: Vec<String>,
    pub lazy: Vec<String>,
}

/// Objects of older releases a patch removes: the GIN index tag reads
/// went through before `event_tags` (§210, dropped concurrently), the
/// narrower parked-delivery index (§64), and the `event_tags` table the
/// tag dictionary's view replaces (§245).
fn is_known_drop(key: &str) -> bool {
    key == "index events_by_tags on events"
        || key
            == format!(
                "index {} on parked_deliveries",
                super::routes::PARKED_DELIVERIES_LEGACY_DEDUP_INDEX
            )
        || key == "table event_tags"
}

pub(crate) fn diff_shape(expected: &[&str], actual: &[String]) -> ShapeDiff {
    let expected_keys: BTreeMap<&str, &str> = expected.iter().map(|l| (shape_key(l), *l)).collect();
    let actual_keys: BTreeMap<&str, &str> =
        actual.iter().map(|l| (shape_key(l), l.as_str())).collect();
    let mut diff = ShapeDiff {
        missing: Vec::new(),
        outdated: Vec::new(),
        unexpected: Vec::new(),
        drops: Vec::new(),
        lazy: Vec::new(),
    };
    for (key, line) in &expected_keys {
        match actual_keys.get(key) {
            None if is_lazy(line) => diff.lazy.push(key.to_string()),
            None => diff.missing.push(key.to_string()),
            Some(actual) if actual != line => diff.outdated.push(key.to_string()),
            Some(_) => {}
        }
    }
    for key in actual_keys.keys() {
        if expected_keys.contains_key(key) {
            continue;
        }
        if is_known_drop(key) {
            diff.drops.push(key.to_string());
        } else {
            diff.unexpected.push(key.to_string());
        }
    }
    diff
}

fn expected_shape() -> Vec<&'static str> {
    EXPECTED_BOUNDED_CONTEXT_SHAPE
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect()
}

async fn relation_exists(conn: &mut PgConnection, name: &str) -> crate::error::Result<bool> {
    Ok(
        sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
            .bind(name)
            .fetch_one(&mut *conn)
            .await?,
    )
}

/// The recorded compatibility row, when the table and the row exist.
async fn recorded_level(conn: &mut PgConnection) -> crate::error::Result<Option<RecordedLevel>> {
    if !relation_exists(conn, "skilj_schema_compatibility").await? {
        return Ok(None);
    }
    let row: Option<(i64, i64, String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT level, min_compatible_level, upgraded_by, upgraded_at \
         FROM skilj_schema_compatibility WHERE id = 1",
    )
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row.map(
        |(level, min_compatible_level, upgraded_by, upgraded_at)| RecordedLevel {
            level,
            min_compatible_level,
            upgraded_by,
            upgraded_at,
        },
    ))
}

/// Versions of migrations recorded as applied that this binary doesn't
/// embed.
async fn applied_migrations(
    conn: &mut PgConnection,
) -> crate::error::Result<Vec<(i64, bool, Vec<u8>)>> {
    if !relation_exists(conn, "_sqlx_migrations").await? {
        return Ok(Vec::new());
    }
    Ok(
        sqlx::query_as("SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&mut *conn)
            .await?,
    )
}

/// Why this binary must not run against the database, if it mustn't: a
/// newer release recorded a level this one is too old for, or applied
/// migrations this one doesn't know without recording that it may
/// (docs/architecture.md §249).
fn compatibility_refusal(
    recorded: Option<&RecordedLevel>,
    unknown_migrations: &[i64],
) -> Option<crate::error::Error> {
    match recorded {
        Some(r) if r.min_compatible_level > SCHEMA_LEVEL => {
            Some(crate::error::Error::SchemaTooNew {
                level: SCHEMA_LEVEL,
                min_compatible_level: r.min_compatible_level,
                upgraded_by: r.upgraded_by.clone(),
            })
        }
        Some(_) => None,
        None => unknown_migrations
            .first()
            .map(|first| crate::error::Error::SchemaTooNew {
                level: SCHEMA_LEVEL,
                min_compatible_level: i64::MAX,
                upgraded_by: format!(
                    "a release that applied migration {first} and recorded no compatibility level"
                ),
            }),
    }
}

/// Refuses with `schema_too_new` when the database was upgraded by a
/// release that doesn't declare this one compatible. Runs before the
/// migrations (docs/architecture.md §249).
pub(crate) async fn check_schema_compatibility(pool: &Pool) -> crate::error::Result<()> {
    let mut conn = pool.acquire().await?;
    let recorded = recorded_level(&mut conn).await?;
    let known: BTreeSet<i64> = migrator().iter().map(|m| m.version).collect();
    let unknown: Vec<i64> = applied_migrations(&mut conn)
        .await?
        .into_iter()
        .map(|(version, _, _)| version)
        .filter(|v| !known.contains(v))
        .collect();
    match compatibility_refusal(recorded.as_ref(), &unknown) {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Records this binary's schema level once every bounded context has been
/// patched to it. Never lowers a recorded level: an older (compatible)
/// binary leaves a newer one's record as it is.
pub async fn record_schema_level(pool: &Pool) -> crate::error::Result<()> {
    sqlx::query(
        "INSERT INTO skilj_schema_compatibility \
             (id, level, min_compatible_level, upgraded_by, upgraded_at) \
         VALUES (1, $1, $2, $3, now()) \
         ON CONFLICT (id) DO UPDATE SET level = EXCLUDED.level, \
             min_compatible_level = EXCLUDED.min_compatible_level, \
             upgraded_by = EXCLUDED.upgraded_by, upgraded_at = EXCLUDED.upgraded_at \
         WHERE skilj_schema_compatibility.level < EXCLUDED.level",
    )
    .bind(SCHEMA_LEVEL)
    .bind(MIN_COMPATIBLE_SCHEMA_LEVEL)
    .bind(env!("CARGO_PKG_VERSION"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn row_count(
    conn: &mut PgConnection,
    bounded_context: &str,
    table: &str,
    exact: bool,
) -> crate::error::Result<RowCount> {
    if exact {
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM {}.\"{}\"",
            schema_ident(bounded_context),
            table.replace('"', "\"\"")
        )))
        .fetch_one(&mut *conn)
        .await?;
        return Ok(RowCount {
            value: Some(count),
            exact: true,
        });
    }
    // -1 until the table was first vacuumed or analyzed.
    let estimate: Option<f32> = sqlx::query_scalar(
        "SELECT c.reltuples FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(format!("bc_{bounded_context}"))
    .bind(table)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(RowCount {
        value: estimate.filter(|e| *e >= 0.0).map(|e| e as i64),
        exact: false,
    })
}

/// The patches that rewrite or delete rows, or hold a lock that blocks
/// writes while they work through a table, and whether each still has
/// work to do here.
async fn rewrites(
    conn: &mut PgConnection,
    bounded_context: &str,
    tables: &BTreeSet<String>,
    exact: bool,
) -> crate::error::Result<Vec<Rewrite>> {
    let raw = format!("bc_{bounded_context}");
    let mut rewrites = Vec::new();

    if tables.contains("events") {
        let event_tags_kind: Option<String> = sqlx::query_scalar(
            "SELECT c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = 'event_tags'",
        )
        .bind(&raw)
        .fetch_optional(&mut *conn)
        .await?;
        if event_tags_kind.as_deref() != Some("v") {
            rewrites.push(Rewrite {
                name: "tag_dictionary".into(),
                table: "events".into(),
                lock: "SHARE ROW EXCLUSIVE".into(),
                blocks: "event writes, while every event's tags are indexed".into(),
                rows: row_count(conn, bounded_context, "events", exact).await?,
            });
        }
        let guards: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND NOT t.tgisinternal AND \
             ((c.relname = 'events' AND t.tgname = 'protect_external_source') OR \
              (c.relname = 'event_types' AND t.tgname = 'protect_existing_external_sources'))",
        )
        .bind(&raw)
        .fetch_one(&mut *conn)
        .await?;
        if guards != 2 {
            rewrites.push(Rewrite {
                name: "source_content_scrub".into(),
                table: "events".into(),
                lock: "SHARE ROW EXCLUSIVE".into(),
                blocks: "event writes and type registration, while protected external \
                         source content is replaced by [redacted]"
                    .into(),
                rows: row_count(conn, bounded_context, "events", exact).await?,
            });
        }
    }

    if tables.contains("parked_deliveries") {
        let has_current: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = $1 AND indexname = $2)",
        )
        .bind(&raw)
        .bind(PARKED_DELIVERIES_OCCURRENCE_INDEX)
        .fetch_one(&mut *conn)
        .await?;
        if !has_current {
            rewrites.push(Rewrite {
                name: "parked_delivery_dedupe".into(),
                table: "parked_deliveries".into(),
                lock: "SHARE".into(),
                blocks: format!(
                    "parked-delivery writes, while duplicates are deleted (all but the newest) \
                     and {PARKED_DELIVERIES_OCCURRENCE_INDEX} is built"
                ),
                rows: row_count(conn, bounded_context, "parked_deliveries", exact).await?,
            });
        }
        // Dropping the legacy index alone, once the current one exists, is
        // an index drop like any other: listed under `unexpected`.
    }

    if tables.contains("idempotency_keys") {
        let scoped: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.key_column_usage \
             WHERE table_schema = $1 AND table_name = 'idempotency_keys' \
             AND constraint_name = 'idempotency_keys_pkey' AND column_name = 'client_id')",
        )
        .bind(&raw)
        .fetch_one(&mut *conn)
        .await?;
        if !scoped {
            rewrites.push(Rewrite {
                name: "idempotency_key_rescope".into(),
                table: "idempotency_keys".into(),
                lock: "ACCESS EXCLUSIVE".into(),
                blocks: "commands with an idempotency key, while the primary key is rebuilt".into(),
                rows: row_count(conn, bounded_context, "idempotency_keys", exact).await?,
            });
        }
    }
    Ok(rewrites)
}

/// The table locks the missing columns, indexes and constraints take, one
/// entry per table, the strongest lock first named.
async fn ddl_locks(
    conn: &mut PgConnection,
    bounded_context: &str,
    missing: &[String],
    tables: &BTreeSet<String>,
    exact: bool,
) -> crate::error::Result<Vec<TableLock>> {
    // table -> (strongest mode, what is added)
    let mut by_table: BTreeMap<String, (&'static str, Vec<String>)> = BTreeMap::new();
    for line in missing {
        let (mode, table, what) = if let Some(rest) = line.strip_prefix("column ") {
            let column = rest.split(' ').next().unwrap_or(rest);
            let Some((table, name)) = column.split_once('.') else {
                continue;
            };
            ("ACCESS EXCLUSIVE", table, format!("column {name}"))
        } else if let Some(rest) = line.strip_prefix("index ") {
            let Some((name, table)) = rest.split_once(" on ") else {
                continue;
            };
            ("SHARE", table, format!("index {name}"))
        } else if let Some(rest) = line.strip_prefix("constraint ") {
            let Some((name, rest)) = rest.split_once(" on ") else {
                continue;
            };
            let table = rest.split(' ').next().unwrap_or(rest);
            ("ACCESS EXCLUSIVE", table, format!("constraint {name}"))
        } else {
            continue;
        };
        // A table that is itself missing is created with all of it.
        if !tables.contains(table) {
            continue;
        }
        let entry = by_table
            .entry(table.to_string())
            .or_insert((mode, Vec::new()));
        if mode == "ACCESS EXCLUSIVE" {
            entry.0 = mode;
        }
        entry.1.push(what);
    }
    let mut locks = Vec::new();
    for (table, (mode, adds)) in by_table {
        let rows = row_count(conn, bounded_context, &table, exact).await?;
        locks.push(TableLock {
            table,
            mode: mode.into(),
            reason: format!("adds {}", adds.join(", ")),
            rows,
        });
    }
    Ok(locks)
}

async fn inspect_bounded_context(
    conn: &mut PgConnection,
    name: &str,
    expected: &[&str],
    exact: bool,
) -> crate::error::Result<BoundedContextReport> {
    let actual = bounded_context_shape(conn, name).await?;
    let tables: BTreeSet<String> = actual
        .iter()
        .filter_map(|l| l.strip_prefix("table "))
        .map(str::to_string)
        .collect();
    let diff = diff_shape(expected, &actual);
    let schema_missing = actual.is_empty();
    let (rewrites, locks) = if schema_missing {
        (Vec::new(), Vec::new())
    } else {
        (
            rewrites(conn, name, &tables, exact).await?,
            ddl_locks(conn, name, &diff.missing, &tables, exact).await?,
        )
    };
    Ok(BoundedContextReport {
        name: name.to_string(),
        schema_missing,
        missing: diff.missing,
        outdated: diff.outdated,
        unexpected: diff.unexpected,
        drops: diff.drops,
        lazy: diff.lazy,
        rewrites,
        locks,
    })
}

fn admin_placeholder() -> BoundedContext {
    BoundedContext {
        name: ADMIN_BOUNDED_CONTEXT_NAME.to_string(),
        status: BoundedContextStatus::Active,
        created_at: DateTime::<Utc>::UNIX_EPOCH,
        created_by: ContextCreator::SystemCreator,
        template: None,
    }
}

async fn inspect_admin(
    conn: &mut PgConnection,
    admin_report: Option<&BoundedContextReport>,
    unknown: &mut Vec<Unknown>,
) -> crate::error::Result<AdminContext> {
    let Some(admin_report) = admin_report.filter(|r| !r.schema_missing) else {
        return Ok(AdminContext {
            exists: false,
            origin_kind_widened: false,
            event_types_to_register: Vec::new(),
            grants_to_baseline: None,
        });
    };
    let schema = schema_ident(ADMIN_BOUNDED_CONTEXT_NAME);
    let checks: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint \
         WHERE conrelid = to_regclass('{}.events') AND contype = 'c'",
        schema.replace('\'', "''")
    )))
    .fetch_all(&mut *conn)
    .await?;
    let origin_kind_widened = checks
        .iter()
        .any(|(def,)| def.contains("origin_kind") && def.contains("skilj_recorded"));

    let admin = admin_placeholder();
    let wanted = AccessEventKind::ALL
        .iter()
        .map(|kind| access_event_type(&admin, *kind))
        .chain(
            MasterKeyEventKind::ALL
                .iter()
                .map(|kind| master_key_event_type(&admin, *kind)),
        );
    let mut event_types_to_register = Vec::new();
    let mut access_types_registered = 0;
    for wanted in wanted {
        let stored: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT schema FROM {schema}.event_types WHERE name = $1"
        )))
        .bind(&wanted.name)
        .fetch_optional(&mut *conn)
        .await?;
        let parse = |s: &str| serde_json::from_str::<serde_json::Value>(s).ok();
        let up_to_date = stored
            .as_deref()
            .is_some_and(|s| parse(s).is_some() && parse(s) == parse(&wanted.schema));
        if stored.is_some() && AccessEventKind::from_name(&wanted.name).is_some() {
            access_types_registered += 1;
        }
        if !up_to_date {
            event_types_to_register.push(wanted.name.clone());
        }
    }

    let active: Vec<(String, String)> = sqlx::query_as(
        "SELECT role_id, bounded_context FROM role_access_mappings WHERE status = 'active' \
         ORDER BY role_id, bounded_context",
    )
    .fetch_all(&mut *conn)
    .await?;
    let grants_to_baseline = if access_types_registered == 0 {
        Some(active.len() as u64)
    } else if admin_report.missing.iter().any(|l| l.contains("event_tag"))
        || admin_report
            .rewrites
            .iter()
            .any(|r| r.name == "tag_dictionary")
    {
        unknown.push(Unknown {
            check: "access_trail_baseline".into(),
            reason: "the admin context's tag index is patched first; the grants to baseline \
                     can't be read until then"
                .into(),
        });
        None
    } else {
        let mut count = 0;
        for (role_id, bounded_context) in &active {
            let records =
                super::access::access_event_records(&mut *conn, role_id, bounded_context).await?;
            if !fold_access_events(&records, DateTime::<Utc>::MAX_UTC).held {
                count += 1;
            }
        }
        Some(count)
    };
    Ok(AdminContext {
        exists: true,
        origin_kind_widened,
        event_types_to_register,
        grants_to_baseline,
    })
}

/// [`crate::preflight::inspect`]: every read in one read-only,
/// repeatable-read transaction.
pub(crate) async fn inspect(
    pool: &Pool,
    options: &InspectOptions,
) -> crate::error::Result<PreflightReport> {
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await?;
    let conn: &mut PgConnection = &mut tx;
    let exact = options.exact_counts;

    let server_version_num: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(&mut *conn)
        .await?;
    let server_major = server_version_num.parse::<u32>().unwrap_or(0) / 10_000;

    let mut refusals = Vec::new();
    let mut unknown = Vec::new();

    // Global migrations.
    let applied = applied_migrations(conn).await?;
    let migrator = migrator();
    let embedded: BTreeMap<i64, _> = migrator
        .iter()
        .filter(|m| !m.migration_type.is_down_migration())
        .map(|m| (m.version, m))
        .collect();
    let mut global = GlobalMigrations::default();
    for (version, success, checksum) in &applied {
        match embedded.get(version) {
            None => global.unknown.push(*version),
            Some(_) if !success => global.failed.push(*version),
            Some(m) if m.checksum.as_ref() != checksum.as_slice() => {
                global.checksum_mismatch.push(*version)
            }
            Some(_) => global.applied.push(*version),
        }
    }
    let applied_versions: BTreeSet<i64> = applied.iter().map(|(v, _, _)| *v).collect();
    for (version, m) in &embedded {
        if !applied_versions.contains(version) {
            global.pending.push(PendingMigration {
                version: *version,
                description: m.description.to_string(),
            });
        }
    }
    for version in &global.failed {
        refusals.push(Refusal {
            code: "migration_failed".into(),
            detail: format!(
                "global migration {version} is recorded as failed; inspect it before rerunning"
            ),
        });
    }
    for version in &global.checksum_mismatch {
        refusals.push(Refusal {
            code: "migration_checksum_mismatch".into(),
            detail: format!("global migration {version} differs from the one that was applied"),
        });
    }

    // Compatibility.
    let recorded = recorded_level(conn).await?;
    if let Some(err) = compatibility_refusal(recorded.as_ref(), &global.unknown) {
        refusals.push(Refusal {
            code: "schema_too_new".into(),
            detail: err.to_string(),
        });
    }
    let compatibility = Compatibility {
        binary_level: SCHEMA_LEVEL,
        binary_min_compatible_level: MIN_COMPATIBLE_SCHEMA_LEVEL,
        recorded,
    };

    // Epoch.
    let epoch = if relation_exists(conn, "skilj_epoch").await? {
        let stored: Option<String> =
            sqlx::query_scalar("SELECT epoch FROM skilj_epoch WHERE id = 1")
                .fetch_optional(&mut *conn)
                .await?;
        let current = super::current_epoch(&mut *conn).await?;
        EpochState {
            recorded: stored.is_some(),
            changed: stored.is_some_and(|s| s != current.as_str()),
        }
    } else {
        EpochState::default()
    };

    // Bounded contexts.
    let names: Vec<String> = if relation_exists(conn, "bounded_contexts").await? {
        sqlx::query_scalar("SELECT name FROM bounded_contexts ORDER BY name")
            .fetch_all(&mut *conn)
            .await?
    } else {
        Vec::new()
    };
    let expected = expected_shape();
    let mut bounded_contexts = Vec::new();
    for name in &names {
        let report = inspect_bounded_context(conn, name, &expected, exact).await?;
        if report.schema_missing {
            refusals.push(Refusal {
                code: "bounded_context_schema_missing".into(),
                detail: format!("bounded context {name} is registered but its schema is gone"),
            });
        }
        bounded_contexts.push(report);
    }
    let admin_report = bounded_contexts
        .iter()
        .find(|r| r.name == ADMIN_BOUNDED_CONTEXT_NAME);
    let admin_context = inspect_admin(conn, admin_report, &mut unknown).await?;

    tx.rollback().await?;

    let upgrade_needed = !global.pending.is_empty()
        || compatibility
            .recorded
            .as_ref()
            .is_none_or(|r| r.level < SCHEMA_LEVEL)
        || !admin_context.exists
        || !admin_context.origin_kind_widened
        || !admin_context.event_types_to_register.is_empty()
        || bounded_contexts
            .iter()
            .any(BoundedContextReport::needs_upgrade);
    let verdict = if !refusals.is_empty() {
        Verdict::Refused
    } else if upgrade_needed {
        Verdict::UpgradeNeeded
    } else {
        Verdict::UpToDate
    };
    Ok(PreflightReport {
        format: REPORT_FORMAT,
        skilj_version: env!("CARGO_PKG_VERSION").to_string(),
        server_major,
        verdict,
        refusals,
        compatibility,
        global,
        epoch,
        admin_context,
        bounded_contexts,
        registrations: Vec::new(),
        unknown,
    })
}

/// Every patch startup runs on a bounded context's schema, in order, to
/// bring one provisioned by an older release up to the current shape
/// ([`EXPECTED_BOUNDED_CONTEXT_SHAPE`]). Each patch checks the catalog
/// first and changes only what is missing (docs/architecture.md §158), so
/// on an up-to-date schema this is a series of catalog reads. Shared by
/// `Skilj::build()` and [`crate::preflight::migrate`]
/// (docs/architecture.md §249).
///
/// Two processes upgrading the same bounded context at once - replicas
/// starting together, or [`crate::preflight::migrate`] next to one - can
/// both find an object missing and both create it: `CREATE TABLE IF NOT
/// EXISTS`, `ADD COLUMN` after a catalog check and `CREATE OR REPLACE
/// FUNCTION` are not safe against a concurrent creator, and the loser
/// fails with a duplicate-object error. Every patch is idempotent, so the
/// loser runs the sequence again - but on a schema missing many objects it
/// caught up with the winner and lost again on the next one, until it ran
/// out of attempts. So a loser runs it again under a session-level
/// advisory lock keyed by the bounded context, on a connection of its own
/// outside the pool (the patches may need every pooled one): a second
/// loser waits for it, then finds its objects. The first pass takes no
/// lock, so an up-to-date bounded context still costs only catalog reads
/// (docs/architecture.md §158). `lock_timeout` applies to waiting for it.
pub async fn upgrade_bounded_context(pool: &Pool, bc: &BoundedContext) -> crate::error::Result<()> {
    match upgrade_bounded_context_once(pool, bc).await {
        // Lost to another process patching the same object: again under
        // the lock, below.
        Err(err) if lost_a_concurrent_creation(&err) => {}
        other => return other,
    }
    // Dropped - closed - when this returns or is cancelled, which releases
    // the lock.
    let mut lock =
        <sqlx::postgres::PgConnection as sqlx::Connection>::connect_with(&pool.connect_options())
            .await?;
    sqlx::query("SELECT pg_advisory_lock(hashtext('skilj_upgrade_bounded_context:' || $1))")
        .bind(&bc.name)
        .execute(&mut lock)
        .await?;
    let outcome = upgrade_bounded_context_retrying(pool, bc).await;
    // Errors closing are not the upgrade's: the server releases the lock
    // with the session either way.
    let _ = sqlx::Connection::close(lock).await;
    outcome
}

/// The sequence, again while it loses to a process that holds no lock -
/// one still on its first pass - which can happen only so often before
/// that process loses too and waits for the lock.
async fn upgrade_bounded_context_retrying(
    pool: &Pool,
    bc: &BoundedContext,
) -> crate::error::Result<()> {
    const ATTEMPTS: usize = 10;
    let mut attempt = 1;
    loop {
        match upgrade_bounded_context_once(pool, bc).await {
            Err(err) if attempt < ATTEMPTS && lost_a_concurrent_creation(&err) => {
                tracing::debug!(
                    bounded_context = %bc.name,
                    error = %err,
                    "another process patched the same object at the same time; patching again"
                );
                attempt += 1;
            }
            other => return other,
        }
    }
}

/// Whether `err` is what creating an object another session created
/// concurrently fails with.
fn lost_a_concurrent_creation(err: &crate::error::Error) -> bool {
    let crate::error::Error::Database(sqlx::Error::Database(db)) = err else {
        return false;
    };
    match db.code().as_deref() {
        // duplicate_table, duplicate_object, duplicate_column,
        // duplicate_function.
        Some("42P07" | "42710" | "42701" | "42723") => true,
        // unique_violation on the catalog itself: a type or relation name
        // taken between the check and the insert.
        Some("23505") => db
            .constraint()
            .is_some_and(|c| c.starts_with("pg_type_") || c.starts_with("pg_class_")),
        // "tuple concurrently updated": two CREATE OR REPLACE of one
        // function or trigger.
        Some("XX000") => db.message().contains("concurrently"),
        _ => false,
    }
}

async fn upgrade_bounded_context_once(
    pool: &Pool,
    bc: &BoundedContext,
) -> crate::error::Result<()> {
    // issue #12: `idempotency_keys` patched into
    // every bounded context, every startup - including
    // ones provisioned before this feature existed, since
    // `provision_bounded_context_schema` itself only ever
    // runs once, at creation, and there's no general
    // per-bounded-context migration mechanism in this
    // codebase. `CREATE TABLE IF NOT EXISTS` makes this
    // free once the table already exists - see
    // `ensure_idempotency_keys_table`'s own doc comment.
    super::ensure_idempotency_keys_table(pool, &bc.name).await?;
    // docs/architecture.md §37: patches an already-
    // provisioned bounded context's `idempotency_keys`
    // (a real table since 0.0.2) onto the `client_id`-
    // scoped shape `ensure_idempotency_keys_table` above
    // already gives a brand-new one directly - a no-op
    // for one that already has it, migrated or fresh.
    // See `migrate_idempotency_keys_client_id_scoping`'s
    // own doc comment.
    super::migrate_idempotency_keys_client_id_scoping(pool, &bc.name).await?;
    // Cross-tenant projection read fix
    // (docs/architecture.md's own write-up of this pass):
    // same "patched into every bounded context, every
    // startup" treatment, for `projection_state.owner`/
    // `projection_rebuild_state.owner` instead - see
    // `ensure_projection_state_owner_columns`'s own doc
    // comment.
    super::ensure_projection_state_owner_columns(pool, &bc.name).await?;
    // issue #25: `projection_state.as_of_sequence`/
    // `projection_rebuild_state.as_of_sequence`, the real
    // fix for a genuine cross-instance double-fold race
    // found while investigating that issue - see
    // `ensure_projection_state_as_of_sequence_columns`'s
    // own doc comment.
    super::ensure_projection_state_as_of_sequence_columns(pool, &bc.name).await?;
    // Same pass's own raw-event half - `event_types.owner_tag_key`/
    // `access_tokens.scope` - see
    // `ensure_event_scoping_columns`'s own doc comment.
    super::ensure_event_scoping_columns(pool, &bc.name).await?;
    // Private-field mechanism (docs/architecture.md's own
    // write-up of this pass) - `event_types.private_fields`/
    // `command_types.private_fields` columns, and the new
    // `private_field_grants` table - same "patched into
    // every bounded context, every startup" treatment. See
    // `ensure_private_field_columns`/
    // `ensure_private_field_grants_table`'s own doc
    // comments.
    super::ensure_private_field_columns(pool, &bc.name).await?;
    super::ensure_projection_owner_tag_key_column(pool, &bc.name).await?;
    super::ensure_projection_requires_capability_column(pool, &bc.name).await?;
    super::ensure_private_field_grants_table(pool, &bc.name).await?;
    // issue #98: the approval policy and
    // proposal tables, same "patched into every bounded
    // context, every startup" treatment.
    super::ensure_command_approval_tables(pool, &bc.name).await?;
    // Secrets (docs/architecture.md's own write-up of
    // this pass) - the new `secret_versions`/
    // `secret_grants` tables, same "patched into every
    // bounded context, every startup" treatment. See
    // `ensure_secret_tables`'s own doc comment.
    super::ensure_secret_tables(pool, &bc.name).await?;
    // Cross-context event router (docs/architecture.md's
    // own write-up of this pass) - same "patched into
    // every bounded context, every startup" treatment.
    // See `ensure_cross_context_route_cursors_table`'s
    // own doc comment.
    super::ensure_cross_context_route_cursors_table(pool, &bc.name).await?;
    // issue #20: native one-shot, per-entity
    // deadlines - same "patched into every bounded
    // context, every startup" treatment, for the new
    // `deadline_cursors`/`deadlines` tables. See
    // `ensure_deadline_cursors_table`/`ensure_deadlines_table`'s
    // own doc comments.
    super::ensure_deadline_cursors_table(pool, &bc.name).await?;
    super::ensure_deadlines_table(pool, &bc.name).await?;
    // issue #25: partitioned async Projection
    // catch-up - same "patched into every bounded
    // context, every startup" treatment, for a brand-new
    // table that needs no migration dance. See
    // `ensure_projection_partition_progress_table`'s own
    // doc comment.
    super::ensure_projection_partition_progress_table(pool, &bc.name).await?;
    // issue #25 (docs/architecture.md §52):
    // partitioned Snapshot catch-up - same treatment,
    // for `Snapshot`'s own twin table. See
    // `ensure_snapshot_partition_progress_table`'s own
    // doc comment.
    super::ensure_snapshot_partition_progress_table(pool, &bc.name).await?;
    // issue #25 (docs/architecture.md §53): the
    // read_cursors checkout mechanism closing the
    // Kafka/AMQP/NATS outbound bridges' double-publish
    // gap. See `ensure_read_cursors_checkout_column`'s
    // own doc comment.
    super::ensure_read_cursors_checkout_column(pool, &bc.name).await?;
    // External-message dedup (docs/architecture.md §39,
    // specs/skilj.allium's own rule CreateExternalEvent) -
    // same "patched into every bounded context, every
    // startup" treatment, for a brand-new table that needs
    // no migration dance. See
    // `ensure_external_message_cursors_table`'s own doc
    // comment.
    super::ensure_external_message_cursors_table(pool, &bc.name).await?;
    // Its per-message counterpart (docs/architecture.md §175).
    super::ensure_external_message_keys_table(pool, &bc.name).await?;
    // "New subscriber replays all history" fix
    // (docs/architecture.md's own write-up of this pass) -
    // `access_tokens.start_from`, same "patched into every
    // bounded context, every startup" treatment. See
    // `ensure_event_read_token_start_from_column`'s own
    // doc comment.
    super::ensure_event_read_token_start_from_column(pool, &bc.name).await?;
    // Correlation/causation ids (issue #18) -
    // same "patched into every bounded context, every
    // startup" treatment. See
    // `ensure_correlation_causation_columns`'s own doc
    // comment.
    super::ensure_correlation_causation_columns(pool, &bc.name).await?;
    // issue #21 (dead-letter/parking for failed
    // event/message handler delivery) - `cross_context_route_cursors`'
    // own three new retry-backoff columns, same "patched
    // into every bounded context, every startup"
    // treatment. See
    // `ensure_cross_context_route_retry_columns`'s own
    // doc comment for why this is a separate `ALTER
    // TABLE` patch rather than folded into
    // `ensure_cross_context_route_cursors_table` above.
    super::ensure_cross_context_route_retry_columns(pool, &bc.name).await?;
    // Same pass's own `parked_deliveries` table - a
    // brand-new table, so (unlike the retry columns just
    // above) this needs no `ALTER TABLE` patch, only the
    // identical `CREATE TABLE IF NOT EXISTS` every other
    // brand-new per-bounded-context table already gets
    // here.
    super::ensure_parked_deliveries_table(pool, &bc.name).await?;
    // issue #25 review (docs/architecture.md
    // §56): the concurrent-instance parked-delivery
    // duplication fix's own belt-and-suspenders unique
    // index, plus the one-time dedup a bounded context
    // that already hit the race needs before that index
    // can even be created. See
    // `migrate_parked_deliveries_dedup_and_unique_index`'s
    // own doc comment.
    super::migrate_parked_deliveries_dedup_and_unique_index(pool, &bc.name).await?;
    // docs/architecture.md §189: the counter the event
    // cache stamps registrations with, instead of
    // hashing them on every read.
    super::ensure_registrations_generation(pool, &bc.name).await?;
    // docs/architecture.md §198: the per-tag index a
    // consistency query's typed items read, and the
    // column a command's query is recorded in, each in
    // its own transaction (§200). Always run here, not
    // skipped for one this process already saw (§204).
    super::make_bounded_context_current(pool, bc).await?;
    Ok(())
}

/// Creates the `admin` bounded context when it doesn't exist yet - a new
/// database - tolerating another instance's startup creating it at the
/// same moment. Shared by `Skilj::build()` and
/// [`crate::preflight::migrate`].
pub async fn ensure_admin_bounded_context(pool: &Pool) -> crate::error::Result<()> {
    let existing_admin_context =
        super::get_bounded_context(pool, crate::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME).await?;
    if let Some(admin_context) = crate::bootstrap::stamp_admin_bounded_context(
        existing_admin_context.as_ref(),
        chrono::Utc::now(),
    ) {
        if let Err(e) = super::insert_bounded_context(pool, &admin_context).await {
            if super::get_bounded_context(pool, crate::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME)
                .await?
                .is_none()
            {
                return Err(e);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_changed_function_body_is_outdated_not_missing() {
        let expected = ["function f() aaaa", "table t", "column t.a bigint not null"];
        let actual = vec![
            "function f() bbbb".to_string(),
            "table t".to_string(),
            "column t.b text null".to_string(),
        ];
        let diff = diff_shape(&expected, &actual);
        assert_eq!(diff.outdated, ["function f()"]);
        assert_eq!(diff.missing, ["column t.a bigint not null"]);
        assert_eq!(diff.unexpected, ["column t.b text null"]);
    }

    #[test]
    fn registered_by_version_is_lazy() {
        let expected = ["column projections.registered_by_version bigint null"];
        let diff = diff_shape(&expected, &[]);
        assert!(diff.missing.is_empty());
        assert_eq!(diff.lazy.len(), 1);
    }

    #[test]
    fn a_compatible_record_lets_unknown_migrations_through() {
        let record = |min| RecordedLevel {
            level: SCHEMA_LEVEL + 1,
            min_compatible_level: min,
            upgraded_by: "9.9.9".into(),
            upgraded_at: Utc::now(),
        };
        assert!(compatibility_refusal(Some(&record(SCHEMA_LEVEL)), &[99]).is_none());
        assert!(compatibility_refusal(Some(&record(SCHEMA_LEVEL + 1)), &[]).is_some());
        assert!(compatibility_refusal(None, &[99]).is_some());
        assert!(compatibility_refusal(None, &[]).is_none());
    }
}
