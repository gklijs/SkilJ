//! How `Skilj::build()` and its kin treat the bounded contexts already in
//! the database. Its own test binary and database: each test holds locks
//! on one bounded context's tables, which any other test's `build()`
//! would meet. Tests are serialized for the same reason.
//!
//! docs/architecture.md §157: `Skilj::build()`, the GraphQL schema build
//! and `forgetSubject`'s deadline sweep each list every bounded context,
//! then query each one's tables. A bounded context another instance
//! hard-deletes in between is skipped rather than failing the whole walk.
//!
//! docs/architecture.md §158: startup's schema patches take no table lock
//! on a bounded context that already has what they add.
//!
//! docs/architecture.md §186: `Skilj::build()` refuses a pool the
//! cross-instance listener would take whole.
use chrono::{SubsecRound, Utc};
use skilj::Skilj;
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::generate_token_id;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

/// This binary's one database, provisioned once per process.
///
/// The `OnceCell` matters more here than in the other test files: `test_database`
/// is called by every test, and `database_url` drops and recreates its
/// database on each call. Called per test it would wipe the database
/// between them, and - because the tests take the `SERIAL` lock *after*
/// this returns - concurrently mid-test.
static TEST_DB: tokio::sync::OnceCell<Option<(String, Pool)>> = tokio::sync::OnceCell::const_new();

async fn test_database() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(|| async {
            let url =
                skilj_test_support::database_url("skilj_startup_bounded_contexts_test").await?;
            let pool = match db::connect(&url).await {
                Ok(pool) => pool,
                Err(e) => {
                    eprintln!("skipping: connecting to PostgreSQL failed: {e}");
                    return None;
                }
            };
            if let Err(e) = db::migrate(&pool).await {
                eprintln!("skipping: migrating PostgreSQL failed: {e}");
                return None;
            }
            Some((url, pool))
        })
        .await
        .as_ref()
        .map(|(url, pool)| (url.clone(), pool.clone()))
}

/// One test at a time: another test's startup would otherwise be what
/// waits on the held table, not the task under test.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn seed_bounded_context(pool: &Pool) -> String {
    let name = format!("doomed_{}", generate_token_id());
    db::insert_bounded_context(
        pool,
        &BoundedContext {
            name: name.clone(),
            status: BoundedContextStatus::Active,
            created_at: Utc::now().trunc_subsecs(6),
            created_by: ContextCreator::SystemCreator,
            template: None,
        },
    )
    .await
    .unwrap();
    name
}

/// Another instance's hard delete of `name`, holding its `table` until
/// `task` is waiting on it, then dropping the bounded context under it.
/// Returns what `task` returned.
async fn delete_while_waiting<T: Send + 'static>(
    pool: &Pool,
    name: &str,
    table: &str,
    task: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let schema = format!("\"bc_{name}\"");
    let mut delete = pool.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE {schema}.{table} IN ACCESS EXCLUSIVE MODE"
    )))
    .execute(&mut *delete)
    .await
    .unwrap();

    let task = tokio::spawn(task);
    let waiting = format!("%{name}%");
    for attempt in 0.. {
        let (blocked,): (i64,) = sqlx::query_as(
            "SELECT count(*) FROM pg_stat_activity \
             WHERE wait_event_type = 'Lock' AND query LIKE $1",
        )
        .bind(&waiting)
        .fetch_one(pool)
        .await
        .unwrap();
        if blocked > 0 {
            break;
        }
        assert!(attempt < 1000, "never reached the bounded context");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&mut *delete)
        .await
        .unwrap();
    sqlx::query("DELETE FROM bounded_contexts WHERE name = $1")
        .bind(name)
        .execute(&mut *delete)
        .await
        .unwrap();
    delete.commit().await.unwrap();
    task.await.unwrap()
}

#[test]
fn a_bounded_context_deleted_during_startup_is_skipped() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let name = seed_bounded_context(&pool).await;

        let started = delete_while_waiting(&pool, &name, "events", async move {
            Skilj::builder(database_url)
                .pool_options(db::PgPoolOptions::new().max_connections(2))
                .build()
                .await
                .map(|_| ())
        })
        .await;
        if let Err(err) = started {
            panic!("startup failed on the deleted bounded context: {err}");
        }
    });
}

/// The GraphQL schema's projection types are built the same way - at
/// startup, after every registration change, and per caller - and skip a
/// deleted bounded context too.
#[test]
fn a_bounded_context_deleted_during_a_schema_build_is_skipped() {
    runtime().block_on(async {
        let Some((_, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let name = seed_bounded_context(&pool).await;

        let built = delete_while_waiting(&pool, &name, "projections", {
            let pool = pool.clone();
            async move {
                skilj_graphql::projection_types::build(&pool, None, &Default::default(), false)
                    .await
                    .map(|_| ())
            }
        })
        .await;
        if let Err(err) = built {
            panic!("the schema build failed on the deleted bounded context: {err}");
        }
    });
}

/// `forgetSubject` walks every bounded context's `deadlines` the same way,
/// and skips a deleted one too.
#[test]
fn a_bounded_context_deleted_while_forgetting_a_subject_is_skipped() {
    runtime().block_on(async {
        let Some((_, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let name = seed_bounded_context(&pool).await;

        let forgotten = delete_while_waiting(&pool, &name, "deadlines", {
            let pool = pool.clone();
            async move {
                db::forget_subject_in_deadlines(&pool, "customers", "customer", "42", Utc::now())
                    .await
            }
        })
        .await;
        match forgotten {
            Ok(n) => assert_eq!(n, 0),
            Err(err) => panic!("forgetting failed on the deleted bounded context: {err}"),
        }
    });
}

/// docs/architecture.md §158: every table of an existing bounded context
/// has a write in flight (`ROW EXCLUSIVE`), as it would on a live
/// deployment; restarting an instance must not wait for them. Each schema
/// patch used to take its lock - `ACCESS EXCLUSIVE` for a column,
/// `SHARE` for an index - before finding nothing to add.
#[test]
fn startup_takes_no_lock_on_an_up_to_date_bounded_context() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let name = seed_bounded_context(&pool).await;

        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT tablename FROM pg_tables WHERE schemaname = $1")
                .bind(format!("bc_{name}"))
                .fetch_all(&pool)
                .await
                .unwrap();
        let mut writes = pool.begin().await.unwrap();
        for (table,) in &tables {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "LOCK TABLE \"bc_{name}\".{table} IN ROW EXCLUSIVE MODE"
            )))
            .execute(&mut *writes)
            .await
            .unwrap();
        }

        let started = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            Skilj::builder(database_url)
                .pool_options(db::PgPoolOptions::new().max_connections(2))
                .build(),
        )
        .await;
        writes.rollback().await.unwrap();
        match started {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => panic!("startup failed: {err}"),
            Err(_) => panic!("startup waited on the in-flight writes"),
        }
    });
}

/// The other half of §158: a bounded context provisioned before a column
/// existed still gets it. A dropped column stays in the catalog, marked
/// dropped, so this also checks that it isn't taken as present.
#[test]
fn startup_still_adds_a_missing_column_and_index() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let name = seed_bounded_context(&pool).await;
        let schema = format!("\"bc_{name}\"");
        for ddl in [
            format!("DROP INDEX {schema}.events_by_correlation_id"),
            format!("ALTER TABLE {schema}.events DROP COLUMN metadata_causation_id"),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(ddl))
                .execute(&pool)
                .await
                .unwrap();
        }

        Skilj::builder(database_url)
            .pool_options(db::PgPoolOptions::new().max_connections(2))
            .build()
            .await
            .unwrap();

        let (column, index): (bool, bool) = sqlx::query_as(
            "SELECT \
                EXISTS (SELECT 1 FROM information_schema.columns \
                    WHERE table_schema = $1 AND table_name = 'events' \
                    AND column_name = 'metadata_causation_id'), \
                EXISTS (SELECT 1 FROM pg_indexes \
                    WHERE schemaname = $1 AND indexname = 'events_by_correlation_id')",
        )
        .bind(format!("bc_{name}"))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(column, "the dropped column wasn't added back");
        assert!(index, "the dropped index wasn't created again");
    });
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct MoneyDepositedPayload {
    amount: i64,
}

struct MoneyDeposited;

impl skilj::EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
}

/// docs/architecture.md §159: reconciliation reads a bounded context's
/// registration tables, so it runs after the schema patches that bring
/// an older bounded context's tables up to date - not before, where a
/// column added since (here `event_types.private_fields`) failed startup
/// before any patch could add it.
#[test]
fn reconciliation_runs_on_a_patched_bounded_context() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let name = seed_bounded_context(&pool).await;
        let role = skilj_core::access_control::Role {
            id: generate_token_id(),
            external_subject: format!("reconciler_{}", generate_token_id()),
            name: "Reconciliation Role".to_string(),
            superadmin: false,
            status: skilj_core::access_control::RoleStatus::Active,
            created_at: Utc::now().trunc_subsecs(6),
            revoked_at: None,
        };
        db::insert_role(&pool, &role).await.unwrap();
        db::insert_role_access_mapping(
            &pool,
            &skilj_core::access_control::RoleAccessMapping {
                role: role.clone(),
                bounded_context: db::get_bounded_context(&pool, &name)
                    .await
                    .unwrap()
                    .unwrap(),
                level: skilj_core::access_control::AccessLevel::Admin,
                can_read_sensitive: false,
                scope: None,
                capabilities: Default::default(),
                delegable: Default::default(),
                status: skilj_core::access_control::RoleStatus::Active,
                created_at: Utc::now().trunc_subsecs(6),
                revoked_at: None,
            },
        )
        .await
        .unwrap();
        let start = || {
            Skilj::builder(database_url.clone())
                .pool_options(db::PgPoolOptions::new().max_connections(2))
                .bounded_context(name.clone())
                .event_type::<MoneyDeposited>()
                .reconciliation_role(role.external_subject.clone())
                .build()
        };
        start().await.unwrap();

        // As a bounded context provisioned before private fields existed.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE \"bc_{name}\".event_types DROP COLUMN private_fields"
        )))
        .execute(&pool)
        .await
        .unwrap();

        if let Err(err) = start().await {
            panic!("startup failed on the older bounded context: {err}");
        }
        let event_type = db::get_event_type(&pool, &name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        assert!(event_type.private_fields.is_empty());
    });
}

/// Every column (with its type, nullability and default), index,
/// constraint and trigger of `bounded_context`'s schema, as sorted text
/// lines.
async fn schema_shape(pool: &Pool, bounded_context: &str) -> Vec<String> {
    let schema = format!("bc_{bounded_context}");
    let mut shape: Vec<String> =
        sqlx::query_as::<_, (String, String, String, String, Option<String>)>(
            "SELECT table_name, column_name, data_type, is_nullable, column_default \
             FROM information_schema.columns WHERE table_schema = $1",
        )
        .bind(&schema)
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(table, column, ty, nullable, default)| {
            // A serial column's default names its sequence, schema included.
            let default = default.map(|d| d.replace(&schema, "bc"));
            format!("column {table}.{column} {ty} nullable={nullable} default={default:?}")
        })
        .collect();
    shape.extend(
        sqlx::query_as::<_, (String,)>(
            "SELECT regexp_replace(indexdef, ' ON \"?' || $1 || '\"?\\.', ' ON ') \
             FROM pg_indexes WHERE schemaname = $1",
        )
        .bind(&schema)
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(def,)| format!("index {def}")),
    );
    shape.extend(
        sqlx::query_as::<_, (String, String, String)>(
            "SELECT c.relname, con.conname, pg_get_constraintdef(con.oid) \
             FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1",
        )
        .bind(&schema)
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(table, name, def)| {
            format!("constraint {table}.{name} {}", def.replace(&schema, "bc"))
        }),
    );
    shape.extend(
        sqlx::query_as::<_, (String,)>(
            "SELECT pg_get_triggerdef(t.oid) FROM pg_trigger t \
             JOIN pg_class c ON c.oid = t.tgrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND NOT t.tgisinternal",
        )
        .bind(&schema)
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(def,)| format!("trigger {}", def.replace(&schema, "bc"))),
    );
    shape.sort();
    shape
}

/// docs/architecture.md §159: a bounded context provisioned by skilj
/// v0.0.1 - the oldest release - comes out of the current version's
/// startup with the same columns, indexes, constraints and triggers as
/// one provisioned today.
/// Every table or column added since needs its startup patch; this is
/// what notices one that's missing. The bounded context is marked current
/// in this process first, as a background tick can (§204).
#[test]
fn startup_upgrades_a_v0_0_1_bounded_context_to_the_current_schema() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let current = seed_bounded_context(&pool).await;
        let old = seed_bounded_context(&pool).await;
        // What a background tick of another instance in this process can
        // do before the swap below: mark the fresh schema current. Startup
        // must patch the v0.0.1 one regardless (docs/architecture.md §204).
        let old_bc = db::get_bounded_context(&pool, &old).await.unwrap().unwrap();
        db::ensure_bounded_context_current(&pool, &old_bc)
            .await
            .unwrap();

        let schema = format!("\"bc_{old}\"");
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
            .execute(&pool)
            .await
            .unwrap();
        let v0_0_1 = include_str!("fixtures/v0_0_1_bounded_context.sql").replace("{schema}", &schema);
        sqlx::raw_sql(sqlx::AssertSqlSafe(v0_0_1))
            .execute(&pool)
            .await
            .unwrap();

        Skilj::builder(database_url)
            .pool_options(db::PgPoolOptions::new().max_connections(2))
            .build()
            .await
            .unwrap();
        // `registered_by_version` is added the first time an instance with
        // an application version registers a type there (§104), not at
        // every startup.
        db::ensure_registration_version_columns(&pool, &old)
            .await
            .unwrap();

        let expected = schema_shape(&pool, &current).await;
        let upgraded = schema_shape(&pool, &old).await;
        let missing: Vec<_> = expected.iter().filter(|l| !upgraded.contains(l)).collect();
        let extra: Vec<_> = upgraded.iter().filter(|l| !expected.contains(l)).collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "upgraded v0.0.1 schema differs from a fresh one\nmissing: {missing:#?}\nextra: {extra:#?}"
        );
    });
}

/// docs/architecture.md §186: the cross-instance listener keeps one pooled
/// connection for the process's lifetime. With a pool of one, every
/// request after `build()` waited out the acquire timeout and failed;
/// `build()` now refuses such a pool instead of starting.
#[test]
fn build_refuses_a_pool_the_listener_would_take_whole() {
    runtime().block_on(async {
        let Some((database_url, _pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        for max_connections in [0, 1] {
            let err = Skilj::builder(database_url.clone())
                .pool_options(db::PgPoolOptions::new().max_connections(max_connections))
                .build()
                .await
                .err()
                .unwrap_or_else(|| panic!("a pool of {max_connections} was accepted"));
            assert!(
                matches!(
                    err,
                    skilj_core::Error::Database(sqlx::Error::Configuration(_))
                ),
                "{err}"
            );
            assert!(err.to_string().contains("at least 2"), "{err}");
        }
        assert_eq!(skilj::MIN_POOL_CONNECTIONS, 2);
    });
}

// --- docs/architecture.md §249: upgrade preflight, migration, compatibility ---

const SHAPE_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../skilj-core/src/db/sql/bounded_context_shape.txt"
);

/// A bounded context whose schema is v0.0.1's, marked current in this
/// process first so no background tick of another test's instance patches
/// part of it meanwhile (docs/architecture.md §204).
async fn v0_0_1_bounded_context(pool: &Pool) -> String {
    let name = seed_bounded_context(pool).await;
    let bc = db::get_bounded_context(pool, &name).await.unwrap().unwrap();
    db::ensure_bounded_context_current(pool, &bc).await.unwrap();
    let schema = format!("\"bc_{name}\"");
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(pool)
        .await
        .unwrap();
    let v0_0_1 = include_str!("fixtures/v0_0_1_bounded_context.sql").replace("{schema}", &schema);
    sqlx::raw_sql(sqlx::AssertSqlSafe(v0_0_1))
        .execute(pool)
        .await
        .unwrap();
    name
}

async fn shape(pool: &Pool, bounded_context: &str) -> Vec<String> {
    let mut conn = pool.acquire().await.unwrap();
    db::bounded_context_shape(&mut conn, bounded_context)
        .await
        .unwrap()
}

/// What a preflight must leave exactly as it found it.
async fn persistent_state(pool: &Pool, bounded_context: &str) -> Vec<String> {
    let mut state = shape(pool, bounded_context).await;
    state.extend(
        sqlx::query_as::<_, (String,)>(
            "SELECT format('%s %s %s', version, success, encode(checksum, 'hex')) \
             FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(l,)| l),
    );
    state.extend(
        sqlx::query_as::<_, (String,)>(
            "SELECT format('%s %s %s', level, min_compatible_level, upgraded_at) \
             FROM skilj_schema_compatibility",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(l,)| l),
    );
    state.extend(
        sqlx::query_as::<_, (String,)>("SELECT name FROM bounded_contexts ORDER BY name")
            .fetch_all(pool)
            .await
            .unwrap()
            .into_iter()
            .map(|(l,)| l),
    );
    state
}

fn report_for<'r>(
    report: &'r skilj::preflight::PreflightReport,
    name: &str,
) -> &'r skilj::preflight::BoundedContextReport {
    report
        .bounded_contexts
        .iter()
        .find(|bc| bc.name == name)
        .unwrap_or_else(|| panic!("{name} not in the report"))
}

/// The expected shape `inspect` compares against is what provisioning
/// creates today. A change to provisioning or a patch re-records it:
/// `SKILJ_RECORD_SHAPE=1 cargo test -p skilj --test startup_bounded_contexts`.
#[test]
fn the_expected_shape_is_a_freshly_provisioned_bounded_context() {
    runtime().block_on(async {
        let Some((_, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let name = seed_bounded_context(&pool).await;
        let fresh = shape(&pool, &name).await;
        assert!(fresh.len() > 100, "{fresh:#?}");
        let recorded = format!(
            "# A bounded context's schema as provisioned today, as\n\
             # db::bounded_context_shape describes it (docs/architecture.md §249).\n\
             # Re-record: SKILJ_RECORD_SHAPE=1 cargo test -p skilj --test startup_bounded_contexts\n\
             {}\n",
            fresh.join("\n")
        );
        if std::env::var_os("SKILJ_RECORD_SHAPE").is_some() {
            std::fs::write(SHAPE_FIXTURE, &recorded).unwrap();
            return;
        }
        let expected: Vec<&str> = db::EXPECTED_BOUNDED_CONTEXT_SHAPE
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
        let missing: Vec<_> = fresh.iter().filter(|l| !expected.contains(&l.as_str())).collect();
        let extra: Vec<_> = expected.iter().filter(|l| !fresh.contains(&l.to_string())).collect();
        assert!(
            missing.is_empty() && extra.is_empty(),
            "the recorded shape is stale; re-record it with SKILJ_RECORD_SHAPE=1\n\
             not recorded: {missing:#?}\nrecorded, not provisioned: {extra:#?}"
        );
    });
}

/// `inspect` reports what startup would change on a v0.0.1 bounded context
/// and changes none of it; after `build()` it reports nothing.
#[test]
fn inspect_reports_a_v0_0_1_bounded_context_and_changes_nothing() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let old = v0_0_1_bounded_context(&pool).await;
        let before = persistent_state(&pool, &old).await;

        let options = skilj::preflight::InspectOptions::new();
        let report = skilj::preflight::inspect(&pool, &options).await.unwrap();
        assert_eq!(
            report.verdict,
            skilj::preflight::Verdict::UpgradeNeeded,
            "{report}"
        );
        let bc = report_for(&report, &old);
        assert!(
            bc.missing
                .contains(&"column events.metadata_causation_id text null".to_string()),
            "{report}"
        );
        assert!(
            bc.missing.iter().any(|l| l.starts_with("table deadlines")),
            "{report}"
        );
        let rewrites: Vec<&str> = bc.rewrites.iter().map(|r| r.name.as_str()).collect();
        assert!(rewrites.contains(&"tag_dictionary"), "{rewrites:?}");
        assert!(rewrites.contains(&"source_content_scrub"), "{rewrites:?}");
        assert!(bc.locks.iter().any(|l| l.table == "events"), "{report}");
        assert!(bc.unexpected.is_empty(), "{report}");
        assert_eq!(bc.drops, ["index events_by_tags on events"], "{report}");
        // Estimates by default; exact on request.
        assert!(bc.rewrites.iter().all(|r| !r.rows.exact));
        let exact = skilj::preflight::inspect(&pool, &options.clone().exact_counts(true))
            .await
            .unwrap();
        let events = report_for(&exact, &old)
            .rewrites
            .iter()
            .find(|r| r.name == "tag_dictionary")
            .unwrap()
            .rows;
        assert_eq!((events.value, events.exact), (Some(0), true));
        // Machine-readable, and back.
        let json = serde_json::to_string(&report).unwrap();
        assert_eq!(
            serde_json::from_str::<skilj::preflight::PreflightReport>(&json).unwrap(),
            report
        );

        assert_eq!(
            persistent_state(&pool, &old).await,
            before,
            "inspect changed something"
        );

        Skilj::builder(database_url)
            .pool_options(db::PgPoolOptions::new().max_connections(2))
            .build()
            .await
            .unwrap();
        let after = skilj::preflight::inspect(&pool, &options).await.unwrap();
        assert_eq!(
            after.verdict,
            skilj::preflight::Verdict::UpToDate,
            "{after}"
        );
        let bc = report_for(&after, &old);
        assert!(!bc.needs_upgrade() && bc.unexpected.is_empty(), "{after}");
        assert_eq!(
            bc.lazy.len(),
            3,
            "registered_by_version is added on first use"
        );
        let recorded = after.compatibility.recorded.as_ref().unwrap();
        assert_eq!(recorded.level, skilj::preflight::SCHEMA_LEVEL);
    });
}

/// A database no release has touched: everything is pending, and the
/// preflight leaves it without a single table.
#[test]
fn inspect_reports_an_empty_database_and_creates_nothing() {
    runtime().block_on(async {
        let Some((_, _pool)) = test_database().await else {
            return;
        };
        let Some(url) = skilj_test_support::database_url("skilj_preflight_empty_test").await else {
            return;
        };
        let pool = db::connect(&url).await.unwrap();
        let tables = || async {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        assert_eq!(tables().await, 0);
        let report = skilj::preflight::inspect(&pool, &skilj::preflight::InspectOptions::new())
            .await
            .unwrap();
        assert_eq!(report.verdict, skilj::preflight::Verdict::UpgradeNeeded);
        assert!(report.global.applied.is_empty());
        assert!(report.global.pending.len() >= 10, "{report}");
        assert!(!report.admin_context.exists);
        assert!(report.compatibility.recorded.is_none());
        assert_eq!(tables().await, 0, "inspect created something");
        pool.close().await;
    });
}

/// `migrate` with a lock timeout gives up on a table another transaction
/// holds; what it finished stays done, and running it again completes it.
#[test]
fn migrate_stops_at_its_lock_timeout_and_a_rerun_completes() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let old = v0_0_1_bounded_context(&pool).await;
        let options = skilj::preflight::MigrateOptions::new()
            .lock_timeout(std::time::Duration::from_millis(200));

        // In-flight writes on the v0.0.1 events table.
        let mut writes = pool.begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "LOCK TABLE \"bc_{old}\".events IN ROW EXCLUSIVE MODE"
        )))
        .execute(&mut *writes)
        .await
        .unwrap();
        let err = skilj::preflight::migrate(&database_url, &options)
            .await
            .expect_err("migrate got past a held lock");
        let lock_not_available = matches!(&err, skilj_core::Error::Database(e)
            if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("55P03"));
        assert!(lock_not_available, "{err:?}");
        writes.rollback().await.unwrap();
        // The patches before the one that waited are committed.
        let partial = shape(&pool, &old).await;
        assert!(
            partial
                .iter()
                .any(|l| l.starts_with("table idempotency_keys")),
            "{partial:#?}"
        );

        let report = skilj::preflight::migrate(&database_url, &options)
            .await
            .unwrap();
        assert!(report_for(&report.before, &old).needs_upgrade());
        assert_eq!(
            report.after.verdict,
            skilj::preflight::Verdict::UpToDate,
            "{}",
            report.after
        );
    });
}

/// Two migrations at once, as two deploy jobs or replicas would run them:
/// both finish, and the schema is current.
#[test]
fn concurrent_migrations_both_finish() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let old = v0_0_1_bounded_context(&pool).await;
        let options = skilj::preflight::MigrateOptions::new();
        let (a, b) = tokio::join!(
            skilj::preflight::migrate(&database_url, &options),
            skilj::preflight::migrate(&database_url, &options),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        for report in [&a, &b] {
            assert!(
                !report_for(&report.after, &old).needs_upgrade(),
                "{}",
                report.after
            );
        }
    });
}

/// `SchemaChanges::Refuse`: an outdated schema is refused, unchanged; a
/// current one starts.
#[test]
fn refuse_mode_refuses_an_outdated_schema_and_starts_on_a_current_one() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let old = v0_0_1_bounded_context(&pool).await;
        let before = persistent_state(&pool, &old).await;
        let refusing = || {
            Skilj::builder(database_url.clone())
                .pool_options(db::PgPoolOptions::new().max_connections(2))
                .schema_changes(skilj::SchemaChanges::Refuse)
                .build()
        };
        match refusing().await.map(|_| ()) {
            Err(err @ skilj_core::Error::SchemaUpgradeRequired { .. }) => {
                assert!(err.to_string().contains(&old), "{err}")
            }
            other => panic!("expected schema_upgrade_required, got {other:?}"),
        }
        assert_eq!(persistent_state(&pool, &old).await, before);

        skilj::preflight::migrate(&database_url, &skilj::preflight::MigrateOptions::new())
            .await
            .unwrap();
        refusing().await.unwrap();
    });
}

/// Sets the recorded compatibility row, returning what it held.
async fn set_recorded_level(pool: &Pool, level: i64, min: i64) {
    sqlx::query(
        "INSERT INTO skilj_schema_compatibility \
             (id, level, min_compatible_level, upgraded_by, upgraded_at) \
         VALUES (1, $1, $2, '9.9.9', now()) \
         ON CONFLICT (id) DO UPDATE SET level = $1, min_compatible_level = $2, \
             upgraded_by = '9.9.9'",
    )
    .bind(level)
    .bind(min)
    .execute(pool)
    .await
    .unwrap();
}

/// docs/architecture.md §249: what a release does with a database a newer
/// one has upgraded. The recorded compatibility level decides, not sqlx's
/// `VersionMissing`: a newer release that declares this one compatible
/// lets it start with migrations it doesn't know; one that doesn't, or
/// migrations recorded without any level, make it refuse with
/// `schema_too_new` before it changes anything.
#[test]
fn a_newer_releases_level_decides_whether_an_older_one_starts() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let level = skilj::preflight::SCHEMA_LEVEL;
        // A migration from a newer release: numbered past every embedded
        // one, recorded as applied the way sqlx records them.
        let newer: i64 = 99_999;
        sqlx::query(
            "INSERT INTO _sqlx_migrations \
             (version, description, success, checksum, execution_time) \
             VALUES ($1, 'from a newer release', true, '\\x00'::bytea, 0)",
        )
        .bind(newer)
        .execute(&pool)
        .await
        .unwrap();
        let start = || {
            Skilj::builder(database_url.clone())
                .pool_options(db::PgPoolOptions::new().max_connections(2))
                .build()
        };

        // Declared compatible: starts, and leaves the newer record alone.
        set_recorded_level(&pool, level + 1, level).await;
        let compatible = start().await.map(|_| ());
        let kept: (i64, String) = sqlx::query_as(
            "SELECT level, upgraded_by FROM skilj_schema_compatibility WHERE id = 1",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        // Not declared compatible.
        set_recorded_level(&pool, level + 1, level + 1).await;
        let too_old = start().await.map(|_| ());
        let preflight = skilj::preflight::inspect(&pool, &skilj::preflight::InspectOptions::new())
            .await
            .unwrap();

        // No level recorded at all, but a migration this release doesn't know.
        sqlx::query("DELETE FROM skilj_schema_compatibility")
            .execute(&pool)
            .await
            .unwrap();
        let unrecorded = start().await.map(|_| ());

        sqlx::query("DELETE FROM _sqlx_migrations WHERE version = $1")
            .bind(newer)
            .execute(&pool)
            .await
            .unwrap();
        set_recorded_level(&pool, level, skilj::preflight::MIN_COMPATIBLE_SCHEMA_LEVEL).await;

        compatible.unwrap();
        assert_eq!(kept, (level + 1, "9.9.9".to_string()));
        for outcome in [too_old, unrecorded] {
            match outcome {
                Err(err @ skilj_core::Error::SchemaTooNew { .. }) => {
                    assert_eq!(
                        skilj_core::error::SkiljRejection::code(&err),
                        "schema_too_new"
                    )
                }
                other => panic!("expected schema_too_new, got {other:?}"),
            }
        }
        assert_eq!(preflight.verdict, skilj::preflight::Verdict::Refused);
        assert_eq!(preflight.refusals[0].code, "schema_too_new");
        assert_eq!(preflight.global.unknown, [newer]);
    });
}

/// A Role with an Admin grant on `bounded_context`, for reconciliation.
async fn reconciler_for(pool: &Pool, bounded_context: &str) -> skilj_core::access_control::Role {
    let role = skilj_core::access_control::Role {
        id: generate_token_id(),
        external_subject: format!("reconciler_{}", generate_token_id()),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: skilj_core::access_control::RoleStatus::Active,
        created_at: Utc::now().trunc_subsecs(6),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();
    db::insert_role_access_mapping(
        pool,
        &skilj_core::access_control::RoleAccessMapping {
            role: role.clone(),
            bounded_context: db::get_bounded_context(pool, bounded_context)
                .await
                .unwrap()
                .unwrap(),
            level: skilj_core::access_control::AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            capabilities: Default::default(),
            delegable: Default::default(),
            status: skilj_core::access_control::RoleStatus::Active,
            created_at: Utc::now().trunc_subsecs(6),
            revoked_at: None,
        },
    )
    .await
    .unwrap();
    role
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct MoneyWithdrawnPayload {
    amount: i64,
}

struct MoneyWithdrawn;

impl skilj::EventType for MoneyWithdrawn {
    type Payload = MoneyWithdrawnPayload;
    const NAME: &'static str = "MoneyWithdrawn";
}

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
struct MoneyDepositedAsTextPayload {
    amount: String,
}

/// `MoneyDeposited` with an incompatible schema.
struct MoneyDepositedAsText;

impl skilj::EventType for MoneyDepositedAsText {
    type Payload = MoneyDepositedAsTextPayload;
    const NAME: &'static str = "MoneyDeposited";
}

/// `SkiljBuilder::preflight` reports what reconciliation would do with the
/// builder's types - new, unchanged, kept for a newer version, refused -
/// and writes nothing (its connections are read-only).
#[test]
fn builder_preflight_previews_registrations_and_writes_nothing() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let name = seed_bounded_context(&pool).await;
        let role = reconciler_for(&pool, &name).await;
        let builder = |version: u64| {
            Skilj::builder(database_url.clone())
                .pool_options(db::PgPoolOptions::new().max_connections(2))
                .bounded_context(name.clone())
                .reconciliation_role(role.external_subject.clone())
                .application_version(version)
        };
        builder(2)
            .event_type::<MoneyDeposited>()
            .build()
            .await
            .unwrap();

        let state = || async {
            let mut state = persistent_state(&pool, &name).await;
            state.extend(
                sqlx::query_as::<_, (String, String, Option<i64>)>(sqlx::AssertSqlSafe(format!(
                    "SELECT name, schema, registered_by_version FROM \"bc_{name}\".event_types \
                     ORDER BY name"
                )))
                .fetch_all(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|row| format!("{row:?}")),
            );
            state
        };
        let before = state().await;
        let outcome = |report: &skilj::preflight::PreflightReport, type_name: &str| {
            report
                .registrations
                .iter()
                .find(|r| r.name == type_name && r.bounded_context == name)
                .map(|r| r.outcome.clone())
        };

        let report = builder(2)
            .event_type::<MoneyDeposited>()
            .event_type::<MoneyWithdrawn>()
            .preflight()
            .await
            .unwrap();
        assert_eq!(
            report.verdict,
            skilj::preflight::Verdict::UpToDate,
            "{report}"
        );
        assert_eq!(
            outcome(&report, "MoneyDeposited").as_deref(),
            Some("unchanged"),
            "{report}"
        );
        assert_eq!(
            outcome(&report, "MoneyWithdrawn").as_deref(),
            Some("registered"),
            "{report}"
        );

        let older = builder(1)
            .event_type::<MoneyDeposited>()
            .preflight()
            .await
            .unwrap();
        assert_eq!(
            outcome(&older, "MoneyDeposited").as_deref(),
            Some("kept_newer"),
            "{older}"
        );

        let incompatible = builder(2)
            .event_type::<MoneyDepositedAsText>()
            .preflight()
            .await
            .unwrap();
        assert_eq!(
            incompatible.verdict,
            skilj::preflight::Verdict::Refused,
            "{incompatible}"
        );
        assert!(!incompatible.refusals.is_empty());

        assert_eq!(state().await, before, "the preflight changed something");
    });
}
