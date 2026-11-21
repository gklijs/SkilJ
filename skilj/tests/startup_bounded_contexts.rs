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
use jiff::Timestamp;
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
            created_at: Timestamp::now()
                .round(
                    jiff::TimestampRound::new()
                        .smallest(jiff::Unit::Microsecond)
                        .mode(jiff::RoundMode::Trunc),
                )
                .unwrap(),
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
                .concurrent_index_interval(None)
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
                db::forget_subject_in_deadlines(
                    &pool,
                    "customers",
                    "customer",
                    "42",
                    Timestamp::now(),
                )
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
                .concurrent_index_interval(None)
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
            .concurrent_index_interval(None)
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
            created_at: Timestamp::now()
                .round(
                    jiff::TimestampRound::new()
                        .smallest(jiff::Unit::Microsecond)
                        .mode(jiff::RoundMode::Trunc),
                )
                .unwrap(),
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
                created_at: Timestamp::now()
                    .round(
                        jiff::TimestampRound::new()
                            .smallest(jiff::Unit::Microsecond)
                            .mode(jiff::RoundMode::Trunc),
                    )
                    .unwrap(),
                revoked_at: None,
            },
        )
        .await
        .unwrap();
        let start = || {
            Skilj::builder(database_url.clone())
                .concurrent_index_interval(None)
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

        Skilj::builder(database_url).concurrent_index_interval(None)
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
                .concurrent_index_interval(None)
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
            .concurrent_index_interval(None)
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

async fn assert_managed_checks(pool: &Pool, schema: &str) {
    let refs = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.event_tag_refs (tag_id, event_type_name, sequence)
         VALUES (1, 'example', 0), (1, 'example', 0)"
    )))
    .execute(pool)
    .await
    .unwrap();
    assert_eq!(refs.rows_affected(), 2);

    for (statement, code, constraint) in [
        (
            format!("INSERT INTO {schema}.sequence(id, next_value) VALUES (false, 0)"),
            "23514",
            "sequence_id_check",
        ),
        (
            format!("INSERT INTO {schema}.sequence(next_value) VALUES (0)"),
            "23505",
            "sequence_pkey",
        ),
        (
            format!(
                "INSERT INTO {schema}.blobs (id, sha256, size, media_type, storage_key,
                created_at, grace_from, state)
                VALUES ('bad', '', 0, '', '', now(), now(), 'unknown')"
            ),
            "23514",
            "blobs_state_check",
        ),
        (
            format!(
                "INSERT INTO {schema}.blob_pins (blob_id, holder_kind, holder_id)
                VALUES ('test_blob', 'unknown', 'holder')"
            ),
            "23514",
            "blob_pins_holder_kind_check",
        ),
        (
            format!("UPDATE {schema}.deadlines SET status = 'unknown' WHERE id = 'forgotten'"),
            "23514",
            "deadlines_status_check",
        ),
    ] {
        let err = sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(pool)
            .await
            .unwrap_err();
        assert_constraint(&err, code, constraint);
    }
}

fn assert_constraint(err: &sqlx::Error, code: &str, constraint: &str) {
    let db = err.as_database_error().unwrap();
    assert_eq!(db.code().as_deref(), Some(code), "{err}");
    assert_eq!(db.constraint(), Some(constraint), "{err}");
}

#[test]
fn bounded_context_checks_validate_and_retry_after_legacy_data_is_corrected() {
    runtime().block_on(async {
        let Some((url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let fresh = seed_bounded_context(&pool).await;
        let old = seed_bounded_context(&pool).await;
        let fresh_schema = format!("\"bc_{fresh}\"");
        let old_schema = format!("\"bc_{old}\"");
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE {old_schema}.sequence DROP COLUMN id;
             ALTER TABLE {old_schema}.event_tag_refs REPLICA IDENTITY DEFAULT;
             ALTER TABLE {old_schema}.deadlines DROP CONSTRAINT deadlines_status_check;
             ALTER TABLE {old_schema}.blobs DROP CONSTRAINT blobs_state_check;
             ALTER TABLE {old_schema}.blob_pins DROP CONSTRAINT blob_pins_holder_kind_check;"
        )))
        .execute(&pool)
        .await
        .unwrap();

        for schema in [&fresh_schema, &old_schema] {
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {schema}.blobs (id, sha256, size, media_type, storage_key,
                    created_at, grace_from, state)
                 VALUES ('test_blob', '', 0, '', '', now(), now(), 'referenced');
                 INSERT INTO {schema}.deadlines (
                    id, schedule_name, fire_at, tags, target_bounded_context, target_command_type,
                    payload, status, created_at)
                 VALUES ('forgotten', 'test', now(), '[]', 'test', 'test', '{{}}', 'forgotten', now());"
            )))
            .execute(&pool)
            .await
            .unwrap();
        }
        assert_managed_checks(&pool, &fresh_schema).await;

        let details = skilj::preflight::InspectOptions::new();
        let options = skilj::preflight::MigrateOptions::new();
        let report = skilj::preflight::inspect(&pool, &details).await.unwrap();
        let pending = report_for(&report, &old);
        assert!(pending.needs_upgrade(), "{report}");
        assert!(pending.locks.iter().any(|lock| {
            lock.table == "event_tag_refs"
                && lock.mode == "ACCESS EXCLUSIVE"
                && lock.reason.contains("sets replica identity FULL")
        }), "{report}");

        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {old_schema}.deadlines (
                id, schedule_name, fire_at, tags, target_bounded_context, target_command_type,
                payload, status, created_at)
             VALUES ('bad', 'test', now(), '[]', 'test', 'test', '{{}}', 'unknown', now())"
        )))
        .execute(&pool)
        .await
        .unwrap();

        let err = skilj::preflight::migrate(&url, &options).await.unwrap_err();
        let skilj_core::Error::Database(err) = err else {
            panic!("expected validation error, got {err}");
        };
        assert_constraint(&err, "23514", "deadlines_status_check");
        let report = skilj::preflight::inspect(&pool, &details).await.unwrap();
        let pending = report_for(&report, &old);
        assert!(pending.needs_upgrade(), "{report}");
        assert!(pending.unexpected.is_empty(), "{report}");
        assert!(pending.locks.iter().any(|lock| {
            lock.table == "deadlines"
                && lock.mode == "SHARE UPDATE EXCLUSIVE"
                && lock.reason == "validates constraint deadlines_status_check; writes go on"
        }), "{report}");
        assert!(report.to_string().contains("validates constraint deadlines_status_check on deadlines"));
        let refused = Skilj::builder(&url)
            .schema_changes(skilj::SchemaChanges::Refuse)
            .build()
            .await;
        assert!(matches!(refused, Err(skilj_core::Error::SchemaUpgradeRequired { .. })));

        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {old_schema}.deadlines SET status = 'cancelled' WHERE id = 'bad'"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let migration = skilj::preflight::migrate(&url, &options).await.unwrap();
        assert!(!report_for(&migration.after, &old).needs_upgrade(), "{}", migration.after);
        assert_eq!(shape(&pool, &old).await, shape(&pool, &fresh).await);
        assert_managed_checks(&pool, &old_schema).await;
        let status: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT status FROM {old_schema}.deadlines WHERE id = 'forgotten'"
        )))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status, "forgotten");
        skilj::preflight::migrate(&url, &options).await.unwrap();
    });
}

#[test]
fn concurrent_upgrades_install_the_sequence_singleton() {
    runtime().block_on(async {
        let Some((_, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let name = seed_bounded_context(&pool).await;
        let schema = format!("\"bc_{name}\"");
        let sql = |sql: &str| sqlx::AssertSqlSafe(sql.replace("{schema}", &schema));
        sqlx::query(sql("ALTER TABLE {schema}.sequence DROP COLUMN id"))
            .execute(&pool)
            .await
            .unwrap();

        let mut blocker = pool.begin().await.unwrap();
        sqlx::query(sql("LOCK TABLE {schema}.sequence IN SHARE MODE"))
            .execute(&mut *blocker)
            .await
            .unwrap();
        let bc = db::get_bounded_context(&pool, &name)
            .await
            .unwrap()
            .unwrap();
        let contenders = (0..2)
            .map(|_| {
                let (pool, bc) = (pool.clone(), bc.clone());
                tokio::spawn(async move { db::upgrade_bounded_context(&pool, &bc).await })
            })
            .collect::<Vec<_>>();
        let relation = format!("{schema}.sequence");
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let waiting: i64 = sqlx::query_scalar(
                    "SELECT count(*) FROM pg_locks WHERE relation = to_regclass($1)
                     AND mode = 'AccessExclusiveLock' AND NOT granted",
                )
                .bind(&relation)
                .fetch_one(&pool)
                .await
                .unwrap();
                if waiting == 2 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("both upgrades should wait for the singleton table lock");
        blocker.commit().await.unwrap();
        for contender in contenders {
            contender.await.unwrap().unwrap();
        }
        let seed: (bool, i64) = sqlx::query_as(sql("SELECT id, next_value FROM {schema}.sequence"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(seed, (true, -1));

        // An upgrade must also restore a missing true-only check even if
        // the primary key is already present.
        sqlx::query(sql(
            "ALTER TABLE {schema}.sequence DROP CONSTRAINT sequence_id_check",
        ))
        .execute(&pool)
        .await
        .unwrap();
        db::upgrade_bounded_context(&pool, &bc).await.unwrap();
        let err = sqlx::query(sql(
            "INSERT INTO {schema}.sequence(id, next_value) VALUES (false, 0)",
        ))
        .execute(&pool)
        .await
        .unwrap_err();
        assert_constraint(&err, "23514", "sequence_id_check");
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
                .concurrent_index_interval(None)
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
                .concurrent_index_interval(None)
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
        created_at: Timestamp::now()
            .round(
                jiff::TimestampRound::new()
                    .smallest(jiff::Unit::Microsecond)
                    .mode(jiff::RoundMode::Trunc),
            )
            .unwrap(),
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
            created_at: Timestamp::now()
                .round(
                    jiff::TimestampRound::new()
                        .smallest(jiff::Unit::Microsecond)
                        .mode(jiff::RoundMode::Trunc),
                )
                .unwrap(),
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
                .concurrent_index_interval(None)
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

// --- docs/architecture.md §256: indexes built concurrently ---------------
//
// Every Skilj these tests build sets `concurrent_index_interval(None)` unless
// the test is about the background task, and is shut down, so no loop
// outlives its test and repairs what another test set up. Each test works on
// bounded contexts of its own, hard-deleted by `Contexts` when it ends, also
// when it panics.

/// Bounded contexts a test created, hard-deleted on drop - on a thread of
/// its own with its own runtime and connection, so it also runs while a
/// panic unwinds.
struct Contexts {
    url: String,
    names: Vec<String>,
}

impl Contexts {
    fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            names: Vec::new(),
        }
    }

    async fn seed(&mut self, pool: &Pool) -> String {
        let name = seed_bounded_context(pool).await;
        self.names.push(name.clone());
        name
    }
}

impl Drop for Contexts {
    fn drop(&mut self) {
        let url = self.url.clone();
        let names = std::mem::take(&mut self.names);
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let Ok(pool) = db::connect(&url).await else {
                    return;
                };
                // Whatever a test left waiting (a barrier, a held key)
                // goes with its connections; end them first.
                let _ = sqlx::query(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE application_name = $1 AND pid <> pg_backend_pid()",
                )
                .bind(db::CONCURRENT_INDEX_APPLICATION_NAME)
                .execute(&pool)
                .await;
                for name in names {
                    let _ = db::hard_delete_bounded_context(&pool, &name).await;
                }
                pool.close().await;
            });
        })
        .join();
    }
}

/// A builder for these tests: a small pool, no background task.
fn index_builder(url: &str) -> skilj::SkiljBuilder {
    Skilj::builder(url.to_string())
        .concurrent_index_interval(None)
        .pool_options(db::PgPoolOptions::new().max_connections(2))
}

/// Puts a bounded context in the shape releases up to 0.4 left it in:
/// `deadlines_due`, no `deadlines_claimable`.
async fn old_index_set(pool: &Pool, name: &str) {
    let schema = format!("\"bc_{name}\"");
    for ddl in [
        format!("DROP INDEX IF EXISTS {schema}.deadlines_claimable"),
        format!(
            "CREATE INDEX IF NOT EXISTS deadlines_due ON {schema}.deadlines (fire_at) \
             WHERE status = 'pending'"
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(ddl))
            .execute(pool)
            .await
            .unwrap();
    }
}

/// `(oid, valid)` of an index of the bounded context, if there is one.
async fn index_of(pool: &Pool, name: &str, index: &str) -> Option<(u32, bool)> {
    let row: Option<(sqlx::postgres::types::Oid, bool)> = sqlx::query_as(
        "SELECT x.indexrelid, x.indisvalid FROM pg_index x \
         WHERE x.indexrelid = to_regclass($1)",
    )
    .bind(format!("\"bc_{name}\".{index}"))
    .fetch_optional(pool)
    .await
    .unwrap();
    row.map(|(oid, valid)| (oid.0, valid))
}

/// The catalog's view of `deadlines_claimable`: access method, key
/// columns, uniqueness, validity and the predicate (deparsed: fine here,
/// nothing holds the table).
async fn claimable_definition(
    pool: &Pool,
    name: &str,
) -> (String, Vec<String>, bool, bool, String) {
    sqlx::query_as(
        "SELECT am.amname::text, \
                ARRAY(SELECT a.attname::text FROM unnest(x.indkey::int2[]) WITH ORDINALITY k(attnum, ord) \
                      JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = k.attnum ORDER BY k.ord), \
                x.indisunique, x.indisvalid, pg_get_expr(x.indpred, x.indrelid) \
           FROM pg_index x JOIN pg_class c ON c.oid = x.indexrelid JOIN pg_am am ON am.oid = c.relam \
          WHERE x.indexrelid = to_regclass($1)",
    )
    .bind(format!("\"bc_{name}\".deadlines_claimable"))
    .fetch_one(pool)
    .await
    .unwrap()
}

/// An open transaction with an uncommitted deadline in the bounded
/// context: a concurrent build or drop waits for it, in a phase the test
/// can see.
async fn barrier(pool: &Pool, name: &str) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = pool.begin().await.unwrap();
    insert_deadline(&mut *tx, name).await;
    tx
}

async fn insert_deadline<'e>(executor: impl sqlx::PgExecutor<'e>, name: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO \"bc_{name}\".deadlines \
         (id, schedule_name, fire_at, tags, target_bounded_context, target_command_type, \
          payload, status, created_at) \
         VALUES ($1, 's', now() + interval '1 hour', '{{}}', 'bc', 'Cmd', 'x', 'pending', now())"
    )))
    .bind(generate_token_id())
    .execute(executor)
    .await
    .unwrap();
}

/// Waits until a statement starting with `prefix` waits on a lock, and
/// returns its backend's pid.
async fn waiting_statement(pool: &Pool, prefix: &str) -> i32 {
    for _ in 0..200 {
        let pid: Option<(i32,)> = sqlx::query_as(
            "SELECT pid FROM pg_stat_activity \
             WHERE wait_event_type = 'Lock' AND query LIKE $1 LIMIT 1",
        )
        .bind(format!("{prefix}%"))
        .fetch_optional(pool)
        .await
        .unwrap();
        if let Some((pid,)) = pid {
            return pid;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("no '{prefix}' ever waited");
}

/// How many concurrent index build connections are open.
async fn build_connections(pool: &Pool) -> i64 {
    let (n,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1")
            .bind(db::CONCURRENT_INDEX_APPLICATION_NAME)
            .fetch_one(pool)
            .await
            .unwrap();
    n
}

/// A write from a connection of its own, which must commit within 5 s.
async fn write_does_not_wait(url: &str, name: &str) {
    let pool = db::connect(url).await.unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        insert_deadline(&pool, name),
    )
    .await
    .expect("a write waited on the concurrent index work");
    pool.close().await;
}

/// Leaves an invalid `deadlines_claimable`: a concurrent build cancelled
/// while it waits for a writer.
async fn interrupt_a_build(pool: &Pool, url: &str, name: &str) {
    let schema = format!("\"bc_{name}\"");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP INDEX IF EXISTS {schema}.deadlines_claimable"
    )))
    .execute(pool)
    .await
    .unwrap();
    let writer = barrier(pool, name).await;
    let build_pool = db::connect(url).await.unwrap();
    let ddl = format!(
        "CREATE INDEX CONCURRENTLY deadlines_claimable ON {schema}.deadlines (fire_at) \
         WHERE status IN ('pending', 'firing')"
    );
    let build = tokio::spawn(async move {
        let result = sqlx::raw_sql(sqlx::AssertSqlSafe(ddl))
            .execute(&build_pool)
            .await;
        build_pool.close().await;
        result
    });
    let pid = waiting_statement(pool, "CREATE INDEX CONCURRENTLY deadlines_claimable").await;
    sqlx::query("SELECT pg_cancel_backend($1)")
        .bind(pid)
        .execute(pool)
        .await
        .unwrap();
    assert!(build.await.unwrap().is_err(), "the build was not cancelled");
    writer.rollback().await.unwrap();
    assert_eq!(
        index_of(pool, name, "deadlines_claimable")
            .await
            .map(|(_, v)| v),
        Some(false),
        "the cancelled build left no invalid index"
    );
}

fn is_sqlstate(err: &skilj_core::Error, code: &str) -> bool {
    matches!(err, skilj_core::Error::Database(e)
        if e.as_database_error().and_then(|d| d.code()).as_deref() == Some(code))
}

/// The poll reads due deadlines through `deadlines_claimable`, not the
/// whole table: on every CI major, with the poll's own query text (E1, E8,
/// E9 in the issue #218 plan).
#[test]
fn deadline_poll_uses_deadlines_claimable() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let name = contexts.seed(&pool).await;
        let schema = format!("\"bc_{name}\"");
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.deadlines (id, schedule_name, fire_at, tags, target_bounded_context, \
               target_command_type, payload, status, created_at, resolved_at) \
             SELECT 'r' || g, 's', now() - interval '1 day' * random() * 30, '{{}}', 'bc', 'Cmd', \
               repeat('x', 200), 'fired', now(), now() FROM generate_series(1, 100000) g; \
             INSERT INTO {schema}.deadlines (id, schedule_name, fire_at, tags, target_bounded_context, \
               target_command_type, payload, status, created_at) \
             SELECT 'p' || g, 's', now() + interval '1 hour' * (random() * 10 - 5), '{{}}', 'bc', 'Cmd', \
               'x', 'pending', now() FROM generate_series(1, 300) g; \
             INSERT INTO {schema}.deadlines (id, schedule_name, fire_at, tags, target_bounded_context, \
               target_command_type, payload, status, created_at, firing_at) \
             SELECT 'f' || g, 's', now() - interval '1 hour', '{{}}', 'bc', 'Cmd', 'x', 'firing', \
               now(), now() - interval '10 minutes' FROM generate_series(1, 5) g; \
             ANALYZE {schema}.deadlines;"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let query = db::due_deadlines_sql(&schema)
            .replace("$1", "(now() - interval '5 minutes')")
            .replace("$2", "now()");
        let (plan,): (serde_json::Value,) =
            sqlx::query_as(sqlx::AssertSqlSafe(format!("EXPLAIN (FORMAT JSON) {query}")))
                .fetch_one(&pool)
                .await
                .unwrap();
        let plan = plan.to_string();
        assert!(plan.contains("\"Index Name\":\"deadlines_claimable\""), "{plan}");
        assert!(!plan.contains("\"Node Type\":\"Seq Scan\""), "{plan}");
    });
}

/// A new bounded context gets `deadlines_claimable` at provisioning, and
/// one from before gets the same index from startup, built concurrently,
/// with `deadlines_due` dropped; a second start finds nothing to do.
#[test]
fn startup_replaces_deadlines_due_concurrently() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let fresh = contexts.seed(&pool).await;
        let old = contexts.seed(&pool).await;
        old_index_set(&pool, &old).await;

        let skilj = index_builder(&database_url).build().await.unwrap().0;
        skilj.shutdown(std::time::Duration::from_secs(20)).await;

        for name in [&fresh, &old] {
            let (am, columns, unique, valid, predicate) = claimable_definition(&pool, name).await;
            assert_eq!(
                (am.as_str(), columns, unique, valid),
                ("btree", vec!["fire_at".to_string()], false, true)
            );
            assert!(
                predicate.contains("pending") && predicate.contains("firing"),
                "{predicate}"
            );
            assert_eq!(index_of(&pool, name, "deadlines_due").await, None);
        }
        assert!(!db::pending_concurrent_index_work(&pool)
            .await
            .unwrap()
            .contains(&old));
    });
}

/// docs/architecture.md §256: building the index waits for a write in
/// flight but lets a later write through - a plain `CREATE INDEX` would
/// queue it behind its `SHARE` lock.
#[test]
fn creating_the_index_does_not_block_writes() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let name = contexts.seed(&pool).await;
        old_index_set(&pool, &name).await;
        let writer = barrier(&pool, &name).await;

        let build = tokio::spawn({
            let pool = pool.clone();
            let name = name.clone();
            async move { db::build_concurrent_indexes(&pool, &name).await }
        });
        waiting_statement(&pool, "CREATE INDEX CONCURRENTLY").await;
        write_does_not_wait(&database_url, &name).await;
        writer.rollback().await.unwrap();
        build.await.unwrap().unwrap();
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable")
                .await
                .map(|(_, v)| v),
            Some(true)
        );
    });
}

/// The same for dropping `deadlines_due`.
#[test]
fn dropping_deadlines_due_does_not_block_writes() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let name = contexts.seed(&pool).await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE INDEX deadlines_due ON \"bc_{name}\".deadlines (fire_at) WHERE status = 'pending'"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let writer = barrier(&pool, &name).await;

        let build = tokio::spawn({
            let pool = pool.clone();
            let name = name.clone();
            async move { db::build_concurrent_indexes(&pool, &name).await }
        });
        let pid = waiting_statement(&pool, "DROP INDEX CONCURRENTLY").await;
        let (application_name,): (String,) =
            sqlx::query_as("SELECT application_name FROM pg_stat_activity WHERE pid = $1")
                .bind(pid)
                .fetch_one(&pool)
                .await
                .unwrap();
        // The drop runs on the build's own connection, never on one of the
        // pool's (PLAN-23 in the issue #218 plan review).
        assert_eq!(application_name, db::CONCURRENT_INDEX_APPLICATION_NAME);
        write_does_not_wait(&database_url, &name).await;
        writer.rollback().await.unwrap();
        build.await.unwrap().unwrap();
        assert_eq!(index_of(&pool, &name, "deadlines_due").await, None);
    });
}

/// An interrupted build leaves an invalid index. During a first upgrade
/// the recorded level still says so, and `Refuse` refuses until `migrate`
/// finished; after one, it is concurrent index work: reported, no reason
/// to refuse, repaired by the background task, by a default-mode start,
/// and by `migrate`.
#[test]
fn an_interrupted_build_is_repaired() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let name = contexts.seed(&pool).await;
        let migrate = || async {
            skilj::preflight::migrate(&database_url, &skilj::preflight::MigrateOptions::new())
                .await
                .unwrap();
        };
        let inspect = || async {
            skilj::preflight::inspect(&pool, &skilj::preflight::InspectOptions::new())
                .await
                .unwrap()
        };
        let refuse = || index_builder(&database_url).schema_changes(skilj::SchemaChanges::Refuse);

        // (i) An interrupted first upgrade.
        migrate().await;
        interrupt_a_build(&pool, &database_url, &name).await;
        let (oid, _) = index_of(&pool, &name, "deadlines_claimable").await.unwrap();
        set_recorded_level(&pool, 1, 1).await;
        assert_eq!(
            inspect().await.verdict,
            skilj::preflight::Verdict::UpgradeNeeded
        );
        let err = refuse()
            .build()
            .await
            .err()
            .expect("Refuse started on an old level");
        assert!(
            matches!(err, skilj_core::Error::SchemaUpgradeRequired { .. }),
            "{err:?}"
        );
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable").await,
            Some((oid, false))
        );
        migrate().await;
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable")
                .await
                .map(|(_, v)| v),
            Some(true)
        );

        // (ii) Damage after a finished upgrade.
        interrupt_a_build(&pool, &database_url, &name).await;
        let (oid, _) = index_of(&pool, &name, "deadlines_claimable").await.unwrap();
        let report = inspect().await;
        assert_eq!(
            report.verdict,
            skilj::preflight::Verdict::UpToDate,
            "{report}"
        );
        let bc = report
            .bounded_contexts
            .iter()
            .find(|bc| bc.name == name)
            .unwrap();
        assert!(!bc.needs_upgrade());
        assert_eq!(
            bc.lazy,
            ["index deadlines_claimable on deadlines invalid".to_string()]
        );
        assert!(bc
            .locks
            .iter()
            .any(|l| l.table == "deadlines" && l.mode == "SHARE UPDATE EXCLUSIVE"));
        let text = report.to_string();
        assert!(
            text.contains("rebuilds index deadlines_claimable concurrently"),
            "{text}"
        );
        // (a) A Refuse instance starts and builds nothing at startup.
        let skilj = refuse().build().await.unwrap().0;
        skilj.shutdown(std::time::Duration::from_secs(20)).await;
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable").await,
            Some((oid, false))
        );
        // (b) Its background task repairs it.
        let skilj = refuse()
            .concurrent_index_interval(Some(std::time::Duration::from_secs(1)))
            .build()
            .await
            .unwrap()
            .0;
        let mut repaired = false;
        for _ in 0..100 {
            if index_of(&pool, &name, "deadlines_claimable")
                .await
                .is_some_and(|(o, v)| v && o != oid)
            {
                repaired = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let report = skilj.shutdown(std::time::Duration::from_secs(20)).await;
        assert!(repaired, "the background task did not rebuild the index");
        assert!(report.stopped.contains(&"concurrent_indexes"), "{report:?}");
        // (c) A default-mode start repairs it.
        interrupt_a_build(&pool, &database_url, &name).await;
        let skilj = index_builder(&database_url).build().await.unwrap().0;
        skilj.shutdown(std::time::Duration::from_secs(20)).await;
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable")
                .await
                .map(|(_, v)| v),
            Some(true)
        );
        // (d) So does migrate.
        interrupt_a_build(&pool, &database_url, &name).await;
        migrate().await;
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable")
                .await
                .map(|(_, v)| v),
            Some(true)
        );
    });
}

/// docs/architecture.md §256 and §158: with nothing to do, the check is a
/// catalog read - no lock on the table, not even the one deparsing an
/// index definition takes, and not the build lock either.
#[test]
fn an_up_to_date_context_takes_no_lock_for_concurrent_indexes() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let name = contexts.seed(&pool).await;
        let mut holder = pool.begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "LOCK TABLE \"bc_{name}\".deadlines IN ACCESS EXCLUSIVE MODE"
        )))
        .execute(&mut *holder)
        .await
        .unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!(
                "skilj_concurrent_index:\"bc_{name}\".deadlines_claimable"
            ))
            .execute(&mut *holder)
            .await
            .unwrap();

        let checked = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let pending = db::pending_concurrent_index_work(&pool).await.unwrap();
            let outcome = db::build_concurrent_indexes(&pool, &name).await.unwrap();
            (pending, outcome)
        })
        .await;
        holder.rollback().await.unwrap();
        let (pending, outcome) = checked.expect("the check waited on a lock");
        assert!(!pending.contains(&name));
        assert_eq!(outcome, db::ConcurrentIndexOutcome::UpToDate);
    });
}

/// Two builders at once: the second waits for the first's build lock by
/// polling, then finds the index built. Both succeed; one index.
#[test]
fn racing_builders_build_one_index() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let name = contexts.seed(&pool).await;
        old_index_set(&pool, &name).await;
        let writer = barrier(&pool, &name).await;

        let spawn = || {
            let pool = pool.clone();
            let name = name.clone();
            tokio::spawn(async move { db::build_concurrent_indexes(&pool, &name).await })
        };
        let first = spawn();
        let second = spawn();
        waiting_statement(&pool, "CREATE INDEX CONCURRENTLY").await;
        // Release only once the second builder is polling for the lock.
        for _ in 0..200 {
            if build_connections(&pool).await >= 2 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            build_connections(&pool).await >= 2,
            "the builders never overlapped"
        );
        writer.rollback().await.unwrap();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable")
                .await
                .map(|(_, v)| v),
            Some(true)
        );
        assert_eq!(index_of(&pool, &name, "deadlines_due").await, None);
    });
}

/// `migrate`'s lock timeout bounds waiting for another process's build
/// lock, and the build's own waits, failing the way every other lock
/// timeout does (`55P03`); a rerun completes. A lock won by any path
/// leaves the configured timeout, not the last attempt's 1 ms.
#[test]
fn migrate_times_out_waiting_for_an_index_build() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let name = contexts.seed(&pool).await;
        let short = skilj::preflight::MigrateOptions::new()
            .lock_timeout(std::time::Duration::from_millis(200));

        // Another process holds the build lock.
        old_index_set(&pool, &name).await;
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!(
                "skilj_concurrent_index:\"bc_{name}\".deadlines_claimable"
            ))
            .execute(&mut *holder)
            .await
            .unwrap();
        let err = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            skilj::preflight::migrate(&database_url, &short),
        )
        .await
        .expect("migrate waited past its lock timeout")
        .expect_err("migrate got past a held build lock");
        assert!(is_sqlstate(&err, "55P03"), "{err:?}");
        holder.rollback().await.unwrap();
        skilj::preflight::migrate(&database_url, &short)
            .await
            .unwrap();
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable")
                .await
                .map(|(_, v)| v),
            Some(true)
        );

        // The build itself waits on a writer past the timeout.
        old_index_set(&pool, &name).await;
        let writer = barrier(&pool, &name).await;
        let err = skilj::preflight::migrate(&database_url, &short)
            .await
            .expect_err("the build got past a write in flight");
        assert!(is_sqlstate(&err, "55P03"), "{err:?}");
        writer.rollback().await.unwrap();
        skilj::preflight::migrate(&database_url, &short)
            .await
            .unwrap();
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable")
                .await
                .map(|(_, v)| v),
            Some(true)
        );

        // A writer holding the build for ~300 ms, under a 2 s timeout.
        old_index_set(&pool, &name).await;
        let writer = barrier(&pool, &name).await;
        let release = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            writer.rollback().await.unwrap();
        });
        skilj::preflight::migrate(
            &database_url,
            &skilj::preflight::MigrateOptions::new()
                .lock_timeout(std::time::Duration::from_secs(2)),
        )
        .await
        .unwrap();
        release.await.unwrap();
        assert_eq!(
            index_of(&pool, &name, "deadlines_claimable")
                .await
                .map(|(_, v)| v),
            Some(true)
        );
    });
}

/// Something other than the index this release creates, under the name of
/// one it manages or replaces, is left alone and reported as unknown - not
/// as work, and no reason to refuse.
#[test]
fn a_foreign_index_is_left_alone() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let full = contexts.seed(&pool).await;
        let unique = contexts.seed(&pool).await;
        for ddl in [
            format!("DROP INDEX \"bc_{full}\".deadlines_claimable"),
            format!("CREATE INDEX deadlines_claimable ON \"bc_{full}\".deadlines (fire_at)"),
            format!("CREATE UNIQUE INDEX deadlines_due ON \"bc_{unique}\".deadlines (id)"),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(ddl))
                .execute(&pool)
                .await
                .unwrap();
        }
        let before = (
            index_of(&pool, &full, "deadlines_claimable").await,
            index_of(&pool, &unique, "deadlines_due").await,
        );

        let skilj = index_builder(&database_url).build().await.unwrap().0;
        skilj.shutdown(std::time::Duration::from_secs(20)).await;
        skilj::preflight::migrate(&database_url, &skilj::preflight::MigrateOptions::new())
            .await
            .unwrap();
        assert_eq!(
            (
                index_of(&pool, &full, "deadlines_claimable").await,
                index_of(&pool, &unique, "deadlines_due").await,
            ),
            before
        );
        let report = skilj::preflight::inspect(&pool, &skilj::preflight::InspectOptions::new())
            .await
            .unwrap();
        for name in [&full, &unique] {
            let bc = report
                .bounded_contexts
                .iter()
                .find(|bc| &bc.name == name)
                .unwrap();
            assert!(!bc.needs_upgrade(), "{bc:?}");
            assert!(bc.lazy.is_empty(), "{bc:?}");
            assert!(bc.locks.is_empty(), "{bc:?}");
            assert!(
                bc.unexpected.iter().any(|l| l.ends_with(" foreign")),
                "{bc:?}"
            );
        }
        index_builder(&database_url)
            .schema_changes(skilj::SchemaChanges::Refuse)
            .build()
            .await
            .unwrap()
            .0
            .shutdown(std::time::Duration::from_secs(20))
            .await;
    });
}

/// Several bounded contexts to upgrade, a two-connection pool: startup
/// builds them one at a time, on one extra connection at most.
#[test]
fn several_old_contexts_with_a_small_pool() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let mut names = Vec::new();
        for _ in 0..3 {
            let name = contexts.seed(&pool).await;
            old_index_set(&pool, &name).await;
            names.push(name);
        }
        let sampling = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let sampler = tokio::spawn({
            let pool = pool.clone();
            let sampling = sampling.clone();
            async move {
                let mut most = 0;
                while sampling.load(std::sync::atomic::Ordering::Relaxed) {
                    most = most.max(build_connections(&pool).await);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                most
            }
        });
        let skilj = index_builder(&database_url).build().await.unwrap().0;
        sampling.store(false, std::sync::atomic::Ordering::Relaxed);
        skilj.shutdown(std::time::Duration::from_secs(20)).await;
        assert!(sampler.await.unwrap() <= 1);
        for name in &names {
            assert_eq!(
                index_of(&pool, name, "deadlines_claimable")
                    .await
                    .map(|(_, v)| v),
                Some(true)
            );
            assert_eq!(index_of(&pool, name, "deadlines_due").await, None);
        }
    });
}

/// A bounded context an older instance provisions after startup (or
/// after the deploy job's migrate) gets its index from the background
/// task; a `Refuse` instance starts without building anything and leaves
/// it to its task.
#[test]
fn the_background_task_upgrades_a_context_provisioned_late() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let second = std::time::Duration::from_secs(1);
        let upgraded = |name: String| {
            let pool = pool.clone();
            async move {
                for _ in 0..100 {
                    if index_of(&pool, &name, "deadlines_claimable")
                        .await
                        .is_some_and(|(_, v)| v)
                        && index_of(&pool, &name, "deadlines_due").await.is_none()
                    {
                        return true;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                false
            }
        };

        // Default mode.
        let skilj = index_builder(&database_url)
            .concurrent_index_interval(Some(second))
            .build()
            .await
            .unwrap()
            .0;
        let late = contexts.seed(&pool).await;
        old_index_set(&pool, &late).await;
        assert!(
            upgraded(late.clone()).await,
            "the task did not upgrade {late}"
        );
        let report = skilj.shutdown(std::time::Duration::from_secs(20)).await;
        assert!(report.stopped.contains(&"concurrent_indexes"), "{report:?}");

        // The deploy-job setup: migrate, then a late context, then a
        // Refuse instance - which must not build at startup (the build
        // lock is held, so a synchronous build would block build()), and
        // whose task then does.
        skilj::preflight::migrate(&database_url, &skilj::preflight::MigrateOptions::new())
            .await
            .unwrap();
        let late = contexts.seed(&pool).await;
        old_index_set(&pool, &late).await;
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!(
                "skilj_concurrent_index:\"bc_{late}\".deadlines_claimable"
            ))
            .execute(&mut *holder)
            .await
            .unwrap();
        let skilj = tokio::time::timeout(
            std::time::Duration::from_secs(20),
            index_builder(&database_url)
                .schema_changes(skilj::SchemaChanges::Refuse)
                .concurrent_index_interval(Some(second))
                .build(),
        )
        .await
        .expect("a Refuse start waited on the index build")
        .unwrap()
        .0;
        assert_eq!(index_of(&pool, &late, "deadlines_claimable").await, None);
        holder.rollback().await.unwrap();
        assert!(
            upgraded(late.clone()).await,
            "the task did not upgrade {late}"
        );
        skilj.shutdown(std::time::Duration::from_secs(20)).await;
    });
}

/// Only the schemas of registered bounded contexts are touched, whatever
/// another schema is called.
#[test]
fn a_schema_outside_the_registry_is_left_alone() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let schema = format!("bc_unregistered_{}", generate_token_id());
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE SCHEMA \"{schema}\"; \
             CREATE TABLE \"{schema}\".deadlines (id text, status text, fire_at timestamptz); \
             CREATE INDEX deadlines_due ON \"{schema}\".deadlines (fire_at) WHERE status = 'pending';"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let skilj = index_builder(&database_url).build().await.unwrap().0;
        skilj.shutdown(std::time::Duration::from_secs(20)).await;
        let (claimable, due): (bool, bool) = sqlx::query_as(
            "SELECT to_regclass($1) IS NOT NULL, to_regclass($2) IS NOT NULL",
        )
        .bind(format!("\"{schema}\".deadlines_claimable"))
        .bind(format!("\"{schema}\".deadlines_due"))
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA \"{schema}\" CASCADE")))
            .execute(&pool)
            .await
            .unwrap();
        assert_eq!((claimable, due), (false, true));
    });
}

/// The build runs on a connection the pool's `after_connect` hook
/// prepared (a deployment may switch role or search path there).
#[test]
fn the_build_connection_runs_the_pools_after_connect_hook() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let name = contexts.seed(&pool).await;
        old_index_set(&pool, &name).await;
        let hooked: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<i32>>> =
            std::sync::Arc::default();
        let options = db::PgPoolOptions::new().max_connections(2).after_connect({
            let hooked = hooked.clone();
            move |conn, _| {
                let hooked = hooked.clone();
                Box::pin(async move {
                    let (pid,): (i32,) = sqlx::query_as("SELECT pg_backend_pid()")
                        .fetch_one(&mut *conn)
                        .await?;
                    hooked.lock().unwrap().insert(pid);
                    Ok(())
                })
            }
        });
        let writer = barrier(&pool, &name).await;
        let start = tokio::spawn({
            let url = database_url.clone();
            async move {
                Skilj::builder(url)
                    .concurrent_index_interval(None)
                    .pool_options(options)
                    .build()
                    .await
            }
        });
        let pid = waiting_statement(&pool, "CREATE INDEX CONCURRENTLY").await;
        assert!(
            hooked.lock().unwrap().contains(&pid),
            "the build ran on an unhooked connection"
        );
        writer.rollback().await.unwrap();
        start
            .await
            .unwrap()
            .unwrap()
            .0
            .shutdown(std::time::Duration::from_secs(20))
            .await;
    });
}

/// A role that may create tables in a bounded context's schema but doesn't
/// own its `deadlines` table can't build the index: the error is
/// `insufficient_privilege`, which the background task reports once and
/// leaves to a migrate run as the owner.
#[test]
fn a_role_that_does_not_own_the_table_gets_insufficient_privilege() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&database_url);
        let name = contexts.seed(&pool).await;
        old_index_set(&pool, &name).await;
        let role = format!("skilj_app_{}", generate_token_id().to_lowercase());
        let schema = format!("\"bc_{name}\"");
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE ROLE \"{role}\" LOGIN PASSWORD 'app'; \
             GRANT USAGE, CREATE ON SCHEMA {schema} TO \"{role}\"; \
             GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO \"{role}\"; \
             GRANT SELECT ON bounded_contexts TO \"{role}\";"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let options: sqlx::postgres::PgConnectOptions = database_url.parse().unwrap();
        let app = db::PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options.username(&role).password("app"))
            .await
            .unwrap();
        let result = db::build_concurrent_indexes(&app, &name).await;
        app.close().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "REVOKE ALL ON ALL TABLES IN SCHEMA {schema} FROM \"{role}\"; \
             REVOKE ALL ON SCHEMA {schema} FROM \"{role}\"; \
             REVOKE ALL ON bounded_contexts FROM \"{role}\"; DROP ROLE \"{role}\";"
        )))
        .execute(&pool)
        .await
        .unwrap();
        let err = result.expect_err("a non-owner built the index");
        assert!(is_sqlstate(&err, "42501"), "{err:?}");
    });
}

#[test]
fn required_proposal_index_refuses_duplicates_and_recovers() {
    runtime().block_on(async {
        let Some((url, pool)) = test_database().await else { return };
        let _serial = SERIAL.lock().await;
        let mut contexts = Contexts::new(&url);
        let name = contexts.seed(&pool).await;
        let schema = format!("\"bc_{name}\"");
        let open = "command_proposals_open_idempotency";
        let old = "command_proposals_pending_idempotency";
        assert!(index_of(&pool, &name, open).await.unwrap().1);
        let run = |statement| {
            let pool = &pool;
            async move { sqlx::raw_sql(sqlx::AssertSqlSafe(statement)).execute(pool).await.unwrap() }
        };
        run(format!(
            "DROP INDEX {schema}.{open};
             CREATE UNIQUE INDEX {old} ON {schema}.command_proposals (command_type_name, metadata_client_id, idempotency_key)
              WHERE status = 'pending' AND idempotency_key IS NOT NULL;
             INSERT INTO {schema}.command_types (name, schema, schema_version, rest_trigger_allowed)
              VALUES ('Transfer', '{{}}', 1, true);
             INSERT INTO {schema}.command_proposals
              (id, command_type_name, payload, metadata_version, metadata_client_id, metadata_created_at,
               status, idempotency_key, created_at)
              VALUES ('first', 'Transfer', '{{}}', 1, 'caller', now(), 'approving', 'shared', now()),
                     ('second', 'Transfer', '{{}}', 1, 'caller', now(), 'pending', 'shared', now()),
                     ('other', 'Transfer', '{{}}', 1, 'caller', now(), 'pending', 'separate', now())",
        )).await;
        set_recorded_level(&pool, 2, 1).await;
        let report = skilj::preflight::inspect(&pool, &skilj::preflight::InspectOptions::new()).await.unwrap();
        assert_eq!(report.verdict, skilj::preflight::Verdict::Refused);
        let duplicate = report.refusals.iter().find(|item| item.code == "duplicate_open_proposals").unwrap();
        assert!(duplicate.detail.contains("first (approving)"), "{duplicate:?}");
        assert!(duplicate.detail.contains("second (pending)"), "{duplicate:?}");
        assert!(!duplicate.detail.contains("other (pending)"), "{duplicate:?}");
        let refusal = index_builder(&url).schema_changes(skilj::SchemaChanges::Refuse)
            .build().await.err().expect("the required index is missing").to_string();
        assert!(refusal.contains("first (approving)"), "{refusal}");
        assert!(refusal.contains("minimum compatible schema level 2"), "{refusal}");
        let options = skilj::preflight::MigrateOptions::new();
        let err = skilj::preflight::migrate(&url, &options).await.unwrap_err();
        assert!(err.to_string().contains("second (pending)"), "{err}");
        assert!(index_of(&pool, &name, old).await.unwrap().1);
        assert_eq!(index_of(&pool, &name, open).await, None);

        let late = db::build_concurrent_indexes(&pool, &name).await.unwrap_err();
        assert!(late.to_string().contains("first (approving)"), "{late}");
        assert!(!index_of(&pool, &name, open).await.unwrap().1);
        assert!(index_of(&pool, &name, old).await.unwrap().1);
        let role = skilj_core::access_control::Role {
            id: generate_token_id(),
            external_subject: generate_token_id(),
            name: "Admin".into(),
            superadmin: false,
            status: skilj_core::access_control::RoleStatus::Active,
            created_at: Timestamp::now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &role).await.unwrap();
        let mapping = skilj_core::access_control::RoleAccessMapping {
            role,
            bounded_context: db::get_bounded_context(&pool, &name).await.unwrap().unwrap(),
            level: skilj_core::access_control::AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            capabilities: Default::default(),
            delegable: Default::default(),
            status: skilj_core::access_control::RoleStatus::Active,
            created_at: Timestamp::now(),
            revoked_at: None,
        };
        let rejected = db::reject_command_proposal(&pool, &name, "second", &mapping, None, Timestamp::now()).await.unwrap();
        assert_eq!(rejected.status, skilj_core::approvals::CommandProposalStatus::Rejected);
        let upgraded = skilj::preflight::migrate(&url, &options).await.unwrap();
        assert_eq!(upgraded.after.compatibility.recorded.unwrap().min_compatible_level, 2);
        assert!(index_of(&pool, &name, open).await.unwrap().1);
        assert_eq!(index_of(&pool, &name, old).await, None);
        let (app, _) = index_builder(&url).schema_changes(skilj::SchemaChanges::Refuse)
            .build().await.unwrap();
        app.shutdown(std::time::Duration::from_secs(5)).await;

        run(format!(
            "DROP INDEX {schema}.{open};
             CREATE UNIQUE INDEX {open} ON {schema}.command_proposals
                 (command_type_name, metadata_client_id, idempotency_key)
              WHERE status = 'pending' AND idempotency_key IS NOT NULL"
        )).await;
        let report = skilj::preflight::inspect(&pool, &skilj::preflight::InspectOptions::new()).await.unwrap();
        assert!(report.refusals.iter().any(|item| item.code == "approval_index_foreign"));
    });
}
