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
                skilj_graphql::projection_types::build(&pool, None)
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

/// Every column (with its type, nullability and default), index and
/// constraint of `bounded_context`'s schema, as sorted text lines.
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
    shape.sort();
    shape
}

/// docs/architecture.md §159: a bounded context provisioned by skilj
/// v0.0.1 - the oldest release - comes out of the current version's
/// startup with the same columns, indexes and constraints as one
/// provisioned today.
/// Every table or column added since needs its startup patch; this is
/// what notices one that's missing.
#[test]
fn startup_upgrades_a_v0_0_1_bounded_context_to_the_current_schema() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        let _serial = SERIAL.lock().await;
        let current = seed_bounded_context(&pool).await;
        let old = seed_bounded_context(&pool).await;

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
