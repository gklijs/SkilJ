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

async fn test_database() -> Option<(String, Pool)> {
    let url =
        skilj_test_support::embedded_database_url("skilj_startup_bounded_contexts_test").await?;
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
