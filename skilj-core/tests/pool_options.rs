//! `db::connect_with`/`PgPoolOptions` - a configurable connection pool
//! (a real production-readiness gap a `/code-review` pass surfaced: no
//! way to tune pool sizing at all before this, unlike every other
//! `SkiljBuilder` knob). `db::connect` itself is unchanged behaviour
//! (`PgPoolOptions::new()`, `sqlx`'s own bare default) - not re-tested
//! here, since every other test file in this crate already exercises it
//! implicitly by using it.
//!
//! Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj-core/tests/persistence.rs` - see that file's own doc comment
//! for the details, not repeated a third time here. Unlike that file,
//! this one needs the raw `database_url` string, not a shared `Pool` -
//! each test builds its own pool with its own options.

use skilj_core::db::{self, PgPoolOptions};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for these tests")
    })
}

struct TestDb {
    database_url: String,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

async fn test_database_url() -> Option<String> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.database_url.clone())
}

async fn provision() -> Option<TestDb> {
    let database_url = skilj_test_support::database_url("skilj_pool_options_test").await?;
    Some(TestDb { database_url })
}

/// The whole point: a pool built via `connect_with` actually carries the
/// caller's own options, not `sqlx`'s bare default - proven by reading
/// them straight back off the live pool (`Pool::options()`), not just
/// trusting that construction didn't error.
#[test]
fn connect_with_applies_the_given_pool_options() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let pool = db::connect_with(&database_url, PgPoolOptions::new().max_connections(3))
            .await
            .unwrap();
        assert_eq!(pool.options().get_max_connections(), 3);
    });
}

/// `db::connect` itself must still go through `sqlx`'s own bare default -
/// unchanged behaviour for every existing caller, not something this new
/// escape hatch was allowed to quietly alter.
#[test]
fn connect_still_uses_the_bare_sqlx_default() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();
        assert_eq!(
            pool.options().get_max_connections(),
            PgPoolOptions::new().get_max_connections(),
            "db::connect must still be sqlx's own bare default, unchanged"
        );
    });
}

/// What one connection has prepared and kept after three distinct
/// statements and the count itself - four, when nothing is evicted.
async fn statements_kept(pool: &db::Pool) -> i64 {
    for k in 1..=3 {
        let sql = format!("SELECT $1::int + {k}");
        let _: (i32,) = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .bind(1)
            .fetch_one(pool)
            .await
            .unwrap();
    }
    let (count,): (i64,) = sqlx::query_as("SELECT count(*) FROM pg_prepared_statements")
        .fetch_one(pool)
        .await
        .unwrap();
    count
}

/// docs/architecture.md §193: `connect_with_statement_cache`'s capacity
/// reaches every connection, and overrides the database URL's
/// `statement-cache-capacity`; without it the URL's applies, and
/// without either sqlx's default. Read back from the server's own
/// `pg_prepared_statements`, since sqlx has no getter for it.
#[test]
fn a_statement_cache_capacity_overrides_the_url_and_the_default() {
    runtime().block_on(async {
        let Some(database_url) = test_database_url().await else {
            return;
        };
        let separator = if database_url.contains('?') { '&' } else { '?' };
        let with_url_capacity = format!("{database_url}{separator}statement-cache-capacity=3");
        let one = || PgPoolOptions::new().max_connections(1);
        for (url, explicit, kept) in [
            (&with_url_capacity, Some(2), 2),
            (&with_url_capacity, None, 3),
            (&database_url, None, 4),
        ] {
            let pool = db::connect_with_statement_cache(url, one(), explicit)
                .await
                .unwrap();
            assert_eq!(
                statements_kept(&pool).await,
                kept,
                "{url} with {explicit:?}"
            );
            assert_eq!(
                db::effective_statement_cache_capacity(url, explicit),
                explicit.unwrap_or(if url == &database_url { 100 } else { 3 })
            );
        }
    });
}
