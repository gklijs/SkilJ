//! Codeberg issue #15: a real, timed A/B proof that
//! `stream::iter(...).buffer_unordered(N)` concurrent fan-out over
//! per-bounded-context startup warm-up work is actually faster than the
//! plain sequential `for` loop it replaced in `SkiljBuilder::build()` -
//! not just "looks more parallel" from the code shape alone. Exercises
//! the exact same two calls that loop makes
//! (`EventCache::warm`/`ensure_idempotency_keys_table`) against the same
//! set of real, seeded bounded contexts, timed both ways in the same
//! test run so the comparison is apples-to-apples against identical
//! data. Same `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! every other `skilj-core/tests/*.rs` file - see `submit_command.rs`'s
//! own doc comment for the details, not repeated a third time here.

use futures_util::stream::{self, StreamExt};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::generate_token_id;

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    pool: Pool,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for startup_warm_up_concurrency tests")
    })
}

async fn test_pool() -> Option<Pool> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.pool.clone())
}

async fn provision() -> Option<TestDb> {
    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        let pool = match db::connect(&database_url).await {
            Ok(pool) => pool,
            Err(e) => {
                eprintln!("skipping: DATABASE_URL is set but connecting failed: {e}");
                return None;
            }
        };
        if let Err(e) = db::migrate(&pool).await {
            eprintln!("skipping: DATABASE_URL migration failed: {e}");
            return None;
        }
        return Some(TestDb {
            pool,
            _embedded: None,
        });
    }

    let mut server = postgresql_embedded::PostgreSQL::default();
    if let Err(e) = server.setup().await {
        eprintln!(
            "skipping: DATABASE_URL not set and embedded PostgreSQL setup failed \
             (no network egress to fetch the binary, or a missing system library \
             like libxml2 it links against): {e}"
        );
        return None;
    }
    if let Err(e) = server.start().await {
        eprintln!("skipping: embedded PostgreSQL failed to start: {e}");
        return None;
    }
    let database_name = "skilj_startup_warm_up_concurrency_test";
    if let Err(e) = server.create_database(database_name).await {
        eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
        return None;
    }
    let pool = match db::connect(&server.settings().url(database_name)).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to embedded PostgreSQL failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating embedded PostgreSQL failed: {e}");
        return None;
    }
    Some(TestDb {
        pool,
        _embedded: Some(server),
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
}

async fn seed_bounded_context(pool: &Pool) -> BoundedContext {
    let bc = BoundedContext {
        name: unique_name("bc"),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    bc
}

/// One bounded context's own share of the startup warm-up work -
/// exactly what `SkiljBuilder::build()`'s own loop body does per
/// iteration.
async fn warm_up_one(pool: &Pool, event_cache: &EventCache, name: &str) {
    event_cache.warm(pool, name).await.unwrap();
    db::ensure_idempotency_keys_table(pool, name).await.unwrap();
}

const BOUNDED_CONTEXT_COUNT: usize = 40;
const CONCURRENCY: usize = 16;

#[test]
fn concurrent_warm_up_is_meaningfully_faster_than_sequential() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };

        let mut names = Vec::with_capacity(BOUNDED_CONTEXT_COUNT);
        for _ in 0..BOUNDED_CONTEXT_COUNT {
            names.push(seed_bounded_context(&pool).await.name);
        }

        // Sequential first - the exact shape the startup loop used to be
        // (a plain `for` with `.await` inside), timed against the same
        // real, seeded bounded contexts the concurrent run below uses.
        let event_cache_sequential = EventCache::new(100);
        let sequential_start = std::time::Instant::now();
        for name in &names {
            warm_up_one(&pool, &event_cache_sequential, name).await;
        }
        let sequential_elapsed = sequential_start.elapsed();

        // Concurrent - what SkiljBuilder::build() actually does now.
        let event_cache_concurrent = EventCache::new(100);
        let concurrent_start = std::time::Instant::now();
        stream::iter(&names)
            .map(|name| warm_up_one(&pool, &event_cache_concurrent, name))
            .buffer_unordered(CONCURRENCY)
            .collect::<Vec<()>>()
            .await;
        let concurrent_elapsed = concurrent_start.elapsed();

        eprintln!(
            "startup warm-up over {BOUNDED_CONTEXT_COUNT} bounded contexts: \
             sequential {sequential_elapsed:?}, concurrent (x{CONCURRENCY}) {concurrent_elapsed:?}"
        );

        // A generous margin (concurrent must be no more than 90% of
        // sequential), not a tight bound - this is a real local Postgres
        // with low round-trip latency, so the gap here understates what a
        // real network deployment would see; the point is proving
        // `buffer_unordered` genuinely overlaps I/O, not pinning an exact
        // speedup ratio that would make this test flaky.
        assert!(
            concurrent_elapsed < sequential_elapsed.mul_f64(0.9),
            "concurrent warm-up ({concurrent_elapsed:?}) was not meaningfully faster than \
             sequential ({sequential_elapsed:?})"
        );
    });
}
