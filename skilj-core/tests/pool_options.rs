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
    _embedded: Option<postgresql_embedded::PostgreSQL>,
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
    if let Ok(database_url) = std::env::var("DATABASE_URL") {
        return Some(TestDb {
            database_url,
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
    let database_name = "skilj_pool_options_test";
    if let Err(e) = server.create_database(database_name).await {
        eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
        return None;
    }
    let database_url = server.settings().url(database_name);
    Some(TestDb {
        database_url,
        _embedded: Some(server),
    })
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
