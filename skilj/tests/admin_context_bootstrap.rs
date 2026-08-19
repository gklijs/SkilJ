//! Real end-to-end regression guard for the drift audit's #4 fix (see
//! project memory `skilj-drift-audit-2026-08-18`): `default BoundedContext
//! admin`/`default SystemCreator skilj` were never actually seeded at
//! startup, leaving `AdminContextIsPermanent`/`SystemCreatedContextIsTheAdminContext`
//! vacuous and letting a superadmin `AddBoundedContext("admin")` for
//! real. `Skilj::builder(...).build()` now stamps it - see
//! `bootstrap::stamp_admin_bounded_context`'s own doc comment. Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj/tests/event_creation_atomicity.rs`, see its own doc comment for
//! the details, not repeated a third time here.

use skilj::Skilj;
use skilj_core::bootstrap::{self, ContextCreator};
use skilj_core::db;
use skilj_core::event_store::BoundedContextStatus;

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for admin_context_bootstrap tests")
    })
}

async fn test_db() -> Option<String> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.database_url.clone())
}

async fn connect_and_migrate(database_url: &str, label: &str) -> Option<()> {
    let pool = match db::connect(database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to {label} failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating {label} failed: {e}");
        return None;
    }
    Some(())
}

async fn provision() -> Option<TestDb> {
    let database_url = if let Ok(database_url) = std::env::var("DATABASE_URL") {
        database_url
    } else {
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
        let database_name = "skilj_admin_context_bootstrap_test";
        if let Err(e) = server.create_database(database_name).await {
            eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
            return None;
        }
        let url = server.settings().url(database_name);
        connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            _embedded: Some(server),
        });
    };

    connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb {
        database_url,
        _embedded: None,
    })
}

/// Note: this test's own database is process-wide-shared with every
/// other test in this binary that reaches the same `TEST_DB` (there is
/// only one per test binary/database name), so `Skilj::builder(...).build()`
/// here may run against a database an earlier test in this same file
/// already stamped `admin` in - which is exactly the "restart" scenario
/// this test wants: the row must already exist and be left untouched,
/// whether this is truly the process's first startup against it or not.
#[test]
fn build_stamps_the_admin_context_once_and_never_restamps_it() {
    runtime().block_on(async {
        let Some(database_url) = test_db().await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();

        let (_skilj, _report) = Skilj::builder(database_url.clone()).build().await.unwrap();

        let admin = db::get_bounded_context(&pool, bootstrap::ADMIN_BOUNDED_CONTEXT_NAME)
            .await
            .unwrap()
            .expect("build() must stamp the admin BoundedContext");
        assert_eq!(admin.status, BoundedContextStatus::Active);
        assert_eq!(admin.created_by, ContextCreator::SystemCreator);
        let first_created_at = admin.created_at;

        // A second "startup" against the same database - the row must
        // not be restamped: same created_at, same created_by, still
        // SystemCreator.
        let (_skilj, _report) = Skilj::builder(database_url.clone()).build().await.unwrap();

        let admin_again = db::get_bounded_context(&pool, bootstrap::ADMIN_BOUNDED_CONTEXT_NAME)
            .await
            .unwrap()
            .expect("admin must still exist after a second build()");
        assert_eq!(admin_again.created_at, first_created_at);
        assert_eq!(admin_again.created_by, ContextCreator::SystemCreator);
    });
}

/// A real, ordinary scenario for this specific line, not a contrived one:
/// several replicas of the same service, each calling `.build()` against
/// one shared database at deploy time, can genuinely race on which one
/// stamps `admin` first. Two builds fired truly concurrently
/// (`tokio::join!`, not just two tests the harness happens to schedule in
/// parallel - `postgresql_embedded`'s own per-database serialisation
/// would hide the race that way) against a *fresh* database - a genuine
/// duplicate-key error surfaced this exact race the first time this test
/// was written (`provision_bounded_context_schema`'s own `CREATE SCHEMA
/// bc_admin` colliding), which is what `SkiljBuilder::build()`'s own
/// check-then-insert-then-recheck now tolerates.
#[test]
fn two_concurrent_builds_against_a_fresh_database_both_succeed() {
    runtime().block_on(async {
        // Deliberately not `test_db()`'s own shared database, and always
        // embedded Postgres regardless of `DATABASE_URL` (unlike every
        // other test here) - this test needs a database nothing has
        // touched yet, so the race is between these two builds
        // specifically, not resolved by an earlier test in this binary
        // (or a shared `DATABASE_URL` database from a prior run) having
        // already stamped `admin`.
        let mut server = postgresql_embedded::PostgreSQL::default();
        if server.setup().await.is_err() {
            eprintln!("skipping: embedded PostgreSQL setup failed (see other tests' own message)");
            return;
        }
        if server.start().await.is_err() {
            eprintln!("skipping: embedded PostgreSQL failed to start");
            return;
        }
        let database_name = "skilj_admin_context_race_test";
        if server.create_database(database_name).await.is_err() {
            eprintln!("skipping: embedded PostgreSQL create_database failed");
            return;
        }
        let database_url = server.settings().url(database_name);
        let Some(()) = connect_and_migrate(&database_url, "embedded PostgreSQL").await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();

        let (first, second) = tokio::join!(
            Skilj::builder(database_url.clone()).build(),
            Skilj::builder(database_url.clone()).build(),
        );
        first.unwrap();
        second.unwrap();

        let admin = db::get_bounded_context(&pool, bootstrap::ADMIN_BOUNDED_CONTEXT_NAME)
            .await
            .unwrap()
            .expect("one of the two concurrent builds must have stamped admin");
        assert_eq!(admin.created_by, ContextCreator::SystemCreator);
    });
}

/// Once `admin` is really seeded, a superadmin's own `AddBoundedContext("admin")`
/// is rejected for real by `existing_contexts` sourced from the database,
/// not just by a hand-built fixture in a pure-function test (see
/// `skilj-core/tests/bounded_context_lifecycle.rs`'s own
/// `add_bounded_context_rejects_the_admin_name_even_though_admin_was_never_added_by_this_rule`,
/// which never touched a real database at all).
#[test]
fn a_superadmin_cannot_really_add_a_bounded_context_named_admin() {
    runtime().block_on(async {
        let Some(database_url) = test_db().await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();

        Skilj::builder(database_url.clone()).build().await.unwrap();

        let existing_contexts = db::list_bounded_contexts(&pool).await.unwrap();
        assert!(
            existing_contexts
                .iter()
                .any(|bc| bc.name == bootstrap::ADMIN_BOUNDED_CONTEXT_NAME),
            "admin must already be among the real, DB-sourced existing_contexts"
        );

        let caller = skilj_core::access_control::Role {
            id: skilj_core::shared::generate_token_id(),
            external_subject: "superadmin-subject".to_string(),
            name: "Superadmin".to_string(),
            superadmin: true,
            status: skilj_core::access_control::RoleStatus::Active,
            created_at: chrono::Utc::now(),
            revoked_at: None,
        };
        let result = bootstrap::add_bounded_context(
            &caller,
            bootstrap::ADMIN_BOUNDED_CONTEXT_NAME.to_string(),
            &existing_contexts,
            chrono::Utc::now(),
        );
        assert!(matches!(
            result,
            Err(skilj_core::Error::Bootstrap(
                skilj_core::bootstrap::Error::BoundedContextNameTaken
            ))
        ));
    });
}
