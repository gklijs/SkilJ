//! Codeberg issue #100 - the supported local mode.
//!
//! These start a real embedded Postgres (installing it on first run), the
//! same dependency the DB-backed suites already fall back to when
//! `DATABASE_URL` is unset.

#![cfg(feature = "local")]

use skilj::local::LocalConfig;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for local_mode")
    })
}

fn temp_dir(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "skilj-local-{label}-{}",
        skilj_core::shared::generate_token_id()
    ))
}

#[test]
fn a_database_name_that_cannot_be_quoted_is_refused() {
    let config = LocalConfig {
        database: "not a database".to_string(),
        ..LocalConfig::default()
    };
    assert!(config.validate().is_err());
}

#[test]
fn local_mode_starts_migrates_and_persists_across_restarts() {
    runtime().block_on(async {
        let data_dir = temp_dir("persist");
        let config = LocalConfig {
            data_dir: data_dir.clone(),
            port: None,
            database: "skilj_local_test".to_string(),
            cleanup: false,
        };

        let first = skilj::local::LocalPostgres::start(config.clone())
            .await
            .expect("the embedded Postgres starts");
        assert!(first.database_url().contains("skilj_local_test"));
        let pool = skilj_core::db::connect(first.database_url()).await.unwrap();
        skilj_core::db::migrate(&pool).await.unwrap();
        pool.close().await;
        first.shutdown().await.expect("it stops");

        // Same data directory: the second start finds the initialized
        // database and its migrations, rather than re-initializing.
        let second = skilj::local::LocalPostgres::start(config)
            .await
            .expect("the embedded Postgres restarts from persisted data");
        let pool = skilj_core::db::connect(second.database_url())
            .await
            .unwrap();
        let applied: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(applied.0 > 0);
        pool.close().await;
        second.shutdown().await.expect("it stops");
        let _ = std::fs::remove_dir_all(data_dir);
    });
}

#[test]
fn local_mode_can_delete_its_data_on_shutdown() {
    runtime().block_on(async {
        let data_dir = temp_dir("cleanup");
        let postgres = skilj::local::LocalPostgres::start(LocalConfig {
            data_dir: data_dir.clone(),
            port: None,
            database: "skilj_cleanup_test".to_string(),
            cleanup: true,
        })
        .await
        .expect("the embedded Postgres starts");
        let password_file = skilj::local::password_file(&data_dir);
        assert!(password_file.exists());
        postgres.shutdown().await.expect("it stops");
        assert!(!data_dir.exists());
        assert!(
            !password_file.exists(),
            "the password must not outlive the data it protects"
        );
    });
}

/// The login is enforced, not just configured: the URL's password gets in,
/// a wrong one and none at all do not. Applies to whichever backend this
/// process gets (embedded, or the system server when running as root).
#[test]
fn local_mode_refuses_a_login_without_the_password() {
    runtime().block_on(async {
        let data_dir = temp_dir("auth");
        let postgres = skilj::local::LocalPostgres::start(LocalConfig {
            data_dir: data_dir.clone(),
            port: None,
            database: "skilj_auth_test".to_string(),
            cleanup: true,
        })
        .await
        .expect("local Postgres starts");
        let url = postgres.database_url().to_string();
        assert!(!postgres.display_url().contains(
            std::fs::read_to_string(skilj::local::password_file(&data_dir))
                .unwrap()
                .trim()
        ));

        let pool = skilj_core::db::connect(&url)
            .await
            .expect("the URL's password gets in");
        pool.close().await;

        let (before_at, after_at) = url.split_once('@').unwrap();
        let user_part = before_at.rsplit_once(':').unwrap().0;
        let wrong = format!("{user_part}:not-the-password@{after_at}");
        assert!(
            skilj_core::db::connect(&wrong).await.is_err(),
            "a wrong password must be refused"
        );
        let none = format!("{user_part}@{after_at}");
        assert!(
            skilj_core::db::connect(&none).await.is_err(),
            "no password must be refused"
        );
        postgres.shutdown().await.expect("it stops");
    });
}
