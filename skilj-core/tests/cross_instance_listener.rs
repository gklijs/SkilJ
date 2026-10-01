//! `cross_instance::Listener` across a dropped connection
//! (docs/architecture.md §83): a notification sent while the listening
//! connection is down is lost, so the listener must say so
//! (`Message::Resync`) rather than reconnect silently, and must be
//! listening again before it does. Its own test binary (and database):
//! it terminates every `LISTEN` backend in the database, which would
//! disturb any other listener sharing it.

use skilj_core::cross_instance::{self, Listener, Message};
use skilj_core::db::{self, Pool};
use std::time::Duration;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for cross_instance_listener tests")
    })
}

async fn test_pool() -> Option<Pool> {
    let database_url =
        skilj_test_support::database_url("skilj_cross_instance_listener_test").await?;
    let pool = match db::connect(&database_url).await {
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
    Some(pool)
}

/// Terminates every other backend in this database whose last statement
/// was a `LISTEN` - the listener's connection - and returns how many.
async fn terminate_listeners(pool: &Pool) -> i64 {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
         WHERE datname = current_database() AND pid <> pg_backend_pid() \
         AND query ILIKE 'LISTEN%'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    count
}

async fn notify_registration_changed(pool: &Pool) {
    sqlx::query("SELECT pg_notify($1, '')")
        .bind(cross_instance::REGISTRATION_CHANGED_CHANNEL)
        .execute(pool)
        .await
        .unwrap();
}

async fn recv(listener: &mut Listener) -> Message {
    tokio::time::timeout(Duration::from_secs(10), listener.recv())
        .await
        .expect("no message within 10s")
        .expect("recv failed")
}

#[test]
fn a_dropped_connection_is_reported_as_resync_after_listening_again() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let mut listener = Listener::connect(&pool).await.unwrap();

        // Sanity: a notification on a healthy connection arrives.
        notify_registration_changed(&pool).await;
        assert!(matches!(
            recv(&mut listener).await,
            Message::RegistrationChanged
        ));

        assert_eq!(terminate_listeners(&pool).await, 1);
        // Sent while the listener's connection is gone: lost for good.
        // The old transparent reconnect then waited forever for a
        // message that never came.
        notify_registration_changed(&pool).await;
        assert!(matches!(recv(&mut listener).await, Message::Resync));

        // And it is listening again.
        notify_registration_changed(&pool).await;
        assert!(matches!(
            recv(&mut listener).await,
            Message::RegistrationChanged
        ));
    });
}
