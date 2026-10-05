//! `FailoverServer` itself: what the base backup holds survives the
//! failover, what was committed after it doesn't, and the promoted server
//! is on a new timeline. Run as root this takes the system-binaries path
//! CI uses (docs/architecture.md §176).

use skilj_test_support::FailoverServer;

async fn count_and_timeline(url: &str) -> (i64, String) {
    let pool = sqlx::PgPool::connect(url).await.unwrap();
    let row: (i64, String) = sqlx::query_as(
        "SELECT (SELECT COUNT(*) FROM t), substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    pool.close().await;
    row
}

#[tokio::test]
async fn a_failover_loses_what_the_backup_lacks_and_starts_a_new_timeline() {
    let Some((server, url)) = FailoverServer::start("failover_server_test").await else {
        return;
    };
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE t (x INT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t VALUES (1)")
        .execute(&pool)
        .await
        .unwrap();
    server.base_backup().unwrap();
    sqlx::query("INSERT INTO t VALUES (2)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let (_, timeline_before) = count_and_timeline(&url).await;

    server.fail_over().unwrap();

    let (count, timeline_after) = count_and_timeline(&url).await;
    assert_eq!(count, 1, "the row committed after the backup is gone");
    assert_ne!(timeline_after, timeline_before);
}
