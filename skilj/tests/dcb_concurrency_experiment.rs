//! Issue #158: isolated PostgreSQL protocol experiment, not production APIs.
//! DATABASE_URL must name a disposable Docker PostgreSQL 18+ server.
//! cargo test --release -p skilj --test dcb_concurrency_experiment dcb_concurrency_experiment -- --ignored --nocapture
use futures_util::future::join_all;
use sqlx::{PgPool, Postgres, Transaction};
use std::time::{Duration, Instant};

const SQL: &str = include_str!("../../scripts/dcb158/protocol.sql");

async fn read(pool: &PgPool, keys: &[String]) -> (Vec<i64>, i64) {
    sqlx::query_as("SELECT * FROM dcb158.read_state($1)")
        .bind(keys)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn append(
    pool: &PgPool,
    mode: &str,
    keys: &[String],
    expected: &[i64],
    request: &str,
    cap: i64,
) -> String {
    sqlx::query_scalar("SELECT dcb158.append($1, $2, $3, $4, 1, $5)")
        .bind(mode)
        .bind(keys)
        .bind(expected)
        .bind(request)
        .bind(cap)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn reset(pool: &PgPool) {
    sqlx::raw_sql("TRUNCATE dcb158.publication, dcb158.batches, dcb158.outbox, dcb158.tags, dcb158.commands, dcb158.guards, dcb158.events; UPDATE dcb158.counters SET value = 0; ALTER SEQUENCE dcb158.gapped RESTART")
        .execute(pool).await.unwrap();
}

async fn scalar(pool: &PgPool, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(pool).await.unwrap()
}

async fn publish(pool: &PgPool) -> i32 {
    sqlx::query_scalar("SELECT dcb158.publish(256)")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn check_publication(pool: &PgPool, total: i64) {
    assert_eq!(
        scalar(pool, "SELECT count(*) FROM dcb158.publication").await,
        total
    );
    assert_eq!(
        scalar(
            pool,
            "SELECT count(*) FROM dcb158.outbox WHERE NOT published"
        )
        .await,
        0
    );
    assert_eq!(
        scalar(
            pool,
            "SELECT coalesce(sum(size), 0)::bigint FROM dcb158.batches"
        )
        .await,
        total
    );
    assert_eq!(scalar(pool, "SELECT count(*) FROM dcb158.tags t JOIN dcb158.publication p ON p.event_id = t.event_id JOIN dcb158.publication prev ON prev.event_id = t.predecessor WHERE (prev.batch, prev.offset_in_batch) >= (p.batch, p.offset_in_batch)").await, 0);
}

async fn correctness(pool: &PgPool, other: &PgPool) {
    for mode in ["serial", "uuid-global", "uuid", "gapped"] {
        reset(pool).await;
        // Two pools (independent instances), same initial empty read. Exactly
        // one create-once can succeed; the other must retry, then reject.
        let keys = vec!["same".into()];
        let (a, b) = tokio::join!(
            append(pool, mode, &keys, &[0], "a", 1),
            append(other, mode, &keys, &[0], "b", 1)
        );
        assert!(a == "accepted" && b == "retry" || b == "accepted" && a == "retry");
        let (revision, balance) = read(pool, &keys).await;
        assert_eq!(balance, 1);
        assert_eq!(
            append(other, mode, &keys, &revision, "c", 1).await,
            "rejected"
        );
        let winner = if a == "accepted" { "a" } else { "b" };
        assert_eq!(
            append(other, mode, &keys, &[0], winner, 1).await,
            "duplicate"
        );
        assert_eq!(scalar(pool, "SELECT count(*) FROM dcb158.events").await, 1);

        reset(pool).await;
        let left = vec!["a".into(), "b".into()];
        let right = vec!["b".into(), "c".into()];
        let (a, b) = tokio::join!(
            append(pool, mode, &left, &[0, 0], "left", i64::MAX),
            append(other, mode, &right, &[0, 0], "right", i64::MAX)
        );
        assert!(a == "accepted" && b == "retry" || b == "accepted" && a == "retry");
        // Changed output/read set is reprepared, never quietly appended.
        let retry_keys = if a == "retry" { &left } else { &right };
        let (revision, _) = read(pool, retry_keys).await;
        assert_eq!(
            append(other, mode, retry_keys, &revision, "retry", i64::MAX).await,
            "accepted"
        );
        while publish(pool).await > 0 {}
        check_publication(pool, 2).await;

        // Roll back an entire accepted append, including command, tag guard,
        // event and outbox. It must be safe to retry its request key.
        let mut tx = pool.begin().await.unwrap();
        accepted_in_tx(&mut tx, mode, "rollback").await;
        tx.rollback().await.unwrap();
        assert_eq!(read(pool, &["rollback".into()]).await, (vec![0], 0));
        assert_eq!(
            append(pool, mode, &["rollback".into()], &[0], "rollback", i64::MAX).await,
            "accepted"
        );
    }

    // Two multi-command batches visit the same keys in opposite orders.
    // Their union lock acquisition must avoid batch-level deadlocks.
    for mode in ["serial", "uuid"] {
        reset(pool).await;
        let a = serde_json::json!([
            {"keys":["a"], "expected":[0], "request":"a1"},
            {"keys":["b"], "expected":[0], "request":"a2"}
        ]);
        let b = serde_json::json!([
            {"keys":["b"], "expected":[0], "request":"b1"},
            {"keys":["a"], "expected":[0], "request":"b2"}
        ]);
        let call = |pool: PgPool, items| async move {
            let results: Vec<String> = sqlx::query_scalar("SELECT dcb158.append_batch($1,$2)")
                .bind(items)
                .bind(mode)
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(results, vec!["accepted", "accepted"]);
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(call(pool.clone(), a), call(other.clone(), b));
        })
        .await
        .expect("opposite batch orders must not deadlock");
        assert_eq!(read(pool, &["a".into()]).await, (vec![2], 2));
        while publish(pool).await > 0 {}
        check_publication(pool, 4).await;
    }

    // Counterexample to a UUID cursor, using actual delayed transactions.
    reset(pool).await;
    let _: String = sqlx::query_scalar("SELECT slot_name::text FROM pg_create_logical_replication_slot('dcb158_order_probe', 'test_decoding')")
        .fetch_one(other).await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    accepted_in_tx(&mut tx, "uuid", "old").await;
    let older: String = sqlx::query_scalar("SELECT id::text FROM dcb158.events")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2)).await;
    assert_eq!(
        append(other, "uuid", &["new".into()], &[0], "new", i64::MAX).await,
        "accepted"
    );
    let cursor: String =
        sqlx::query_scalar("SELECT id::text FROM dcb158.events ORDER BY id DESC LIMIT 1")
            .fetch_one(other)
            .await
            .unwrap();
    assert!(older < cursor);
    tx.commit().await.unwrap();
    assert_eq!(scalar(pool, "SELECT count(*) FROM dcb158.events").await, 2);
    let missed: i64 = sqlx::query_scalar("SELECT count(*) FROM dcb158.events WHERE id > $1::uuid")
        .bind(&cursor)
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(
        missed, 0,
        "UUID cursor has permanently skipped the late commit"
    );
    let changes: Vec<(String, String)> = sqlx::query_as("SELECT lsn::text, data FROM pg_logical_slot_get_changes('dcb158_order_probe', NULL, NULL, 'skip-empty-xacts', '1', 'include-xids', '0')")
        .fetch_all(other).await.unwrap();
    let decoded_events: Vec<_> = changes
        .iter()
        .filter(|(_, data)| data.starts_with("table dcb158.events: INSERT:"))
        .collect();
    assert_eq!(decoded_events.len(), 2);
    assert!(
        decoded_events[0].1.contains(&cursor),
        "first commit must decode first"
    );
    assert!(
        decoded_events[1].1.contains(&older),
        "late commit must decode second despite its lower UUID"
    );
    let commits: Vec<_> = changes
        .iter()
        .filter(|(_, data)| data.starts_with("COMMIT"))
        .collect();
    assert_eq!(commits.len(), 2);
    println!(
        "CDC commit order verified: {:?}",
        commits.iter().map(|(lsn, _)| lsn).collect::<Vec<_>>()
    );
    sqlx::query("SELECT pg_drop_replication_slot('dcb158_order_probe')")
        .execute(other)
        .await
        .unwrap();
    let ids = vec![cursor, older];
    let published: Vec<String> =
        sqlx::query_scalar("SELECT dcb158.publish_decoded($1::text[]::uuid[], $2::pg_lsn)")
            .bind(&ids)
            .bind(&commits[1].0)
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(published, vec!["new", "old"]);
    let replayed: Vec<String> =
        sqlx::query_scalar("SELECT dcb158.publish_decoded($1::text[]::uuid[], $2::pg_lsn)")
            .bind(&ids)
            .bind(&commits[1].0)
            .fetch_one(pool)
            .await
            .unwrap();
    assert!(
        replayed.is_empty(),
        "replayed WAL must retain the original batch mapping"
    );
    while publish(pool).await > 0 {}
    check_publication(pool, 2).await;
    println!("CORRECTNESS passed: two pools, same-key cap, overlapping tags, idempotency, rollback, publication order; UUID cursor counterexample reproduced");
}

async fn accepted_in_tx(tx: &mut Transaction<'_, Postgres>, mode: &str, key: &str) {
    let outcome: String = sqlx::query_scalar(
        "SELECT dcb158.append($1, ARRAY[$2::text], ARRAY[0::bigint], $2, 1, 100)",
    )
    .bind(mode)
    .bind(key)
    .fetch_one(&mut **tx)
    .await
    .unwrap();
    assert_eq!(outcome, "accepted");
}

#[derive(Clone, Copy, Debug)]
enum Workload {
    Independent,
    Hot,
    Cross,
}

struct Pending {
    item: serde_json::Value,
    reply: tokio::sync::oneshot::Sender<String>,
}

#[derive(Clone, Copy)]
struct BatchSettings {
    linger: Duration,
    max_size: usize,
}

#[derive(Default)]
struct IngestBatchStats {
    batches: usize,
    commands: usize,
    max: usize,
}

fn key_lane(keys: &[String], lanes: usize) -> usize {
    // Stable FNV-1a routing; a single hot key always reaches one batcher.
    // Multi-tag operations still acquire the union of database locks, so
    // routing is a throughput choice rather than a correctness assumption.
    let hash = keys[0].bytes().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
    });
    (hash % lanes as u64) as usize
}

fn group_committer(
    pool: PgPool,
    mode: &'static str,
    settings: BatchSettings,
) -> (
    tokio::sync::mpsc::Sender<Pending>,
    tokio::task::JoinHandle<IngestBatchStats>,
) {
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<Pending>(256);
    let task = tokio::spawn(async move {
        let mut stats = IngestBatchStats::default();
        while let Some(first) = receiver.recv().await {
            let mut pending = vec![first];
            let deadline = tokio::time::Instant::now() + settings.linger;
            while pending.len() < settings.max_size {
                match receiver.try_recv() {
                    Ok(item) => pending.push(item),
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                        if tokio::time::Instant::now() < deadline =>
                    {
                        match tokio::time::timeout_at(deadline, receiver.recv()).await {
                            Ok(Some(item)) => pending.push(item),
                            _ => break,
                        }
                    }
                    Err(_) => break,
                }
            }
            let items: Vec<_> = pending.iter().map(|p| p.item.clone()).collect();
            stats.batches += 1;
            stats.commands += pending.len();
            stats.max = stats.max.max(pending.len());
            let results: Vec<String> = sqlx::query_scalar("SELECT dcb158.append_batch($1, $2)")
                .bind(serde_json::Value::Array(items))
                .bind(mode)
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(pending.len(), results.len());
            for (item, result) in pending.into_iter().zip(results) {
                let _ = item.reply.send(result);
            }
        }
        stats
    });
    (sender, task)
}

async fn decode_and_publish(pool: &PgPool) -> Vec<String> {
    let changes: Vec<(String, String)> = sqlx::query_as("SELECT lsn::text, data FROM pg_logical_slot_peek_changes('dcb158_benchmark', NULL, 10000, 'skip-empty-xacts', '1', 'include-xids', '0')")
        .fetch_all(pool).await.unwrap();
    let Some((high, _)) = changes.last() else {
        return Vec::new();
    };
    let mut ids = Vec::new();
    let mut pending = Vec::new();
    for (_, data) in &changes {
        if let Some(tail) = data.strip_prefix("table dcb158.events: INSERT: id[uuid]:'") {
            let id = tail.split('\'').next().unwrap();
            assert_eq!(id.len(), 36);
            pending.push(id.to_string());
        } else if data.starts_with("COMMIT") {
            ids.append(&mut pending);
        }
    }
    assert!(
        pending.is_empty(),
        "decoding must finish complete transactions"
    );
    let requests = if ids.is_empty() {
        Vec::new()
    } else {
        sqlx::query_scalar("SELECT dcb158.publish_decoded($1::text[]::uuid[], $2::pg_lsn)")
            .bind(ids)
            .bind(high)
            .fetch_one(pool)
            .await
            .unwrap()
    };
    // Persist mapping before acknowledging WAL. A crash between steps
    // replays the same UUIDs, which retain their original batch positions.
    let _: i64 = sqlx::query_scalar("SELECT count(*)::bigint FROM pg_logical_slot_get_changes('dcb158_benchmark', $1::pg_lsn, NULL, 'skip-empty-xacts', '1', 'include-xids', '0')")
        .bind(high).fetch_one(pool).await.unwrap();
    requests
}

async fn scenario(pool: &PgPool, mode: &str, workload: Workload, workers: usize, commands: usize) {
    reset(pool).await;
    let wal_order = std::env::var("SKILJ_EXPERIMENT_ORDER").as_deref() == Ok("wal");
    if wal_order {
        let _: String = sqlx::query_scalar("SELECT slot_name::text FROM pg_create_logical_replication_slot('dcb158_benchmark', 'test_decoding')")
            .fetch_one(pool).await.unwrap();
    }
    let lanes = if mode == "uuid-group" {
        std::env::var("SKILJ_EXPERIMENT_LANES")
            .ok()
            .map(|s| s.parse().unwrap())
            .unwrap_or(8)
    } else {
        1
    };
    assert!(lanes > 0);
    let linger_us = std::env::var("SKILJ_EXPERIMENT_LINGER_US")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(0);
    let max_size = std::env::var("SKILJ_EXPERIMENT_BATCH_MAX")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(256);
    assert!((1..=256).contains(&max_size));
    let settings = BatchSettings {
        linger: Duration::from_micros(linger_us),
        max_size,
    };
    let pause_us = std::env::var("SKILJ_EXPERIMENT_PAUSE_US")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(0);
    let cached_decision = std::env::var("SKILJ_EXPERIMENT_CACHED_DECISION").as_deref() == Ok("1");
    let (group_senders, group_tasks): (Vec<_>, Vec<_>) = (0..lanes)
        .map(|_| {
            group_committer(
                pool.clone(),
                if mode == "uuid-group" {
                    "uuid"
                } else {
                    "serial"
                },
                settings,
            )
        })
        .unzip();
    let started = Instant::now();
    let publisher_pool = pool.clone();
    let (done, mut done_rx) = tokio::sync::watch::channel(false);
    let publisher = tokio::spawn(async move {
        let mut batches = 0;
        let mut observed = std::collections::HashMap::new();
        loop {
            // Sample completion before querying: an empty query started
            // before the final ingest commit is not proof of a drained log.
            let finished = *done_rx.borrow_and_update();
            let requests: Vec<String> = if wal_order {
                decode_and_publish(&publisher_pool).await
            } else {
                sqlx::query_scalar("SELECT dcb158.publish_requests(256)")
                    .fetch_one(&publisher_pool)
                    .await
                    .unwrap()
            };
            let count = requests.len();
            let now = Instant::now();
            for request in requests {
                assert!(observed.insert(request, now).is_none());
            }
            batches += usize::from(count > 0);
            if finished
                && count == 0
                && scalar(
                    &publisher_pool,
                    "SELECT count(*) FROM dcb158.outbox WHERE NOT published",
                )
                .await
                    == 0
            {
                break;
            }
            if count == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        (batches, observed)
    });
    let results = join_all((0..workers).map(|worker| {
        let group_senders = group_senders.clone();
        async move {
        let mut latencies = Vec::new();
        let mut retries = 0;
        for n in (worker..commands).step_by(workers) {
            if pause_us > 0 { tokio::time::sleep(Duration::from_micros(pause_us)).await; }
            let begin = Instant::now();
            let keys = match workload {
                Workload::Independent => vec![format!("key-{worker}")],
                Workload::Hot => vec!["hot".into()],
                Workload::Cross => {
                    let mut keys = vec![
                        format!("key-{worker}"),
                        format!("key-{}", (worker + 1) % workers),
                    ];
                    keys.sort();
                    keys.dedup();
                    keys
                }
            };
            loop {
                let (revision, balance) = read(pool, &keys).await;
                let outcome = if matches!(mode, "serial-group" | "uuid-group") {
                    let (reply, receiver) = tokio::sync::oneshot::channel();
                    group_senders[key_lane(&keys, lanes)].send(Pending {
                        item: serde_json::json!({"keys": keys, "expected": revision, "request": format!("request-{n}"), "balance": cached_decision.then_some(balance)}), reply,
                    }).await.unwrap();
                    receiver.await.unwrap()
                } else if cached_decision {
                    sqlx::query_scalar("SELECT dcb158.append($1,$2,$3,$4,1,$5,$6)")
                        .bind(mode).bind(&keys).bind(&revision).bind(format!("request-{n}"))
                        .bind(i64::MAX / 2).bind(balance).fetch_one(pool).await.unwrap()
                } else { append(
                    pool,
                    mode,
                    &keys,
                    &revision,
                    &format!("request-{n}"),
                    i64::MAX / 2,
                )
                .await };
                match outcome.as_str() {
                    "accepted" => break,
                    "retry" => retries += 1,
                    other => panic!("unexpected {other}"),
                }
            }
            latencies.push((begin.elapsed(), format!("request-{n}"), Instant::now()));
        }
        (latencies, retries)
    }}))
    .await;
    drop(group_senders);
    let mut ingest_batches = IngestBatchStats::default();
    for task in group_tasks {
        let stats = task.await.unwrap();
        ingest_batches.batches += stats.batches;
        ingest_batches.commands += stats.commands;
        ingest_batches.max = ingest_batches.max.max(stats.max);
    }
    let ingest_elapsed = started.elapsed();
    done.send(true).unwrap();
    let (publication_batches, observed) = publisher.await.unwrap();
    let published_elapsed = started.elapsed();
    if wal_order {
        sqlx::query("SELECT pg_drop_replication_slot('dcb158_benchmark')")
            .execute(pool)
            .await
            .unwrap();
    }
    let mut publication_lags: Vec<_> = results
        .iter()
        .flat_map(|r| r.0.iter())
        .map(|(_, request, ack)| observed[request].saturating_duration_since(*ack))
        .collect();
    publication_lags.sort();
    let mut latencies: Vec<_> = results
        .iter()
        .flat_map(|r| r.0.iter().map(|r| r.0))
        .collect();
    latencies.sort();
    let retries: usize = results.iter().map(|r| r.1).sum();
    assert_eq!(
        scalar(pool, "SELECT count(*) FROM dcb158.events").await,
        commands as i64
    );
    assert_eq!(
        scalar(pool, "SELECT count(*) FROM dcb158.commands").await,
        commands as i64
    );
    assert_eq!(
        scalar(pool, "SELECT sum(amount)::bigint FROM dcb158.events").await,
        commands as i64
    );
    check_publication(pool, commands as i64).await;
    println!("RESULT mode={mode} wal_order={wal_order} cached_decision={cached_decision} routing=key linger_us={linger_us} batch_max={max_size} pause_us={pause_us} workload={workload:?} workers={workers} commands={commands} ingest_per_s={:.1} published_per_s={:.1} p50_ms={:.3} p99_ms={:.3} drain_ms={:.3} retries={retries} publication_batches={publication_batches} stream_lag_p50_ms={:.3} stream_lag_p99_ms={:.3} ingest_batch_mean={:.2} ingest_batch_max={}",
        commands as f64 / ingest_elapsed.as_secs_f64(), commands as f64 / published_elapsed.as_secs_f64(),
        latencies[latencies.len()/2].as_secs_f64()*1000.0,
        latencies[(latencies.len()-1)*99/100].as_secs_f64()*1000.0,
        (published_elapsed-ingest_elapsed).as_secs_f64()*1000.0,
        publication_lags[publication_lags.len()/2].as_secs_f64()*1000.0,
        publication_lags[(publication_lags.len()-1)*99/100].as_secs_f64()*1000.0,
        ingest_batches.commands as f64 / ingest_batches.batches.max(1) as f64,
        ingest_batches.max);
}

#[test]
#[ignore = "explicit Docker PostgreSQL protocol experiment"]
fn dcb_concurrency_experiment() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        assert!(std::env::var("DATABASE_URL").is_ok(), "set DATABASE_URL to Docker PostgreSQL");
        let url = skilj_test_support::database_url("skilj_dcb158_experiment").await.expect("database setup must not skip");
        let pool = sqlx::postgres::PgPoolOptions::new().max_connections(90).test_before_acquire(false).connect(&url).await.unwrap();
        let other = PgPool::connect(&url).await.unwrap();
        let settings: Vec<(String, String)> = sqlx::query_as("SELECT name, setting FROM pg_settings WHERE name IN ('fsync','synchronous_commit','full_page_writes','server_version') ORDER BY name").fetch_all(&pool).await.unwrap();
        println!("SETTINGS {settings:?}");
        for name in ["fsync", "synchronous_commit", "full_page_writes"] {
            assert!(settings.iter().any(|(n,v)| n == name && v == "on"));
        }
        sqlx::raw_sql(SQL).execute(&pool).await.unwrap();
        correctness(&pool, &other).await;
        let commands = std::env::var("SKILJ_EXPERIMENT_COMMANDS").ok().map(|n| n.parse().unwrap()).unwrap_or(1600);
        let rounds = std::env::var("SKILJ_EXPERIMENT_ROUNDS").ok().map(|n| n.parse().unwrap()).unwrap_or(3);
        let modes = ["serial", "serial-group", "uuid-global", "uuid", "gapped", "uuid-group"];
        for round in 0..rounds {
            println!("ROUND {}", round + 1);
            for workload in [Workload::Independent, Workload::Hot, Workload::Cross] {
                if let Ok(only) = std::env::var("SKILJ_EXPERIMENT_WORKLOAD") {
                    if only != format!("{workload:?}") { continue; }
                }
                for workers in [1, 8, 32, 80] {
                    if let Ok(only) = std::env::var("SKILJ_EXPERIMENT_WORKERS") {
                        if !only.split(',').any(|n| n.parse::<usize>().unwrap() == workers) { continue; }
                    }
                    for i in 0..modes.len() {
                        if let Ok(only) = std::env::var("SKILJ_EXPERIMENT_MODES") {
                            if !only.split(',').any(|mode| mode == modes[(i + round) % modes.len()]) { continue; }
                        }
                        // Rotate order to avoid always giving one mode a
                        // warmer cache or earlier checkpoint position.
                        scenario(&pool, modes[(i + round) % modes.len()], workload, workers, commands).await;
                    }
                }
            }
        }
        other.close().await;
        pool.close().await;
    });
}

#[test]
#[ignore = "explicit Docker PostgreSQL tag-layout component benchmark"]
fn tag_interning_experiment() {
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        assert!(std::env::var("DATABASE_URL").is_ok(), "set DATABASE_URL to Docker PostgreSQL");
        let url = skilj_test_support::database_url("skilj_dcb158_tags").await.expect("database setup must not skip");
        let pool = PgPool::connect(&url).await.unwrap();
        for name in ["fsync", "synchronous_commit", "full_page_writes"] {
            let value: String = sqlx::query_scalar("SELECT setting FROM pg_settings WHERE name = $1")
                .bind(name).fetch_one(&pool).await.unwrap();
            assert_eq!(value, "on");
        }
        sqlx::raw_sql(include_str!("../../scripts/dcb158/tags.sql")).execute(&pool).await.unwrap();
        let rows: i32 = std::env::var("SKILJ_TAG_ROWS").ok().map(|s| s.parse().unwrap()).unwrap_or(200_000);
        let probes: i32 = 10_000;
        for keys in [1_000, rows] {
            for round in 1..=3 {
                sqlx::raw_sql("TRUNCATE raw_tags, interned_tags, tag_dictionary RESTART IDENTITY").execute(&pool).await.unwrap();
                let mut raw_ms = 0.0;
                let mut intern_ms = 0.0;
                for intern in if round % 2 == 0 { [true, false] } else { [false, true] } {
                    let started = Instant::now();
                    if intern {
                        sqlx::query("SELECT seed_dictionary($1)").bind(keys).execute(&pool).await.unwrap();
                        sqlx::query("SELECT seed_interned($1,$2)").bind(rows).bind(keys).execute(&pool).await.unwrap();
                        intern_ms = started.elapsed().as_secs_f64() * 1000.0;
                    } else {
                        sqlx::query("SELECT seed_raw($1,$2)").bind(rows).bind(keys).execute(&pool).await.unwrap();
                        raw_ms = started.elapsed().as_secs_f64() * 1000.0;
                    }
                }
                for sql in ["VACUUM ANALYZE raw_tags", "VACUUM ANALYZE interned_tags", "VACUUM ANALYZE tag_dictionary"] {
                    sqlx::query(sql).execute(&pool).await.unwrap();
                }
                let raw_bytes = scalar(&pool, "SELECT pg_total_relation_size('raw_tags')").await;
                let tag_bytes = scalar(&pool, "SELECT pg_total_relation_size('interned_tags')").await;
                let dictionary_bytes = scalar(&pool, "SELECT pg_total_relation_size('tag_dictionary')").await;
                let (values, ids): (Vec<String>, Vec<i64>) = sqlx::query_as("SELECT array_agg(tag_value ORDER BY id), array_agg(id ORDER BY id) FROM tag_dictionary WHERE id <= $1")
                    .bind(keys.min(probes)).fetch_one(&pool).await.unwrap();
                let mut checksums = Vec::new();
                for kind in ["raw", "interned", "cached"] {
                    let sql = match kind {
                        "raw" => "SELECT probe_raw($1,$2,$3)",
                        "interned" => "SELECT probe_interned($1,$2,$3)",
                        _ => "SELECT probe_cached_id($1,$2,$3)",
                    };
                    // Warm the function's plans and all fixture tag values.
                    let _: i64 = sqlx::query_scalar(sql).bind(probes).bind(&values).bind(&ids).fetch_one(&pool).await.unwrap();
                    let started = Instant::now();
                    let checksum: i64 = sqlx::query_scalar(sql).bind(probes).bind(&values).bind(&ids).fetch_one(&pool).await.unwrap();
                    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                    checksums.push(checksum);
                    println!("TAGS round={round} keys={keys} rows={rows} kind={kind} probes={probes} probe_ms={elapsed_ms:.3} raw_seed_ms={raw_ms:.3} intern_seed_ms={intern_ms:.3} raw_bytes={raw_bytes} intern_tag_bytes={tag_bytes} dictionary_bytes={dictionary_bytes} intern_total_bytes={}", tag_bytes + dictionary_bytes);
                }
                assert!(checksums[0] > 0);
                assert!(checksums.iter().all(|sum| *sum == checksums[0]), "layout variants must return exactly the same positions");
            }
        }
        sqlx::raw_sql("INSERT INTO tag_dictionary(tag_key,tag_value) VALUES ('account','literal_%used'),('account','literal_%unused'),('account','literalXYZnotmatch'); INSERT INTO interned_tags SELECT id,'Deposited',200001 + id FROM tag_dictionary WHERE tag_value IN ('literal_%used','literalXYZnotmatch')")
            .execute(&pool).await.unwrap();
        let used: Vec<(i64, String)> = sqlx::query_as("SELECT * FROM suggest_tags('account','literal_%',20)").fetch_all(&pool).await.unwrap();
        assert_eq!(used.len(), 1);
        assert_eq!(used[0].1, "literal_%used");
        let registered: Vec<(i64, String)> = sqlx::query_as("SELECT * FROM suggest_tags('account','literal_%',20,false)").fetch_all(&pool).await.unwrap();
        assert_eq!(registered.len(), 2);
        let edge_cases: bool = sqlx::query_scalar("SELECT prefix_upper('') IS NULL AND prefix_upper(chr(1114111)) IS NULL AND prefix_upper('a' || chr(1114111)) = 'b' AND prefix_upper(chr(55295)) = chr(57344)")
            .fetch_one(&pool).await.unwrap();
        assert!(edge_cases, "prefix bounds must handle empty, maximum Unicode and surrogate boundaries");
        let unknown: Vec<(i64, String)> = sqlx::query_as("SELECT * FROM suggest_tags('missing','',20)").fetch_all(&pool).await.unwrap();
        assert!(unknown.is_empty());
        let mut search_tx = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL plan_cache_mode = force_generic_plan").execute(&mut *search_tx).await.unwrap();
        let mut search_times = Vec::new();
        for n in 0..200 {
            let prefix = format!("{n:02x}");
            let started = Instant::now();
            let found: Vec<(i64, String)> = sqlx::query_as("SELECT * FROM suggest_tags('account',$1,20)")
                .bind(&prefix).fetch_all(&mut *search_tx).await.unwrap();
            search_times.push(started.elapsed());
            assert!(found.len() <= 20);
            assert!(found.iter().all(|(_, value)| value.starts_with(&prefix)));
        }
        search_times.sort();
        println!("SEARCH dictionary_entries={} probes=200 max_results=20 p50_ms={:.3} p99_ms={:.3} generic_plan=true literal_wildcards=checked unused_registrations=excluded",
            rows + 3, search_times[100].as_secs_f64()*1000.0, search_times[197].as_secs_f64()*1000.0);
        let plan: Vec<String> = sqlx::query_scalar("EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM suggest_tags('account',$1,20)").bind("ff").fetch_all(&mut *search_tx).await.unwrap();
        assert!(!plan.iter().any(|line| line.contains("hashed SubPlan")), "suggestions must not build a hash of every tag reference");
        println!("SEARCH PLAN\n{}", plan.join("\n"));
        search_tx.commit().await.unwrap();
        pool.close().await;
    });
}
