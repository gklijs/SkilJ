//! What one command sends Postgres - not a test, so `#[ignore]`d. Run with
//! the server logging statements and `DATABASE_URL` pointing at it:
//!
//! ```sh
//! cargo test --release -p skilj --test command_round_trips -- --ignored --nocapture
//! ```
//!
//! After a warm-up, `COMMANDS` deposits go through `POST
//! /v1/commands/trigger` from one caller, one after another, between two
//! marker statements (`SELECT 'skilj-round-trips-start'` and `...-end`)
//! sent on a connection of the test's own. Every statement the server
//! logs between them, divided by `COMMANDS`, is what one command costs in
//! round trips - the cost a single caller pays at every millisecond of
//! network latency (docs/architecture.md §196). Background tasks run
//! meanwhile and show up too, as statements that don't scale with
//! `COMMANDS`.
//!
//! With `SKILJ_BENCH_LATENCY_MS` set, it first checks what a round trip
//! through `command_throughput.rs`' latency proxy really costs, by timing
//! `SELECT 1` through it.
mod deposit_bench;
use deposit_bench::{deposit, latency_proxy, setup};
use skilj_core::db;
use skilj_core::shared::generate_token_id;
use std::time::Duration;

const COMMANDS: usize = 50;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

#[test]
#[ignore = "benchmark - run explicitly, see the module docs"]
fn command_round_trips() {
    runtime().block_on(async {
        let Some(database_url) =
            skilj_test_support::database_url("skilj_command_round_trips_bench").await
        else {
            eprintln!("skipping: no test database");
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();
        db::migrate(&pool).await.unwrap();
        if let Some(latency_ms) = std::env::var("SKILJ_BENCH_LATENCY_MS")
            .ok()
            .and_then(|ms| ms.parse::<u64>().ok())
        {
            let proxied = latency_proxy(&database_url, Duration::from_millis(latency_ms)).await;
            for (name, options) in [
                (
                    "pool, default options",
                    db::PgPoolOptions::new().max_connections(1),
                ),
                (
                    "pool, test_before_acquire(false)",
                    db::PgPoolOptions::new()
                        .max_connections(1)
                        .test_before_acquire(false),
                ),
            ] {
                let one = db::connect_with(&proxied, options).await.unwrap();
                sqlx::query("SELECT 1").execute(&one).await.unwrap();
                let started = std::time::Instant::now();
                for _ in 0..50 {
                    sqlx::query("SELECT 1").execute(&one).await.unwrap();
                }
                println!(
                    "SELECT 1 through the proxy at +{latency_ms} ms, {name}: {:.2} ms",
                    started.elapsed().as_secs_f64() * 1000.0 / 50.0
                );
                let mut conn = one.acquire().await.unwrap();
                let started = std::time::Instant::now();
                for _ in 0..50 {
                    sqlx::query("SELECT 1").execute(&mut *conn).await.unwrap();
                }
                println!(
                    "SELECT 1 through the proxy at +{latency_ms} ms, one held connection: {:.2} ms",
                    started.elapsed().as_secs_f64() * 1000.0 / 50.0
                );
            }
        }
        let skilj_url = match std::env::var("SKILJ_BENCH_LATENCY_MS")
            .ok()
            .and_then(|ms| ms.parse::<u64>().ok())
        {
            Some(ms) => latency_proxy(&database_url, Duration::from_millis(ms)).await,
            None => database_url,
        };
        let setup = setup(skilj_url, &pool).await;
        let run = generate_token_id();
        for n in 0..20 {
            deposit(&setup, &format!("{run}-warm-{n}")).await;
        }
        sqlx::query("SELECT 'skilj-round-trips-start'")
            .execute(&pool)
            .await
            .unwrap();
        let mut total = Duration::ZERO;
        for n in 0..COMMANDS {
            total += deposit(&setup, &format!("{run}-{n}")).await;
        }
        println!(
            "one caller: {:.2} ms per command",
            total.as_secs_f64() * 1000.0 / COMMANDS as f64
        );
        sqlx::query("SELECT 'skilj-round-trips-end'")
            .execute(&pool)
            .await
            .unwrap();
        println!("{COMMANDS} commands between the markers");
        setup.skilj.shutdown(Duration::from_secs(10)).await;
    });
}
