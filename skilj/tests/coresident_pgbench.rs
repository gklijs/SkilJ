//! What an application sharing skilj's Postgres loses, for Codeberg issue
//! #46 - a benchmark, not a test, so `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --release -p skilj --test coresident_pgbench -- --ignored --nocapture
//! ```
//!
//! skilj is meant to live in the application's own Postgres (§2.2), so
//! the number a deployment decision needs is not skilj's throughput but
//! what the neighbouring OLTP workload gives up. Here that workload is
//! `pgbench`'s default TPC-B-like script, with its tables in the same
//! database as skilj's, run for `SKILJ_BENCH_SECONDS` (default 30) per
//! phase:
//!
//! 1. **alone**, twice, before any `Skilj` exists - the baseline, and how
//!    much two identical runs differ;
//! 2. **idle**: a built `Skilj` with its background tasks running, and
//!    no commands;
//! 3. alongside a steady command load through `POST /v1/commands/trigger`
//!    on the in-process router: **spread** (every command on its own
//!    account) paced at 100 and 250 commands/s, then as fast as 8 and 32
//!    callers go, and **hot** (one account) from 8;
//! 4. **alone** again after `Skilj::shutdown`, to show drift over the run.
//!
//! Each row reports pgbench's TPS and per-transaction latency (p50/p99,
//! from its `-l` log), the change against the mean of the two baseline
//! runs, and the commands skilj completed meanwhile. pgbench comes from
//! `PGBENCH` if set, otherwise from the embedded server's own binaries,
//! otherwise from `PATH` (`DATABASE_URL` to run against a server of your
//! own, as the other benchmarks). See docs/performance.md and
//! docs/architecture.md §184 for the numbers.

mod deposit_bench;

use deposit_bench::{deposit, setup, Setup};
use skilj_core::db;
use skilj_core::shared::generate_token_id;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// pgbench's scale factor: 1M rows in `pgbench_accounts`, 10 branches.
const SCALE: u32 = 10;
/// pgbench clients, and the threads driving them.
const CLIENTS: u32 = 8;
const THREADS: u32 = 4;

/// One pgbench run's result.
struct PgbenchRun {
    tps: f64,
    p50_ms: f64,
    p99_ms: f64,
}

async fn pgbench_program() -> PathBuf {
    if let Some(path) = std::env::var_os("PGBENCH") {
        return path.into();
    }
    match skilj_test_support::embedded_binary_dir().await {
        Some(dir) if dir.join("pgbench").exists() => dir.join("pgbench"),
        _ => "pgbench".into(),
    }
}

/// `pgbench -i`: creates and fills the pgbench tables in skilj's database.
async fn pgbench_init(program: &Path, url: &str) {
    let output = tokio::process::Command::new(program)
        .args(["-i", "-q", "-s", &SCALE.to_string(), url])
        .output()
        .await
        .unwrap_or_else(|e| panic!("running {}: {e}", program.display()));
    assert!(
        output.status.success(),
        "pgbench -i failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// One timed pgbench run. Its per-transaction log goes to a directory of
/// its own, which is read for the latency percentiles and removed.
async fn pgbench_run(program: &Path, url: &str, seconds: u64) -> PgbenchRun {
    let log_dir = std::env::temp_dir().join(format!("skilj_pgbench_{}", generate_token_id()));
    std::fs::create_dir_all(&log_dir).unwrap();
    let output = tokio::process::Command::new(program)
        .args([
            "-c",
            &CLIENTS.to_string(),
            "-j",
            &THREADS.to_string(),
            "-T",
            &seconds.to_string(),
            "-l",
            "--log-prefix",
        ])
        .arg(log_dir.join("log"))
        .arg(url)
        .output()
        .await
        .unwrap_or_else(|e| panic!("running {}: {e}", program.display()));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "pgbench failed: {stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let tps = stdout
        .lines()
        .find_map(|line| line.trim().strip_prefix("tps = "))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no tps line in pgbench's output: {stdout}"));

    // `client_id transaction_no time script_no time_epoch time_us`, where
    // `time` is the transaction's latency in microseconds.
    let mut latencies: Vec<u64> = Vec::new();
    for entry in std::fs::read_dir(&log_dir).unwrap() {
        let contents = std::fs::read_to_string(entry.unwrap().path()).unwrap();
        latencies.extend(
            contents
                .lines()
                .filter_map(|line| line.split_whitespace().nth(2)?.parse::<u64>().ok()),
        );
    }
    std::fs::remove_dir_all(&log_dir).unwrap();
    assert!(!latencies.is_empty(), "pgbench logged no transactions");
    latencies.sort_unstable();
    let pct = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize] as f64 / 1000.0;
    PgbenchRun {
        tps,
        p50_ms: pct(0.50),
        p99_ms: pct(0.99),
    }
}

#[derive(Clone, Copy)]
enum Load {
    /// pgbench with no `Skilj` built, or after it shut down.
    Alone,
    /// A running `Skilj`, no commands.
    Idle,
    /// Spread commands at a fixed rate per second, from
    /// [`PACED_CALLERS`] callers.
    Paced(u32),
    /// Spread commands from this many callers, each as fast as it can.
    Spread(usize),
    /// Every command on one account, from this many callers.
    Hot(usize),
}

/// The callers a [`Load::Paced`] rate is split across.
const PACED_CALLERS: usize = 8;

impl Load {
    fn label(self) -> String {
        match self {
            Load::Alone => "alone".to_string(),
            Load::Idle => "skilj idle".to_string(),
            Load::Paced(rate) => format!("{rate} cmd/s"),
            Load::Spread(callers) => format!("spread x{callers}"),
            Load::Hot(callers) => format!("hot x{callers}"),
        }
    }
}

/// Deposits from `callers` concurrent callers, each submitting one after
/// another - at most one per `period`, if given - until `stop`. Returns
/// the latencies of the completed ones.
async fn command_load(
    setup: &Setup,
    hot: bool,
    callers: usize,
    period: Option<Duration>,
    stop: &AtomicBool,
) -> Vec<Duration> {
    let run = generate_token_id();
    let tasks = (0..callers).map(|caller| {
        let run = run.clone();
        async move {
            let mut latencies = Vec::new();
            let mut n = 0;
            // A caller that falls behind doesn't burst to catch up: the
            // achieved rate is what gets reported.
            let mut pace = period.map(|period| {
                let mut pace = tokio::time::interval(period);
                pace.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                pace
            });
            while !stop.load(Ordering::Relaxed) {
                if let Some(pace) = &mut pace {
                    pace.tick().await;
                }
                let account = if hot {
                    format!("{run}-hot")
                } else {
                    format!("{run}-{caller}-{n}")
                };
                latencies.push(deposit(setup, &account).await);
                n += 1;
            }
            latencies
        }
    });
    futures_util::future::join_all(tasks)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// One phase: pgbench for `seconds`, alongside `load`. Prints one row.
async fn phase(
    program: &Path,
    url: &str,
    seconds: u64,
    setup: Option<&Setup>,
    load: Load,
    baseline: Option<&PgbenchRun>,
) -> PgbenchRun {
    let stop = AtomicBool::new(false);
    let started = Instant::now();
    let commands = async {
        let (setup, hot, callers, period) = match (setup, load) {
            (Some(setup), Load::Paced(rate)) => (
                setup,
                false,
                PACED_CALLERS,
                Some(Duration::from_secs(1) * PACED_CALLERS as u32 / rate),
            ),
            (Some(setup), Load::Spread(callers)) => (setup, false, callers, None),
            (Some(setup), Load::Hot(callers)) => (setup, true, callers, None),
            _ => return Vec::new(),
        };
        command_load(setup, hot, callers, period, &stop).await
    };
    let pgbench = async {
        let run = pgbench_run(program, url, seconds).await;
        stop.store(true, Ordering::Relaxed);
        run
    };
    let (run, mut latencies) = tokio::join!(pgbench, commands);
    let elapsed = started.elapsed();

    let delta = |now: f64, then: f64| 100.0 * (now - then) / then;
    let vs_baseline = match baseline {
        Some(b) => format!(
            "tps {:>+6.1}%  p50 {:>+6.1}%  p99 {:>+6.1}%",
            delta(run.tps, b.tps),
            delta(run.p50_ms, b.p50_ms),
            delta(run.p99_ms, b.p99_ms),
        ),
        None => String::new(),
    };
    let skilj = if latencies.is_empty() {
        String::new()
    } else {
        latencies.sort_unstable();
        format!(
            " | skilj {:>5.0} cmd/s  p99 {:>6.1} ms",
            latencies.len() as f64 / elapsed.as_secs_f64(),
            latencies[(latencies.len() - 1) * 99 / 100].as_secs_f64() * 1000.0,
        )
    };
    println!(
        "{:<12} pgbench {:>7.0} tps  p50 {:>5.2} ms  p99 {:>6.2} ms  {vs_baseline}{skilj}",
        load.label(),
        run.tps,
        run.p50_ms,
        run.p99_ms,
    );
    run
}

#[test]
#[ignore = "benchmark - run explicitly, see the module docs"]
fn coresident_pgbench() {
    let seconds: u64 = std::env::var("SKILJ_BENCH_SECONDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let Some(database_url) = skilj_test_support::database_url("skilj_coresident_pgbench").await
        else {
            eprintln!("skipping: no test database");
            return;
        };
        let program = pgbench_program().await;
        let pool = db::connect(&database_url).await.unwrap();
        db::migrate(&pool).await.unwrap();
        pgbench_init(&program, &database_url).await;
        println!(
            "--- pgbench -c {CLIENTS} -j {THREADS} -s {SCALE}, {seconds} s per phase, \
             in skilj's database ---"
        );

        let first = phase(&program, &database_url, seconds, None, Load::Alone, None).await;
        let second = phase(
            &program,
            &database_url,
            seconds,
            None,
            Load::Alone,
            Some(&first),
        )
        .await;
        let baseline = PgbenchRun {
            tps: (first.tps + second.tps) / 2.0,
            p50_ms: (first.p50_ms + second.p50_ms) / 2.0,
            p99_ms: (first.p99_ms + second.p99_ms) / 2.0,
        };

        let setup = setup(database_url.clone(), &pool).await;
        // Warm-up: caches, prepared statements, pool connections.
        let warm_up = AtomicBool::new(false);
        tokio::join!(command_load(&setup, false, 8, None, &warm_up), async {
            tokio::time::sleep(Duration::from_secs(3)).await;
            warm_up.store(true, Ordering::Relaxed);
        });

        for load in [
            Load::Idle,
            Load::Paced(100),
            Load::Paced(250),
            Load::Spread(8),
            Load::Spread(32),
            Load::Hot(8),
        ] {
            phase(
                &program,
                &database_url,
                seconds,
                Some(&setup),
                load,
                Some(&baseline),
            )
            .await;
        }

        let Setup { skilj, router, .. } = setup;
        drop(router);
        skilj.shutdown(Duration::from_secs(10)).await;
        phase(
            &program,
            &database_url,
            seconds,
            None,
            Load::Alone,
            Some(&baseline),
        )
        .await;
    });
}
