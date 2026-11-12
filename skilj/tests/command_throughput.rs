//! Command throughput benchmark for issue #45 - not a test, so
//! `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --release -p skilj --test command_throughput -- --ignored --nocapture
//! ```
//!
//! (`DATABASE_URL` to run against a server of your own, otherwise the
//! embedded one.) Commands go through `POST /v1/commands/trigger` on
//! `Skilj::rest_router()` in-process - no network - from `workers`
//! concurrent callers, each submitting its share one after another, into
//! one bounded context: the per-bounded-context ceiling
//! docs/performance.md is about. Workloads:
//!
//! - **spread**: every command on its own account - `decide()` sees an
//!   empty history, the cheapest case;
//! - **ind-hot**: one reused account per caller, so keys are independent
//!   while each key's history grows;
//! - **cross**: one event tagged with two adjacent callers' accounts,
//!   forming a ring of overlapping consistency boundaries;
//! - **hot**: every command on one account - `decide()` folds that
//!   account's whole history, which grows as the run goes;
//! - **hot-snap**: every command on one account, through `DepositFast`,
//!   which decides from a `Balance` snapshot plus the events since it
//!   (docs/architecture.md §19, §188);
//! - **hot-none**: every command on one account, through `DepositBlind`,
//!   whose query reads nothing (docs/architecture.md §201).
//!
//! Besides throughput and latency, it collects the command batcher's own
//! `debug` events (`command batch leader lock wait`, `command batch phase
//! timing`) through a tracing layer, so each row also says how large the
//! batches were and where the time inside the lock went. The
//! `command optimistic read` event adds the read before the lock: its
//! average, the events it read, and the slowest read and decide; `command
//! decide under lock` adds how often a conflict made a command decide
//! again under the lock, and the slowest of those (docs/architecture.md
//! §203). See
//! docs/performance.md and docs/architecture.md §178 for the numbers.
//!
//! `SKILJ_BENCH_BATCH=n` adds, per workload, rows for 1 and 8 callers
//! that send their deposits `n` at a time through
//! `POST /v1/commands/trigger/batch` (docs/architecture.md §227); a
//! command's latency there is its call's.
//!
//! `SKILJ_BENCH_REQUIRE_DURABLE=1` refuses skipped database setup and
//! requires fsync, synchronous_commit and full_page_writes to be on.
//!
//! `SKILJ_BENCH_RECORD`, `SKILJ_BENCH_ROUNDS` and
//! `SKILJ_BENCH_SLOWDOWN_US` are `skilj_test_support::bench`'s: one JSON
//! line per row for `scripts/bench/compare.mjs`, the number of measured
//! rounds (2), and a known delay inside every request
//! (docs/performance.md, "Comparing two commits").
//! See docs/disjoint-consistency-boundaries.md for issue #158's analysis
//! and the remaining measurement work.

mod deposit_bench;

use deposit_bench::{
    deposit_batch_through, deposit_through, latency_proxy, linked_deposit_through, setup_instances,
    Setup,
};
use skilj_core::db;
use skilj_core::shared::generate_token_id;
use skilj_test_support::bench;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer as _;

/// Commands per scenario, unless `SKILJ_BENCH_COMMANDS` says otherwise.
const COMMANDS: usize = 800;

/// Raised when what a row measures changes, so `scripts/bench/compare.mjs`
/// stops comparing it with an older commit's.
const BENCH_VERSION: u32 = 1;

/// A whole-number environment setting, or `default`.
fn env_setting(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().unwrap_or_else(|_| panic!("{name}={value:?}")))
        .unwrap_or(default)
}

/// What the batcher's `debug` events add up to over one scenario.
#[derive(Default, Debug, Clone)]
struct BatchStats {
    batches: u64,
    commands: u64,
    lock_waits: u64,
    lock_wait_us: u64,
    decide_us: u64,
    sequence_us: u64,
    persist_us: u64,
    commit_us: u64,
    delta_queries: u64,
    delta_query_us: u64,
    reads: u64,
    events_read: u64,
    read_us: u64,
    read_max_us: u64,
    read_decide_us: u64,
    decide_max_us: u64,
    redecides: u64,
    redecide_max_us: u64,
}

/// A tracing layer summing the batcher's `debug` events into a
/// [`BatchStats`].
#[derive(Clone, Default)]
struct BatchStatsLayer(Arc<Mutex<BatchStats>>);

#[derive(Default)]
struct Fields {
    message: String,
    numbers: std::collections::HashMap<&'static str, u64>,
}

impl tracing::field::Visit for Fields {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.numbers.insert(field.name(), value);
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.numbers.insert(field.name(), value.max(0) as u64);
    }
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.numbers
            .insert(field.name(), u64::try_from(value).unwrap_or(u64::MAX));
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for BatchStatsLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        let n = |name| fields.numbers.get(name).copied().unwrap_or(0);
        let mut stats = self.0.lock().unwrap();
        match fields.message.as_str() {
            "command batch leader lock wait" => {
                stats.lock_waits += 1;
                stats.lock_wait_us += n("lock_wait_us");
            }
            "command decide delta query" => {
                stats.delta_queries += 1;
                stats.delta_query_us += n("delta_query_us");
            }
            "command optimistic read" => {
                stats.reads += 1;
                stats.events_read += n("events_read");
                stats.read_us += n("read_us");
                stats.read_max_us = stats.read_max_us.max(n("read_us"));
                stats.read_decide_us += n("decide_us");
                stats.decide_max_us = stats.decide_max_us.max(n("decide_us"));
            }
            "command decide under lock" => {
                stats.redecides += 1;
                stats.redecide_max_us = stats.redecide_max_us.max(n("decide_us"));
            }
            "command batch phase timing" => {
                stats.batches += 1;
                stats.commands += n("batch_size");
                stats.decide_us += n("decide_us");
                stats.sequence_us += n("sequence_us");
                stats.persist_us += n("persist_us");
                stats.commit_us += n("commit_us");
            }
            _ => {}
        }
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

#[derive(Clone, Copy)]
enum Workload {
    Spread,
    IndependentHot,
    CrossTag,
    Hot,
    HotSnapshot,
    HotNothing,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Workload::Spread => "spread",
            Workload::IndependentHot => "ind-hot",
            Workload::CrossTag => "cross",
            Workload::Hot => "hot",
            Workload::HotSnapshot => "hot-snap",
            Workload::HotNothing => "hot-none",
        }
    }
}

/// What the measured scenarios share: where they record, and the run's
/// settings their records repeat.
struct Run<'a> {
    recorder: &'a bench::Recorder,
    /// `None` for the warm-up, which is not recorded.
    round: Option<usize>,
    instances: usize,
    latency_ms: usize,
}

/// `COMMANDS` deposits from `workers` concurrent callers. Prints one row
/// and, unless it is the warm-up, records it.
async fn scenario(
    run_settings: &Run<'_>,
    setup: &Setup,
    stats: &BatchStatsLayer,
    workload: Workload,
    workers: usize,
    commands: usize,
    batch: usize,
) {
    let run = generate_token_id();
    let per_worker = (commands / workers).max(1);
    *stats.0.lock().unwrap() = BatchStats::default();
    let started = Instant::now();
    let tasks = (0..workers).map(|worker| {
        let run = run.clone();
        async move {
            let mut latencies = Vec::with_capacity(per_worker);
            let accounts: Vec<String> = (0..per_worker)
                .map(|n| match workload {
                    Workload::Spread => format!("{run}-{worker}-{n}"),
                    Workload::IndependentHot => format!("{run}-key-{worker}"),
                    Workload::CrossTag => format!("{run}-key-{worker}"),
                    Workload::Hot | Workload::HotSnapshot | Workload::HotNothing => {
                        format!("{run}-hot")
                    }
                })
                .collect();
            let credential = match workload {
                Workload::HotSnapshot => &setup.fast_credential,
                Workload::HotNothing => &setup.blind_credential,
                _ => &setup.credential,
            };
            if matches!(workload, Workload::CrossTag) {
                for account in &accounts {
                    // Adjacent pairs form a ring: each event overlaps two
                    // workers' boundaries without one universal hot tag.
                    let pair = [
                        account.clone(),
                        format!("{run}-key-{}", (worker + 1) % workers),
                    ];
                    latencies.push(
                        linked_deposit_through(
                            setup.router_for(worker),
                            &setup.linked_credential,
                            &pair,
                        )
                        .await,
                    );
                }
            } else if batch > 1 {
                // docs/architecture.md §227: `batch` deposits a call, each
                // command's latency that of its call.
                for chunk in accounts.chunks(batch) {
                    let latency =
                        deposit_batch_through(setup.router_for(worker), credential, chunk).await;
                    latencies.extend(std::iter::repeat_n(latency, chunk.len()));
                }
            } else {
                for account in &accounts {
                    latencies
                        .push(deposit_through(setup.router_for(worker), credential, account).await);
                }
            }
            latencies
        }
    });
    let mut latencies: Vec<Duration> = futures_util::future::join_all(tasks)
        .await
        .into_iter()
        .flatten()
        .collect();
    let elapsed = started.elapsed();
    latencies.sort_unstable();
    let pct = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    let s = stats.0.lock().unwrap().clone();
    let per_command = |us: u64| us as f64 / s.commands.max(1) as f64;
    println!(
        "{:<8} {:>3} workers {:>3} a call {:>6.0} cmd/s  p50 {:>6.1} ms  p99 {:>6.1} ms | batch {:>5.1}  \
         lock wait {:>6.1} ms | per cmd in lock: decide {:>5.0} us (delta query {:>5.0} us, {:>3.0}%)  \
         seq {:>4.0} us  persist {:>5.0} us  commit {:>5.0} us | before lock: read {:>5.0} us \
         ({:>5.0} events)  decide {:>5.0} us  slowest read {:>6.1} ms  slowest decide {:>6.2} ms | \
         decided again under lock {:>4} times, slowest {:>6.2} ms",
        workload.name(),
        workers,
        batch.max(1),
        latencies.len() as f64 / elapsed.as_secs_f64(),
        pct(0.50).as_secs_f64() * 1000.0,
        pct(0.99).as_secs_f64() * 1000.0,
        s.commands as f64 / s.batches.max(1) as f64,
        s.lock_wait_us as f64 / s.lock_waits.max(1) as f64 / 1000.0,
        per_command(s.decide_us),
        per_command(s.delta_query_us),
        100.0 * s.delta_queries as f64 / s.commands.max(1) as f64,
        per_command(s.sequence_us),
        per_command(s.persist_us),
        per_command(s.commit_us),
        s.read_us as f64 / s.reads.max(1) as f64,
        s.events_read as f64 / s.reads.max(1) as f64,
        s.read_decide_us as f64 / s.reads.max(1) as f64,
        s.read_max_us as f64 / 1000.0,
        s.decide_max_us as f64 / 1000.0,
        s.redecides,
        s.redecide_max_us as f64 / 1000.0,
    );
    if let Some(round) = run_settings.round {
        let mut name = format!("{}/{workers}w", workload.name());
        if batch > 1 {
            name.push_str(&format!("/{batch}c"));
        }
        run_settings.recorder.scenario(bench::Scenario {
            name,
            round,
            unit: "command",
            operations: latencies.len(),
            expected_operations: per_worker * workers,
            elapsed,
            samples: &latencies,
            settings: serde_json::json!({
                "workload": workload.name(),
                "workers": workers,
                "commands_per_call": batch.max(1),
                "commands": commands,
                "instances": run_settings.instances,
                "added_round_trip_ms": run_settings.latency_ms,
                "batches": s.batches,
                "commands_in_batches": s.commands,
            }),
        });
    }
}

#[test]
#[ignore = "benchmark - run explicitly, see the module docs"]
fn command_throughput() {
    let recorder = bench::Recorder::from_env("command_throughput", BENCH_VERSION);
    let stats = BatchStatsLayer::default();
    let subscriber = tracing_subscriber::registry().with(
        stats
            .clone()
            .with_filter(tracing_subscriber::EnvFilter::new("skilj_core=debug")),
    );
    // Global, not `set_default`: the batch leader logs from the runtime's
    // worker threads. This binary holds this one test.
    tracing::subscriber::set_global_default(subscriber).unwrap();
    runtime().block_on(async {
        let Some(database_url) =
            skilj_test_support::database_url("skilj_command_throughput_bench").await
        else {
            assert_ne!(
                std::env::var("SKILJ_BENCH_REQUIRE_DURABLE").as_deref(),
                Ok("1"),
                "durable benchmark requires a working database; it cannot skip"
            );
            recorder.skip("no test database");
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();
        // Never report an embedded, non-durable run as production evidence.
        let settings: Vec<(String, String)> = sqlx::query_as(
            "SELECT name, setting FROM pg_settings WHERE name IN \
             ('fsync', 'synchronous_commit', 'full_page_writes', 'server_version') ORDER BY name",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        println!("Postgres settings: {settings:?}");
        if std::env::var("SKILJ_BENCH_REQUIRE_DURABLE").as_deref() == Ok("1") {
            for name in ["fsync", "synchronous_commit", "full_page_writes"] {
                assert!(
                    settings
                        .iter()
                        .any(|(key, value)| key == name && value == "on"),
                    "durable benchmark requires {name}=on"
                );
            }
        }
        db::migrate(&pool).await.unwrap();
        // docs/architecture.md §196: the in-process router makes every
        // round trip to Postgres nearly free, which understates what a
        // per-command statement costs a deployment whose database is
        // across a network. `SKILJ_BENCH_LATENCY_MS` adds that round trip
        // between skilj and Postgres; `SKILJ_BENCH_INSTANCES` runs several
        // instances on the one bounded context, each with its own batcher.
        let latency_ms = env_setting("SKILJ_BENCH_LATENCY_MS", 0);
        let instances = env_setting("SKILJ_BENCH_INSTANCES", 1);
        let commands = env_setting("SKILJ_BENCH_COMMANDS", COMMANDS);
        // docs/architecture.md §227: also 1 and 8 callers sending this many
        // deposits per `POST /v1/commands/trigger/batch` call.
        let batch = env_setting("SKILJ_BENCH_BATCH", 0);
        let skilj_url = if latency_ms > 0 {
            latency_proxy(&database_url, Duration::from_millis(latency_ms as u64)).await
        } else {
            database_url
        };
        let setup = setup_instances(skilj_url, &pool, instances).await;
        println!(
            "round trip to Postgres +{latency_ms} ms, {instances} instance(s), \
             {commands} commands per scenario"
        );

        let rounds = bench::rounds(2);
        let mut run = Run {
            recorder: &recorder,
            round: None,
            instances,
            latency_ms,
        };

        // Warm-up: caches, prepared statements, pool connections.
        scenario(&run, &setup, &stats, Workload::Spread, 8, commands, 1).await;

        for round in 1..=rounds {
            run.round = Some(round);
            println!("--- round {round} ---");
            for workload in [
                Workload::Spread,
                Workload::IndependentHot,
                Workload::CrossTag,
                Workload::Hot,
                Workload::HotSnapshot,
                Workload::HotNothing,
            ] {
                let workload_name = workload.name();
                if let Ok(selected) = std::env::var("SKILJ_BENCH_WORKLOADS") {
                    if !selected.split(',').any(|name| name == workload_name) {
                        continue;
                    }
                }
                for workers in [1, 8, 32, 80] {
                    if let Ok(selected) = std::env::var("SKILJ_BENCH_WORKERS") {
                        if !selected
                            .split(',')
                            .any(|n| n.parse::<usize>().unwrap() == workers)
                        {
                            continue;
                        }
                    }
                    scenario(&run, &setup, &stats, workload, workers, commands, 1).await;
                }
                if batch > 1 && !matches!(workload, Workload::CrossTag) {
                    for workers in [1, 8] {
                        scenario(&run, &setup, &stats, workload, workers, commands, batch).await;
                    }
                }
            }
        }
        setup.shutdown().await;
        recorder.complete(serde_json::json!({
            "postgres": bench::postgres_settings(&pool).await,
            "rounds": rounds,
            "warmup": "one unrecorded spread scenario with 8 callers",
            "require_durable": std::env::var("SKILJ_BENCH_REQUIRE_DURABLE").as_deref() == Ok("1"),
            "boundary": "in-process POST /v1/commands/trigger on Skilj::rest_router(), no network",
        }));
    });
}
