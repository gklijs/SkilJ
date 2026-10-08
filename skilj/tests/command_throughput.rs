//! Command throughput benchmark for Codeberg issue #45 - not a test, so
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
//! docs/performance.md is about. Two workloads:
//!
//! - **spread**: every command on its own account - `decide()` sees an
//!   empty history, the cheapest case;
//! - **hot**: every command on one account - `decide()` folds that
//!   account's whole history, which grows as the run goes;
//! - **hot-snap**: every command on one account, through `DepositFast`,
//!   which decides from a `Balance` snapshot plus the events since it
//!   (docs/architecture.md §19, §188).
//!
//! Besides throughput and latency, it collects the command batcher's own
//! `debug` events (`command batch leader lock wait`, `command batch phase
//! timing`) through a tracing layer, so each row also says how large the
//! batches were and where the time inside the lock went. See
//! docs/performance.md and docs/architecture.md §178 for the numbers.

mod deposit_bench;

use deposit_bench::{deposit_through, latency_proxy, setup_instances, Setup};
use skilj_core::db;
use skilj_core::shared::generate_token_id;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer as _;

/// Commands per scenario, unless `SKILJ_BENCH_COMMANDS` says otherwise.
const COMMANDS: usize = 800;

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
    Hot,
    HotSnapshot,
}

/// `COMMANDS` deposits from `workers` concurrent callers. Prints one row.
async fn scenario(
    setup: &Setup,
    stats: &BatchStatsLayer,
    workload: Workload,
    workers: usize,
    commands: usize,
) {
    let run = generate_token_id();
    let per_worker = (commands / workers).max(1);
    *stats.0.lock().unwrap() = BatchStats::default();
    let started = Instant::now();
    let tasks = (0..workers).map(|worker| {
        let run = run.clone();
        async move {
            let mut latencies = Vec::with_capacity(per_worker);
            for n in 0..per_worker {
                let account = match workload {
                    Workload::Spread => format!("{run}-{worker}-{n}"),
                    Workload::Hot | Workload::HotSnapshot => format!("{run}-hot"),
                };
                let credential = match workload {
                    Workload::HotSnapshot => &setup.fast_credential,
                    _ => &setup.credential,
                };
                latencies
                    .push(deposit_through(setup.router_for(worker), credential, &account).await);
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
        "{:<8} {:>3} workers {:>6.0} cmd/s  p50 {:>6.1} ms  p99 {:>6.1} ms | batch {:>5.1}  \
         lock wait {:>6.1} ms | per cmd in lock: decide {:>5.0} us (delta query {:>5.0} us, {:>3.0}%)  \
         seq {:>4.0} us  persist {:>5.0} us  commit {:>5.0} us",
        match workload {
            Workload::Spread => "spread",
            Workload::Hot => "hot",
            Workload::HotSnapshot => "hot-snap",
        },
        workers,
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
    );
}

#[test]
#[ignore = "benchmark - run explicitly, see the module docs"]
fn command_throughput() {
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
            eprintln!("skipping: no test database");
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();
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

        // Warm-up: caches, prepared statements, pool connections.
        scenario(&setup, &stats, Workload::Spread, 8, commands).await;

        for round in 1..=2 {
            println!("--- round {round} ---");
            for workload in [Workload::Spread, Workload::Hot, Workload::HotSnapshot] {
                for workers in [1, 8, 32, 80] {
                    scenario(&setup, &stats, workload, workers, commands).await;
                }
            }
        }
        setup.shutdown().await;
    });
}
