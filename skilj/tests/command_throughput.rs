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
//!   account's whole history, which grows as the run goes.
//!
//! Besides throughput and latency, it collects the command batcher's own
//! `debug` events (`command batch leader lock wait`, `command batch phase
//! timing`) through a tracing layer, so each row also says how large the
//! batches were and where the time inside the lock went. See
//! docs/performance.md and docs/architecture.md §178 for the numbers.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, Event};
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{
    generate_token_id, generate_token_secret, CommandDecision, EventSpec, TagMapping,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tower::ServiceExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer as _;

/// Commands per scenario.
const COMMANDS: usize = 800;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct DepositedPayload {
    account_id: String,
    amount: i64,
}

struct Deposited;

impl EventType for Deposited {
    type Payload = DepositedPayload;
    const NAME: &'static str = "Deposited";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping {
            key: "account".into(),
            field: "account_id".into(),
        }]
    }
}

enum AccountEvent {
    Deposited(DepositedPayload),
}

impl BoundedContextEvent for AccountEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "Deposited" => Some(serde_json::from_str(&event.payload).map(AccountEvent::Deposited)),
            _ => None,
        }
    }
}

struct Deposit;

impl CommandType for Deposit {
    type Payload = DepositedPayload;
    type Event = AccountEvent;
    const NAME: &'static str = "Deposit";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping {
            key: "account".into(),
            field: "account_id".into(),
        }]
    }
    /// A real decision over the account's history: deposits are capped,
    /// so the balance has to be folded first.
    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        let balance: i64 = matching_events
            .iter()
            .map(|AccountEvent::Deposited(d)| d.amount)
            .sum();
        if balance + payload.amount > i64::MAX / 2 {
            return CommandDecision::Rejected {
                reason: "balance cap".to_string(),
                kind: "balance_cap".to_string(),
            };
        }
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "Deposited".to_string(),
                payload: serde_json::json!({
                    "account_id": payload.account_id,
                    "amount": payload.amount,
                }),
            }],
        }
    }
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

fn now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

struct Setup {
    router: axum::Router,
    credential: String,
}

async fn setup(database_url: String, pool: &Pool) -> Setup {
    let subject = format!("subject_{}", generate_token_id());
    let role = Role {
        id: generate_token_id(),
        external_subject: subject.clone(),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();
    let bc_name = format!("accounts_{}", generate_token_id());
    let bc = BoundedContext {
        name: bc_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    let mapping = RoleAccessMapping {
        role,
        bounded_context: bc,
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        scope: None,
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(pool, &mapping)
        .await
        .unwrap();

    // The pool of a modest deployment: half of it may lead batches.
    let (skilj, _) = Skilj::builder(database_url)
        .pool_options(db::PgPoolOptions::new().max_connections(20))
        .bounded_context(bc_name.clone())
        .event_type::<Deposited>()
        .command_type::<Deposit>()
        .reconciliation_role(subject)
        .build()
        .await
        .unwrap();

    let command_type = db::get_command_type(pool, &bc_name, "Deposit")
        .await
        .unwrap()
        .unwrap();
    let token = access_control::create_command_token(
        &mapping,
        &command_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        now(),
    )
    .unwrap();
    db::insert_command_token(pool, &token).await.unwrap();

    let router = skilj.rest_router();
    // The `Skilj` owns background tasks the router relies on.
    std::mem::forget(skilj);
    Setup {
        router,
        credential: format!("{}.{}", token.id, token.secret),
    }
}

/// Submits one deposit and returns its latency.
async fn deposit(setup: &Setup, account_id: &str) -> Duration {
    let body = serde_json::json!({ "payload": { "account_id": account_id, "amount": 1 } });
    let request = Request::builder()
        .method("POST")
        .uri("/v1/commands/trigger")
        .header("authorization", format!("Bearer {}", setup.credential))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let started = Instant::now();
    let response = setup.router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    started.elapsed()
}

#[derive(Clone, Copy)]
enum Workload {
    Spread,
    Hot,
}

/// `COMMANDS` deposits from `workers` concurrent callers. Prints one row.
async fn scenario(setup: &Setup, stats: &BatchStatsLayer, workload: Workload, workers: usize) {
    let run = generate_token_id();
    let per_worker = COMMANDS / workers;
    *stats.0.lock().unwrap() = BatchStats::default();
    let started = Instant::now();
    let tasks = (0..workers).map(|worker| {
        let run = run.clone();
        async move {
            let mut latencies = Vec::with_capacity(per_worker);
            for n in 0..per_worker {
                let account = match workload {
                    Workload::Spread => format!("{run}-{worker}-{n}"),
                    Workload::Hot => format!("{run}-hot"),
                };
                latencies.push(deposit(setup, &account).await);
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
        "{:<6} {:>3} workers {:>6.0} cmd/s  p50 {:>6.1} ms  p99 {:>6.1} ms | batch {:>5.1}  \
         lock wait {:>6.1} ms | per cmd in lock: decide {:>5.0} us (delta query {:>5.0} us, {:>3.0}%)  \
         seq {:>4.0} us  persist {:>5.0} us  commit {:>5.0} us",
        match workload {
            Workload::Spread => "spread",
            Workload::Hot => "hot",
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
        let setup = setup(database_url, &pool).await;

        // Warm-up: caches, prepared statements, pool connections.
        scenario(&setup, &stats, Workload::Spread, 8).await;

        for round in 1..=2 {
            println!("--- round {round} ({COMMANDS} commands each) ---");
            for workload in [Workload::Spread, Workload::Hot] {
                for workers in [1, 8, 32, 80] {
                    scenario(&setup, &stats, workload, workers).await;
                }
            }
        }
    });
}
