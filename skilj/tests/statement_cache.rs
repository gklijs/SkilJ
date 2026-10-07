//! sqlx statement cache benchmark for Codeberg issue #56 - not a test, so
//! `#[ignore]`d. Run with:
//!
//! ```sh
//! cargo test --release -p skilj --test statement_cache -- --ignored --nocapture
//! ```
//!
//! sqlx caches up to `statement-cache-capacity` (default 100) prepared
//! statements per connection, least recently used out. skilj's SQL is
//! schema-qualified per bounded context, so each bounded context brings
//! its own set. A miss costs a Parse/Describe round trip, and an eviction
//! a Close round trip, on top of the execute. This spreads deposits over
//! `K` bounded contexts in one `Skilj` and compares throughput at several
//! capacities, set the way a deployment would: the
//! `statement-cache-capacity` parameter of the database URL. It also counts
//! the distinct statements the run sent, from sqlx's own `sqlx::query`
//! events. See docs/architecture.md §193.

// Only the types and `deposit_through`; the rest is command_throughput's.
#[allow(dead_code)]
mod deposit_bench;

use deposit_bench::{deposit_through, now, Deposit, Deposited};
use skilj::Skilj;
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer as _;

/// Commands per measured scenario.
const COMMANDS: usize = 1600;

/// Concurrent callers.
const WORKERS: usize = 8;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

/// Collects the SQL of every `sqlx::query` event while `on` is set.
#[derive(Clone, Default)]
struct StatementLog {
    on: Arc<AtomicBool>,
    statements: Arc<Mutex<BTreeSet<String>>>,
}

#[derive(Default)]
struct Sql {
    summary: String,
    statement: String,
}

impl tracing::field::Visit for Sql {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "summary" => self.summary = value.to_string(),
            "db.statement" => self.statement = value.trim().to_string(),
            _ => {}
        }
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.record_str(field, &format!("{value:?}"));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for StatementLog {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut sql = Sql::default();
        event.record(&mut sql);
        // sqlx leaves `db.statement` empty when the summary is the whole
        // statement.
        let text = if sql.statement.is_empty() {
            sql.summary
        } else {
            sql.statement
        };
        self.statements.lock().unwrap().insert(text);
    }
}

struct Setup {
    skilj: Skilj,
    router: axum::Router,
    /// One `Deposit` token per bounded context.
    credentials: Vec<String>,
    bc_names: Vec<String>,
}

async fn setup(database_url: &str, pool: &Pool, contexts: usize, capacity: usize) -> Setup {
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
    let mut mappings = Vec::new();
    for _ in 0..contexts {
        let bc = BoundedContext {
            name: format!("stmt_{}", generate_token_id()),
            status: BoundedContextStatus::Active,
            created_at: now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(pool, &bc).await.unwrap();
        let mapping = RoleAccessMapping {
            role: role.clone(),
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
        mappings.push(mapping);
    }

    let separator = if database_url.contains('?') { '&' } else { '?' };
    let url = format!("{database_url}{separator}statement-cache-capacity={capacity}");
    let mut builder = Skilj::builder(url)
        .pool_options(db::PgPoolOptions::new().max_connections(20))
        .reconciliation_role(subject);
    for mapping in &mappings {
        builder = builder
            .bounded_context(mapping.bounded_context.name.clone())
            .event_type::<Deposited>()
            .command_type::<Deposit>();
    }
    let (skilj, _) = builder.build().await.unwrap();

    let mut credentials = Vec::new();
    for mapping in &mappings {
        let command_type = db::get_command_type(pool, &mapping.bounded_context.name, "Deposit")
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_command_token(
            mapping,
            &command_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            now(),
        )
        .unwrap();
        db::insert_command_token(pool, &token).await.unwrap();
        credentials.push(format!("{}.{}", token.id, token.secret));
    }
    Setup {
        router: skilj.rest_router(),
        skilj,
        credentials,
        bc_names: mappings
            .into_iter()
            .map(|m| m.bounded_context.name)
            .collect(),
    }
}

/// `commands` deposits from `WORKERS` callers, each command on its own
/// account, round-robin over the bounded contexts. Returns commands/s and
/// the p50 latency.
async fn run(setup: &Setup, commands: usize) -> (f64, Duration) {
    let run = generate_token_id();
    let per_worker = commands / WORKERS;
    let started = Instant::now();
    let tasks = (0..WORKERS).map(|worker| {
        let run = run.clone();
        async move {
            let mut latencies = Vec::with_capacity(per_worker);
            for n in 0..per_worker {
                let credential =
                    &setup.credentials[(worker + n * WORKERS) % setup.credentials.len()];
                let latency =
                    deposit_through(&setup.router, credential, &format!("{run}-{worker}-{n}"))
                        .await;
                latencies.push(latency);
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
    (
        latencies.len() as f64 / elapsed.as_secs_f64(),
        latencies[latencies.len() / 2],
    )
}

#[test]
#[ignore = "benchmark - run explicitly, see the module docs"]
fn statement_cache() {
    let log = StatementLog::default();
    let on = log.on.clone();
    let subscriber = tracing_subscriber::registry().with(log.clone().with_filter(
        tracing_subscriber::filter::dynamic_filter_fn(move |metadata, _| {
            metadata.target() == "sqlx::query" && on.load(Ordering::Relaxed)
        }),
    ));
    tracing::subscriber::set_global_default(subscriber).unwrap();
    runtime().block_on(async {
        let Some(database_url) =
            skilj_test_support::database_url("skilj_statement_cache_bench").await
        else {
            eprintln!("skipping: no test database");
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();
        db::migrate(&pool).await.unwrap();

        // What one bounded context's commands send, and how it scales.
        for contexts in [1, 8] {
            let setup = setup(&database_url, &pool, contexts, 100).await;
            run(&setup, 400).await;
            log.statements.lock().unwrap().clear();
            log.on.store(true, Ordering::Relaxed);
            run(&setup, 800).await;
            log.on.store(false, Ordering::Relaxed);
            let statements = std::mem::take(&mut *log.statements.lock().unwrap());
            let mut per_context: HashMap<&str, usize> = HashMap::new();
            let mut shared = 0;
            for statement in &statements {
                match setup
                    .bc_names
                    .iter()
                    .find(|bc| statement.contains(bc.as_str()))
                {
                    Some(bc) => *per_context.entry(bc).or_default() += 1,
                    None => shared += 1,
                }
            }
            println!(
                "{contexts:>3} bounded contexts: {} distinct statements - {shared} shared, \
                 per bounded context {:?}",
                statements.len(),
                per_context.values().collect::<BTreeSet<_>>(),
            );
            if contexts == 1 {
                for statement in &statements {
                    println!("    {}", statement.replace('\n', " "));
                }
            }
            setup.skilj.shutdown(Duration::from_secs(10)).await;
        }

        for round in 1..=2 {
            println!("--- round {round} ({COMMANDS} commands, {WORKERS} callers) ---");
            for contexts in [1, 8, 32, 64] {
                for capacity in [100, 400, 1600] {
                    let setup = setup(&database_url, &pool, contexts, capacity).await;
                    // Warm-up: every bounded context's statements, every
                    // pool connection.
                    run(&setup, 800).await;
                    let (rate, p50) = run(&setup, COMMANDS).await;
                    println!(
                        "{contexts:>3} bounded contexts, capacity {capacity:>4}: \
                         {rate:>6.0} cmd/s  p50 {:>5.1} ms",
                        p50.as_secs_f64() * 1000.0
                    );
                    setup.skilj.shutdown(Duration::from_secs(10)).await;
                }
            }
        }
    });
}
