//! Inbound throughput benchmark for Codeberg issue #41 - not a test, so
//! `#[ignore]`d. Run with:
//!
//! ```sh
//! DATABASE_URL=postgres://... cargo test --release -p skilj \
//!     --test inbound_throughput -- --ignored --nocapture
//! ```
//!
//! It answers "how fast can a broker bridge feed skilj?" the way the
//! bridges call it today: `POST /v1/events/external` (with `dedupe`) and
//! `POST /v1/commands/trigger` (with an `Idempotency-Key`), each awaited
//! before the next is sent - one message in flight per consumer - and
//! then with one message in flight per *partition* across several
//! partitions, which is the most concurrency a bridge can add without
//! breaking per-partition order (§39's dedupe watermark requires a
//! partition's messages in order; so does `decide()` for commands). The
//! requests go through `Skilj::rest_router()` in-process, so there is no
//! network latency: a real bridge is slower per message, which only makes
//! one-in-flight cost more. See docs/architecture.md §172 for the numbers
//! and what was concluded from them.

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
use std::time::{Duration, Instant};
use tower::ServiceExt;

/// Messages per scenario.
const MESSAGES: usize = 400;
/// Partitions in the concurrent scenarios.
const PARTITIONS: usize = 8;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct OrderPlacedPayload {
    order_id: String,
}

struct OrderPlaced;

impl EventType for OrderPlaced {
    type Payload = OrderPlacedPayload;
    const NAME: &'static str = "OrderPlaced";
    fn external_creation_allowed() -> bool {
        true
    }
}

enum OrderEvent {
    #[allow(dead_code)]
    OrderPlaced(OrderPlacedPayload),
}

impl BoundedContextEvent for OrderEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "OrderPlaced" => {
                Some(serde_json::from_str(&event.payload).map(OrderEvent::OrderPlaced))
            }
            _ => None,
        }
    }
}

struct PlaceOrder;

impl CommandType for PlaceOrder {
    type Payload = OrderPlacedPayload;
    type Event = OrderEvent;
    const NAME: &'static str = "PlaceOrder";
    fn rest_trigger_allowed() -> bool {
        true
    }
    /// A consistency tag per order, as a real command would have - each
    /// decide reads the (empty) history of its own order.
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping {
            key: "order".into(),
            field: "order_id".into(),
        }]
    }
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "OrderPlaced".to_string(),
                payload: serde_json::json!({ "order_id": payload.order_id }),
            }],
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
    external_credential: String,
    command_credential: String,
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
    let bc_name = format!("orders_{}", generate_token_id());
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

    let (skilj, _) = Skilj::builder(database_url)
        .pool_options(db::PgPoolOptions::new().max_connections(20))
        .bounded_context(bc_name.clone())
        .event_type::<OrderPlaced>()
        .command_type::<PlaceOrder>()
        .reconciliation_role(subject)
        .build()
        .await
        .unwrap();

    let event_type = db::get_event_type(pool, &bc_name, "OrderPlaced")
        .await
        .unwrap()
        .unwrap();
    let external = access_control::create_external_event_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        now(),
    )
    .unwrap();
    db::insert_external_event_token(pool, &external)
        .await
        .unwrap();
    let command_type = db::get_command_type(pool, &bc_name, "PlaceOrder")
        .await
        .unwrap()
        .unwrap();
    let command = access_control::create_command_token(
        &mapping,
        &command_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        now(),
    )
    .unwrap();
    db::insert_command_token(pool, &command).await.unwrap();

    let router = skilj.rest_router();
    // The `Skilj` owns background tasks the router relies on.
    std::mem::forget(skilj);
    Setup {
        router,
        external_credential: format!("{}.{}", external.id, external.secret),
        command_credential: format!("{}.{}", command.id, command.secret),
    }
}

#[derive(Clone, Copy)]
enum Kind {
    External,
    Trigger,
}

/// Sends one message the way a bridge does and returns its latency.
async fn send(setup: &Setup, kind: Kind, partition: usize, offset: usize, run: &str) -> Duration {
    let order_id = format!("{run}-{partition}-{offset}");
    let (uri, credential, body, key) = match kind {
        Kind::External => (
            "/v1/events/external",
            &setup.external_credential,
            serde_json::json!({
                "payload": { "order_id": order_id },
                "sourceContent": format!("kafka:{run}:{partition}:{offset}"),
                "dedupe": { "partitionKey": format!("{run}:{partition}"), "sequence": offset },
            }),
            None,
        ),
        Kind::Trigger => (
            "/v1/commands/trigger",
            &setup.command_credential,
            serde_json::json!({ "payload": { "order_id": order_id } }),
            Some(format!("{run}:{partition}:{offset}")),
        ),
    };
    let mut request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json");
    if let Some(key) = key {
        request = request.header("Idempotency-Key", key);
    }
    let request = request.body(Body::from(body.to_string())).unwrap();
    let started = Instant::now();
    let response = setup.router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "{uri}: {status} {}",
        String::from_utf8_lossy(&bytes)
    );
    started.elapsed()
}

/// `MESSAGES` messages over `partitions` partitions, one in flight per
/// partition. Prints throughput and latency percentiles.
async fn scenario(setup: &Setup, kind: Kind, partitions: usize, label: &str) {
    let run = generate_token_id();
    let per_partition = MESSAGES / partitions;
    let started = Instant::now();
    let tasks = (0..partitions).map(|partition| {
        let run = run.clone();
        async move {
            let mut latencies = Vec::with_capacity(per_partition);
            for offset in 0..per_partition {
                latencies.push(send(setup, kind, partition, offset, &run).await);
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
    println!(
        "{label:<44} {:>7.0} msg/s   p50 {:>6.1} ms   p99 {:>6.1} ms",
        latencies.len() as f64 / elapsed.as_secs_f64(),
        pct(0.50).as_secs_f64() * 1000.0,
        pct(0.99).as_secs_f64() * 1000.0,
    );
}

#[test]
#[ignore = "benchmark - run explicitly, see the module docs"]
fn inbound_throughput() {
    runtime().block_on(async {
        let Some(database_url) =
            skilj_test_support::database_url("skilj_inbound_throughput_bench").await
        else {
            eprintln!("skipping: no test database");
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();
        db::migrate(&pool).await.unwrap();
        let setup = setup(database_url, &pool).await;

        // Warm-up: caches, prepared statements, pool connections.
        scenario(&setup, Kind::External, PARTITIONS, "(warm-up, external)").await;
        scenario(&setup, Kind::Trigger, PARTITIONS, "(warm-up, trigger)").await;

        for round in 1..=2 {
            println!("--- round {round} ({MESSAGES} messages each) ---");
            scenario(&setup, Kind::External, 1, "external event, 1 in flight").await;
            scenario(
                &setup,
                Kind::External,
                PARTITIONS,
                &format!("external event, {PARTITIONS} partitions in flight"),
            )
            .await;
            scenario(&setup, Kind::Trigger, 1, "command trigger, 1 in flight").await;
            scenario(
                &setup,
                Kind::Trigger,
                PARTITIONS,
                &format!("command trigger, {PARTITIONS} partitions in flight"),
            )
            .await;
        }
    });
}
