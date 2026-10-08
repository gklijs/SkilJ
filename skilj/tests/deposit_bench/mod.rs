//! What `command_throughput.rs` and `coresident_pgbench.rs` share: a
//! bounded context with one capped `Deposit` command over `Deposited`
//! events tagged by account, a REST token for it, and a helper that
//! submits one deposit through the in-process router. `DepositFast` is
//! the same command deciding from a `Balance` snapshot (§19).

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj, Snapshot};
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

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DepositedPayload {
    pub account_id: String,
    pub amount: i64,
}

pub struct Deposited;

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

pub enum AccountEvent {
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

pub struct Deposit;

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
        deposit_decision(payload, balance_of(matching_events))
    }
}

fn deposit_decision(payload: &DepositedPayload, balance: i64) -> CommandDecision {
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

fn balance_of(events: &[AccountEvent]) -> i64 {
    events
        .iter()
        .map(|AccountEvent::Deposited(d)| d.amount)
        .sum()
}

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct BalanceState {
    pub balance: i64,
}

pub struct Balance;

impl Snapshot for Balance {
    type State = BalanceState;
    type Event = AccountEvent;
    const NAME: &'static str = "Balance";
    const TAG_KEY: &'static str = "account";
    const VERSION: u64 = 1;
    fn fold(state: &mut Self::State, AccountEvent::Deposited(d): &Self::Event) {
        state.balance += d.amount;
    }
}

/// `Deposit`, deciding from the `Balance` snapshot plus the events since.
pub struct DepositFast;

impl CommandType for DepositFast {
    type Payload = DepositedPayload;
    type Event = AccountEvent;
    const NAME: &'static str = "DepositFast";
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn tag_mappings() -> Vec<TagMapping> {
        Deposit::tag_mappings()
    }
    fn snapshot() -> Option<&'static str> {
        Some(Balance::NAME)
    }
    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        deposit_decision(payload, balance_of(matching_events))
    }
    fn decide_from_snapshot(
        payload: &Self::Payload,
        snapshot_state_json: &str,
        events_since_snapshot: &[Self::Event],
    ) -> CommandDecision {
        let state: BalanceState = serde_json::from_str(snapshot_state_json).unwrap_or_default();
        deposit_decision(payload, state.balance + balance_of(events_since_snapshot))
    }
}

pub fn now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

pub struct Setup {
    /// Owns the background tasks `router` relies on.
    pub skilj: Skilj,
    pub router: axum::Router,
    /// More instances on the same database and bounded context, for
    /// `command_throughput.rs`' `SKILJ_BENCH_INSTANCES`: each with its own
    /// command batcher, as separate processes would be.
    #[allow(dead_code)] // command_throughput.rs only
    pub others: Vec<(Skilj, axum::Router)>,
    /// `Deposit`'s token.
    pub credential: String,
    /// `DepositFast`'s token.
    #[allow(dead_code)] // command_throughput.rs only
    pub fast_credential: String,
}

#[allow(dead_code)] // coresident_pgbench.rs only
pub async fn setup(database_url: String, pool: &Pool) -> Setup {
    setup_instances(database_url, pool, 1).await
}

impl Setup {
    /// The router a caller numbered `worker` submits through: callers are
    /// spread over the instances round-robin.
    #[allow(dead_code)] // command_throughput.rs only
    pub fn router_for(&self, worker: usize) -> &axum::Router {
        match worker % (self.others.len() + 1) {
            0 => &self.router,
            n => &self.others[n - 1].1,
        }
    }

    /// Shuts every instance down.
    #[allow(dead_code)] // command_throughput.rs only
    pub async fn shutdown(self) {
        for (skilj, _) in self.others {
            skilj.shutdown(Duration::from_secs(10)).await;
        }
        self.skilj.shutdown(Duration::from_secs(10)).await;
    }
}

/// [`setup`], with `instances` `Skilj` instances on `database_url`, all
/// serving the one bounded context.
pub async fn setup_instances(database_url: String, pool: &Pool, instances: usize) -> Setup {
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
    let build = || async {
        Skilj::builder(database_url.clone())
            .pool_options(
                // skilj's defaults, unless `SKILJ_BENCH_TEST_BEFORE_ACQUIRE=true`
                // asks for sqlx's acquire ping back (docs/architecture.md §196).
                skilj::default_pool_options()
                    .max_connections(20)
                    .test_before_acquire(
                        std::env::var("SKILJ_BENCH_TEST_BEFORE_ACQUIRE").as_deref() == Ok("true"),
                    ),
            )
            .bounded_context(bc_name.clone())
            .event_type::<Deposited>()
            .command_type::<Deposit>()
            .command_type::<DepositFast>()
            .snapshot::<Balance>()
            .reconciliation_role(subject.clone())
            .build()
            .await
            .unwrap()
            .0
    };
    let skilj = build().await;
    let mut others = Vec::new();
    for _ in 1..instances.max(1) {
        let other = build().await;
        let router = other.rest_router();
        others.push((other, router));
    }

    let mut credentials = Vec::new();
    for name in ["Deposit", "DepositFast"] {
        let command_type = db::get_command_type(pool, &bc_name, name)
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
        credentials.push(format!("{}.{}", token.id, token.secret));
    }

    Setup {
        router: skilj.rest_router(),
        skilj,
        others,
        fast_credential: credentials.pop().unwrap(),
        credential: credentials.pop().unwrap(),
    }
}

/// Submits one deposit and returns its latency.
#[allow(dead_code)] // coresident_pgbench.rs only
pub async fn deposit(setup: &Setup, account_id: &str) -> Duration {
    deposit_with(setup, &setup.credential, account_id).await
}

/// Submits one deposit with `credential`'s command and returns its
/// latency.
#[allow(dead_code)] // coresident_pgbench.rs only, through `deposit`
pub async fn deposit_with(setup: &Setup, credential: &str, account_id: &str) -> Duration {
    deposit_through(&setup.router, credential, account_id).await
}

/// [`deposit_with`] through any router.
pub async fn deposit_through(
    router: &axum::Router,
    credential: &str,
    account_id: &str,
) -> Duration {
    let body = serde_json::json!({ "payload": { "account_id": account_id, "amount": 1 } });
    let request = Request::builder()
        .method("POST")
        .uri("/v1/commands/trigger")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let started = Instant::now();
    let response = router.clone().oneshot(request).await.unwrap();
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

/// A TCP proxy in front of `database_url`'s server that delays every
/// chunk by half of `round_trip` in each direction, so a request and its
/// answer take `round_trip` longer, as across a network
/// (docs/architecture.md §196). Delays are scheduled per chunk, not
/// served one after another, so a stream of chunks keeps its throughput
/// and only gains latency. Plain threads and `std::thread::sleep`, not
/// tokio: tokio's timer ticks in whole milliseconds, which turned a 0.5 ms
/// delay into 1-2 ms and a "1 ms" round trip into about 3.5. Returns
/// `database_url` pointed at the proxy.
#[allow(dead_code)] // command_throughput.rs only
pub async fn latency_proxy(database_url: &str, round_trip: Duration) -> String {
    let (prefix, rest) = database_url
        .split_once('@')
        .expect("a DATABASE_URL with user@host");
    let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let upstream = authority.to_string();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let local = listener.local_addr().unwrap();
    let one_way = round_trip / 2;
    std::thread::spawn(move || {
        for client in listener.incoming() {
            let Ok(client) = client else {
                return;
            };
            let Ok(server) = std::net::TcpStream::connect(&upstream) else {
                continue;
            };
            let _ = client.set_nodelay(true);
            let _ = server.set_nodelay(true);
            let (client_out, server_out) =
                (client.try_clone().unwrap(), server.try_clone().unwrap());
            delayed_copy(client, server_out, one_way);
            delayed_copy(server, client_out, one_way);
        }
    });
    format!("{prefix}@{local}{path}")
}

/// Copies `from` to `to` on two threads of its own, each chunk `delay`
/// after it was read.
fn delayed_copy(mut from: std::net::TcpStream, mut to: std::net::TcpStream, delay: Duration) {
    use std::io::{Read, Write};
    let (sender, receiver) = std::sync::mpsc::channel::<(Instant, Vec<u8>)>();
    std::thread::spawn(move || {
        while let Ok((due, chunk)) = receiver.recv() {
            let now = Instant::now();
            if due > now {
                std::thread::sleep(due - now);
            }
            if to.write_all(&chunk).is_err() {
                return;
            }
        }
        let _ = to.shutdown(std::net::Shutdown::Write);
    });
    std::thread::spawn(move || {
        let mut buffer = vec![0u8; 64 * 1024];
        loop {
            match from.read(&mut buffer) {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if sender
                        .send((Instant::now() + delay, buffer[..n].to_vec()))
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    });
}
