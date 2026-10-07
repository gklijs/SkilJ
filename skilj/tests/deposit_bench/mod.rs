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
    /// `Deposit`'s token.
    pub credential: String,
    /// `DepositFast`'s token.
    #[allow(dead_code)] // command_throughput.rs only
    pub fast_credential: String,
}

pub async fn setup(database_url: String, pool: &Pool) -> Setup {
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
        .command_type::<DepositFast>()
        .snapshot::<Balance>()
        .reconciliation_role(subject)
        .build()
        .await
        .unwrap();

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
        fast_credential: credentials.pop().unwrap(),
        credential: credentials.pop().unwrap(),
    }
}

/// Submits one deposit and returns its latency.
pub async fn deposit(setup: &Setup, account_id: &str) -> Duration {
    deposit_with(setup, &setup.credential, account_id).await
}

/// Submits one deposit with `credential`'s command and returns its
/// latency.
#[allow(dead_code)] // command_throughput.rs only
pub async fn deposit_with(setup: &Setup, credential: &str, account_id: &str) -> Duration {
    let body = serde_json::json!({ "payload": { "account_id": account_id, "amount": 1 } });
    let request = Request::builder()
        .method("POST")
        .uri("/v1/commands/trigger")
        .header("authorization", format!("Bearer {credential}"))
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
