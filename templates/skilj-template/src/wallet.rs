//! A minimal worked bounded context: deposit/withdraw money into a
//! wallet. Every event and command here tags on `wallet` alone, keyed
//! by the payload's own `wallet_id` - two different wallets' commands
//! never share a tag, so `decide()` for wallet `"a"` structurally never
//! sees anything that happened to wallet `"b"`. Modeled directly on
//! skilj's own worked example (`skilj-demo/src/banking.rs`, in the main
//! skilj repository) - see docs/architecture.md there for the full
//! reasoning behind this shape.
//!
//! There's no separate "open wallet" step: the first `Deposit` for a
//! given `wallet_id` is what brings it into existence - `decide()`
//! folds `matching_events` (empty, the first time) to find the current
//! balance, so an unopened wallet reads as balance zero.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{auto_register, CommandType, EventType, Projection};
use skilj_core::event_store::Event;
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{CommandDecision, EventSpec, TagMapping};

pub const BOUNDED_CONTEXT: &str = "wallet";

fn wallet_tag() -> Vec<TagMapping> {
    vec![TagMapping {
        key: "wallet".into(),
        field: "wallet_id".into(),
    }]
}

// --- events ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DepositedPayload {
    pub wallet_id: String,
    pub amount: i64,
}

pub struct Deposited;

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for Deposited {
    type Payload = DepositedPayload;
    const NAME: &'static str = "Deposited";
    fn tag_mappings() -> Vec<TagMapping> {
        wallet_tag()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WithdrawnPayload {
    pub wallet_id: String,
    pub amount: i64,
}

pub struct Withdrawn;

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for Withdrawn {
    type Payload = WithdrawnPayload;
    const NAME: &'static str = "Withdrawn";
    fn tag_mappings() -> Vec<TagMapping> {
        wallet_tag()
    }
}

/// This bounded context's own hand-written event enum - every `decide()`
/// below only ever sees the events its own tags matched, so folding
/// this never has to check which wallet an event belongs to.
pub enum WalletEvent {
    Deposited(DepositedPayload),
    Withdrawn(WithdrawnPayload),
}

impl BoundedContextEvent for WalletEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "Deposited" => Some(serde_json::from_str(&event.payload).map(WalletEvent::Deposited)),
            "Withdrawn" => Some(serde_json::from_str(&event.payload).map(WalletEvent::Withdrawn)),
            _ => None,
        }
    }
}

fn balance_of(matching_events: &[WalletEvent]) -> i64 {
    matching_events.iter().fold(0i64, |balance, event| match event {
        WalletEvent::Deposited(p) => balance + p.amount,
        WalletEvent::Withdrawn(p) => balance - p.amount,
    })
}

// --- commands ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DepositPayload {
    pub wallet_id: String,
    pub amount: i64,
}

pub struct Deposit;

#[auto_register(BOUNDED_CONTEXT)]
impl CommandType for Deposit {
    type Payload = DepositPayload;
    type Event = WalletEvent;
    const NAME: &'static str = "Deposit";
    fn tag_mappings() -> Vec<TagMapping> {
        wallet_tag()
    }
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn decide(payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        if payload.amount <= 0 {
            return CommandDecision::Rejected {
                reason: "deposit amount must be positive".into(),
                kind: "invalid_amount".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "Deposited".into(),
                payload: serde_json::json!({
                    "wallet_id": payload.wallet_id,
                    "amount": payload.amount,
                }),
            }],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WithdrawPayload {
    pub wallet_id: String,
    pub amount: i64,
}

pub struct Withdraw;

#[auto_register(BOUNDED_CONTEXT)]
impl CommandType for Withdraw {
    type Payload = WithdrawPayload;
    type Event = WalletEvent;
    const NAME: &'static str = "Withdraw";
    fn tag_mappings() -> Vec<TagMapping> {
        wallet_tag()
    }
    fn rest_trigger_allowed() -> bool {
        true
    }
    /// `matching_events` is already every `Deposited`/`Withdrawn` this
    /// wallet has ever had, and nothing from any other wallet - the
    /// whole point of tagging both the command and the events on
    /// `wallet_id` alone.
    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        if payload.amount <= 0 {
            return CommandDecision::Rejected {
                reason: "withdrawal amount must be positive".into(),
                kind: "invalid_amount".into(),
            };
        }
        let balance = balance_of(matching_events);
        if payload.amount > balance {
            return CommandDecision::Rejected {
                reason: format!(
                    "wallet {} has balance {balance}, cannot withdraw {}",
                    payload.wallet_id, payload.amount
                ),
                kind: "insufficient_funds".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "Withdrawn".into(),
                payload: serde_json::json!({
                    "wallet_id": payload.wallet_id,
                    "amount": payload.amount,
                }),
            }],
        }
    }
}

// --- projection ---

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct BalanceState {
    pub balance: i64,
}

/// Keyed by `wallet_id` - one instance per wallet. `sync: true` so
/// reading it back always reflects the command that was just triggered,
/// no polling delay.
pub struct Balance;

#[auto_register(BOUNDED_CONTEXT)]
impl Projection for Balance {
    type State = BalanceState;
    type Event = WalletEvent;
    const NAME: &'static str = "Balance";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["Deposited", "Withdrawn"]
    }
    fn sync() -> bool {
        true
    }
    fn keys(event: &Self::Event) -> Vec<String> {
        match event {
            WalletEvent::Deposited(p) => vec![p.wallet_id.clone()],
            WalletEvent::Withdrawn(p) => vec![p.wallet_id.clone()],
        }
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            WalletEvent::Deposited(p) => state.balance += p.amount,
            WalletEvent::Withdrawn(p) => state.balance -= p.amount,
        }
    }
}
