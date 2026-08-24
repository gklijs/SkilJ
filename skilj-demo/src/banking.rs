//! The "easy" half of this demo (see the module doc comment in `lib.rs`):
//! withdrawing money only ever needs the one account's own history. Every
//! event and command here tags on `account` alone, keyed by the payload's
//! own `account_id` - two different accounts' commands never share a tag,
//! so `decide()` for account `"a"` structurally never sees anything that
//! happened to account `"b"`, and their commits never contend for the
//! same `submit_command` retry either. No coordination between account
//! aggregates is needed because none is ever attempted.
//!
//! There's no separate "open account" step: the first `DepositMoney` for
//! a given `account_id` is what brings it into existence: `decide()`
//! folds `matching_events` (empty, the first time) to find the current
//! balance, so an unopened account reads as balance zero.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Projection, SkiljBuilder};
use skilj_core::event_store::Event;
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{CommandDecision, EventSpec, TagMapping};

pub const BOUNDED_CONTEXT: &str = "banking";

fn account_tag() -> Vec<TagMapping> {
    vec![TagMapping {
        key: "account".into(),
        field: "account_id".into(),
    }]
}

// --- events ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MoneyDepositedPayload {
    pub account_id: String,
    pub amount: i64,
}

pub struct MoneyDeposited;

impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
    fn tag_mappings() -> Vec<TagMapping> {
        account_tag()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MoneyWithdrawnPayload {
    pub account_id: String,
    pub amount: i64,
}

pub struct MoneyWithdrawn;

impl EventType for MoneyWithdrawn {
    type Payload = MoneyWithdrawnPayload;
    const NAME: &'static str = "MoneyWithdrawn";
    fn tag_mappings() -> Vec<TagMapping> {
        account_tag()
    }
}

/// This bounded context's own hand-written event enum - see
/// docs/architecture.md §1.4/§1.6. `matching_events` handed to every
/// `decide()` below is always scoped to one `account_id`'s own tag, so
/// folding it never has to check which account an event belongs to.
pub enum BankingEvent {
    MoneyDeposited(MoneyDepositedPayload),
    MoneyWithdrawn(MoneyWithdrawnPayload),
}

impl BoundedContextEvent for BankingEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "MoneyDeposited" => {
                Some(serde_json::from_str(&event.payload).map(BankingEvent::MoneyDeposited))
            }
            "MoneyWithdrawn" => {
                Some(serde_json::from_str(&event.payload).map(BankingEvent::MoneyWithdrawn))
            }
            _ => None,
        }
    }
}

fn balance_of(matching_events: &[BankingEvent]) -> i64 {
    matching_events.iter().fold(0i64, |balance, event| match event {
        BankingEvent::MoneyDeposited(p) => balance + p.amount,
        BankingEvent::MoneyWithdrawn(p) => balance - p.amount,
    })
}

// --- commands ---

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DepositMoneyPayload {
    pub account_id: String,
    pub amount: i64,
}

pub struct DepositMoney;

impl CommandType for DepositMoney {
    type Payload = DepositMoneyPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "DepositMoney";
    fn tag_mappings() -> Vec<TagMapping> {
        account_tag()
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
                event_type: "MoneyDeposited".into(),
                payload: serde_json::json!({
                    "account_id": payload.account_id,
                    "amount": payload.amount,
                }),
            }],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WithdrawMoneyPayload {
    pub account_id: String,
    pub amount: i64,
}

pub struct WithdrawMoney;

impl CommandType for WithdrawMoney {
    type Payload = WithdrawMoneyPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "WithdrawMoney";
    fn tag_mappings() -> Vec<TagMapping> {
        account_tag()
    }
    fn rest_trigger_allowed() -> bool {
        true
    }
    /// `matching_events` is already every `MoneyDeposited`/`MoneyWithdrawn`
    /// this account has ever had, and nothing from any other account -
    /// the whole point of tagging both the command and the events on
    /// `account_id` alone.
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
                    "account {} has balance {balance}, cannot withdraw {}",
                    payload.account_id, payload.amount
                ),
                kind: "insufficient_funds".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "MoneyWithdrawn".into(),
                payload: serde_json::json!({
                    "account_id": payload.account_id,
                    "amount": payload.amount,
                }),
            }],
        }
    }
}

// --- projection ---

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct AccountBalanceState {
    pub balance: i64,
}

/// Keyed by `account_id` - one instance per account, per `Projection::keys`'
/// own doc comment. `sync: true` so `GET`ting it back (`db::
/// get_projection_state` in this crate's own tests, or `ProjectionQuery`
/// over GraphQL in a real caller) always reflects the command that was
/// just triggered, no polling delay.
pub struct AccountBalance;

impl Projection for AccountBalance {
    type State = AccountBalanceState;
    type Event = BankingEvent;
    const NAME: &'static str = "AccountBalance";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["MoneyDeposited", "MoneyWithdrawn"]
    }
    fn sync() -> bool {
        true
    }
    fn keys(event: &Self::Event) -> Vec<String> {
        match event {
            BankingEvent::MoneyDeposited(p) => vec![p.account_id.clone()],
            BankingEvent::MoneyWithdrawn(p) => vec![p.account_id.clone()],
        }
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            BankingEvent::MoneyDeposited(p) => state.balance += p.amount,
            BankingEvent::MoneyWithdrawn(p) => state.balance -= p.amount,
        }
    }
}

/// Registers this bounded context's event/command/projection types onto
/// `builder`, scoped by `.bounded_context(BOUNDED_CONTEXT)` - see
/// `lib.rs`'s own `register()`.
pub fn register(builder: SkiljBuilder) -> SkiljBuilder {
    builder
        .bounded_context(BOUNDED_CONTEXT)
        .event_type::<MoneyDeposited>()
        .event_type::<MoneyWithdrawn>()
        .command_type::<DepositMoney>()
        .command_type::<WithdrawMoney>()
        .projection::<AccountBalance>()
}
