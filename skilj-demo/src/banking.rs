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
//!
//! **Codeberg issue #5's narrower cut (docs/architecture.md §17)**: the
//! event/command *shape* half of this file - the payload structs, the
//! `EventType`/`CommandType` impls' declarative fields, and the shared
//! `BankingEvent` enum + its `BoundedContextEvent` impl - is generated
//! at build time from `banking.skilj.toml` (`build.rs`), not
//! hand-written here. What stays hand-written, below, is exactly the
//! real logic: `decide_deposit_money`/`decide_withdraw_money` (the
//! generated `CommandType::decide()` methods each delegate to one of
//! these by name - see `skilj-codegen`'s own doc comment for the naming
//! convention), the shared `balance_of` helper, and the `AccountBalance`
//! projection in full (`Projection` generation is out of scope for this
//! pass - see §17 and §16's own Finding 3 for why). `courses.rs` has no
//! `.skilj.toml` counterpart and stays fully hand-written - its own
//! point is real, non-generatable `decide()` logic, so converting it
//! would prove nothing this file doesn't already prove.

use serde::{Deserialize, Serialize};
use skilj_core::shared::{CommandDecision, EventSpec};

include!(concat!(env!("OUT_DIR"), "/banking_generated.rs"));

fn balance_of(matching_events: &[BankingEvent]) -> i64 {
    matching_events
        .iter()
        .fold(0i64, |balance, event| match event {
            BankingEvent::MoneyDeposited(p) => balance + p.amount,
            BankingEvent::MoneyWithdrawn(p) => balance - p.amount,
        })
}

fn decide_deposit_money(payload: &DepositMoneyPayload, _matching_events: &[BankingEvent]) -> CommandDecision {
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

/// `matching_events` is already every `MoneyDeposited`/`MoneyWithdrawn`
/// this account has ever had, and nothing from any other account - the
/// whole point of tagging both the command and the events on
/// `account_id` alone.
fn decide_withdraw_money(payload: &WithdrawMoneyPayload, matching_events: &[BankingEvent]) -> CommandDecision {
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

// --- projection --- (untouched - Projection generation is out of scope
// for this pass, see this file's own doc comment)

#[derive(Debug, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AccountBalanceState {
    pub balance: i64,
}

/// Keyed by `account_id` - one instance per account, per `Projection::keys`'
/// own doc comment. `sync: true` so `GET`ting it back (`db::
/// get_projection_state` in this crate's own tests, or `ProjectionQuery`
/// over GraphQL in a real caller) always reflects the command that was
/// just triggered, no polling delay.
pub struct AccountBalance;

#[skilj::auto_register(BOUNDED_CONTEXT)]
impl skilj::Projection for AccountBalance {
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
