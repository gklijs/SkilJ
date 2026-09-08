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
//! **Codeberg issue #5's narrower cut ([docs/architecture.md §17](../../docs/architecture.md#event-command-codegen-real))**: the
//! event/command *shape* half of this file - the payload structs, the
//! `EventType`/`CommandType` impls' declarative fields, and the shared
//! `BankingEvent` enum + its `BoundedContextEvent` impl - is generated
//! at build time from `banking.skilj.toml` (`build.rs`), not
//! hand-written here. What stays hand-written, below, is exactly the
//! real logic: `decide_deposit_money`/`decide_withdraw_money` (the
//! generated `CommandType::decide()` methods each delegate to one of
//! these by name - see `skilj-codegen`'s own doc comment for the naming
//! convention), the shared `apply_money_event`/`balance_of` helpers, and
//! the `AccountBalance` projection in full (`Projection` generation is
//! out of scope for this pass - see [§17](../../docs/architecture.md#event-command-codegen-real) and [§16](../../docs/architecture.md#declarative-bounded-context-codegen-prototype)'s own Finding 3 for
//! why). `courses.rs` has no `.skilj.toml` counterpart and stays fully
//! hand-written - its own point is real, non-generatable `decide()`
//! logic, so converting it would prove nothing this file doesn't
//! already prove.
//!
//! **[§19](../../docs/architecture.md#optional-snapshotting-matching-events)'s own real adopter**: `AccountBalanceSnapshot`/`WithdrawMoneyFast`
//! near the bottom of this file are deliberately *not* part of the
//! codegen'd shape above - `skilj-codegen` doesn't generate `snapshot()`/
//! `decide_from_snapshot()` (out of scope for this pass, see
//! [docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)'s own "Files to touch"), so proving
//! snapshotting end-to-end needed one small, genuinely hand-written
//! `CommandType` instead of retrofitting the generated `WithdrawMoney`.
//! Same real business logic as `decide_withdraw_money` above, on
//! purpose - see that pair's own doc comment for why sharing
//! `apply_money_event` keeps them from drifting apart.

use serde::{Deserialize, Serialize};
use skilj_core::shared::{CommandDecision, EventSpec};

include!(concat!(env!("OUT_DIR"), "/banking_generated.rs"));

/// One event's own effect on a running balance - `balance_of`'s per-step
/// fold, and `AccountBalanceSnapshot::fold`'s identical one below ([§19](../../docs/architecture.md#optional-snapshotting-matching-events)),
/// factored out so the two can never drift apart the way two
/// independently hand-maintained copies of the same match could.
fn apply_money_event(balance: i64, event: &BankingEvent) -> i64 {
    match event {
        BankingEvent::MoneyDeposited(p) => balance + p.amount,
        BankingEvent::MoneyWithdrawn(p) => balance - p.amount,
    }
}

fn balance_of(matching_events: &[BankingEvent]) -> i64 {
    matching_events.iter().fold(0i64, apply_money_event)
}

fn decide_deposit_money(
    payload: &DepositMoneyPayload,
    _matching_events: &[BankingEvent],
) -> CommandDecision {
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
fn decide_withdraw_money(
    payload: &WithdrawMoneyPayload,
    matching_events: &[BankingEvent],
) -> CommandDecision {
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

// --- snapshot (docs/architecture.md §19) --- see this file's own doc
// comment for why this pair is hand-written rather than codegen'd.

#[derive(Debug, Default, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AccountBalanceSnapshotState {
    pub balance: i64,
}

/// Scoped to `account` alone - `WithdrawMoney`/`DepositMoney`'s own
/// single tag, the exact shape `Snapshot` requires (`crate::plugin::Snapshot`'s
/// own doc comment). `fold` is `apply_money_event`, the identical
/// per-event step `balance_of` already uses above.
pub struct AccountBalanceSnapshot;

#[skilj::auto_register(BOUNDED_CONTEXT)]
impl skilj::Snapshot for AccountBalanceSnapshot {
    type State = AccountBalanceSnapshotState;
    type Event = BankingEvent;
    const NAME: &'static str = "AccountBalanceSnapshot";
    const TAG_KEY: &'static str = "account";
    const VERSION: u64 = 1;
    fn fold(state: &mut Self::State, event: &Self::Event) {
        state.balance = apply_money_event(state.balance, event);
    }
}

/// `WithdrawMoney`'s own real behaviour (see `decide_withdraw_money`
/// above), reachable through the snapshot-accelerated path instead -
/// two separate `CommandType`s, not the same one wearing two hats,
/// since `snapshot()`/`decide_from_snapshot()` aren't part of
/// `banking.skilj.toml`'s own generated shape (this file's own module
/// doc comment). A real consuming app with a genuinely large per-account
/// history would give its *actual* `WithdrawMoney` this treatment
/// directly; this demo keeps both side by side so the ordinary path
/// (`WithdrawMoney`) and the snapshot-accelerated one
/// (`WithdrawMoneyFast`) are each independently provable against the
/// identical real events.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WithdrawMoneyFastPayload {
    pub account_id: String,
    pub amount: i64,
}

pub struct WithdrawMoneyFast;

#[skilj::auto_register(BOUNDED_CONTEXT)]
impl skilj::CommandType for WithdrawMoneyFast {
    type Payload = WithdrawMoneyFastPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "WithdrawMoneyFast";
    fn tag_mappings() -> Vec<skilj_core::shared::TagMapping> {
        vec![skilj_core::shared::TagMapping {
            key: "account".to_string(),
            field: "account_id".to_string(),
        }]
    }
    fn rest_trigger_allowed() -> bool {
        true
    }
    fn snapshot() -> Option<&'static str> {
        Some("AccountBalanceSnapshot")
    }

    /// Never actually reached once `snapshot()` is `Some` - the
    /// framework always prefers `decide_from_snapshot` for an opted-in
    /// command type (`CommandType::snapshot()`'s own doc comment) - but
    /// still required, since `decide()` has no default. Delegates to
    /// the identical `withdraw_decision` both paths share, so they can
    /// never disagree.
    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        withdraw_decision(payload, balance_of(matching_events))
    }

    fn decide_from_snapshot(
        payload: &Self::Payload,
        snapshot_state_json: &str,
        events_since_snapshot: &[Self::Event],
    ) -> CommandDecision {
        let snapshot: AccountBalanceSnapshotState =
            serde_json::from_str(snapshot_state_json).unwrap_or_default();
        let balance = events_since_snapshot
            .iter()
            .fold(snapshot.balance, apply_money_event);
        withdraw_decision(payload, balance)
    }
}

/// The one real rule both `WithdrawMoneyFast::decide`/`decide_from_snapshot`
/// share - identical to `decide_withdraw_money`'s own body above, just
/// taking an already-computed `balance` instead of `matching_events`
/// directly, since the two callers arrive at that balance two different
/// ways (a full replay vs. a folded snapshot plus a short tail).
fn withdraw_decision(payload: &WithdrawMoneyFastPayload, balance: i64) -> CommandDecision {
    if payload.amount <= 0 {
        return CommandDecision::Rejected {
            reason: "withdrawal amount must be positive".into(),
            kind: "invalid_amount".into(),
        };
    }
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
