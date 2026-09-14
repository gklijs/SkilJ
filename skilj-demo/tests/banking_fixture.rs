//! `skilj-test-fixture`'s real-world adopter proof (Codeberg issue #19,
//! docs/architecture.md §45): the same `banking.rs` scenarios the real
//! Postgres-backed integration suite (`tests/banking.rs`) exercises end
//! to end, here run purely in-process against `decide()`/`project()`
//! directly - no `Skilj::builder().build()`, no embedded Postgres, no
//! HTTP round trip. Not a replacement for `tests/banking.rs` (which
//! still owns proving persistence/DCB conflict handling/access control
//! actually work) - a much faster complement for the plugin author's own
//! decision/fold logic.

use skilj_core::shared::EventSpec;
use skilj_demo::banking::{
    AccountBalance, AccountBalanceState, BankingEvent, DepositMoney, DepositMoneyPayload,
    MoneyDepositedPayload, MoneyWithdrawnPayload, WithdrawMoney, WithdrawMoneyPayload,
};
use skilj_test_fixture::command::GivenEvents as GivenCommandEvents;
use skilj_test_fixture::projection::GivenEvents as GivenProjectionEvents;

#[test]
fn deposit_of_a_positive_amount_is_accepted() {
    GivenCommandEvents::<DepositMoney>::new()
        .when(DepositMoneyPayload {
            account_id: "a".into(),
            amount: 100,
        })
        .then_accepted(vec![EventSpec {
            event_type: "MoneyDeposited".into(),
            payload: serde_json::json!({ "account_id": "a", "amount": 100 }),
        }]);
}

#[test]
fn deposit_of_a_non_positive_amount_is_rejected() {
    GivenCommandEvents::<DepositMoney>::new()
        .when(DepositMoneyPayload {
            account_id: "a".into(),
            amount: 0,
        })
        .then_rejected("invalid_amount");
}

#[test]
fn withdrawal_within_balance_is_accepted() {
    GivenCommandEvents::<WithdrawMoney>::new()
        .event(BankingEvent::MoneyDeposited(MoneyDepositedPayload {
            account_id: "a".into(),
            amount: 100,
        }))
        .when(WithdrawMoneyPayload {
            account_id: "a".into(),
            amount: 60,
        })
        .then_accepted(vec![EventSpec {
            event_type: "MoneyWithdrawn".into(),
            payload: serde_json::json!({ "account_id": "a", "amount": 60 }),
        }]);
}

#[test]
fn withdrawal_beyond_balance_is_rejected() {
    // Only account "a"'s own history is given - the exact `matching_events`
    // scoping `decide_withdraw_money`'s own doc comment describes; this
    // fixture doesn't reconstruct that scoping itself (see the crate root
    // doc comment), it's just given directly, by construction.
    GivenCommandEvents::<WithdrawMoney>::new()
        .event(BankingEvent::MoneyDeposited(MoneyDepositedPayload {
            account_id: "a".into(),
            amount: 100,
        }))
        .event(BankingEvent::MoneyWithdrawn(MoneyWithdrawnPayload {
            account_id: "a".into(),
            amount: 100,
        }))
        .when(WithdrawMoneyPayload {
            account_id: "a".into(),
            amount: 1,
        })
        .then_rejected("insufficient_funds");
}

#[test]
fn account_balance_projection_folds_deposits_and_withdrawals() {
    GivenProjectionEvents::<AccountBalance>::new()
        .event(BankingEvent::MoneyDeposited(MoneyDepositedPayload {
            account_id: "a".into(),
            amount: 100,
        }))
        .event(BankingEvent::MoneyWithdrawn(MoneyWithdrawnPayload {
            account_id: "a".into(),
            amount: 40,
        }))
        .then_state(AccountBalanceState { balance: 60 });
}
