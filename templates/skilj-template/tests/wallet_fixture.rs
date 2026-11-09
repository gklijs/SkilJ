//! Pure decision and fold tests; histories are supplied explicitly.
//! Tag selection, projection keys, persistence, authorization and concurrency
//! need integration tests against a running application.

use skilj_core::shared::EventSpec;
use skilj_test_fixture::command::GivenEvents as GivenCommandEvents;
use skilj_test_fixture::projection::GivenEvents as GivenProjectionEvents;
use {{crate_name}}::wallet::{
    Balance, BalanceState, Deposit, DepositPayload, DepositedPayload, WalletEvent, Withdraw,
    WithdrawPayload, WithdrawnPayload,
};

#[test]
fn deposit_of_a_positive_amount_is_accepted() {
    GivenCommandEvents::<Deposit>::new()
        .when(DepositPayload {
            wallet_id: "a".into(),
            amount: 100,
        })
        .then_accepted(vec![EventSpec {
            event_type: "Deposited".into(),
            payload: serde_json::json!({ "wallet_id": "a", "amount": 100 }),
        }]);
}

#[test]
fn deposit_of_a_non_positive_amount_is_rejected() {
    for amount in [0, -1] {
        GivenCommandEvents::<Deposit>::new()
            .when(DepositPayload {
                wallet_id: "a".into(),
                amount,
            })
            .then_rejected("invalid_amount");
    }
}

#[test]
fn withdrawal_within_balance_is_accepted() {
    GivenCommandEvents::<Withdraw>::new()
        .event(WalletEvent::Deposited(DepositedPayload {
            wallet_id: "a".into(),
            amount: 100,
        }))
        .when(WithdrawPayload {
            wallet_id: "a".into(),
            amount: 60,
        })
        .then_accepted(vec![EventSpec {
            event_type: "Withdrawn".into(),
            payload: serde_json::json!({ "wallet_id": "a", "amount": 60 }),
        }]);
}

#[test]
fn withdrawal_beyond_balance_is_rejected() {
    // Supply only this wallet's history; tag selection is an integration concern.
    GivenCommandEvents::<Withdraw>::new()
        .event(WalletEvent::Deposited(DepositedPayload {
            wallet_id: "a".into(),
            amount: 100,
        }))
        .event(WalletEvent::Withdrawn(WithdrawnPayload {
            wallet_id: "a".into(),
            amount: 100,
        }))
        .when(WithdrawPayload {
            wallet_id: "a".into(),
            amount: 1,
        })
        .then_rejected("insufficient_funds");
}

#[test]
fn wallet_balance_projection_folds_deposits_and_withdrawals() {
    GivenProjectionEvents::<Balance>::new()
        .event(WalletEvent::Deposited(DepositedPayload {
            wallet_id: "a".into(),
            amount: 100,
        }))
        .event(WalletEvent::Withdrawn(WithdrawnPayload {
            wallet_id: "a".into(),
            amount: 40,
        }))
        .then_state(BalanceState { balance: 60 });
}

#[test]
fn nonpositive_withdrawals_are_rejected() {
    for amount in [0, -1] {
        GivenCommandEvents::<Withdraw>::new()
            .when(WithdrawPayload {
                wallet_id: "a".into(),
                amount,
            })
            .then_rejected("invalid_amount");
    }
}

#[test]
fn unopened_wallet_cannot_be_overdrawn() {
    GivenCommandEvents::<Withdraw>::new()
        .when(WithdrawPayload {
            wallet_id: "a".into(),
            amount: 1,
        })
        .then_rejected("insufficient_funds");
}

#[test]
fn withdrawal_of_exact_balance_is_accepted() {
    GivenCommandEvents::<Withdraw>::new()
        .event(WalletEvent::Deposited(DepositedPayload {
            wallet_id: "a".into(),
            amount: 100,
        }))
        .when(WithdrawPayload {
            wallet_id: "a".into(),
            amount: 100,
        })
        .then_accepted(vec![EventSpec {
            event_type: "Withdrawn".into(),
            payload: serde_json::json!({ "wallet_id": "a", "amount": 100 }),
        }]);
}

#[test]
fn empty_wallet_balance_is_zero() {
    GivenProjectionEvents::<Balance>::new().then_state(BalanceState { balance: 0 });
}
