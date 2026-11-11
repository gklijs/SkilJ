//! Acceptance tests for tasks/add-command.md, copied into the project's
//! `tests/` by check.sh. They name what the task names, nothing more.

use skilj::{CommandType, EventType};
use skilj_core::shared::{EventSpec, TagMapping};
use skilj_test_fixture::command::GivenEvents;
use template_check::wallet::{
    CloseWallet, CloseWalletPayload, Deposit, DepositPayload, DepositedPayload, WalletClosed,
    WalletClosedPayload, WalletEvent, Withdraw, WithdrawPayload, WithdrawnPayload,
};

fn deposited(amount: i64) -> WalletEvent {
    WalletEvent::Deposited(DepositedPayload {
        wallet_id: "a".into(),
        amount,
    })
}

fn withdrawn(amount: i64) -> WalletEvent {
    WalletEvent::Withdrawn(WithdrawnPayload {
        wallet_id: "a".into(),
        amount,
    })
}

fn closed() -> WalletEvent {
    WalletEvent::WalletClosed(WalletClosedPayload {
        wallet_id: "a".into(),
    })
}

fn close() -> CloseWalletPayload {
    CloseWalletPayload {
        wallet_id: "a".into(),
    }
}

fn wallet_closed_event() -> Vec<EventSpec> {
    vec![EventSpec {
        event_type: "WalletClosed".into(),
        payload: serde_json::json!({ "wallet_id": "a" }),
    }]
}

#[test]
fn a_wallet_that_never_had_a_deposit_can_be_closed() {
    GivenEvents::<CloseWallet>::new()
        .when(close())
        .then_accepted(wallet_closed_event());
}

#[test]
fn an_emptied_wallet_can_be_closed() {
    GivenEvents::<CloseWallet>::new()
        .events([deposited(100), withdrawn(100)])
        .when(close())
        .then_accepted(wallet_closed_event());
}

#[test]
fn a_wallet_holding_money_cannot_be_closed() {
    GivenEvents::<CloseWallet>::new()
        .events([deposited(100), withdrawn(40)])
        .when(close())
        .then_rejected("wallet_not_empty");
}

#[test]
fn a_closed_wallet_refuses_closing_deposits_and_withdrawals() {
    GivenEvents::<CloseWallet>::new()
        .event(closed())
        .when(close())
        .then_rejected("wallet_closed");
    GivenEvents::<Deposit>::new()
        .events([deposited(100), withdrawn(100), closed()])
        .when(DepositPayload {
            wallet_id: "a".into(),
            amount: 1,
        })
        .then_rejected("wallet_closed");
    GivenEvents::<Withdraw>::new()
        .events([deposited(100), withdrawn(100), closed()])
        .when(WithdrawPayload {
            wallet_id: "a".into(),
            amount: 1,
        })
        .then_rejected("wallet_closed");
}

/// The command finds the wallet's events, and the wallet's commands find
/// the closing, only when both carry the wallet's tag.
#[test]
fn the_command_and_its_event_are_tagged_with_the_wallet() {
    let wallet = TagMapping::new("wallet", "wallet_id");
    assert!(CloseWallet::tag_mappings().contains(&wallet));
    assert!(WalletClosed::tag_mappings().contains(&wallet));
    assert_eq!(<CloseWallet as CommandType>::NAME, "CloseWallet");
    assert_eq!(<WalletClosed as EventType>::NAME, "WalletClosed");
    assert!(CloseWallet::rest_trigger_allowed());
}
