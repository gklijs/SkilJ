//! Acceptance tests for tasks/temporal-activity.md, copied into the
//! project's `tests/` by check.sh. With --run, check.sh starts Temporal in
//! Docker and checks that an opened wallet starts its workflow, and that a
//! wallet opened while the server ran without Temporal doesn't.

use skilj::{CommandType, EventType};
use skilj_core::shared::{EventSpec, TagMapping};
use skilj_temporal::MappingAction;
use skilj_test_fixture::command::GivenEvents;
use template_check::onboarding::onboarding_mapping;
use template_check::wallet::{
    DepositedPayload, OpenWallet, OpenWalletPayload, WalletEvent, WalletOpened,
    WalletOpenedPayload, Withdraw, WithdrawPayload,
};

fn open(wallet_id: &str) -> OpenWalletPayload {
    OpenWalletPayload {
        wallet_id: wallet_id.into(),
        owner_id: "o-1".into(),
    }
}

fn opened(wallet_id: &str) -> WalletEvent {
    WalletEvent::WalletOpened(WalletOpenedPayload {
        wallet_id: wallet_id.into(),
        owner_id: "o-1".into(),
    })
}

#[test]
fn a_wallet_is_opened() {
    GivenEvents::<OpenWallet>::new()
        .when(open("w1"))
        .then_accepted(vec![EventSpec {
            event_type: "WalletOpened".into(),
            payload: serde_json::json!({ "wallet_id": "w1", "owner_id": "o-1" }),
        }]);
}

/// So a wallet starts one workflow: a second Start would be refused by
/// Temporal and taken for success by the bridge.
#[test]
fn an_open_wallet_is_not_opened_again() {
    GivenEvents::<OpenWallet>::new()
        .event(opened("w1"))
        .when(open("w1"))
        .then_rejected("wallet_already_open");
}

#[test]
fn opening_leaves_the_balance_alone() {
    GivenEvents::<Withdraw>::new()
        .events([
            opened("w1"),
            WalletEvent::Deposited(DepositedPayload {
                wallet_id: "w1".into(),
                amount: 100,
            }),
        ])
        .when(WithdrawPayload {
            wallet_id: "w1".into(),
            amount: 100,
        })
        .then_accepted(vec![EventSpec {
            event_type: "Withdrawn".into(),
            payload: serde_json::json!({ "wallet_id": "w1", "amount": 100 }),
        }]);
}

#[test]
fn wallet_opened_starts_wallet_onboarding_correlated_by_the_wallet() {
    let mapping = onboarding_mapping("token-id.token-secret".to_string());
    assert_eq!(mapping.event_type, "WalletOpened");
    assert_eq!(mapping.credential, "token-id.token-secret");
    assert_eq!(mapping.correlation_tag_key, "wallet");
    match mapping.action {
        MappingAction::Start {
            workflow_type,
            task_queue,
        } => {
            assert_eq!(workflow_type, "WalletOnboarding");
            assert_eq!(task_queue, "wallet-onboarding");
        }
        _ => panic!("WalletOpened starts a workflow, it doesn't signal one"),
    }
}

/// The bridge reads it over REST with an event read token, and finds the
/// wallet by its tag.
#[test]
fn wallet_opened_is_readable_and_tagged_with_the_wallet() {
    assert!(WalletOpened::event_read_allowed());
    let wallet = TagMapping::new("wallet", "wallet_id");
    assert!(WalletOpened::tag_mappings().contains(&wallet));
    assert!(OpenWallet::tag_mappings().contains(&wallet));
    assert_eq!(<OpenWallet as CommandType>::NAME, "OpenWallet");
    assert_eq!(<WalletOpened as EventType>::NAME, "WalletOpened");
    assert!(OpenWallet::rest_trigger_allowed());
}
