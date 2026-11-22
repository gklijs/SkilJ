//! Acceptance tests for tasks/wallet-statement.md, copied into the
//! project's `tests/` by check.sh.
//!
//! The projection is driven the way skilj's writer drives it: every
//! event goes to each key `keys()` returns for it, with that key, into
//! that key's own state.

use std::collections::BTreeMap;

use skilj::Projection;
use template_check::wallet::{
    DepositedPayload, WalletEvent, WalletStatement, WalletStatementState, WithdrawnPayload,
};

fn deposited(wallet_id: &str, amount: i64) -> WalletEvent {
    WalletEvent::Deposited(DepositedPayload {
        wallet_id: wallet_id.into(),
        amount,
    })
}

fn withdrawn(wallet_id: &str, amount: i64) -> WalletEvent {
    WalletEvent::Withdrawn(WithdrawnPayload {
        wallet_id: wallet_id.into(),
        amount,
    })
}

fn wallet_of(event: &WalletEvent) -> &str {
    match event {
        WalletEvent::Deposited(p) => &p.wallet_id,
        WalletEvent::Withdrawn(p) => &p.wallet_id,
        #[allow(unreachable_patterns, reason = "the task adds no event, but a solution may")]
        _ => panic!("the history holds deposits and withdrawals only"),
    }
}

fn project(history: &[WalletEvent]) -> BTreeMap<String, WalletStatementState> {
    let mut states: BTreeMap<String, WalletStatementState> = BTreeMap::new();
    for event in history {
        let keys = WalletStatement::keys(event);
        assert_eq!(keys, vec![wallet_of(event).to_string()], "keys() of an event is its wallet");
        for key in keys {
            let state = states.entry(key.clone()).or_default();
            WalletStatement::project(state, event, &key);
        }
    }
    states
}

fn lines(state: &WalletStatementState) -> Vec<(String, i64, i64)> {
    state
        .entries
        .iter()
        .map(|entry| (entry.kind.clone(), entry.amount, entry.balance))
        .collect()
}

fn line(kind: &str, amount: i64, balance: i64) -> (String, i64, i64) {
    (kind.to_string(), amount, balance)
}

#[test]
fn each_wallet_has_its_own_transactions_in_order_with_a_running_balance() {
    let states = project(&[
        deposited("w1", 100),
        deposited("w2", 30),
        withdrawn("w1", 40),
        deposited("w1", 5),
        withdrawn("w2", 30),
        deposited("w2", 7),
    ]);
    assert_eq!(states.len(), 2);
    assert_eq!(
        lines(&states["w1"]),
        vec![line("deposit", 100, 100), line("withdrawal", 40, 60), line("deposit", 5, 65)]
    );
    assert_eq!(
        lines(&states["w2"]),
        vec![line("deposit", 30, 30), line("withdrawal", 30, 0), line("deposit", 7, 7)]
    );
}

#[test]
fn a_new_statement_is_empty() {
    assert!(WalletStatementState::default().entries.is_empty());
}

#[test]
fn it_is_up_to_date_when_a_command_is_accepted() {
    assert!(WalletStatement::sync());
}

#[test]
fn it_reads_deposits_and_withdrawals() {
    let mut consumed = WalletStatement::consumed_event_types();
    consumed.sort_unstable();
    assert_eq!(consumed, vec!["Deposited", "Withdrawn"]);
    assert_eq!(<WalletStatement as Projection>::NAME, "WalletStatement");
}
