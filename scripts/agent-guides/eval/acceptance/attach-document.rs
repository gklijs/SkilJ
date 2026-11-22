//! Acceptance tests for tasks/attach-document.md, copied into the
//! project's `tests/` by check.sh.

use skilj::{CommandType, EventType};
use skilj_core::blob::{BlobField, BlobRef};
use skilj_core::shared::{EventSpec, TagMapping};
use skilj_test_fixture::command::GivenEvents;
use template_check::wallet::{
    AttachStatement, AttachStatementPayload, DepositedPayload, StatementAttached,
    StatementAttachedPayload, WalletEvent, Withdraw, WithdrawPayload,
};

fn pdf() -> BlobRef {
    BlobRef {
        id: "b-1".into(),
        sha256: "0f".repeat(32),
        size: 2048,
        media_type: "application/pdf".into(),
    }
}

fn attach(month: &str) -> AttachStatementPayload {
    AttachStatementPayload {
        wallet_id: "w1".into(),
        owner_id: "o-1".into(),
        month: month.into(),
        document: pdf(),
    }
}

fn attached(month: &str) -> WalletEvent {
    WalletEvent::StatementAttached(StatementAttachedPayload {
        wallet_id: "w1".into(),
        owner_id: "o-1".into(),
        month: month.into(),
        document: pdf(),
    })
}

#[test]
fn a_statement_is_attached_with_its_reference() {
    GivenEvents::<AttachStatement>::new()
        .when(attach("2026-10"))
        .then_accepted(vec![EventSpec {
            event_type: "StatementAttached".into(),
            payload: serde_json::json!({
                "wallet_id": "w1",
                "owner_id": "o-1",
                "month": "2026-10",
                "document": pdf(),
            }),
        }]);
}

#[test]
fn one_statement_per_month() {
    GivenEvents::<AttachStatement>::new()
        .event(attached("2026-10"))
        .when(attach("2026-10"))
        .then_rejected("statement_already_attached");
}

#[test]
fn a_statement_does_not_change_the_balance() {
    GivenEvents::<Withdraw>::new()
        .events([
            WalletEvent::Deposited(DepositedPayload {
                wallet_id: "w1".into(),
                amount: 100,
            }),
            attached("2026-10"),
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

/// A blob, encrypted under the owner's key (subject `owner`), on the
/// command too: its payload is stored as well. Not a sensitive field: a
/// blob field never is (invalid_blob_field).
#[test]
fn the_document_is_a_blob_of_the_owner_on_the_event_and_the_command() {
    let expected = vec![BlobField::with_subject("document", "owner", "owner_id")];
    assert_eq!(StatementAttached::blob_fields(), expected);
    assert_eq!(AttachStatement::blob_fields(), expected);
    for field in StatementAttached::sensitive_fields().into_iter().chain(AttachStatement::sensitive_fields()) {
        assert_ne!(field.field, "document", "{field:?}");
    }
}

#[test]
fn the_command_and_its_event_are_tagged_with_the_wallet() {
    let wallet = TagMapping::new("wallet", "wallet_id");
    assert!(AttachStatement::tag_mappings().contains(&wallet));
    assert!(StatementAttached::tag_mappings().contains(&wallet));
    assert_eq!(<AttachStatement as CommandType>::NAME, "AttachStatement");
    assert_eq!(<StatementAttached as EventType>::NAME, "StatementAttached");
    assert!(AttachStatement::rest_trigger_allowed());
}
