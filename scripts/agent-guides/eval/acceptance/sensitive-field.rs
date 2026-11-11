//! Acceptance tests for tasks/sensitive-field.md, copied into the
//! project's `tests/` by check.sh.

use skilj::{CommandType, EventType};
use skilj_core::shared::{EventSpec, SensitiveField, TagMapping};
use skilj_test_fixture::command::GivenEvents;
use template_check::wallet::{
    DepositedPayload, OwnerRegistered, OwnerRegisteredPayload, RegisterOwner,
    RegisterOwnerPayload, WalletEvent, Withdraw, WithdrawPayload,
};

fn register(owner_id: &str, email: &str) -> RegisterOwnerPayload {
    RegisterOwnerPayload {
        wallet_id: "a".into(),
        owner_id: owner_id.into(),
        email: email.into(),
    }
}

fn registered(owner_id: &str, email: &str) -> WalletEvent {
    WalletEvent::OwnerRegistered(OwnerRegisteredPayload {
        wallet_id: "a".into(),
        owner_id: owner_id.into(),
        email: email.into(),
    })
}

#[test]
fn a_wallet_without_an_owner_gets_one() {
    GivenEvents::<RegisterOwner>::new()
        .when(register("o-1", "ada@example.com"))
        .then_accepted(vec![EventSpec {
            event_type: "OwnerRegistered".into(),
            payload: serde_json::json!({
                "wallet_id": "a",
                "owner_id": "o-1",
                "email": "ada@example.com",
            }),
        }]);
}

#[test]
fn a_wallet_with_an_owner_refuses_another() {
    GivenEvents::<RegisterOwner>::new()
        .event(registered("o-1", "ada@example.com"))
        .when(register("o-2", "grace@example.com"))
        .then_rejected("owner_already_registered");
}

#[test]
fn an_owner_does_not_change_the_balance() {
    GivenEvents::<Withdraw>::new()
        .events([
            WalletEvent::Deposited(DepositedPayload {
                wallet_id: "a".into(),
                amount: 100,
            }),
            registered("o-1", "ada@example.com"),
        ])
        .when(WithdrawPayload {
            wallet_id: "a".into(),
            amount: 100,
        })
        .then_accepted(vec![EventSpec {
            event_type: "Withdrawn".into(),
            payload: serde_json::json!({ "wallet_id": "a", "amount": 100 }),
        }]);
}

/// Encrypted under the owner's key, so forgetting the owner erases it:
/// the email is the sensitive field and `owner_id` names whose it is. On
/// the command as well: its payload is stored too.
fn assert_email_is_the_owners(sensitive: Vec<SensitiveField>, what: &str) {
    assert_eq!(sensitive.len(), 1, "{what}: one sensitive field, got {sensitive:?}");
    assert_eq!(sensitive[0].field, "email", "{what}");
    assert_eq!(sensitive[0].subject_field, "owner_id", "{what}");
    assert!(!sensitive[0].subject_key.is_empty(), "{what}");
}

#[test]
fn the_email_is_encrypted_under_the_owners_key_on_the_event_and_the_command() {
    assert_email_is_the_owners(OwnerRegistered::sensitive_fields(), "OwnerRegistered");
    assert_email_is_the_owners(RegisterOwner::sensitive_fields(), "RegisterOwner");
}

/// A tag is stored and served in the clear.
#[test]
fn the_email_is_not_a_tag() {
    for mapping in OwnerRegistered::tag_mappings().into_iter().chain(RegisterOwner::tag_mappings()) {
        assert_ne!(mapping.field, "email", "{mapping:?}");
    }
}

#[test]
fn the_command_and_its_event_are_tagged_with_the_wallet() {
    let wallet = TagMapping::new("wallet", "wallet_id");
    assert!(RegisterOwner::tag_mappings().contains(&wallet));
    assert!(OwnerRegistered::tag_mappings().contains(&wallet));
    assert_eq!(<RegisterOwner as CommandType>::NAME, "RegisterOwner");
    assert_eq!(<OwnerRegistered as EventType>::NAME, "OwnerRegistered");
    assert!(RegisterOwner::rest_trigger_allowed());
}
