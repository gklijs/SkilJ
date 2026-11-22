//! Acceptance tests for tasks/unique-email.md, copied into the project's
//! `tests/` by check.sh.

use skilj::{normalize, BlindErasure, BlindTag, CommandType, EventType};
use skilj_core::shared::{EventSpec, SensitiveField, TagMapping};
use skilj_test_fixture::command::GivenEvents;
use template_check::wallet::{
    OwnerEmail, OwnerRegistered, OwnerRegisteredPayload, RegisterOwner, RegisterOwnerPayload,
    WalletEvent,
};

fn register(wallet_id: &str, owner_id: &str, email: &str) -> RegisterOwnerPayload {
    RegisterOwnerPayload {
        wallet_id: wallet_id.into(),
        owner_id: owner_id.into(),
        email: email.into(),
    }
}

fn registered(wallet_id: &str, owner_id: &str, email: &str) -> WalletEvent {
    WalletEvent::OwnerRegistered(OwnerRegisteredPayload {
        wallet_id: wallet_id.into(),
        owner_id: owner_id.into(),
        email: email.into(),
    })
}

#[test]
fn an_address_nobody_registered_is_accepted() {
    GivenEvents::<RegisterOwner>::new()
        .when(register("w1", "o-1", "ada@example.com"))
        .then_accepted(vec![EventSpec {
            event_type: "OwnerRegistered".into(),
            payload: serde_json::json!({
                "wallet_id": "w1",
                "owner_id": "o-1",
                "email": "ada@example.com",
            }),
        }]);
}

/// The blind tag selected the earlier registration of the same address;
/// decide() rejects on its being there, in whatever wallet.
#[test]
fn an_address_registered_before_is_taken() {
    GivenEvents::<RegisterOwner>::new()
        .event(registered("w1", "o-1", "ada@example.com"))
        .when(register("w2", "o-2", "ada@example.com"))
        .then_rejected("email_taken");
}

fn assert_is_the_rule<B: BlindTag>() {
    assert_eq!(B::normalize("Ada@Example.com"), B::normalize("ada@example.com"));
    assert_eq!(B::normalize(" Ada@Example.com "), normalize::email(" Ada@Example.com "));
    assert_ne!(B::normalize("ada@example.com"), B::normalize("grace@example.com"));
}

#[test]
fn addresses_are_compared_as_email_addresses() {
    assert_is_the_rule::<OwnerEmail>();
}

fn the_email_field(what: &str, sensitive: Vec<SensitiveField>) -> SensitiveField {
    let email: Vec<_> = sensitive.into_iter().filter(|field| field.field == "email").collect();
    assert_eq!(email.len(), 1, "{what}: email is a sensitive field: {email:?}");
    let email = email.into_iter().next().unwrap();
    assert_eq!(email.subject_field, "owner_id", "{what}: encrypted under the owner's key");
    email
}

/// Forgetting the owner removes the tag, so the address is free again;
/// erasure finds the owner's records by a plain tag on owner_id.
#[test]
fn forgetting_the_owner_frees_the_address() {
    let erasure = <OwnerEmail as BlindTag>::ERASURE;
    for (what, tags, sensitive) in [
        ("OwnerRegistered", OwnerRegistered::tag_mappings(), OwnerRegistered::sensitive_fields()),
        ("RegisterOwner", RegisterOwner::tag_mappings(), RegisterOwner::sensitive_fields()),
    ] {
        let email = the_email_field(what, sensitive);
        let BlindErasure::WithSubject { subject_key, subject_tag } = erasure else {
            panic!("OwnerEmail keeps the hash after the owner is forgotten: {erasure:?}");
        };
        assert_eq!(subject_key, email.subject_key, "{what}: erased with the owner the email is encrypted for");
        assert!(
            tags.contains(&TagMapping::new(subject_tag, "owner_id")),
            "{what}: a plain tag {subject_tag} on owner_id, for erasure: {tags:?}"
        );
    }
}

#[test]
fn the_email_is_a_blind_tag_and_never_a_plain_one() {
    for (what, tags) in [
        ("OwnerRegistered", OwnerRegistered::tag_mappings()),
        ("RegisterOwner", RegisterOwner::tag_mappings()),
    ] {
        assert!(
            tags.contains(&TagMapping::blind::<OwnerEmail>("email")),
            "{what}: email mapped blind under OwnerEmail: {tags:?}"
        );
        for mapping in &tags {
            assert!(mapping.field != "email" || mapping.blind, "{what}: {mapping:?} stores the address in the clear");
        }
        assert!(tags.contains(&TagMapping::new("wallet", "wallet_id")), "{what}: {tags:?}");
    }
    assert_eq!(<RegisterOwner as CommandType>::NAME, "RegisterOwner");
    assert_eq!(<OwnerRegistered as EventType>::NAME, "OwnerRegistered");
    assert!(RegisterOwner::rest_trigger_allowed());
}
