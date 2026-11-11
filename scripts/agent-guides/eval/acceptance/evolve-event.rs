//! Acceptance tests for tasks/evolve-event.md, copied into the project's
//! `tests/` by check.sh.

use skilj::CommandType;
use skilj_core::event_store::schema_is_backwards_compatible;
use skilj_core::shared::CommandDecision;
use template_check::wallet::{Deposit, DepositPayload, DepositedPayload};

/// The schemas the template registers, before the change: what the
/// existing database holds.
const STORED_DEPOSITED: &str = r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"DepositedPayload","type":"object","properties":{"amount":{"type":"integer","format":"int64"},"wallet_id":{"type":"string"}},"required":["wallet_id","amount"]}"#;
const STORED_DEPOSIT: &str = r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","title":"DepositPayload","type":"object","properties":{"amount":{"type":"integer","format":"int64"},"wallet_id":{"type":"string"}},"required":["wallet_id","amount"]}"#;

fn schema<T: schemars::JsonSchema>() -> String {
    serde_json::to_string(&schemars::schema_for!(T)).unwrap()
}

/// What startup checks before it re-registers a type: a schema the stored
/// one can't evolve into fails the start with `SchemaIncompatible`.
#[test]
fn the_new_schemas_are_revisions_of_the_stored_ones() {
    assert!(schema_is_backwards_compatible(STORED_DEPOSITED, &schema::<DepositedPayload>()));
    assert!(schema_is_backwards_compatible(STORED_DEPOSIT, &schema::<DepositPayload>()));
}

#[test]
fn a_deposit_stored_without_a_reference_still_reads() {
    let stored = serde_json::json!({ "wallet_id": "a", "amount": 100 });
    let payload: DepositedPayload = serde_json::from_value(stored).unwrap();
    assert_eq!(payload.reference, None);
}

#[test]
fn a_client_that_sends_no_reference_still_deposits() {
    let sent = serde_json::json!({ "wallet_id": "a", "amount": 100 });
    let payload: DepositPayload = serde_json::from_value(sent).unwrap();
    let CommandDecision::Accepted { events } = Deposit::decide(&payload, &[]) else {
        panic!("a deposit without a reference was rejected");
    };
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "Deposited");
    let reference = events[0].payload.get("reference");
    assert!(reference.is_none_or(serde_json::Value::is_null), "got {reference:?}");
    // What is emitted has to read back as the event's payload.
    let recorded: DepositedPayload = serde_json::from_value(events[0].payload.clone()).unwrap();
    assert_eq!(recorded.reference, None);
}

#[test]
fn a_reference_is_recorded_on_the_event() {
    let sent = serde_json::json!({ "wallet_id": "a", "amount": 100, "reference": "rent" });
    let payload: DepositPayload = serde_json::from_value(sent).unwrap();
    let CommandDecision::Accepted { events } = Deposit::decide(&payload, &[]) else {
        panic!("a deposit with a reference was rejected");
    };
    assert_eq!(events[0].payload["reference"], "rent");
    assert_eq!(events[0].payload["wallet_id"], "a");
    assert_eq!(events[0].payload["amount"], 100);
}
