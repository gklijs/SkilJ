//! Tests for `plugin::upcast_payload`/`UpcastStep` (docs/architecture.md
//! [§33](../../docs/architecture.md#payload-upcasting)) - sugar around the version-branch pattern a hand-written
//! `BoundedContextEvent::try_from_event` already had every ingredient
//! for by hand (`event.metadata.version` plus the raw JSON payload), not
//! a new registration/storage/schema surface, so unlike `type_registration.rs`'s
//! neighbouring `schema_is_backwards_compatible` tests there is no
//! `RegisterEventType`/`RegisterCommandType` obligation this belongs to
//! - purely a pure-function unit-test file, no Postgres involved.

use serde::Deserialize;
use serde_json::json;
use skilj_core::plugin::{upcast_payload, UpcastStep};

#[derive(Debug, Deserialize, PartialEq)]
struct MoneyDepositedV2 {
    amount_cents: i64,
}

/// The scenario [docs/architecture.md §33](../../docs/architecture.md#payload-upcasting) uses throughout: a float-dollar
/// `amount` (schema_version 1) renamed and retyped to an integer-cent
/// `amount_cents` (schema_version 2) - a genuine reshape
/// `schema_is_backwards_compatible` would reject as a revision of the
/// same type.
fn money_deposited_upcasts() -> Vec<UpcastStep> {
    vec![UpcastStep {
        to_version: 2,
        transform: |mut v| {
            if let Some(amount) = v.get("amount").and_then(|a| a.as_f64()) {
                if let Some(obj) = v.as_object_mut() {
                    obj.remove("amount");
                    obj.insert(
                        "amount_cents".into(),
                        ((amount * 100.0).round() as i64).into(),
                    );
                }
            }
            v
        },
    }]
}

#[test]
fn a_payload_written_before_the_steps_to_version_gets_the_step_applied() {
    let payload = json!({ "amount": 12.5 }).to_string();
    let result: MoneyDepositedV2 = upcast_payload(&payload, 1, &money_deposited_upcasts()).unwrap();
    assert_eq!(result, MoneyDepositedV2 { amount_cents: 1250 });
}

#[test]
fn a_payload_already_at_the_steps_to_version_skips_the_transform() {
    // Written at version 2 - already in the post-transform shape, so the
    // step must not run (running it again would find no "amount" field
    // and silently produce amount_cents: 0, not the same bug the missing
    // branch would be, but still wrong).
    let payload = json!({ "amount_cents": 1250 }).to_string();
    let result: MoneyDepositedV2 = upcast_payload(&payload, 2, &money_deposited_upcasts()).unwrap();
    assert_eq!(result, MoneyDepositedV2 { amount_cents: 1250 });
}

#[test]
fn a_payload_written_after_every_step_skips_the_whole_chain() {
    let payload = json!({ "amount_cents": 999 }).to_string();
    let result: MoneyDepositedV2 = upcast_payload(&payload, 5, &money_deposited_upcasts()).unwrap();
    assert_eq!(result, MoneyDepositedV2 { amount_cents: 999 });
}

#[test]
fn an_empty_chain_is_a_plain_deserialize() {
    let payload = json!({ "amount_cents": 42 }).to_string();
    let result: MoneyDepositedV2 = upcast_payload(&payload, 1, &[]).unwrap();
    assert_eq!(result, MoneyDepositedV2 { amount_cents: 42 });
}

#[test]
fn malformed_json_fails_before_any_transform_runs() {
    let result: Result<MoneyDepositedV2, _> =
        upcast_payload("not json", 1, &money_deposited_upcasts());
    assert!(result.is_err());
}

#[test]
fn a_transform_that_still_leaves_the_target_shape_unsatisfied_surfaces_as_a_deserialize_error() {
    // "amount" absent entirely - the transform's own guard leaves the
    // value untouched, so there is still no amount_cents field for T.
    let payload = json!({ "unrelated": true }).to_string();
    let result: Result<MoneyDepositedV2, _> =
        upcast_payload(&payload, 1, &money_deposited_upcasts());
    assert!(result.is_err());
}

/// Multi-step chain, applied in order: version 1 needs both steps
/// (amount -> amount_cents, then amount_cents -> amount_in_minor_units),
/// version 2 needs only the second, version 3 needs neither.
#[derive(Debug, Deserialize, PartialEq)]
struct MoneyDepositedV3 {
    amount_in_minor_units: i64,
}

fn chained_upcasts() -> Vec<UpcastStep> {
    vec![
        UpcastStep {
            to_version: 2,
            transform: |mut v| {
                if let Some(amount) = v.get("amount").and_then(|a| a.as_f64()) {
                    if let Some(obj) = v.as_object_mut() {
                        obj.remove("amount");
                        obj.insert(
                            "amount_cents".into(),
                            ((amount * 100.0).round() as i64).into(),
                        );
                    }
                }
                v
            },
        },
        UpcastStep {
            to_version: 3,
            transform: |mut v| {
                if let Some(cents) = v.get("amount_cents").and_then(|a| a.as_i64()) {
                    if let Some(obj) = v.as_object_mut() {
                        obj.remove("amount_cents");
                        obj.insert("amount_in_minor_units".into(), cents.into());
                    }
                }
                v
            },
        },
    ]
}

#[test]
fn a_payload_at_the_oldest_version_runs_every_step_in_order() {
    let payload = json!({ "amount": 3.0 }).to_string();
    let result: MoneyDepositedV3 = upcast_payload(&payload, 1, &chained_upcasts()).unwrap();
    assert_eq!(
        result,
        MoneyDepositedV3 {
            amount_in_minor_units: 300
        }
    );
}

#[test]
fn a_payload_at_an_intermediate_version_runs_only_the_remaining_steps() {
    let payload = json!({ "amount_cents": 300 }).to_string();
    let result: MoneyDepositedV3 = upcast_payload(&payload, 2, &chained_upcasts()).unwrap();
    assert_eq!(
        result,
        MoneyDepositedV3 {
            amount_in_minor_units: 300
        }
    );
}

#[test]
fn a_payload_at_the_latest_version_runs_no_steps() {
    let payload = json!({ "amount_in_minor_units": 300 }).to_string();
    let result: MoneyDepositedV3 = upcast_payload(&payload, 3, &chained_upcasts()).unwrap();
    assert_eq!(
        result,
        MoneyDepositedV3 {
            amount_in_minor_units: 300
        }
    );
}
