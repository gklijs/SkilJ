//! `skilj_codegen::emhcl::import` against a real `emhcl export` (v0.17.0) of
//! `fixtures/banking.em.hcl`, skilj-demo's banking context written with the
//! naming convention of docs/event-modeling-hcl.md.
//!
//! The imported `.skilj.toml` is compared against the checked-in
//! `fixtures/banking.imported.skilj.toml`. After an intended change, re-record
//! it with `SKILJ_RECORD_EMHCL=1 cargo test -p skilj-codegen --test emhcl_import`
//! and review the diff.

use serde_json::{json, Value};

const BANKING_JSON: &str = include_str!("fixtures/banking.emhcl.json");
const EXPECTED_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/banking.imported.skilj.toml"
);

#[test]
fn imports_banking_as_the_checked_in_skilj_toml() {
    let import = skilj_codegen::emhcl::import(BANKING_JSON, "banking").unwrap();
    assert!(import.notes.is_empty(), "{:?}", import.notes);
    if std::env::var_os("SKILJ_RECORD_EMHCL").is_some() {
        std::fs::write(EXPECTED_PATH, &import.skilj_toml).unwrap();
    }
    let expected = std::fs::read_to_string(EXPECTED_PATH).unwrap();
    assert_eq!(import.skilj_toml, expected);
}

#[test]
fn the_import_generates_and_matches_the_demo_shapes() {
    let import = skilj_codegen::emhcl::import(BANKING_JSON, "banking").unwrap();
    let output = skilj_codegen::generate(&import.skilj_toml).unwrap();
    assert!(output.contains("impl ::skilj::EventType for MoneyDeposited"));
    assert!(output.contains("impl ::skilj::CommandType for WithdrawMoney"));
    assert!(output.contains(r#"TagMapping::new("account_id", "account_id")"#));

    // Same events and commands, fields, tagged fields and REST triggering as
    // the hand-written skilj-demo/src/banking.skilj.toml. Two things differ:
    // the demo names its tag `account`, where the convention keeps the field
    // name, and a model doesn't say that a decision reads no history
    // (`reads_nothing`).
    let demo: toml::Value =
        toml::from_str(include_str!("../../skilj-demo/src/banking.skilj.toml")).unwrap();
    let imported: toml::Value = toml::from_str(&import.skilj_toml).unwrap();
    assert_eq!(imported["bounded_context"], demo["bounded_context"]);
    let comparable = |v: &toml::Value, kind: &str| {
        let mut types = v[kind].as_array().unwrap().clone();
        for item in &mut types {
            let table = item.as_table_mut().unwrap();
            table.remove("reads_nothing");
            // Keep which fields are tagged, not what the tags are called.
            let mut tagged: Vec<_> = table["tags"]
                .as_table()
                .unwrap()
                .values()
                .map(|f| f.as_str().unwrap().to_string())
                .collect();
            tagged.sort();
            table.insert("tags".into(), tagged.into());
        }
        types
    };
    for kind in ["event_type", "command_type"] {
        assert_eq!(
            comparable(&imported, kind),
            comparable(&demo, kind),
            "{kind}"
        );
    }
}

#[test]
fn rejection_kinds_come_from_the_scenarios() {
    let import = skilj_codegen::emhcl::import(BANKING_JSON, "banking").unwrap();
    assert!(import
        .skilj_toml
        .contains("# Rejected as: insufficient_funds, invalid_amount\n[[command_type]]\n# From command.withdraw_money.withdraw_money"));
}

fn element(id: &str, fields: Value, deps: Value) -> Value {
    json!({ "id": id, "title": id, "type": "EVENT", "fields": fields, "dependencies": deps })
}

fn id_field(name: &str) -> Value {
    json!({ "name": name, "type": "String", "idAttribute": true })
}

#[test]
fn leaves_out_what_it_cannot_express_and_says_why() {
    let model = json!({ "slices": [{
        "id": "workflow.place_order", "title": "Place order", "sliceType": "STATE_CHANGE",
        "events": [
            element("event.orders.order_placed", json!([
                id_field("order_id"),
                { "name": "customer_email", "type": "String", "pii": true },
            ]), json!([])),
            element("event.orders.order_priced", json!([
                id_field("order_id"),
                { "name": "total", "type": "Decimal" },
                { "name": "lines", "type": "String", "cardinality": "List" },
            ]), json!([])),
            { "id": "event.payments.payment_received", "context": "EXTERNAL", "title": "x",
              "type": "EVENT", "fields": [], "dependencies": [] },
        ],
        "commands": [
            element("command.place_order.place_order", json!([id_field("order_id")]),
                json!([{ "id": "event.orders.order_placed", "type": "OUTBOUND" }])),
        ],
        "specifications": [{
            "id": "scenario.place_order.empty",
            "when": [{ "type": "SPEC_COMMAND", "title": "Place Order",
                       "linkedId": "command.place_order.place_order" }],
            "then": [{ "type": "SPEC_ERROR", "title": "The order is empty" }],
        }],
    }]});
    let import = skilj_codegen::emhcl::import(&model.to_string(), "orders").unwrap();
    let toml = &import.skilj_toml;
    assert!(toml.contains("name = \"OrderPlaced\""));
    assert!(toml.contains("name = \"PlaceOrder\""));
    assert!(!toml.contains("OrderPriced"));
    assert!(!toml.contains("PaymentReceived"));
    assert!(!toml.contains("rest_trigger_allowed"));
    let notes = import.notes.join("\n");
    assert!(
        notes.contains("customer_email` is personal data"),
        "{notes}"
    );
    assert!(notes.contains("event.orders.order_priced: left out, hand-write it: `total` has type Decimal, `lines` is a list"), "{notes}");
    assert!(
        notes.contains("doesn't follow `<kind>: <reason>`"),
        "{notes}"
    );
    skilj_codegen::generate(toml).unwrap();
}

#[test]
fn an_unknown_bounded_context_names_the_ones_there_are() {
    let err = skilj_codegen::emhcl::import(BANKING_JSON, "shipping").unwrap_err();
    assert!(
        err.to_string().contains("it has events of: banking"),
        "{err}"
    );
    let err = skilj_codegen::emhcl::import(BANKING_JSON, "Banking").unwrap_err();
    assert!(
        err.to_string().contains("not a skilj bounded context name"),
        "{err}"
    );
    let err = skilj_codegen::emhcl::import("{\"slices\": 3}", "banking").unwrap_err();
    assert!(matches!(err, skilj_codegen::Error::Json(_)));
}

#[test]
fn the_error_stays_unwind_safe() {
    fn unwind_safe<T: std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
    unwind_safe::<skilj_codegen::Error>();
}
