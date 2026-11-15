//! `skilj_codegen::import::import` for every source, against skilj-demo's
//! banking context written in each source's format (docs/modeling-tools.md):
//!
//! - `fixtures/banking.emhcl.json`: a real `emhcl export` (see
//!   `tests/emhcl_import.rs`);
//! - `fixtures/banking.em.json`: Event Modeling Specification JSON as
//!   Nebulit's Event Modeling app writes it, with numeric ids, titles,
//!   camelCase fields and copies of events in a state view;
//! - `fixtures/banking.prooph-board.em.json`: the same format as the prooph
//!   board route's skill writes it from a board, with the REST endpoint on the
//!   command and rejections as specifications;
//! - `fixtures/banking.evml`: the `.evml` text language, which
//!   event-modeling-tools' own parser (`event-modeling-language` 1.0.0)
//!   accepts without diagnostics.
//!
//! Each import must generate, pass `skilj_codegen::validate`, and have the
//! same events and commands, fields, tagged fields, REST triggering and
//! rejection kinds as `skilj-demo/src/banking.skilj.toml` (which names its tag
//! `account` and says `reads_nothing`, two things no model records). Each is
//! also compared against its checked-in `fixtures/banking.<source>.imported.skilj.toml`;
//! after an intended change, re-record them with
//! `SKILJ_RECORD_IMPORT=1 cargo test -p skilj-codegen --test import` and
//! review the diff.

use skilj_codegen::import::{import, Source};

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

fn fixtures() -> [(Source, &'static str, &'static str); 4] {
    [
        (
            Source::Emhcl,
            "banking.emhcl.json",
            "banking.imported.skilj.toml",
        ),
        (
            Source::EmJson,
            "banking.em.json",
            "banking.em-json.imported.skilj.toml",
        ),
        (
            Source::EmJson,
            "banking.prooph-board.em.json",
            "banking.prooph-board.imported.skilj.toml",
        ),
        (
            Source::Evml,
            "banking.evml",
            "banking.evml.imported.skilj.toml",
        ),
    ]
}

fn read(name: &str) -> String {
    std::fs::read_to_string(format!("{FIXTURES}/{name}")).unwrap()
}

/// Events and commands with their fields, which fields are tagged (not what
/// the tags are called), and REST triggering.
fn comparable(v: &toml::Value, kind: &str) -> Vec<toml::Value> {
    let mut types = v[kind].as_array().unwrap().clone();
    for item in &mut types {
        let table = item.as_table_mut().unwrap();
        table.remove("reads_nothing");
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
}

#[test]
fn every_source_imports_banking_as_the_demo_has_it() {
    let demo: toml::Value =
        toml::from_str(include_str!("../../skilj-demo/src/banking.skilj.toml")).unwrap();
    for (source, input, recorded) in fixtures() {
        let imported =
            import(source, &read(input), "banking").unwrap_or_else(|e| panic!("{input}: {e}"));
        assert!(imported.notes.is_empty(), "{input}: {:?}", imported.notes);
        assert!(
            skilj_codegen::validate(&imported.skilj_toml)
                .unwrap()
                .is_empty(),
            "{input}"
        );
        let generated = skilj_codegen::generate(&imported.skilj_toml).unwrap();
        assert!(generated.contains("impl ::skilj::CommandType for WithdrawMoney"));

        let parsed: toml::Value = toml::from_str(&imported.skilj_toml).unwrap();
        assert_eq!(parsed["bounded_context"], demo["bounded_context"]);
        for kind in ["event_type", "command_type"] {
            assert_eq!(
                comparable(&parsed, kind),
                comparable(&demo, kind),
                "{input}: {kind}"
            );
        }
        assert!(
            imported
                .skilj_toml
                .contains("# Rejected as: invalid_amount\n[[command_type]]"),
            "{input}"
        );
        assert!(
            imported
                .skilj_toml
                .contains("# Rejected as: insufficient_funds, invalid_amount\n[[command_type]]"),
            "{input}"
        );

        let path = format!("{FIXTURES}/{recorded}");
        if std::env::var_os("SKILJ_RECORD_IMPORT").is_some() && source != Source::Emhcl {
            std::fs::write(&path, &imported.skilj_toml).unwrap();
        }
        assert_eq!(
            imported.skilj_toml,
            std::fs::read_to_string(&path).unwrap(),
            "{input}"
        );
    }
}

#[test]
fn em_json_names_by_titles_and_contexts_and_says_what_it_left_out() {
    let model = serde_json::json!({ "slices": [{
        "id": "1", "title": "Place order", "context": "Order Desk", "sliceType": "STATE_CHANGE",
        "commands": [{ "id": "10", "title": "Place Order", "type": "COMMAND",
            "fields": [{ "name": "orderId", "type": "UUID", "idAttribute": true }],
            "dependencies": [{ "id": "11", "type": "OUTBOUND", "title": "Order Placed", "elementType": "EVENT" }] }],
        "events": [
            { "id": "11", "title": "Order Placed", "type": "EVENT",
              "fields": [
                  { "name": "orderId", "type": "UUID", "idAttribute": true },
                  { "name": "customerEmail", "type": "String", "pii": true },
              ], "dependencies": [] },
            { "id": "12", "title": "Order Priced", "type": "EVENT",
              "fields": [{ "name": "total", "type": "Double" }, { "name": "note", "type": "String", "optional": true }],
              "dependencies": [] },
            { "id": "13", "title": "Payment Received", "type": "EVENT", "context": "EXTERNAL",
              "fields": [], "dependencies": [] },
            { "id": "14", "title": "Shipped", "type": "EVENT", "modelContext": "Shipping",
              "fields": [], "dependencies": [] },
        ],
        "readmodels": [], "screens": [], "processors": [], "tables": [],
        "specifications": [{ "id": "20", "title": "empty", "linkedId": "10", "given": [],
            "when": [{ "id": "21", "title": "Place Order", "type": "SPEC_COMMAND", "linkedId": "10" }],
            "then": [{ "id": "22", "title": "The order is empty", "type": "SPEC_ERROR" }] }],
    }]});
    let imported = import(Source::EmJson, &model.to_string(), "order_desk").unwrap();
    let toml = &imported.skilj_toml;
    assert!(toml.contains("name = \"OrderPlaced\""), "{toml}");
    assert!(
        toml.contains("{ name = \"customer_email\", type = \"string\" }"),
        "{toml}"
    );
    assert!(
        toml.contains("tags = { order_id = \"order_id\" }"),
        "{toml}"
    );
    assert!(
        toml.contains("# Rejected as: rejected\n[[command_type]]\n# From Place Order (10)"),
        "{toml}"
    );
    assert!(
        !toml.contains("OrderPriced")
            && !toml.contains("PaymentReceived")
            && !toml.contains("Shipped")
    );
    let notes = imported.notes.join("\n");
    assert!(
        notes.contains("Order Placed (11): `customer_email` is personal data"),
        "{notes}"
    );
    assert!(
        notes.contains("Order Priced (12): left out, hand-write it: `total` has type Double, `note` is optional"),
        "{notes}"
    );

    let err =
        skilj_codegen::import::import(Source::EmJson, &model.to_string(), "billing").unwrap_err();
    assert!(
        err.to_string()
            .contains("it has events of: order_desk, shipping"),
        "{err}"
    );
}

#[test]
fn evml_reads_examples_namespaces_and_inferred_relations() {
    let model = r#"eventmodeling
---
title: ignored front matter
---
title Orders
%% a comment
tf 01 ui Shop.OrderScreen
tf 02 cmd PlaceOrder { order_id: 'o-1' tag, count: 2 }
tf 03 evt Orders.OrderPlaced { order_id: 'o-1' tag, email: 'a@b.c' pii }
tf 04 cmd PriceOrder [[Price]]
rf 05 evt Billing.Invoiced
tf 06 evt orders.OrderPriced ->> 04 [[Price]]
tf 07 evt OrphanEvent

data Price `json`{
  "order_id": "o-1",
  "total": 20.5
}
"#;
    let imported = import(Source::Evml, model, "orders").unwrap();
    let toml = &imported.skilj_toml;
    assert!(toml.contains("name = \"PlaceOrder\""), "{toml}");
    assert!(
        toml.contains("{ name = \"count\", type = \"i64\" }"),
        "{toml}"
    );
    assert!(!toml.contains("rest_trigger_allowed"), "{toml}");
    let body = toml.split_once("bounded_context").unwrap().1;
    assert!(
        !body.contains("OrderPriced") && !body.contains("PriceOrder"),
        "{toml}"
    );
    let notes = imported.notes.join("\n");
    assert!(
        notes.contains("Orders.OrderPlaced (frame 03): `email` is personal data"),
        "{notes}"
    );
    assert!(
        notes.contains(
            "orders.OrderPriced (frame 06): left out, hand-write it: `total` has type Number"
        ),
        "{notes}"
    );
    assert!(notes.contains("PriceOrder (frame 04): left out"), "{notes}");

    let err = import(Source::Evml, model, "shop").unwrap_err();
    assert!(
        err.to_string()
            .contains("it has events of: (no namespace), Billing, Orders, orders"),
        "{err}"
    );
    let err = import(Source::Evml, "eventmodeling\ntf 01 widget X\n", "orders").unwrap_err();
    assert!(
        matches!(&err, skilj_codegen::Error::Evml { line: 2, .. }),
        "{err}"
    );
    let err = import(
        Source::Evml,
        "eventmodeling\ndata X {\n  a: string\n",
        "orders",
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("line 2: a data block isn't closed"),
        "{err}"
    );
}
