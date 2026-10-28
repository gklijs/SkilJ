//! The demo domain: a bank with accounts, deposits and withdrawals.

use serde_json::json;
use skilj_core::decl::*;
use skilj_core::model::*;

pub fn bank_decl() -> BoundedContextDecl {
    let opened = EventTypeDecl::new(
        "AccountOpened",
        json!({"type":"object","properties":{
            "account_id":{"type":"string"},"owner_id":{"type":"string"},
            "company":{"type":"string"},"email":{"type":"string"},
            "note":{"type":"string"},"advisor_id":{"type":"string"}
        },"required":["account_id","owner_id","company"]}),
    )
    .tag("account", "account_id")
    .tag("company", "company")
    .owner("company")
    .sensitive("email", "user", "owner_id")
    .private(PrivateField {
        field: "note".into(),
        kind: PrivateFieldKind::Own,
        capability: None,
        addressee_field: None,
    })
    .readable()
    .external()
    .direct();
    let deposited = EventTypeDecl::new(
        "MoneyDeposited",
        json!({"type":"object","properties":{"account_id":{"type":"string"},"amount":{"type":"integer"},"company":{"type":"string"}},"required":["account_id","amount"]}),
    )
    .tag("account", "account_id")
    .tag("company", "company")
    .owner("company")
    .readable()
    .external();
    let withdrawn = EventTypeDecl::new(
        "MoneyWithdrawn",
        json!({"type":"object","properties":{"account_id":{"type":"string"},"amount":{"type":"integer"},"company":{"type":"string"}},"required":["account_id","amount"]}),
    )
    .tag("account", "account_id")
    .tag("company", "company")
    .owner("company")
    .readable();

    let open = CommandTypeDecl::new(
        "OpenAccount",
        json!({"type":"object","properties":{
            "account_id":{"type":"string"},"owner_id":{"type":"string"},"company":{"type":"string"},
            "email":{"type":"string"},"note":{"type":"string"}
        },"required":["account_id","owner_id","company"]}),
        |input| {
            if input.events.iter().any(|e| e.event_type == "AccountOpened") {
                return Decision::reject("account_exists", "an account with this id exists");
            }
            Decision::accept(vec![EventSpec::new("AccountOpened", input.payload.clone())])
        },
    )
    .tag("account", "account_id")
    .tag("company", "company")
    .owner("company")
    .sensitive("email", "user", "owner_id")
    .query_item(item(&["AccountOpened"], &["account"], Some(1)))
    .rest_trigger();

    let deposit = CommandTypeDecl::new(
        "Deposit",
        json!({"type":"object","properties":{"account_id":{"type":"string"},"amount":{"type":"integer"},"company":{"type":"string"}},"required":["account_id","amount"]}),
        |input| {
            let amount = input.payload["amount"].as_i64().unwrap_or(0);
            if amount <= 0 {
                return Decision::reject("invalid_amount", "amount must be positive");
            }
            if !input.events.iter().any(|e| e.event_type == "AccountOpened") {
                return Decision::reject("no_account", "no such account");
            }
            Decision::accept(vec![EventSpec::new("MoneyDeposited", input.payload.clone())])
        },
    )
    .tag("account", "account_id")
    .tag("company", "company")
    .owner("company")
    .rest_trigger();

    let withdraw = CommandTypeDecl::new(
        "Withdraw",
        json!({"type":"object","properties":{"account_id":{"type":"string"},"amount":{"type":"integer"},"company":{"type":"string"}},"required":["account_id","amount"]}),
        |input| {
            let account = input.payload["account_id"].as_str().unwrap_or_default();
            let balance = input
                .snapshot("balance", account)
                .and_then(|v| v["balance"].as_i64())
                .unwrap_or(0);
            let amount = input.payload["amount"].as_i64().unwrap_or(0);
            if amount > balance {
                return Decision::reject("insufficient_balance", format!("balance {balance} < {amount}"));
            }
            Decision::accept(vec![EventSpec::new("MoneyWithdrawn", input.payload.clone())])
        },
    )
    .tag("account", "account_id")
    .tag("company", "company")
    .owner("company")
    .query_item(snapshot_item("balance", "account"));

    let balance_summary = SummaryDecl::new(
        "balance",
        "account",
        1,
        json!({"balance": 0}),
        |state, e, _| {
            let b = state["balance"].as_i64().unwrap_or(0);
            let a = e.payload["amount"].as_i64().unwrap_or(0);
            match e.event_type.as_str() {
                "MoneyDeposited" => json!({"balance": b + a}),
                "MoneyWithdrawn" => json!({"balance": b - a}),
                _ => state.clone(),
            }
        },
    );

    let balances = ProjectionDecl::new(
        "Balance",
        json!({"type":"object","properties":{"balance":{"type":"integer"},"email":{"type":"string"}}}),
        &["AccountOpened", "MoneyDeposited", "MoneyWithdrawn"],
        |e| vec![e.payload["account_id"].as_str().unwrap_or_default().to_string()],
        |_, state, e| {
            let mut s = state.cloned().unwrap_or(json!({"balance": 0}));
            let a = e.payload["amount"].as_i64().unwrap_or(0);
            match e.event_type.as_str() {
                "AccountOpened" => s["email"] = e.payload["email"].clone(),
                "MoneyDeposited" => s["balance"] = json!(s["balance"].as_i64().unwrap_or(0) + a),
                "MoneyWithdrawn" => s["balance"] = json!(s["balance"].as_i64().unwrap_or(0) - a),
                _ => {}
            }
            Some(s)
        },
    )
    .owner("company");

    let count = ProjectionDecl::new(
        "AccountCount",
        json!({"type":"object","properties":{"count":{"type":"integer"}}}),
        &["AccountOpened"],
        |_| vec![String::new()],
        |_, state, _| {
            Some(json!({"count": state.and_then(|s| s["count"].as_i64()).unwrap_or(0) + 1}))
        },
    )
    .sync();

    BoundedContextDecl::new("bank")
        .event(opened)
        .event(deposited)
        .event(withdrawn)
        .command(open)
        .command(deposit)
        .command(withdraw)
        .summary(balance_summary)
        .projection(balances)
        .projection(count)
}
