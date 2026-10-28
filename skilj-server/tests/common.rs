#![allow(dead_code)]
//! Test harness: a fresh database per test, a banking context, a bootstrapped
//! superadmin and an admin role with a grant.

use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use skilj_core::crypto::LocalMasterKey;
use skilj_core::decl::*;
use skilj_core::model::*;
use skilj_core::Skilj;
use sqlx::postgres::PgPoolOptions;
use sqlx::Executor;

pub fn server_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432".to_string())
}

/// Create a fresh database and return a pool on it.
pub async fn fresh_db(name: &str) -> sqlx::PgPool {
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&format!("{}/postgres", server_url()))
        .await
        .expect("connect to postgres");
    let db = format!("skilj_t_{name}");
    admin
        .execute(format!("drop database if exists {db} with (force)").as_str())
        .await
        .unwrap();
    admin
        .execute(format!("create database {db}").as_str())
        .await
        .unwrap();
    PgPoolOptions::new()
        .max_connections(20)
        .connect(&format!("{}/{db}", server_url()))
        .await
        .unwrap()
}

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

pub struct Env {
    pub skilj: Skilj,
    pub superadmin: Role,
    pub admin_role: Role,
    pub admin: Grant,
}

pub async fn start_with(name: &str, decls: Vec<BoundedContextDecl>, contexts: &[&str]) -> Env {
    let pool = fresh_db(name).await;
    let secret = Arc::new(Mutex::new(String::new()));
    let s2 = secret.clone();
    let mut b = Skilj::builder()
        .pool(pool.clone())
        .master_key(Arc::new(LocalMasterKey::new("m1", [9u8; 32])))
        .blind_index_root_key(b"blind-root".to_vec())
        .without_background_tasks()
        .on_bootstrap_secret(move |s| *s2.lock().unwrap() = s.to_string());
    for d in &decls {
        b = b.context(d.clone());
    }
    let skilj = b.start().await.expect("start");
    let secret = secret.lock().unwrap().clone();
    let superadmin = skilj
        .create_superadmin(&secret, "root", "root@idp")
        .await
        .unwrap();
    let admin_role = skilj
        .create_role(&superadmin, "ops", false, "ops@idp")
        .await
        .unwrap();
    for c in contexts {
        skilj.add_bounded_context(&superadmin, c).await.unwrap();
        skilj
            .grant_access(
                &superadmin,
                &admin_role.id,
                c,
                AccessLevel::Admin,
                false,
                None,
            )
            .await
            .unwrap();
    }
    let report = skilj.reconcile("ops@idp").await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let admin = skilj.resolve_grant("ops@idp", contexts[0]).await.unwrap();
    Env {
        skilj,
        superadmin,
        admin_role,
        admin,
    }
}

pub async fn start(name: &str) -> Env {
    start_with(name, vec![bank_decl()], &["bank"]).await
}

impl Env {
    /// A role with a grant on bank.
    pub async fn role(
        &self,
        subject: &str,
        level: AccessLevel,
        sensitive: bool,
        scope: Option<&str>,
    ) -> Grant {
        let r = self
            .skilj
            .create_role(&self.superadmin, subject, false, subject)
            .await
            .unwrap();
        self.skilj
            .grant_access(&self.superadmin, &r.id, "bank", level, sensitive, scope)
            .await
            .unwrap();
        self.skilj.resolve_grant(subject, "bank").await.unwrap()
    }
}

pub fn open(id: &str, company: &str) -> Value {
    json!({"account_id": id, "owner_id": format!("user-{id}"), "company": company, "email": format!("{id}@x.nl"), "note": "secret note"})
}
