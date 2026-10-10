//! End-to-end (within this crate) tests for `skilj_codegen::generate` -
//! a real `.skilj.toml` fixture in, assert the emitted, `prettyplease`-
//! formatted Rust source contains the expected constructs. Real
//! compilation of the generated code is proven separately, by
//! `skilj-demo`'s own build (`skilj-demo/build.rs`/`src/banking.rs`) and
//! its existing test suite running unchanged against it - this file's
//! job is `generate`'s own output shape, in isolation.

const BANKING_TOML: &str = r#"
bounded_context = "banking"

[[event_type]]
name = "MoneyDeposited"
fields = [
    { name = "account_id", type = "string" },
    { name = "amount", type = "i64" },
]
tags = { account = "account_id" }

[[event_type]]
name = "MoneyWithdrawn"
fields = [
    { name = "account_id", type = "string" },
    { name = "amount", type = "i64" },
]
tags = { account = "account_id" }

[[command_type]]
name = "DepositMoney"
rest_trigger_allowed = true
fields = [
    { name = "account_id", type = "string" },
    { name = "amount", type = "i64" },
]
tags = { account = "account_id" }
"#;

#[test]
fn generates_the_payload_struct_with_fields_in_declared_order() {
    let output = skilj_codegen::generate(BANKING_TOML).unwrap();
    assert!(output.contains("pub struct MoneyDepositedPayload"));
    // Declared order (account_id before amount), not alphabetical -
    // `spec.rs`'s own doc comment explains why `fields` is an array,
    // not a map.
    let account_pos = output.find("pub account_id: String").unwrap();
    let amount_pos = output.find("pub amount: i64").unwrap();
    assert!(
        account_pos < amount_pos,
        "fields should stay in .skilj.toml order"
    );
}

#[test]
fn generates_a_real_event_type_impl_with_tag_mappings() {
    let output = skilj_codegen::generate(BANKING_TOML).unwrap();
    assert!(output.contains("impl ::skilj::EventType for MoneyDeposited"));
    assert!(output.contains(r#"const NAME: &'static str = "MoneyDeposited""#));
    assert!(output.contains("fn tag_mappings"));
    assert!(output.contains(r#""account".to_string()"#));
    assert!(output.contains(r#""account_id".to_string()"#));
}

#[test]
fn generates_a_command_type_impl_that_delegates_decide_to_a_hand_written_function() {
    let output = skilj_codegen::generate(BANKING_TOML).unwrap();
    assert!(output.contains("impl ::skilj::CommandType for DepositMoney"));
    assert!(output.contains("fn rest_trigger_allowed"));
    assert!(output.contains("decide_deposit_money(payload, matching_events)"));
    // The delegated-to function itself is never generated - that's the
    // whole point, it's hand-written Rust the including module supplies.
    assert!(!output.contains("fn decide_deposit_money"));
}

#[test]
fn generates_the_shared_event_enum_and_its_bounded_context_event_impl() {
    let output = skilj_codegen::generate(BANKING_TOML).unwrap();
    assert!(output.contains("pub enum BankingEvent"));
    assert!(output.contains("MoneyDeposited(MoneyDepositedPayload)"));
    assert!(output.contains("MoneyWithdrawn(MoneyWithdrawnPayload)"));
    assert!(output.contains("impl ::skilj_core::plugin::BoundedContextEvent for BankingEvent"));
    assert!(output.contains(r#""MoneyDeposited" =>"#));
    assert!(output.contains("BankingEvent::MoneyDeposited"));
}

#[test]
fn a_type_with_no_tags_gets_no_tag_mappings_override() {
    let toml = r#"
        bounded_context = "banking"
        [[event_type]]
        name = "SomethingHappened"
        fields = [ { name = "note", type = "string" } ]
    "#;
    let output = skilj_codegen::generate(toml).unwrap();
    assert!(!output.contains("fn tag_mappings"));
}

#[test]
fn an_empty_spec_still_generates_the_bounded_context_const_and_empty_enum() {
    let output = skilj_codegen::generate(r#"bounded_context = "empty""#).unwrap();
    assert!(output.contains(r#"pub const BOUNDED_CONTEXT: &str = "empty""#));
    assert!(output.contains("pub enum EmptyEvent"));
}

#[test]
fn a_malformed_toml_file_is_a_real_error_not_a_panic() {
    let result = skilj_codegen::generate("this is not valid toml {{{");
    assert!(matches!(result, Err(skilj_codegen::Error::Toml(_))));
}

/// A deferred field (`sensitive_fields`, not yet covered by this format -
/// see `spec.rs`'s own doc comment) or a typo of a covered one must be a
/// real build-time error, not a silently-ignored key that leaves a
/// user believing something was configured that never took effect.
#[test]
fn an_unknown_field_on_an_event_type_is_a_real_error_not_silently_dropped() {
    let toml = r#"
        bounded_context = "banking"
        [[event_type]]
        name = "SomethingHappened"
        fields = [ { name = "note", type = "string" } ]
        sensitive_fields = [ { field = "note", subject_key = "user", subject_field = "note" } ]
    "#;
    let result = skilj_codegen::generate(toml);
    assert!(matches!(result, Err(skilj_codegen::Error::Toml(_))));
}

#[test]
fn a_typo_d_field_name_is_a_real_error_not_silently_dropped() {
    let toml = r#"
        bounded_context = "banking"
        [[event_type]]
        name = "SomethingHappened"
        fields = [ { name = "note", type = "string" } ]
        taggs = { account = "note" }
    "#;
    let result = skilj_codegen::generate(toml);
    assert!(matches!(result, Err(skilj_codegen::Error::Toml(_))));
}

fn invalid(toml: &str) -> Vec<String> {
    match skilj_codegen::generate(toml) {
        Err(skilj_codegen::Error::Invalid(problems)) => problems,
        other => panic!("expected Error::Invalid, got {other:?}"),
    }
}

/// Inputs that used to panic inside `build.rs` (`format_ident!` on a
/// non-identifier), pass through to a rustc error inside generated code,
/// or be blamed on skilj-codegen itself - now each a plain, named
/// problem, and all of them reported at once.
#[test]
fn an_unusable_spec_is_a_list_of_named_problems_not_a_panic() {
    let problems = invalid(
        r#"
        bounded_context = "Bad-Name"
        [[event_type]]
        name = "order-placed"
        [[event_type]]
        name = "Deposited"
        fields = [
            { name = "amount", type = "i64" },
            { name = "amount", type = "string" },
            { name = "self", type = "string" },
        ]
        tags = { account = "account_id" }
        [[command_type]]
        name = "Deposited"
    "#,
    );
    let all = problems.join("\n");
    for expected in [
        "bounded_context \"Bad-Name\"",
        "event_type \"order-placed\": the name must be a Rust identifier",
        "field \"amount\" is declared twice",
        "field \"self\" must be a Rust identifier",
        "tag \"account\" maps to field \"account_id\", which this type doesn't declare",
        "command_type \"Deposited\" generates `Deposited`, which event_type \"Deposited\" already generates",
    ] {
        assert!(all.contains(expected), "missing {expected:?} in:\n{all}");
    }
}

#[test]
fn a_type_named_like_the_generated_event_enum_collides() {
    let problems = invalid(
        r#"
        bounded_context = "banking"
        [[event_type]]
        name = "BankingEvent"
    "#,
    );
    assert!(
        problems[0].contains("which the bounded context's event enum already generates"),
        "{problems:?}"
    );
}

/// A keyword field name is common in JSON (`type`) and is supported, as a
/// raw identifier - not refused, and not left to fail as generated code.
#[test]
fn a_keyword_field_is_emitted_as_a_raw_identifier() {
    let output = skilj_codegen::generate(
        r#"
        bounded_context = "shop"
        [[event_type]]
        name = "ItemAdded"
        fields = [ { name = "type", type = "string" } ]
        tags = { kind = "type" }
    "#,
    )
    .unwrap();
    assert!(output.contains("pub r#type: String"), "{output}");
    // The tag mapping names the wire field, `type`, not `r#type`.
    assert!(output.contains(r#"field : "type""#), "{output}");
}

/// What makes the raw identifier above the right choice: serde and
/// schemars both name an `r#type` field plain `type` on the wire and in
/// the JSON schema skilj registers, so payloads look as the TOML says.
#[test]
fn a_raw_identifier_field_is_plain_type_on_the_wire_and_in_the_schema() {
    #[derive(serde::Serialize, schemars::JsonSchema)]
    struct Payload {
        r#type: String,
    }
    let json = serde_json::to_value(Payload {
        r#type: "book".into(),
    })
    .unwrap();
    assert_eq!(json, serde_json::json!({ "type": "book" }));
    let schema = serde_json::to_value(schemars::schema_for!(Payload)).unwrap();
    assert!(schema["properties"].get("type").is_some(), "{schema}");
}

const HELPDESK_TOML: &str = r#"
bounded_context = "helpdesk"

[[event_type]]
name = "CompanySignedUp"
fields = [{ name = "company_id", type = "string" }]
tags = { company = "company_id" }

[[event_type]]
name = "TicketCreated"
fields = [
    { name = "company_id", type = "string" },
    { name = "ticket_id", type = "string" },
]
tags = { company = "company_id", ticket = "ticket_id" }

[[command_type]]
name = "CreateTicket"
fields = [
    { name = "company_id", type = "string" },
    { name = "ticket_id", type = "string" },
]
tags = { company = "company_id", ticket = "ticket_id" }

[[command_type.query]]
event_types = ["CompanySignedUp"]
tags = ["company"]
latest = 1

[[command_type.query]]
event_types = ["TicketCreated"]
tags = ["ticket"]
"#;

/// docs/architecture.md §198: `[[command_type.query]]` items become a
/// `consistency_query()` override, each tag looked up in the command
/// type's own `tags`.
#[test]
fn a_command_type_query_becomes_a_consistency_query_override() {
    let output = skilj_codegen::generate(HELPDESK_TOML).unwrap();
    assert!(
        output.contains("fn consistency_query() -> Vec<::skilj_core::shared::QueryItemMapping>")
    );
    assert!(output.contains(r#"event_types: Vec::from(["CompanySignedUp".to_string()])"#));
    assert!(output.contains("latest: Some(1u32)"));
    assert!(output.contains("latest: None"));
    assert!(output.contains(r#"field: "ticket_id".to_string()"#));
}

#[test]
fn a_command_type_without_a_query_gets_no_override() {
    let output = skilj_codegen::generate(BANKING_TOML).unwrap();
    assert!(!output.contains("fn consistency_query"));
}

#[test]
fn an_unusable_query_item_is_a_named_problem() {
    let problems = invalid(
        r#"
bounded_context = "helpdesk"

[[event_type]]
name = "TicketCreated"
fields = [{ name = "ticket_id", type = "string" }]

[[command_type]]
name = "CreateTicket"
fields = [{ name = "ticket_id", type = "string" }]
tags = { ticket = "ticket_id" }

[[command_type.query]]
event_types = ["CompanySignedUp"]
tags = ["company"]
latest = 0

[[command_type.query]]
"#,
    );
    let expected = [
        "query item 1 names event type \"CompanySignedUp\", which this file doesn't declare",
        "query item 1 names tag \"company\", which isn't one of the command type's tags",
        "query item 1 keeps the last 0 matches",
        "query item 2 names neither event types nor tags",
    ];
    for expected in expected {
        assert!(
            problems.iter().any(|p| p.contains(expected)),
            "missing {expected:?} in {problems:?}"
        );
    }
}
