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
