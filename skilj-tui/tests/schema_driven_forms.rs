//! End-to-end (within-process) tests for Codeberg issue #8: `App`'s
//! Commands picker/form flow and Query Events' checklist, driven purely
//! through `App::handle` the same way `main.rs`'s own loop would -
//! `AppEvent::Term` for real keypresses, and the two new
//! `CommandTypesResult`/`QueryEventsTypesResult` variants standing in
//! for what a real `commandTypes`/`eventTypes` fetch would deliver
//! (`graphql.rs`'s own `Client::request` already has its own coverage
//! in `tests/graphql_client.rs` - this file's job is what `App` does
//! with a parsed result, not the wire fetch itself). No real server
//! needed: switching tabs does spawn a real (bogus-endpoint, harmless -
//! its `Err` result is simply never consumed) fetch task in the
//! background, exactly as it would against a real server, but every
//! assertion here drives state via a synthetic result instead of
//! waiting on it.

use crossterm::event::{Event as TermEvent, KeyCode, KeyEvent, KeyModifiers};
use serde_json::json;
use skilj_tui::app::{App, AppEvent, CommandsStage};
use skilj_tui::graphql::Client;
use std::sync::Arc;
use tokio::sync::mpsc;

fn key(code: KeyCode) -> AppEvent {
    AppEvent::Term(TermEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

fn new_app() -> App {
    let client = Arc::new(Client::new(
        "http://127.0.0.1:1/graphql".parse().unwrap(),
        "test-token".to_string(),
    ));
    let (tx, _rx) = mpsc::unbounded_channel();
    App::new(client, "banking".to_string(), tx)
}

const DEPOSIT_SCHEMA: &str = r#"{
    "type": "object",
    "required": ["account_id", "amount"],
    "properties": {
        "account_id": { "type": "string" },
        "amount": { "type": "integer" }
    }
}"#;

#[tokio::test]
async fn picking_a_command_type_generates_a_form_from_its_own_schema() {
    let mut app = new_app();

    // Switching to the Commands tab kicks off a real (bogus-endpoint)
    // fetch in the background - never awaited or consumed here, the
    // point is what App does once a result (real or, as here,
    // synthetic) actually arrives.
    app.handle(key(KeyCode::Char('3')));
    match &app.commands.stage {
        CommandsStage::Picking { loading, .. } => {
            assert!(*loading, "entering the tab should start a fetch")
        }
        CommandsStage::Form { .. } => panic!("must start in Picking, not Form"),
    }

    app.handle(AppEvent::CommandTypesResult(Ok(json!({
        "commandTypes": [{ "name": "DepositMoney", "schema": DEPOSIT_SCHEMA }]
    }))));
    match &app.commands.stage {
        CommandsStage::Picking { types, loading, .. } => {
            assert!(!*loading);
            assert_eq!(types.len(), 1);
            assert_eq!(types[0].name, "DepositMoney");
        }
        CommandsStage::Form { .. } => panic!("a type list result must not itself change stage"),
    }

    app.handle(key(KeyCode::Enter));
    let CommandsStage::Form {
        type_name,
        fields,
        focus,
        ..
    } = &app.commands.stage
    else {
        panic!("Enter on a picked row must transition to Form");
    };
    assert_eq!(type_name, "DepositMoney");
    assert_eq!(*focus, 0);
    let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, ["account_id", "amount"]);
}

#[tokio::test]
async fn esc_returns_from_the_form_to_the_picker_without_a_refetch() {
    let mut app = new_app();
    app.handle(key(KeyCode::Char('3')));
    app.handle(AppEvent::CommandTypesResult(Ok(json!({
        "commandTypes": [{ "name": "DepositMoney", "schema": DEPOSIT_SCHEMA }]
    }))));
    app.handle(key(KeyCode::Enter));
    assert!(matches!(app.commands.stage, CommandsStage::Form { .. }));

    app.handle(key(KeyCode::Esc));
    match &app.commands.stage {
        CommandsStage::Picking { types, loading, .. } => {
            assert_eq!(
                types.len(),
                1,
                "the previously fetched list survives Esc, no refetch"
            );
            assert!(!*loading);
        }
        CommandsStage::Form { .. } => panic!("Esc must return to Picking"),
    }
    assert!(
        !app.should_quit,
        "Esc inside the form must not quit the app"
    );
}

#[tokio::test]
async fn esc_quits_when_there_is_no_form_to_back_out_of() {
    let mut app = new_app();
    app.handle(key(KeyCode::Esc));
    assert!(app.should_quit);
}

#[tokio::test]
async fn typing_into_a_generated_field_and_submitting_assembles_the_right_payload() {
    let mut app = new_app();
    app.handle(key(KeyCode::Char('3')));
    app.handle(AppEvent::CommandTypesResult(Ok(json!({
        "commandTypes": [{ "name": "DepositMoney", "schema": DEPOSIT_SCHEMA }]
    }))));
    app.handle(key(KeyCode::Enter));

    // account_id is field 0 (alphabetical `properties` order - see
    // form.rs's own doc comment on why).
    for c in "acc-1".chars() {
        app.handle(key(KeyCode::Char(c)));
    }
    app.handle(key(KeyCode::Tab));
    for c in "42".chars() {
        app.handle(key(KeyCode::Char(c)));
    }

    let CommandsStage::Form {
        fields, loading, ..
    } = &app.commands.stage
    else {
        panic!("still in Form");
    };
    let payload = skilj_tui::form::assemble_payload(fields).unwrap();
    assert_eq!(payload, json!({ "account_id": "acc-1", "amount": 42 }));
    assert!(!*loading, "typing must not itself submit");

    app.handle(key(KeyCode::Enter));
    let CommandsStage::Form { loading, .. } = &app.commands.stage else {
        panic!("still in Form");
    };
    assert!(
        *loading,
        "Enter must submit (spawns the real mutation task)"
    );
}

#[tokio::test]
async fn a_boolean_field_toggles_on_space_instead_of_accepting_typed_text() {
    let mut app = new_app();
    app.handle(key(KeyCode::Char('3')));
    let schema = json!({
        "type": "object",
        "properties": { "urgent": { "type": "boolean" } }
    })
    .to_string();
    app.handle(AppEvent::CommandTypesResult(Ok(json!({
        "commandTypes": [{ "name": "FlagAccount", "schema": schema }]
    }))));
    app.handle(key(KeyCode::Enter));

    app.handle(key(KeyCode::Char(' ')));
    let CommandsStage::Form { fields, .. } = &app.commands.stage else {
        panic!("still in Form")
    };
    assert!(matches!(
        fields[0].widget,
        skilj_tui::form::Widget::Bool(true)
    ));

    app.handle(key(KeyCode::Char(' ')));
    let CommandsStage::Form { fields, .. } = &app.commands.stage else {
        panic!("still in Form")
    };
    assert!(matches!(
        fields[0].widget,
        skilj_tui::form::Widget::Bool(false)
    ));
}

#[tokio::test]
async fn query_events_checklist_toggles_and_runs_with_only_checked_types() {
    let mut app = new_app();
    app.handle(key(KeyCode::Char('2')));
    assert!(
        app.query_events.types_loading,
        "entering the tab should start a fetch"
    );

    app.handle(AppEvent::QueryEventsTypesResult(Ok(json!({
        "eventTypes": [
            { "name": "MoneyDeposited", "schema": "{}" },
            { "name": "MoneyWithdrawn", "schema": "{}" }
        ]
    }))));
    assert_eq!(app.query_events.types.len(), 2);
    assert!(
        app.query_events.checked.is_empty(),
        "nothing checked until the operator toggles one"
    );

    // Nothing checked yet - Enter must not run, and must say why.
    app.handle(key(KeyCode::Enter));
    assert!(!app.query_events.query_loading);
    assert!(app.query_events.query_error.is_some());

    app.handle(key(KeyCode::Char(' ')));
    assert!(app.query_events.checked.contains(&0));
    app.handle(key(KeyCode::Down));
    app.handle(key(KeyCode::Char(' ')));
    assert!(app.query_events.checked.contains(&1));

    app.handle(key(KeyCode::Enter));
    assert!(
        app.query_events.query_loading,
        "Enter with something checked must run the query"
    );
    assert!(app.query_events.query_error.is_none());
}
