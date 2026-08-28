//! `App`-level tests for Codeberg issue #7's own client-side pieces:
//! the Live Events `/`-gated substring filter, and the digit-tab-switch
//! fix extended to `ProjectionsTab` (a real, not just latent, gap once
//! `waitForSequence` made every one of its fields inherently numeric-
//! capable). Same "drive purely through `App::handle`" shape
//! `tests/schema_driven_forms.rs` already established for issue #8 -
//! see that file's own doc comment for why a bogus-endpoint fetch task
//! spawned in the background is harmless and never awaited here.

use crossterm::event::{Event as TermEvent, KeyCode, KeyEvent, KeyModifiers};
use serde_json::json;
use skilj_tui::app::{App, AppEvent, ProjectionField, Tab};
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

// --- Live Events filter ---

#[tokio::test]
async fn slash_enters_filter_mode_and_digits_type_into_the_filter_not_switch_tabs() {
    let mut app = new_app();
    assert_eq!(app.tab, Tab::LiveEvents);
    assert!(!app.live_events_filter_active);

    app.handle(key(KeyCode::Char('/')));
    assert!(app.live_events_filter_active);

    // The whole point: "42" must land in the filter text, not jump to
    // Query Events/Commands mid-type - the identical regression shape
    // issue #8's own digit-key fix covers for Commands.
    app.handle(key(KeyCode::Char('4')));
    app.handle(key(KeyCode::Char('2')));
    assert_eq!(app.live_events_filter.value, "42");
    assert_eq!(
        app.tab,
        Tab::LiveEvents,
        "tab must not have switched while composing the filter"
    );

    app.handle(key(KeyCode::Esc));
    assert!(
        !app.live_events_filter_active,
        "Esc exits filter-compose mode"
    );
    assert!(
        !app.should_quit,
        "Esc inside the filter must not quit the app"
    );

    // Filter-compose mode off again - digits behave normally once more.
    app.handle(key(KeyCode::Char('3')));
    assert_eq!(app.tab, Tab::Commands);
}

#[tokio::test]
async fn the_filter_text_survives_after_leaving_compose_mode_and_keeps_filtering() {
    let mut app = new_app();
    app.handle(AppEvent::LiveEvent(Ok(
        json!({"sequence": 1, "payload": "{\"amount\":42}"}),
    )));
    app.handle(AppEvent::LiveEvent(Ok(
        json!({"sequence": 2, "payload": "{\"amount\":7}"}),
    )));

    app.handle(key(KeyCode::Char('/')));
    for c in "42".chars() {
        app.handle(key(KeyCode::Char(c)));
    }
    app.handle(key(KeyCode::Enter));
    assert!(
        !app.live_events_filter_active,
        "Enter also exits filter-compose mode"
    );
    assert_eq!(
        app.live_events_filter.value, "42",
        "the typed filter text itself must survive"
    );
    assert_eq!(
        app.live_events.len(),
        2,
        "filtering is a view concern (ui.rs) - App keeps every event"
    );
}

// --- Projections: waitForSequence field + the extended digit-switch fix ---

#[tokio::test]
async fn tab_cycles_through_all_three_projection_fields() {
    let mut app = new_app();
    app.handle(key(KeyCode::Char('4')));
    assert_eq!(app.projections.focus, ProjectionField::Name);
    app.handle(key(KeyCode::Tab));
    assert_eq!(app.projections.focus, ProjectionField::Key);
    app.handle(key(KeyCode::Tab));
    assert_eq!(app.projections.focus, ProjectionField::WaitForSequence);
    app.handle(key(KeyCode::Tab));
    assert_eq!(
        app.projections.focus,
        ProjectionField::Name,
        "cycles back around"
    );
}

#[tokio::test]
async fn digits_reach_the_focused_projection_field_instead_of_switching_tabs() {
    let mut app = new_app();
    app.handle(key(KeyCode::Char('4')));
    app.handle(key(KeyCode::Tab));
    app.handle(key(KeyCode::Tab));
    assert_eq!(app.projections.focus, ProjectionField::WaitForSequence);

    for c in "142".chars() {
        app.handle(key(KeyCode::Char(c)));
    }
    assert_eq!(app.projections.wait_for_sequence.value, "142");
    assert_eq!(
        app.tab,
        Tab::Projections,
        "tab must not have switched while editing a projection field"
    );
}

/// Every field on this tab is now free text that can legitimately
/// contain a digit (`WaitForSequence` inherently so), so - unlike every
/// other tab - digits can never switch *away* from Projections; Esc is
/// the one and only way out, and it must actually work, not trap the
/// operator (a real bug this pass's own tests caught: the first draft
/// suppressed digit-switching tab-wide with no escape hatch at all).
#[tokio::test]
async fn esc_leaves_the_projections_tab_since_digits_never_can() {
    let mut app = new_app();
    app.handle(key(KeyCode::Char('4')));
    assert_eq!(app.tab, Tab::Projections);

    app.handle(key(KeyCode::Char('1')));
    assert_eq!(
        app.tab,
        Tab::Projections,
        "digits type into the focused field, never switch away"
    );
    assert_eq!(app.projections.name.value, "1");

    app.handle(key(KeyCode::Esc));
    assert_eq!(app.tab, Tab::LiveEvents);
    assert!(
        !app.should_quit,
        "Esc leaves the tab, it must not quit the app"
    );
}

#[tokio::test]
async fn an_unparseable_wait_for_sequence_blocks_submission_with_an_inline_error() {
    let mut app = new_app();
    app.handle(key(KeyCode::Char('4')));
    for c in "AccountBalance".chars() {
        app.handle(key(KeyCode::Char(c)));
    }
    app.handle(key(KeyCode::Tab));
    app.handle(key(KeyCode::Tab));
    for c in "not-a-number".chars() {
        app.handle(key(KeyCode::Char(c)));
    }

    app.handle(key(KeyCode::Enter));
    assert!(
        !app.projections.loading,
        "an invalid sequence must not submit"
    );
    assert!(app.projections.error.is_some());
}
