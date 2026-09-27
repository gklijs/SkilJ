//! The "view" half - reads `App`, never mutates it. One `draw` entry
//! point per frame, called from `main.rs`'s own loop.

use crate::app::{App, CommandsStage, ProjectionField, Tab};
use crate::form::Widget as FormWidget;
use crate::json_style;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Tabs, Wrap};
use ratatui::Frame;
use serde_json::Value;

pub fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(frame.area());

    draw_tabs(frame, app, chunks[0]);
    match app.tab {
        Tab::LiveEvents => draw_live_events(frame, app, chunks[1]),
        Tab::QueryEvents => draw_query_events(frame, app, chunks[1]),
        Tab::Commands => draw_commands(frame, app, chunks[1]),
        Tab::Projections => draw_projections(frame, app, chunks[1]),
        Tab::ParkedDeliveries => draw_parked_deliveries(frame, app, chunks[1]),
    }
    draw_status_line(frame, app, chunks[2]);
}

fn draw_tabs(frame: &mut Frame, app: &App, area: Rect) {
    let titles: Vec<Line> = Tab::ALL
        .iter()
        .enumerate()
        .map(|(i, t)| Line::from(format!("{}: {}", i + 1, t.title())))
        .collect();
    let selected = Tab::ALL.iter().position(|t| *t == app.tab).unwrap_or(0);
    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title(format!(
            "skilj-tui - {} (Esc / Ctrl+C to quit)",
            app.bounded_context
        )))
        .select(selected)
        .highlight_style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .fg(Color::Cyan),
        );
    frame.render_widget(tabs, area);
}

fn draw_status_line(frame: &mut Frame, app: &App, area: Rect) {
    let text = app.status.as_deref().unwrap_or("");
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(Color::Yellow)),
        area,
    );
}

/// Codeberg issue #7: `/` enters filter-compose mode (see
/// `App::handle_live_events_key`'s own doc comment for why), substring-
/// matched case-insensitively against each event's own compact JSON
/// rendering - client-side only, no server round trip.
fn draw_live_events(frame: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let filter_style = if app.live_events_filter_active {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default()
    };
    let filter_title = if app.live_events_filter_active {
        "Filter (substring) - Enter/Esc to stop editing"
    } else {
        "Filter - / to edit"
    };
    let filter_box = Paragraph::new(app.live_events_filter.value.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(filter_style)
            .title(filter_title),
    );
    frame.render_widget(filter_box, chunks[0]);

    let connected = if app.live_connected {
        "connected"
    } else {
        "connecting..."
    };
    let filter = app.live_events_filter.value.to_lowercase();
    let items: Vec<ListItem> = app
        .live_events
        .iter()
        .rev()
        .filter(|event| {
            filter.is_empty()
                || json_style::compact_plain(event)
                    .to_lowercase()
                    .contains(&filter)
        })
        .map(|event| ListItem::new(json_style::compact(event)))
        .collect();
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Live Events ({connected}) - newest first")),
    );
    frame.render_widget(list, chunks[1]);
}

/// Codeberg issue #8: a real multi-select checklist over
/// `eventTypes(boundedContext)`, replacing v1's free-text comma-separated
/// field - `Space` toggles, `Enter` runs the query with whatever's
/// checked, `r` refreshes the list.
fn draw_query_events(frame: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Min(0)])
        .split(area);

    let picker_title = if let Some(err) = &app.query_events.types_error {
        format!("Event types - {err}")
    } else if app.query_events.types_loading {
        "Event types - loading...".to_string()
    } else {
        "Event types - Space to toggle, Enter to run, n for the next page, r to refresh".to_string()
    };
    let items: Vec<ListItem> = app
        .query_events
        .types
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let mark = if app.query_events.checked.contains(&i) {
                "[x]"
            } else {
                "[ ]"
            };
            ListItem::new(format!("{mark} {}", t.name))
        })
        .collect();
    let is_empty = items.is_empty();
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(picker_title))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    let mut state = ListState::default();
    if !is_empty {
        state.select(Some(app.query_events.list_selected));
    }
    frame.render_stateful_widget(list, chunks[0], &mut state);

    let body = if let Some(err) = &app.query_events.query_error {
        Paragraph::new(err.as_str()).style(Style::default().fg(Color::Red))
    } else if app.query_events.query_loading {
        Paragraph::new("running...")
    } else {
        let lines: Vec<Line> = app
            .query_events
            .results
            .iter()
            .map(json_style::compact)
            .collect();
        Paragraph::new(lines)
    };
    frame.render_widget(
        body.wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::ALL).title(format!(
                "Results ({} shown - n loads the next page)",
                app.query_events.results.len()
            ))),
        chunks[1],
    );
}

/// Codeberg issue #8: a real picker over `commandTypes(boundedContext)`
/// replacing v1's free-text type name, then a form generated from the
/// picked type's own schema (`form::fields_from_schema`) replacing v1's
/// raw-JSON payload entry.
fn draw_commands(frame: &mut Frame, app: &App, area: Rect) {
    match &app.commands.stage {
        CommandsStage::Picking {
            types,
            list_selected,
            loading,
            error,
        } => {
            draw_commands_picking(
                frame,
                area,
                types,
                *list_selected,
                *loading,
                error.as_deref(),
            );
        }
        CommandsStage::Form {
            type_name,
            fields,
            focus,
            result,
            error,
            loading,
            ..
        } => {
            draw_commands_form(
                frame,
                area,
                type_name,
                fields,
                *focus,
                result.as_ref(),
                error.as_deref(),
                *loading,
            );
        }
    }
}

fn draw_commands_picking(
    frame: &mut Frame,
    area: Rect,
    types: &[crate::app::TypeOption],
    list_selected: usize,
    loading: bool,
    error: Option<&str>,
) {
    let title = if let Some(err) = error {
        format!("Command types - {err}")
    } else if loading {
        "Command types - loading...".to_string()
    } else {
        "Command types - Enter to pick, r to refresh".to_string()
    };
    let items: Vec<ListItem> = types
        .iter()
        .map(|t| ListItem::new(t.name.clone()))
        .collect();
    let is_empty = items.is_empty();
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    let mut state = ListState::default();
    if !is_empty {
        state.select(Some(list_selected));
    }
    frame.render_stateful_widget(list, area, &mut state);
}

#[allow(clippy::too_many_arguments)]
fn draw_commands_form(
    frame: &mut Frame,
    area: Rect,
    type_name: &str,
    fields: &[crate::form::Field],
    focus: usize,
    result: Option<&serde_json::Value>,
    error: Option<&str>,
    loading: bool,
) {
    // One row per field (or a single "no fields" line for a schema with
    // none), plus the result panel - `Min(0)` on the result row lets it
    // absorb whatever space the fields don't need.
    let field_rows = fields.len().max(1);
    let mut constraints: Vec<Constraint> = (0..field_rows).map(|_| Constraint::Length(3)).collect();
    constraints.push(Constraint::Min(0));
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    if fields.is_empty() {
        let empty = Paragraph::new("(this type's schema declares no fields - Enter to submit {})")
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("{type_name} - Esc to pick a different type")),
            );
        frame.render_widget(empty, chunks[0]);
    } else {
        for (i, field) in fields.iter().enumerate() {
            let focused = i == focus;
            let (value, kind) = match &field.widget {
                FormWidget::Text(v) => (v.as_str(), "text"),
                FormWidget::Number(v) => (v.as_str(), "number"),
                FormWidget::RawJson(v) => (v.as_str(), "raw JSON"),
                FormWidget::Bool(b) => (if *b { "[x]" } else { "[ ]" }, "boolean, Space to toggle"),
            };
            let title = if i == 0 {
                format!(
                    "{type_name}.{} ({kind}) - Tab to switch field, Esc to pick a different type",
                    field.name
                )
            } else {
                format!("{} ({kind})", field.name)
            };
            let input = Paragraph::new(value).block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(focus_style(focused))
                    .title(title),
            );
            frame.render_widget(input, chunks[i]);
        }
    }

    let body = if let Some(err) = error {
        Paragraph::new(err).style(Style::default().fg(Color::Red))
    } else if loading {
        Paragraph::new("submitting...")
    } else if let Some(result) = result {
        Paragraph::new(json_style::pretty(result))
    } else {
        Paragraph::new("Enter to submit")
    };
    frame.render_widget(
        body.wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::ALL).title("Result")),
        chunks[field_rows],
    );
}

fn draw_projections(frame: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(area);

    let name_style = focus_style(app.projections.focus == ProjectionField::Name);
    let name_input = Paragraph::new(app.projections.name.value.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(name_style)
            .title("Projection name - Tab to switch field, Esc to leave this tab"),
    );
    frame.render_widget(name_input, chunks[0]);

    let key_style = focus_style(app.projections.focus == ProjectionField::Key);
    let key_input = Paragraph::new(app.projections.key.value.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(key_style)
            .title("Key (empty = default instance)"),
    );
    frame.render_widget(key_input, chunks[1]);

    // Codeberg issue #7's time-travel projection viewer - see
    // `projection_query::fetch`'s own doc comment for why the title says
    // "at least as fresh as", not "as of".
    let wait_style = focus_style(app.projections.focus == ProjectionField::WaitForSequence);
    let wait_input = Paragraph::new(app.projections.wait_for_sequence.value.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(wait_style)
            .title("At least as fresh as sequence (empty = no wait) - Enter to run"),
    );
    frame.render_widget(wait_input, chunks[2]);

    let body = if let Some(err) = &app.projections.error {
        Paragraph::new(err.as_str()).style(Style::default().fg(Color::Red))
    } else if app.projections.loading {
        Paragraph::new("running...")
    } else if let Some(result) = &app.projections.result {
        Paragraph::new(json_style::pretty(result))
    } else {
        Paragraph::new("")
    };
    frame.render_widget(
        body.wrap(Wrap { trim: false })
            .block(Block::default().borders(Borders::ALL).title("State")),
        chunks[3],
    );
}

/// Codeberg issue #21 - `parkedDeliveries`/`retryParkedDelivery`/
/// `discardParkedDelivery`: a list of parked deliveries (Enter retries
/// the selected row, `d` discards it, `r` refreshes), with the selected
/// row's own full JSON (including `requestJson` - what a retry actually
/// redrives - too wide to fit the list row itself) shown below.
fn draw_parked_deliveries(frame: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(50), Constraint::Min(0)])
        .split(area);

    let title = if let Some(err) = &app.parked_deliveries.error {
        format!("Parked deliveries - {err}")
    } else if app.parked_deliveries.loading {
        "Parked deliveries - loading...".to_string()
    } else {
        "Parked deliveries - Enter to retry, d to discard, r to refresh".to_string()
    };
    let items: Vec<ListItem> = app
        .parked_deliveries
        .items
        .iter()
        .map(|item| {
            let source = item.get("source").and_then(Value::as_str).unwrap_or("");
            let identifier = item.get("identifier").and_then(Value::as_str).unwrap_or("");
            let attempts = item
                .get("attemptCount")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let error = item.get("error").and_then(Value::as_str).unwrap_or("");
            ListItem::new(format!(
                "{source} {identifier} (attempts: {attempts}) - {error}"
            ))
        })
        .collect();
    let is_empty = items.is_empty();
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    let mut state = ListState::default();
    if !is_empty {
        state.select(Some(app.parked_deliveries.list_selected));
    }
    frame.render_stateful_widget(list, chunks[0], &mut state);

    let body = app
        .parked_deliveries
        .items
        .get(app.parked_deliveries.list_selected)
        .map(json_style::pretty)
        .unwrap_or_default();
    frame.render_widget(
        Paragraph::new(body).wrap(Wrap { trim: false }).block(
            Block::default()
                .borders(Borders::ALL)
                .title("Selected delivery"),
        ),
        chunks[1],
    );
}

fn focus_style(focused: bool) -> Style {
    if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default()
    }
}
