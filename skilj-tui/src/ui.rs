//! The "view" half - reads `App`, never mutates it. One `draw` entry
//! point per frame, called from `main.rs`'s own loop.

use crate::app::{App, CommandField, ProjectionField, Tab};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, List, ListItem, Paragraph, Tabs, Wrap};
use ratatui::Frame;

pub fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0), Constraint::Length(1)])
        .split(frame.area());

    draw_tabs(frame, app, chunks[0]);
    match app.tab {
        Tab::LiveEvents => draw_live_events(frame, app, chunks[1]),
        Tab::QueryEvents => draw_query_events(frame, app, chunks[1]),
        Tab::Commands => draw_commands(frame, app, chunks[1]),
        Tab::Projections => draw_projections(frame, app, chunks[1]),
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
            "skilj-tui - {} (q / Esc / Ctrl+C to quit)",
            app.bounded_context
        )))
        .select(selected)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan));
    frame.render_widget(tabs, area);
}

fn draw_status_line(frame: &mut Frame, app: &App, area: Rect) {
    let text = app.status.as_deref().unwrap_or("");
    frame.render_widget(Paragraph::new(text).style(Style::default().fg(Color::Yellow)), area);
}

fn draw_live_events(frame: &mut Frame, app: &App, area: Rect) {
    let connected = if app.live_connected { "connected" } else { "connecting..." };
    let items: Vec<ListItem> = app
        .live_events
        .iter()
        .rev()
        .map(|event| ListItem::new(pretty_compact(event)))
        .collect();
    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .title(format!("Live Events ({connected}) - newest first")),
    );
    frame.render_widget(list, area);
}

fn draw_query_events(frame: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let input = Paragraph::new(app.query_events.event_types.value.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .title("Event types (comma-separated) - Enter to run"),
    );
    frame.render_widget(input, chunks[0]);

    let body = if let Some(err) = &app.query_events.error {
        Paragraph::new(err.as_str()).style(Style::default().fg(Color::Red))
    } else if app.query_events.loading {
        Paragraph::new("running...")
    } else {
        let lines: Vec<Line> = app
            .query_events
            .results
            .iter()
            .map(|e| Line::from(pretty_compact(e)))
            .collect();
        Paragraph::new(lines)
    };
    frame.render_widget(
        body.wrap(Wrap { trim: false }).block(Block::default().borders(Borders::ALL).title("Results")),
        chunks[1],
    );
}

fn draw_commands(frame: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let type_style = focus_style(app.commands.focus == CommandField::TypeName);
    let type_input = Paragraph::new(app.commands.type_name.value.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(type_style)
            .title("Command type name - Tab to switch field"),
    );
    frame.render_widget(type_input, chunks[0]);

    let payload_style = focus_style(app.commands.focus == CommandField::Payload);
    let payload_input = Paragraph::new(app.commands.payload.value.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(payload_style)
            .title("Payload (raw JSON, empty = {}) - Enter to submit"),
    );
    frame.render_widget(payload_input, chunks[1]);

    let body = if let Some(err) = &app.commands.error {
        Paragraph::new(err.as_str()).style(Style::default().fg(Color::Red))
    } else if app.commands.loading {
        Paragraph::new("submitting...")
    } else if let Some(result) = &app.commands.result {
        Paragraph::new(pretty(result))
    } else {
        Paragraph::new("")
    };
    frame.render_widget(
        body.wrap(Wrap { trim: false }).block(Block::default().borders(Borders::ALL).title("Result")),
        chunks[2],
    );
}

fn draw_projections(frame: &mut Frame, app: &App, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Length(3), Constraint::Min(0)])
        .split(area);

    let name_style = focus_style(app.projections.focus == ProjectionField::Name);
    let name_input = Paragraph::new(app.projections.name.value.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(name_style)
            .title("Projection name - Tab to switch field"),
    );
    frame.render_widget(name_input, chunks[0]);

    let key_style = focus_style(app.projections.focus == ProjectionField::Key);
    let key_input = Paragraph::new(app.projections.key.value.as_str()).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(key_style)
            .title("Key (empty = default instance) - Enter to run"),
    );
    frame.render_widget(key_input, chunks[1]);

    let body = if let Some(err) = &app.projections.error {
        Paragraph::new(err.as_str()).style(Style::default().fg(Color::Red))
    } else if app.projections.loading {
        Paragraph::new("running...")
    } else if let Some(result) = &app.projections.result {
        Paragraph::new(pretty(result))
    } else {
        Paragraph::new("")
    };
    frame.render_widget(
        body.wrap(Wrap { trim: false }).block(Block::default().borders(Borders::ALL).title("State")),
        chunks[2],
    );
}

fn focus_style(focused: bool) -> Style {
    if focused {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default()
    }
}

fn pretty(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn pretty_compact(value: &serde_json::Value) -> String {
    value.to_string()
}
