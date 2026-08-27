//! The "view" half - reads `App`, never mutates it. Same
//! `Tabs`/status-line skeleton `skilj-tui`'s own `ui.rs` uses, with a
//! single-selection `List` per tab instead of that crate's own
//! form/live-feed widgets - every tab here is read-only browsing, never
//! input.

use crate::app::{App, Tab};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Tabs};
use ratatui::Frame;

pub fn draw(frame: &mut Frame, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0), Constraint::Length(1)])
        .split(frame.area());

    draw_tabs(frame, app, chunks[0]);
    match app.tab {
        Tab::BoundedContexts => draw_bounded_contexts(frame, app, chunks[1]),
        Tab::EventTypes => draw_event_types(frame, app, chunks[1]),
        Tab::CommandTypes => draw_command_types(frame, app, chunks[1]),
        Tab::Projections => draw_projections(frame, app, chunks[1]),
        Tab::Events => draw_events(frame, app, chunks[1]),
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
    let title = match &app.selected_bc {
        Some(bc) => format!("skilj-inspector - {bc} (read-only) (q / Esc to quit)"),
        None => "skilj-inspector - read-only (q / Esc to quit)".to_string(),
    };
    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title(title))
        .select(selected)
        .highlight_style(Style::default().add_modifier(Modifier::BOLD).fg(Color::Cyan));
    frame.render_widget(tabs, area);
}

fn draw_status_line(frame: &mut Frame, app: &App, area: Rect) {
    frame.render_widget(
        Paragraph::new(app.status.as_str()).style(Style::default().fg(Color::Yellow)),
        area,
    );
}

fn selectable_list<'a>(items: Vec<ListItem<'a>>, title: &str, selected: usize) -> (List<'a>, ListState) {
    let is_empty = items.is_empty();
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(title.to_string()))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    let mut state = ListState::default();
    if !is_empty {
        state.select(Some(selected));
    }
    (list, state)
}

fn draw_bounded_contexts(frame: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .bounded_contexts
        .iter()
        .map(|bc| ListItem::new(format!("{}  [{:?}]", bc.name, bc.status)))
        .collect();
    let (list, mut state) = selectable_list(
        items,
        "Bounded Contexts - Enter to drill in",
        app.bc_list_selected,
    );
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_event_types(frame: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .data
        .as_ref()
        .map(|d| {
            d.event_types
                .iter()
                .map(|et| {
                    ListItem::new(format!(
                        "{}  v{}  external={} direct={} scheduled={} read={}",
                        et.name,
                        et.schema_version,
                        et.external_creation_allowed,
                        et.direct_creation_allowed,
                        et.system_triggered_allowed,
                        et.event_read_allowed
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let (list, mut state) = selectable_list(items, "Event Types", app.list_selected);
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_command_types(frame: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .data
        .as_ref()
        .map(|d| {
            d.command_types
                .iter()
                .map(|ct| {
                    ListItem::new(format!(
                        "{}  v{}  rest_trigger={}",
                        ct.name, ct.schema_version, ct.rest_trigger_allowed
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let (list, mut state) = selectable_list(items, "Command Types", app.list_selected);
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_projections(frame: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .data
        .as_ref()
        .map(|d| {
            d.projections
                .iter()
                .map(|p| {
                    ListItem::new(format!(
                        "{}  v{}  sync={}  caught_up_to={:?}",
                        p.name, p.schema_version, p.sync, p.caught_up_to
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let (list, mut state) = selectable_list(items, "Projections", app.list_selected);
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_events(frame: &mut Frame, app: &App, area: Rect) {
    let items: Vec<ListItem> = app
        .data
        .as_ref()
        .map(|d| {
            d.recent_events
                .iter()
                .map(|e| {
                    ListItem::new(format!(
                        "#{}  {}  {}",
                        e.sequence, e.event_type.name, e.payload
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    let (list, mut state) = selectable_list(
        items,
        &format!("Events (newest first, most recent {})", crate::data::RECENT_EVENTS_LIMIT),
        app.list_selected,
    );
    frame.render_stateful_widget(list, area, &mut state);
}
