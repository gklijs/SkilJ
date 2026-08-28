//! The "model" half - `App::handle_key` is the only thing that ever
//! mutates state, `ui::draw` only ever reads it (the same split
//! `skilj-tui`'s own `app.rs`/`ui.rs` already use). No live updates, no
//! background subscription: every read is a key press away, so a plain
//! `crossterm::event::poll` loop in `main.rs` is enough - no dedicated
//! input thread or channel needed, unlike `skilj-tui`'s own websocket
//! subscription.

use crate::data::{self, BoundedContextData};
use crossterm::event::KeyCode;
use skilj_core::db::Pool;
use skilj_core::event_store::BoundedContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    BoundedContexts,
    EventTypes,
    CommandTypes,
    Projections,
    Events,
}

impl Tab {
    pub const ALL: [Tab; 5] = [
        Tab::BoundedContexts,
        Tab::EventTypes,
        Tab::CommandTypes,
        Tab::Projections,
        Tab::Events,
    ];

    pub fn title(&self) -> &'static str {
        match self {
            Tab::BoundedContexts => "Bounded Contexts",
            Tab::EventTypes => "Event Types",
            Tab::CommandTypes => "Command Types",
            Tab::Projections => "Projections",
            Tab::Events => "Events",
        }
    }
}

pub struct App {
    pool: Pool,
    pub tab: Tab,
    pub bounded_contexts: Vec<BoundedContext>,
    pub bc_list_selected: usize,
    pub selected_bc: Option<String>,
    pub data: Option<BoundedContextData>,
    pub list_selected: usize,
    pub status: String,
    pub should_quit: bool,
}

impl App {
    pub fn new(pool: Pool, bounded_contexts: Vec<BoundedContext>) -> Self {
        Self {
            pool,
            tab: Tab::BoundedContexts,
            bounded_contexts,
            bc_list_selected: 0,
            selected_bc: None,
            data: None,
            list_selected: 0,
            status: "↑/↓ select · Enter drill in · 1-5 tabs · r refresh · q quit".to_string(),
            should_quit: false,
        }
    }

    /// Length of whichever list the active tab is showing - `ui::draw`
    /// and `handle_key`'s own bounds-clamping both need this, so it's
    /// computed once, here, rather than duplicated in both places.
    fn active_list_len(&self) -> usize {
        match self.tab {
            Tab::BoundedContexts => self.bounded_contexts.len(),
            Tab::EventTypes => self.data.as_ref().map_or(0, |d| d.event_types.len()),
            Tab::CommandTypes => self.data.as_ref().map_or(0, |d| d.command_types.len()),
            Tab::Projections => self.data.as_ref().map_or(0, |d| d.projections.len()),
            Tab::Events => self.data.as_ref().map_or(0, |d| d.recent_events.len()),
        }
    }

    pub async fn handle_key(&mut self, code: KeyCode) {
        match code {
            KeyCode::Char('q') | KeyCode::Esc => self.should_quit = true,
            KeyCode::Char('1') => self.tab = Tab::BoundedContexts,
            KeyCode::Char('2') => self.switch_to(Tab::EventTypes),
            KeyCode::Char('3') => self.switch_to(Tab::CommandTypes),
            KeyCode::Char('4') => self.switch_to(Tab::Projections),
            KeyCode::Char('5') => self.switch_to(Tab::Events),
            KeyCode::Up => {
                let sel = self.selected_index_mut();
                *sel = sel.saturating_sub(1);
            }
            KeyCode::Down => {
                let len = self.active_list_len();
                let sel = self.selected_index_mut();
                if len > 0 && *sel + 1 < len {
                    *sel += 1;
                }
            }
            KeyCode::Enter if self.tab == Tab::BoundedContexts => self.drill_in().await,
            KeyCode::Char('r') => self.refresh().await,
            _ => {}
        }
    }

    fn selected_index_mut(&mut self) -> &mut usize {
        match self.tab {
            Tab::BoundedContexts => &mut self.bc_list_selected,
            _ => &mut self.list_selected,
        }
    }

    fn switch_to(&mut self, tab: Tab) {
        if self.selected_bc.is_none() {
            self.status =
                "select a bounded context first (Enter on the Bounded Contexts tab)".to_string();
            return;
        }
        self.tab = tab;
        self.list_selected = 0;
    }

    async fn drill_in(&mut self) {
        let Some(bc) = self.bounded_contexts.get(self.bc_list_selected) else {
            return;
        };
        let name = bc.name.clone();
        match data::load_bounded_context_data(&self.pool, &name).await {
            Ok(loaded) => {
                self.data = Some(loaded);
                self.selected_bc = Some(name.clone());
                self.list_selected = 0;
                self.tab = Tab::EventTypes;
                self.status = format!("{name}: loaded");
            }
            Err(err) => self.status = format!("failed to load {name}: {err}"),
        }
    }

    async fn refresh(&mut self) {
        match self.tab {
            Tab::BoundedContexts => match data::load_bounded_contexts(&self.pool).await {
                Ok(bcs) => {
                    self.bounded_contexts = bcs;
                    self.status = "refreshed".to_string();
                }
                Err(err) => self.status = format!("refresh failed: {err}"),
            },
            _ => {
                if let Some(name) = self.selected_bc.clone() {
                    match data::load_bounded_context_data(&self.pool, &name).await {
                        Ok(loaded) => {
                            self.data = Some(loaded);
                            self.status = "refreshed".to_string();
                        }
                        Err(err) => self.status = format!("refresh failed: {err}"),
                    }
                }
            }
        }
    }
}
