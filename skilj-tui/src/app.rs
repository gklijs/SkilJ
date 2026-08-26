//! Application state and its update logic - the "model" and "update"
//! halves of the usual ratatui `loop { draw(&model); model = update(model, event) }`
//! shape. `ui.rs` is the "view" half, reading this but never mutating it.

use crate::graphql::{Client, ClientError};
use crossterm::event::{Event as TermEvent, KeyCode, KeyEvent, KeyModifiers};
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc;

/// The four v1 tabs - see docs/architecture.md §11 for what's
/// deliberately not here yet (schema-driven forms, the superadmin
/// directory, admin-console operations).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tab {
    LiveEvents,
    QueryEvents,
    Commands,
    Projections,
}

impl Tab {
    pub const ALL: [Tab; 4] = [Tab::LiveEvents, Tab::QueryEvents, Tab::Commands, Tab::Projections];

    pub fn title(self) -> &'static str {
        match self {
            Tab::LiveEvents => "Live Events",
            Tab::QueryEvents => "Query Events",
            Tab::Commands => "Commands",
            Tab::Projections => "Projections",
        }
    }
}

/// A minimal single-line editable text buffer - ratatui ships no input
/// widget itself, and this app's forms are simple enough (one or two
/// plain-text fields per tab) that a small hand-rolled one is simpler
/// than a new dependency for it.
#[derive(Debug, Default, Clone)]
pub struct TextInput {
    pub value: String,
}

impl TextInput {
    pub fn push(&mut self, c: char) {
        self.value.push(c);
    }

    pub fn backspace(&mut self) {
        self.value.pop();
    }
}

/// Everything that can change app state - fed into one channel from
/// three sources: the terminal-input reader task, the Live Events
/// subscription task, and whichever ad hoc query/mutation task the user
/// most recently triggered (each tagged with its own variant so the
/// result lands in the right pane without extra routing).
pub enum AppEvent {
    Term(TermEvent),
    LiveEvent(Result<Value, ClientError>),
    QueryEventsResult(Result<Value, ClientError>),
    CommandResult(Result<Value, ClientError>),
    ProjectionResult(Result<Value, ClientError>),
}

const MAX_LIVE_EVENTS: usize = 200;

pub struct QueryEventsTab {
    pub event_types: TextInput,
    pub results: Vec<Value>,
    pub error: Option<String>,
    pub loading: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandField {
    TypeName,
    Payload,
}

pub struct CommandsTab {
    pub focus: CommandField,
    pub type_name: TextInput,
    pub payload: TextInput,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub loading: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionField {
    Name,
    Key,
}

pub struct ProjectionsTab {
    pub focus: ProjectionField,
    pub name: TextInput,
    pub key: TextInput,
    pub result: Option<Value>,
    pub error: Option<String>,
    pub loading: bool,
}

pub struct App {
    pub should_quit: bool,
    pub tab: Tab,
    pub bounded_context: String,
    pub status: Option<String>,

    pub live_events: VecDeque<Value>,
    pub live_connected: bool,

    pub query_events: QueryEventsTab,
    pub commands: CommandsTab,
    pub projections: ProjectionsTab,

    client: Arc<Client>,
    events_tx: mpsc::UnboundedSender<AppEvent>,
}

impl App {
    pub fn new(client: Arc<Client>, bounded_context: String, events_tx: mpsc::UnboundedSender<AppEvent>) -> Self {
        Self {
            should_quit: false,
            tab: Tab::LiveEvents,
            bounded_context,
            status: None,
            live_events: VecDeque::new(),
            live_connected: false,
            query_events: QueryEventsTab {
                event_types: TextInput::default(),
                results: Vec::new(),
                error: None,
                loading: false,
            },
            commands: CommandsTab {
                focus: CommandField::TypeName,
                type_name: TextInput::default(),
                payload: TextInput::default(),
                result: None,
                error: None,
                loading: false,
            },
            projections: ProjectionsTab {
                focus: ProjectionField::Name,
                name: TextInput::default(),
                key: TextInput::default(),
                result: None,
                error: None,
                loading: false,
            },
            client,
            events_tx,
        }
    }

    /// The one place `AppEvent`s are applied - keeps `main.rs`'s own
    /// loop to "read an event, call this, redraw".
    pub fn handle(&mut self, event: AppEvent) {
        match event {
            AppEvent::Term(TermEvent::Key(key)) => self.handle_key(key),
            AppEvent::Term(_) => {}
            AppEvent::LiveEvent(Ok(data)) => {
                self.live_connected = true;
                if self.live_events.len() >= MAX_LIVE_EVENTS {
                    self.live_events.pop_front();
                }
                self.live_events.push_back(data);
            }
            AppEvent::LiveEvent(Err(e)) => {
                self.live_connected = false;
                self.status = Some(format!("live events: {e}"));
            }
            AppEvent::QueryEventsResult(result) => {
                self.query_events.loading = false;
                match result {
                    Ok(data) => {
                        self.query_events.results = data
                            .get("queryEvents")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default();
                        self.query_events.error = None;
                    }
                    Err(e) => self.query_events.error = Some(e.to_string()),
                }
            }
            AppEvent::CommandResult(result) => {
                self.commands.loading = false;
                match result {
                    Ok(data) => {
                        self.commands.result = data.get("submitCommand").cloned();
                        self.commands.error = None;
                    }
                    Err(e) => self.commands.error = Some(e.to_string()),
                }
            }
            AppEvent::ProjectionResult(result) => {
                self.projections.loading = false;
                match result {
                    Ok(data) => {
                        self.projections.result = data.get("projection").cloned();
                        self.projections.error = None;
                    }
                    Err(e) => self.projections.error = Some(e.to_string()),
                }
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.should_quit = true;
            return;
        }

        // Digits switch the main tab regardless of what's focused
        // in-tab - editable fields here are plain text, not numeric, so
        // there's no real ambiguity to worry about.
        match key.code {
            KeyCode::Char('1') => return self.tab = Tab::LiveEvents,
            KeyCode::Char('2') => return self.tab = Tab::QueryEvents,
            KeyCode::Char('3') => return self.tab = Tab::Commands,
            KeyCode::Char('4') => return self.tab = Tab::Projections,
            KeyCode::Esc => {
                self.should_quit = true;
                return;
            }
            _ => {}
        }

        match self.tab {
            Tab::LiveEvents => {}
            Tab::QueryEvents => self.handle_query_events_key(key),
            Tab::Commands => self.handle_commands_key(key),
            Tab::Projections => self.handle_projections_key(key),
        }
    }

    fn handle_query_events_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => self.submit_query_events(),
            KeyCode::Backspace => self.query_events.event_types.backspace(),
            KeyCode::Char(c) => self.query_events.event_types.push(c),
            _ => {}
        }
    }

    fn submit_query_events(&mut self) {
        let event_types: Vec<Value> = self
            .query_events
            .event_types
            .value
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| Value::String(s.to_string()))
            .collect();
        if event_types.is_empty() {
            self.query_events.error = Some("enter at least one event type (comma-separated)".into());
            return;
        }
        self.query_events.loading = true;
        self.query_events.error = None;
        let client = self.client.clone();
        let tx = self.events_tx.clone();
        let bounded_context = self.bounded_context.clone();
        tokio::spawn(async move {
            let result = client
                .request(
                    "query($bc: String!, $types: [String!]!) { \
                        queryEvents(boundedContext: $bc, eventTypes: $types) { sequence payload } \
                    }",
                    serde_json::json!({ "bc": bounded_context, "types": event_types }),
                )
                .await;
            let _ = tx.send(AppEvent::QueryEventsResult(result));
        });
    }

    fn handle_commands_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Tab => {
                self.commands.focus = match self.commands.focus {
                    CommandField::TypeName => CommandField::Payload,
                    CommandField::Payload => CommandField::TypeName,
                };
            }
            KeyCode::Enter => self.submit_command(),
            KeyCode::Backspace => self.commands_focused_field().backspace(),
            KeyCode::Char(c) => self.commands_focused_field().push(c),
            _ => {}
        }
    }

    fn commands_focused_field(&mut self) -> &mut TextInput {
        match self.commands.focus {
            CommandField::TypeName => &mut self.commands.type_name,
            CommandField::Payload => &mut self.commands.payload,
        }
    }

    fn submit_command(&mut self) {
        let type_name = self.commands.type_name.value.trim().to_string();
        if type_name.is_empty() {
            self.commands.error = Some("enter a command type name".into());
            return;
        }
        let payload_str = if self.commands.payload.value.trim().is_empty() {
            "{}".to_string()
        } else {
            self.commands.payload.value.clone()
        };
        if let Err(e) = serde_json::from_str::<Value>(&payload_str) {
            self.commands.error = Some(format!("payload is not valid JSON: {e}"));
            return;
        }
        self.commands.loading = true;
        self.commands.error = None;
        let client = self.client.clone();
        let tx = self.events_tx.clone();
        let bounded_context = self.bounded_context.clone();
        tokio::spawn(async move {
            let result = client
                .request(
                    "mutation($bc: String!, $type: String!, $payload: String!) { \
                        submitCommand(boundedContext: $bc, commandTypeName: $type, payload: $payload) { \
                            accepted rejectionReason rejectionKind triggeredEventSequences \
                        } \
                    }",
                    serde_json::json!({ "bc": bounded_context, "type": type_name, "payload": payload_str }),
                )
                .await;
            let _ = tx.send(AppEvent::CommandResult(result));
        });
    }

    fn handle_projections_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Tab => {
                self.projections.focus = match self.projections.focus {
                    ProjectionField::Name => ProjectionField::Key,
                    ProjectionField::Key => ProjectionField::Name,
                };
            }
            KeyCode::Enter => self.submit_projection(),
            KeyCode::Backspace => self.projections_focused_field().backspace(),
            KeyCode::Char(c) => self.projections_focused_field().push(c),
            _ => {}
        }
    }

    fn projections_focused_field(&mut self) -> &mut TextInput {
        match self.projections.focus {
            ProjectionField::Name => &mut self.projections.name,
            ProjectionField::Key => &mut self.projections.key,
        }
    }

    fn submit_projection(&mut self) {
        let name = self.projections.name.value.trim().to_string();
        if name.is_empty() {
            self.projections.error = Some("enter a projection name".into());
            return;
        }
        let key = self.projections.key.value.trim().to_string();
        self.projections.loading = true;
        self.projections.error = None;
        let client = self.client.clone();
        let tx = self.events_tx.clone();
        let bounded_context = self.bounded_context.clone();
        tokio::spawn(async move {
            let key = if key.is_empty() { None } else { Some(key.as_str()) };
            let result = crate::projection_query::fetch(&client, &bounded_context, &name, key).await;
            let _ = tx.send(AppEvent::ProjectionResult(result));
        });
    }
}
