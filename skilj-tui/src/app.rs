//! Application state and its update logic - the "model" and "update"
//! halves of the usual ratatui `loop { draw(&model); model = update(model, event) }`
//! shape. `ui.rs` is the "view" half, reading this but never mutating it.

use crate::form;
use crate::graphql::{Client, ClientError};
use crossterm::event::{Event as TermEvent, KeyCode, KeyEvent, KeyModifiers};
use serde_json::Value;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc;

/// The four v1 tabs - see [docs/architecture.md §11](../../docs/architecture.md#skilj-tui-console) for what's
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
    pub const ALL: [Tab; 4] = [
        Tab::LiveEvents,
        Tab::QueryEvents,
        Tab::Commands,
        Tab::Projections,
    ];

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
    QueryEventsTypesResult(Result<Value, ClientError>),
    CommandResult(Result<Value, ClientError>),
    CommandTypesResult(Result<Value, ClientError>),
    ProjectionResult(Result<Value, ClientError>),
}

const MAX_LIVE_EVENTS: usize = 200;

/// One row of a `commandTypes`/`eventTypes` result - name plus the raw
/// JSON Schema string `form::fields_from_schema` builds a form from
/// (Commands) or nothing further needed for (Query Events, which only
/// ever needs the name to query by).
#[derive(Debug, Clone)]
pub struct TypeOption {
    pub name: String,
    pub schema: String,
}

fn parse_type_options(data: &Value, field: &str) -> Vec<TypeOption> {
    data.get(field)
        .and_then(Value::as_array)
        .map(|types| {
            types
                .iter()
                .filter_map(|t| {
                    let name = t.get("name")?.as_str()?.to_string();
                    let schema = t
                        .get("schema")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string();
                    Some(TypeOption { name, schema })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Query Events - Codeberg issue #8: the free-text, comma-separated
/// `event_types` field became a real picker over `eventTypes(boundedContext)`
/// ([§13](../../docs/architecture.md#self-describing-graphql-surface)/Codeberg issue #6's "5a"). One flat struct rather than a
/// `CommandsTab`-style two-stage enum: picking which types to include and
/// seeing the last query's results are never mutually exclusive views
/// the way Commands' picker/form are - both stay visible together, the
/// same layout the old text-field-plus-results split already had.
pub struct QueryEventsTab {
    pub types: Vec<TypeOption>,
    pub checked: HashSet<usize>,
    pub list_selected: usize,
    pub types_loading: bool,
    pub types_error: Option<String>,
    pub results: Vec<Value>,
    pub query_error: Option<String>,
    pub query_loading: bool,
}

/// Commands - Codeberg issue #8. Picking a type and filling in its
/// generated form are mutually exclusive views (unlike Query Events'
/// picker+results, which coexist), so this is a real two-stage enum:
/// `Picking` a real, registered command type, then a `Form` generated
/// from that type's own schema (`form::fields_from_schema`) replaces
/// v1's raw-JSON payload entry.
pub enum CommandsStage {
    Picking {
        types: Vec<TypeOption>,
        list_selected: usize,
        loading: bool,
        error: Option<String>,
    },
    Form {
        // Carried over from `Picking` at pick time (never re-fetched) -
        // see `commands_form_back_to_picking`'s own doc comment.
        types: Vec<TypeOption>,
        list_selected: usize,
        type_name: String,
        fields: Vec<form::Field>,
        focus: usize,
        result: Option<Value>,
        error: Option<String>,
        loading: bool,
    },
}

pub struct CommandsTab {
    pub stage: CommandsStage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionField {
    Name,
    Key,
    // Codeberg issue #7's time-travel projection viewer -
    // `projection_query::fetch`'s own `wait_for_sequence` param, see its
    // doc comment for why this is a freshness guarantee, not a
    // historical snapshot.
    WaitForSequence,
}

pub struct ProjectionsTab {
    pub focus: ProjectionField,
    pub name: TextInput,
    pub key: TextInput,
    pub wait_for_sequence: TextInput,
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
    pub live_events_filter: TextInput,
    pub live_events_filter_active: bool,

    pub query_events: QueryEventsTab,
    pub commands: CommandsTab,
    pub projections: ProjectionsTab,

    client: Arc<Client>,
    events_tx: mpsc::UnboundedSender<AppEvent>,
}

impl App {
    pub fn new(
        client: Arc<Client>,
        bounded_context: String,
        events_tx: mpsc::UnboundedSender<AppEvent>,
    ) -> Self {
        Self {
            should_quit: false,
            tab: Tab::LiveEvents,
            bounded_context,
            status: None,
            live_events: VecDeque::new(),
            live_connected: false,
            live_events_filter: TextInput::default(),
            live_events_filter_active: false,
            query_events: QueryEventsTab {
                types: Vec::new(),
                checked: HashSet::new(),
                list_selected: 0,
                types_loading: false,
                types_error: None,
                results: Vec::new(),
                query_error: None,
                query_loading: false,
            },
            commands: CommandsTab {
                stage: CommandsStage::Picking {
                    types: Vec::new(),
                    list_selected: 0,
                    loading: false,
                    error: None,
                },
            },
            projections: ProjectionsTab {
                focus: ProjectionField::Name,
                name: TextInput::default(),
                key: TextInput::default(),
                wait_for_sequence: TextInput::default(),
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
                self.query_events.query_loading = false;
                match result {
                    Ok(data) => {
                        self.query_events.results = data
                            .get("queryEvents")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default();
                        self.query_events.query_error = None;
                    }
                    Err(e) => self.query_events.query_error = Some(e.to_string()),
                }
            }
            AppEvent::QueryEventsTypesResult(result) => {
                self.query_events.types_loading = false;
                match result {
                    Ok(data) => {
                        self.query_events.types = parse_type_options(&data, "eventTypes");
                        self.query_events.checked.clear();
                        self.query_events.list_selected = 0;
                        self.query_events.types_error = None;
                    }
                    Err(e) => self.query_events.types_error = Some(e.to_string()),
                }
            }
            AppEvent::CommandResult(result) => {
                if let CommandsStage::Form {
                    loading,
                    error,
                    result: slot,
                    ..
                } = &mut self.commands.stage
                {
                    *loading = false;
                    match result {
                        Ok(data) => {
                            *slot = data.get("submitCommand").cloned();
                            *error = None;
                        }
                        Err(e) => *error = Some(e.to_string()),
                    }
                }
            }
            AppEvent::CommandTypesResult(result) => {
                if let CommandsStage::Picking {
                    types,
                    list_selected,
                    loading,
                    error,
                } = &mut self.commands.stage
                {
                    *loading = false;
                    match result {
                        Ok(data) => {
                            *types = parse_type_options(&data, "commandTypes");
                            *list_selected = 0;
                            *error = None;
                        }
                        Err(e) => *error = Some(e.to_string()),
                    }
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

        // Esc backs out of a sub-view first - Commands' generated form,
        // back to its type picker (cheaply, from the list already
        // fetched, not a refetch), Live Events' own filter-compose mode,
        // or Projections' own always-focused fields (all three fields
        // there accept digits now that `WaitForSequence` exists, so
        // every state on that tab intercepts them - unlike Commands,
        // Projections has no "picker" sub-state with no free text to
        // fall back to, so Esc's own job here is to leave the tab
        // entirely, back to Live Events, rather than clear one flag) -
        // and only quits the app when there's no such view to back out
        // of. Ctrl+C above always quits regardless of what's focused.
        if key.code == KeyCode::Esc {
            if let Tab::Commands = self.tab {
                if matches!(self.commands.stage, CommandsStage::Form { .. }) {
                    self.commands_form_back_to_picking();
                    return;
                }
            }
            if self.tab == Tab::LiveEvents && self.live_events_filter_active {
                self.live_events_filter_active = false;
                return;
            }
            if self.tab == Tab::Projections {
                self.tab = Tab::LiveEvents;
                return;
            }
            self.should_quit = true;
            return;
        }

        // Digits switch the main tab regardless of what's focused
        // in-tab - true for every tab except one with real free-text
        // entry live right now: Commands' own generated form (Codeberg
        // issue #8) and Projections' three fields (issue #7's own
        // `waitForSequence` addition made this a real, not just latent,
        // gap there too - see `ProjectionField::WaitForSequence`'s own
        // doc comment). A schema-driven `Widget::Number`/`Widget::Text`
        // field, or a projection key like "acc-1"/a sequence number, can
        // legitimately contain any digit, so digits there must reach the
        // focused field instead of jumping tabs. Commands' *picker*
        // stage has no text entry at all (Up/Down/Enter/`r` only), so
        // digits stay safe to switch tabs there, same as Query Events'
        // now-checklist-only (never free-text) picker and Live Events'
        // own `/`-gated filter (issue #7 - inactive unless composing,
        // see the filter-mode check below). Switching into Query Events/
        // Commands for the first time (or after a stage reset with
        // nothing loaded) kicks off that tab's own type-list fetch - `r`
        // refreshes it manually afterward.
        let editing_free_text = (self.tab == Tab::Commands
            && matches!(self.commands.stage, CommandsStage::Form { .. }))
            || self.tab == Tab::Projections
            || (self.tab == Tab::LiveEvents && self.live_events_filter_active);
        if !editing_free_text {
            match key.code {
                KeyCode::Char('1') => return self.tab = Tab::LiveEvents,
                KeyCode::Char('2') => {
                    self.tab = Tab::QueryEvents;
                    if self.query_events.types.is_empty() && !self.query_events.types_loading {
                        self.fetch_event_types();
                    }
                    return;
                }
                KeyCode::Char('3') => {
                    self.tab = Tab::Commands;
                    if let CommandsStage::Picking { types, loading, .. } = &self.commands.stage {
                        if types.is_empty() && !*loading {
                            self.fetch_command_types();
                        }
                    }
                    return;
                }
                KeyCode::Char('4') => return self.tab = Tab::Projections,
                _ => {}
            }
        }

        match self.tab {
            Tab::LiveEvents => self.handle_live_events_key(key),
            Tab::QueryEvents => self.handle_query_events_key(key),
            Tab::Commands => self.handle_commands_key(key),
            Tab::Projections => self.handle_projections_key(key),
        }
    }

    // --- Live Events: `/`-gated substring filter (Codeberg issue #7) ---

    /// `/` enters filter-compose mode (matching `less`/`vim`'s own
    /// well-known search key) rather than always-on free text - the
    /// latter would reopen the exact digit-tab-switch conflict issue #8
    /// already found and fixed for Commands (a filter like "42" would
    /// jump to Query Events mid-type). Backspace/`Char` edit the filter
    /// while active; `Enter`/`Esc` (handled at the top of `handle_key`)
    /// both exit compose mode back to normal browsing - `Enter` doesn't
    /// need to "run" anything since filtering is live, client-side, no
    /// round trip (`ui::draw_live_events` reads `live_events_filter`
    /// directly on every frame).
    fn handle_live_events_key(&mut self, key: KeyEvent) {
        if !self.live_events_filter_active {
            if key.code == KeyCode::Char('/') {
                self.live_events_filter_active = true;
            }
            return;
        }
        match key.code {
            KeyCode::Enter => self.live_events_filter_active = false,
            KeyCode::Backspace => self.live_events_filter.backspace(),
            KeyCode::Char(c) => self.live_events_filter.push(c),
            _ => {}
        }
    }

    // --- Query Events: a checklist over real eventTypes, replacing v1's
    //     free-text comma-separated field (Codeberg issue #8) ---

    fn handle_query_events_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Up => {
                self.query_events.list_selected = self.query_events.list_selected.saturating_sub(1);
            }
            KeyCode::Down => {
                let len = self.query_events.types.len();
                if len > 0 && self.query_events.list_selected + 1 < len {
                    self.query_events.list_selected += 1;
                }
            }
            KeyCode::Char(' ') => {
                let idx = self.query_events.list_selected;
                if idx < self.query_events.types.len() && !self.query_events.checked.remove(&idx) {
                    self.query_events.checked.insert(idx);
                }
            }
            KeyCode::Char('r') => self.fetch_event_types(),
            KeyCode::Enter => self.submit_query_events(),
            _ => {}
        }
    }

    fn fetch_event_types(&mut self) {
        self.query_events.types_loading = true;
        self.query_events.types_error = None;
        let client = self.client.clone();
        let tx = self.events_tx.clone();
        let bounded_context = self.bounded_context.clone();
        tokio::spawn(async move {
            let result = client
                .request(
                    "query($bc: String!) { eventTypes(boundedContext: $bc) { name schema } }",
                    serde_json::json!({ "bc": bounded_context }),
                )
                .await;
            let _ = tx.send(AppEvent::QueryEventsTypesResult(result));
        });
    }

    fn submit_query_events(&mut self) {
        let event_types: Vec<Value> = self
            .query_events
            .checked
            .iter()
            .filter_map(|&i| self.query_events.types.get(i))
            .map(|t| Value::String(t.name.clone()))
            .collect();
        if event_types.is_empty() {
            self.query_events.query_error =
                Some("select at least one event type (Space to toggle)".into());
            return;
        }
        self.query_events.query_loading = true;
        self.query_events.query_error = None;
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

    // --- Commands: pick a real, registered command type, then fill in
    //     the form generated from its own schema (Codeberg issue #8) ---

    fn handle_commands_key(&mut self, key: KeyEvent) {
        match &self.commands.stage {
            CommandsStage::Picking { .. } => self.handle_commands_picking_key(key),
            CommandsStage::Form { .. } => self.handle_commands_form_key(key),
        }
    }

    fn handle_commands_picking_key(&mut self, key: KeyEvent) {
        let CommandsStage::Picking {
            types,
            list_selected,
            ..
        } = &mut self.commands.stage
        else {
            return;
        };
        match key.code {
            KeyCode::Up => *list_selected = list_selected.saturating_sub(1),
            KeyCode::Down => {
                if !types.is_empty() && *list_selected + 1 < types.len() {
                    *list_selected += 1;
                }
            }
            KeyCode::Enter => self.pick_command_type(),
            KeyCode::Char('r') => self.fetch_command_types(),
            _ => {}
        }
    }

    fn fetch_command_types(&mut self) {
        if let CommandsStage::Picking { loading, error, .. } = &mut self.commands.stage {
            *loading = true;
            *error = None;
        }
        let client = self.client.clone();
        let tx = self.events_tx.clone();
        let bounded_context = self.bounded_context.clone();
        tokio::spawn(async move {
            let result = client
                .request(
                    "query($bc: String!) { commandTypes(boundedContext: $bc) { name schema } }",
                    serde_json::json!({ "bc": bounded_context }),
                )
                .await;
            let _ = tx.send(AppEvent::CommandTypesResult(result));
        });
    }

    /// Enter on a picked row - generates the form from that type's own
    /// schema (`form::fields_from_schema`, no extra round trip: the
    /// schema already came back with the type list) and carries `types`/
    /// `list_selected` into `Form` too, so `commands_form_back_to_picking`
    /// can restore the picker without a refetch.
    fn pick_command_type(&mut self) {
        let CommandsStage::Picking {
            types,
            list_selected,
            ..
        } = &self.commands.stage
        else {
            return;
        };
        let Some(picked) = types.get(*list_selected) else {
            return;
        };
        let type_name = picked.name.clone();
        let fields = form::fields_from_schema(&picked.schema);
        let types = types.clone();
        let list_selected = *list_selected;
        self.commands.stage = CommandsStage::Form {
            types,
            list_selected,
            type_name,
            fields,
            focus: 0,
            result: None,
            error: None,
            loading: false,
        };
    }

    fn commands_form_back_to_picking(&mut self) {
        let CommandsStage::Form {
            types,
            list_selected,
            ..
        } = &self.commands.stage
        else {
            return;
        };
        self.commands.stage = CommandsStage::Picking {
            types: types.clone(),
            list_selected: *list_selected,
            loading: false,
            error: None,
        };
    }

    fn handle_commands_form_key(&mut self, key: KeyEvent) {
        let CommandsStage::Form { fields, focus, .. } = &mut self.commands.stage else {
            return;
        };
        match key.code {
            KeyCode::Tab => {
                if !fields.is_empty() {
                    *focus = (*focus + 1) % fields.len();
                }
            }
            KeyCode::Enter => self.submit_command(),
            KeyCode::Backspace => {
                if let Some(field) = fields.get_mut(*focus) {
                    match &mut field.widget {
                        form::Widget::Text(s)
                        | form::Widget::Number(s)
                        | form::Widget::RawJson(s) => {
                            s.pop();
                        }
                        form::Widget::Bool(_) => {}
                    }
                }
            }
            // Space toggles a focused boolean field; for every other
            // widget it's an ordinary character (a raw-JSON or string
            // value may legitimately contain one).
            KeyCode::Char(' ') => {
                if let Some(field) = fields.get_mut(*focus) {
                    match &mut field.widget {
                        form::Widget::Bool(b) => *b = !*b,
                        form::Widget::Text(s)
                        | form::Widget::Number(s)
                        | form::Widget::RawJson(s) => {
                            s.push(' ');
                        }
                    }
                }
            }
            KeyCode::Char(c) => {
                if let Some(field) = fields.get_mut(*focus) {
                    if let form::Widget::Text(s)
                    | form::Widget::Number(s)
                    | form::Widget::RawJson(s) = &mut field.widget
                    {
                        s.push(c);
                    }
                }
            }
            _ => {}
        }
    }

    fn submit_command(&mut self) {
        let CommandsStage::Form {
            type_name,
            fields,
            loading,
            error,
            ..
        } = &mut self.commands.stage
        else {
            return;
        };
        let payload = match form::assemble_payload(fields) {
            Ok(payload) => payload,
            Err(e) => {
                *error = Some(e);
                return;
            }
        };
        *loading = true;
        *error = None;
        let type_name = type_name.clone();
        let payload_str = payload.to_string();
        let client = self.client.clone();
        let tx = self.events_tx.clone();
        let bounded_context = self.bounded_context.clone();
        tokio::spawn(async move {
            let result = client
                .request(
                    "mutation($bc: String!, $type: String!, $payload: String!) { \
                        submitCommand(boundedContext: $bc, commandTypeName: $type, payload: $payload) { \
                            accepted rejectionReason rejectionKind triggeredEventSequences \
                            matchingEvents { sequence eventTypeName payload } \
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
                    ProjectionField::Key => ProjectionField::WaitForSequence,
                    ProjectionField::WaitForSequence => ProjectionField::Name,
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
            ProjectionField::WaitForSequence => &mut self.projections.wait_for_sequence,
        }
    }

    fn submit_projection(&mut self) {
        let name = self.projections.name.value.trim().to_string();
        if name.is_empty() {
            self.projections.error = Some("enter a projection name".into());
            return;
        }
        let key = self.projections.key.value.trim().to_string();
        let wait_for_sequence_str = self.projections.wait_for_sequence.value.trim().to_string();
        let wait_for_sequence = if wait_for_sequence_str.is_empty() {
            None
        } else {
            match wait_for_sequence_str.parse::<i64>() {
                Ok(seq) => Some(seq),
                Err(_) => {
                    self.projections.error = Some(format!(
                        "{wait_for_sequence_str:?} is not a valid sequence number"
                    ));
                    return;
                }
            }
        };
        self.projections.loading = true;
        self.projections.error = None;
        let client = self.client.clone();
        let tx = self.events_tx.clone();
        let bounded_context = self.bounded_context.clone();
        tokio::spawn(async move {
            let key = if key.is_empty() {
                None
            } else {
                Some(key.as_str())
            };
            let result = crate::projection_query::fetch(
                &client,
                &bounded_context,
                &name,
                key,
                wait_for_sequence,
            )
            .await;
            let _ = tx.send(AppEvent::ProjectionResult(result));
        });
    }
}
