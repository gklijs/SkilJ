//! Reads slice-based Event Modeling JSON: the [Event Modeling Specification](https://github.com/dilgerma/event-modeling-spec)
//! schema, which `emhcl export` writes and Nebulit's Event Modeling app for
//! Miro exports. The two differ in how elements are named, not in shape:
//!
//! - [`Naming::Labels`]: an `emhcl export`, whose ids carry the emhcl labels
//!   (`event.banking.money_deposited`). Names come from labels, never
//!   titles; the bounded context is the id's second part
//!   (`docs/event-modeling-hcl.md`).
//! - [`Naming::Titles`]: any other document, whose ids are a tool's own
//!   (`3458764513820540932`). A name is the element's title in PascalCase, a
//!   field name its name in `snake_case`, and the bounded context the
//!   element's `modelContext`, or else its slice's `context`, in
//!   `snake_case` (`docs/modeling-tools.md`).
//!
//! In both, an `EXTERNAL` event is left out, a copy of an element
//! (`elementCopy`, pointing at the original through `linkedId`) is the
//! original, and a command belongs to the bounded context of the events it
//! emits.

use super::{
    convert, is_label, is_type_name, no_events, pascal_case, pascal_case_title, rejection_kind,
    snake_case, FieldIn, Model, TypeOut, REST_TRIGGER_ENDPOINT,
};
use crate::Error;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Naming {
    Labels,
    Titles,
}

#[derive(Deserialize)]
struct Document {
    #[serde(default)]
    slices: Vec<Slice>,
}

#[derive(Deserialize)]
struct Slice {
    #[serde(default)]
    context: Option<String>,
    #[serde(default)]
    commands: Vec<Element>,
    #[serde(default)]
    events: Vec<Element>,
    #[serde(default)]
    screens: Vec<Element>,
    #[serde(default)]
    specifications: Vec<Specification>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Element {
    id: String,
    #[serde(default)]
    title: String,
    #[serde(default)]
    context: Option<String>,
    #[serde(default)]
    model_context: Option<String>,
    #[serde(default)]
    fields: Vec<Field>,
    #[serde(default)]
    dependencies: Vec<Dependency>,
    #[serde(default)]
    api_endpoint: Option<String>,
    #[serde(default)]
    element_copy: bool,
    #[serde(default)]
    linked_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Dependency {
    id: String,
    #[serde(rename = "type")]
    direction: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Field {
    name: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(default)]
    cardinality: Option<String>,
    #[serde(default)]
    subfields: Vec<Field>,
    #[serde(default)]
    optional: bool,
    #[serde(default)]
    id_attribute: bool,
    #[serde(default)]
    pii: bool,
}

#[derive(Deserialize)]
struct Specification {
    #[serde(default)]
    when: Vec<Step>,
    #[serde(default)]
    then: Vec<Step>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Step {
    #[serde(default)]
    title: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(default)]
    linked_id: Option<String>,
}

impl Element {
    /// The id of the element this one stands for: the original of a copy.
    fn canonical_id(&self) -> &str {
        match self.linked_id.as_deref() {
            Some(original) if self.element_copy && !original.is_empty() => original,
            _ => &self.id,
        }
    }

    fn external(&self) -> bool {
        self.context.as_deref() == Some("EXTERNAL")
    }

    /// The `snake_case` bounded context a title-named element is in, if any.
    fn bounded_context(&self, slice: &Slice) -> Option<String> {
        [self.model_context.as_deref(), slice.context.as_deref()]
            .into_iter()
            .flatten()
            .map(snake_case)
            .find(|c| !c.is_empty())
    }

    fn emits(&self) -> impl Iterator<Item = &str> {
        self.dependencies
            .iter()
            .filter(|d| d.direction == "OUTBOUND")
            .map(|d| d.id.as_str())
    }
}

pub(super) fn read(json: &str, bounded_context: &str, naming: Naming) -> Result<Model, Error> {
    let document: Document = serde_json::from_str(json).map_err(|e| Error::Json(e.to_string()))?;
    let event_prefix = format!("event.{bounded_context}.");
    let id_marker = match naming {
        Naming::Labels => "`id_attribute = true`",
        Naming::Titles => "`idAttribute: true`",
    };
    let mut notes = Vec::new();

    // A copy's id, to the original's.
    let mut originals: BTreeMap<&str, &str> = BTreeMap::new();
    for slice in &document.slices {
        for element in slice.events.iter().chain(&slice.commands) {
            originals.insert(&element.id, element.canonical_id());
        }
    }
    let original = |id: &str| -> String { originals.get(id).copied().unwrap_or(id).to_string() };

    let mut seen = BTreeSet::new();
    let mut events = Vec::new();
    let mut contexts = BTreeSet::new();
    for slice in &document.slices {
        for element in &slice.events {
            let member = match naming {
                Naming::Labels => {
                    if let Some(context) = element
                        .id
                        .strip_prefix("event.")
                        .and_then(|r| r.split('.').next())
                    {
                        contexts.insert(context.to_string());
                    }
                    element.id.starts_with(&event_prefix)
                }
                Naming::Titles => {
                    let context = element.bounded_context(slice);
                    if !element.external() && !element.element_copy {
                        contexts.insert(context.clone().unwrap_or_else(|| "(no context)".into()));
                    }
                    context.as_deref() == Some(bounded_context)
                }
            };
            if element.external() || !member || !seen.insert(element.canonical_id().to_string()) {
                continue;
            }
            if let Some(out) = convert_element(element, naming, id_marker, &mut notes) {
                events.push(out);
            }
        }
    }
    if seen.is_empty() {
        return Err(no_events(bounded_context, contexts));
    }

    let mut commands = Vec::new();
    let mut seen_commands = BTreeSet::new();
    for slice in &document.slices {
        for element in &slice.commands {
            let emits_here = element.emits().any(|id| match naming {
                Naming::Labels => id.starts_with(&event_prefix),
                Naming::Titles => seen.contains(&original(id)),
            });
            if !emits_here || !seen_commands.insert(element.canonical_id().to_string()) {
                continue;
            }
            let Some(mut out) = convert_element(element, naming, id_marker, &mut notes) else {
                continue;
            };
            let rest = |endpoint: Option<&str>| {
                endpoint.is_some_and(|e| e.trim() == REST_TRIGGER_ENDPOINT)
            };
            out.rest_trigger_allowed = rest(element.api_endpoint.as_deref())
                || slice.screens.iter().any(|screen| {
                    rest(screen.api_endpoint.as_deref())
                        && screen.emits().any(|id| id == element.id)
                });
            commands.push((element.canonical_id().to_string(), out));
        }
    }

    let specifications = document.slices.iter().flat_map(|s| &s.specifications);
    for specification in specifications {
        let Some(command_id) = specification
            .when
            .iter()
            .find(|s| s.ty == "SPEC_COMMAND")
            .and_then(|s| s.linked_id.as_deref())
        else {
            continue;
        };
        let command_id = original(command_id);
        let Some((_, command)) = commands.iter_mut().find(|(id, _)| *id == command_id) else {
            continue;
        };
        for step in specification.then.iter().filter(|s| s.ty == "SPEC_ERROR") {
            command
                .rejections
                .insert(rejection_kind(&step.title).to_string());
        }
    }

    Ok(Model {
        events,
        commands: commands.into_iter().map(|(_, out)| out).collect(),
        notes,
    })
}

/// Converts one element, or returns `None` with a note saying why it has to
/// be hand-written.
fn convert_element(
    element: &Element,
    naming: Naming,
    id_marker: &str,
    notes: &mut Vec<String>,
) -> Option<TypeOut> {
    let (source_id, name) = match naming {
        Naming::Labels => {
            let label = element.id.rsplit('.').next().unwrap_or_default();
            if !is_label(label) {
                notes.push(format!(
                    "{}: left out, its id doesn't end in an emhcl label",
                    element.id
                ));
                return None;
            }
            (element.id.clone(), pascal_case(label))
        }
        Naming::Titles => {
            let source_id = format!("{} ({})", element.title.trim(), element.id);
            let name = pascal_case_title(&element.title);
            if !is_type_name(&name) {
                notes.push(format!(
                    "{source_id}: left out, its title doesn't start with a letter"
                ));
                return None;
            }
            (source_id, name)
        }
    };
    let fields = element
        .fields
        .iter()
        .map(|field| FieldIn {
            name: match naming {
                Naming::Labels => field.name.clone(),
                Naming::Titles => snake_case(&field.name),
            },
            ty: &field.ty,
            list: field.cardinality.as_deref() == Some("List"),
            nested: !field.subfields.is_empty(),
            optional: field.optional,
            id: field.id_attribute,
            pii: field.pii,
        })
        .collect();
    convert(&source_id, name, fields, id_marker, notes)
}
