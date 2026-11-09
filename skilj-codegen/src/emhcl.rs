//! Imports a model made with [Event Modeling HCL](https://event-modeling-hcl.github.io)
//! into a `.skilj.toml` (docs/architecture.md §243,
//! `docs/event-modeling-hcl.md`).
//!
//! The input is what `emhcl export model.em.hcl -o model.json` writes: the
//! slice-based Event Modeling JSON. That format carries everything this
//! import needs - element ids, fields with their types and key flags,
//! dependencies, API endpoints and scenarios - and none of what it drops
//! (lifecycles, aggregate identities, scenario cases) matters here. Reading
//! the JSON rather than the HCL keeps an HCL parser and the language's own
//! validation out of this crate: validate with `emhcl` first.
//!
//! The naming convention, the whole of it:
//!
//! - The bounded context's emhcl label is the skilj bounded context name;
//!   both are lowercase `snake_case`.
//! - An event's or command's emhcl label is its `NAME` in PascalCase:
//!   `money_deposited` becomes `MoneyDeposited`.
//! - A field with `id_attribute = true` is a DCB tag, keyed by its name
//!   without a trailing `_id`: `account_id` becomes `account = "account_id"`.
//!   An aggregate isn't needed and isn't read.
//! - A command belongs to the bounded context of the events it emits.
//! - A command issued by a screen whose `api_endpoint` is
//!   `POST /v1/commands/trigger` gets `rest_trigger_allowed = true`.
//! - A scenario's `then { error = "<kind>: <reason>" }` is a rejection with
//!   that `kind` and `reason`. The kinds are written as a comment above the
//!   command, to implement in its `decide_*` function.
//!
//! A type with a field this format can't express (a list, nested fields, an
//! optional field, a `Double`, `Decimal` or `Custom`) is left out with a note
//! saying why, to hand-write instead - the same rule `.skilj.toml` itself
//! follows. Personal data (`pii = true`) is kept, with a note: choosing its
//! subject is a decision the model doesn't record.

use crate::spec::{self, BoundedContextSpec};
use crate::Error;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

/// The endpoint a screen names to make its command `rest_trigger_allowed`.
pub const REST_TRIGGER_ENDPOINT: &str = "POST /v1/commands/trigger";

/// The result of [`import`].
#[derive(Debug)]
#[non_exhaustive]
pub struct Import {
    /// The `.skilj.toml` source. [`crate::generate`] accepts it as it is.
    pub skilj_toml: String,
    /// What the import left out or couldn't decide, one line each. The same
    /// lines head `skilj_toml` as comments.
    pub notes: Vec<String>,
}

#[derive(Deserialize)]
struct Document {
    #[serde(default)]
    slices: Vec<Slice>,
}

#[derive(Deserialize)]
struct Slice {
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
    context: Option<String>,
    #[serde(default)]
    fields: Vec<Field>,
    #[serde(default)]
    dependencies: Vec<Dependency>,
    #[serde(default)]
    api_endpoint: Option<String>,
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
    id: String,
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

/// One event or command type, ready to write.
struct TypeOut {
    source_id: String,
    name: String,
    fields: Vec<(String, &'static str)>,
    tags: BTreeMap<String, String>,
    rest_trigger_allowed: bool,
    rejections: BTreeSet<String>,
}

/// Imports the events and commands of `bounded_context` from `emhcl export`
/// JSON into a `.skilj.toml`.
///
/// Refused as [`Error::Json`] when `json` isn't the export format, and as
/// [`Error::Invalid`] when `bounded_context` isn't a skilj bounded context
/// name, has no events in the model, or the result wouldn't generate (two
/// commands with one label in different workflows, say).
pub fn import(json: &str, bounded_context: &str) -> Result<Import, Error> {
    let document: Document = serde_json::from_str(json).map_err(|e| Error::Json(e.to_string()))?;
    if !spec::is_bounded_context_name(bounded_context) {
        return Err(Error::Invalid(vec![format!(
            "`{bounded_context}` is not a skilj bounded context name: a lowercase letter, then lowercase letters, digits or `_`, at most 40 characters"
        )]));
    }
    let event_prefix = format!("event.{bounded_context}.");
    let mut notes = Vec::new();

    let mut seen = BTreeSet::new();
    let mut events = Vec::new();
    let mut contexts = BTreeSet::new();
    for element in document.slices.iter().flat_map(|s| &s.events) {
        if let Some(context) = element
            .id
            .strip_prefix("event.")
            .and_then(|r| r.split('.').next())
        {
            contexts.insert(context.to_string());
        }
        let external = element.context.as_deref() == Some("EXTERNAL");
        if external || !element.id.starts_with(&event_prefix) || !seen.insert(element.id.clone()) {
            continue;
        }
        if let Some(out) = convert(element, &mut notes) {
            events.push(out);
        }
    }
    if seen.is_empty() {
        let found: Vec<_> = contexts.into_iter().collect();
        return Err(Error::Invalid(vec![format!(
            "the model has no events of bounded context `{bounded_context}`; it has events of: {}",
            if found.is_empty() {
                "none".to_string()
            } else {
                found.join(", ")
            }
        )]));
    }

    let mut commands = Vec::new();
    let mut seen = BTreeSet::new();
    for slice in &document.slices {
        for element in &slice.commands {
            let emits_here = element
                .dependencies
                .iter()
                .any(|d| d.direction == "OUTBOUND" && d.id.starts_with(&event_prefix));
            if !emits_here || !seen.insert(element.id.clone()) {
                continue;
            }
            let Some(mut out) = convert(element, &mut notes) else {
                continue;
            };
            out.rest_trigger_allowed = slice.screens.iter().any(|screen| {
                screen
                    .api_endpoint
                    .as_deref()
                    .is_some_and(|e| e.trim() == REST_TRIGGER_ENDPOINT)
                    && screen
                        .dependencies
                        .iter()
                        .any(|d| d.direction == "OUTBOUND" && d.id == element.id)
            });
            commands.push(out);
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
        let Some(command) = commands.iter_mut().find(|c| c.source_id == command_id) else {
            continue;
        };
        for step in specification.then.iter().filter(|s| s.ty == "SPEC_ERROR") {
            match rejection_kind(&step.title) {
                Some(kind) => {
                    command.rejections.insert(kind.to_string());
                }
                None => notes.push(format!(
                    "{}: the error {:?} doesn't follow `<kind>: <reason>`, so its rejection kind is unknown",
                    specification.id, step.title
                )),
            }
        }
    }

    let skilj_toml = write_toml(bounded_context, &events, &commands, &notes);
    let parsed: BoundedContextSpec = toml::from_str(&skilj_toml).map_err(Error::Toml)?;
    let problems = spec::validate(&parsed);
    if !problems.is_empty() {
        return Err(Error::Invalid(problems));
    }
    Ok(Import { skilj_toml, notes })
}

/// The `kind` of a `<kind>: <reason>` error, when it is a lowercase
/// `snake_case` label.
fn rejection_kind(error: &str) -> Option<&str> {
    let (kind, reason) = error.split_once(':')?;
    let mut chars = kind.chars();
    let label = chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    (label && !reason.trim().is_empty()).then_some(kind)
}

/// `money_deposited` -> `MoneyDeposited`.
fn pascal_case(label: &str) -> String {
    label
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// `account_id` -> `account`; a name without `_id`, or only `_id`, stays.
fn tag_key(field: &str) -> &str {
    match field.strip_suffix("_id") {
        Some(key) if !key.is_empty() => key,
        _ => field,
    }
}

fn is_label(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Converts one event or command, or returns `None` with a note saying why
/// it has to be hand-written.
fn convert(element: &Element, notes: &mut Vec<String>) -> Option<TypeOut> {
    let label = element.id.rsplit('.').next().unwrap_or_default();
    if !is_label(label) {
        notes.push(format!(
            "{}: left out, its id doesn't end in an emhcl label",
            element.id
        ));
        return None;
    }
    let mut fields = Vec::new();
    let mut tags = BTreeMap::new();
    let mut unsupported = Vec::new();
    for field in &element.fields {
        if !is_label(&field.name) {
            unsupported.push(format!("`{}` is not a snake_case field name", field.name));
            continue;
        }
        let ty = match field.ty.as_str() {
            "String" | "UUID" | "Date" | "DateTime" => Some("string"),
            "Int" | "Long" => Some("i64"),
            "Boolean" => Some("bool"),
            _ => None,
        };
        let list = field.cardinality.as_deref() == Some("List");
        let reason = match (ty, list, field.subfields.is_empty(), field.optional) {
            (None, ..) => Some(format!("`{}` has type {}", field.name, field.ty)),
            (_, true, ..) => Some(format!("`{}` is a list", field.name)),
            (_, _, false, _) => Some(format!("`{}` has subfields", field.name)),
            (_, _, _, true) => Some(format!("`{}` is optional", field.name)),
            (Some(ty), false, true, false) => {
                fields.push((field.name.clone(), ty));
                None
            }
        };
        if let Some(reason) = reason {
            unsupported.push(reason);
            continue;
        }
        if field.id_attribute {
            let key = tag_key(&field.name);
            let key = if tags.contains_key(key) {
                field.name.as_str()
            } else {
                key
            };
            tags.insert(key.to_string(), field.name.clone());
        }
        if field.pii {
            notes.push(format!(
                "{}: `{}` is personal data; add a `sensitive` entry for it, or a `[[blind_tag]]` if a rule matches on it",
                element.id, field.name
            ));
        }
    }
    if !unsupported.is_empty() {
        notes.push(format!(
            "{}: left out, hand-write it: {}",
            element.id,
            unsupported.join(", ")
        ));
        return None;
    }
    if tags.is_empty() {
        notes.push(format!(
            "{}: no field has `id_attribute = true`, so it has no DCB tags",
            element.id
        ));
    }
    Some(TypeOut {
        source_id: element.id.clone(),
        name: pascal_case(label),
        fields,
        tags,
        rest_trigger_allowed: false,
        rejections: BTreeSet::new(),
    })
}

fn write_toml(
    bounded_context: &str,
    events: &[TypeOut],
    commands: &[TypeOut],
    notes: &[String],
) -> String {
    let mut out = String::new();
    out.push_str("# Imported from an Event Modeling HCL model by skilj_codegen::emhcl::import.\n");
    out.push_str(
        "# Command decisions stay hand-written: one decide_<name> function per command.\n",
    );
    for note in notes {
        let _ = writeln!(out, "# NOTE: {}", note.replace('\n', " "));
    }
    let _ = writeln!(out, "\nbounded_context = \"{bounded_context}\"");
    for event in events {
        out.push_str("\n[[event_type]]\n");
        write_type(&mut out, event);
    }
    for command in commands {
        out.push('\n');
        if !command.rejections.is_empty() {
            let kinds: Vec<_> = command.rejections.iter().map(String::as_str).collect();
            let _ = writeln!(out, "# Rejected as: {}", kinds.join(", "));
        }
        out.push_str("[[command_type]]\n");
        write_type(&mut out, command);
    }
    out
}

fn write_type(out: &mut String, item: &TypeOut) {
    let _ = writeln!(out, "# From {}", item.source_id);
    let _ = writeln!(out, "name = \"{}\"", item.name);
    if item.rest_trigger_allowed {
        out.push_str("rest_trigger_allowed = true\n");
    }
    if item.fields.is_empty() {
        out.push_str("fields = []\n");
    } else {
        out.push_str("fields = [\n");
        for (name, ty) in &item.fields {
            let _ = writeln!(out, "    {{ name = \"{name}\", type = \"{ty}\" }},");
        }
        out.push_str("]\n");
    }
    let tags: Vec<_> = item
        .tags
        .iter()
        .map(|(key, field)| format!("{key} = \"{field}\""))
        .collect();
    if tags.is_empty() {
        out.push_str("tags = {}\n");
    } else {
        let _ = writeln!(out, "tags = {{ {} }}", tags.join(", "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_convention() {
        assert_eq!(pascal_case("money_deposited"), "MoneyDeposited");
        assert_eq!(pascal_case("open_v2_course"), "OpenV2Course");
        assert_eq!(tag_key("account_id"), "account");
        assert_eq!(tag_key("iban"), "iban");
        assert_eq!(tag_key("_id"), "_id");
    }

    #[test]
    fn rejection_kinds_need_a_label_and_a_reason() {
        assert_eq!(
            rejection_kind("insufficient_funds: withdrawal exceeds the balance"),
            Some("insufficient_funds")
        );
        assert_eq!(rejection_kind("Insufficient funds"), None);
        assert_eq!(rejection_kind("Insufficient funds: no"), None);
        assert_eq!(rejection_kind("insufficient_funds:"), None);
    }
}
