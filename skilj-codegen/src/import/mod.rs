//! Imports one bounded context from a model made with an Event Modeling tool
//! into a `.skilj.toml` (docs/architecture.md §243, §253,
//! `docs/modeling-tools.md`).
//!
//! `.skilj.toml` is the one format every tool imports to: [`crate::generate`]
//! reads only that. Each [`Source`] has a reader and a naming convention, and
//! all of them share what happens after reading:
//!
//! - an event's or command's name is its PascalCase `NAME`;
//! - a field marked as an id is a DCB tag with the field name as its key;
//! - a field marked as personal data is kept, with a note to declare it
//!   sensitive or blind;
//! - `String`, `UUID`, `Date` and `DateTime` become `string`, `Int` and `Long`
//!   `i64`, `Boolean` `bool`;
//! - a type with a field `.skilj.toml` can't express (a list, nested fields, an
//!   optional field, a `Double`, `Decimal`, `Number` or `Custom`) is left out
//!   with a note saying why, to hand-write instead;
//! - a rejection written as `<kind>: <reason>` has that kind; one without a
//!   kind in front has the generic kind [`GENERIC_REJECTION_KIND`]. The kinds
//!   are a comment above the command, for its `decide_*` function;
//! - a command whose issuer is the REST trigger endpoint gets
//!   `rest_trigger_allowed = true`;
//! - a type without an id field gets a note that it has no tags.
//!
//! The result is validated like any `.skilj.toml` before it is returned, so an
//! import either generates or names its problems. Nothing is written back to
//! the tool.

mod evml;
mod slices;

use crate::spec::{self, BoundedContextSpec};
use crate::Error;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

/// The kind of a rejection that names none.
pub const GENERIC_REJECTION_KIND: &str = "rejected";

/// The endpoint an issuer names to make its command `rest_trigger_allowed`.
pub const REST_TRIGGER_ENDPOINT: &str = "POST /v1/commands/trigger";

/// Where a model comes from: which reader and which naming convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Source {
    /// What `emhcl export` writes from an [Event Modeling HCL](https://event-modeling-hcl.github.io)
    /// model, named by emhcl labels (`docs/event-modeling-hcl.md`).
    Emhcl,
    /// A document in the [Event Modeling Specification](https://github.com/dilgerma/event-modeling-spec)
    /// JSON schema, named by element titles and contexts: what Nebulit's Event
    /// Modeling app for Miro exports, and what the prooph board route writes
    /// (`docs/modeling-tools.md`).
    EmJson,
    /// A model file in the text language of
    /// [event-modeling-tools](https://github.com/lgazo/event-modeling-tools)
    /// (`.evml`, also Mermaid's `eventmodeling` diagram).
    Evml,
}

impl Source {
    /// Every source, in the order `skilj-codegen import --help` lists them.
    pub const ALL: [Source; 3] = [Source::Emhcl, Source::EmJson, Source::Evml];

    /// The name `skilj-codegen import --from` takes.
    pub fn name(self) -> &'static str {
        match self {
            Source::Emhcl => "emhcl",
            Source::EmJson => "em-json",
            Source::Evml => "evml",
        }
    }

    /// The source named `name`, as `skilj-codegen import --from` takes it.
    pub fn from_name(name: &str) -> Option<Source> {
        Source::ALL.into_iter().find(|s| s.name() == name)
    }

    fn header(self) -> &'static str {
        match self {
            // Unchanged since the emhcl_import example, so an import made
            // with it and one made with the subcommand are the same file.
            Source::Emhcl => {
                "Imported from an Event Modeling HCL model by skilj_codegen::emhcl::import."
            }
            Source::EmJson => {
                "Imported from Event Modeling JSON by skilj-codegen import --from em-json."
            }
            Source::Evml => "Imported from an .evml model by skilj-codegen import --from evml.",
        }
    }
}

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

/// Imports the events and commands of `bounded_context` from `input`, a model
/// read as `source` says, into a `.skilj.toml`.
///
/// Refused as [`Error::Json`] or [`Error::Evml`] when `input` isn't in the
/// source's format, and as [`Error::Invalid`] when `bounded_context` isn't a
/// skilj bounded context name, has no events in the model, or the result
/// wouldn't generate (two commands with one name, say).
pub fn import(source: Source, input: &str, bounded_context: &str) -> Result<Import, Error> {
    if !spec::is_bounded_context_name(bounded_context) {
        return Err(Error::Invalid(vec![format!(
            "`{bounded_context}` is not a skilj bounded context name: a lowercase letter, then lowercase letters, digits or `_`, at most 40 characters"
        )]));
    }
    let model = match source {
        Source::Emhcl => slices::read(input, bounded_context, slices::Naming::Labels)?,
        Source::EmJson => slices::read(input, bounded_context, slices::Naming::Titles)?,
        Source::Evml => evml::read(input, bounded_context)?,
    };
    let skilj_toml = write_toml(source.header(), bounded_context, &model);
    let parsed: BoundedContextSpec = toml::from_str(&skilj_toml).map_err(Error::Toml)?;
    let problems = spec::validate(&parsed);
    if !problems.is_empty() {
        return Err(Error::Invalid(problems));
    }
    Ok(Import {
        skilj_toml,
        notes: model.notes,
    })
}

/// What a reader found, ready to write.
struct Model {
    events: Vec<TypeOut>,
    commands: Vec<TypeOut>,
    notes: Vec<String>,
}

/// The error for a bounded context the model has no events of.
fn no_events(bounded_context: &str, found: BTreeSet<String>) -> Error {
    let found: Vec<_> = found.into_iter().collect();
    Error::Invalid(vec![format!(
        "the model has no events of bounded context `{bounded_context}`; it has events of: {}",
        if found.is_empty() {
            "none".to_string()
        } else {
            found.join(", ")
        }
    )])
}

/// One event or command type, ready to write.
struct TypeOut {
    /// Names the model element in the comment above the type, and in notes.
    source_id: String,
    name: String,
    fields: Vec<(String, &'static str)>,
    tags: BTreeMap<String, String>,
    rest_trigger_allowed: bool,
    rejections: BTreeSet<String>,
}

/// A field as a reader found it, in the Event Modeling Specification's terms.
struct FieldIn<'a> {
    name: String,
    /// `String`, `Int`, `Boolean`, ... as the specification names them.
    ty: &'a str,
    list: bool,
    nested: bool,
    optional: bool,
    id: bool,
    pii: bool,
}

/// Converts one event or command, or returns `None` with a note saying why
/// it has to be hand-written.
/// `id_marker` names how the source marks an id field, for the note on a
/// type without one.
fn convert(
    source_id: &str,
    name: String,
    fields: Vec<FieldIn<'_>>,
    id_marker: &str,
    notes: &mut Vec<String>,
) -> Option<TypeOut> {
    let mut out_fields = Vec::new();
    let mut tags = BTreeMap::new();
    let mut unsupported = Vec::new();
    for field in fields {
        if !is_label(&field.name) {
            unsupported.push(format!("`{}` is not a snake_case field name", field.name));
            continue;
        }
        let ty = match field.ty {
            "String" | "UUID" | "Date" | "DateTime" => Some("string"),
            "Int" | "Long" => Some("i64"),
            "Boolean" => Some("bool"),
            _ => None,
        };
        let reason = match (ty, field.list, field.nested, field.optional) {
            (None, ..) => Some(format!("`{}` has type {}", field.name, field.ty)),
            (_, true, ..) => Some(format!("`{}` is a list", field.name)),
            (_, _, true, _) => Some(format!("`{}` has subfields", field.name)),
            (_, _, _, true) => Some(format!("`{}` is optional", field.name)),
            (Some(ty), false, false, false) => {
                out_fields.push((field.name.clone(), ty));
                None
            }
        };
        if let Some(reason) = reason {
            unsupported.push(reason);
            continue;
        }
        if field.id {
            tags.insert(field.name.clone(), field.name.clone());
        }
        if field.pii {
            notes.push(format!(
                "{source_id}: `{}` is personal data; add a `sensitive` entry for it, or a `[[blind_tag]]` if a rule matches on it",
                field.name
            ));
        }
    }
    if !unsupported.is_empty() {
        notes.push(format!(
            "{source_id}: left out, hand-write it: {}",
            unsupported.join(", ")
        ));
        return None;
    }
    if tags.is_empty() {
        notes.push(format!(
            "{source_id}: no field has {id_marker}, so it has no DCB tags"
        ));
    }
    Some(TypeOut {
        source_id: source_id.to_string(),
        name,
        fields: out_fields,
        tags,
        rest_trigger_allowed: false,
        rejections: BTreeSet::new(),
    })
}

/// The `kind` of a `<kind>: <reason>` rejection, when it is a lowercase
/// `snake_case` label; the generic kind otherwise.
fn rejection_kind(error: &str) -> &str {
    let Some((kind, reason)) = error.split_once(':') else {
        return GENERIC_REJECTION_KIND;
    };
    if is_label(kind) && !reason.trim().is_empty() {
        kind
    } else {
        GENERIC_REJECTION_KIND
    }
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

/// `Money Deposited` or `money-deposited` -> `MoneyDeposited`: every run of
/// ASCII letters and digits, its first letter upper case.
fn pascal_case_title(title: &str) -> String {
    title
        .split(|c: char| !c.is_ascii_alphanumeric())
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

/// `accountId`, `Account Id` or `account-id` -> `account_id`.
fn snake_case(name: &str) -> String {
    let mut out = String::new();
    let mut previous_lower = false;
    for c in name.chars() {
        if c.is_ascii_uppercase() {
            if previous_lower && !out.ends_with('_') {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
            previous_lower = false;
        } else if c.is_ascii_alphanumeric() {
            out.push(c);
            previous_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
        } else {
            if !out.is_empty() && !out.ends_with('_') {
                out.push('_');
            }
            previous_lower = false;
        }
    }
    out.trim_end_matches('_').to_string()
}

/// A lowercase `snake_case` label: a lowercase letter, then lowercase
/// letters, digits or `_`.
fn is_label(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// A PascalCase type name: an upper case letter, then letters and digits.
fn is_type_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_uppercase()) && chars.all(|c| c.is_ascii_alphanumeric())
}

fn write_toml(header: &str, bounded_context: &str, model: &Model) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# {header}");
    out.push_str(
        "# Command decisions stay hand-written: one decide_<name> function per command.\n",
    );
    for note in &model.notes {
        let _ = writeln!(out, "# NOTE: {}", note.replace('\n', " "));
    }
    let _ = writeln!(out, "\nbounded_context = \"{bounded_context}\"");
    for event in &model.events {
        out.push_str("\n[[event_type]]\n");
        write_type(&mut out, event);
    }
    for command in &model.commands {
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
    let _ = writeln!(out, "# From {}", item.source_id.replace('\n', " "));
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
    fn names_follow_the_conventions() {
        assert_eq!(pascal_case("money_deposited"), "MoneyDeposited");
        assert_eq!(pascal_case("open_v2_course"), "OpenV2Course");
        assert_eq!(pascal_case_title("Money Deposited"), "MoneyDeposited");
        assert_eq!(pascal_case_title("money-deposited"), "MoneyDeposited");
        assert_eq!(pascal_case_title("  Add item (v2) "), "AddItemV2");
        assert_eq!(snake_case("accountId"), "account_id");
        assert_eq!(snake_case("account_id"), "account_id");
        assert_eq!(snake_case("Account Id"), "account_id");
        assert_eq!(snake_case("IBAN"), "iban");
        assert_eq!(snake_case("line2Total"), "line2_total");
    }

    #[test]
    fn rejection_kinds_need_a_label_and_a_reason() {
        assert_eq!(
            rejection_kind("insufficient_funds: withdrawal exceeds the balance"),
            "insufficient_funds"
        );
        assert_eq!(rejection_kind("Insufficient funds"), "rejected");
        assert_eq!(rejection_kind("Insufficient funds: no"), "rejected");
        assert_eq!(rejection_kind("insufficient_funds:"), "rejected");
    }

    #[test]
    fn every_source_has_a_unique_name() {
        for source in Source::ALL {
            assert_eq!(Source::from_name(source.name()), Some(source));
        }
        assert_eq!(Source::from_name("qlerify"), None);
    }
}
