//! The declarative shape a `.skilj.toml` file describes - one bounded
//! context's event/command *shapes* only (fields, DCB tags, consistency
//! queries, `rest_trigger_allowed`). Deliberately narrow (Codeberg issue #5's
//! own "narrower cut" - see this crate's own root doc comment and
//! [docs/architecture.md §17](../../docs/architecture.md#event-command-codegen-real)): `decide()` bodies, `sensitive_fields`,
//! scheduling, `#[requires_role]`, and every `Projection` concept are
//! all real, legitimate parts of the plugin API this format doesn't
//! cover yet - named here as deliberately deferred, not silently
//! missing, matching this project's own "don't build ahead of what's
//! wired" convention. Every struct below carries `#[serde(deny_unknown_fields)]`
//! so that promise holds on the input side too: a `.skilj.toml` naming
//! one of those deferred fields (or a typo of a covered one, e.g.
//! `taggs`) is a real `toml::de::Error` at `generate()` time, not a
//! silently-ignored key.
//!
//! `fields` is an array of `{name, type}` tables, not a TOML map -
//! deliberately, so field order in the generated struct matches the
//! order written in the `.skilj.toml` file. A TOML inline/dotted table
//! (`{ account_id = "string" }`) has no defined ordering once
//! deserialised into a plain map; an array does.

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundedContextSpec {
    pub bounded_context: String,
    /// `Vec` field name stays plural (idiomatic Rust); the TOML key is
    /// singular (`[[event_type]]`) since each array-of-tables block
    /// declares exactly one event type - the same convention `Cargo.toml`
    /// itself uses (`[[bin]]`/`[[test]]`, not `[[bins]]`/`[[tests]]`).
    #[serde(default, rename = "event_type")]
    pub event_types: Vec<EventTypeSpec>,
    #[serde(default, rename = "command_type")]
    pub command_types: Vec<CommandTypeSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventTypeSpec {
    pub name: String,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    /// `key -> field` - `TagMapping`'s own two-part shape. A `BTreeMap`,
    /// not an ordered map: unlike `fields`, tag order has no effect on
    /// `matching_events`' own union semantics (skilj-tui's own
    /// `references/dcb-tags.md` in the `skilj-event-modeling` skill has
    /// the full explanation of what a tag actually does) - alphabetical
    /// is simply the cheapest deterministic order to emit.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandTypeSpec {
    pub name: String,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub rest_trigger_allowed: bool,
    /// `CommandType::consistency_query()` (docs/architecture.md §198), one
    /// `[[command_type.query]]` table per item. Empty, the default, is no
    /// override: every event carrying any of `tags`.
    #[serde(default)]
    pub query: Vec<QueryItemSpec>,
}

/// One consistency query item. `tags` names keys of the command type's
/// own `tags`, so an item's tag mappings are always among the command
/// type's, as `build()` requires; `latest` is how many of the newest
/// matches, every match when absent.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryItemSpec {
    #[serde(default)]
    pub event_types: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub latest: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: FieldType,
}

/// The scalar leaf shapes the plugin API's own schema rules already
/// require for a `tag_mappings`/`sensitive_fields` target (see the
/// `skilj` skill's own `references/event-type.md`) - not a general type
/// system. A payload field this format can't express (a nested object,
/// an enum, a list) stays a reason to hand-write that one type instead
/// of describing it here, not a gap to widen this enum for casually.
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    I64,
    Bool,
}

/// Rust's keywords (2024 edition, strict and reserved) - a type name may
/// not be one; a field name may, emitted as a raw identifier (`r#type`),
/// which serde and schemars both still name `type` on the wire.
const RUST_KEYWORDS: &[&str] = &[
    "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern",
    "false", "fn", "for", "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut",
    "pub", "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true", "type",
    "unsafe", "use", "where", "while", "abstract", "become", "box", "do", "final", "macro",
    "override", "priv", "try", "typeof", "unsized", "virtual", "yield",
];

/// Keywords that can't be raw identifiers either.
const NOT_RAW_IDENTIFIERS: &[&str] = &["self", "Self", "super", "crate", "_"];

pub(crate) fn is_keyword(name: &str) -> bool {
    RUST_KEYWORDS.contains(&name)
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
        && name != "_"
}

/// skilj-core's `valid_bounded_context_name`, mirrored (this crate
/// doesn't depend on skilj-core): a lowercase ASCII letter, then lowercase
/// letters, digits or `_`, at most 40 characters.
fn is_bounded_context_name(name: &str) -> bool {
    let mut chars = name.chars();
    name.len() <= 40
        && chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Every problem with `spec` that would otherwise surface as a panic in
/// `build.rs`, a compile error inside generated code, or a registration
/// failure at startup - all of them, not just the first, so one build
/// shows everything to fix. Empty when `spec` is sound.
pub(crate) fn validate(spec: &BoundedContextSpec) -> Vec<String> {
    let mut problems = Vec::new();
    if !is_bounded_context_name(&spec.bounded_context) {
        problems.push(format!(
            "bounded_context {:?} must start with a lowercase letter and contain only \
             lowercase letters, digits and `_` (at most 40 characters)",
            spec.bounded_context
        ));
    }

    // Everything `emit` names, in one set: marker types are unit structs,
    // so they share the value namespace with `BOUNDED_CONTEXT` and the
    // `decide_*` functions.
    let mut generated: BTreeMap<String, String> = BTreeMap::new();
    let mut claim = |name: String, what: String, problems: &mut Vec<String>| {
        if let Some(earlier) = generated.get(&name) {
            problems.push(format!(
                "{what} generates `{name}`, which {earlier} already generates"
            ));
        } else {
            generated.insert(name, what);
        }
    };
    claim(
        "BOUNDED_CONTEXT".to_string(),
        "the bounded_context constant".to_string(),
        &mut problems,
    );
    if is_bounded_context_name(&spec.bounded_context) {
        claim(
            crate::emit::event_enum_name(&spec.bounded_context),
            "the bounded context's event enum".to_string(),
            &mut problems,
        );
    }

    let types = spec
        .event_types
        .iter()
        .map(|e| ("event_type", &e.name, &e.fields, &e.tags))
        .chain(
            spec.command_types
                .iter()
                .map(|c| ("command_type", &c.name, &c.fields, &c.tags)),
        );
    for (kind, name, fields, tags) in types {
        let what = format!("{kind} {name:?}");
        if !is_identifier(name) || is_keyword(name) {
            problems.push(format!(
                "{what}: the name must be a Rust identifier (a letter or `_`, then letters, \
                 digits or `_`) and not a keyword"
            ));
            continue;
        }
        claim(name.clone(), what.clone(), &mut problems);
        claim(format!("{name}Payload"), what.clone(), &mut problems);
        if kind == "command_type" {
            claim(
                format!("decide_{}", crate::emit::pascal_case_to_snake_case(name)),
                what.clone(),
                &mut problems,
            );
        }

        let mut field_names = std::collections::BTreeSet::new();
        for field in fields {
            if !is_identifier(&field.name) || NOT_RAW_IDENTIFIERS.contains(&field.name.as_str()) {
                problems.push(format!(
                    "{what}: field {:?} must be a Rust identifier (a letter or `_`, then \
                     letters, digits or `_`; keywords are fine except self/Self/super/crate)",
                    field.name
                ));
            }
            if !field_names.insert(field.name.as_str()) {
                problems.push(format!("{what}: field {:?} is declared twice", field.name));
            }
        }
        for (tag_key, field) in tags {
            if !field_names.contains(field.as_str()) {
                problems.push(format!(
                    "{what}: tag {tag_key:?} maps to field {field:?}, which this type doesn't declare"
                ));
            }
        }
    }
    let event_type_names: std::collections::BTreeSet<&str> =
        spec.event_types.iter().map(|e| e.name.as_str()).collect();
    for command in &spec.command_types {
        let what = format!("command_type {:?}", command.name);
        for (index, item) in command.query.iter().enumerate() {
            let item_what = format!("{what}: query item {}", index + 1);
            if item.event_types.is_empty() && item.tags.is_empty() {
                problems.push(format!(
                    "{item_what} names neither event types nor tags, so it would match every event"
                ));
            }
            if item.latest == Some(0) {
                problems.push(format!(
                    "{item_what} keeps the last 0 matches; leave latest out for every match"
                ));
            }
            for event_type in &item.event_types {
                if !event_type_names.contains(event_type.as_str()) {
                    problems.push(format!(
                        "{item_what} names event type {event_type:?}, which this file doesn't declare"
                    ));
                }
            }
            for tag in &item.tags {
                if !command.tags.contains_key(tag) {
                    problems.push(format!(
                        "{item_what} names tag {tag:?}, which isn't one of the command type's tags"
                    ));
                }
            }
        }
    }
    problems
}
