//! The declarative shape a `.skilj.toml` file describes - one bounded
//! context's event/command *shapes* only (fields, DCB tags,
//! `rest_trigger_allowed`). Deliberately narrow (Codeberg issue #5's
//! own "narrower cut" - see this crate's own root doc comment and
//! docs/architecture.md §17): `decide()` bodies, `sensitive_fields`,
//! scheduling, `#[requires_role]`, and every `Projection` concept are
//! all real, legitimate parts of the plugin API this format doesn't
//! cover yet - named here as deliberately deferred, not silently
//! missing, matching this project's own "don't build ahead of what's
//! wired" convention.
//!
//! `fields` is an array of `{name, type}` tables, not a TOML map -
//! deliberately, so field order in the generated struct matches the
//! order written in the `.skilj.toml` file. A TOML inline/dotted table
//! (`{ account_id = "string" }`) has no defined ordering once
//! deserialised into a plain map; an array does.

use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
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
pub struct CommandTypeSpec {
    pub name: String,
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub rest_trigger_allowed: bool,
}

#[derive(Debug, Deserialize)]
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
