//! The declarative shape a `.skilj.toml` file describes - one bounded
//! context's event/command *shapes* only (fields, DCB tags, consistency
//! queries, `rest_trigger_allowed`). Deliberately narrow (issue #5's
//! own "narrower cut" - see this crate's own root doc comment and
//! [docs/architecture.md §17](../../docs/architecture.md#event-command-codegen-real)): `decide()` bodies,
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

/// A whole `.skilj.toml` file: one bounded context's declared types.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundedContextSpec {
    /// The bounded context's name, checked against skilj's own naming rule.
    pub bounded_context: String,
    /// `Vec` field name stays plural (idiomatic Rust); the TOML key is
    /// singular (`[[event_type]]`) since each array-of-tables block
    /// declares exactly one event type - the same convention `Cargo.toml`
    /// itself uses (`[[bin]]`/`[[test]]`, not `[[bins]]`/`[[tests]]`).
    #[serde(default, rename = "event_type")]
    pub event_types: Vec<EventTypeSpec>,
    /// One `[[command_type]]` block per command type.
    #[serde(default, rename = "command_type")]
    pub command_types: Vec<CommandTypeSpec>,
    /// One `[[blind_tag]]` per blind tag key (docs/architecture.md §205).
    /// Every type's tag, and every query item's, on one of these keys is
    /// generated as `TagMapping::blind`.
    #[serde(default, rename = "blind_tag")]
    pub blind_tags: Vec<BlindTagSpec>,
    /// Registers the built-in `EventRetracted`, as `SkiljBuilder::retractions`
    /// does (docs/architecture.md §235), so a `retractable` command can emit
    /// one.
    #[serde(default)]
    pub retractions: bool,
    /// Registers the built-in `EventDoubted` and `DoubtLifted`, as
    /// `SkiljBuilder::doubts` does (docs/architecture.md §236). A query item
    /// names them to read them.
    #[serde(default)]
    pub doubts: bool,
}

/// A `BlindTag` impl (docs/architecture.md §205).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlindTagSpec {
    /// The generated marker type's name, e.g. `CustomerEmail`.
    pub name: String,
    /// `BlindTag::TAG_KEY`.
    pub key: String,
    /// `BlindTag::normalize`: `"email"` or `"compact_uppercase"` (the
    /// ready-made `skilj::normalize` functions), or the path of a
    /// `fn(&str) -> String` of your own, e.g. `"crate::customers::handle"`.
    /// Absent: the value as it is.
    pub normalize: Option<String>,
    /// `BlindTag::NORMALIZER_VERSION` - bump only together with a change
    /// to what the normalizer returns.
    #[serde(default = "first_version")]
    pub normalizer_version: u32,
    /// `BlindTag::ERASURE`.
    pub erasure: ErasureSpec,
    /// For `erasure = "with_subject"`: the `SensitiveField.subject_key`
    /// whose `forgetSubject` removes the hashes, and - unless
    /// `subject_tag` says otherwise - the plain tag key carrying the
    /// subject's value.
    pub subject: Option<String>,
    /// For `erasure = "with_subject"`: the plain tag key carrying the
    /// subject's value, when it isn't `subject` itself.
    pub subject_tag: Option<String>,
}

fn first_version() -> u32 {
    1
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
/// `BlindTag::ERASURE`: what forgetting a subject does to the hashes.
#[non_exhaustive]
pub enum ErasureSpec {
    /// `Erasure::Keep`: the hash stays, so the value stays taken.
    Keep,
    /// `Erasure::WithSubject`: forgetting the subject removes the hash,
    /// so the value is free again.
    WithSubject,
}

/// The ready-made normalizers `BlindTagSpec::normalize` can name.
pub(crate) const NORMALIZERS: &[&str] = &["email", "compact_uppercase"];

/// A `SensitiveField` (`sensitive = [{ field, subject_key, subject_field }]`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SensitiveSpec {
    /// The payload field that is encrypted.
    pub field: String,
    /// The namespace of the subject whose key encrypts it, e.g. `customer`.
    pub subject_key: String,
    /// The payload field holding the subject's identifier, never encrypted.
    pub subject_field: String,
}

/// The subject of a `blob`-typed field whose bytes are encrypted
/// (`blobs = [{ field, subject_key, subject_field }]`).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobSpec {
    pub field: String,
    pub subject_key: String,
    pub subject_field: String,
}

/// One `[[event_type]]` block: an `EventType` impl and its payload struct.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventTypeSpec {
    /// The event type's name, also the generated Rust type's.
    pub name: String,
    /// The payload's fields, in the order they are emitted.
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
    /// `sensitive_fields()` - encrypted per subject.
    #[serde(default)]
    pub sensitive: Vec<SensitiveSpec>,
    /// Subjects for its `blob`-typed fields.
    #[serde(default)]
    pub blobs: Vec<BlobSpec>,
}

/// One `[[command_type]]` block: a `CommandType` impl and its payload
/// struct, its `decide()` delegating to a hand-written `decide_*` function.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandTypeSpec {
    /// The command type's name, also the generated Rust type's.
    pub name: String,
    /// The payload's fields, in the order they are emitted.
    #[serde(default)]
    pub fields: Vec<FieldSpec>,
    /// `key -> field`, as `EventTypeSpec::tags`; also the consistency
    /// tags unless `query` or `reads_nothing` says otherwise.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    /// See `EventTypeSpec::sensitive`.
    #[serde(default)]
    pub sensitive: Vec<SensitiveSpec>,
    /// See `EventTypeSpec::blobs`.
    #[serde(default)]
    pub blobs: Vec<BlobSpec>,
    /// Whether REST command tokens may submit this type.
    #[serde(default)]
    pub rest_trigger_allowed: bool,
    /// `CommandType::consistency_query()` (docs/architecture.md §198), one
    /// `[[command_type.query]]` table per item. Empty, the default, is no
    /// override: every event carrying any of `tags`.
    #[serde(default)]
    pub query: Vec<QueryItemSpec>,
    /// `decide()` needs no history: `consistency_query()` is
    /// `QueryItemMapping::nothing()` (docs/architecture.md §201). Refused
    /// together with `query`.
    #[serde(default)]
    pub reads_nothing: bool,
    /// `decide_*` gets `Retractable<...>` events, so with their positions and
    /// tags, and it can emit a retraction. Each query item with event types
    /// also reads the retractions, and can't have `latest`: the newest event
    /// carrying a value may be the retraction of an old claim. Needs
    /// `retractions = true` (docs/architecture.md §235).
    #[serde(default)]
    pub retractable: bool,
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

/// One payload field: `{ name, type }`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldSpec {
    /// The field's name, on the wire and in Rust (`r#` for a keyword).
    pub name: String,
    /// The field's type, `type` in TOML.
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
#[non_exhaustive]
pub enum FieldType {
    /// A `String`.
    String,
    /// An `i64`.
    I64,
    /// A `bool`.
    Bool,
    /// A `skilj_core::blob::BlobRef`: declared as a blob field of its type,
    /// with the subject `blobs` gives it, if any (docs/architecture.md §239).
    Blob,
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
pub(crate) fn is_bounded_context_name(name: &str) -> bool {
    let mut chars = name.chars();
    name.len() <= 40
        && chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// Checks every `[[blind_tag]]`, claiming its marker type's name, and
/// returns each blind key with the `subject_tag` it is erased by, if any.
fn validate_blind_tags(
    spec: &BoundedContextSpec,
    problems: &mut Problems,
    claim: &mut dyn FnMut(String, String, &mut Problems),
) -> BTreeMap<String, Option<String>> {
    let mut keys = BTreeMap::new();
    for blind in &spec.blind_tags {
        let what = format!("blind_tag {:?}", blind.name);
        problems.context = what.clone();
        if !is_identifier(&blind.name) || is_keyword(&blind.name) {
            problems.push(
                "invalid_blind_tag_name",
                format!("{what}: the name must be a Rust identifier and not a keyword"),
            );
        } else {
            claim(blind.name.clone(), what.clone(), problems);
        }
        if let Some(normalize) = &blind.normalize {
            let path = normalize.contains("::") && syn::parse_str::<syn::Path>(normalize).is_ok();
            if !NORMALIZERS.contains(&normalize.as_str()) && !path {
                problems.push(
                    "unknown_normalizer",
                    format!(
                    "{what}: normalize {normalize:?} is neither one of {NORMALIZERS:?} nor the \
                     path of a function (e.g. \"crate::customers::normalize_handle\")"
                ),
                );
            }
        }
        let subject_tag = match blind.erasure {
            ErasureSpec::Keep => {
                if blind.subject.is_some() || blind.subject_tag.is_some() {
                    problems.push(
                        "blind_tag_keep_with_subject",
                        format!("{what}: erasure = \"keep\" takes no subject or subject_tag"),
                    );
                }
                None
            }
            ErasureSpec::WithSubject => match &blind.subject {
                Some(subject) => Some(blind.subject_tag.clone().unwrap_or_else(|| subject.clone())),
                None => {
                    problems.push(
                        "blind_tag_subject_missing",
                        format!(
                            "{what}: erasure = \"with_subject\" needs subject, the subject_key \
                         whose forgetSubject removes the hashes"
                        ),
                    );
                    None
                }
            },
        };
        if keys.insert(blind.key.clone(), subject_tag).is_some() {
            problems.push(
                "duplicate_blind_tag_key",
                format!("{what}: tag key {:?} has another [[blind_tag]]", blind.key),
            );
        }
    }
    for blind in &spec.blind_tags {
        problems.context = format!("blind_tag {:?}", blind.name);
        if let Some(Some(subject_tag)) = keys.get(&blind.key) {
            if keys.contains_key(subject_tag) {
                problems.push(
                    "blind_subject_tag_is_blind",
                    format!(
                        "blind_tag {:?}: its subject_tag {subject_tag:?} is itself a blind tag",
                        blind.name
                    ),
                );
            }
        }
    }
    keys
}

#[derive(Default)]
struct Problems {
    context: String,
    items: Vec<crate::ValidationProblem>,
}

impl Problems {
    /// A problem with the declaration in `context`; `code` is a row of
    /// `codes::CODES`, written out at every call so the code check finds it.
    fn push(&mut self, code: &'static str, message: String) {
        self.items
            .push(crate::problem(code, self.context.clone(), message, None));
    }
}

/// Every problem with `spec` that would otherwise surface as a panic in
/// `build.rs`, a compile error inside generated code, or a registration
/// failure at startup - all of them, not just the first, so one build
/// shows everything to fix. Empty when `spec` is sound.
pub(crate) fn validate_detailed(spec: &BoundedContextSpec) -> Vec<crate::ValidationProblem> {
    let mut problems = Problems {
        context: "bounded_context".into(),
        ..Problems::default()
    };
    if !is_bounded_context_name(&spec.bounded_context) {
        problems.push(
            "invalid_bounded_context_name",
            format!(
                "bounded_context {:?} must start with a lowercase letter and contain only \
             lowercase letters, digits and `_` (at most 40 characters)",
                spec.bounded_context
            ),
        );
    }

    // Everything `emit` names, in one set: marker types are unit structs,
    // so they share the value namespace with `BOUNDED_CONTEXT` and the
    // `decide_*` functions.
    let mut generated: BTreeMap<String, String> = BTreeMap::new();
    let mut claim = |name: String, what: String, problems: &mut Problems| {
        if let Some(earlier) = generated.get(&name) {
            problems.context = what.clone();
            problems.push(
                "duplicate_generated_item",
                format!("{what} generates `{name}`, which {earlier} already generates"),
            );
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

    let blind_keys = validate_blind_tags(spec, &mut problems, &mut |name, what, problems| {
        claim(name, what, problems)
    });

    for name in crate::emit::built_in_event_types(spec) {
        claim(
            name.to_string(),
            "the built-in event type".to_string(),
            &mut problems,
        );
    }

    let blob_types = spec
        .event_types
        .iter()
        .map(|e| {
            (
                "event_type",
                &e.name,
                &e.fields,
                &e.tags,
                &e.sensitive,
                &e.blobs,
            )
        })
        .chain(spec.command_types.iter().map(|c| {
            (
                "command_type",
                &c.name,
                &c.fields,
                &c.tags,
                &c.sensitive,
                &c.blobs,
            )
        }));
    for (kind, name, fields, tags, sensitive, blobs) in blob_types {
        problems.context = format!("{kind} {name:?}");
        let is_blob = |field: &str| {
            fields
                .iter()
                .any(|f| f.name == field && f.ty == FieldType::Blob)
        };
        for blob in blobs {
            if !is_blob(&blob.field) {
                problems.push(
                    "blob_field_not_blob",
                    format!(
                        "{kind} {name:?}: blobs names {:?}, which is no field of type blob",
                        blob.field
                    ),
                );
            }
            if !fields.iter().any(|f| f.name == blob.subject_field) {
                problems.push(
                    "blob_subject_field_not_declared",
                    format!(
                        "{kind} {name:?}: blob subject field {:?} is no field of this type",
                        blob.subject_field
                    ),
                );
            }
        }
        for sensitive in sensitive {
            if is_blob(&sensitive.field) {
                problems.push("sensitive_blob_field", format!(
                    "{kind} {name:?}: blob field {:?} can't be sensitive; give it a subject in blobs",
                    sensitive.field
                ));
            }
        }
        for (tag_key, field) in tags {
            if is_blob(field) {
                problems.push(
                    "tag_on_blob_field",
                    format!("{kind} {name:?}: tag {tag_key:?} can't map blob field {field:?}"),
                );
            }
        }
    }

    let types = spec
        .event_types
        .iter()
        .map(|e| ("event_type", &e.name, &e.fields, &e.tags, &e.sensitive))
        .chain(
            spec.command_types
                .iter()
                .map(|c| ("command_type", &c.name, &c.fields, &c.tags, &c.sensitive)),
        );
    for (kind, name, fields, tags, sensitive) in types {
        let what = format!("{kind} {name:?}");
        problems.context = what.clone();
        if !is_identifier(name) || is_keyword(name) {
            problems.push(
                "invalid_type_name",
                format!(
                    "{what}: the name must be a Rust identifier (a letter or `_`, then letters, \
                 digits or `_`) and not a keyword"
                ),
            );
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
                problems.push(
                    "invalid_field_name",
                    format!(
                        "{what}: field {:?} must be a Rust identifier (a letter or `_`, then \
                     letters, digits or `_`; keywords are fine except self/Self/super/crate)",
                        field.name
                    ),
                );
            }
            if !field_names.insert(field.name.as_str()) {
                problems.push(
                    "duplicate_field",
                    format!("{what}: field {:?} is declared twice", field.name),
                );
            }
        }
        for (tag_key, field) in tags {
            if !field_names.contains(field.as_str()) {
                problems.push("tag_field_not_declared", format!(
                    "{what}: tag {tag_key:?} maps to field {field:?}, which this type doesn't declare"
                ));
            }
        }
        for sensitive in sensitive {
            for field in [&sensitive.field, &sensitive.subject_field] {
                if !field_names.contains(field.as_str()) {
                    problems.push(
                        "sensitive_field_not_declared",
                        format!(
                            "{what}: sensitive field {:?} names field {field:?}, which this type \
                         doesn't declare",
                            sensitive.field
                        ),
                    );
                }
            }
            if let Some((tag_key, _)) = tags
                .iter()
                .find(|(key, field)| **field == sensitive.field && !blind_keys.contains_key(*key))
            {
                problems.push(
                    "tag_on_sensitive_field",
                    format!(
                        "{what}: tag {tag_key:?} stores sensitive field {:?} in the clear; make \
                     {tag_key:?} a [[blind_tag]] to tag it as a keyed hash",
                        sensitive.field
                    ),
                );
            }
        }
        for tag_key in tags.keys() {
            if let Some(Some(subject_tag)) = blind_keys.get(tag_key) {
                if !tags.contains_key(subject_tag) {
                    problems.push(
                        "blind_subject_tag_missing",
                        format!(
                        "{what}: blind tag {tag_key:?} is erased with its subject, so the type \
                         must also tag {subject_tag:?}"
                    ),
                    );
                }
            }
        }
    }
    let built_in_event_types = crate::emit::built_in_event_types(spec);
    for event in &spec.event_types {
        problems.context = format!("event_type {:?}", event.name);
        if crate::emit::BUILT_IN_EVENT_TYPES.contains(&event.name.as_str()) {
            problems.push(
                "reserved_event_type_name",
                format!(
                    "event_type {:?}: this name is reserved for a built-in event type; set \
                 retractions = true or doubts = true to register it",
                    event.name
                ),
            );
        }
    }
    let mut event_type_names: std::collections::BTreeSet<&str> =
        spec.event_types.iter().map(|e| e.name.as_str()).collect();
    event_type_names.extend(built_in_event_types.iter().copied());
    for command in &spec.command_types {
        let what = format!("command_type {:?}", command.name);
        problems.context = what.clone();
        if command.retractable && !spec.retractions {
            problems.push(
                "retractable_without_retractions",
                format!("{what}: retractable needs retractions = true at the top of the file"),
            );
        }
        if command.retractable && command.query.iter().any(|item| item.latest.is_some()) {
            problems.push(
                "retractable_query_latest",
                format!(
                    "{what}: a retractable command folds around retractions, which needs every \
                 match read, so its query items can't have latest"
                ),
            );
        }
        if command.reads_nothing && !command.query.is_empty() {
            problems.push(
                "reads_nothing_with_query",
                format!("{what}: reads_nothing reads no events, so it can't have query items too"),
            );
        }
        for (index, item) in command.query.iter().enumerate() {
            let item_what = format!("{what}: query item {}", index + 1);
            problems.context = item_what.clone();
            if item.event_types.is_empty() && item.tags.is_empty() {
                problems.push(
                    "empty_query_item",
                    format!(
                        "{item_what} names neither event types nor tags, so it matches nothing; \
                     for a command that needs no history, set reads_nothing = true instead"
                    ),
                );
            }
            if item.latest == Some(0) {
                problems.push(
                    "query_latest_zero",
                    format!(
                        "{item_what} keeps the last 0 matches; leave latest out for every match"
                    ),
                );
            }
            for event_type in &item.event_types {
                if !event_type_names.contains(event_type.as_str()) {
                    problems.push("query_event_type_not_declared", format!(
                        "{item_what} names event type {event_type:?}, which this file doesn't declare"
                    ));
                }
            }
            for tag in &item.tags {
                if !command.tags.contains_key(tag) {
                    problems.push(
                        "query_tag_not_declared",
                        format!(
                        "{item_what} names tag {tag:?}, which isn't one of the command type's tags"
                    ),
                    );
                }
            }
        }
    }
    problems.items
}

/// What [`parse`] read: the model, or the field types this format doesn't
/// have, each a problem of its own (`unknown_field_type`) instead of a TOML
/// error, next to any other shape error in the file (`parse`).
pub(crate) enum Parsed {
    Spec(BoundedContextSpec),
    UnknownFieldTypes(Vec<crate::ValidationProblem>),
}

/// The lenient read of a model that only looks at field types: no
/// `deny_unknown_fields`, every key optional, so it reads a file the strict
/// parse refused.
#[derive(Deserialize, Default)]
#[serde(default)]
struct Probe {
    event_type: Vec<ProbeType>,
    command_type: Vec<ProbeType>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ProbeType {
    name: Option<toml::Value>,
    fields: Vec<ProbeField>,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct ProbeField {
    name: Option<toml::Value>,
    #[serde(rename = "type")]
    ty: Option<toml::Spanned<toml::Value>>,
}

/// Why `value` is no [`FieldType`], from `FieldType`'s own deserializer - the
/// one the strict parse uses, so no list of names can drift from the enum.
/// `None` when it is one.
fn unknown_field_type(value: &str) -> Option<String> {
    use serde::de::IntoDeserializer;
    let deserializer: serde::de::value::StrDeserializer<'_, serde::de::value::Error> =
        value.into_deserializer();
    FieldType::deserialize(deserializer)
        .err()
        .map(|error| error.to_string())
}

/// Parses a `.skilj.toml`. A field type this format doesn't have is
/// skilj-codegen's own problem, found with `FieldType`'s deserializer, never
/// by reading the TOML error's text; every other syntax or shape error stays
/// the TOML error, with its position.
pub(crate) fn parse(source: &str) -> Result<Parsed, toml::de::Error> {
    let error = match toml::from_str::<BoundedContextSpec>(source) {
        Ok(spec) => return Ok(Parsed::Spec(spec)),
        Err(error) => error,
    };
    let Ok(probe) = toml::from_str::<Probe>(source) else {
        return Err(error);
    };
    let mut problems = Vec::new();
    let types = probe
        .event_type
        .iter()
        .map(|t| ("event_type", t))
        .chain(probe.command_type.iter().map(|t| ("command_type", t)));
    for (kind, declared) in types {
        let type_name = declared
            .name
            .as_ref()
            .and_then(|name| name.as_str())
            .unwrap_or("?");
        for field in &declared.fields {
            let Some(ty) = &field.ty else { continue };
            let Some(value) = ty.get_ref().as_str() else {
                continue;
            };
            let Some(why) = unknown_field_type(value) else {
                continue;
            };
            let field_name = field
                .name
                .as_ref()
                .and_then(|name| name.as_str())
                .unwrap_or("?");
            problems.push(crate::problem(
                "unknown_field_type",
                format!("{kind} {type_name:?}"),
                format!(
                    "{kind} {type_name:?}: field {field_name:?} has type {value:?}, which this \
                     format doesn't have ({why})"
                ),
                Some(ty.span()),
            ));
        }
    }
    if problems.is_empty() {
        return Err(error);
    }
    // In file order: a `[[command_type]]` may come before an `[[event_type]]`.
    problems.sort_by_key(|problem| problem.span.as_ref().map(|span| span.start));
    // Anything else wrong with the file: the same text with every unknown
    // type replaced by a known one, the whole literal with its quotes, from
    // the last to the first so the earlier spans stay put.
    const KNOWN: &str = "\"string\"";
    let mut replaced = source.to_string();
    for problem in problems.iter().rev() {
        if let Some(span) = &problem.span {
            replaced.replace_range(span.clone(), KNOWN);
        }
    }
    if let Err(other) = toml::from_str::<BoundedContextSpec>(&replaced) {
        let span = other.span().map(|span| {
            let start = original_offset(&problems, KNOWN.len(), span.start);
            let end = original_offset(&problems, KNOWN.len(), span.end).max(start);
            start..end
        });
        problems.push(crate::problem(
            "parse",
            String::new(),
            other.message().to_string(),
            span,
        ));
    }
    Ok(Parsed::UnknownFieldTypes(problems))
}

/// The offset in the original source of `offset` in the text where each
/// problem's span was replaced by `replacement_len` bytes. An offset inside a
/// replacement maps to the start of the value it replaced.
fn original_offset(
    problems: &[crate::ValidationProblem],
    replacement_len: usize,
    offset: usize,
) -> usize {
    let mut shift: isize = 0;
    for span in problems.iter().filter_map(|problem| problem.span.as_ref()) {
        let start = span.start.saturating_add_signed(shift);
        if offset < start {
            break;
        }
        if offset < start + replacement_len {
            return span.start;
        }
        shift += replacement_len as isize - span.len() as isize;
    }
    offset.saturating_add_signed(-shift)
}

#[cfg(test)]
mod tests {
    use crate::codes::CODES;

    const TYPE: &str = "[[event_type]]\nname = \"Placed\"\n";
    const COMMAND: &str = "[[command_type]]\nname = \"Place\"\nfields = [{ name = \"id\", type = \"string\" }]\ntags = { id = \"id\" }\n";

    /// One minimal model per rule: the code it must produce, and nothing else.
    fn cases() -> Vec<(&'static str, String)> {
        let model = |rest: &str| format!("bounded_context = \"shop\"\n{rest}");
        vec![
            ("invalid_bounded_context_name", "bounded_context = \"Shop\"\n".to_string()),
            ("duplicate_generated_item", model("[[event_type]]\nname = \"Place\"\n[[command_type]]\nname = \"Place\"\n")),
            ("invalid_blind_tag_name", model("[[blind_tag]]\nname = \"fn\"\nkey = \"email\"\nerasure = \"keep\"\n")),
            ("unknown_normalizer", model("[[blind_tag]]\nname = \"Email\"\nkey = \"email\"\nnormalize = \"lowercase\"\nerasure = \"keep\"\n")),
            ("blind_tag_keep_with_subject", model("[[blind_tag]]\nname = \"Email\"\nkey = \"email\"\nerasure = \"keep\"\nsubject = \"customer\"\n")),
            ("blind_tag_subject_missing", model("[[blind_tag]]\nname = \"Email\"\nkey = \"email\"\nerasure = \"with_subject\"\n")),
            ("duplicate_blind_tag_key", model("[[blind_tag]]\nname = \"Email\"\nkey = \"email\"\nerasure = \"keep\"\n[[blind_tag]]\nname = \"Mail\"\nkey = \"email\"\nerasure = \"keep\"\n")),
            ("blind_subject_tag_is_blind", model("[[blind_tag]]\nname = \"Email\"\nkey = \"email\"\nerasure = \"with_subject\"\nsubject = \"customer\"\n[[blind_tag]]\nname = \"Customer\"\nkey = \"customer\"\nerasure = \"keep\"\n")),
            ("blind_subject_tag_missing", model("[[blind_tag]]\nname = \"Email\"\nkey = \"email\"\nerasure = \"with_subject\"\nsubject = \"customer\"\n[[event_type]]\nname = \"Claimed\"\nfields = [{ name = \"email\", type = \"string\" }]\ntags = { email = \"email\" }\n")),
            ("blob_field_not_blob", model(&format!("{TYPE}fields = [{{ name = \"doc\", type = \"string\" }}]\nblobs = [{{ field = \"doc\", subject_key = \"c\", subject_field = \"doc\" }}]\n"))),
            ("blob_subject_field_not_declared", model(&format!("{TYPE}fields = [{{ name = \"doc\", type = \"blob\" }}]\nblobs = [{{ field = \"doc\", subject_key = \"c\", subject_field = \"owner\" }}]\n"))),
            ("sensitive_blob_field", model(&format!("{TYPE}fields = [{{ name = \"doc\", type = \"blob\" }}, {{ name = \"id\", type = \"string\" }}]\nsensitive = [{{ field = \"doc\", subject_key = \"c\", subject_field = \"id\" }}]\n"))),
            ("tag_on_blob_field", model(&format!("{TYPE}fields = [{{ name = \"doc\", type = \"blob\" }}]\ntags = {{ doc = \"doc\" }}\n"))),
            ("invalid_type_name", model("[[event_type]]\nname = \"order-placed\"\n")),
            ("invalid_field_name", model(&format!("{TYPE}fields = [{{ name = \"bad-name\", type = \"string\" }}]\n"))),
            ("duplicate_field", model(&format!("{TYPE}fields = [{{ name = \"id\", type = \"string\" }}, {{ name = \"id\", type = \"i64\" }}]\n"))),
            ("tag_field_not_declared", model(&format!("{TYPE}fields = [{{ name = \"id\", type = \"string\" }}]\ntags = {{ id = \"ident\" }}\n"))),
            ("sensitive_field_not_declared", model(&format!("{TYPE}fields = [{{ name = \"id\", type = \"string\" }}]\nsensitive = [{{ field = \"email\", subject_key = \"c\", subject_field = \"id\" }}]\n"))),
            ("tag_on_sensitive_field", model(&format!("{TYPE}fields = [{{ name = \"id\", type = \"string\" }}, {{ name = \"email\", type = \"string\" }}]\ntags = {{ email = \"email\" }}\nsensitive = [{{ field = \"email\", subject_key = \"c\", subject_field = \"id\" }}]\n"))),
            ("reserved_event_type_name", model("[[event_type]]\nname = \"EventRetracted\"\n")),
            ("retractable_without_retractions", model(&format!("{COMMAND}retractable = true\n"))),
            ("retractable_query_latest", format!("bounded_context = \"shop\"\nretractions = true\n{COMMAND}retractable = true\n[[command_type.query]]\ntags = [\"id\"]\nlatest = 1\n")),
            ("reads_nothing_with_query", model(&format!("{COMMAND}reads_nothing = true\n[[command_type.query]]\ntags = [\"id\"]\n"))),
            ("empty_query_item", model(&format!("{COMMAND}[[command_type.query]]\n"))),
            ("query_latest_zero", model(&format!("{COMMAND}[[command_type.query]]\ntags = [\"id\"]\nlatest = 0\n"))),
            ("query_event_type_not_declared", model(&format!("{COMMAND}[[command_type.query]]\nevent_types = [\"Missing\"]\n"))),
            ("query_tag_not_declared", model(&format!("{COMMAND}[[command_type.query]]\ntags = [\"order\"]\n"))),
            ("unknown_field_type", model(&format!("{TYPE}fields = [{{ name = \"id\", type = \"uuid\" }}]\n"))),
        ]
    }

    #[test]
    fn each_rule_has_its_own_code() {
        for (code, source) in cases() {
            let problems = crate::validate(&source).unwrap();
            assert!(!problems.is_empty(), "{code}: no problem");
            for problem in &problems {
                assert_eq!(problem.code, code, "{source}\n{problems:#?}");
            }
            // generate() refuses the same input with the same codes.
            match crate::generate(&source) {
                Err(crate::Error::Invalid(refused)) => {
                    let codes: Vec<_> = refused.iter().map(|p| p.code).collect();
                    assert_eq!(codes, vec![code; problems.len()], "{source}");
                }
                other => panic!("{code}: {other:?}"),
            }
        }
    }

    /// Every rule code the table lists is produced by one of the models above,
    /// so a code whose rule was deleted can't stay in the table unnoticed.
    #[test]
    fn every_rule_code_is_tested() {
        let tested: std::collections::BTreeSet<_> =
            cases().into_iter().map(|(code, _)| code).collect();
        let rules: std::collections::BTreeSet<_> = CODES
            .iter()
            .filter(|entry| entry.category == "semantic" || entry.code == "unknown_field_type")
            // import's own rule, asserted in tests/cli.rs and tests/import.rs.
            .filter(|entry| entry.code != "no_events_for_bounded_context")
            .map(|entry| entry.code)
            .collect();
        assert_eq!(tested, rules);
    }

    #[test]
    fn guided_problems_carry_the_table_s_hint_and_guide() {
        let source = &cases()
            .into_iter()
            .find(|(code, _)| *code == "tag_on_sensitive_field")
            .unwrap()
            .1;
        let problem = &crate::validate(source).unwrap()[0];
        assert_eq!(
            problem.guide,
            Some("skilj-uniqueness#first-is-the-value-personal-data")
        );
        assert!(problem.hint.is_some());
        assert_eq!(problem.category(), "semantic");
        assert!(problem.span.is_none());
        let source = "bounded_context = \"Shop\"";
        let problem = &crate::validate(source).unwrap()[0];
        assert!(problem.hint.is_none() && problem.guide.is_none());
    }

    /// The value's whole literal, quotes included, whatever its quoting, and
    /// the other shape error mapped back to where it is in the original.
    #[test]
    fn unknown_types_in_any_quoting_and_a_shape_error_have_exact_positions() {
        for literal in ["'uuid'", "\"uuid\"", "\"\"\"uuid\"\"\"", "'''uuid'''"] {
            let source = format!(
                "bounded_context = \"shop\"\n[[command_type]]\nname = \"Place\"\nfields = [{{ name = \"a\", type = {literal} }}, {{ name = \"b\", type = 'decimal' }}]\ntaggs = {{ a = \"a\" }}\n[[event_type]]\nname = \"Placed\"\nfields = [{{ name = \"id\", type = \"\"\"money\"\"\" }}]\n"
            );
            let problems = crate::validate(&source).unwrap();
            let found: Vec<_> = problems
                .iter()
                .map(|p| (p.code, p.span.clone().map(|span| &source[span])))
                .collect();
            // The unknown types in file order, then the shape error, at
            // `taggs` in the original text.
            assert_eq!(
                found,
                [
                    ("unknown_field_type", Some(literal)),
                    ("unknown_field_type", Some("'decimal'")),
                    ("unknown_field_type", Some("\"\"\"money\"\"\"")),
                    ("parse", Some("taggs")),
                ],
                "{literal}"
            );
            assert_eq!(problems[0].context, "command_type \"Place\"");
            assert_eq!(problems[2].context, "event_type \"Placed\"");
            assert!(problems[3].context.is_empty());
        }
    }

    #[test]
    fn a_non_string_type_and_a_syntax_error_stay_toml_errors_with_a_position() {
        for source in [
            "bounded_context = \"shop\"\n[[event_type]]\nname = \"Placed\"\nfields = [{ name = \"a\", type = 3 }]\n",
            "bounded_context = \"shop\"\n[[event_type]\n",
            // No unknown type: the TOML error as it is.
            "bounded_context = \"shop\"\n[[event_type]]\nname = \"Placed\"\ntaggs = {}\n",
        ] {
            match crate::validate(source) {
                Err(crate::Error::Toml(error)) => assert!(error.span().is_some(), "{source}"),
                other => panic!("{source}: {other:?}"),
            }
            assert!(matches!(crate::generate(source), Err(crate::Error::Toml(_))));
        }
    }
}
