//! Cross-cutting value types, used across every domain module - and the
//! home for primitives (once implemented) reused by both
//! `access_control`'s `AccessToken` and `bootstrap`'s `BootstrapSecret`
//! (`generate_token_secret`, `secret_matches`, `generate_token_id`). See
//! docs/architecture.md §3.2 and `specs/skilj.allium`'s Value Types
//! section, which this module mirrors closely.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Names a payload field to filter, tag, or mark sensitive: a bare
/// top-level name, or a two-segment dotted path reaching one leaf inside
/// the spec's one permitted level of nesting (e.g. `"address.country"`).
/// See the payload schema shape note above `entity CommandType` in
/// `specs/skilj.allium`.
pub type FieldPath = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Filter {
    pub field: FieldPath,
    pub operator: FilterOperator,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Equals,
    Contains,
    IsLike,
    GreaterThan,
    LessThan,
}

/// See `value Metadata` in the spec. `version` and every other
/// sequence-adjacent integer in this codebase is `i64` - see
/// docs/architecture.md §2.2.1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Metadata {
    pub r#type: String,
    pub version: i64,
    pub client_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// `value` is `None` (absent) rather than the tag being omitted entirely
/// when the mapped payload field was itself absent - see `Tag.value` in
/// the spec for why that's deliberate, not an oversight.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct Tag {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TagMapping {
    pub key: String,
    pub field: FieldPath,
}

/// `subject_field` is never encrypted - it's the plaintext identifier the
/// `EncryptionKey` is looked up or derived by. Only `field` is what gets
/// swapped for ciphertext (at the leaf, for a dotted path - see
/// `protect_sensitive_fields` in `event_store`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SensitiveField {
    pub field: FieldPath,
    pub subject_key: String,
    pub subject_field: FieldPath,
}

/// The whole contract a bounded context's `decide()` returns - see
/// `value CommandDecision` in the spec, and docs/architecture.md
/// §4.1/§5.4/§7.3 for why a rejection is typed data, never a
/// GraphQL/HTTP-level error.
#[derive(Debug, Clone)]
pub enum CommandDecision {
    Accepted { events: Vec<EventSpec> },
    Rejected { reason: String, kind: String },
}

/// One event a `decide()` call wants appended, on acceptance.
#[derive(Debug, Clone)]
pub struct EventSpec {
    pub event_type: String,
    pub payload: serde_json::Value,
}

// TODO: generate_token_secret, secret_matches, generate_token_id - the
// crypto primitives `access_control::AccessToken` and
// `bootstrap::BootstrapSecret` both build on (see specs/skilj.allium's
// AccessToken.secret note and docs/architecture.md §3.2).
