//! Value types, enums and stored entities from the spec.

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The admin bounded context's name.
pub const ADMIN_CONTEXT: &str = "admin";
/// client_id for records with no caller (scheduler, library itself).
pub const SYSTEM_CLIENT_ID: &str = "system";

// ---------------------------------------------------------------- values

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    /// `None` is the "never set" state (absent field, empty or absent list).
    pub value: Option<String>,
}

impl Tag {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Tag {
            key: key.into(),
            value: Some(value.into()),
        }
    }
    pub fn none(key: impl Into<String>) -> Self {
        Tag {
            key: key.into(),
            value: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TagMapping {
    pub key: String,
    pub field: String,
    #[serde(default)]
    pub blind: bool,
}

impl TagMapping {
    pub fn new(key: impl Into<String>, field: impl Into<String>) -> Self {
        TagMapping {
            key: key.into(),
            field: field.into(),
            blind: false,
        }
    }
    pub fn blind(key: impl Into<String>, field: impl Into<String>) -> Self {
        TagMapping {
            key: key.into(),
            field: field.into(),
            blind: true,
        }
    }
}

/// One item of a command type's consistency query, as declared.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct QueryItemMapping {
    pub event_types: BTreeSet<String>,
    /// Tag mapping *keys* drawn from the command type's own tag_mappings.
    pub tag_keys: BTreeSet<String>,
    pub latest: Option<u32>,
    pub snapshot: Option<String>,
}

/// A query item resolved against one payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryItem {
    pub event_types: BTreeSet<String>,
    pub tags: BTreeSet<Tag>,
    pub latest: Option<u32>,
    pub snapshot: Option<String>,
}

impl QueryItem {
    /// The DCB item match: type in the item's types (or none named) AND
    /// carries every one of the item's tags. Naming neither matches nothing.
    pub fn matches(&self, event_type: &str, tags: &BTreeSet<Tag>) -> bool {
        (!self.event_types.is_empty() || !self.tags.is_empty())
            && (self.event_types.is_empty() || self.event_types.contains(event_type))
            && self.tags.iter().all(|t| tags.contains(t))
    }

    pub fn nothing() -> Self {
        QueryItem {
            event_types: BTreeSet::new(),
            tags: BTreeSet::new(),
            latest: None,
            snapshot: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SensitiveField {
    pub field: String,
    pub subject_key: String,
    pub subject_field: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivateFieldKind {
    Own,
    Capability,
    Addressed,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PrivateField {
    pub field: String,
    pub kind: PrivateFieldKind,
    #[serde(default)]
    pub capability: Option<String>,
    #[serde(default)]
    pub addressee_field: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Equals,
    Contains,
    IsLike,
    GreaterThan,
    LessThan,
    Near,
    SimilarColor,
    InSubnet,
    In,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Filter {
    pub field: String,
    pub operator: FilterOperator,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessLevel {
    Read,
    Write,
    Admin,
}

impl AccessLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            AccessLevel::Read => "read",
            AccessLevel::Write => "write",
            AccessLevel::Admin => "admin",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "read" => AccessLevel::Read,
            "write" => AccessLevel::Write,
            "admin" => AccessLevel::Admin,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    AutoAdvance,
    ManualAck,
}

impl AckMode {
    pub fn as_str(self) -> &'static str {
        match self {
            AckMode::AutoAdvance => "auto_advance",
            AckMode::ManualAck => "manual_ack",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "auto_advance" => Some(AckMode::AutoAdvance),
            "manual_ack" => Some(AckMode::ManualAck),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EventReadStartPosition {
    #[default]
    Beginning,
    Latest,
    AtSequence,
    AtTime,
}

impl EventReadStartPosition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Beginning => "beginning",
            Self::Latest => "latest",
            Self::AtSequence => "at_sequence",
            Self::AtTime => "at_time",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "beginning" => Self::Beginning,
            "latest" => Self::Latest,
            "at_sequence" => Self::AtSequence,
            "at_time" => Self::AtTime,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissedOccurrencePolicy {
    Skip,
    FireOnce,
    ReplayBacklog,
}

impl MissedOccurrencePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::FireOnce => "fire_once",
            Self::ReplayBacklog => "replay_backlog",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "skip" => Self::Skip,
            "fire_once" => Self::FireOnce,
            "replay_backlog" => Self::ReplayBacklog,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(rename = "type")]
    pub type_name: String,
    pub version: i32,
    pub client_id: String,
    pub created_at: DateTime<Utc>,
    pub correlation_id: String,
    pub causation_id: Option<String>,
}

/// Where an event came from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Origin {
    CommandTriggered {
        command_id: String,
    },
    ExternalTriggered {
        source_content: String,
        source_context: Option<String>,
    },
    DirectlyCreated,
    SystemTriggered {
        occurrence_at: DateTime<Utc>,
    },
    SkiljRecorded,
}

impl Origin {
    pub fn kind(&self) -> &'static str {
        match self {
            Origin::CommandTriggered { .. } => "command_triggered",
            Origin::ExternalTriggered { .. } => "external_triggered",
            Origin::DirectlyCreated => "directly_created",
            Origin::SystemTriggered { .. } => "system_triggered",
            Origin::SkiljRecorded => "skilj_recorded",
        }
    }
}

/// An event as stored (payload as stored: sensitive leaves are ciphertext,
/// private leaves plaintext).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredEvent {
    pub bounded_context: String,
    pub sequence: i64,
    pub event_type: String,
    pub payload: Value,
    pub metadata: Metadata,
    pub tags: BTreeSet<Tag>,
    pub encryption_key_ids: Vec<String>,
    pub origin: Origin,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredCommand {
    pub id: String,
    pub bounded_context: String,
    pub command_type: String,
    pub payload: Value,
    pub metadata: Metadata,
    pub encryption_key_ids: Vec<String>,
    pub consistency_tags: BTreeSet<Tag>,
    pub consistency_query: Vec<QueryItem>,
    pub consistency_boundary: Option<i64>,
    /// Recording order (the spec's `recorded_after`).
    pub recorded: i64,
    pub triggered_sequences: Vec<i64>,
}

// ---------------------------------------------------------- registered

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextStatus {
    Active,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ContextCreator {
    Superadmin { role_id: String },
    System,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundedContext {
    pub name: String,
    pub status: ContextStatus,
    pub created_at: DateTime<Utc>,
    pub created_by: ContextCreator,
    pub template: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventTypeDef {
    pub bounded_context: String,
    pub name: String,
    pub schema: Value,
    pub schema_version: i32,
    pub tag_mappings: Vec<TagMapping>,
    pub owner_tag_key: Option<String>,
    pub sensitive_fields: Vec<SensitiveField>,
    pub private_fields: Vec<PrivateField>,
    pub external_creation_allowed: bool,
    pub direct_creation_allowed: bool,
    pub system_triggered_allowed: bool,
    pub system_triggered_schedule: Option<String>,
    pub missed_occurrence_policy: Option<MissedOccurrencePolicy>,
    pub schedule_position: Option<DateTime<Utc>>,
    pub last_fired_at: Option<DateTime<Utc>>,
    pub event_read_allowed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CommandTypeDef {
    pub bounded_context: String,
    pub name: String,
    pub schema: Value,
    pub schema_version: i32,
    pub tag_mappings: Vec<TagMapping>,
    pub owner_tag_key: Option<String>,
    pub sensitive_fields: Vec<SensitiveField>,
    pub private_fields: Vec<PrivateField>,
    pub rest_trigger_allowed: bool,
}

/// The parts of a type that payload handling needs; EventType and
/// CommandType are interchangeable here, as the spec says.
pub trait PayloadType {
    fn bounded_context(&self) -> &str;
    fn type_name(&self) -> &str;
    fn tag_mappings(&self) -> &[TagMapping];
    fn sensitive_fields(&self) -> &[SensitiveField];
    fn private_fields(&self) -> &[PrivateField];
    fn owner_tag_key(&self) -> Option<&str>;
    fn schema_version(&self) -> i32;
}

macro_rules! payload_type {
    ($t:ty) => {
        impl PayloadType for $t {
            fn bounded_context(&self) -> &str {
                &self.bounded_context
            }
            fn type_name(&self) -> &str {
                &self.name
            }
            fn tag_mappings(&self) -> &[TagMapping] {
                &self.tag_mappings
            }
            fn sensitive_fields(&self) -> &[SensitiveField] {
                &self.sensitive_fields
            }
            fn private_fields(&self) -> &[PrivateField] {
                &self.private_fields
            }
            fn owner_tag_key(&self) -> Option<&str> {
                self.owner_tag_key.as_deref()
            }
            fn schema_version(&self) -> i32 {
                self.schema_version
            }
        }
    };
}
payload_type!(EventTypeDef);
payload_type!(CommandTypeDef);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionDef {
    pub bounded_context: String,
    pub name: String,
    pub schema: Value,
    pub schema_version: i32,
    pub consumed_event_types: BTreeSet<String>,
    pub sync: bool,
    pub requires_capability: Option<String>,
    pub caught_up_to: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebuildStatus {
    Pending,
    Building,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionRebuild {
    pub bounded_context: String,
    pub projection: String,
    pub schema: Value,
    pub schema_version: i32,
    pub consumed_event_types: BTreeSet<String>,
    pub sync: bool,
    pub caught_up_to: Option<i64>,
    pub status: RebuildStatus,
}

// ------------------------------------------------------------- access

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Role {
    pub id: String,
    pub external_subject: String,
    pub name: String,
    pub superadmin: bool,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessMapping {
    pub id: String,
    pub role_id: String,
    pub bounded_context: String,
    pub level: AccessLevel,
    pub can_read_sensitive: bool,
    pub scope: Option<String>,
    pub capabilities: BTreeSet<String>,
    pub delegable: BTreeSet<String>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// A resolved GraphQL caller in one bounded context: the grant plus the
/// Role behind it (for client_id and external_subject).
#[derive(Debug, Clone)]
pub struct Grant {
    pub mapping: AccessMapping,
    pub role: Role,
}

impl Grant {
    pub fn has_level(&self, level: AccessLevel) -> bool {
        self.mapping.active && self.role.active && self.mapping.level >= level
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenPurpose {
    ExternalEvent,
    DirectCreation,
    Command,
    EventRead,
}

impl TokenPurpose {
    pub fn as_str(self) -> &'static str {
        match self {
            TokenPurpose::ExternalEvent => "external_event",
            TokenPurpose::DirectCreation => "direct_creation",
            TokenPurpose::Command => "command",
            TokenPurpose::EventRead => "event_read",
        }
    }
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "external_event" => TokenPurpose::ExternalEvent,
            "direct_creation" => TokenPurpose::DirectCreation,
            "command" => TokenPurpose::Command,
            "event_read" => TokenPurpose::EventRead,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessToken {
    pub id: String,
    pub purpose: TokenPurpose,
    pub bounded_context: String,
    /// EventType name, or CommandType name for a command token.
    pub type_name: String,
    pub scope: Option<String>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub start_from: EventReadStartPosition,
    pub start_at_sequence: Option<i64>,
    pub start_at_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateFieldGrant {
    pub id: String,
    pub bounded_context: String,
    pub grantor_role_id: String,
    pub grantee_role_id: String,
    pub event_sequence: Option<i64>,
    pub command_id: Option<String>,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EncryptionKeyInfo {
    pub id: String,
    pub bounded_context: String,
    pub subject_key: String,
    pub subject_value: String,
    pub active: bool,
    pub created_at: DateTime<Utc>,
    pub destroyed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadCursor {
    pub token_id: String,
    pub ack_mode: AckMode,
    pub sequence: i64,
    pub updated_at: DateTime<Utc>,
    pub checked_out_at: Option<DateTime<Utc>>,
}

/// Library configuration (the spec's `config` block plus process-start
/// knobs it names but leaves to "runtime configuration").
#[derive(Debug, Clone)]
pub struct Config {
    pub read_cursor_checkout_lease: chrono::Duration,
    pub max_events_per_read: usize,
    pub idempotency_key_retention: chrono::Duration,
    pub max_query_tags: usize,
    pub max_items_per_batch: usize,
    /// Event cache warm-up count per bounded context.
    pub event_cache_size: usize,
    /// await_projection_caught_up's wait bound.
    pub projection_wait: std::time::Duration,
    /// How often the async projection consumer and scheduler tick.
    pub background_tick: std::time::Duration,
    pub max_filters: usize,
    pub max_filter_value_len: usize,
    pub max_capabilities_per_grant: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            read_cursor_checkout_lease: chrono::Duration::minutes(5),
            max_events_per_read: 1000,
            idempotency_key_retention: chrono::Duration::hours(48),
            max_query_tags: 32,
            max_items_per_batch: 100,
            event_cache_size: 1000,
            projection_wait: std::time::Duration::from_secs(5),
            background_tick: std::time::Duration::from_millis(200),
            max_filters: 16,
            max_filter_value_len: 512,
            max_capabilities_per_grant: 16,
        }
    }
}
