//! What an application compiles into its binary: bounded context
//! declarations with their deciders, folds and scheduled payload producers.
//! The spec leaves this plugin boundary as a prose contract; here it is
//! plain data plus closures, no macros.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::model::{
    MissedOccurrencePolicy, PrivateField, QueryItemMapping, SensitiveField, StoredEvent, Tag,
    TagMapping,
};

/// One event decide() wants appended.
#[derive(Debug, Clone, PartialEq)]
pub struct EventSpec {
    pub event_type: String,
    pub payload: Value,
}

impl EventSpec {
    pub fn new(event_type: impl Into<String>, payload: Value) -> Self {
        EventSpec {
            event_type: event_type.into(),
            payload,
        }
    }
}

/// CommandDecision as a real two-case sum: a rejection always carries both
/// reason and kind, an acceptance carries neither.
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    Accept(Vec<EventSpec>),
    Reject { reason: String, kind: String },
}

impl Decision {
    pub fn accept(events: Vec<EventSpec>) -> Self {
        Decision::Accept(events)
    }
    pub fn reject(kind: impl Into<String>, reason: impl Into<String>) -> Self {
        Decision::Reject {
            reason: reason.into(),
            kind: kind.into(),
        }
    }
}

/// Everything decide() is handed.
pub struct DecisionInput<'a> {
    /// The command payload as submitted (plaintext).
    pub payload: &'a Value,
    /// query_matching_events, in sequence order, with sensitive leaves
    /// decrypted where their key is still active.
    pub events: &'a [StoredEvent],
    /// Snapshot items: (snapshot name, tag value) -> folded state.
    pub snapshots: &'a BTreeMap<(String, String), Value>,
    /// The instant of the decision.
    pub now: DateTime<Utc>,
}

impl DecisionInput<'_> {
    pub fn snapshot(&self, name: &str, tag_value: &str) -> Option<&Value> {
        self.snapshots
            .get(&(name.to_string(), tag_value.to_string()))
    }
}

pub type DecideFn = Arc<dyn Fn(&DecisionInput<'_>) -> Decision + Send + Sync>;
pub type ScheduledPayloadFn = Arc<dyn Fn(DateTime<Utc>) -> Value + Send + Sync>;
pub type ProjectionKeysFn = Arc<dyn Fn(&StoredEvent) -> Vec<String> + Send + Sync>;
pub type ProjectionFoldFn =
    Arc<dyn Fn(&str, Option<&Value>, &StoredEvent) -> Option<Value> + Send + Sync>;
pub type SummaryFoldFn = Arc<dyn Fn(&Value, &StoredEvent, &str) -> Value + Send + Sync>;

#[derive(Clone)]
pub struct EventTypeDecl {
    pub name: String,
    pub schema: Value,
    pub tag_mappings: Vec<TagMapping>,
    pub owner_tag_key: Option<String>,
    pub sensitive_fields: Vec<SensitiveField>,
    pub private_fields: Vec<PrivateField>,
    pub external_creation_allowed: bool,
    pub direct_creation_allowed: bool,
    pub event_read_allowed: bool,
    pub schedule: Option<(String, MissedOccurrencePolicy, ScheduledPayloadFn)>,
}

impl EventTypeDecl {
    pub fn new(name: impl Into<String>, schema: Value) -> Self {
        EventTypeDecl {
            name: name.into(),
            schema,
            tag_mappings: vec![],
            owner_tag_key: None,
            sensitive_fields: vec![],
            private_fields: vec![],
            external_creation_allowed: false,
            direct_creation_allowed: false,
            event_read_allowed: false,
            schedule: None,
        }
    }
    pub fn tag(mut self, key: &str, field: &str) -> Self {
        self.tag_mappings.push(TagMapping::new(key, field));
        self
    }
    pub fn blind_tag(mut self, key: &str, field: &str) -> Self {
        self.tag_mappings.push(TagMapping::blind(key, field));
        self
    }
    pub fn owner(mut self, key: &str) -> Self {
        self.owner_tag_key = Some(key.into());
        self
    }
    pub fn sensitive(mut self, field: &str, subject_key: &str, subject_field: &str) -> Self {
        self.sensitive_fields.push(SensitiveField {
            field: field.into(),
            subject_key: subject_key.into(),
            subject_field: subject_field.into(),
        });
        self
    }
    pub fn private(mut self, p: PrivateField) -> Self {
        self.private_fields.push(p);
        self
    }
    pub fn external(mut self) -> Self {
        self.external_creation_allowed = true;
        self
    }
    pub fn direct(mut self) -> Self {
        self.direct_creation_allowed = true;
        self
    }
    pub fn readable(mut self) -> Self {
        self.event_read_allowed = true;
        self
    }
    pub fn scheduled(
        mut self,
        cron: &str,
        policy: MissedOccurrencePolicy,
        payload: impl Fn(DateTime<Utc>) -> Value + Send + Sync + 'static,
    ) -> Self {
        self.schedule = Some((cron.into(), policy, Arc::new(payload)));
        self
    }
}

#[derive(Clone)]
pub struct CommandTypeDecl {
    pub name: String,
    pub schema: Value,
    pub tag_mappings: Vec<TagMapping>,
    pub owner_tag_key: Option<String>,
    pub sensitive_fields: Vec<SensitiveField>,
    pub private_fields: Vec<PrivateField>,
    pub rest_trigger_allowed: bool,
    pub consistency_query: Vec<QueryItemMapping>,
    pub decide: DecideFn,
}

impl CommandTypeDecl {
    pub fn new(
        name: impl Into<String>,
        schema: Value,
        decide: impl Fn(&DecisionInput<'_>) -> Decision + Send + Sync + 'static,
    ) -> Self {
        CommandTypeDecl {
            name: name.into(),
            schema,
            tag_mappings: vec![],
            owner_tag_key: None,
            sensitive_fields: vec![],
            private_fields: vec![],
            rest_trigger_allowed: false,
            consistency_query: vec![],
            decide: Arc::new(decide),
        }
    }
    pub fn tag(mut self, key: &str, field: &str) -> Self {
        self.tag_mappings.push(TagMapping::new(key, field));
        self
    }
    pub fn blind_tag(mut self, key: &str, field: &str) -> Self {
        self.tag_mappings.push(TagMapping::blind(key, field));
        self
    }
    pub fn owner(mut self, key: &str) -> Self {
        self.owner_tag_key = Some(key.into());
        self
    }
    pub fn sensitive(mut self, field: &str, subject_key: &str, subject_field: &str) -> Self {
        self.sensitive_fields.push(SensitiveField {
            field: field.into(),
            subject_key: subject_key.into(),
            subject_field: subject_field.into(),
        });
        self
    }
    pub fn private(mut self, p: PrivateField) -> Self {
        self.private_fields.push(p);
        self
    }
    pub fn rest_trigger(mut self) -> Self {
        self.rest_trigger_allowed = true;
        self
    }
    pub fn query_item(mut self, item: QueryItemMapping) -> Self {
        self.consistency_query.push(item);
        self
    }
}

/// Helpers for building consistency query items.
pub fn item(event_types: &[&str], tag_keys: &[&str], latest: Option<u32>) -> QueryItemMapping {
    QueryItemMapping {
        event_types: event_types.iter().map(|s| s.to_string()).collect(),
        tag_keys: tag_keys.iter().map(|s| s.to_string()).collect(),
        latest,
        snapshot: None,
    }
}

pub fn snapshot_item(summary: &str, tag_key: &str) -> QueryItemMapping {
    QueryItemMapping {
        event_types: BTreeSet::new(),
        tag_keys: [tag_key.to_string()].into_iter().collect(),
        latest: None,
        snapshot: Some(summary.to_string()),
    }
}

/// The nothing item: a decision from the payload alone.
pub fn nothing_item() -> QueryItemMapping {
    QueryItemMapping::default()
}

#[derive(Clone)]
pub struct ProjectionDecl {
    pub name: String,
    pub schema: Value,
    pub consumed_event_types: BTreeSet<String>,
    pub sync: bool,
    pub requires_capability: Option<String>,
    /// Compiled-only owner dimension (spec: no registration surface).
    pub owner_tag_key: Option<String>,
    pub keys: ProjectionKeysFn,
    pub fold: ProjectionFoldFn,
}

impl ProjectionDecl {
    /// A projection keyed by the instance keys `keys` returns per event,
    /// folding each with `fold` (returning None deletes the instance).
    pub fn new(
        name: impl Into<String>,
        schema: Value,
        consumed: &[&str],
        keys: impl Fn(&StoredEvent) -> Vec<String> + Send + Sync + 'static,
        fold: impl Fn(&str, Option<&Value>, &StoredEvent) -> Option<Value> + Send + Sync + 'static,
    ) -> Self {
        ProjectionDecl {
            name: name.into(),
            schema,
            consumed_event_types: consumed.iter().map(|s| s.to_string()).collect(),
            sync: false,
            requires_capability: None,
            owner_tag_key: None,
            keys: Arc::new(keys),
            fold: Arc::new(fold),
        }
    }
    pub fn sync(mut self) -> Self {
        self.sync = true;
        self
    }
    pub fn requires(mut self, capability: &str) -> Self {
        self.requires_capability = Some(capability.into());
        self
    }
    pub fn owner(mut self, key: &str) -> Self {
        self.owner_tag_key = Some(key.into());
        self
    }
}

/// A summary ("snapshot") a command's snapshot items read: state per value
/// of one tag key, folded from every event carrying that value.
#[derive(Clone)]
pub struct SummaryDecl {
    pub name: String,
    pub tag_key: String,
    pub version: i32,
    pub initial: Value,
    pub owner_tag_key: Option<String>,
    pub fold: SummaryFoldFn,
}

impl SummaryDecl {
    pub fn new(
        name: impl Into<String>,
        tag_key: impl Into<String>,
        version: i32,
        initial: Value,
        fold: impl Fn(&Value, &StoredEvent, &str) -> Value + Send + Sync + 'static,
    ) -> Self {
        SummaryDecl {
            name: name.into(),
            tag_key: tag_key.into(),
            version,
            initial,
            owner_tag_key: None,
            fold: Arc::new(fold),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlindTagDecl {
    pub tag_key: String,
    pub normalizer_version: i32,
    pub erasure_subject_key: Option<String>,
    pub erasure_subject_tag: Option<String>,
}

/// One bounded context as the binary declares it.
#[derive(Clone)]
pub struct BoundedContextDecl {
    pub name: String,
    pub event_types: Vec<EventTypeDecl>,
    pub command_types: Vec<CommandTypeDecl>,
    pub projections: Vec<ProjectionDecl>,
    pub summaries: Vec<SummaryDecl>,
    pub blind_tags: Vec<BlindTagDecl>,
}

impl BoundedContextDecl {
    pub fn new(name: impl Into<String>) -> Self {
        BoundedContextDecl {
            name: name.into(),
            event_types: vec![],
            command_types: vec![],
            projections: vec![],
            summaries: vec![],
            blind_tags: vec![],
        }
    }
    pub fn event(mut self, e: EventTypeDecl) -> Self {
        self.event_types.push(e);
        self
    }
    pub fn command(mut self, c: CommandTypeDecl) -> Self {
        self.command_types.push(c);
        self
    }
    pub fn projection(mut self, p: ProjectionDecl) -> Self {
        self.projections.push(p);
        self
    }
    pub fn summary(mut self, s: SummaryDecl) -> Self {
        self.summaries.push(s);
        self
    }
    pub fn blind(mut self, b: BlindTagDecl) -> Self {
        self.blind_tags.push(b);
        self
    }

    /// The ConsistencyQueryIsWellFormed checks that don't need registered
    /// state: refused when the application is built.
    pub fn check(&self) -> crate::Result<()> {
        use crate::error::Error;
        for ct in &self.command_types {
            let q = &ct.consistency_query;
            for item in q {
                for k in &item.tag_keys {
                    if !ct.tag_mappings.iter().any(|m| &m.key == k) {
                        return Err(Error::Config(format!(
                            "{}.{}: query item names tag key '{k}' the command type doesn't map",
                            self.name, ct.name
                        )));
                    }
                }
                for et in &item.event_types {
                    if !self.event_types.iter().any(|e| &e.name == et) {
                        return Err(Error::Config(format!(
                            "{}.{}: query item names event type '{et}' this context doesn't declare",
                            self.name, ct.name
                        )));
                    }
                }
                let is_nothing = item.event_types.is_empty() && item.tag_keys.is_empty();
                if is_nothing && (q.len() != 1 || item.latest.is_some() || item.snapshot.is_some())
                {
                    return Err(Error::Config(format!(
                        "{}.{}: the nothing item must stand alone without latest",
                        self.name, ct.name
                    )));
                }
                if item.latest == Some(0) {
                    return Err(Error::Config(format!(
                        "{}.{}: latest must be at least 1",
                        self.name, ct.name
                    )));
                }
                if let Some(s) = &item.snapshot {
                    let summary =
                        self.summaries
                            .iter()
                            .find(|x| &x.name == s)
                            .ok_or_else(|| {
                                Error::Config(format!(
                                    "{}.{}: snapshot item names undeclared summary '{s}'",
                                    self.name, ct.name
                                ))
                            })?;
                    if item.tag_keys.len() != 1
                        || !item.event_types.is_empty()
                        || item.latest.is_some()
                        || !item.tag_keys.contains(&summary.tag_key)
                    {
                        return Err(Error::Config(format!(
                            "{}.{}: a snapshot item has exactly the summary's tag key and nothing else",
                            self.name, ct.name
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Tags of an event under a key, for projection owner derivation.
pub fn tag_value<'a>(tags: &'a BTreeSet<Tag>, key: &str) -> Option<&'a str> {
    tags.iter()
        .find(|t| t.key == key)
        .and_then(|t| t.value.as_deref())
}
