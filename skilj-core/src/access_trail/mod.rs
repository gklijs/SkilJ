//! The access trail (docs/architecture.md §229, `rule RecordAccessEvent`,
//! `rule CapabilityHistory`): every change to who holds what in a bounded
//! context is also an event in the `admin` bounded context, written in the
//! same transaction as the change, so "could this grant read X on that
//! date" is answered from stored facts instead of from rows that only say
//! what is true now.
//!
//! This module is the pure half: the six event types, how an event is
//! built from a change, and the fold behind `capabilityHistory`. The
//! transaction that writes them is `db::record_access_event` and the
//! `*_with_trail` functions around it.
//!
//! Payloads carry ids only (the Role, the bounded context, the acting
//! Role), never a name or an `external_subject`, so nothing here is
//! personal data a person could ask to have forgotten. `scope` is the one
//! free-form value, an opaque string the grant itself already stores.

use crate::access_control::AccessLevel;
use crate::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME;
use crate::event_store::{BoundedContext, Event, EventOrigin, EventType};
use crate::shared::{Metadata, Tag, TagMapping};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::BTreeSet;

/// `access_event_type_names` in the spec: the six event types SkilJ
/// registers in the admin bounded context itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccessEventKind {
    /// A `RoleAccessMapping` was made.
    AccessGranted,
    /// One ended, because the mapping or its Role was revoked.
    AccessRevoked,
    /// A capability was added to a grant.
    CapabilityGranted,
    /// A capability was removed from a grant.
    CapabilityRevoked,
    /// A grant's delegable set was replaced.
    DelegableCapabilitiesSet,
    /// What an existing, active grant held when the trail began.
    AccessBaselined,
}

impl AccessEventKind {
    /// Every kind, in the order they are registered.
    pub const ALL: [AccessEventKind; 6] = [
        AccessEventKind::AccessGranted,
        AccessEventKind::AccessRevoked,
        AccessEventKind::CapabilityGranted,
        AccessEventKind::CapabilityRevoked,
        AccessEventKind::DelegableCapabilitiesSet,
        AccessEventKind::AccessBaselined,
    ];

    /// The event type's name in the admin bounded context.
    pub fn name(self) -> &'static str {
        match self {
            AccessEventKind::AccessGranted => "AccessGranted",
            AccessEventKind::AccessRevoked => "AccessRevoked",
            AccessEventKind::CapabilityGranted => "CapabilityGranted",
            AccessEventKind::CapabilityRevoked => "CapabilityRevoked",
            AccessEventKind::DelegableCapabilitiesSet => "DelegableCapabilitiesSet",
            AccessEventKind::AccessBaselined => "AccessBaselined",
        }
    }

    /// The kind an event type name stands for, or `None` for any other name.
    pub fn from_name(name: &str) -> Option<AccessEventKind> {
        Self::ALL.into_iter().find(|kind| kind.name() == name)
    }

    /// The payload fields beyond `role_id` and `bounded_context`, as
    /// `(name, JSON Schema type, required)`.
    fn extra_fields(self) -> &'static [(&'static str, &'static str, bool)] {
        match self {
            AccessEventKind::AccessGranted => &[
                ("actor_role_id", "string", false),
                ("level", "string", true),
                ("can_read_sensitive", "boolean", true),
                ("scope", "string", false),
            ],
            AccessEventKind::AccessRevoked => &[
                ("actor_role_id", "string", false),
                ("reason", "string", true),
            ],
            AccessEventKind::CapabilityGranted | AccessEventKind::CapabilityRevoked => &[
                ("actor_role_id", "string", false),
                ("capability", "string", true),
                ("via", "string", true),
            ],
            AccessEventKind::DelegableCapabilitiesSet => &[
                ("actor_role_id", "string", false),
                ("delegable", "array", true),
            ],
            AccessEventKind::AccessBaselined => &[
                ("actor_role_id", "string", false),
                ("level", "string", true),
                ("can_read_sensitive", "boolean", true),
                ("scope", "string", false),
                ("capabilities", "array", true),
                ("delegable", "array", true),
            ],
        }
    }
}

/// `access_event_schema(name)`: the JSON Schema of the kind's payload.
pub fn access_event_schema(kind: AccessEventKind) -> String {
    let mut properties = serde_json::Map::new();
    let mut required = vec![json!("role_id"), json!("bounded_context")];
    properties.insert("role_id".into(), json!({ "type": "string" }));
    properties.insert("bounded_context".into(), json!({ "type": "string" }));
    for (name, ty, is_required) in kind.extra_fields() {
        let property = if *ty == "array" {
            json!({ "type": "array", "items": { "type": "string" } })
        } else if *name == "actor_role_id" || *name == "scope" {
            json!({ "type": ["string", "null"] })
        } else {
            json!({ "type": ty })
        };
        properties.insert((*name).into(), property);
        if *is_required {
            required.push(json!(name));
        }
    }
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
    })
    .to_string()
}

/// The event type as registered in the admin bounded context: tagged by
/// `role` and `bounded_context`, creatable by nobody but the library.
pub fn access_event_type(admin: &BoundedContext, kind: AccessEventKind) -> EventType {
    EventType {
        bounded_context: admin.clone(),
        name: kind.name().to_string(),
        schema: access_event_schema(kind),
        schema_version: 1,
        tag_mappings: vec![
            TagMapping::new("role", "role_id"),
            TagMapping::new("bounded_context", "bounded_context"),
        ],
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: false,
        direct_creation_allowed: false,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: false,
    }
}

/// `level` as the payload spells it.
pub fn level_str(level: AccessLevel) -> &'static str {
    match level {
        AccessLevel::Read => "read",
        AccessLevel::Write => "write",
        AccessLevel::Admin => "admin",
    }
}

/// What a change is about, and the kind-specific payload fields.
/// `actor_role_id` is the Role that made the change, `None` for a
/// baseline or a change no Role made (local mode's seed grant).
pub struct AccessChange {
    /// Which event this is.
    pub kind: AccessEventKind,
    /// The Role whose grant changed.
    pub role_id: String,
    /// The bounded context the grant is in.
    pub bounded_context: String,
    /// The Role that made the change.
    pub actor_role_id: Option<String>,
    /// The kind-specific fields (see [`AccessEventKind`]).
    pub details: serde_json::Map<String, Value>,
}

impl AccessChange {
    /// `AccessGranted`.
    pub fn granted(
        role_id: &str,
        bounded_context: &str,
        actor_role_id: Option<&str>,
        level: AccessLevel,
        can_read_sensitive: bool,
        scope: Option<&str>,
    ) -> Self {
        Self::new(
            AccessEventKind::AccessGranted,
            role_id,
            bounded_context,
            actor_role_id,
            json!({
                "level": level_str(level),
                "can_read_sensitive": can_read_sensitive,
                "scope": scope,
            }),
        )
    }

    /// `AccessRevoked`; `role_revoked` when the Role's revocation ended it.
    pub fn revoked(
        role_id: &str,
        bounded_context: &str,
        actor_role_id: &str,
        role_revoked: bool,
    ) -> Self {
        Self::new(
            AccessEventKind::AccessRevoked,
            role_id,
            bounded_context,
            Some(actor_role_id),
            json!({ "reason": if role_revoked { "role_revoked" } else { "grant_revoked" } }),
        )
    }

    /// `CapabilityGranted`/`CapabilityRevoked`; `via_admin` when the actor
    /// acted as a context admin rather than as a superadmin.
    pub fn capability(
        granted: bool,
        role_id: &str,
        bounded_context: &str,
        actor_role_id: &str,
        capability: &str,
        via_admin: bool,
    ) -> Self {
        Self::new(
            if granted {
                AccessEventKind::CapabilityGranted
            } else {
                AccessEventKind::CapabilityRevoked
            },
            role_id,
            bounded_context,
            Some(actor_role_id),
            json!({
                "capability": capability,
                "via": if via_admin { "admin" } else { "superadmin" },
            }),
        )
    }

    /// `DelegableCapabilitiesSet`.
    pub fn delegable_set(
        role_id: &str,
        bounded_context: &str,
        actor_role_id: &str,
        delegable: &BTreeSet<String>,
    ) -> Self {
        Self::new(
            AccessEventKind::DelegableCapabilitiesSet,
            role_id,
            bounded_context,
            Some(actor_role_id),
            json!({ "delegable": delegable }),
        )
    }

    /// `AccessBaselined`: `mapping`'s state right now.
    pub fn baselined(mapping: &crate::access_control::RoleAccessMapping) -> Self {
        Self::new(
            AccessEventKind::AccessBaselined,
            &mapping.role.id,
            &mapping.bounded_context.name,
            None,
            json!({
                "level": level_str(mapping.level),
                "can_read_sensitive": mapping.can_read_sensitive,
                "scope": mapping.scope,
                "capabilities": mapping.capabilities,
                "delegable": mapping.delegable,
            }),
        )
    }

    fn new(
        kind: AccessEventKind,
        role_id: &str,
        bounded_context: &str,
        actor_role_id: Option<&str>,
        details: Value,
    ) -> Self {
        let Value::Object(details) = details else {
            unreachable!("every constructor above passes a JSON object")
        };
        Self {
            kind,
            role_id: role_id.to_string(),
            bounded_context: bounded_context.to_string(),
            actor_role_id: actor_role_id.map(str::to_string),
            details,
        }
    }

    /// The payload `rule RecordAccessEvent` writes: the details plus the
    /// Role, the bounded context and the actor.
    pub fn payload(&self) -> String {
        let mut payload = self.details.clone();
        payload.insert("role_id".into(), json!(self.role_id));
        payload.insert("bounded_context".into(), json!(self.bounded_context));
        payload.insert("actor_role_id".into(), json!(self.actor_role_id));
        Value::Object(payload).to_string()
    }

    /// The event for this change at `sequence`: `SkiljRecorded`,
    /// `client_id` the literal `"system"`, tagged by Role and bounded
    /// context (what `derive_tags` would produce from the payload).
    pub fn into_event(self, admin: &BoundedContext, sequence: i64, now: DateTime<Utc>) -> Event {
        debug_assert_eq!(admin.name, ADMIN_BOUNDED_CONTEXT_NAME);
        let event_type = access_event_type(admin, self.kind);
        let payload = self.payload();
        Event {
            bounded_context: admin.clone(),
            metadata: Metadata {
                r#type: event_type.name.clone(),
                version: event_type.schema_version,
                client_id: "system".to_string(),
                created_at: now,
                correlation_id: Some(crate::shared::generate_token_id()),
                causation_id: None,
            },
            event_type,
            payload,
            sequence,
            tags: vec![
                Tag {
                    key: "role".into(),
                    value: Some(self.role_id),
                },
                Tag {
                    key: "bounded_context".into(),
                    value: Some(self.bounded_context),
                },
            ],
            encryption_keys: Vec::new(),
            origin: EventOrigin::SkiljRecorded,
        }
    }
}

/// One stored access event, as the fold reads it.
#[derive(Debug, Clone)]
pub struct AccessEventRecord {
    /// Which kind it is.
    pub kind: AccessEventKind,
    /// Its payload.
    pub payload: Value,
    /// When it was recorded.
    pub created_at: DateTime<Utc>,
}

/// What `capabilityHistory` answers (`CapabilityHistoryReturned`).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AccessHistory {
    /// Whether the trail had begun for this grant by `as_of`; when not,
    /// the rest says nothing about what the grant held.
    pub known: bool,
    /// Whether the grant was active.
    pub held: bool,
    /// Its level while held.
    pub level: Option<String>,
    /// Whether it could read sensitive fields while held.
    pub can_read_sensitive: bool,
    /// Its scope while held.
    pub scope: Option<String>,
    /// The capabilities it held.
    pub capabilities: BTreeSet<String>,
    /// What it could delegate.
    pub delegable: BTreeSet<String>,
}

fn string_set(value: Option<&Value>) -> BTreeSet<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// `fold_access_events`: the grant's state after every event recorded at or
/// before `as_of`, applied in the order given (stored order, i.e. by
/// sequence). A revoked grant holds nothing, so its capabilities and
/// delegable set are empty until a new grant begins.
pub fn fold_access_events(events: &[AccessEventRecord], as_of: DateTime<Utc>) -> AccessHistory {
    let mut state = AccessHistory::default();
    for event in events.iter().filter(|event| event.created_at <= as_of) {
        state.known = true;
        let payload = &event.payload;
        match event.kind {
            AccessEventKind::AccessGranted | AccessEventKind::AccessBaselined => {
                state.held = true;
                state.level = payload["level"].as_str().map(str::to_string);
                state.can_read_sensitive = payload["can_read_sensitive"].as_bool().unwrap_or(false);
                state.scope = payload["scope"].as_str().map(str::to_string);
                state.capabilities = string_set(payload.get("capabilities"));
                state.delegable = string_set(payload.get("delegable"));
            }
            AccessEventKind::AccessRevoked => {
                state = AccessHistory {
                    known: true,
                    ..AccessHistory::default()
                };
            }
            AccessEventKind::CapabilityGranted => {
                if let Some(capability) = payload["capability"].as_str() {
                    state.capabilities.insert(capability.to_string());
                }
            }
            AccessEventKind::CapabilityRevoked => {
                if let Some(capability) = payload["capability"].as_str() {
                    state.capabilities.remove(capability);
                }
            }
            AccessEventKind::DelegableCapabilitiesSet => {
                state.delegable = string_set(payload.get("delegable"));
            }
        }
    }
    state
}
