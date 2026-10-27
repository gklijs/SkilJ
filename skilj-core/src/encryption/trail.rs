//! The master-key trail (docs/architecture.md §230): when the encryption
//! master key in use changed, and when each bounded context's data keys
//! were rewrapped under it, as events in the `admin` bounded context -
//! the same place, origin and shape as the access trail
//! (`crate::access_trail`), so "which master key protected this data on
//! that date" is answered from stored facts.
//!
//! Payloads carry fingerprints (`EncryptionMasterKey::fingerprint`), never
//! a key: a fingerprint is an HMAC of a fixed label, which tells keys apart
//! and reveals nothing about them.

use crate::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME;
use crate::event_store::{BoundedContext, Event, EventOrigin, EventType};
use crate::shared::{Metadata, Tag, TagMapping};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

/// The two event types SkilJ registers in the admin bounded context for
/// the master key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MasterKeyEventKind {
    /// A master key became the one new data keys are wrapped under: the
    /// first one a deployment used, or a rotation's new key.
    MasterKeyActivated,
    /// A bounded context's data keys were rewrapped under the master key
    /// in use.
    DataKeysRewrapped,
}

impl MasterKeyEventKind {
    /// Every kind, in the order they are registered.
    pub const ALL: [MasterKeyEventKind; 2] = [
        MasterKeyEventKind::MasterKeyActivated,
        MasterKeyEventKind::DataKeysRewrapped,
    ];

    /// The event type's name in the admin bounded context.
    pub fn name(self) -> &'static str {
        match self {
            MasterKeyEventKind::MasterKeyActivated => "EncryptionMasterKeyActivated",
            MasterKeyEventKind::DataKeysRewrapped => "EncryptionDataKeysRewrapped",
        }
    }

    /// The payload fields, as `(name, JSON Schema type, required)`.
    fn fields(self) -> &'static [(&'static str, &'static str, bool)] {
        match self {
            MasterKeyEventKind::MasterKeyActivated => &[
                ("fingerprint", "string", true),
                ("previous_fingerprint", "string", false),
                ("actor_role_id", "string", false),
            ],
            MasterKeyEventKind::DataKeysRewrapped => &[
                ("fingerprint", "string", true),
                ("bounded_context", "string", true),
                ("rewrapped", "integer", true),
                ("unreadable", "integer", true),
                ("actor_role_id", "string", false),
            ],
        }
    }

    /// The tags, as `(tag key, payload field)`.
    fn tags(self) -> &'static [(&'static str, &'static str)] {
        match self {
            MasterKeyEventKind::MasterKeyActivated => &[("master_key", "fingerprint")],
            MasterKeyEventKind::DataKeysRewrapped => &[
                ("master_key", "fingerprint"),
                ("bounded_context", "bounded_context"),
            ],
        }
    }
}

/// The JSON Schema of the kind's payload.
pub fn master_key_event_schema(kind: MasterKeyEventKind) -> String {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for (name, ty, is_required) in kind.fields() {
        let property = if *is_required {
            json!({ "type": ty })
        } else {
            json!({ "type": [ty, "null"] })
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

/// The event type as registered in the admin bounded context: creatable
/// by nobody but the library, like the access trail's.
pub fn master_key_event_type(admin: &BoundedContext, kind: MasterKeyEventKind) -> EventType {
    EventType {
        bounded_context: admin.clone(),
        name: kind.name().to_string(),
        schema: master_key_event_schema(kind),
        schema_version: 1,
        tag_mappings: kind
            .tags()
            .iter()
            .map(|(key, field)| TagMapping::new(*key, *field))
            .collect(),
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

/// One master-key event to record.
#[derive(Debug, Clone)]
pub struct MasterKeyChange {
    /// Which event this is.
    pub kind: MasterKeyEventKind,
    /// The payload (see [`MasterKeyEventKind`]).
    pub payload: serde_json::Map<String, Value>,
}

impl MasterKeyChange {
    /// `EncryptionMasterKeyActivated`: `fingerprint` is now the key new
    /// data keys are wrapped under, replacing `previous` (`None` for the
    /// first). `actor_role_id` is the reconciliation Role that rotated,
    /// `None` when startup recorded the key a deployment already used.
    pub fn activated(
        fingerprint: &str,
        previous: Option<&str>,
        actor_role_id: Option<&str>,
    ) -> Self {
        Self::new(
            MasterKeyEventKind::MasterKeyActivated,
            json!({
                "fingerprint": fingerprint,
                "previous_fingerprint": previous,
                "actor_role_id": actor_role_id,
            }),
        )
    }

    /// `EncryptionDataKeysRewrapped`: `rewrapped` of `bounded_context`'s
    /// active data keys are now wrapped under `fingerprint`; `unreadable`
    /// ones could be unwrapped by no configured key and were left as
    /// they were.
    pub fn rewrapped(
        fingerprint: &str,
        bounded_context: &str,
        rewrapped: u64,
        unreadable: u64,
        actor_role_id: &str,
    ) -> Self {
        Self::new(
            MasterKeyEventKind::DataKeysRewrapped,
            json!({
                "fingerprint": fingerprint,
                "bounded_context": bounded_context,
                "rewrapped": rewrapped,
                "unreadable": unreadable,
                "actor_role_id": actor_role_id,
            }),
        )
    }

    fn new(kind: MasterKeyEventKind, payload: Value) -> Self {
        let Value::Object(payload) = payload else {
            unreachable!("every constructor above passes a JSON object")
        };
        Self { kind, payload }
    }

    /// The event for this change at `sequence`: `SkiljRecorded`,
    /// `client_id` the literal `"system"`, tagged as the type maps it.
    pub fn into_event(self, admin: &BoundedContext, sequence: i64, now: DateTime<Utc>) -> Event {
        debug_assert_eq!(admin.name, ADMIN_BOUNDED_CONTEXT_NAME);
        let event_type = master_key_event_type(admin, self.kind);
        let tags = self
            .kind
            .tags()
            .iter()
            .map(|(key, field)| Tag {
                key: (*key).to_string(),
                value: self
                    .payload
                    .get(*field)
                    .and_then(Value::as_str)
                    .map(str::to_string),
            })
            .collect();
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
            payload: Value::Object(self.payload).to_string(),
            event_type,
            sequence,
            tags,
            encryption_keys: Vec::new(),
            origin: EventOrigin::SkiljRecorded,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admin() -> BoundedContext {
        BoundedContext {
            name: ADMIN_BOUNDED_CONTEXT_NAME.to_string(),
            status: crate::event_store::BoundedContextStatus::Active,
            created_at: Utc::now(),
            created_by: crate::bootstrap::ContextCreator::SystemCreator,
            template: None,
        }
    }

    #[test]
    fn a_rewrap_event_is_tagged_by_key_and_bounded_context_and_matches_its_schema() {
        let event = MasterKeyChange::rewrapped("ab12", "banking", 3, 1, "role-1").into_event(
            &admin(),
            7,
            Utc::now(),
        );
        assert_eq!(event.origin, EventOrigin::SkiljRecorded);
        assert_eq!(event.metadata.client_id, "system");
        let tags: Vec<_> = event
            .tags
            .iter()
            .map(|t| (t.key.as_str(), t.value.as_deref()))
            .collect();
        assert_eq!(
            tags,
            vec![
                ("master_key", Some("ab12")),
                ("bounded_context", Some("banking"))
            ]
        );
        let schema: Value = serde_json::from_str(&event.event_type.schema).unwrap();
        let payload: Value = serde_json::from_str(&event.payload).unwrap();
        assert!(jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&payload));
    }

    #[test]
    fn the_first_activation_has_no_previous_key_or_actor() {
        let event =
            MasterKeyChange::activated("ab12", None, None).into_event(&admin(), 0, Utc::now());
        let payload: Value = serde_json::from_str(&event.payload).unwrap();
        assert_eq!(payload["previous_fingerprint"], Value::Null);
        assert_eq!(payload["actor_role_id"], Value::Null);
        let schema: Value = serde_json::from_str(&event.event_type.schema).unwrap();
        assert!(jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&payload));
    }
}
