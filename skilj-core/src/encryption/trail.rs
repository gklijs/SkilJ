//! The master-key trail (docs/architecture.md §230): every rotation of
//! the master key, every bounded context's data keys rewrapped under the
//! new version, and every retirement of the old versions, as events in
//! the `admin` bounded context - the same place, origin and shape as the
//! access trail (`crate::access_trail`), so when a key version came into
//! use and when it stopped being usable is answered from stored facts.
//!
//! Payloads name the provider and versions, never a key.

use crate::bootstrap::ADMIN_BOUNDED_CONTEXT_NAME;
use crate::event_store::{BoundedContext, Event, EventOrigin, EventType};
use crate::shared::{Metadata, Tag, TagMapping};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

/// The event types SkilJ registers in the admin bounded context for the
/// master key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MasterKeyEventKind {
    /// A master key version became the one new data keys are wrapped
    /// under: a rotation, or the version in use when the trail began.
    MasterKeyRotated,
    /// A bounded context's data keys were rewrapped under it.
    DataKeysRewrapped,
    /// Every version below one was made unusable at the provider.
    MasterKeyRetired,
}

/// Why a version came into use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RotationReason {
    /// The version in use when skilj first recorded one.
    Adopted,
    /// A superadmin asked for it.
    Requested,
    /// The rotation schedule came due.
    Scheduled,
    /// The provider's key was rotated outside skilj (with Vault's own
    /// CLI, say), and skilj noticed.
    External,
}

impl RotationReason {
    /// As the payload spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            RotationReason::Adopted => "adopted",
            RotationReason::Requested => "requested",
            RotationReason::Scheduled => "scheduled",
            RotationReason::External => "external",
        }
    }
}

impl MasterKeyEventKind {
    /// Every kind, in the order they are registered.
    pub const ALL: [MasterKeyEventKind; 3] = [
        MasterKeyEventKind::MasterKeyRotated,
        MasterKeyEventKind::DataKeysRewrapped,
        MasterKeyEventKind::MasterKeyRetired,
    ];

    /// The event type's name in the admin bounded context.
    pub fn name(self) -> &'static str {
        match self {
            MasterKeyEventKind::MasterKeyRotated => "EncryptionMasterKeyRotated",
            MasterKeyEventKind::DataKeysRewrapped => "EncryptionDataKeysRewrapped",
            MasterKeyEventKind::MasterKeyRetired => "EncryptionMasterKeyRetired",
        }
    }

    /// The payload fields, as `(name, JSON Schema type, required)`.
    fn fields(self) -> &'static [(&'static str, &'static str, bool)] {
        match self {
            MasterKeyEventKind::MasterKeyRotated => &[
                ("provider", "string", true),
                ("version", "integer", true),
                ("previous_version", "integer", false),
                ("reason", "string", true),
                ("retire_immediately", "boolean", true),
                ("actor_role_id", "string", false),
            ],
            MasterKeyEventKind::DataKeysRewrapped => &[
                ("provider", "string", true),
                ("version", "integer", true),
                ("bounded_context", "string", true),
                ("rewrapped", "integer", true),
                ("unreadable", "integer", true),
            ],
            MasterKeyEventKind::MasterKeyRetired => &[
                ("provider", "string", true),
                ("below_version", "integer", true),
            ],
        }
    }

    /// The tags, as `(tag key, payload field)`.
    fn tags(self) -> &'static [(&'static str, &'static str)] {
        match self {
            MasterKeyEventKind::DataKeysRewrapped => &[
                ("master_key", "provider"),
                ("bounded_context", "bounded_context"),
            ],
            _ => &[("master_key", "provider")],
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
    /// `EncryptionMasterKeyRotated`: `version` of `provider`'s key is now
    /// the one in use, after `previous` (`None` when the trail begins).
    /// `actor_role_id` is the superadmin who asked, `None` otherwise.
    pub fn rotated(
        provider: &str,
        version: u32,
        previous: Option<u32>,
        reason: RotationReason,
        retire_immediately: bool,
        actor_role_id: Option<&str>,
    ) -> Self {
        Self::new(
            MasterKeyEventKind::MasterKeyRotated,
            json!({
                "provider": provider,
                "version": version,
                "previous_version": previous,
                "reason": reason.as_str(),
                "retire_immediately": retire_immediately,
                "actor_role_id": actor_role_id,
            }),
        )
    }

    /// `EncryptionDataKeysRewrapped`: `rewrapped` of `bounded_context`'s
    /// active data keys are now wrapped under `version`; `unreadable` ones
    /// could not be unwrapped and were left as they were.
    pub fn rewrapped(
        provider: &str,
        version: u32,
        bounded_context: &str,
        rewrapped: u64,
        unreadable: u64,
    ) -> Self {
        Self::new(
            MasterKeyEventKind::DataKeysRewrapped,
            json!({
                "provider": provider,
                "version": version,
                "bounded_context": bounded_context,
                "rewrapped": rewrapped,
                "unreadable": unreadable,
            }),
        )
    }

    /// `EncryptionMasterKeyRetired`: every version of `provider`'s key
    /// below `below_version` is unusable now.
    pub fn retired(provider: &str, below_version: u32) -> Self {
        Self::new(
            MasterKeyEventKind::MasterKeyRetired,
            json!({ "provider": provider, "below_version": below_version }),
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

    fn valid(event: &Event) -> bool {
        let schema: Value = serde_json::from_str(&event.event_type.schema).unwrap();
        let payload: Value = serde_json::from_str(&event.payload).unwrap();
        jsonschema::validator_for(&schema)
            .unwrap()
            .is_valid(&payload)
    }

    #[test]
    fn every_event_matches_its_schema_and_is_tagged_by_provider() {
        let events = [
            MasterKeyChange::rotated("file", 1, None, RotationReason::Adopted, false, None),
            MasterKeyChange::rotated(
                "file",
                2,
                Some(1),
                RotationReason::Requested,
                true,
                Some("r"),
            ),
            MasterKeyChange::rewrapped("file", 2, "banking", 3, 0),
            MasterKeyChange::retired("file", 2),
        ];
        for change in events {
            let event = change.into_event(&admin(), 1, Utc::now());
            assert!(
                valid(&event),
                "{} matches its schema",
                event.event_type.name
            );
            assert_eq!(event.origin, EventOrigin::SkiljRecorded);
            assert_eq!(event.tags[0].key, "master_key");
            assert_eq!(event.tags[0].value.as_deref(), Some("file"));
        }
    }

    #[test]
    fn a_rewrap_is_tagged_by_its_bounded_context_too() {
        let event = MasterKeyChange::rewrapped("file", 2, "banking", 3, 0).into_event(
            &admin(),
            1,
            Utc::now(),
        );
        assert_eq!(event.tags[1].key, "bounded_context");
        assert_eq!(event.tags[1].value.as_deref(), Some("banking"));
    }
}
