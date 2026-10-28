//! Row <-> model mapping.

use std::collections::BTreeSet;

use sqlx::postgres::PgRow;
use sqlx::Row;

use crate::error::{Error, Result};
use crate::model::*;

pub(crate) const EVENT_COLUMNS: &str =
    "bounded_context, sequence, event_type, payload, metadata, tags, encryption_key_ids, origin";

pub(crate) fn event_from_row(row: &PgRow) -> Result<StoredEvent> {
    Ok(StoredEvent {
        bounded_context: row.try_get("bounded_context")?,
        sequence: row.try_get("sequence")?,
        event_type: row.try_get("event_type")?,
        payload: row.try_get("payload")?,
        metadata: serde_json::from_value(row.try_get("metadata")?)?,
        tags: serde_json::from_value(row.try_get("tags")?)?,
        encryption_key_ids: row.try_get("encryption_key_ids")?,
        origin: serde_json::from_value(row.try_get("origin")?)?,
    })
}

pub(crate) const COMMAND_COLUMNS: &str = "c.id, c.recorded, c.bounded_context, c.command_type, c.payload, c.metadata, \
     c.encryption_key_ids, c.consistency_tags, c.consistency_query, c.consistency_boundary, \
     coalesce((select array_agg(e.sequence order by e.sequence) from events e where e.command_id = c.id), '{}') as triggered";

pub(crate) fn command_from_row(row: &PgRow) -> Result<StoredCommand> {
    Ok(StoredCommand {
        id: row.try_get("id")?,
        recorded: row.try_get("recorded")?,
        bounded_context: row.try_get("bounded_context")?,
        command_type: row.try_get("command_type")?,
        payload: row.try_get("payload")?,
        metadata: serde_json::from_value(row.try_get("metadata")?)?,
        encryption_key_ids: row.try_get("encryption_key_ids")?,
        consistency_tags: serde_json::from_value(row.try_get("consistency_tags")?)?,
        consistency_query: serde_json::from_value(row.try_get("consistency_query")?)?,
        consistency_boundary: row.try_get("consistency_boundary")?,
        triggered_sequences: row.try_get("triggered")?,
    })
}

pub(crate) const ROLE_COLUMNS: &str = "id, external_subject, name, superadmin, active, created_at, revoked_at";

pub(crate) fn role_from_row(row: &PgRow) -> Result<Role> {
    Ok(Role {
        id: row.try_get("id")?,
        external_subject: row.try_get("external_subject")?,
        name: row.try_get("name")?,
        superadmin: row.try_get("superadmin")?,
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

pub(crate) const MAPPING_COLUMNS: &str = "id, role_id, bounded_context, level, can_read_sensitive, scope, capabilities, delegable, active, created_at, revoked_at";

pub(crate) fn mapping_from_row(row: &PgRow) -> Result<AccessMapping> {
    let level: String = row.try_get("level")?;
    let caps: Vec<String> = row.try_get("capabilities")?;
    let del: Vec<String> = row.try_get("delegable")?;
    Ok(AccessMapping {
        id: row.try_get("id")?,
        role_id: row.try_get("role_id")?,
        bounded_context: row.try_get("bounded_context")?,
        level: AccessLevel::parse(&level).ok_or_else(|| Error::Config(format!("bad level {level}")))?,
        can_read_sensitive: row.try_get("can_read_sensitive")?,
        scope: row.try_get("scope")?,
        capabilities: caps.into_iter().collect::<BTreeSet<_>>(),
        delegable: del.into_iter().collect::<BTreeSet<_>>(),
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

pub(crate) const TOKEN_COLUMNS: &str = "id, purpose, bounded_context, type_name, scope, active, created_at, revoked_at, start_from, start_at_sequence, start_at_time";

pub(crate) fn token_from_row(row: &PgRow) -> Result<AccessToken> {
    let purpose: String = row.try_get("purpose")?;
    let start: String = row.try_get("start_from")?;
    Ok(AccessToken {
        id: row.try_get("id")?,
        purpose: TokenPurpose::parse(&purpose).ok_or_else(|| Error::Config(format!("bad purpose {purpose}")))?,
        bounded_context: row.try_get("bounded_context")?,
        type_name: row.try_get("type_name")?,
        scope: row.try_get("scope")?,
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
        start_from: EventReadStartPosition::parse(&start).unwrap_or_default(),
        start_at_sequence: row.try_get("start_at_sequence")?,
        start_at_time: row.try_get("start_at_time")?,
    })
}

pub(crate) const GRANT_COLUMNS: &str =
    "id, bounded_context, grantor, grantee, event_sequence, command_id, active, created_at, revoked_at";

pub(crate) fn pfg_from_row(row: &PgRow) -> Result<PrivateFieldGrant> {
    Ok(PrivateFieldGrant {
        id: row.try_get("id")?,
        bounded_context: row.try_get("bounded_context")?,
        grantor_role_id: row.try_get("grantor")?,
        grantee_role_id: row.try_get("grantee")?,
        event_sequence: row.try_get("event_sequence")?,
        command_id: row.try_get("command_id")?,
        active: row.try_get("active")?,
        created_at: row.try_get("created_at")?,
        revoked_at: row.try_get("revoked_at")?,
    })
}

pub(crate) fn context_from_row(row: &PgRow) -> Result<BoundedContext> {
    let status: String = row.try_get("status")?;
    let by: Option<String> = row.try_get("created_by_role")?;
    Ok(BoundedContext {
        name: row.try_get("name")?,
        status: if status == "active" {
            ContextStatus::Active
        } else {
            ContextStatus::Archived
        },
        created_at: row.try_get("created_at")?,
        created_by: match by {
            Some(role_id) => ContextCreator::Superadmin { role_id },
            None => ContextCreator::System,
        },
        template: row.try_get("template")?,
    })
}

pub(crate) fn tag_arrays(tags: &BTreeSet<Tag>) -> (Vec<String>, Vec<Option<String>>) {
    tags.iter().map(|t| (t.key.clone(), t.value.clone())).unzip()
}
