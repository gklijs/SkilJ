//! render_event, render_command, redact_private_fields,
//! render_would_be_event and is_default_private_reader.

use serde::Serialize;
use serde_json::Value;
use sqlx::Row;

use super::Skilj;
use crate::error::Result;
use crate::model::*;
use crate::schema::{read_path, read_path_mut};
use crate::tags::scalar_tag_value;

/// A record as one caller may see it.
#[derive(Debug, Clone, Serialize)]
pub struct Rendered {
    pub bounded_context: String,
    /// Event sequence, or None for a command / would-be event.
    pub sequence: Option<i64>,
    pub command_id: Option<String>,
    #[serde(rename = "type")]
    pub type_name: String,
    pub payload: Value,
    pub metadata: Option<Metadata>,
    pub tags: Vec<Tag>,
    pub origin: Option<Origin>,
}

/// The private-field grants that name one Role as grantee in one context,
/// loaded once per read.
pub(crate) struct PrivateAccess {
    grants: Vec<PrivateFieldGrant>,
}

impl Skilj {
    pub(crate) async fn private_access(&self, grant: &Grant) -> Result<PrivateAccess> {
        let rows = sqlx::query(&format!(
            "select {} from private_field_grants where bounded_context = $1 and grantee = $2 and active",
            super::rows::GRANT_COLUMNS
        ))
        .bind(&grant.mapping.bounded_context)
        .bind(&grant.role.id)
        .fetch_all(&self.inner.pool)
        .await?;
        Ok(PrivateAccess {
            grants: rows
                .iter()
                .map(super::rows::pfg_from_row)
                .collect::<Result<_>>()?,
        })
    }

    /// render_event(event, access_mapping).
    pub(crate) async fn render_event(
        &self,
        e: &StoredEvent,
        grant: &Grant,
        pa: &PrivateAccess,
    ) -> Result<Rendered> {
        let ty = self
            .inner
            .registry
            .read()
            .await
            .event_type(&e.bounded_context, &e.event_type)
            .ok();
        let payload = self.decrypt_for(grant, &e.payload).await?;
        let payload = match &ty {
            Some(t) => redact_for(
                &t.0.private_fields,
                payload,
                Record::Event(e),
                grant,
                pa,
                &self.role_subjects(pa).await?,
            ),
            None => payload,
        };
        Ok(Rendered {
            bounded_context: e.bounded_context.clone(),
            sequence: Some(e.sequence),
            command_id: match &e.origin {
                Origin::CommandTriggered { command_id } => Some(command_id.clone()),
                _ => None,
            },
            type_name: e.event_type.clone(),
            payload,
            metadata: Some(e.metadata.clone()),
            tags: e.tags.iter().cloned().collect(),
            origin: Some(e.origin.clone()),
        })
    }

    /// render_command(command, access_mapping).
    pub(crate) async fn render_command(
        &self,
        c: &StoredCommand,
        grant: &Grant,
        pa: &PrivateAccess,
    ) -> Result<Rendered> {
        let ty = self
            .inner
            .registry
            .read()
            .await
            .command_type(&c.bounded_context, &c.command_type)
            .ok();
        let payload = self.decrypt_for(grant, &c.payload).await?;
        let payload = match &ty {
            Some(t) => redact_for(
                &t.0.private_fields,
                payload,
                Record::Command(c),
                grant,
                pa,
                &self.role_subjects(pa).await?,
            ),
            None => payload,
        };
        Ok(Rendered {
            bounded_context: c.bounded_context.clone(),
            sequence: None,
            command_id: Some(c.id.clone()),
            type_name: c.command_type.clone(),
            payload,
            metadata: Some(c.metadata.clone()),
            tags: c.consistency_tags.iter().cloned().collect(),
            origin: None,
        })
    }

    /// render_would_be_event: nothing was encrypted; a sensitive field the
    /// caller isn't granted is null. Only blanket private grants apply.
    pub(crate) async fn render_would_be(
        &self,
        bc: &str,
        spec: &crate::decl::EventSpec,
        client_id: &str,
        grant: &Grant,
        pa: &PrivateAccess,
    ) -> Result<Rendered> {
        let ty = self
            .inner
            .registry
            .read()
            .await
            .event_type(bc, &spec.event_type)?;
        let mut payload = spec.payload.clone();
        for f in &ty.0.sensitive_fields {
            let subject = read_path(&spec.payload, &f.subject_field).and_then(scalar_tag_value);
            let granted = grant.mapping.can_read_sensitive
                || subject.as_deref() == Some(grant.role.external_subject.as_str());
            if !granted {
                if let Some(leaf) = read_path_mut(&mut payload, &f.field) {
                    *leaf = Value::Null;
                }
            }
        }
        let fake = StoredEvent {
            bounded_context: bc.into(),
            sequence: -1,
            event_type: spec.event_type.clone(),
            payload: spec.payload.clone(),
            metadata: Metadata {
                type_name: spec.event_type.clone(),
                version: ty.0.schema_version,
                client_id: client_id.into(),
                created_at: Self::now(),
                correlation_id: String::new(),
                causation_id: None,
            },
            tags: Default::default(),
            encryption_key_ids: vec![],
            origin: Origin::DirectlyCreated,
        };
        let only_blanket = PrivateAccess {
            grants: pa
                .grants
                .iter()
                .filter(|g| g.event_sequence.is_none() && g.command_id.is_none())
                .cloned()
                .collect(),
        };
        let subjects = self.role_subjects(&only_blanket).await?;
        let payload = redact_for(
            &ty.0.private_fields,
            payload,
            Record::Event(&fake),
            grant,
            &only_blanket,
            &subjects,
        );
        Ok(Rendered {
            bounded_context: bc.into(),
            sequence: None,
            command_id: None,
            type_name: spec.event_type.clone(),
            payload,
            metadata: None,
            tags: vec![],
            origin: None,
        })
    }

    /// Decrypt each sensitive leaf the grant is entitled to: either
    /// can_read_sensitive, or the leaf's subject is the caller's own
    /// external subject.
    pub(crate) async fn decrypt_for(&self, grant: &Grant, v: &Value) -> Result<Value> {
        let all = grant.mapping.can_read_sensitive;
        let me = grant.role.external_subject.clone();
        self.decrypt_where(v, &move |subject: &str| all || subject == me)
            .await
    }

    /// External subjects of the grantors of blanket grants (needed to
    /// re-derive their default-reader standing for addressed fields).
    async fn role_subjects(
        &self,
        pa: &PrivateAccess,
    ) -> Result<std::collections::BTreeMap<String, String>> {
        let ids: Vec<String> = pa
            .grants
            .iter()
            .filter(|g| g.event_sequence.is_none() && g.command_id.is_none())
            .map(|g| g.grantor_role_id.clone())
            .collect();
        if ids.is_empty() {
            return Ok(Default::default());
        }
        let rows = sqlx::query("select id, external_subject from roles where id = any($1)")
            .bind(&ids)
            .fetch_all(&self.inner.pool)
            .await?;
        let mut m = std::collections::BTreeMap::new();
        for r in rows {
            m.insert(
                r.try_get::<String, _>("id")?,
                r.try_get::<String, _>("external_subject")?,
            );
        }
        Ok(m)
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Record<'a> {
    Event(&'a StoredEvent),
    Command(&'a StoredCommand),
}

impl Record<'_> {
    fn client_id(&self) -> &str {
        match self {
            Record::Event(e) => &e.metadata.client_id,
            Record::Command(c) => &c.metadata.client_id,
        }
    }
    fn payload(&self) -> &Value {
        match self {
            Record::Event(e) => &e.payload,
            Record::Command(c) => &c.payload,
        }
    }
    fn names(&self, g: &PrivateFieldGrant) -> bool {
        match self {
            Record::Event(e) => g.event_sequence == Some(e.sequence) && e.sequence >= 0,
            Record::Command(c) => g.command_id.as_deref() == Some(c.id.as_str()),
        }
    }
}

/// is_default_private_reader(record, role id, external subject).
pub(crate) fn is_default_private_reader(
    fields: &[PrivateField],
    record: Record<'_>,
    role_id: &str,
    external_subject: &str,
) -> bool {
    fields.iter().any(|p| match p.kind {
        PrivateFieldKind::Own => record.client_id() == role_id,
        PrivateFieldKind::Addressed => p
            .addressee_field
            .as_deref()
            .and_then(|a| read_path(record.payload(), a))
            .and_then(scalar_tag_value)
            .is_some_and(|v| v == external_subject),
        PrivateFieldKind::Capability => false,
    })
}

fn redact_for(
    fields: &[PrivateField],
    mut payload: Value,
    record: Record<'_>,
    grant: &Grant,
    pa: &PrivateAccess,
    grantor_subjects: &std::collections::BTreeMap<String, String>,
) -> Value {
    if fields.is_empty() {
        return payload;
    }
    // Entitlement is decided per field, against that field's own kind. The
    // spec's is_default_private_reader is record-level ("some own-kind
    // field ... or some addressed-kind field ..."), which would let the
    // addressee read the creator's own-kind fields and vice versa; we read
    // that as unintended.
    let shared = pa.grants.iter().any(|g| record.names(g));
    let blankets: Vec<&PrivateFieldGrant> = pa
        .grants
        .iter()
        .filter(|g| g.event_sequence.is_none() && g.command_id.is_none())
        .collect();
    for p in fields {
        let entitled = match p.kind {
            PrivateFieldKind::Capability => p
                .capability
                .as_ref()
                .is_some_and(|c| grant.mapping.capabilities.contains(c)),
            PrivateFieldKind::Own | PrivateFieldKind::Addressed => {
                let one = std::slice::from_ref(p);
                is_default_private_reader(one, record, &grant.role.id, &grant.role.external_subject)
                    || shared
                    || blankets.iter().any(|g| {
                        let subj = grantor_subjects
                            .get(&g.grantor_role_id)
                            .map(String::as_str)
                            .unwrap_or("");
                        is_default_private_reader(one, record, &g.grantor_role_id, subj)
                    })
            }
        };
        if !entitled {
            if let Some(leaf) = read_path_mut(&mut payload, &p.field) {
                *leaf = Value::Null;
            }
        }
    }
    payload
}

/// redact_private_fields: the REST track nulls every private leaf.
pub(crate) fn redact_all(fields: &[PrivateField], mut payload: Value) -> Value {
    for p in fields {
        if let Some(leaf) = read_path_mut(&mut payload, &p.field) {
            *leaf = Value::Null;
        }
    }
    payload
}

/// Rendering for the REST track: as stored, private leaves nulled.
pub(crate) fn render_rest(e: &StoredEvent, ty: &EventTypeDef) -> Rendered {
    Rendered {
        bounded_context: e.bounded_context.clone(),
        sequence: Some(e.sequence),
        command_id: match &e.origin {
            Origin::CommandTriggered { command_id } => Some(command_id.clone()),
            _ => None,
        },
        type_name: e.event_type.clone(),
        payload: redact_all(&ty.private_fields, e.payload.clone()),
        metadata: Some(e.metadata.clone()),
        tags: e.tags.iter().cloned().collect(),
        origin: None,
    }
}

impl Skilj {
    /// Render one stored event for a GraphQL caller (e.g. the events its own
    /// command just triggered).
    pub async fn render_for(&self, grant: &Grant, e: &StoredEvent) -> Result<Rendered> {
        let pa = self.private_access(grant).await?;
        self.render_event(e, grant, &pa).await
    }
}
