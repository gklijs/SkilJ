//! Persistence for command types rule groups.

use super::*;

// --- CommandType ---

#[derive(sqlx::FromRow)]
pub(super) struct CommandTypeRow {
    pub(super) name: String,
    pub(super) schema: String,
    pub(super) schema_version: i64,
    pub(super) tag_mappings: Json<Vec<TagMapping>>,
    pub(super) owner_tag_key: Option<String>,
    pub(super) sensitive_fields: Json<Vec<SensitiveField>>,
    pub(super) rest_trigger_allowed: bool,
    pub(super) private_fields: Json<Vec<PrivateField>>,
    pub(super) blob_fields: Json<Vec<crate::blob::BlobField>>,
}

impl CommandTypeRow {
    pub(super) fn into_domain(self, bounded_context: BoundedContext) -> CommandType {
        CommandType {
            bounded_context,
            name: self.name,
            schema: self.schema,
            schema_version: self.schema_version,
            tag_mappings: self.tag_mappings.0,
            owner_tag_key: self.owner_tag_key,
            sensitive_fields: self.sensitive_fields.0,
            private_fields: self.private_fields.0,
            blob_fields: self.blob_fields.0,
            rest_trigger_allowed: self.rest_trigger_allowed,
        }
    }
}

pub(super) const COMMAND_TYPE_COLUMNS: &str =
    "name, schema, schema_version, tag_mappings, owner_tag_key, \
    sensitive_fields, rest_trigger_allowed, private_fields, blob_fields";

/// See `upsert_event_type` above - same shape and reasoning.
#[tracing::instrument(skip_all)]
pub async fn upsert_command_type(pool: &Pool, ct: &CommandType) -> crate::error::Result<()> {
    check_blind_tag_mappings(pool, &ct.bounded_context.name, &ct.tag_mappings).await?;
    let schema = schema_ident(&ct.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.command_types ({COMMAND_TYPE_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
         ON CONFLICT (name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            tag_mappings = EXCLUDED.tag_mappings, owner_tag_key = EXCLUDED.owner_tag_key, \
            sensitive_fields = EXCLUDED.sensitive_fields, \
            rest_trigger_allowed = EXCLUDED.rest_trigger_allowed, \
            private_fields = EXCLUDED.private_fields, \
            blob_fields = EXCLUDED.blob_fields"
    )))
    .bind(&ct.name)
    .bind(&ct.schema)
    .bind(ct.schema_version)
    .bind(Json(&ct.tag_mappings))
    .bind(&ct.owner_tag_key)
    .bind(Json(&ct.sensitive_fields))
    .bind(ct.rest_trigger_allowed)
    .bind(Json(&ct.private_fields))
    .bind(Json(&ct.blob_fields))
    .execute(pool)
    .await?;
    notify_registration_changed(pool).await;
    Ok(())
}
