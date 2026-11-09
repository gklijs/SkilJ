//! Decrypted, temporary event history for pure command decisions.

use super::*;
use std::borrow::Cow;

/// Decrypt copies only: the stored history, event cache, fold inputs and
/// rejection history must never receive these plaintext payloads. The locked
/// path uses its existing connection and retains subject key locks to commit.
pub(super) async fn decision_history<'a>(
    db: &mut Db<'_>,
    events: &'a [Event],
    key_service: Option<&KeyService>,
) -> crate::error::Result<Cow<'a, [Event]>> {
    if events
        .iter()
        .all(|event| event.event_type.sensitive_fields.is_empty())
    {
        return Ok(Cow::Borrowed(events));
    }
    let locked = matches!(db, Db::Conn(_));
    let mut resolved = std::collections::HashMap::new();
    let mut decrypted = events.to_vec();
    for event in &mut decrypted {
        // Mixed histories keep unprotected payloads byte-for-byte. They need
        // neither JSON conversion nor cryptographic validation. The slice API
        // still requires owning the complete vector when any event changes.
        if event.event_type.sensitive_fields.is_empty() {
            continue;
        }
        let unavailable = || crate::event_store::Error::SensitiveHistoryUnavailable {
            sequence: event.sequence,
            event_type: event.event_type.name.clone(),
        };
        let mut payload: serde_json::Value =
            serde_json::from_str(&event.payload).map_err(|_| unavailable())?;
        for field in &event.event_type.sensitive_fields {
            let Some(leaf) = crate::event_store::payload_field_value(&payload, &field.field)
                .filter(|value| !value.is_null())
            else {
                continue;
            };
            let ciphertext = leaf.as_str().ok_or_else(unavailable)?.to_string();
            let subject_value =
                crate::event_store::payload_field_value(&payload, &field.subject_field)
                    .and_then(crate::event_store::json_scalar_to_string)
                    .ok_or_else(unavailable)?;
            let subject = (
                event.bounded_context.name.clone(),
                field.subject_key.clone(),
                subject_value.clone(),
            );
            if !resolved.contains_key(&subject) {
                if secrets::is_reserved_subject_key(&field.subject_key) {
                    return Err(unavailable().into());
                }
                let schema = schema_ident(&event.bounded_context.name);
                let lock = if locked { " FOR NO KEY UPDATE" } else { "" };
                let row: EncryptionKeyRow = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT {ENCRYPTION_KEY_COLUMNS} FROM {schema}.encryption_keys \
                     WHERE subject_key = $1 AND subject_value = $2 AND status = 'active'{lock}"
                )))
                .bind(&field.subject_key)
                .bind(&subject_value)
                .fetch_optional(&mut *db.conn().await?)
                .await?
                .ok_or_else(unavailable)?;
                let service = key_service.ok_or(encryption::Error::MasterKeyNotConfigured)?;
                let key = unwrap_row(db, &event.bounded_context.name, service, &row).await?;
                resolved.insert(subject.clone(), key);
            }
            let plaintext = encryption::decrypt_leaf(&resolved[&subject], &ciphertext)
                .map_err(|_| unavailable())?;
            *crate::event_store::payload_field_value_mut(&mut payload, &field.field)
                .expect("the selected leaf exists") = serde_json::Value::String(plaintext);
        }
        event.payload = serde_json::to_string(&payload).expect("JSON value serializes");
    }
    Ok(Cow::Owned(decrypted))
}

#[cfg(test)]
mod tests;
