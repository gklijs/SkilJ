//! EncryptionKey provisioning and resolution (protect_sensitive_fields and
//! the decrypting half of render_event/read_projection).

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use sqlx::Row;

use super::Skilj;
use crate::crypto;
use crate::error::{Error, Result};
use crate::model::{PayloadType, SensitiveField};
use crate::schema::{read_path, read_path_mut};
use crate::tags::scalar_tag_value;

/// A payload with its sensitive leaves replaced, and the keys used.
#[derive(Debug, Clone)]
pub struct Protected {
    pub payload: Value,
    pub key_ids: Vec<String>,
}

impl Skilj {
    /// Get or create the active key for a subject, outside any write
    /// transaction (the spec: provisioning before the sequence lock).
    pub(crate) async fn provision_key(
        &self,
        bc: &str,
        subject_key: &str,
        subject_value: &str,
    ) -> Result<(String, Vec<u8>)> {
        loop {
            if let Some(row) = sqlx::query(
                "select id, wrapped, master_key_id from encryption_keys
                 where bounded_context = $1 and subject_key = $2 and subject_value = $3 and status = 'active'",
            )
            .bind(bc)
            .bind(subject_key)
            .bind(subject_value)
            .fetch_optional(&self.inner.pool)
            .await?
            {
                let id: String = row.try_get("id")?;
                let key = self.unwrap_row(&id, &row)?;
                return Ok((id, key));
            }
            let id = crypto::generate_id();
            let data_key = crypto::new_data_key();
            let wrapped = self.inner.master.wrap(&data_key)?;
            let inserted = sqlx::query(
                "insert into encryption_keys (id, bounded_context, subject_key, subject_value, wrapped, master_key_id, status, created_at)
                 values ($1, $2, $3, $4, $5, $6, 'active', $7) on conflict do nothing",
            )
            .bind(&id)
            .bind(bc)
            .bind(subject_key)
            .bind(subject_value)
            .bind(&wrapped)
            .bind(self.inner.master.key_id())
            .bind(Self::now())
            .execute(&self.inner.pool)
            .await?
            .rows_affected();
            if inserted == 1 {
                self.inner
                    .data_keys
                    .lock()
                    .await
                    .insert(id.clone(), Some(data_key.to_vec()));
                return Ok((id, data_key.to_vec()));
            }
            // Lost the race to a concurrent provisioning: read theirs.
        }
    }

    fn unwrap_row(&self, _id: &str, row: &sqlx::postgres::PgRow) -> Result<Vec<u8>> {
        let wrapped: Option<Vec<u8>> = row.try_get("wrapped")?;
        let master_id: String = row.try_get("master_key_id")?;
        let wrapped = wrapped.ok_or_else(|| Error::Crypto("key destroyed".into()))?;
        self.inner.master.unwrap(&master_id, &wrapped)
    }

    /// The data key behind a key id, or None once destroyed.
    pub(crate) async fn data_key(&self, id: &str) -> Result<Option<Vec<u8>>> {
        if let Some(k) = self.inner.data_keys.lock().await.get(id) {
            return Ok(k.clone());
        }
        let row =
            sqlx::query("select wrapped, master_key_id, status from encryption_keys where id = $1")
                .bind(id)
                .fetch_optional(&self.inner.pool)
                .await?;
        let key = match row {
            None => None,
            Some(row) => {
                let status: String = row.try_get("status")?;
                if status == "active" {
                    Some(self.unwrap_row(id, &row)?)
                } else {
                    None
                }
            }
        };
        self.inner
            .data_keys
            .lock()
            .await
            .insert(id.to_string(), key.clone());
        Ok(key)
    }

    pub(crate) async fn forget_cached_key(&self, id: &str) {
        self.inner
            .data_keys
            .lock()
            .await
            .insert(id.to_string(), None);
    }

    /// protect_sensitive_fields(type, payload).
    pub(crate) async fn protect(
        &self,
        ty: &(dyn PayloadType + Sync),
        payload: &Value,
    ) -> Result<Protected> {
        let fields = ty.sensitive_fields();
        if fields.is_empty() {
            return Ok(Protected {
                payload: payload.clone(),
                key_ids: vec![],
            });
        }
        let mut out = payload.clone();
        let mut used = BTreeSet::new();
        let mut keys: BTreeMap<(String, String), (String, Vec<u8>)> = BTreeMap::new();
        for f in fields {
            let Some(subject) = subject_value(payload, f) else {
                // No subject: nothing to key the field to. The spec doesn't
                // say; an unkeyable sensitive value is refused rather than
                // stored in the clear.
                if read_path(payload, &f.field).is_some_and(|v| !v.is_null()) {
                    return Err(Error::invalid(format!(
                        "sensitive field '{}' has a value but its subject field '{}' is absent",
                        f.field, f.subject_field
                    )));
                }
                continue;
            };
            let Some(leaf) = read_path_mut(&mut out, &f.field) else {
                continue;
            };
            if leaf.is_null() {
                continue;
            }
            let k = (f.subject_key.clone(), subject.clone());
            if !keys.contains_key(&k) {
                let got = self
                    .provision_key(ty.bounded_context(), &f.subject_key, &subject)
                    .await?;
                keys.insert(k.clone(), got);
            }
            let (id, data_key) = &keys[&k];
            *leaf = Value::String(crypto::encrypt_leaf(id, data_key, leaf)?);
            used.insert(id.clone());
        }
        Ok(Protected {
            payload: out,
            key_ids: used.into_iter().collect(),
        })
    }

    /// Decrypt every ciphertext leaf anywhere in a JSON value for which
    /// `allow(key subject_value)` holds and whose key is still active.
    pub(crate) async fn decrypt_where(
        &self,
        v: &Value,
        allow: &(dyn Fn(&str) -> bool + Send + Sync),
    ) -> Result<Value> {
        let mut out = v.clone();
        let mut ids = BTreeSet::new();
        collect_key_ids(&out, &mut ids);
        if ids.is_empty() {
            return Ok(out);
        }
        let mut usable: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for id in ids {
            let Some(subject) = self.key_subject(&id).await? else {
                continue;
            };
            if !allow(&subject) {
                continue;
            }
            if let Some(k) = self.data_key(&id).await? {
                usable.insert(id, k);
            }
        }
        decrypt_in_place(&mut out, &usable)?;
        Ok(out)
    }

    pub(crate) async fn key_subject(&self, id: &str) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("select subject_value from encryption_keys where id = $1")
                .bind(id)
                .fetch_optional(&self.inner.pool)
                .await?,
        )
    }
}

pub(crate) fn subject_value(payload: &Value, f: &SensitiveField) -> Option<String> {
    read_path(payload, &f.subject_field).and_then(scalar_tag_value)
}

fn collect_key_ids(v: &Value, out: &mut BTreeSet<String>) {
    match v {
        Value::String(_) => {
            if let Some(id) = crypto::ciphertext_key_id(v) {
                out.insert(id.to_string());
            }
        }
        Value::Array(a) => a.iter().for_each(|x| collect_key_ids(x, out)),
        Value::Object(o) => o.values().for_each(|x| collect_key_ids(x, out)),
        _ => {}
    }
}

fn decrypt_in_place(v: &mut Value, keys: &BTreeMap<String, Vec<u8>>) -> Result<()> {
    match v {
        Value::String(_) => {
            if let Some(id) = crypto::ciphertext_key_id(v) {
                if let Some(k) = keys.get(id) {
                    *v = crypto::decrypt_leaf(k, v)?;
                }
            }
        }
        Value::Array(a) => {
            for x in a {
                decrypt_in_place(x, keys)?;
            }
        }
        Value::Object(o) => {
            for x in o.values_mut() {
                decrypt_in_place(x, keys)?;
            }
        }
        _ => {}
    }
    Ok(())
}
