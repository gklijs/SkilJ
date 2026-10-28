//! Appending events (every origin) and dispatching committed events, in
//! sequence order, to the cache and the bus.

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::{Postgres, Transaction};

use super::rows::{event_from_row, tag_arrays, EVENT_COLUMNS};
use super::{BusMessage, Skilj};
use crate::crypto;
use crate::error::{Error, Result};
use crate::model::*;
use crate::tags::{derive_tags, owner_scope_satisfied};
use crate::validate;

/// An event about to be written.
pub(crate) struct NewEvent {
    pub event_type: String,
    pub payload: Value,
    pub tags: BTreeSet<Tag>,
    pub key_ids: Vec<String>,
    pub metadata: Metadata,
    pub origin: Origin,
    pub command_id: Option<String>,
}

/// One item of SubmitExternalEvents.
#[derive(Debug, Clone, Default)]
pub struct ExternalEventItem {
    pub payload: Value,
    pub source_content: String,
    pub source_context: Option<String>,
    pub dedupe_partition_key: Option<String>,
    pub dedupe_sequence: Option<i64>,
    pub idempotency_key: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

/// What an external submission answers.
#[derive(Debug, Clone)]
pub enum ExternalOutcome {
    Created(StoredEvent),
    /// A redelivery: nothing written. The idempotency-key form knows which
    /// event the message became; the watermark form doesn't.
    Redelivered { sequence: Option<i64> },
    Failed(String, &'static str),
    NotProcessed,
}

impl Skilj {
    /// Lock the bounded context's sequence row; returns the next sequence.
    pub(crate) async fn lock_counter(&self, tx: &mut Transaction<'_, Postgres>, bc: &str) -> Result<i64> {
        let next: Option<i64> =
            sqlx::query_scalar("select next_sequence from sequence_counters where bounded_context = $1 for update")
                .bind(bc)
                .fetch_optional(&mut **tx)
                .await?;
        next.ok_or_else(|| Error::not_found(format!("bounded context {bc}")))
    }

    /// Write events at the next sequences (the counter must already be
    /// locked), fold sync projections, advance the counter, notify.
    pub(crate) async fn append_locked(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        bc: &str,
        mut next: i64,
        events: Vec<NewEvent>,
    ) -> Result<Vec<StoredEvent>> {
        let mut written = Vec::with_capacity(events.len());
        for e in events {
            let stored = StoredEvent {
                bounded_context: bc.to_string(),
                sequence: next,
                event_type: e.event_type,
                payload: e.payload,
                metadata: e.metadata,
                tags: e.tags,
                encryption_key_ids: e.key_ids,
                origin: e.origin,
            };
            sqlx::query(
                "insert into events (bounded_context, sequence, event_type, payload, metadata, tags, encryption_key_ids, origin, command_id, created_at)
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            )
            .bind(bc)
            .bind(stored.sequence)
            .bind(&stored.event_type)
            .bind(&stored.payload)
            .bind(serde_json::to_value(&stored.metadata)?)
            .bind(serde_json::to_value(&stored.tags)?)
            .bind(&stored.encryption_key_ids)
            .bind(serde_json::to_value(&stored.origin)?)
            .bind(&e.command_id)
            .bind(stored.metadata.created_at)
            .execute(&mut **tx)
            .await?;
            let (keys, values) = tag_arrays(&stored.tags);
            sqlx::query(
                "insert into event_tags (bounded_context, sequence, key, value)
                 select $1, $2, k, v from unnest($3::text[], $4::text[]) as t(k, v)",
            )
            .bind(bc)
            .bind(stored.sequence)
            .bind(&keys)
            .bind(&values)
            .execute(&mut **tx)
            .await?;
            self.fold_sync_projections(tx, &stored).await?;
            next += 1;
            written.push(stored);
        }
        if !written.is_empty() {
            sqlx::query("update sequence_counters set next_sequence = $2 where bounded_context = $1")
                .bind(bc)
                .bind(next)
                .execute(&mut **tx)
                .await?;
            sqlx::query(
                "update projections set caught_up_to = $2
                 where bounded_context = $1 and (def->>'sync')::boolean",
            )
            .bind(bc)
            .bind(next - 1)
            .execute(&mut **tx)
            .await?;
            let payload = json!({"bc": bc, "from": written[0].sequence, "to": next - 1}).to_string();
            sqlx::query("select pg_notify('skilj_events', $1)")
                .bind(payload)
                .execute(&mut **tx)
                .await?;
        }
        Ok(written)
    }

    /// After commit: hand events to the in-order dispatcher.
    pub(crate) async fn after_commit(&self, events: &[StoredEvent]) {
        for e in events {
            if let Err(err) = self.dispatch(Arc::new(e.clone())).await {
                tracing::warn!("dispatch failed: {err}");
            }
        }
    }

    /// Emit committed events to the cache and the bus in sequence order,
    /// once each. Sequence order is commit order (the counter lock is held
    /// to commit), so seeing sequence n means everything before it is
    /// committed and can be read back.
    pub(crate) async fn dispatch(&self, e: Arc<StoredEvent>) -> Result<()> {
        let bc = e.bounded_context.clone();
        let lock = self.inner.dispatch_lock(&bc).await;
        let mut expected = lock.lock().await;
        let exp = match *expected {
            Some(x) => x,
            None => {
                // First time this context is dispatched on this instance:
                // start here.
                e.sequence
            }
        };
        if e.sequence < exp {
            return Ok(());
        }
        let mut batch = vec![];
        if e.sequence > exp {
            let rows = sqlx::query(&format!(
                "select {EVENT_COLUMNS} from events where bounded_context = $1 and sequence >= $2 and sequence < $3 order by sequence"
            ))
            .bind(&bc)
            .bind(exp)
            .bind(e.sequence)
            .fetch_all(&self.inner.pool)
            .await?;
            for r in rows {
                batch.push(Arc::new(event_from_row(&r)?));
            }
        }
        batch.push(e);
        for ev in batch {
            *expected = Some(ev.sequence + 1);
            self.inner.cache.append(ev.clone(), self.inner.config.event_cache_size);
            let _ = self.inner.bus.send(BusMessage::Event(ev));
        }
        Ok(())
    }

    /// Fetch and dispatch a committed range (from the listener).
    pub(crate) async fn dispatch_range(&self, bc: &str, from: i64, to: i64) -> Result<()> {
        let lock = self.inner.dispatch_lock(bc).await;
        let start = {
            let expected = lock.lock().await;
            match *expected {
                Some(x) if x > to => return Ok(()),
                Some(x) => x.max(from),
                None => from,
            }
        };
        let rows = sqlx::query(&format!(
            "select {EVENT_COLUMNS} from events where bounded_context = $1 and sequence >= $2 and sequence <= $3 order by sequence"
        ))
        .bind(bc)
        .bind(start)
        .bind(to)
        .fetch_all(&self.inner.pool)
        .await?;
        for r in rows {
            self.dispatch(Arc::new(event_from_row(&r)?)).await?;
        }
        Ok(())
    }

    pub(crate) async fn warm_cache(&self) -> Result<()> {
        let contexts: Vec<String> = self.inner.registry.read().await.contexts.keys().cloned().collect();
        for bc in contexts {
            self.warm_context(&bc).await?;
        }
        Ok(())
    }

    pub(crate) async fn warm_context(&self, bc: &str) -> Result<()> {
        let rows = sqlx::query(&format!(
            "select {EVENT_COLUMNS} from events where bounded_context = $1 order by sequence desc limit $2"
        ))
        .bind(bc)
        .bind(self.inner.config.event_cache_size as i64)
        .fetch_all(&self.inner.pool)
        .await?;
        let mut events: Vec<StoredEvent> = rows.iter().map(event_from_row).collect::<Result<_>>()?;
        events.reverse();
        let next: i64 = sqlx::query_scalar("select coalesce(max(next_sequence), 0) from sequence_counters where bounded_context = $1")
            .bind(bc)
            .fetch_one(&self.inner.pool)
            .await?;
        let lock = self.inner.dispatch_lock(bc).await;
        let mut expected = lock.lock().await;
        self.inner.cache.warm(bc, events, self.inner.config.event_cache_size);
        *expected = Some(next);
        Ok(())
    }

    // ------------------------------------------------------- origins

    /// CreateExternalEvent.
    pub async fn submit_external_event(
        &self,
        token: &AccessToken,
        event_type: &str,
        item: ExternalEventItem,
    ) -> Result<ExternalOutcome> {
        let mut outs = self.submit_external_events(token, event_type, vec![item], false).await?;
        match outs.pop() {
            Some(ExternalOutcome::Failed(msg, code)) => Err(error_from_code(code, msg)),
            Some(o) => Ok(o),
            None => Err(Error::invalid("no outcome")),
        }
    }

    /// CreateExternalEventBatch.
    pub async fn submit_external_events(
        &self,
        token: &AccessToken,
        event_type: &str,
        items: Vec<ExternalEventItem>,
        stop_at_first_failure: bool,
    ) -> Result<Vec<ExternalOutcome>> {
        if items.len() > self.inner.config.max_items_per_batch {
            return Err(Error::invalid(format!(
                "a batch carries at most {} items",
                self.inner.config.max_items_per_batch
            )));
        }
        let mut out = Vec::with_capacity(items.len());
        let mut failed = false;
        for item in items {
            if failed {
                out.push(ExternalOutcome::NotProcessed);
                continue;
            }
            match self.external_one(token, event_type, item).await {
                Ok(o) => out.push(o),
                Err(e) => {
                    failed = stop_at_first_failure;
                    out.push(ExternalOutcome::Failed(e.to_string(), e.code()));
                }
            }
        }
        Ok(out)
    }

    async fn external_one(&self, token: &AccessToken, event_type: &str, item: ExternalEventItem) -> Result<ExternalOutcome> {
        let (bc, ty) = self.token_event_type(token, TokenPurpose::ExternalEvent, event_type).await?;
        if !ty.0.external_creation_allowed {
            return Err(Error::forbidden("external creation is not allowed for this event type"));
        }
        self.inner.registry.read().await.is_active(&bc)?;
        ty.1.validate(&item.payload)?;
        validate::valid_correlation_id(item.correlation_id.as_deref())?;
        validate::valid_correlation_id(item.causation_id.as_deref())?;
        if item.dedupe_partition_key.is_some() != item.dedupe_sequence.is_some() {
            return Err(Error::invalid("dedupe_partition_key and dedupe_sequence travel as a pair"));
        }
        if item.idempotency_key.is_some() && item.dedupe_partition_key.is_some() {
            return Err(Error::invalid("name either a dedupe cursor or an idempotency key, not both"));
        }
        validate::valid_idempotency_key(item.idempotency_key.as_deref())?;
        let tags = derive_tags(&bc, &ty.0.tag_mappings, &item.payload, self.inner.blind.as_ref())?;
        if !owner_scope_satisfied(&tags, ty.0.owner_tag_key.as_deref(), token.scope.as_deref()) {
            return Err(Error::forbidden("token scope does not cover this event's owner"));
        }
        let protected = self.protect(&ty.0, &item.payload).await?;
        let mut tx = self.inner.pool.begin().await?;
        let next = self.lock_counter(&mut tx, &bc).await?;
        // Redelivery checks under the same lock that orders the writes.
        if let (Some(pk), Some(seq)) = (&item.dedupe_partition_key, item.dedupe_sequence) {
            let wm: Option<i64> = sqlx::query_scalar(
                "select watermark from external_watermarks where token_id = $1 and partition_key = $2 for update",
            )
            .bind(&token.id)
            .bind(pk)
            .fetch_optional(&mut *tx)
            .await?;
            if wm.is_some_and(|w| seq <= w) {
                return Ok(ExternalOutcome::Redelivered { sequence: None });
            }
        }
        if let Some(key) = &item.idempotency_key {
            let cutoff = Self::now() - self.inner.config.idempotency_key_retention;
            let prior: Option<i64> =
                sqlx::query_scalar("select sequence from external_keys where token_id = $1 and key = $2 and recorded_at >= $3")
                    .bind(&token.id)
                    .bind(key)
                    .bind(cutoff)
                    .fetch_optional(&mut *tx)
                    .await?;
            if let Some(s) = prior {
                return Ok(ExternalOutcome::Redelivered { sequence: Some(s) });
            }
        }
        let metadata = Metadata {
            type_name: ty.0.name.clone(),
            version: ty.0.schema_version,
            client_id: token.id.clone(),
            created_at: Self::now(),
            correlation_id: item.correlation_id.clone().unwrap_or_else(crypto::generate_id),
            causation_id: item.causation_id.clone(),
        };
        let written = self
            .append_locked(
                &mut tx,
                &bc,
                next,
                vec![NewEvent {
                    event_type: ty.0.name.clone(),
                    payload: protected.payload,
                    tags,
                    key_ids: protected.key_ids,
                    metadata,
                    origin: Origin::ExternalTriggered {
                        source_content: item.source_content,
                        source_context: item.source_context,
                    },
                    command_id: None,
                }],
            )
            .await?;
        if let (Some(pk), Some(seq)) = (&item.dedupe_partition_key, item.dedupe_sequence) {
            sqlx::query(
                "insert into external_watermarks (token_id, partition_key, watermark) values ($1, $2, $3)
                 on conflict (token_id, partition_key) do update set watermark = excluded.watermark",
            )
            .bind(&token.id)
            .bind(pk)
            .bind(seq)
            .execute(&mut *tx)
            .await?;
        }
        if let Some(key) = &item.idempotency_key {
            sqlx::query(
                "insert into external_keys (token_id, key, sequence, recorded_at) values ($1, $2, $3, $4)
                 on conflict (token_id, key) do update set sequence = excluded.sequence, recorded_at = excluded.recorded_at",
            )
            .bind(&token.id)
            .bind(key)
            .bind(written[0].sequence)
            .bind(Self::now())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        self.after_commit(&written).await;
        Ok(ExternalOutcome::Created(written.into_iter().next().expect("one event")))
    }

    /// CreateDirectEvent.
    pub async fn submit_direct_event(
        &self,
        token: &AccessToken,
        event_type: &str,
        payload: Value,
        correlation_id: Option<String>,
        causation_id: Option<String>,
    ) -> Result<StoredEvent> {
        let (bc, ty) = self.token_event_type(token, TokenPurpose::DirectCreation, event_type).await?;
        if !ty.0.direct_creation_allowed {
            return Err(Error::forbidden("direct creation is not allowed for this event type"));
        }
        self.inner.registry.read().await.is_active(&bc)?;
        ty.1.validate(&payload)?;
        validate::valid_correlation_id(correlation_id.as_deref())?;
        validate::valid_correlation_id(causation_id.as_deref())?;
        let tags = derive_tags(&bc, &ty.0.tag_mappings, &payload, self.inner.blind.as_ref())?;
        if !owner_scope_satisfied(&tags, ty.0.owner_tag_key.as_deref(), token.scope.as_deref()) {
            return Err(Error::forbidden("token scope does not cover this event's owner"));
        }
        let protected = self.protect(&ty.0, &payload).await?;
        let mut tx = self.inner.pool.begin().await?;
        let next = self.lock_counter(&mut tx, &bc).await?;
        let written = self
            .append_locked(
                &mut tx,
                &bc,
                next,
                vec![NewEvent {
                    event_type: ty.0.name.clone(),
                    payload: protected.payload,
                    tags,
                    key_ids: protected.key_ids,
                    metadata: Metadata {
                        type_name: ty.0.name.clone(),
                        version: ty.0.schema_version,
                        client_id: token.id.clone(),
                        created_at: Self::now(),
                        correlation_id: correlation_id.unwrap_or_else(crypto::generate_id),
                        causation_id,
                    },
                    origin: Origin::DirectlyCreated,
                    command_id: None,
                }],
            )
            .await?;
        tx.commit().await?;
        self.after_commit(&written).await;
        Ok(written.into_iter().next().expect("one event"))
    }

    /// CreateSystemEvent for one occurrence, inside the scheduler's
    /// transaction (which holds the event type row lock).
    pub(crate) async fn create_system_event(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        def: &EventTypeDef,
        payload: Value,
        occurrence_at: DateTime<Utc>,
    ) -> Result<Vec<StoredEvent>> {
        let schema = crate::schema::Schema::parse(&def.schema)?;
        // Not re-checked by the spec (the obligation is on compiled code);
        // checked here because a bad payload would be stored forever.
        schema.validate(&payload)?;
        let tags = derive_tags(&def.bounded_context, &def.tag_mappings, &payload, self.inner.blind.as_ref())?;
        let protected = self.protect(def, &payload).await?;
        let next = self.lock_counter(tx, &def.bounded_context).await?;
        self.append_locked(
            tx,
            &def.bounded_context,
            next,
            vec![NewEvent {
                event_type: def.name.clone(),
                payload: protected.payload,
                tags,
                key_ids: protected.key_ids,
                metadata: Metadata {
                    type_name: def.name.clone(),
                    version: def.schema_version,
                    client_id: SYSTEM_CLIENT_ID.into(),
                    created_at: Self::now(),
                    correlation_id: crypto::generate_id(),
                    causation_id: None,
                },
                origin: Origin::SystemTriggered { occurrence_at },
                command_id: None,
            }],
        )
        .await
    }

    /// RecordAccessEvent, inside the transaction making the change.
    pub(crate) async fn record_access_event(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        event_type_name: &str,
        role_id: &str,
        bounded_context: &str,
        payload: Value,
    ) -> Result<Vec<StoredEvent>> {
        let mut full = payload;
        if let Value::Object(m) = &mut full {
            m.insert("role_id".into(), Value::String(role_id.into()));
            m.insert("bounded_context".into(), Value::String(bounded_context.into()));
        }
        let tags: BTreeSet<Tag> = [Tag::new("role", role_id), Tag::new("bounded_context", bounded_context)]
            .into_iter()
            .collect();
        let next = self.lock_counter(tx, ADMIN_CONTEXT).await?;
        self.append_locked(
            tx,
            ADMIN_CONTEXT,
            next,
            vec![NewEvent {
                event_type: event_type_name.into(),
                payload: full,
                tags,
                key_ids: vec![],
                metadata: Metadata {
                    type_name: event_type_name.into(),
                    version: 1,
                    client_id: SYSTEM_CLIENT_ID.into(),
                    created_at: Self::now(),
                    correlation_id: crypto::generate_id(),
                    causation_id: None,
                },
                origin: Origin::SkiljRecorded,
                command_id: None,
            }],
        )
        .await
    }

    /// Resolve a token's event type, checking purpose and type.
    pub(crate) async fn token_event_type(
        &self,
        token: &AccessToken,
        purpose: TokenPurpose,
        event_type: &str,
    ) -> Result<(String, Arc<(EventTypeDef, crate::schema::Schema)>)> {
        if !token.active {
            return Err(Error::forbidden("token is revoked"));
        }
        if token.purpose != purpose {
            return Err(Error::forbidden("token is not for this capability"));
        }
        if token.type_name != event_type {
            return Err(Error::forbidden("token is scoped to another event type"));
        }
        let ty = self
            .inner
            .registry
            .read()
            .await
            .event_type(&token.bounded_context, event_type)?;
        Ok((token.bounded_context.clone(), ty))
    }
}

pub(crate) fn error_from_code(code: &str, msg: String) -> Error {
    match code {
        "NOT_FOUND" => Error::NotFound(msg),
        "FORBIDDEN" => Error::Forbidden(msg),
        "CONFLICT" => Error::Conflict(msg),
        "ARCHIVED" => Error::Archived(msg),
        _ => Error::Invalid(msg),
    }
}

impl super::Inner {
    pub(crate) async fn dispatch_lock(&self, bc: &str) -> Arc<tokio::sync::Mutex<Option<i64>>> {
        let mut m = self.dispatchers.lock().expect("dispatch map");
        m.entry(bc.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
            .clone()
    }
}
