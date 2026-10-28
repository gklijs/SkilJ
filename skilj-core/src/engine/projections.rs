//! project(): the sync path (inside the writing transaction), the async
//! consumer, rebuild replay and promotion, and QueryProjection.

use std::time::Instant;

use serde_json::Value;
use sqlx::{PgConnection, Postgres, Row, Transaction};

use super::rows::{event_from_row, EVENT_COLUMNS};
use super::Skilj;
use crate::decl::{tag_value, ProjectionDecl};
use crate::error::{Error, Result};
use crate::model::*;

impl Skilj {
    pub(crate) fn projection_decl(&self, bc: &str, name: &str) -> Option<&ProjectionDecl> {
        self.decl_for(bc)?.projections.iter().find(|p| p.name == name)
    }

    /// Fold one event into one generation of one projection.
    pub(crate) async fn fold_event(
        &self,
        conn: &mut PgConnection,
        decl: &ProjectionDecl,
        generation: &str,
        e: &StoredEvent,
    ) -> Result<()> {
        let keys = (decl.keys)(e);
        for key in keys {
            let row = sqlx::query(
                "select state, owner from projection_state
                 where bounded_context = $1 and projection = $2 and generation = $3 and key = $4 for update",
            )
            .bind(&e.bounded_context)
            .bind(&decl.name)
            .bind(generation)
            .bind(&key)
            .fetch_optional(&mut *conn)
            .await?;
            let (state, owner): (Option<Value>, Option<String>) = match row {
                Some(r) => (Some(r.try_get("state")?), r.try_get("owner")?),
                None => (None, None),
            };
            let new_owner = decl
                .owner_tag_key
                .as_deref()
                .and_then(|k| tag_value(&e.tags, k).map(str::to_string))
                .or(owner);
            match (decl.fold)(&key, state.as_ref(), e) {
                Some(next) => {
                    sqlx::query(
                        "insert into projection_state (bounded_context, projection, generation, key, state, owner)
                         values ($1, $2, $3, $4, $5, $6)
                         on conflict (bounded_context, projection, generation, key)
                         do update set state = excluded.state, owner = excluded.owner",
                    )
                    .bind(&e.bounded_context)
                    .bind(&decl.name)
                    .bind(generation)
                    .bind(&key)
                    .bind(next)
                    .bind(new_owner)
                    .execute(&mut *conn)
                    .await?;
                }
                None => {
                    sqlx::query(
                        "delete from projection_state where bounded_context = $1 and projection = $2 and generation = $3 and key = $4",
                    )
                    .bind(&e.bounded_context)
                    .bind(&decl.name)
                    .bind(generation)
                    .bind(&key)
                    .execute(&mut *conn)
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Inline update of every sync projection consuming this event's type.
    pub(crate) async fn fold_sync_projections(&self, tx: &mut Transaction<'_, Postgres>, e: &StoredEvent) -> Result<()> {
        let sync: Vec<ProjectionDef> = self
            .inner
            .registry
            .read()
            .await
            .projections_of(&e.bounded_context)
            .into_iter()
            .filter(|p| p.sync && p.consumed_event_types.contains(&e.event_type))
            .cloned()
            .collect();
        for p in sync {
            let decl = self.projection_decl(&e.bounded_context, &p.name).ok_or_else(|| {
                Error::NotDeclared(format!(
                    "sync projection {}.{} consumes {} but this process doesn't carry it",
                    e.bounded_context, p.name, e.event_type
                ))
            })?;
            self.fold_event(tx, decl, "live", e).await?;
        }
        Ok(())
    }

    /// Fold (after, upto] into a generation (first-time sync registration).
    pub(crate) async fn fold_range(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        bc: &str,
        decl: &ProjectionDecl,
        generation: &str,
        after: i64,
        upto: i64,
    ) -> Result<()> {
        let types: Vec<String> = decl.consumed_event_types.iter().cloned().collect();
        let rows = sqlx::query(&format!(
            "select {EVENT_COLUMNS} from events where bounded_context = $1 and sequence > $2 and sequence <= $3
               and event_type = any($4) order by sequence"
        ))
        .bind(bc)
        .bind(after)
        .bind(upto)
        .bind(&types)
        .fetch_all(&mut **tx)
        .await?;
        for r in rows {
            let e = event_from_row(&r)?;
            self.fold_event(tx, decl, generation, &e).await?;
        }
        Ok(())
    }

    /// One step of the async consumer for every projection this process
    /// carries. Returns how many events were walked.
    pub(crate) async fn step_projections(&self) -> Result<usize> {
        let batch = 500i64;
        let mut walked = 0usize;
        let targets: Vec<(String, String)> = self
            .inner
            .registry
            .read()
            .await
            .projections
            .keys()
            .filter(|(bc, name)| self.projection_decl(bc, name).is_some())
            .cloned()
            .collect();
        for (bc, name) in targets {
            let decl = self.projection_decl(&bc, &name).expect("declared").clone();
            // Live, async.
            let mut tx = self.inner.pool.begin().await?;
            let row = sqlx::query(
                "select def, caught_up_to from projections where bounded_context = $1 and name = $2 for update skip locked",
            )
            .bind(&bc)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(row) = row {
                let def: ProjectionDef = serde_json::from_value(row.try_get("def")?)?;
                let pos: Option<i64> = row.try_get("caught_up_to")?;
                if !def.sync {
                    walked += self.walk(&mut tx, &bc, &decl, &def.consumed_event_types, "live", pos, batch).await?;
                }
            }
            tx.commit().await?;
            // A building rebuild.
            let mut tx = self.inner.pool.begin().await?;
            let row = sqlx::query(
                "select def, caught_up_to from projection_rebuilds
                 where bounded_context = $1 and projection = $2 and status = 'building' for update skip locked",
            )
            .bind(&bc)
            .bind(&name)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(row) = row {
                let staged: ProjectionRebuild = serde_json::from_value(row.try_get("def")?)?;
                let pos: Option<i64> = row.try_get("caught_up_to")?;
                let n = self
                    .walk(&mut tx, &bc, &decl, &staged.consumed_event_types, "rebuild", pos, batch)
                    .await?;
                walked += n;
                let new_pos: Option<i64> = sqlx::query_scalar(
                    "select caught_up_to from projection_rebuilds where bounded_context = $1 and projection = $2 and status = 'building'",
                )
                .bind(&bc)
                .bind(&name)
                .fetch_one(&mut *tx)
                .await?;
                // Promote once caught up, under the sequence lock so nothing
                // is committed in between.
                let latest = self.lock_counter(&mut tx, &bc).await? - 1;
                if new_pos.unwrap_or(-1) >= latest {
                    self.promote(&mut tx, &bc, &name, &staged, latest).await?;
                }
            }
            tx.commit().await?;
        }
        if walked > 0 {
            self.reload_registry().await?;
        }
        Ok(walked)
    }

    #[allow(clippy::too_many_arguments)]
    async fn walk(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        bc: &str,
        decl: &ProjectionDecl,
        consumed: &std::collections::BTreeSet<String>,
        generation: &str,
        pos: Option<i64>,
        batch: i64,
    ) -> Result<usize> {
        let after = pos.unwrap_or(-1);
        let rows = sqlx::query(&format!(
            "select {EVENT_COLUMNS} from events where bounded_context = $1 and sequence > $2 order by sequence limit $3"
        ))
        .bind(bc)
        .bind(after)
        .bind(batch)
        .fetch_all(&mut **tx)
        .await?;
        let n = rows.len();
        let mut last = after;
        for r in rows {
            let e = event_from_row(&r)?;
            if consumed.contains(&e.event_type) {
                self.fold_event(tx, decl, generation, &e).await?;
            }
            last = e.sequence;
        }
        if n > 0 {
            let sql = if generation == "live" {
                "update projections set caught_up_to = $3 where bounded_context = $1 and name = $2"
            } else {
                "update projection_rebuilds set caught_up_to = $3 where bounded_context = $1 and projection = $2 and status = 'building'"
            };
            sqlx::query(sql).bind(bc).bind(&decl.name).bind(last).execute(&mut **tx).await?;
        }
        Ok(n)
    }

    async fn promote(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        bc: &str,
        name: &str,
        staged: &ProjectionRebuild,
        latest: i64,
    ) -> Result<()> {
        let old: Value = sqlx::query_scalar("select def from projections where bounded_context = $1 and name = $2 for update")
            .bind(bc)
            .bind(name)
            .fetch_one(&mut **tx)
            .await?;
        let mut def: ProjectionDef = serde_json::from_value(old)?;
        def.schema = staged.schema.clone();
        def.schema_version = staged.schema_version;
        def.consumed_event_types = staged.consumed_event_types.clone();
        def.sync = staged.sync;
        sqlx::query("update projections set def = $3, caught_up_to = $4 where bounded_context = $1 and name = $2")
            .bind(bc)
            .bind(name)
            .bind(serde_json::to_value(&def)?)
            .bind(if latest >= 0 { Some(latest) } else { None })
            .execute(&mut **tx)
            .await?;
        sqlx::query("delete from projection_state where bounded_context = $1 and projection = $2 and generation = 'live'")
            .bind(bc)
            .bind(name)
            .execute(&mut **tx)
            .await?;
        sqlx::query(
            "update projection_state set generation = 'live' where bounded_context = $1 and projection = $2 and generation = 'rebuild'",
        )
        .bind(bc)
        .bind(name)
        .execute(&mut **tx)
        .await?;
        sqlx::query("delete from projection_rebuilds where bounded_context = $1 and projection = $2 and status = 'building'")
            .bind(bc)
            .bind(name)
            .execute(&mut **tx)
            .await?;
        self.notify_registry_changed(tx).await?;
        Ok(())
    }

    /// QueryProjection.
    pub async fn query_projection(
        &self,
        grant: &Grant,
        projection: &str,
        key: Option<&str>,
        wait_for_sequence: Option<i64>,
    ) -> Result<Option<Value>> {
        if !grant.has_level(AccessLevel::Read) {
            return Err(Error::forbidden("grant is revoked"));
        }
        let bc = grant.mapping.bounded_context.clone();
        let def = self.inner.registry.read().await.projection(&bc, projection)?.clone();
        let decl = self.projection_decl(&bc, projection);
        // The capability gate covers the projection whole; failing it is
        // answered as "does not exist".
        if let Some(cap) = &def.requires_capability {
            if !grant.mapping.capabilities.contains(cap) {
                return Err(Error::not_found(format!("projection {bc}.{projection}")));
            }
        }
        let key = key.unwrap_or("");
        if let Some(seq) = wait_for_sequence {
            let next: i64 = sqlx::query_scalar("select next_sequence from sequence_counters where bounded_context = $1")
                .bind(&bc)
                .fetch_one(&self.inner.pool)
                .await?;
            if seq >= next {
                return Err(Error::invalid(format!("sequence {seq} has not been committed")));
            }
            self.await_caught_up(&bc, projection, seq).await?;
        }
        let row = sqlx::query(
            "select state, owner from projection_state where bounded_context = $1 and projection = $2 and generation = 'live' and key = $3",
        )
        .bind(&bc)
        .bind(projection)
        .bind(key)
        .fetch_optional(&self.inner.pool)
        .await?;
        let (state, owner): (Option<Value>, Option<String>) = match row {
            Some(r) => (Some(r.try_get("state")?), r.try_get("owner")?),
            None => (None, None),
        };
        // Owner scope: affirmatively provable, or invisible.
        let owner_key = decl.and_then(|d| d.owner_tag_key.clone());
        if let (Some(scope), Some(_)) = (&grant.mapping.scope, &owner_key) {
            if owner.as_deref() != Some(scope.as_str()) {
                return Err(Error::not_found(format!("projection instance {bc}.{projection}[{key}]")));
            }
        }
        let Some(state) = state else { return Ok(None) };
        // read_projection: the instance key is the subject; entitled
        // callers get every leaf encrypted for that subject decrypted.
        let entitled = !key.is_empty() && (grant.mapping.can_read_sensitive || grant.role.external_subject == key);
        if entitled {
            let k = key.to_string();
            Ok(Some(self.decrypt_where(&state, &move |s: &str| s == k).await?))
        } else {
            Ok(Some(state))
        }
    }

    async fn await_caught_up(&self, bc: &str, projection: &str, seq: i64) -> Result<()> {
        let deadline = Instant::now() + self.inner.config.projection_wait;
        loop {
            let pos: Option<i64> =
                sqlx::query_scalar("select caught_up_to from projections where bounded_context = $1 and name = $2")
                    .bind(bc)
                    .bind(projection)
                    .fetch_one(&self.inner.pool)
                    .await?;
            if pos.is_some_and(|p| p >= seq) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::Timeout(seq));
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    /// All instance keys of a projection the grant may see (not in the spec:
    /// QueryProjection exposes "no listing of the rest"; used by tests and
    /// the inspector-free admin view only).
    pub async fn projection_keys(&self, bc: &str, projection: &str) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "select key from projection_state where bounded_context = $1 and projection = $2 and generation = 'live' order by key",
        )
        .bind(bc)
        .bind(projection)
        .fetch_all(&self.inner.pool)
        .await?)
    }
}
