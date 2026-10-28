//! Registered state (bounded contexts, types, projections), its in-memory
//! mirror, and the registration rules.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};

use super::rows::context_from_row;
use super::Skilj;
use crate::decl::{CommandTypeDecl, EventTypeDecl, ProjectionDecl};
use crate::error::{Error, Result};
use crate::model::*;
use crate::schema::Schema;
use crate::validate;

/// The in-memory mirror of registered state. Reloaded from Postgres after
/// any registration on any instance.
#[derive(Default, Clone)]
pub struct Registry {
    pub contexts: BTreeMap<String, BoundedContext>,
    pub event_types: BTreeMap<(String, String), Arc<(EventTypeDef, Schema)>>,
    pub command_types: BTreeMap<(String, String), Arc<(CommandTypeDef, Schema)>>,
    pub projections: BTreeMap<(String, String), ProjectionDef>,
    pub rebuilds: Vec<ProjectionRebuild>,
}

impl Registry {
    pub fn context(&self, name: &str) -> Result<&BoundedContext> {
        self.contexts
            .get(name)
            .ok_or_else(|| Error::not_found(format!("bounded context {name}")))
    }
    pub fn event_type(&self, bc: &str, name: &str) -> Result<Arc<(EventTypeDef, Schema)>> {
        self.event_types
            .get(&(bc.to_string(), name.to_string()))
            .cloned()
            .ok_or_else(|| Error::not_found(format!("event type {bc}.{name}")))
    }
    pub fn command_type(&self, bc: &str, name: &str) -> Result<Arc<(CommandTypeDef, Schema)>> {
        self.command_types
            .get(&(bc.to_string(), name.to_string()))
            .cloned()
            .ok_or_else(|| Error::not_found(format!("command type {bc}.{name}")))
    }
    pub fn projection(&self, bc: &str, name: &str) -> Result<&ProjectionDef> {
        self.projections
            .get(&(bc.to_string(), name.to_string()))
            .ok_or_else(|| Error::not_found(format!("projection {bc}.{name}")))
    }
    pub fn event_types_of(&self, bc: &str) -> Vec<Arc<(EventTypeDef, Schema)>> {
        self.event_types
            .iter()
            .filter(|((b, _), _)| b == bc)
            .map(|(_, v)| v.clone())
            .collect()
    }
    pub fn command_types_of(&self, bc: &str) -> Vec<Arc<(CommandTypeDef, Schema)>> {
        self.command_types
            .iter()
            .filter(|((b, _), _)| b == bc)
            .map(|(_, v)| v.clone())
            .collect()
    }
    pub fn projections_of(&self, bc: &str) -> Vec<&ProjectionDef> {
        self.projections
            .iter()
            .filter(|((b, _), _)| b == bc)
            .map(|(_, v)| v)
            .collect()
    }
    pub fn is_active(&self, bc: &str) -> Result<()> {
        match self.context(bc)?.status {
            ContextStatus::Active => Ok(()),
            ContextStatus::Archived => Err(Error::Archived(bc.to_string())),
        }
    }
}

/// What the startup reconciliation loop did.
#[derive(Debug, Default, Clone)]
pub struct ReconciliationReport {
    pub registered: Vec<String>,
    pub staged_rebuilds: Vec<String>,
    pub skipped_no_access: Vec<String>,
    pub skipped_missing_context: Vec<String>,
    pub errors: Vec<String>,
}

/// On whose authority a registration runs.
pub(crate) enum Authority<'a> {
    Grant(&'a Grant),
    /// A superadmin applying a template (no grant gate, see spec).
    Superadmin,
}

impl Skilj {
    /// A clone of the current registry mirror.
    pub async fn registry(&self) -> Registry {
        self.inner.registry.read().await.clone()
    }

    pub(crate) async fn reload_registry(&self) -> Result<()> {
        let pool = &self.inner.pool;
        let mut reg = Registry::default();
        for row in sqlx::query("select * from bounded_contexts")
            .fetch_all(pool)
            .await?
        {
            let c = context_from_row(&row)?;
            reg.contexts.insert(c.name.clone(), c);
        }
        for row in sqlx::query("select def from event_types")
            .fetch_all(pool)
            .await?
        {
            let def: EventTypeDef = serde_json::from_value(row.try_get("def")?)?;
            let schema = Schema::parse(&def.schema)?;
            reg.event_types.insert(
                (def.bounded_context.clone(), def.name.clone()),
                Arc::new((def, schema)),
            );
        }
        for row in sqlx::query("select def from command_types")
            .fetch_all(pool)
            .await?
        {
            let def: CommandTypeDef = serde_json::from_value(row.try_get("def")?)?;
            let schema = Schema::parse(&def.schema)?;
            reg.command_types.insert(
                (def.bounded_context.clone(), def.name.clone()),
                Arc::new((def, schema)),
            );
        }
        for row in sqlx::query("select def, caught_up_to from projections")
            .fetch_all(pool)
            .await?
        {
            let mut def: ProjectionDef = serde_json::from_value(row.try_get("def")?)?;
            def.caught_up_to = row.try_get("caught_up_to")?;
            reg.projections
                .insert((def.bounded_context.clone(), def.name.clone()), def);
        }
        for row in sqlx::query("select def, caught_up_to, status from projection_rebuilds")
            .fetch_all(pool)
            .await?
        {
            let mut r: ProjectionRebuild = serde_json::from_value(row.try_get("def")?)?;
            r.caught_up_to = row.try_get("caught_up_to")?;
            let status: String = row.try_get("status")?;
            r.status = if status == "pending" {
                RebuildStatus::Pending
            } else {
                RebuildStatus::Building
            };
            reg.rebuilds.push(r);
        }
        *self.inner.templates.write().expect("templates") = reg
            .contexts
            .values()
            .filter_map(|c| c.template.clone().map(|t| (c.name.clone(), t)))
            .collect();
        *self.inner.registry.write().await = reg;
        Ok(())
    }

    /// Tell every instance (this one included, via its listener) to reload.
    pub(crate) async fn notify_registry_changed(
        &self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<()> {
        sqlx::query("select pg_notify('skilj_changes', '{\"kind\":\"registry\"}')")
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    fn check_authority(&self, auth: &Authority<'_>, bc: &str) -> Result<()> {
        if let Authority::Grant(g) = auth {
            if !g.has_level(AccessLevel::Admin) {
                return Err(Error::forbidden("registration needs an admin grant"));
            }
            if g.mapping.bounded_context != bc {
                return Err(Error::forbidden("grant is for another bounded context"));
            }
        }
        Ok(())
    }

    /// Blind keys: declared by a BlindTag, never mixed with plain mappings
    /// of the same key in the context.
    async fn check_blind_consistency(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        bc: &str,
        mappings: &[TagMapping],
        this_kind: &str,
        this_name: &str,
    ) -> Result<()> {
        let blind_keys: BTreeSet<String> = sqlx::query_scalar::<_, String>(
            "select tag_key from blind_tags where bounded_context = $1",
        )
        .bind(bc)
        .fetch_all(&mut **tx)
        .await?
        .into_iter()
        .collect();
        for m in mappings {
            if m.blind && !blind_keys.contains(&m.key) {
                return Err(Error::invalid(format!(
                    "blind tag key '{}' is not declared by a BlindTag of {bc}",
                    m.key
                )));
            }
            if !m.blind && blind_keys.contains(&m.key) {
                return Err(Error::invalid(format!(
                    "tag key '{}' is blind in {bc}; every mapping of it must be blind",
                    m.key
                )));
            }
        }
        let reg = self.inner.registry.read().await;
        let others = reg
            .event_types_of(bc)
            .into_iter()
            .filter(|t| !(this_kind == "event" && t.0.name == this_name))
            .flat_map(|t| t.0.tag_mappings.clone())
            .chain(
                reg.command_types_of(bc)
                    .into_iter()
                    .filter(|t| !(this_kind == "command" && t.0.name == this_name))
                    .flat_map(|t| t.0.tag_mappings.clone()),
            )
            .collect::<Vec<_>>();
        for m in mappings {
            if others.iter().any(|o| o.key == m.key && o.blind != m.blind) {
                return Err(Error::invalid(format!(
                    "tag key '{}' is mapped {} elsewhere in {bc}",
                    m.key,
                    if m.blind { "plainly" } else { "blind" }
                )));
            }
        }
        Ok(())
    }

    /// RegisterEventType.
    pub async fn register_event_type(
        &self,
        grant: &Grant,
        bc: &str,
        decl: &EventTypeDecl,
    ) -> Result<bool> {
        self.register_event_type_as(&Authority::Grant(grant), bc, decl)
            .await
    }

    pub(crate) async fn register_event_type_as(
        &self,
        auth: &Authority<'_>,
        bc: &str,
        decl: &EventTypeDecl,
    ) -> Result<bool> {
        self.check_authority(auth, bc)?;
        self.inner.registry.read().await.is_active(bc)?;
        if bc == ADMIN_CONTEXT {
            return Err(Error::forbidden(
                "the admin context's types are SkilJ's own",
            ));
        }
        validate::valid_type_name(&decl.name)?;
        let schema = Schema::parse(&decl.schema)?;
        validate::validate_field_declarations(
            &schema,
            &decl.tag_mappings,
            &decl.sensitive_fields,
            &decl.private_fields,
            decl.owner_tag_key.as_deref(),
        )?;
        let (sched, policy) = match &decl.schedule {
            Some((cron, policy, _)) => {
                crate::engine::background::parse_cron(cron)?;
                (Some(cron.clone()), Some(*policy))
            }
            None => (None, None),
        };
        let system = decl.schedule.is_some();
        let mut tx = self.inner.pool.begin().await?;
        self.check_blind_consistency(&mut tx, bc, &decl.tag_mappings, "event", &decl.name)
            .await?;
        let existing: Option<EventTypeDef> = sqlx::query_scalar::<_, Value>(
            "select def from event_types where bounded_context = $1 and name = $2 for update",
        )
        .bind(bc)
        .bind(&decl.name)
        .fetch_optional(&mut *tx)
        .await?
        .map(serde_json::from_value)
        .transpose()?;
        let now = Self::now();
        let def = match &existing {
            Some(old) => {
                let old_schema = Schema::parse(&old.schema)?;
                schema.is_backwards_compatible_with(&old_schema)?;
                for m in &old.tag_mappings {
                    if !decl.tag_mappings.iter().any(|n| n.key == m.key) {
                        return Err(Error::invalid(format!(
                            "tag key '{}' may not be dropped: stored events carry it",
                            m.key
                        )));
                    }
                }
                let newly_enabled = system && !old.system_triggered_allowed;
                EventTypeDef {
                    bounded_context: bc.into(),
                    name: decl.name.clone(),
                    schema_version: if old.schema != decl.schema {
                        old.schema_version + 1
                    } else {
                        old.schema_version
                    },
                    schema: decl.schema.clone(),
                    tag_mappings: decl.tag_mappings.clone(),
                    owner_tag_key: decl.owner_tag_key.clone(),
                    sensitive_fields: decl.sensitive_fields.clone(),
                    private_fields: decl.private_fields.clone(),
                    external_creation_allowed: decl.external_creation_allowed,
                    direct_creation_allowed: decl.direct_creation_allowed,
                    system_triggered_allowed: system,
                    system_triggered_schedule: sched,
                    missed_occurrence_policy: policy,
                    schedule_position: if newly_enabled {
                        Some(now)
                    } else {
                        old.schedule_position
                    },
                    last_fired_at: old.last_fired_at,
                    event_read_allowed: decl.event_read_allowed,
                }
            }
            None => EventTypeDef {
                bounded_context: bc.into(),
                name: decl.name.clone(),
                schema: decl.schema.clone(),
                schema_version: 1,
                tag_mappings: decl.tag_mappings.clone(),
                owner_tag_key: decl.owner_tag_key.clone(),
                sensitive_fields: decl.sensitive_fields.clone(),
                private_fields: decl.private_fields.clone(),
                external_creation_allowed: decl.external_creation_allowed,
                direct_creation_allowed: decl.direct_creation_allowed,
                system_triggered_allowed: system,
                system_triggered_schedule: sched,
                missed_occurrence_policy: policy,
                schedule_position: if system { Some(now) } else { None },
                last_fired_at: None,
                event_read_allowed: decl.event_read_allowed,
            },
        };
        if existing.as_ref() == Some(&def) {
            return Ok(false);
        }
        sqlx::query(
            "insert into event_types (bounded_context, name, def) values ($1, $2, $3)
             on conflict (bounded_context, name) do update set def = excluded.def",
        )
        .bind(bc)
        .bind(&decl.name)
        .bind(serde_json::to_value(&def)?)
        .execute(&mut *tx)
        .await?;
        self.notify_registry_changed(&mut tx).await?;
        tx.commit().await?;
        self.reload_registry().await?;
        Ok(true)
    }

    /// RegisterCommandType.
    pub async fn register_command_type(
        &self,
        grant: &Grant,
        bc: &str,
        decl: &CommandTypeDecl,
    ) -> Result<bool> {
        self.register_command_type_as(&Authority::Grant(grant), bc, decl)
            .await
    }

    pub(crate) async fn register_command_type_as(
        &self,
        auth: &Authority<'_>,
        bc: &str,
        decl: &CommandTypeDecl,
    ) -> Result<bool> {
        self.check_authority(auth, bc)?;
        self.inner.registry.read().await.is_active(bc)?;
        if bc == ADMIN_CONTEXT {
            return Err(Error::forbidden(
                "the admin context's types are SkilJ's own",
            ));
        }
        validate::valid_type_name(&decl.name)?;
        let schema = Schema::parse(&decl.schema)?;
        validate::validate_field_declarations(
            &schema,
            &decl.tag_mappings,
            &decl.sensitive_fields,
            &decl.private_fields,
            decl.owner_tag_key.as_deref(),
        )?;
        let mut tx = self.inner.pool.begin().await?;
        self.check_blind_consistency(&mut tx, bc, &decl.tag_mappings, "command", &decl.name)
            .await?;
        let existing: Option<CommandTypeDef> = sqlx::query_scalar::<_, Value>(
            "select def from command_types where bounded_context = $1 and name = $2 for update",
        )
        .bind(bc)
        .bind(&decl.name)
        .fetch_optional(&mut *tx)
        .await?
        .map(serde_json::from_value)
        .transpose()?;
        let mut version = 1;
        if let Some(old) = &existing {
            schema.is_backwards_compatible_with(&Schema::parse(&old.schema)?)?;
            for m in &old.tag_mappings {
                if !decl.tag_mappings.iter().any(|n| n.key == m.key) {
                    return Err(Error::invalid(format!(
                        "tag key '{}' may not be dropped: stored commands carry it",
                        m.key
                    )));
                }
            }
            version = if old.schema != decl.schema {
                old.schema_version + 1
            } else {
                old.schema_version
            };
        }
        let def = CommandTypeDef {
            bounded_context: bc.into(),
            name: decl.name.clone(),
            schema: decl.schema.clone(),
            schema_version: version,
            tag_mappings: decl.tag_mappings.clone(),
            owner_tag_key: decl.owner_tag_key.clone(),
            sensitive_fields: decl.sensitive_fields.clone(),
            private_fields: decl.private_fields.clone(),
            rest_trigger_allowed: decl.rest_trigger_allowed,
        };
        if existing.as_ref() == Some(&def) {
            return Ok(false);
        }
        sqlx::query(
            "insert into command_types (bounded_context, name, def) values ($1, $2, $3)
             on conflict (bounded_context, name) do update set def = excluded.def",
        )
        .bind(bc)
        .bind(&decl.name)
        .bind(serde_json::to_value(&def)?)
        .execute(&mut *tx)
        .await?;
        self.notify_registry_changed(&mut tx).await?;
        tx.commit().await?;
        self.reload_registry().await?;
        Ok(true)
    }

    /// RegisterProjection. Returns "created", "updated", "staged" or "unchanged".
    pub async fn register_projection(
        &self,
        grant: &Grant,
        bc: &str,
        decl: &ProjectionDecl,
    ) -> Result<&'static str> {
        self.register_projection_as(&Authority::Grant(grant), bc, decl)
            .await
    }

    pub(crate) async fn register_projection_as(
        &self,
        auth: &Authority<'_>,
        bc: &str,
        decl: &ProjectionDecl,
    ) -> Result<&'static str> {
        self.check_authority(auth, bc)?;
        self.inner.registry.read().await.is_active(bc)?;
        validate::valid_type_name(&decl.name)?;
        let schema = Schema::parse(&decl.schema)?;
        if let Some(c) = &decl.requires_capability {
            validate::valid_capability_name(c)?;
        }
        {
            let reg = self.inner.registry.read().await;
            for et in &decl.consumed_event_types {
                reg.event_type(bc, et).map_err(|_| {
                    Error::invalid(format!(
                        "projection consumes '{et}', which is not an event type of {bc}"
                    ))
                })?;
            }
        }
        let mut tx = self.inner.pool.begin().await?;
        // Under the context's sequence lock: whether a consumed type has
        // history, and the first-time sync fold, must not race a writer.
        let latest = self.lock_counter(&mut tx, bc).await? - 1;
        let row = sqlx::query("select def, caught_up_to from projections where bounded_context = $1 and name = $2 for update")
            .bind(bc)
            .bind(&decl.name)
            .fetch_optional(&mut *tx)
            .await?;
        let outcome;
        match row {
            None => {
                let has_history = self
                    .types_have_history(&mut tx, bc, &decl.consumed_event_types)
                    .await?;
                let def = ProjectionDef {
                    bounded_context: bc.into(),
                    name: decl.name.clone(),
                    schema: decl.schema.clone(),
                    schema_version: 1,
                    consumed_event_types: decl.consumed_event_types.clone(),
                    sync: decl.sync,
                    requires_capability: decl.requires_capability.clone(),
                    caught_up_to: None,
                };
                sqlx::query("insert into projections (bounded_context, name, def, caught_up_to) values ($1, $2, $3, null)")
                    .bind(bc)
                    .bind(&decl.name)
                    .bind(serde_json::to_value(&def)?)
                    .execute(&mut *tx)
                    .await?;
                if decl.sync && has_history {
                    // Fold history in now: a sync projection has no consumer.
                    self.fold_range(&mut tx, bc, decl, "live", -1, latest)
                        .await?;
                    sqlx::query("update projections set caught_up_to = $3 where bounded_context = $1 and name = $2")
                        .bind(bc)
                        .bind(&decl.name)
                        .bind(latest)
                        .execute(&mut *tx)
                        .await?;
                }
                outcome = "created";
            }
            Some(row) => {
                let old: ProjectionDef = serde_json::from_value(row.try_get("def")?)?;
                let old_schema = Schema::parse(&old.schema)?;
                schema.is_backwards_compatible_with(&old_schema)?;
                let schema_changed = old.schema != decl.schema;
                let changed: BTreeSet<String> = decl
                    .consumed_event_types
                    .symmetric_difference(&old.consumed_event_types)
                    .cloned()
                    .collect();
                let change_has_history = self.types_have_history(&mut tx, bc, &changed).await?;
                let becoming_sync = !old.sync && decl.sync;
                if schema_changed || change_has_history || becoming_sync {
                    let staged = ProjectionRebuild {
                        bounded_context: bc.into(),
                        projection: decl.name.clone(),
                        schema: decl.schema.clone(),
                        schema_version: if schema_changed {
                            old.schema_version + 1
                        } else {
                            old.schema_version
                        },
                        consumed_event_types: decl.consumed_event_types.clone(),
                        sync: decl.sync,
                        caught_up_to: None,
                        status: RebuildStatus::Pending,
                    };
                    let existing_pending: Option<Value> = sqlx::query_scalar(
                        "select def from projection_rebuilds where bounded_context = $1 and projection = $2 and status = 'pending'",
                    )
                    .bind(bc)
                    .bind(&decl.name)
                    .fetch_optional(&mut *tx)
                    .await?;
                    let same = existing_pending
                        .map(serde_json::from_value::<ProjectionRebuild>)
                        .transpose()?
                        .is_some_and(|p| p == staged);
                    let cap_changed = old.requires_capability != decl.requires_capability;
                    if cap_changed {
                        let mut d = old.clone();
                        d.requires_capability = decl.requires_capability.clone();
                        sqlx::query("update projections set def = $3 where bounded_context = $1 and name = $2")
                            .bind(bc)
                            .bind(&decl.name)
                            .bind(serde_json::to_value(&d)?)
                            .execute(&mut *tx)
                            .await?;
                    }
                    if same && !cap_changed {
                        return Ok("unchanged");
                    }
                    if !same {
                        sqlx::query(
                            "insert into projection_rebuilds (bounded_context, projection, status, def, caught_up_to)
                             values ($1, $2, 'pending', $3, null)
                             on conflict (bounded_context, projection, status) do update set def = excluded.def, caught_up_to = null",
                        )
                        .bind(bc)
                        .bind(&decl.name)
                        .bind(serde_json::to_value(&staged)?)
                        .execute(&mut *tx)
                        .await?;
                    }
                    outcome = if same { "updated" } else { "staged" };
                } else {
                    let mut d = old.clone();
                    d.consumed_event_types = decl.consumed_event_types.clone();
                    d.sync = decl.sync;
                    d.requires_capability = decl.requires_capability.clone();
                    if d == old {
                        return Ok("unchanged");
                    }
                    sqlx::query(
                        "update projections set def = $3 where bounded_context = $1 and name = $2",
                    )
                    .bind(bc)
                    .bind(&decl.name)
                    .bind(serde_json::to_value(&d)?)
                    .execute(&mut *tx)
                    .await?;
                    outcome = "updated";
                }
            }
        }
        self.notify_registry_changed(&mut tx).await?;
        tx.commit().await?;
        self.reload_registry().await?;
        Ok(outcome)
    }

    async fn types_have_history(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        bc: &str,
        types: &BTreeSet<String>,
    ) -> Result<bool> {
        if types.is_empty() {
            return Ok(false);
        }
        let names: Vec<String> = types.iter().cloned().collect();
        Ok(sqlx::query_scalar(
            "select exists(select 1 from events where bounded_context = $1 and event_type = any($2))",
        )
        .bind(bc)
        .bind(&names)
        .fetch_one(&mut **tx)
        .await?)
    }

    /// RebuildProjection: pending -> building.
    pub async fn rebuild_projection(&self, grant: &Grant, projection: &str) -> Result<()> {
        let bc = &grant.mapping.bounded_context;
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("needs an admin grant"));
        }
        self.inner
            .registry
            .read()
            .await
            .projection(bc, projection)?;
        let mut tx = self.inner.pool.begin().await?;
        let building: bool = sqlx::query_scalar(
            "select exists(select 1 from projection_rebuilds where bounded_context = $1 and projection = $2 and status = 'building')",
        )
        .bind(bc)
        .bind(projection)
        .fetch_one(&mut *tx)
        .await?;
        if building {
            return Err(Error::conflict(
                "a rebuild is already building; trigger the pending one when it finishes",
            ));
        }
        let n = sqlx::query(
            "update projection_rebuilds set status = 'building', caught_up_to = null
             where bounded_context = $1 and projection = $2 and status = 'pending'",
        )
        .bind(bc)
        .bind(projection)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(Error::conflict("nothing staged to rebuild"));
        }
        sqlx::query("delete from projection_state where bounded_context = $1 and projection = $2 and generation = 'rebuild'")
            .bind(bc)
            .bind(projection)
            .execute(&mut *tx)
            .await?;
        self.notify_registry_changed(&mut tx).await?;
        tx.commit().await?;
        self.reload_registry().await
    }

    /// DiscardProjectionRebuild.
    pub async fn discard_projection_rebuild(&self, grant: &Grant, projection: &str) -> Result<()> {
        let bc = &grant.mapping.bounded_context;
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("needs an admin grant"));
        }
        self.inner
            .registry
            .read()
            .await
            .projection(bc, projection)?;
        let mut tx = self.inner.pool.begin().await?;
        let n = sqlx::query("delete from projection_rebuilds where bounded_context = $1 and projection = $2 and status = 'pending'")
            .bind(bc)
            .bind(projection)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n == 0 {
            return Err(Error::conflict("nothing staged to discard"));
        }
        self.notify_registry_changed(&mut tx).await?;
        tx.commit().await?;
        self.reload_registry().await
    }

    /// Register a binary's blind tag declarations, and refuse to start when
    /// they disagree with what is stored.
    pub(crate) async fn check_blind_tags(&self) -> Result<()> {
        for decl in self.inner.decls.values() {
            for b in &decl.blind_tags {
                let row = sqlx::query(
                    "select normalizer_version, key_fingerprint from blind_tags where bounded_context = $1 and tag_key = $2",
                )
                .bind(&decl.name)
                .bind(&b.tag_key)
                .fetch_optional(&self.inner.pool)
                .await?;
                let Some(row) = row else { continue };
                let nv: i32 = row.try_get("normalizer_version")?;
                if nv != b.normalizer_version {
                    return Err(Error::Config(format!(
                        "{}.{}: stored normalizer version {nv}, this instance declares {}",
                        decl.name, b.tag_key, b.normalizer_version
                    )));
                }
                let fp: Option<String> = row.try_get("key_fingerprint")?;
                let hmac = self.inner.blind_hmac.as_ref().ok_or_else(|| {
                    Error::Config(format!(
                        "{}.{} is blind but no blind index key is configured",
                        decl.name, b.tag_key
                    ))
                })?;
                let mine = hmac.fingerprint(&decl.name, &b.tag_key);
                match fp {
                    Some(fp) if fp != mine => {
                        return Err(Error::Config(format!(
                            "{}.{}: configured blind index key differs from the stored one",
                            decl.name, b.tag_key
                        )))
                    }
                    None => {
                        sqlx::query("update blind_tags set key_fingerprint = $3 where bounded_context = $1 and tag_key = $2")
                            .bind(&decl.name)
                            .bind(&b.tag_key)
                            .bind(&mine)
                            .execute(&self.inner.pool)
                            .await?;
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn register_blind_tags(&self, bc: &str) -> Result<()> {
        let Some(decl) = self.inner.decls.get(bc) else {
            return Ok(());
        };
        for b in &decl.blind_tags {
            let reg = self.inner.registry.read().await;
            let plain = reg
                .event_types_of(bc)
                .iter()
                .flat_map(|t| t.0.tag_mappings.clone())
                .chain(
                    reg.command_types_of(bc)
                        .iter()
                        .flat_map(|t| t.0.tag_mappings.clone()),
                )
                .any(|m| m.key == b.tag_key && !m.blind);
            drop(reg);
            if plain {
                return Err(Error::Config(format!(
                    "{bc}.{}: already mapped plainly; its values are stored in the clear and it can't become blind",
                    b.tag_key
                )));
            }
            let fp = self
                .inner
                .blind_hmac
                .as_ref()
                .map(|h| h.fingerprint(bc, &b.tag_key));
            sqlx::query(
                "insert into blind_tags (bounded_context, tag_key, normalizer_version, key_generation, key_fingerprint, erasure_subject_key, erasure_subject_tag)
                 values ($1, $2, $3, 1, $4, $5, $6) on conflict do nothing",
            )
            .bind(bc)
            .bind(&b.tag_key)
            .bind(b.normalizer_version)
            .bind(fp)
            .bind(&b.erasure_subject_key)
            .bind(&b.erasure_subject_tag)
            .execute(&self.inner.pool)
            .await?;
        }
        self.check_blind_tags().await
    }

    /// The startup reconciliation loop: register every declared type as the
    /// given Role, event types before command types before projections.
    pub async fn reconcile(&self, external_subject: &str) -> Result<ReconciliationReport> {
        let mut report = ReconciliationReport::default();
        let role = match self.role_by_subject(external_subject).await? {
            Some(r) => r,
            None => {
                report.skipped_no_access = self.inner.decls.keys().cloned().collect();
                return Ok(report);
            }
        };
        for (name, decl) in &self.inner.decls {
            if !self.inner.registry.read().await.contexts.contains_key(name) {
                report.skipped_missing_context.push(name.clone());
                continue;
            }
            let grant = match self.grant_for_role(&role, name).await? {
                Some(g) if g.has_level(AccessLevel::Admin) => g,
                _ => {
                    report.skipped_no_access.push(name.clone());
                    continue;
                }
            };
            if let Err(e) = self.register_blind_tags(name).await {
                report.errors.push(format!("{name}: {e}"));
                continue;
            }
            for e in &decl.event_types {
                match self.register_event_type(&grant, name, e).await {
                    Ok(true) => report.registered.push(format!("{name}.{}", e.name)),
                    Ok(false) => {}
                    Err(err) => report.errors.push(format!("{name}.{}: {err}", e.name)),
                }
            }
            for c in &decl.command_types {
                match self.register_command_type(&grant, name, c).await {
                    Ok(true) => report.registered.push(format!("{name}.{}", c.name)),
                    Ok(false) => {}
                    Err(err) => report.errors.push(format!("{name}.{}: {err}", c.name)),
                }
            }
            for p in &decl.projections {
                match self.register_projection(&grant, name, p).await {
                    Ok("staged") => report.staged_rebuilds.push(format!("{name}.{}", p.name)),
                    Ok("unchanged") => {}
                    Ok(_) => report.registered.push(format!("{name}.{}", p.name)),
                    Err(err) => report.errors.push(format!("{name}.{}: {err}", p.name)),
                }
            }
        }
        for e in &report.errors {
            tracing::error!("reconciliation: {e}");
        }
        Ok(report)
    }

    /// PrivateFieldsAreTheProjectionsToKeep: report, refuse nothing.
    pub(crate) async fn report_private_projection_inputs(&self) {
        let reg = self.inner.registry.read().await;
        for decl in self.inner.decls.values() {
            for p in &decl.projections {
                for et in &p.consumed_event_types {
                    if let Ok(t) = reg.event_type(&decl.name, et) {
                        if !t.0.private_fields.is_empty() {
                            tracing::info!(
                                "projection {}.{} consumes {et}, which has private fields; whatever it keeps is visible to every reader of the projection",
                                decl.name,
                                p.name
                            );
                        }
                    }
                }
            }
        }
    }
}

impl Skilj {
    /// What surface TypeRegistration exposes, for the admin's own context.
    pub async fn type_registration_view(&self, grant: &Grant) -> Result<Value> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("needs an admin grant"));
        }
        let bc = &grant.mapping.bounded_context;
        let reg = self.inner.registry.read().await;
        let decl = self.decl_for(bc);
        let event_types: Vec<Value> = reg
            .event_types_of(bc)
            .iter()
            .map(|t| serde_json::to_value(&t.0).unwrap_or_default())
            .collect();
        let command_types: Vec<Value> = reg
            .command_types_of(bc)
            .iter()
            .map(|t| {
                let mut v = serde_json::to_value(&t.0).unwrap_or_default();
                // As this instance has it deployed, not as registered.
                let q = decl
                    .and_then(|d| d.command_types.iter().find(|c| c.name == t.0.name))
                    .map(|c| serde_json::to_value(&c.consistency_query).unwrap_or_default())
                    .unwrap_or(Value::Array(vec![]));
                v["consistency_query"] = q;
                v
            })
            .collect();
        let projections: Vec<Value> = reg
            .projections_of(bc)
            .iter()
            .map(|p| {
                let rebuilds: Vec<Value> = reg
                    .rebuilds
                    .iter()
                    .filter(|r| &r.bounded_context == bc && r.projection == p.name)
                    .map(|r| serde_json::json!({"status": r.status, "schema_version": r.schema_version, "caught_up_to": r.caught_up_to}))
                    .collect();
                let mut v = serde_json::to_value(p).unwrap_or_default();
                v["rebuilds"] = Value::Array(rebuilds);
                v
            })
            .collect();
        Ok(serde_json::json!({
            "event_types": event_types,
            "command_types": command_types,
            "projections": projections,
        }))
    }
}
