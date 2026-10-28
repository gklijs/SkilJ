//! AddBoundedContext, ListBoundedContexts, ArchiveBoundedContext,
//! DeleteBoundedContext.

use serde::Serialize;
use serde_json::json;
use sqlx::Row;

use super::rows::{mapping_from_row, MAPPING_COLUMNS};
use super::Skilj;
use crate::error::{Error, Result};
use crate::model::*;
use crate::validate;

/// One row of the bounded context directory.
#[derive(Debug, Clone, Serialize)]
pub struct ContextDirectoryEntry {
    pub context: BoundedContext,
    pub mappings: Vec<(Role, AccessMapping)>,
}

impl Skilj {
    fn superadmin(caller: &Role) -> Result<()> {
        if caller.active && caller.superadmin {
            Ok(())
        } else {
            Err(Error::forbidden("needs an active superadmin role"))
        }
    }

    pub(crate) async fn insert_context(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        caller: &Role,
        name: &str,
        template: Option<&str>,
    ) -> Result<()> {
        validate::valid_bounded_context_name(name)?;
        let n = sqlx::query(
            "insert into bounded_contexts (name, status, created_at, created_by_role, template)
             values ($1, 'active', $2, $3, $4) on conflict do nothing",
        )
        .bind(name)
        .bind(Self::now())
        .bind(&caller.id)
        .bind(template)
        .execute(&mut **tx)
        .await?
        .rows_affected();
        if n == 0 {
            return Err(Error::conflict(format!("a bounded context named {name} exists")));
        }
        sqlx::query("insert into sequence_counters (bounded_context) values ($1)")
            .bind(name)
            .execute(&mut **tx)
            .await?;
        self.notify_registry_changed(tx).await?;
        Ok(())
    }

    /// AddBoundedContext: creates the context and grants nobody anything.
    pub async fn add_bounded_context(&self, caller: &Role, name: &str) -> Result<()> {
        Self::superadmin(caller)?;
        let mut tx = self.inner.pool.begin().await?;
        self.insert_context(&mut tx, caller, name, None).await?;
        tx.commit().await?;
        self.reload_registry().await?;
        self.warm_context(name).await
    }

    /// ListBoundedContexts.
    pub async fn list_bounded_contexts(&self, caller: &Role) -> Result<Vec<ContextDirectoryEntry>> {
        Self::superadmin(caller)?;
        let reg = self.registry().await;
        let by_ctx = self.mappings_by_role().await?;
        let roles = self.list_roles(caller).await?;
        let mut out = vec![];
        for c in reg.contexts.values() {
            let mappings = by_ctx
                .get(&c.name)
                .map(|ms| {
                    ms.iter()
                        .filter_map(|m| roles.iter().find(|r| r.id == m.role_id).map(|r| (r.clone(), m.clone())))
                        .collect()
                })
                .unwrap_or_default();
            out.push(ContextDirectoryEntry {
                context: c.clone(),
                mappings,
            });
        }
        Ok(out)
    }

    /// ArchiveBoundedContext.
    pub async fn archive_bounded_context(&self, grant: &Grant) -> Result<()> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("archiving needs an admin grant"));
        }
        let bc = &grant.mapping.bounded_context;
        if bc == ADMIN_CONTEXT {
            return Err(Error::forbidden("the admin context is permanent"));
        }
        let mut tx = self.inner.pool.begin().await?;
        let n = sqlx::query("update bounded_contexts set status = 'archived' where name = $1 and status = 'active'")
            .bind(bc)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if n == 0 {
            return Err(Error::conflict("already archived"));
        }
        self.notify_registry_changed(&mut tx).await?;
        tx.commit().await?;
        self.reload_registry().await
    }

    /// DeleteBoundedContext: archived first, superadmin only, everything
    /// scoped to it goes; the grants' ends are recorded in the trail.
    pub async fn delete_bounded_context(&self, caller: &Role, name: &str) -> Result<()> {
        Self::superadmin(caller)?;
        if name == ADMIN_CONTEXT {
            return Err(Error::forbidden("the admin context is permanent"));
        }
        let mut tx = self.inner.pool.begin().await?;
        let status: Option<String> = sqlx::query_scalar("select status from bounded_contexts where name = $1 for update")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;
        match status.as_deref() {
            None => return Err(Error::not_found(format!("bounded context {name}"))),
            Some("active") => return Err(Error::conflict("archive the bounded context before deleting it")),
            _ => {}
        }
        let mut events = vec![];
        let mappings = sqlx::query(&format!("select {MAPPING_COLUMNS} from access_mappings where bounded_context = $1 and active"))
            .bind(name)
            .fetch_all(&mut *tx)
            .await?;
        for m in mappings {
            let m = mapping_from_row(&m)?;
            events.extend(
                self.record_access_event(&mut tx, "AccessRevoked", &m.role_id, name, json!({"actor_role_id": caller.id, "reason": "bounded_context_deleted"}))
                    .await?,
            );
        }
        let grants = sqlx::query("select id, grantee from private_field_grants where bounded_context = $1 and active")
            .bind(name)
            .fetch_all(&mut *tx)
            .await?;
        for g in grants {
            let id: String = g.try_get("id")?;
            let grantee: String = g.try_get("grantee")?;
            events.extend(
                self.record_access_event(
                    &mut tx,
                    "PrivateFieldAccessRevoked",
                    &grantee,
                    name,
                    json!({"actor_role_id": caller.id, "grant_id": id, "reason": "bounded_context_deleted"}),
                )
                .await?,
            );
        }
        // Order matters for the foreign keys without cascades.
        sqlx::query("delete from event_tags where bounded_context = $1").bind(name).execute(&mut *tx).await?;
        sqlx::query("delete from bounded_contexts where name = $1").bind(name).execute(&mut *tx).await?;
        self.notify_registry_changed(&mut tx).await?;
        tx.commit().await?;
        self.after_commit(&events).await;
        self.inner.cache.invalidate(name);
        self.inner.dispatchers.lock().expect("dispatch map").remove(name);
        let _ = self.inner.bus.send(super::BusMessage::AccessChanged { mapping_id: None, role_id: None });
        self.reload_registry().await
    }
}
