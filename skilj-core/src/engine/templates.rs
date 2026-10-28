//! CreateBoundedContextFromTemplate, ResyncBoundedContextFromTemplate and
//! ApplyTemplateRegistrations.
//!
//! Applying a template resubmits the template's *registered* types as
//! registrations against the tenant. Registered state carries no decide()
//! or project(): those come from this binary. A tenant is only usable on a
//! process that declares deciders for it - which the spec never says. We
//! resolve deciders for a tenant from its template's declaration.

use super::registry::Authority;
use super::Skilj;
use crate::decl::{CommandTypeDecl, EventTypeDecl, ProjectionDecl};
use crate::error::{Error, Result};
use crate::model::*;

impl Skilj {
    /// CreateBoundedContextFromTemplate.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_from_template(
        &self,
        caller: &Role,
        template: &str,
        name: &str,
        role_id: &str,
        level: AccessLevel,
        can_read_sensitive: bool,
        scope: Option<&str>,
    ) -> Result<Vec<String>> {
        if !(caller.active && caller.superadmin) {
            return Err(Error::forbidden("needs an active superadmin role"));
        }
        {
            let reg = self.inner.registry.read().await;
            let t = reg.context(template)?;
            if t.status != ContextStatus::Active {
                return Err(Error::conflict("the template is archived"));
            }
            if t.template.is_some() {
                return Err(Error::invalid("a tenant cannot be a template"));
            }
            if template == ADMIN_CONTEXT {
                return Err(Error::invalid("the admin context is not a template"));
            }
        }
        let role = self.role_by_id(role_id).await?;
        if !role.active {
            return Err(Error::conflict("role is revoked"));
        }
        let mut tx = self.inner.pool.begin().await?;
        self.insert_context(&mut tx, caller, name, Some(template)).await?;
        let (m, events) = self
            .insert_mapping(&mut tx, &caller.id, &role, name, level, can_read_sensitive, scope)
            .await?;
        // Blind tags are part of what a tenant needs to register blind
        // mappings; copy the template's (fingerprint recorded by the first
        // instance that declares them).
        sqlx::query(
            "insert into blind_tags (bounded_context, tag_key, normalizer_version, key_generation, key_fingerprint, erasure_subject_key, erasure_subject_tag)
             select $2, tag_key, normalizer_version, 1, null, erasure_subject_key, erasure_subject_tag from blind_tags where bounded_context = $1",
        )
        .bind(template)
        .bind(name)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        self.after_commit(&events).await;
        let _ = self.inner.bus.send(super::BusMessage::AccessChanged {
            mapping_id: Some(m.id),
            role_id: Some(role.id),
        });
        self.reload_registry().await?;
        self.warm_context(name).await?;
        self.apply_template(name, template).await
    }

    /// ResyncBoundedContextFromTemplate.
    pub async fn resync_from_template(&self, caller: &Role, name: &str) -> Result<Vec<String>> {
        if !(caller.active && caller.superadmin) {
            return Err(Error::forbidden("needs an active superadmin role"));
        }
        let template = {
            let reg = self.inner.registry.read().await;
            let c = reg.context(name)?;
            if c.status != ContextStatus::Active {
                return Err(Error::Archived(name.into()));
            }
            let t = c.template.clone().ok_or_else(|| Error::invalid(format!("{name} has no template")))?;
            if reg.context(&t)?.status != ContextStatus::Active {
                return Err(Error::conflict("the template is archived"));
            }
            t
        };
        self.apply_template(name, &template).await
    }

    /// ApplyTemplateRegistrations: event types, then command types, then
    /// projections, each a registration through the ordinary rule. Returns
    /// per-type errors rather than failing the whole application.
    async fn apply_template(&self, name: &str, template: &str) -> Result<Vec<String>> {
        let reg = self.registry().await;
        let mut errors = vec![];
        for t in reg.event_types_of(template) {
            let d = &t.0;
            let decl = self.inner.decls.get(template).and_then(|c| c.event_types.iter().find(|e| e.name == d.name));
            let schedule = match (&d.system_triggered_schedule, d.missed_occurrence_policy, decl.and_then(|x| x.schedule.clone())) {
                (Some(cron), Some(p), Some((_, _, f))) => Some((cron.clone(), p, f)),
                _ => None,
            };
            let e = EventTypeDecl {
                name: d.name.clone(),
                schema: d.schema.clone(),
                tag_mappings: d.tag_mappings.clone(),
                owner_tag_key: d.owner_tag_key.clone(),
                sensitive_fields: d.sensitive_fields.clone(),
                private_fields: d.private_fields.clone(),
                external_creation_allowed: d.external_creation_allowed,
                direct_creation_allowed: d.direct_creation_allowed,
                event_read_allowed: d.event_read_allowed,
                schedule,
            };
            if let Err(err) = self.register_event_type_as(&Authority::Superadmin, name, &e).await {
                errors.push(format!("{}: {err}", d.name));
            }
        }
        for t in reg.command_types_of(template) {
            let d = &t.0;
            let decl = self.inner.decls.get(template).and_then(|c| c.command_types.iter().find(|x| x.name == d.name));
            let mut c = match decl {
                Some(c) => c.clone(),
                None => CommandTypeDecl::new(d.name.clone(), d.schema.clone(), |_| {
                    crate::decl::Decision::reject("not_declared", "no decider in this process")
                }),
            };
            c.schema = d.schema.clone();
            c.tag_mappings = d.tag_mappings.clone();
            c.owner_tag_key = d.owner_tag_key.clone();
            c.sensitive_fields = d.sensitive_fields.clone();
            c.private_fields = d.private_fields.clone();
            c.rest_trigger_allowed = d.rest_trigger_allowed;
            if let Err(err) = self.register_command_type_as(&Authority::Superadmin, name, &c).await {
                errors.push(format!("{}: {err}", d.name));
            }
        }
        for p in reg.projections_of(template) {
            let decl = self
                .inner
                .decls
                .get(template)
                .and_then(|c| c.projections.iter().find(|x| x.name == p.name))
                .cloned();
            let mut pd = match decl {
                Some(d) => d,
                None => ProjectionDecl::new(p.name.clone(), p.schema.clone(), &[], |_| vec![], |_, _, _| None),
            };
            pd.schema = p.schema.clone();
            pd.consumed_event_types = p.consumed_event_types.clone();
            pd.sync = p.sync;
            pd.requires_capability = p.requires_capability.clone();
            if let Err(err) = self.register_projection_as(&Authority::Superadmin, name, &pd).await {
                errors.push(format!("{}: {err}", p.name));
            }
        }
        Ok(errors)
    }
}
