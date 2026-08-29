//! `surface BoundedContextTemplating` - `createBoundedContextFromTemplate`,
//! `resyncBoundedContextFromTemplate` (Codeberg issue #13). `Superadmin`-
//! gated, like `bounded_context_creation`/`bounded_context_deletion`
//! (`@guarantee SuperadminOnly`) - not a per-context `RoleAccessMapping`,
//! since both are operations on the set of contexts a deployment has
//! rather than operations within one.
//!
//! `createBoundedContextFromTemplate` also grants the given `role` access
//! to the new tenant in the same call (`@guarantee
//! AccessGrantedWithCreation`) - a deliberate, spec-documented exception
//! to `addBoundedContext`'s own "creation and access-granting are always
//! two separate acts" precedent, justified by this feature's whole point:
//! adding a tenant should be one ops action, not several coordinated ones.
//!
//! The type-registration copy step both mutations share
//! (`apply_template_registrations`) runs on the caller's own superadmin
//! authority, not a real per-context `RoleAccessMapping` - see the note
//! above `rule CreateBoundedContextFromTemplate` in specs/skilj.allium for
//! why: the grant `createBoundedContextFromTemplate` makes can be at any
//! level (even `read`), and `resyncBoundedContextFromTemplate` names no
//! mapping at all to gate on. `synthetic_admin_mapping` builds a
//! transient, never-persisted `RoleAccessMapping` purely to satisfy
//! `register_event_type`/`register_command_type`/`register_projection`'s
//! own existing gate, reusing those functions - and their real
//! validation/versioning/staging behaviour - wholesale rather than
//! duplicating it.

use super::{load_bounded_context_with_mappings, not_found, require_caller};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::db::Pool;
use skilj_core::event_store::{BoundedContext, EventType};
use skilj_core::plugin::ProjectionDispatcher;
use skilj_core::projections::{ProjectionRebuildStatus, ProjectionRegistration};
use std::collections::HashMap;

fn synthetic_admin_mapping(
    caller: &Role,
    bounded_context: &BoundedContext,
    now: chrono::DateTime<chrono::Utc>,
) -> RoleAccessMapping {
    RoleAccessMapping {
        role: caller.clone(),
        bounded_context: bounded_context.clone(),
        level: AccessLevel::Admin,
        can_read_sensitive: true,
        status: RoleStatus::Active,
        created_at: now,
        revoked_at: None,
    }
}

/// See `rule ApplyTemplateRegistrations` - the shared step
/// `createBoundedContextFromTemplate`/`resyncBoundedContextFromTemplate`
/// both drive. Loop order (event types, then command types, then
/// projections) matches `skilj/src/lib.rs`'s own `reconcile_*` order, for
/// the same reason: `ConsistencyTagKeysAreDeclared` needs a `CommandType`
/// tag key already declared by an `EventType`, and `RegisterProjection`'s
/// `consumed_event_types` argument needs those `EventType`s to already
/// exist in `tenant`'s own schema.
async fn apply_template_registrations(
    pool: &Pool,
    caller: &Role,
    template: &BoundedContext,
    tenant: &BoundedContext,
    projection_dispatcher: &dyn ProjectionDispatcher,
    now: chrono::DateTime<chrono::Utc>,
) -> async_graphql::Result<()> {
    let mapping = synthetic_admin_mapping(caller, tenant, now);

    let template_event_types =
        skilj_core::db::list_event_types_for_bounded_context(pool, &template.name)
            .await
            .map_err(to_graphql_error)?;
    let mut tenant_event_types_by_name: HashMap<String, EventType> = HashMap::new();
    for source in template_event_types {
        let existing = skilj_core::db::get_event_type(pool, &tenant.name, &source.name)
            .await
            .map_err(to_graphql_error)?;
        let registration = skilj_core::event_store::register_event_type(
            &mapping,
            tenant,
            source.name.clone(),
            source.schema.clone(),
            source.tag_mappings.clone(),
            source.sensitive_fields.clone(),
            source.external_creation_allowed,
            source.direct_creation_allowed,
            source.system_triggered_allowed,
            source.system_triggered_schedule.clone(),
            source.missed_occurrence_policy,
            source.event_read_allowed,
            existing.as_ref(),
            now,
        )
        .map_err(to_graphql_error)?;
        skilj_core::db::upsert_event_type(pool, registration.event_type())
            .await
            .map_err(to_graphql_error)?;
        tenant_event_types_by_name.insert(
            registration.event_type().name.clone(),
            registration.event_type().clone(),
        );
    }

    let template_command_types =
        skilj_core::db::list_command_types_for_bounded_context(pool, &template.name)
            .await
            .map_err(to_graphql_error)?;
    for source in template_command_types {
        let existing = skilj_core::db::get_command_type(pool, &tenant.name, &source.name)
            .await
            .map_err(to_graphql_error)?;
        let registration = skilj_core::event_store::register_command_type(
            &mapping,
            tenant,
            source.name.clone(),
            source.schema.clone(),
            source.tag_mappings.clone(),
            source.sensitive_fields.clone(),
            source.rest_trigger_allowed,
            existing.as_ref(),
        )
        .map_err(to_graphql_error)?;
        skilj_core::db::upsert_command_type(pool, registration.command_type())
            .await
            .map_err(to_graphql_error)?;
    }

    let template_projections =
        skilj_core::db::list_projections_for_bounded_context(pool, &template.name)
            .await
            .map_err(to_graphql_error)?;
    let tenant_events = skilj_core::db::list_events_for_bounded_context(pool, &tenant.name)
        .await
        .map_err(to_graphql_error)?;
    for source in template_projections {
        let mut consumed_event_types = Vec::with_capacity(source.consumed_event_types.len());
        for consumed in &source.consumed_event_types {
            let tenant_event_type = tenant_event_types_by_name
                .get(&consumed.name)
                .cloned()
                .ok_or_else(|| not_found("EventType", &consumed.name))?;
            consumed_event_types.push(tenant_event_type);
        }
        let existing = skilj_core::db::get_projection(pool, &tenant.name, &source.name)
            .await
            .map_err(to_graphql_error)?;
        let staged = skilj_core::db::get_projection_rebuild(
            pool,
            &tenant.name,
            &source.name,
            ProjectionRebuildStatus::Pending,
        )
        .await
        .map_err(to_graphql_error)?;

        let registration = skilj_core::projections::register_projection(
            &mapping,
            tenant,
            source.name.clone(),
            source.schema.clone(),
            consumed_event_types,
            source.sync,
            existing.as_ref(),
            staged.as_ref(),
            &tenant_events,
        )
        .map_err(to_graphql_error)?;

        match registration {
            ProjectionRegistration::Created {
                projection,
                needs_history_fold,
            } => {
                skilj_core::db::upsert_projection(pool, &projection)
                    .await
                    .map_err(to_graphql_error)?;
                if needs_history_fold {
                    skilj_core::db::fold_history_into_new_sync_projection(
                        pool,
                        &projection,
                        projection_dispatcher,
                    )
                    .await
                    .map_err(to_graphql_error)?;
                }
            }
            ProjectionRegistration::ReconciledTrivially(projection) => {
                skilj_core::db::upsert_projection(pool, &projection)
                    .await
                    .map_err(to_graphql_error)?;
            }
            ProjectionRegistration::RebuildStaged(rebuild) => {
                skilj_core::db::upsert_projection_rebuild(pool, &rebuild)
                    .await
                    .map_err(to_graphql_error)?;
            }
        }
    }

    Ok(())
}

/// `createBoundedContextFromTemplate(template: String!, name: String!, roleId: ID!, level: AccessLevel!, canReadSensitive: Boolean!): BoundedContext!`
pub fn create_bounded_context_from_template_field() -> Field {
    Field::new(
        "createBoundedContextFromTemplate",
        TypeRef::named_nn("BoundedContext"),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let template_name = ctx.args.try_get("template")?.string()?.to_string();
                let name = ctx.args.try_get("name")?.string()?.to_string();
                let role_id = ctx.args.try_get("roleId")?.string()?.to_string();
                let level = match ctx.args.try_get("level")?.enum_name()? {
                    "READ" => AccessLevel::Read,
                    "WRITE" => AccessLevel::Write,
                    _ => AccessLevel::Admin,
                };
                let can_read_sensitive = ctx.args.try_get("canReadSensitive")?.boolean()?;
                let now = chrono::Utc::now();

                let template = skilj_core::db::get_bounded_context(&state.pool, &template_name)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("BoundedContext", &template_name))?;
                let existing_contexts = skilj_core::db::list_bounded_contexts(&state.pool)
                    .await
                    .map_err(to_graphql_error)?;

                let tenant = skilj_core::bootstrap::create_bounded_context_from_template(
                    &caller,
                    &template,
                    name.clone(),
                    &existing_contexts,
                    now,
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::insert_bounded_context(&state.pool, &tenant)
                    .await
                    .map_err(to_graphql_error)?;

                let role = skilj_core::db::get_role(&state.pool, &role_id)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("Role", &role_id))?;
                let existing_mappings = skilj_core::db::list_role_access_mappings(&state.pool)
                    .await
                    .map_err(to_graphql_error)?;
                let mapping = skilj_core::access_control::grant_role_access_mapping(
                    &caller,
                    &role,
                    &tenant,
                    level,
                    can_read_sensitive,
                    &existing_mappings,
                    now,
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::insert_role_access_mapping(&state.pool, &mapping)
                    .await
                    .map_err(to_graphql_error)?;

                apply_template_registrations(
                    &state.pool,
                    &caller,
                    &template,
                    &tenant,
                    state.projection_dispatcher.as_ref(),
                    now,
                )
                .await?;

                let with_mappings = load_bounded_context_with_mappings(&state.pool, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .expect("just inserted this bounded context - it must be readable back");
                Ok(Some(FieldValue::owned_any(with_mappings)))
            })
        },
    )
    .argument(InputValue::new(
        "template",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
    .argument(InputValue::new("roleId", TypeRef::named_nn(TypeRef::ID)))
    .argument(InputValue::new("level", TypeRef::named_nn("AccessLevel")))
    .argument(InputValue::new(
        "canReadSensitive",
        TypeRef::named_nn(TypeRef::BOOLEAN),
    ))
}

/// `resyncBoundedContextFromTemplate(boundedContext: String!): BoundedContext!`
pub fn resync_bounded_context_from_template_field() -> Field {
    Field::new(
        "resyncBoundedContextFromTemplate",
        TypeRef::named_nn("BoundedContext"),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let now = chrono::Utc::now();

                let tenant =
                    skilj_core::db::get_bounded_context(&state.pool, &bounded_context_name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("BoundedContext", &bounded_context_name))?;
                let template =
                    skilj_core::bootstrap::resync_bounded_context_from_template(&caller, &tenant)
                        .map_err(to_graphql_error)?
                        .clone();

                apply_template_registrations(
                    &state.pool,
                    &caller,
                    &template,
                    &tenant,
                    state.projection_dispatcher.as_ref(),
                    now,
                )
                .await?;

                let with_mappings =
                    load_bounded_context_with_mappings(&state.pool, &bounded_context_name)
                        .await
                        .map_err(to_graphql_error)?
                        .expect(
                            "just resynced this bounded context - it must still be readable back",
                        );
                Ok(Some(FieldValue::owned_any(with_mappings)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}
