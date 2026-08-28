//! `surface AccessManagement` - `createRole`, `revokeRole`,
//! `grantRoleAccessMapping`, `revokeRoleAccessMapping`. All four are
//! `Superadmin`-gated - `require_caller` alone resolves the actor;
//! `access_control::create_role`/`revoke_role`/`grant_role_access_mapping`/
//! `revoke_role_access_mapping` each re-check `caller.superadmin` for
//! real via `require_active_superadmin`, so this module never duplicates
//! that check itself.

use super::{not_found, require_caller};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::access_control::AccessLevel;
use skilj_core::shared::generate_token_id;

/// `createRole(name: String!, superadmin: Boolean!, externalSubject: String!): Role!`
pub fn create_role_field() -> Field {
    Field::new("createRole", TypeRef::named_nn("Role"), |ctx| {
        FieldFuture::new(async move {
            let caller = require_caller(&ctx)?;
            let state = ctx.data::<GraphqlState>()?;
            let name = ctx.args.try_get("name")?.string()?.to_string();
            let superadmin = ctx.args.try_get("superadmin")?.boolean()?;
            let external_subject = ctx.args.try_get("externalSubject")?.string()?.to_string();

            let existing_roles = skilj_core::db::list_roles(&state.pool)
                .await
                .map_err(to_graphql_error)?;
            let role = skilj_core::access_control::create_role(
                &caller,
                name,
                superadmin,
                external_subject,
                &existing_roles,
                generate_token_id(),
                chrono::Utc::now(),
            )
            .map_err(to_graphql_error)?;
            skilj_core::db::insert_role(&state.pool, &role)
                .await
                .map_err(to_graphql_error)?;

            Ok(Some(FieldValue::owned_any(role)))
        })
    })
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
    .argument(InputValue::new(
        "superadmin",
        TypeRef::named_nn(TypeRef::BOOLEAN),
    ))
    .argument(InputValue::new(
        "externalSubject",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}

/// `revokeRole(roleId: ID!): Role!`
pub fn revoke_role_field() -> Field {
    Field::new("revokeRole", TypeRef::named_nn("Role"), |ctx| {
        FieldFuture::new(async move {
            let caller = require_caller(&ctx)?;
            let state = ctx.data::<GraphqlState>()?;
            let role_id = ctx.args.try_get("roleId")?.string()?.to_string();

            let role = skilj_core::db::get_role(&state.pool, &role_id)
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(|| not_found("Role", &role_id))?;
            let active_mappings =
                skilj_core::db::list_active_role_access_mappings_for_role(&state.pool, &role_id)
                    .await
                    .map_err(to_graphql_error)?;

            let (revoked_role, revoked_mappings) = skilj_core::access_control::revoke_role(
                &caller,
                &role,
                &active_mappings,
                chrono::Utc::now(),
            )
            .map_err(to_graphql_error)?;

            skilj_core::db::revoke_role_and_mappings(&state.pool, &revoked_role, &revoked_mappings)
                .await
                .map_err(to_graphql_error)?;

            // RevocationClosesTheConnection's push half (drift audit
            // finding #4) - one notification per mapping this cascade
            // just revoked, since each may back a live EventSubscription
            // in its own bounded context. `notify_revocation` alongside
            // it is the same push's cross-instance half (`@guarantee
            // DeliverySpansInstances`) - see `skilj_core::cross_instance`'s
            // own module doc comment.
            for mapping in &revoked_mappings {
                let revoked = skilj_core::access_control::RevokedMapping {
                    role_id: mapping.role.id.clone(),
                    bounded_context: mapping.bounded_context.name.clone(),
                };
                state.revocation_broadcaster.publish(revoked.clone());
                skilj_core::db::notify_revocation(
                    &state.pool,
                    &revoked,
                    state.revocation_broadcaster.instance_id(),
                )
                .await;
            }

            Ok(Some(FieldValue::owned_any(revoked_role)))
        })
    })
    .argument(InputValue::new("roleId", TypeRef::named_nn(TypeRef::ID)))
}

/// `grantRoleAccessMapping(roleId: ID!, boundedContext: String!, level: AccessLevel!, canReadSensitive: Boolean!): RoleAccessMapping!`
pub fn grant_role_access_mapping_field() -> Field {
    Field::new(
        "grantRoleAccessMapping",
        TypeRef::named_nn("RoleAccessMapping"),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let role_id = ctx.args.try_get("roleId")?.string()?.to_string();
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let level = match ctx.args.try_get("level")?.enum_name()? {
                    "READ" => AccessLevel::Read,
                    "WRITE" => AccessLevel::Write,
                    _ => AccessLevel::Admin,
                };
                let can_read_sensitive = ctx.args.try_get("canReadSensitive")?.boolean()?;

                let role = skilj_core::db::get_role(&state.pool, &role_id)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("Role", &role_id))?;
                let bounded_context =
                    skilj_core::db::get_bounded_context(&state.pool, &bounded_context_name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("BoundedContext", &bounded_context_name))?;
                let existing_mappings = skilj_core::db::list_role_access_mappings(&state.pool)
                    .await
                    .map_err(to_graphql_error)?;

                let mapping = skilj_core::access_control::grant_role_access_mapping(
                    &caller,
                    &role,
                    &bounded_context,
                    level,
                    can_read_sensitive,
                    &existing_mappings,
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::insert_role_access_mapping(&state.pool, &mapping)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(mapping)))
            })
        },
    )
    .argument(InputValue::new("roleId", TypeRef::named_nn(TypeRef::ID)))
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("level", TypeRef::named_nn("AccessLevel")))
    .argument(InputValue::new(
        "canReadSensitive",
        TypeRef::named_nn(TypeRef::BOOLEAN),
    ))
}

/// `revokeRoleAccessMapping(roleId: ID!, boundedContext: String!): RoleAccessMapping!`,
/// identifying the mapping to revoke by its `(role, bounded_context)`
/// key, the same unambiguous pair `db::get_active_role_access_mapping`
/// already looks up by (see its own doc comment: at most one active
/// mapping can ever match).
pub fn revoke_role_access_mapping_field() -> Field {
    Field::new(
        "revokeRoleAccessMapping",
        TypeRef::named_nn("RoleAccessMapping"),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let role_id = ctx.args.try_get("roleId")?.string()?.to_string();
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();

                let mapping = skilj_core::db::get_active_role_access_mapping(
                    &state.pool,
                    &role_id,
                    &bounded_context_name,
                )
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(|| {
                    not_found(
                        "RoleAccessMapping",
                        &format!("{role_id}/{bounded_context_name}"),
                    )
                })?;

                let revoked = skilj_core::access_control::revoke_role_access_mapping(
                    &caller,
                    &mapping,
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::revoke_active_role_access_mapping(
                    &state.pool,
                    &role_id,
                    &bounded_context_name,
                    revoked
                        .revoked_at
                        .expect("revoke_role_access_mapping always stamps revoked_at"),
                )
                .await
                .map_err(to_graphql_error)?;

                // RevocationClosesTheConnection's push half (drift audit
                // finding #4), plus its cross-instance half - see
                // revoke_role_field's own identical call above.
                let revoked_mapping = skilj_core::access_control::RevokedMapping {
                    role_id: role_id.clone(),
                    bounded_context: bounded_context_name.clone(),
                };
                state
                    .revocation_broadcaster
                    .publish(revoked_mapping.clone());
                skilj_core::db::notify_revocation(
                    &state.pool,
                    &revoked_mapping,
                    state.revocation_broadcaster.instance_id(),
                )
                .await;

                Ok(Some(FieldValue::owned_any(revoked)))
            })
        },
    )
    .argument(InputValue::new("roleId", TypeRef::named_nn(TypeRef::ID)))
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}
