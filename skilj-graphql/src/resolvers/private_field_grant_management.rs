//! `surface PrivateFieldGrantManagement` - `grantPrivateFieldAccessForEvent`,
//! `grantPrivateFieldAccessForCommand`, `revokePrivateFieldAccess`,
//! `listPrivateFieldGrants`. `ReadAccess`-gated
//! (`require_read_mapping`), not `AdminAccess`: this is self-service
//! sharing, the defining trait of the private-field mechanism - a caller
//! decides who else may read the private fields of a record it is
//! itself the default reader of, needing no admin anywhere (see
//! `access_control::grant_private_field_access_for_event`'s own doc
//! comment). `own`-kind default readers are always write-or-above
//! already (submitting a command requires `WriteAccess`), but an
//! `addressed`-kind one may be anyone the payload names regardless of
//! its own level - `ReadAccess` is what lets that caller reach this
//! surface at all, not just read what it was addressed with.

use super::{not_found, require_read_mapping};
use crate::error::to_graphql_error;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};

/// `grantPrivateFieldAccessForEvent(boundedContext: String!, granteeRoleId: ID!, eventSequence: Int): PrivateFieldGrant!` -
/// `eventSequence` omitted mints a blanket grant (see `value PrivateField`'s
/// own doc comment on what that reaches); named, a per-record one,
/// checked against `is_default_private_reader`.
pub fn grant_private_field_access_for_event_field(n: &Naming) -> Field {
    Field::new(
        n.root("grantPrivateFieldAccessForEvent"),
        TypeRef::named_nn(n.ty("PrivateFieldGrant")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let grantee_role_id = ctx.args.try_get("granteeRoleId")?.string()?.to_string();
                let event_sequence = ctx
                    .args
                    .get("eventSequence")
                    .filter(|v| !v.is_null())
                    .map(|v| v.i64())
                    .transpose()?;

                let grantee = skilj_core::db::get_role(&state.pool, &grantee_role_id)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("Role", &grantee_role_id))?;
                let event = match event_sequence {
                    Some(sequence) => Some(
                        skilj_core::db::get_event_by_sequence(
                            &state.pool,
                            &bounded_context_name,
                            sequence,
                        )
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("Event", &sequence.to_string()))?,
                    ),
                    None => None,
                };

                let grant = skilj_core::access_control::grant_private_field_access_for_event(
                    &access_mapping,
                    &grantee,
                    event.as_ref(),
                    skilj_core::shared::generate_token_id(),
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::insert_private_field_grant(&state.pool, &grant)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(grant)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "granteeRoleId",
        TypeRef::named_nn(TypeRef::ID),
    ))
    .argument(InputValue::new(
        "eventSequence",
        TypeRef::named(TypeRef::INT),
    ))
}

/// `grantPrivateFieldAccessForCommand(boundedContext: String!, granteeRoleId: ID!, commandId: ID): PrivateFieldGrant!` -
/// same shape and reasoning as `grantPrivateFieldAccessForEvent` above,
/// checked against a `Command` instead of an `Event`.
pub fn grant_private_field_access_for_command_field(n: &Naming) -> Field {
    Field::new(
        n.root("grantPrivateFieldAccessForCommand"),
        TypeRef::named_nn(n.ty("PrivateFieldGrant")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let grantee_role_id = ctx.args.try_get("granteeRoleId")?.string()?.to_string();
                let command_id = ctx
                    .args
                    .get("commandId")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?;

                let grantee = skilj_core::db::get_role(&state.pool, &grantee_role_id)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("Role", &grantee_role_id))?;
                let command = match &command_id {
                    Some(id) => Some(
                        skilj_core::db::get_command_by_external_id(
                            &state.pool,
                            &bounded_context_name,
                            id,
                        )
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("Command", id))?,
                    ),
                    None => None,
                };

                let grant = skilj_core::access_control::grant_private_field_access_for_command(
                    &access_mapping,
                    &grantee,
                    command.as_ref(),
                    skilj_core::shared::generate_token_id(),
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::insert_private_field_grant(&state.pool, &grant)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(grant)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "granteeRoleId",
        TypeRef::named_nn(TypeRef::ID),
    ))
    .argument(InputValue::new("commandId", TypeRef::named(TypeRef::ID)))
}

/// `revokePrivateFieldAccess(boundedContext: String!, grantId: ID!): PrivateFieldGrant!` -
/// only the `Role` that made the grant may end it, not the grantee and
/// not an admin (see `access_control::revoke_private_field_access`'s own
/// doc comment).
pub fn revoke_private_field_access_field(n: &Naming) -> Field {
    Field::new(
        n.root("revokePrivateFieldAccess"),
        TypeRef::named_nn(n.ty("PrivateFieldGrant")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let grant_id = ctx.args.try_get("grantId")?.string()?.to_string();

                let grant = skilj_core::db::get_private_field_grant(
                    &state.pool,
                    &bounded_context_name,
                    &grant_id,
                )
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(|| not_found("PrivateFieldGrant", &grant_id))?;

                let revoked = skilj_core::access_control::revoke_private_field_access(
                    &access_mapping,
                    &grant,
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::update_private_field_grant(&state.pool, &revoked)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(revoked)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("grantId", TypeRef::named_nn(TypeRef::ID)))
}

/// `listPrivateFieldGrants(boundedContext: String!, grantorRoleId: ID): [PrivateFieldGrant!]!` -
/// omitted `grantorRoleId` lists the caller's own outgoing grants,
/// available to anyone; naming a *different* grantor requires
/// `access_mapping.level = admin` - the compliance/oversight half (see
/// `access_control::list_private_field_grants`'s own doc comment).
pub fn list_private_field_grants_field(n: &Naming) -> Field {
    Field::new(
        n.root("listPrivateFieldGrants"),
        TypeRef::named_nn_list_nn(n.ty("PrivateFieldGrant")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let grantor_role_id = ctx
                    .args
                    .get("grantorRoleId")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?;

                let grantor = match &grantor_role_id {
                    Some(id) => Some(
                        skilj_core::db::get_role(&state.pool, id)
                            .await
                            .map_err(to_graphql_error)?
                            .ok_or_else(|| not_found("Role", id))?,
                    ),
                    None => None,
                };

                let all_grants_in_context = skilj_core::db::list_private_field_grants_for_context(
                    &state.pool,
                    &bounded_context_name,
                )
                .await
                .map_err(to_graphql_error)?;
                let grants = skilj_core::access_control::list_private_field_grants(
                    &access_mapping,
                    grantor.as_ref(),
                    &all_grants_in_context,
                )
                .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::list(
                    grants.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "grantorRoleId",
        TypeRef::named(TypeRef::ID),
    ))
}
