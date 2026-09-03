//! `surface EventTypeAdminOperations` - `createExternalEventToken`,
//! `createDirectCreationToken`, `createEventReadToken`. `AdminAccess`-
//! gated on the target `EventType`'s own bounded context.

use super::{create_type_token_field, not_found, require_admin_mapping};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};

pub fn create_external_event_token_field() -> Field {
    create_type_token_field!(
        "createExternalEventToken",
        "ExternalEventToken",
        "eventTypeName",
        "EventType",
        skilj_core::db::get_event_type,
        skilj_core::access_control::create_external_event_token,
        skilj_core::db::insert_external_event_token
    )
}
pub fn create_direct_creation_token_field() -> Field {
    create_type_token_field!(
        "createDirectCreationToken",
        "DirectCreationToken",
        "eventTypeName",
        "EventType",
        skilj_core::db::get_event_type,
        skilj_core::access_control::create_direct_creation_token,
        skilj_core::db::insert_direct_creation_token
    )
}
/// `createEventReadToken(boundedContext: String!, eventTypeName: String!, scope: String): EventReadToken!` -
/// hand-written, not `create_type_token_field!`, since `EventReadToken`
/// alone carries `scope` (cross-tenant read fix, docs/architecture.md's
/// own write-up of these passes) - see that macro's own doc comment for
/// why. Same shape as its expansion otherwise, plus the extra argument.
pub fn create_event_read_token_field() -> Field {
    Field::new(
        "createEventReadToken",
        TypeRef::named_nn("EventReadToken"),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let type_name = ctx.args.try_get("eventTypeName")?.string()?.to_string();
                let scope = ctx
                    .args
                    .get("scope")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?;

                let target_type =
                    skilj_core::db::get_event_type(&state.pool, &bounded_context_name, &type_name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("EventType", &type_name))?;

                let token = skilj_core::access_control::create_event_read_token(
                    &access_mapping,
                    &target_type,
                    skilj_core::shared::generate_token_id(),
                    skilj_core::shared::generate_token_secret(),
                    scope,
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::insert_event_read_token(&state.pool, &token)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(token)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "eventTypeName",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("scope", TypeRef::named(TypeRef::STRING)))
}
