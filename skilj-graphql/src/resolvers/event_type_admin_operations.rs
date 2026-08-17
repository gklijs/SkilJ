//! `surface EventTypeAdminOperations` - `createExternalEventToken`,
//! `createDirectCreationToken`, `createEventReadToken`. `AdminAccess`-
//! gated on the target `EventType`'s own bounded context.

use super::{not_found, require_admin_mapping};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::shared::{generate_token_id, generate_token_secret};

/// Shared by all three mutations below - each differs only in which
/// `access_control::create_*_token` pure function and `db::insert_*_token`
/// persistence call it uses.
macro_rules! create_event_type_token_field {
    ($field_name:literal, $return_type:literal, $create_fn:path, $insert_fn:path) => {
        Field::new($field_name, TypeRef::named_nn($return_type), |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let event_type_name = ctx.args.try_get("eventTypeName")?.string()?.to_string();

                let event_type = skilj_core::db::get_event_type(
                    &state.pool,
                    &bounded_context_name,
                    &event_type_name,
                )
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(|| not_found("EventType", &event_type_name))?;

                let token = $create_fn(
                    &access_mapping,
                    &event_type,
                    generate_token_id(),
                    generate_token_secret(),
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                $insert_fn(&state.pool, &token)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(token)))
            })
        })
        .argument(InputValue::new(
            "boundedContext",
            TypeRef::named_nn(TypeRef::STRING),
        ))
        .argument(InputValue::new(
            "eventTypeName",
            TypeRef::named_nn(TypeRef::STRING),
        ))
    };
}

/// `createExternalEventToken(boundedContext: String!, eventTypeName: String!): ExternalEventToken!`
pub fn create_external_event_token_field() -> Field {
    create_event_type_token_field!(
        "createExternalEventToken",
        "ExternalEventToken",
        skilj_core::access_control::create_external_event_token,
        skilj_core::db::insert_external_event_token
    )
}

/// `createDirectCreationToken(boundedContext: String!, eventTypeName: String!): DirectCreationToken!`
pub fn create_direct_creation_token_field() -> Field {
    create_event_type_token_field!(
        "createDirectCreationToken",
        "DirectCreationToken",
        skilj_core::access_control::create_direct_creation_token,
        skilj_core::db::insert_direct_creation_token
    )
}

/// `createEventReadToken(boundedContext: String!, eventTypeName: String!): EventReadToken!`
pub fn create_event_read_token_field() -> Field {
    create_event_type_token_field!(
        "createEventReadToken",
        "EventReadToken",
        skilj_core::access_control::create_event_read_token,
        skilj_core::db::insert_event_read_token
    )
}
