//! `surface CommandTypeAdminOperations` - `createCommandToken`. Same
//! shape as `event_type_admin_operations`, scoped to a `CommandType`
//! instead of an `EventType`.

use super::{not_found, require_admin_mapping};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::shared::{generate_token_id, generate_token_secret};

/// `createCommandToken(boundedContext: String!, commandTypeName: String!): CommandToken!`
pub fn create_command_token_field() -> Field {
    Field::new(
        "createCommandToken",
        TypeRef::named_nn("CommandToken"),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let command_type_name = ctx.args.try_get("commandTypeName")?.string()?.to_string();

                let command_type = skilj_core::db::get_command_type(
                    &state.pool,
                    &bounded_context_name,
                    &command_type_name,
                )
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(|| not_found("CommandType", &command_type_name))?;

                let token = skilj_core::access_control::create_command_token(
                    &access_mapping,
                    &command_type,
                    generate_token_id(),
                    generate_token_secret(),
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::insert_command_token(&state.pool, &token)
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
        "commandTypeName",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}
