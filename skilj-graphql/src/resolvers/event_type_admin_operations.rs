//! `surface EventTypeAdminOperations` - `createExternalEventToken`,
//! `createDirectCreationToken`, `createEventReadToken`. `AdminAccess`-
//! gated on the target `EventType`'s own bounded context.

use super::{create_type_token_field, not_found, parse_event_read_start_position, parse_rfc3339};
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
/// `createEventReadToken(boundedContext: String!, eventTypeName: String!, scope: String, startFrom: EventReadStartPosition, startAtSequence: Int, startAtTime: String): EventReadToken!` -
/// hand-written again rather than the `create_type_token_field!` macro
/// invocation this used to be (docs/architecture.md's own write-up of
/// this pass): `start_from` is the one parameter `create_event_read_token`
/// doesn't share with its three siblings, so it no longer fits that
/// macro's "every `create_*_token` function takes an identical parameter
/// list" premise. Same body the macro would have generated, plus the
/// extra arguments - see `create_type_token_field!`'s own doc comment
/// for the parts that didn't change.
///
/// `startAtSequence`/`startAtTime` (docs/architecture.md's own write-up
/// of this later pass) - `Int`/`String` the same way `queryEvents`'s own
/// `afterSequence`/`fetchCommands`'s own `after`/`before` already are
/// (`.i64()`/`parse_rfc3339` - no custom scalar exists in this schema
/// for either). Passed straight through to `create_event_read_token`
/// unvalidated at this layer; the three-way exclusivity with `startFrom`
/// is `create_event_read_token`'s own guard, rendered here through the
/// same `to_graphql_error` every other rejection already goes through.
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
                    super::require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let type_name = ctx.args.try_get("eventTypeName")?.string()?.to_string();
                let scope = ctx
                    .args
                    .get("scope")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?;
                let start_from = match ctx.args.get("startFrom").filter(|v| !v.is_null()) {
                    Some(v) => Some(parse_event_read_start_position(v.enum_name()?)),
                    None => None,
                };
                let start_at_sequence = ctx
                    .args
                    .get("startAtSequence")
                    .filter(|v| !v.is_null())
                    .map(|v| v.i64())
                    .transpose()?;
                let start_at_time = ctx
                    .args
                    .get("startAtTime")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?
                    .map(|s| parse_rfc3339(&s))
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
                    start_from,
                    start_at_sequence,
                    start_at_time,
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
    .argument(InputValue::new(
        "startFrom",
        TypeRef::named("EventReadStartPosition"),
    ))
    .argument(InputValue::new(
        "startAtSequence",
        TypeRef::named(TypeRef::INT),
    ))
    .argument(InputValue::new(
        "startAtTime",
        TypeRef::named(TypeRef::STRING),
    ))
}
