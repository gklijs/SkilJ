//! `surface CommandQuery` - `fetchCommands`. `AdminAccess`-gated. Plain
//! `after`/`before` timestamp arguments and a flat `[String!]!` back,
//! not a Relay connection - `fetch_commands` pages by timestamp, not a
//! sequence/id cursor (`Command` carries no exposed id at all), so a
//! literal `edges{node,cursor}`/`pageInfo` envelope doesn't naturally
//! fit it the way `EventQuery`'s genuine sequence cursor does (confirmed
//! with the user - see the Phase 3 plan).

use super::{not_found, require_admin_mapping};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, InputValue, TypeRef};

fn parse_rfc3339(value: &str) -> async_graphql::Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|e| async_graphql::Error::new(format!("not a valid RFC3339 timestamp: {e}")))
}

/// `fetchCommands(boundedContext: String!, commandTypes: [String!]!, after: String, before: String, triggeredEvent: Int): [String!]!`
pub fn fetch_commands_field() -> Field {
    Field::new(
        "fetchCommands",
        TypeRef::named_nn_list_nn(TypeRef::STRING),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let mut command_types = Vec::new();
                for item in ctx.args.try_get("commandTypes")?.list()?.iter() {
                    let name = item.string()?.to_string();
                    let ct =
                        skilj_core::db::get_command_type(&state.pool, &bounded_context_name, &name)
                            .await
                            .map_err(to_graphql_error)?
                            .ok_or_else(|| not_found("CommandType", &name))?;
                    command_types.push(ct);
                }
                let after = ctx
                    .args
                    .get("after")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?
                    .map(|s| parse_rfc3339(&s))
                    .transpose()?;
                let before = ctx
                    .args
                    .get("before")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?
                    .map(|s| parse_rfc3339(&s))
                    .transpose()?;
                let triggered_event_sequence = ctx
                    .args
                    .get("triggeredEvent")
                    .filter(|v| !v.is_null())
                    .map(|v| v.i64())
                    .transpose()?;
                let triggered_event = match triggered_event_sequence {
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

                let bounded_context_commands = skilj_core::db::list_commands_for_bounded_context(
                    &state.pool,
                    &bounded_context_name,
                )
                .await
                .map_err(to_graphql_error)?;

                let rendered = skilj_core::event_store::fetch_commands(
                    &access_mapping,
                    &command_types,
                    after,
                    before,
                    triggered_event.as_ref(),
                    &bounded_context_commands,
                )
                .map_err(to_graphql_error)?;

                Ok(Some(
                    rendered
                        .into_iter()
                        .map(async_graphql::Value::from)
                        .collect::<Vec<_>>(),
                ))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "commandTypes",
        TypeRef::named_nn_list_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new("before", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new(
        "triggeredEvent",
        TypeRef::named(TypeRef::INT),
    ))
}
