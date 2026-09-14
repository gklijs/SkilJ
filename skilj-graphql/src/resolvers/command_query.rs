//! `surface CommandQuery` - `fetchCommands`. `AdminAccess`-gated. Plain
//! `after`/`before` timestamp arguments and a flat `[String!]!` back,
//! not a Relay connection - `fetch_commands` pages by timestamp, not a
//! sequence/id cursor: `Command` gained a real `id` (drift audit finding
//! #12, 2026-08-20, see project memory `skilj-drift-audit-2026-08-20`),
//! but it's never surfaced on this wire - added purely to make
//! `FetchCommands`'s own `triggered_event` lookup an identity comparison
//! rather than a structural one, not as a client-facing cursor - so a
//! literal `edges{node,cursor}`/`pageInfo` envelope still doesn't
//! naturally fit this surface the way `EventQuery`'s genuine sequence
//! cursor does (confirmed with the user - see the Phase 3 plan).

use super::{not_found, parse_rfc3339, require_admin_mapping, resolve_read_data_keys};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, InputValue, TypeRef};

/// `fetchCommands(boundedContext: String!, commandTypes: [String!]!, after: String, before: String, triggeredEvent: Int, correlationId: String): [String!]!`
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
                // Codeberg issue #18 - "show me everything in this
                // transaction".
                let correlation_id = ctx
                    .args
                    .get("correlationId")
                    .filter(|v| !v.is_null())
                    .and_then(|v| v.string().ok())
                    .map(|s| s.to_string());

                let bounded_context_commands = skilj_core::db::list_commands_for_bounded_context(
                    &state.pool,
                    &bounded_context_name,
                )
                .await
                .map_err(to_graphql_error)?;

                // Same scoped-to-commandTypes pre-resolution `queryEvents`
                // uses - see that resolver's own comment for why this
                // doesn't try to replicate fetch_commands' full filter.
                let mut data_keys = std::collections::HashMap::new();
                for c in bounded_context_commands.iter().filter(|c| {
                    c.bounded_context == access_mapping.bounded_context
                        && (command_types.is_empty() || command_types.contains(&c.command_type))
                }) {
                    resolve_read_data_keys(
                        &state.pool,
                        &bounded_context_name,
                        &c.command_type.sensitive_fields,
                        &c.payload,
                        &access_mapping,
                        state.encryption_master_key.as_ref(),
                        &mut data_keys,
                    )
                    .await?;
                }

                let private_field_grants =
                    super::load_private_field_grants(&state.pool, &bounded_context_name).await?;
                let rendered = skilj_core::event_store::fetch_commands(
                    &access_mapping,
                    &command_types,
                    after,
                    before,
                    triggered_event.as_ref(),
                    correlation_id.as_deref(),
                    &bounded_context_commands,
                    |sk, sv| data_keys.get(&(sk.to_string(), sv.to_string())).cloned(),
                    &private_field_grants,
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
    .argument(InputValue::new(
        "correlationId",
        TypeRef::named(TypeRef::STRING),
    ))
}
