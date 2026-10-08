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

use super::{
    distinct_names, not_found, parse_rfc3339, require_admin_mapping, resolve_read_data_keys,
};
use crate::error::to_graphql_error;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};

/// `fetchCommands(boundedContext: String!, commandTypes: [String!]!, after: String, before: String, triggeredEvent: Int, correlationId: String): [String!]!`
pub fn fetch_commands_field(n: &Naming) -> Field {
    Field::new(
        n.root("fetchCommands"),
        TypeRef::named_nn_list_nn(n.ty("QueriedCommand")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let mut command_types = Vec::new();
                for name in distinct_names(&ctx.args.try_get("commandTypes")?)? {
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

                // Where the previous page stopped (rule `FetchCommands`'
                // `after_command`), resolved to its recording position.
                let after_command_id = ctx
                    .args
                    .get("afterCommandId")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?;
                let (after_command, after_position) = match &after_command_id {
                    Some(id) => {
                        let command = skilj_core::db::get_command_by_external_id(
                            &state.pool,
                            &bounded_context_name,
                            id,
                        )
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("Command", id))?;
                        let position = skilj_core::db::command_recording_position(
                            &state.pool,
                            &bounded_context_name,
                            id,
                        )
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("Command", id))?;
                        (Some(command), position)
                    }
                    None => (None, -1),
                };

                // At most `max_events_per_read` commands, in recording
                // order, loaded a chunk at a time.
                let page = skilj_core::db::collect_command_page(
                    &state.pool,
                    &bounded_context_name,
                    after_position,
                    state.max_events_per_read,
                    |chunk, remaining| {
                        skilj_core::event_store::fetch_commands_select(
                            &access_mapping,
                            &command_types,
                            after,
                            before,
                            triggered_event.as_ref(),
                            correlation_id.as_deref(),
                            after_command.as_ref(),
                            chunk,
                            remaining,
                        )
                    },
                )
                .await
                .map_err(to_graphql_error)?;

                // Decryption keys for the served commands only.
                let mut data_keys = std::collections::HashMap::new();
                for c in &page {
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

                let private_field_grants = super::load_private_field_grants(
                    &state.pool,
                    &bounded_context_name,
                    &access_mapping.role,
                )
                .await?;
                Ok(Some(FieldValue::list(page.iter().map(|c| {
                    FieldValue::owned_any((
                        c.id.clone(),
                        c.metadata.created_at.to_rfc3339(),
                        skilj_core::event_store::render_command(
                            c,
                            &access_mapping,
                            &|sk, sv| {
                                data_keys
                                    .get(&(sk.to_string(), sv.to_string()))
                                    .cloned()
                                    .flatten()
                            },
                            &private_field_grants,
                        ),
                    ))
                }))))
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
    .argument(InputValue::new(
        "afterCommandId",
        TypeRef::named(TypeRef::STRING),
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
