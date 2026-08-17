//! `surface CommandSubmission` - `submitCommand`, the GraphQL/Role
//! counterpart to `CommandTrigger`'s REST/`CommandToken` path (both
//! converge on `process_command`). Mirrors `skilj-rest`'s own
//! `post_commands_trigger` handler almost exactly
//! (`skilj-rest/src/routes/mod.rs`) - same pre-resolve-`EventType`s/
//! pre-allocate-`sequence`s-before-calling-`process_command` pattern,
//! since `process_command`'s own closures stay synchronous by design
//! (§1.1: `decide()` and everything downstream of it is I/O-free).
//!
//! `WriteAccess`-gated, not `AdminAccess`: unlike every other Phase 2/3
//! resolver, the mapping lookup here has **no level filter** -
//! `authorise_command_submission` itself checks `Write | Admin` and
//! returns its own precise `InsufficientAccessLevel` for a read-only
//! grant, so collapsing that into `require_admin_mapping`'s coarser
//! `GrantNotActive` first would hide a real, more specific rejection.
//!
//! This is also where `CommandDispatcher::required_role` (§1.3.1) - the
//! `#[requires_role(...)]` proc-macro gate, built with no caller two
//! sessions ago - finally gets one: checked *before* `dispatch`, so an
//! unauthorised caller never reaches `decide()`.
//!
//! `ProjectionQuery`/`EventSubscription` stay out of this phase (and
//! this crate entirely, for now) - see the Phase 3 plan
//! (`/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`) for why:
//! `ProjectionQuery` needs `project()` (still `todo!()` - no projection
//! state exists anywhere to query), `EventSubscription` needs a
//! real-time event-delivery mechanism (none exists yet). Neither is a
//! GraphQL-plumbing gap this pass can close.

use super::{not_found, require_caller};
use crate::error::to_graphql_error;
use crate::gql_types::SubmitCommandResult;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use async_graphql::ErrorExtensions;
use chrono::Utc;
use skilj_core::event_store::EventType;
use skilj_core::shared::CommandDecision;
use std::collections::HashMap;

fn insufficient_role_error(required: &str) -> async_graphql::Error {
    async_graphql::Error::new(format!(
        "this command type requires the caller's Role.name to be {required:?}"
    ))
    .extend_with(|_, ext| ext.set("code", "insufficient_role"))
}

fn no_decider_registered_error() -> async_graphql::Error {
    async_graphql::Error::new("this CommandType has no decide() registered in the running process")
        .extend_with(|_, ext| ext.set("code", "no_decider_registered"))
}

/// `submitCommand(boundedContext: String!, commandTypeName: String!, payload: String!): SubmitCommandPayload!`
pub fn submit_command_field() -> Field {
    Field::new(
        "submitCommand",
        TypeRef::named_nn("SubmitCommandPayload"),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let command_type_name = ctx.args.try_get("commandTypeName")?.string()?.to_string();
                let payload = ctx.args.try_get("payload")?.string()?.to_string();

                // WriteAccess: any active mapping, any level -
                // authorise_command_submission itself enforces
                // Write|Admin and returns its own precise rejection for
                // a read-only one (see this module's own doc comment).
                let access_mapping = skilj_core::db::get_active_role_access_mapping(
                    &state.pool,
                    &caller.id,
                    &bounded_context_name,
                )
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(|| {
                    to_graphql_error(skilj_core::access_control::Error::GrantNotActive)
                })?;

                let command_type = skilj_core::db::get_command_type(
                    &state.pool,
                    &bounded_context_name,
                    &command_type_name,
                )
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(|| not_found("CommandType", &command_type_name))?;

                let authorised = skilj_core::event_store::authorise_command_submission(
                    &access_mapping,
                    &command_type,
                    payload,
                )
                .map_err(to_graphql_error)?;

                // §1.3.1: checked before dispatch, so an unauthorised
                // caller never reaches decide().
                match state
                    .dispatcher
                    .required_role(&bounded_context_name, &command_type_name)
                {
                    None => return Err(no_decider_registered_error()),
                    Some(Some(required)) if required != caller.name => {
                        return Err(insufficient_role_error(required))
                    }
                    Some(_) => {}
                }

                let bounded_context_events = skilj_core::db::list_events_for_bounded_context(
                    &state.pool,
                    &bounded_context_name,
                )
                .await
                .map_err(to_graphql_error)?;
                let consistency_tags = skilj_core::event_store::derive_tags(
                    &authorised.command_type.tag_mappings,
                    &authorised.payload,
                );
                let (_boundary, matching_events) =
                    skilj_core::event_store::consistency_boundary_and_matching_events(
                        &bounded_context_events,
                        &consistency_tags,
                    );

                let decision = match state.dispatcher.dispatch(
                    &bounded_context_name,
                    &authorised.command_type.name,
                    &authorised.payload,
                    &matching_events,
                ) {
                    None => return Err(no_decider_registered_error()),
                    Some(Err(e)) => return Err(to_graphql_error(e)),
                    Some(Ok(decision)) => decision,
                };

                let event_specs = match decision {
                    CommandDecision::Rejected { reason, kind } => {
                        // §5.4/§7.3: a legitimate business outcome, not
                        // a GraphQL error - process_command itself is
                        // never called on this branch.
                        return Ok(Some(FieldValue::owned_any(SubmitCommandResult {
                            accepted: false,
                            triggered_event_sequences: None,
                            rejection_reason: Some(reason),
                            rejection_kind: Some(kind),
                        })));
                    }
                    CommandDecision::Accepted { events } => events,
                };

                // process_command's own resolve_event_type/next_sequence
                // are plain sync closures (decide() and everything
                // downstream stays I/O-free per §1.1) - every EventType
                // lookup and sequence allocation happens first, here.
                let mut event_types_by_name: HashMap<String, EventType> = HashMap::new();
                for spec in &event_specs {
                    if let std::collections::hash_map::Entry::Vacant(entry) =
                        event_types_by_name.entry(spec.event_type.clone())
                    {
                        if let Some(et) = skilj_core::db::get_event_type(
                            &state.pool,
                            &bounded_context_name,
                            &spec.event_type,
                        )
                        .await
                        .map_err(to_graphql_error)?
                        {
                            entry.insert(et);
                        }
                    }
                }
                let mut sequences = Vec::with_capacity(event_specs.len());
                for _ in 0..event_specs.len() {
                    sequences.push(
                        skilj_core::db::next_sequence(&state.pool, &bounded_context_name)
                            .await
                            .map_err(to_graphql_error)?,
                    );
                }
                let mut sequences = sequences.into_iter();

                let result: skilj_core::event_store::ProcessCommandResult =
                    skilj_core::event_store::process_command(
                        &authorised.command_type,
                        &authorised.payload,
                        &authorised.client_id,
                        &bounded_context_events,
                        CommandDecision::Accepted {
                            events: event_specs,
                        },
                        |name| event_types_by_name.get(name).cloned(),
                        || {
                            sequences.next().expect(
                                "process_command called next_sequence more times than there are \
                                 accepted events",
                            )
                        },
                        Utc::now(),
                    )
                    .map_err(to_graphql_error)?;

                let command_id = skilj_core::db::insert_command(&state.pool, &result.command)
                    .await
                    .map_err(to_graphql_error)?;
                let mut triggered_event_sequences = Vec::with_capacity(result.events.len());
                for event in &result.events {
                    skilj_core::db::insert_event_and_update_sync_projections(
                        &state.pool,
                        event,
                        Some(command_id),
                        state.projection_dispatcher.as_ref(),
                    )
                    .await
                    .map_err(to_graphql_error)?;
                    triggered_event_sequences.push(event.sequence);
                }

                Ok(Some(FieldValue::owned_any(SubmitCommandResult {
                    accepted: true,
                    triggered_event_sequences: Some(triggered_event_sequences),
                    rejection_reason: None,
                    rejection_kind: None,
                })))
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
    .argument(InputValue::new(
        "payload",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}
