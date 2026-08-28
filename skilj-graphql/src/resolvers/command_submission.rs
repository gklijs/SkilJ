//! `surface CommandSubmission` - `submitCommand`, the GraphQL/Role
//! counterpart to `CommandTrigger`'s REST/`CommandToken` path. Both
//! resolve their own authorisation and run `dispatch`'s first,
//! optimistic call here, then converge on the identical shared
//! `skilj_core::db::submit_command` (see its own doc comment) for
//! everything from there on: the DCB conflict re-check under
//! `next_sequence`'s own lock, the possible redispatch, and the atomic
//! `process_command` + insert. Mirrors `skilj-rest`'s own
//! `post_commands_trigger` handler almost exactly
//! (`skilj-rest/src/routes/mod.rs`) up to that point.
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
//! `ProjectionQuery`/`EventSubscription` stayed out of this particular
//! pass at the time this module was first written - both are real now,
//! in `resolvers::projection_query`/`resolvers::event_subscription`
//! respectively.

use super::{not_found, require_caller};
use crate::error::to_graphql_error;
use crate::gql_types::SubmitCommandResult;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use async_graphql::ErrorExtensions;
use chrono::Utc;

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

                // docs/architecture.md §19's "Problem 1" fix: derive_tags
                // runs first so the fetch below can go straight to the
                // tag-indexed query instead of pulling the whole bounded
                // context and filtering in memory - `bounded_context_events`
                // is already tag-scoped from here on, not literally every
                // event in the bounded context.
                let consistency_tags = skilj_core::event_store::derive_tags(
                    &authorised.command_type.tag_mappings,
                    &authorised.payload,
                );

                // docs/architecture.md §19's "Problem 2" - resolved
                // once, shared with skilj-rest's own identical branch
                // via skilj_core::db::resolve_snapshot_context. `None`
                // either when this command type doesn't opt into
                // snapshotting at all, or opts in but its own derived
                // tags don't match the snapshot's single tag key
                // exactly (a silent fallback to the ordinary path, not
                // an error - see CommandType::snapshot()'s own doc
                // comment).
                let snapshot_context = match state
                    .dispatcher
                    .snapshot_name(&bounded_context_name, &authorised.command_type.name)
                {
                    Some(Some(snapshot_name)) => skilj_core::db::resolve_snapshot_context(
                        &state.pool,
                        &bounded_context_name,
                        state.snapshot_dispatcher.as_ref(),
                        snapshot_name,
                        &consistency_tags,
                    )
                    .await
                    .map_err(to_graphql_error)?,
                    _ => None,
                };

                // The tag-indexed fetch (§19's "Problem 1") already
                // supports an `after_sequence` bound for exactly this
                // reason - `events_since_snapshot` when a snapshot
                // context resolved, the full tag-scoped set otherwise.
                // `matching_events` names it either way, since it's the
                // one thing both `dispatch`/`dispatch_from_snapshot`
                // below are fed.
                let bounded_context_events =
                    skilj_core::db::list_events_for_bounded_context_matching_tags_cached(
                        &state.pool,
                        &state.event_cache,
                        &bounded_context_name,
                        &consistency_tags,
                        snapshot_context.as_ref().map(|ctx| ctx.as_of_sequence),
                    )
                    .await
                    .map_err(to_graphql_error)?;
                let (_boundary, matching_events) =
                    skilj_core::event_store::consistency_boundary_and_matching_events(
                        &bounded_context_events,
                        &consistency_tags,
                    );

                let decision = match &snapshot_context {
                    Some(ctx) => match state.dispatcher.dispatch_from_snapshot(
                        &bounded_context_name,
                        &authorised.command_type.name,
                        &authorised.payload,
                        &ctx.state_json,
                        &matching_events,
                    ) {
                        None => return Err(no_decider_registered_error()),
                        Some(Err(e)) => return Err(to_graphql_error(e)),
                        Some(Ok(decision)) => decision,
                    },
                    None => match state.dispatcher.dispatch(
                        &bounded_context_name,
                        &authorised.command_type.name,
                        &authorised.payload,
                        &matching_events,
                    ) {
                        None => return Err(no_decider_registered_error()),
                        Some(Err(e)) => return Err(to_graphql_error(e)),
                        Some(Ok(decision)) => decision,
                    },
                };

                // The optimistic, unlocked half ends here - `decision`
                // above is dispatch()'s own first call. skilj_core::db::
                // submit_command below re-checks this under
                // next_sequence's own lock and redispatches if a DCB
                // conflict actually happened in between (see its own doc
                // comment) before persisting anything.
                let outcome = skilj_core::db::submit_command(
                    &state.pool,
                    state.dispatcher.as_ref(),
                    state.projection_dispatcher.as_ref(),
                    &state.event_broadcaster,
                    &state.event_cache,
                    &authorised.command_type,
                    &authorised.payload,
                    &authorised.client_id,
                    &bounded_context_events,
                    &consistency_tags,
                    &matching_events,
                    decision,
                    state.encryption_master_key.as_ref(),
                    Utc::now(),
                    snapshot_context
                        .as_ref()
                        .map(|ctx| skilj_core::db::SnapshotContext {
                            state_json: &ctx.state_json,
                            as_of_sequence: ctx.as_of_sequence,
                        }),
                )
                .await
                .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(match outcome {
                    // §5.4/§7.3: a legitimate business outcome, not a
                    // GraphQL error - whether this was the first
                    // decision or a DCB-conflict retry inside
                    // submit_command, a rejection renders identically
                    // either way.
                    skilj_core::db::SubmitCommandOutcome::Rejected { reason, kind, matching_events } => {
                        SubmitCommandResult {
                            accepted: false,
                            triggered_event_sequences: None,
                            rejection_reason: Some(reason),
                            rejection_kind: Some(kind),
                            // Codeberg issue #7's DCB conflict visualizer -
                            // Some even when empty (a rejection whose
                            // decider didn't reject *because* of a
                            // conflict still had a real, if empty, set to
                            // decide from) - None is reserved for
                            // "accepted, not applicable" below. Gated on
                            // Admin: this WriteAccess-gated resolver lets
                            // any Write-level caller submit commands, but
                            // matching_events is full raw event content -
                            // the same thing queryEvents/countEvents/
                            // inspectEvent require Admin for. Without this
                            // gate a Write-only caller could construct a
                            // command whose tags scope any account they
                            // like, force a rejection, and read that
                            // account's whole matching-event history back
                            // through this field - a read side channel
                            // around the Admin-only query surface.
                            matching_events: (access_mapping.level
                                == skilj_core::access_control::AccessLevel::Admin)
                                .then_some(matching_events),
                        }
                    }
                    skilj_core::db::SubmitCommandOutcome::Accepted { events, .. } => {
                        SubmitCommandResult {
                            accepted: true,
                            triggered_event_sequences: Some(
                                events.iter().map(|e| e.sequence).collect(),
                            ),
                            rejection_reason: None,
                            rejection_kind: None,
                            matching_events: None,
                        }
                    }
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
