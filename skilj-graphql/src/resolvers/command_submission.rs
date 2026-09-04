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

/// `submitCommand(boundedContext: String!, commandTypeName: String!, payload: String!, idempotencyKey: String): SubmitCommandPayload!`
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
                // Codeberg issue #12: optional, backward compatible -
                // omitted (the existing default for every caller) means
                // skip the idempotency mechanism entirely, not "generate
                // a key anyway" - see skilj_core::db::submit_command's
                // own doc comment for why that's the right realisation
                // of "unchanged behaviour when absent".
                let idempotency_key = ctx
                    .args
                    .get("idempotencyKey")
                    .filter(|v| !v.is_null())
                    .and_then(|v| v.string().ok())
                    .map(|s| s.to_string());

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

                // The full "optimistic decide, then locked submit"
                // sequence (docs/architecture.md §19's own "Problem 1"/
                // "Problem 2" fixes), shared with skilj-rest's identical
                // branch (and the cross-context event router) via
                // skilj_core::db::decide_and_submit_command rather than
                // each duplicating the dance. `required_role` above is
                // still this resolver's own concern - checked before
                // this call, not folded into it, since REST triggering
                // never needs it (§1.3.1).
                let outcome = skilj_core::db::decide_and_submit_command(
                    &state.pool,
                    state.dispatcher.as_ref(),
                    state.projection_dispatcher.as_ref(),
                    state.snapshot_dispatcher.as_ref(),
                    &state.event_broadcaster,
                    &state.event_cache,
                    &authorised.command_type,
                    &authorised.payload,
                    &authorised.client_id,
                    state.encryption_master_key.as_ref(),
                    Utc::now(),
                    idempotency_key.as_deref(),
                )
                .await
                .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(match outcome {
                    // §5.4/§7.3: a legitimate business outcome, not a
                    // GraphQL error - whether this was the first
                    // decision or a DCB-conflict retry inside
                    // submit_command, a rejection renders identically
                    // either way.
                    skilj_core::db::SubmitCommandOutcome::Rejected {
                        reason,
                        kind,
                        matching_events,
                    } => {
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
                            deduplicated: false,
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
                            deduplicated: false,
                        }
                    }
                    // Codeberg issue #12: a cached prior answer, not a
                    // fresh decision - no live matchingEvents to show
                    // (nothing was redispatched), same as a normal
                    // Accepted response otherwise.
                    skilj_core::db::SubmitCommandOutcome::Deduplicated {
                        triggered_event_sequences,
                    } => SubmitCommandResult {
                        accepted: true,
                        triggered_event_sequences: Some(triggered_event_sequences),
                        rejection_reason: None,
                        rejection_kind: None,
                        matching_events: None,
                        deduplicated: true,
                    },
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
    .argument(InputValue::new(
        "idempotencyKey",
        TypeRef::named(TypeRef::STRING),
    ))
}
