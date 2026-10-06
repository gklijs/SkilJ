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
use crate::gql_types::{DryRunCommandResult, SubmitCommandResult};
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use async_graphql::ErrorExtensions;
use chrono::Utc;
use skilj_core::access_control::RoleAccessMapping;

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

/// `submitCommand(boundedContext: String!, commandTypeName: String!, payload: String!, idempotencyKey: String, correlationId: String, causationId: String): SubmitCommandPayload!`
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
                // A security-review finding on CrossContextRoute
                // (docs/architecture.md §36): its own background task's
                // internally-derived idempotency keys share this same
                // table's namespace, unscoped by caller - reject a
                // caller-supplied key that impersonates one before it
                // ever reaches the shared idempotency lookup, rather
                // than letting an ordinary Write-level caller pre-plant
                // one and silently swallow a real route delivery. See
                // `reject_reserved_idempotency_key`'s own doc comment.
                skilj_core::event_store::reject_reserved_idempotency_key(
                    idempotency_key.as_deref(),
                )
                .map_err(to_graphql_error)?;
                // Codeberg issue #18 - both optional, same "absent" shape
                // as idempotencyKey above, but a different meaning: an
                // absent correlationId is generated server-side, never
                // skipped (see valid_correlation_id's own doc comment).
                let correlation_id = ctx
                    .args
                    .get("correlationId")
                    .filter(|v| !v.is_null())
                    .and_then(|v| v.string().ok())
                    .map(|s| s.to_string());
                let causation_id = ctx
                    .args
                    .get("causationId")
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
                    correlation_id,
                    causation_id,
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
                // never needs it (§1.3.1). Routed through
                // `state.command_batcher` rather than calling that
                // function directly (Codeberg issue #32, round two) -
                // this resolver's own real-world concurrent traffic is
                // exactly what `CommandBatcher` exists to coalesce into
                // fewer bounded-context lock acquisitions; see its own
                // module doc comment.
                let outcome = state
                    .command_batcher
                    .decide_and_submit(
                        &state.pool,
                        &state.dispatcher,
                        &state.projection_dispatcher,
                        state.snapshot_dispatcher.as_ref(),
                        &state.event_broadcaster,
                        &state.event_cache,
                        &authorised.command_type,
                        &authorised.payload,
                        &authorised.client_id,
                        authorised.correlation_id.as_deref(),
                        authorised.causation_id.as_deref(),
                        state.encryption_master_key.as_ref(),
                        Utc::now(),
                        idempotency_key.as_deref(),
                    )
                    .await
                    .map_err(to_graphql_error)?;

                // Codeberg issue #7's DCB conflict visualizer, for an
                // Admin-level caller only: this WriteAccess-gated resolver
                // lets any Write-level caller submit commands, but the
                // matching events are event content - what queryEvents/
                // countEvents/inspectEvent require Admin for. Without the
                // gate a Write-only caller could force a rejection whose
                // tags scope any account and read its history back here
                // (`MatchingEventsRequiresAdminLevel`). And served the way
                // queryEvents serves them (docs/architecture.md §118):
                // only within the caller's owner scope, at most
                // `max_events_per_read` (the latest), each rendered -
                // sensitive fields decrypted only where granted, private
                // fields redacted unless entitled. They used to be
                // returned raw, whole and unscoped.
                let (matching_events, matching_events_truncated) = match &outcome {
                    skilj_core::db::SubmitCommandOutcome::Rejected {
                        matching_events, ..
                    } if access_mapping.level == skilj_core::access_control::AccessLevel::Admin => {
                        let (rendered, truncated) =
                            render_matching_events(state, &access_mapping, matching_events).await?;
                        (Some(rendered), Some(truncated))
                    }
                    _ => (None, None),
                };

                Ok(Some(FieldValue::owned_any(match outcome {
                    // §5.4/§7.3: a legitimate business outcome, not a
                    // GraphQL error - whether this was the first
                    // decision or a DCB-conflict retry inside
                    // submit_command, a rejection renders identically
                    // either way.
                    skilj_core::db::SubmitCommandOutcome::Rejected { reason, kind, .. } => {
                        SubmitCommandResult {
                            accepted: false,
                            triggered_event_sequences: None,
                            rejection_reason: Some(reason),
                            rejection_kind: Some(kind),
                            // Some even when empty (a rejection whose
                            // decider didn't reject *because* of a
                            // conflict still had a real, if empty, set to
                            // decide from) - None is reserved for
                            // "accepted, not applicable" below, and for a
                            // caller below Admin (see above).
                            matching_events,
                            matching_events_truncated,
                            deduplicated: false,
                            correlation_id: None,
                        }
                    }
                    skilj_core::db::SubmitCommandOutcome::Accepted { command, events } => {
                        SubmitCommandResult {
                            accepted: true,
                            triggered_event_sequences: Some(
                                events.iter().map(|e| e.sequence).collect(),
                            ),
                            rejection_reason: None,
                            rejection_kind: None,
                            matching_events: None,
                            matching_events_truncated: None,
                            deduplicated: false,
                            correlation_id: command.metadata.correlation_id,
                        }
                    }
                    // Codeberg issue #12: a cached prior answer, not a
                    // fresh decision - no live matchingEvents to show
                    // (nothing was redispatched), same as a normal
                    // Accepted response otherwise. No correlationId
                    // either, for the same reason (Codeberg issue #18).
                    skilj_core::db::SubmitCommandOutcome::Deduplicated {
                        triggered_event_sequences,
                    } => SubmitCommandResult {
                        accepted: true,
                        triggered_event_sequences: Some(triggered_event_sequences),
                        rejection_reason: None,
                        rejection_kind: None,
                        matching_events: None,
                        matching_events_truncated: None,
                        deduplicated: true,
                        correlation_id: None,
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
    .argument(InputValue::new(
        "correlationId",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "causationId",
        TypeRef::named(TypeRef::STRING),
    ))
}

/// A command's matching events as an admin-level caller is shown them,
/// on a `submitCommand` rejection and on every `dryRunCommand`: served the
/// way queryEvents serves them (docs/architecture.md §118) - only within
/// the caller's owner scope, at most `max_events_per_read` (the latest),
/// each rendered: sensitive fields decrypted only where granted, private
/// fields redacted unless entitled. `true` alongside when events within
/// scope were left out for the cap.
async fn render_matching_events(
    state: &GraphqlState,
    access_mapping: &RoleAccessMapping,
    matching_events: &[skilj_core::event_store::Event],
) -> async_graphql::Result<(Vec<skilj_core::event_store::Event>, bool)> {
    let bounded_context_name = &access_mapping.bounded_context.name;
    let (visible, truncated) = skilj_core::event_store::visible_matching_events(
        access_mapping,
        matching_events,
        state.max_events_per_read,
    );
    let mut data_keys = std::collections::HashMap::new();
    for e in &visible {
        super::resolve_read_data_keys(
            &state.pool,
            bounded_context_name,
            &e.event_type.sensitive_fields,
            &e.payload,
            access_mapping,
            state.encryption_master_key.as_ref(),
            &mut data_keys,
        )
        .await?;
    }
    let grants =
        super::load_private_field_grants(&state.pool, bounded_context_name, &access_mapping.role)
            .await?;
    let resolve = |sk: &str, sv: &str| {
        data_keys
            .get(&(sk.to_string(), sv.to_string()))
            .cloned()
            .flatten()
    };
    let rendered = visible
        .into_iter()
        .map(|e| skilj_core::event_store::Event {
            payload: skilj_core::event_store::render_event(&e, access_mapping, &resolve, &grants),
            ..e
        })
        .collect();
    Ok((rendered, truncated))
}

/// `dryRunCommand(boundedContext: String!, commandTypeName: String!, payload: String!): DryRunCommandPayload!`,
/// `rule DryRunCommand` (Codeberg issue #47). What `submitCommand` would
/// decide for this payload right now, with nothing persisted: no command,
/// no event, no sequence, no encryption key, no idempotency record (see
/// `skilj_core::db::dry_run_command`). A query, not a mutation, for that
/// reason.
///
/// Admin level, not the write level `submitCommand` needs: the would-be
/// events `decide()` emits can carry the history it read, and the
/// matching events are event content (`MatchingEventsRequiresAdminLevel`).
/// The same `#[requires_role]` check as `submitCommand` applies, before
/// `decide()` runs.
pub fn dry_run_command_field() -> Field {
    Field::new(
        "dryRunCommand",
        TypeRef::named_nn("DryRunCommandPayload"),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let command_type_name = ctx.args.try_get("commandTypeName")?.string()?.to_string();
                let payload = ctx.args.try_get("payload")?.string()?.to_string();

                // Any active mapping, any level, as for submitCommand:
                // authorise_command_dry_run returns its own precise
                // InsufficientAccessLevel for one below admin.
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

                let authorised = skilj_core::event_store::authorise_command_dry_run(
                    &access_mapping,
                    &command_type,
                    payload,
                )
                .map_err(to_graphql_error)?;

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

                let dry_run = skilj_core::db::dry_run_command(
                    &state.pool,
                    state.dispatcher.as_ref(),
                    state.snapshot_dispatcher.as_ref(),
                    &state.event_cache,
                    &authorised.command_type,
                    &authorised.payload,
                    &authorised.client_id,
                    state.encryption_master_key.as_ref(),
                )
                .await
                .map_err(to_graphql_error)?;

                let (matching_events, matching_events_truncated) =
                    render_matching_events(state, &access_mapping, &dry_run.matching_events)
                        .await?;
                let result = match dry_run.decision {
                    skilj_core::db::DryRunDecision::Accepted { events } => {
                        let grants = super::load_private_field_grants(
                            &state.pool,
                            &bounded_context_name,
                            &access_mapping.role,
                        )
                        .await?;
                        DryRunCommandResult {
                            accepted: true,
                            rejection_reason: None,
                            rejection_kind: None,
                            events: events
                                .into_iter()
                                .map(|e| skilj_core::event_store::WouldBeEvent {
                                    payload: skilj_core::event_store::render_would_be_event(
                                        &e,
                                        &access_mapping,
                                        &grants,
                                    ),
                                    ..e
                                })
                                .collect(),
                            matching_events,
                            matching_events_truncated,
                        }
                    }
                    skilj_core::db::DryRunDecision::Rejected { reason, kind } => {
                        DryRunCommandResult {
                            accepted: false,
                            rejection_reason: Some(reason),
                            rejection_kind: Some(kind),
                            events: Vec::new(),
                            matching_events,
                            matching_events_truncated,
                        }
                    }
                };
                Ok(Some(FieldValue::owned_any(result)))
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
