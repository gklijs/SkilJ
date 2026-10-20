//! issue #98 - proposals held for human approval.
//!
//! `submitCommand` and REST `POST /v1/commands/trigger` create a pending
//! `CommandProposal` when the command type's `requires_approval()` is
//! true. This module is the human half: list pending proposals, approve
//! one (running the ordinary submission pipeline at that moment), or
//! reject it. Approval requires an active admin-level
//! `RoleAccessMapping`; a type may narrow that further with its own
//! `approval_role()`.
//!
//! GraphQL-only on purpose: every approval decision needs a Role identity
//! to audit (`decidedByRoleId`), and REST's bearer credentials are
//! capability tokens with no Role behind them. REST can therefore create
//! proposals but not decide them.

use super::{
    load_private_field_grants, not_found, require_admin_mapping, require_caller,
    resolve_read_data_keys,
};
use crate::error::to_graphql_error;
use crate::gql_types::{CommandProposalData, CommandProposalDecisionResult};
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use async_graphql::ErrorExtensions;
use chrono::Utc;
use skilj_core::access_control::RoleAccessMapping;
use skilj_core::approvals::CommandProposalStatus;
use skilj_core::event_store::Command;

fn status_from_graphql(value: &str) -> Option<CommandProposalStatus> {
    Some(match value {
        "PENDING" => CommandProposalStatus::Pending,
        "APPROVING" => CommandProposalStatus::Approving,
        "APPROVED" => CommandProposalStatus::Approved,
        "REJECTED" => CommandProposalStatus::Rejected,
        "EXPIRED" => CommandProposalStatus::Expired,
        "FORGOTTEN" => CommandProposalStatus::Forgotten,
        _ => return None,
    })
}

/// Renders a proposal's payload with the same two-grant pass
/// `fetchCommands` applies to a real command: sensitive fields decrypt
/// only where granted, private fields redact unless entitled. The
/// synthetic `Command` only exists for that renderer; its id is the
/// proposal's, so no per-record grant can accidentally match it.
async fn render_proposal(
    state: &GraphqlState,
    access_mapping: &RoleAccessMapping,
    proposal: skilj_core::approvals::CommandProposal,
) -> async_graphql::Result<CommandProposalData> {
    let command_type = skilj_core::db::get_command_type(
        &state.pool,
        &proposal.bounded_context.name,
        &proposal.command_type_name,
    )
    .await
    .map_err(to_graphql_error)?
    .ok_or_else(|| not_found("CommandType", &proposal.command_type_name))?;

    let mut data_keys = std::collections::HashMap::new();
    resolve_read_data_keys(
        &state.pool,
        &proposal.bounded_context.name,
        &command_type.sensitive_fields,
        &proposal.payload,
        access_mapping,
        state.encryption_master_key.as_ref(),
        &mut data_keys,
    )
    .await?;
    let grants = load_private_field_grants(
        &state.pool,
        &proposal.bounded_context.name,
        &access_mapping.role,
    )
    .await?;
    let resolve = |sk: &str, sv: &str| {
        data_keys
            .get(&(sk.to_string(), sv.to_string()))
            .cloned()
            .flatten()
    };
    let command = Command {
        id: proposal.id.clone(),
        bounded_context: proposal.bounded_context.clone(),
        command_type,
        payload: proposal.payload.clone(),
        metadata: proposal.metadata.clone(),
        encryption_keys: Vec::new(),
        consistency_tags: Vec::new(),
        consistency_query: Vec::new(),
        consistency_boundary: None,
    };
    let payload =
        skilj_core::event_store::render_command(&command, access_mapping, &resolve, &grants);
    Ok(CommandProposalData { proposal, payload })
}

/// `commandProposals(boundedContext: String!, commandTypeName: String, status: CommandProposalStatus, after: String): [CommandProposal!]!`
pub fn command_proposals_field(n: &Naming) -> Field {
    Field::new(
        n.root("commandProposals"),
        TypeRef::named_nn_list_nn(n.ty("CommandProposal")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let status = ctx
                    .args
                    .get("status")
                    .filter(|v| !v.is_null())
                    .map(|v| v.enum_name().map(str::to_string))
                    .transpose()?
                    .map(|value| {
                        status_from_graphql(&value)
                            .ok_or_else(|| async_graphql::Error::new("unknown proposal status"))
                    })
                    .transpose()?;
                let command_type_name = ctx
                    .args
                    .get("commandTypeName")
                    .filter(|v| !v.is_null())
                    .and_then(|v| v.string().ok())
                    .map(str::to_string);

                // Bounded like every other list read (docs/architecture.md
                // §218): at most `max_events_per_read` proposals, continued
                // with the last one's `cursor` as `after`.
                let after = match ctx.args.get("after").filter(|v| !v.is_null()) {
                    Some(value) => {
                        let raw = value.string()?;
                        Some(
                            skilj_core::db::CommandProposalCursor::decode(raw).ok_or_else(
                                || {
                                    async_graphql::Error::new(format!(
                                        "{raw:?} is not a commandProposals cursor"
                                    ))
                                    .extend_with(|_, ext| ext.set("code", "invalid_cursor"))
                                },
                            )?,
                        )
                    }
                    None => None,
                };
                let limit = i64::try_from(state.max_events_per_read.max(1)).unwrap_or(i64::MAX);

                let proposals = skilj_core::db::list_command_proposals_page(
                    &state.pool,
                    &bounded_context_name,
                    status,
                    command_type_name.as_deref(),
                    access_mapping.scope.as_deref(),
                    after.as_ref(),
                    limit,
                    Utc::now(),
                )
                .await
                .map_err(to_graphql_error)?;

                let mut rendered = Vec::with_capacity(proposals.len());
                for proposal in proposals {
                    rendered.push(render_proposal(state, &access_mapping, proposal).await?);
                }
                Ok(Some(FieldValue::list(
                    rendered.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "commandTypeName",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "status",
        TypeRef::named(n.ty("CommandProposalStatus")),
    ))
    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
}

/// `approveCommand(boundedContext: String!, proposalId: ID!): CommandProposalDecisionPayload!`
pub fn approve_command_field(n: &Naming) -> Field {
    Field::new(
        n.root("approveCommand"),
        TypeRef::named_nn(n.ty("CommandProposalDecisionPayload")),
        |ctx| {
            FieldFuture::new(async move {
                require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let proposal_id = ctx.args.try_get("proposalId")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let approval = skilj_core::db::approve_command_proposal(
                    &state.pool,
                    state.dispatcher.as_ref(),
                    state.projection_dispatcher.as_ref(),
                    state.snapshot_dispatcher.as_ref(),
                    &state.event_broadcaster,
                    &state.event_cache,
                    &bounded_context_name,
                    &proposal_id,
                    &access_mapping,
                    state.encryption_master_key.as_ref(),
                    Utc::now(),
                )
                .await
                .map_err(to_graphql_error)?;

                let (accepted, rejection_reason, rejection_kind, triggered_event_sequences) =
                    match &approval.outcome {
                        skilj_core::db::SubmitCommandOutcome::Accepted { events, .. } => (
                            true,
                            None,
                            None,
                            Some(events.iter().map(|e| e.sequence).collect()),
                        ),
                        skilj_core::db::SubmitCommandOutcome::Rejected { reason, kind, .. } => {
                            (false, Some(reason.clone()), Some(kind.clone()), None)
                        }
                        skilj_core::db::SubmitCommandOutcome::Deduplicated {
                            triggered_event_sequences,
                        } => (true, None, None, Some(triggered_event_sequences.clone())),
                        _ => {
                            return Err(to_graphql_error(skilj_core::error::Error::Unsupported {
                                what: "SubmitCommandOutcome",
                            }))
                        }
                    };
                let proposal = render_proposal(state, &access_mapping, approval.proposal).await?;
                Ok(Some(FieldValue::owned_any(CommandProposalDecisionResult {
                    proposal,
                    accepted,
                    rejection_reason,
                    rejection_kind,
                    triggered_event_sequences,
                })))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "proposalId",
        TypeRef::named_nn(TypeRef::ID),
    ))
}

/// `rejectCommand(boundedContext: String!, proposalId: ID!, reason: String): CommandProposalDecisionPayload!`
pub fn reject_command_field(n: &Naming) -> Field {
    Field::new(
        n.root("rejectCommand"),
        TypeRef::named_nn(n.ty("CommandProposalDecisionPayload")),
        |ctx| {
            FieldFuture::new(async move {
                require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let proposal_id = ctx.args.try_get("proposalId")?.string()?.to_string();
                let reason = ctx
                    .args
                    .get("reason")
                    .filter(|v| !v.is_null())
                    .and_then(|v| v.string().ok())
                    .map(str::to_string);
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let proposal = skilj_core::db::reject_command_proposal(
                    &state.pool,
                    &bounded_context_name,
                    &proposal_id,
                    &access_mapping,
                    reason.as_deref(),
                    Utc::now(),
                )
                .await
                .map_err(to_graphql_error)?;
                let proposal = render_proposal(state, &access_mapping, proposal).await?;
                Ok(Some(FieldValue::owned_any(CommandProposalDecisionResult {
                    proposal,
                    accepted: false,
                    rejection_reason: reason,
                    rejection_kind: Some("rejected_by_approver".to_string()),
                    triggered_event_sequences: None,
                })))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "proposalId",
        TypeRef::named_nn(TypeRef::ID),
    ))
    .argument(InputValue::new("reason", TypeRef::named(TypeRef::STRING)))
}
