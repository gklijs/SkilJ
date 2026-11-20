//! Persistence for approvals rule groups.

use super::concurrent_indexes::OPEN_PROPOSAL_KEY_PREDICATE;
use super::*;
use crate::event_store::CommandAuthorised;
use crate::payload_limits::PayloadLimits;
use std::collections::HashMap;

// --- Command approvals (issue #98) ---

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_command_approval_policy(
    pool: &Pool,
    bounded_context: &str,
    command_type_name: &str,
) -> crate::error::Result<Option<CommandApprovalPolicy>> {
    let schema = schema_ident(bounded_context);
    let row: Option<(Option<String>, Option<i64>, bool)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT approver_role, deadline_seconds, allow_self_approval \
             FROM {schema}.command_approval_policies WHERE command_type_name = $1"
        )))
        .bind(command_type_name)
        .fetch_optional(pool)
        .await?;
    Ok(
        row.map(|(approver_role, deadline_seconds, allow_self_approval)| {
            CommandApprovalPolicy::new(approver_role, deadline_seconds)
                .allowing_self_approval(allow_self_approval)
        }),
    )
}

/// Writes `policy` for `command_type_name`. `None` removes the row, so the
/// type no longer requires approval - an explicit admin decision
/// (`registerCommandType` with `requiresApproval: false`), not something
/// startup reconciliation does: a deploy that stops declaring
/// `requires_approval()` leaves the stored policy in place, the same
/// "startup never removes a protection" stance as §102.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn set_command_approval_policy(
    pool: &Pool,
    bounded_context: &str,
    command_type_name: &str,
    policy: Option<&CommandApprovalPolicy>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    match policy {
        Some(policy) => {
            if policy.deadline_seconds.is_some_and(|seconds| seconds <= 0) {
                return Err(crate::approvals::Error::InvalidApprovalPolicy.into());
            }
            if crate::db::get_command_type(pool, bounded_context, command_type_name)
                .await?
                .is_none()
            {
                return Err(crate::error::Error::row_not_found());
            }
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {schema}.command_approval_policies \
                 (command_type_name, approver_role, deadline_seconds, allow_self_approval) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (command_type_name) DO UPDATE SET \
                    approver_role = EXCLUDED.approver_role, \
                    deadline_seconds = EXCLUDED.deadline_seconds, \
                    allow_self_approval = EXCLUDED.allow_self_approval"
            )))
            .bind(command_type_name)
            .bind(policy.approver_role.as_deref())
            .bind(policy.deadline_seconds)
            .bind(policy.allow_self_approval)
            .execute(pool)
            .await?;
        }
        None => {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {schema}.command_approval_policies WHERE command_type_name = $1"
            )))
            .bind(command_type_name)
            .execute(pool)
            .await?;
        }
    }
    Ok(())
}

/// What an already authorised submission resolves to at the approval gate.
#[non_exhaustive]
pub enum ApprovalSubmission {
    /// Continue with ordinary command submission.
    Ordinary,
    /// The ID of a new or existing open proposal.
    Proposal(String),
    /// The original acceptance, read from the same snapshot as the routing decision.
    Deduplicated(Vec<i64>),
}

#[derive(Default)]
struct ApprovalKey {
    proposal_id: Option<String>,
    sequences: Option<Vec<i64>>,
}

/// The approval policy and matching keys from one statement snapshot.
pub struct ApprovalRouting {
    policy: Option<CommandApprovalPolicy>,
    keys: HashMap<String, ApprovalKey>,
}

impl ApprovalRouting {
    /// Whether routing this item may write a proposal before returning.
    pub fn may_write_proposal(&self, key: Option<&str>) -> bool {
        self.policy.is_some()
            && key
                .and_then(|key| self.keys.get(key))
                .is_none_or(|hit| hit.proposal_id.is_none() && hit.sequences.is_none())
    }

    fn resolved(&self, key: Option<&str>) -> Option<ApprovalSubmission> {
        if let Some(hit) = key.and_then(|key| self.keys.get(key)) {
            if let Some(id) = &hit.proposal_id {
                return Some(ApprovalSubmission::Proposal(id.clone()));
            }
            if let Some(sequences) = &hit.sequences {
                return Some(ApprovalSubmission::Deduplicated(sequences.clone()));
            }
        }
        // With no policy, the batcher still handles retained keys.
        // Keep unmatched submissions on that path to preserve ordinary
        // command and batch ordering.
        if self.policy.is_none() {
            Some(ApprovalSubmission::Ordinary)
        } else {
            None
        }
    }
}

#[derive(sqlx::FromRow)]
struct ApprovalRoutingRow {
    has_policy: bool,
    approver_role: Option<String>,
    deadline_seconds: Option<i64>,
    allow_self_approval: Option<bool>,
    key: Option<String>,
    proposal_id: Option<String>,
    sequences: Option<Vec<i64>>,
}

/// Read the policy, open proposals and retained command outcomes for one
/// authorised type and client, including a batch's keys, in one snapshot.
pub async fn approval_routing(
    pool: &Pool,
    command_type: &CommandType,
    client_id: &str,
    keys: &[&str],
) -> crate::error::Result<ApprovalRouting> {
    let schema = schema_ident(&command_type.bounded_context.name);
    let rows: Vec<ApprovalRoutingRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT policy.command_type_name IS NOT NULL AS has_policy,
                policy.approver_role, policy.deadline_seconds, policy.allow_self_approval,
                input.key, proposal.id AS proposal_id, accepted.triggered_event_sequences AS sequences
           FROM (VALUES ($1::text)) AS requested(command_type_name)
           LEFT JOIN {schema}.command_approval_policies policy USING (command_type_name)
           LEFT JOIN unnest($3::text[]) AS input(key) ON true
           LEFT JOIN LATERAL (
               SELECT id FROM {schema}.command_proposals
               WHERE command_type_name = requested.command_type_name AND metadata_client_id = $2
                 AND idempotency_key = input.key AND {OPEN_PROPOSAL_KEY_PREDICATE}
           ) proposal ON true
           LEFT JOIN {schema}.idempotency_keys accepted
             ON policy.command_type_name IS NOT NULL
            AND accepted.command_type_name = requested.command_type_name
            AND accepted.client_id = $2 AND accepted.idempotency_key = input.key"
    )))
    .bind(&command_type.name)
    .bind(client_id)
    .bind(keys)
    .fetch_all(pool)
    .await?;
    let policy = rows.first().filter(|row| row.has_policy).map(|row| {
        CommandApprovalPolicy::new(row.approver_role.clone(), row.deadline_seconds)
            .allowing_self_approval(row.allow_self_approval.unwrap_or_default())
    });
    let keys = rows
        .into_iter()
        .filter_map(|row| {
            row.key.map(|key| {
                (
                    key,
                    ApprovalKey {
                        proposal_id: row.proposal_id,
                        sequences: row.sequences,
                    },
                )
            })
        })
        .collect();
    Ok(ApprovalRouting { policy, keys })
}

const APPROVAL_ROUTING_ATTEMPTS: usize = 4;

fn routing_changed_too_often() -> crate::error::Error {
    sqlx::Error::Protocol(
        "the approval policy or proposal changed repeatedly during submission; retry".into(),
    )
    .into()
}

/// Route a command after the caller's authorisation. A retained accepted
/// outcome is returned directly and must not be submitted to the batcher.
pub async fn route_approval_submission(
    pool: &Pool,
    authorised: &CommandAuthorised,
    key: Option<&str>,
    key_service: Option<&KeyService>,
    payload_limits: PayloadLimits,
    now: Timestamp,
) -> crate::error::Result<ApprovalSubmission> {
    let keys: Vec<_> = key.into_iter().collect();
    let routing =
        approval_routing(pool, &authorised.command_type, &authorised.client_id, &keys).await?;
    route_approval_submission_with(
        pool,
        authorised,
        key,
        key_service,
        payload_limits,
        now,
        &routing,
    )
    .await
}

/// Route using an initial shared snapshot. A mutation still checks the
/// policy under lock and refreshes the decision when a concurrent change wins.
pub async fn route_approval_submission_with(
    pool: &Pool,
    authorised: &CommandAuthorised,
    key: Option<&str>,
    key_service: Option<&KeyService>,
    payload_limits: PayloadLimits,
    now: Timestamp,
    routing: &ApprovalRouting,
) -> crate::error::Result<ApprovalSubmission> {
    let command_type = &authorised.command_type;
    let mut fresh = None;
    for _ in 0..APPROVAL_ROUTING_ATTEMPTS {
        let current = fresh.as_ref().unwrap_or(routing);
        if let Some(result) = current.resolved(key) {
            return Ok(result);
        }
        if let Some(policy) = &current.policy {
            match write_proposal(
                pool,
                command_type,
                authorised,
                policy,
                key,
                key_service,
                payload_limits,
                now,
                true,
            )
            .await?
            {
                ProposalWrite::Proposed(proposal) => {
                    return Ok(ApprovalSubmission::Proposal(proposal.id))
                }
                ProposalWrite::Deduplicated(sequences) => {
                    return Ok(ApprovalSubmission::Deduplicated(sequences))
                }
                ProposalWrite::Refresh => {}
            }
        }
        let keys: Vec<_> = key.into_iter().collect();
        fresh = Some(approval_routing(pool, command_type, &authorised.client_id, &keys).await?);
    }
    Err(routing_changed_too_often())
}

enum ProposalWrite {
    Proposed(Box<CommandProposal>),
    Deduplicated(Vec<i64>),
    Refresh,
}

/// Inserts a proposal for an already-authorised submission. The payload is
/// encrypted at rest with the command type's own sensitive fields, exactly
/// as a real command's is. With an idempotency key, a repeated open
/// submission for the same `(command type, client, key)` returns the
/// original proposal rather than creating a second one.
#[allow(
    clippy::too_many_arguments,
    reason = "one parameter per value the statement or scan uses"
)]
#[tracing::instrument(skip_all, fields(bounded_context = %command_type.bounded_context.name))]
pub async fn propose_command(
    pool: &Pool,
    command_type: &CommandType,
    authorised: &crate::event_store::CommandAuthorised,
    policy: &CommandApprovalPolicy,
    idempotency_key: Option<&str>,
    key_service: Option<&KeyService>,
    payload_limits: crate::payload_limits::PayloadLimits,
    now: Timestamp,
) -> crate::error::Result<CommandProposal> {
    for _ in 0..APPROVAL_ROUTING_ATTEMPTS {
        match write_proposal(
            pool,
            command_type,
            authorised,
            policy,
            idempotency_key,
            key_service,
            payload_limits,
            now,
            false,
        )
        .await?
        {
            ProposalWrite::Proposed(proposal) => return Ok(*proposal),
            ProposalWrite::Refresh => {}
            ProposalWrite::Deduplicated(_) => return Err(routing_changed_too_often()),
        }
    }
    Err(routing_changed_too_often())
}

#[allow(
    clippy::too_many_arguments,
    reason = "shares authorised proposal writes with the legacy core entry point"
)]
async fn write_proposal(
    pool: &Pool,
    command_type: &CommandType,
    authorised: &CommandAuthorised,
    policy: &CommandApprovalPolicy,
    idempotency_key: Option<&str>,
    key_service: Option<&KeyService>,
    payload_limits: PayloadLimits,
    now: Timestamp,
    recheck_routing: bool,
) -> crate::error::Result<ProposalWrite> {
    let bounded_context_name = &command_type.bounded_context.name;
    let schema = schema_ident(bounded_context_name);

    let mut resolved = std::collections::HashMap::new();
    resolve_encryption_keys(
        pool,
        bounded_context_name,
        &command_type.sensitive_fields,
        &authorised.payload,
        key_service,
        &mut resolved,
    )
    .await?;
    let protected = crate::event_store::try_protect_sensitive_fields(
        &command_type.sensitive_fields,
        &authorised.payload,
        |subject_key, subject_value| {
            let (_, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect("every subject protect_sensitive_fields asks for was resolved just above");
            (
                resolved
                    .get(&(subject_key.to_string(), subject_value.to_string()))
                    .expect("resolved just above")
                    .0
                    .clone(),
                data_key.clone(),
            )
        },
    )?;

    // A proposal is stored too, and an approver would only find out at the
    // end (docs/architecture.md §238, §239). Not under the write lock: the
    // approval's own submission checks again under it, against the blobs
    // this holds from collection below.
    payload_limits_override(pool, bounded_context_name)
        .await?
        .apply(payload_limits)
        .check(
            bounded_context_name,
            crate::payload_limits::PayloadKind::Command,
            &command_type.name,
            &protected.payload,
        )?;
    let mut blob_claims = Vec::new();
    for declared in crate::blob::declared_blob_refs(&command_type.blob_fields, &protected.payload)?
    {
        blob_claims.push(BlobClaim {
            event_sequence: None,
            field: declared.field,
            reference: declared.reference,
            subject: declared.subject,
        });
    }
    let blob_ids: Vec<String> = blob_claims
        .iter()
        .map(|claim| claim.reference.id.clone())
        .collect();
    // The check, the proposal and its pins in one transaction: a proposal
    // that exists can always be approved as far as its blobs go. Held first,
    // so two proposals of one blob queue rather than deadlock on upgrading
    // the check's shared lock.
    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *tx)
        .await?;
    let deadline_seconds = if recheck_routing {
        let policy: Option<(Option<i64>,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT deadline_seconds FROM {schema}.command_approval_policies
             WHERE command_type_name = $1 FOR SHARE"
        )))
        .bind(&command_type.name)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((deadline_seconds,)) = policy else {
            tx.rollback().await?;
            return Ok(ProposalWrite::Refresh);
        };
        deadline_seconds
    } else {
        policy.deadline_seconds
    };
    hold_blobs_for_pin(&mut tx, bounded_context_name, &blob_ids, now).await?;
    check_blob_claims(&mut tx, bounded_context_name, &blob_claims).await?;

    let id = crate::shared::try_generate_token_id()?;
    let expires_at = deadline_seconds.map(|seconds| now + jiff::SignedDuration::from_secs(seconds));
    let inserted: Option<ProposalRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.command_proposals \
         (id, command_type_name, payload, metadata_version, metadata_client_id, \
          metadata_created_at, metadata_correlation_id, metadata_causation_id, status, \
          idempotency_key, created_at, expires_at) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'pending',$9,$6,$10) \
         ON CONFLICT (command_type_name, metadata_client_id, idempotency_key) \
             WHERE {OPEN_PROPOSAL_KEY_PREDICATE} \
         DO NOTHING \
         RETURNING {PROPOSAL_COLUMNS}"
    )))
    .bind(&id)
    .bind(&command_type.name)
    .bind(&protected.payload)
    .bind(1_i64)
    .bind(&authorised.client_id)
    .bind(now.to_sqlx())
    .bind(&authorised.correlation_id)
    .bind(&authorised.causation_id)
    .bind(idempotency_key)
    .bind(expires_at.map(|t| t.to_sqlx()))
    .fetch_optional(&mut *tx)
    .await?;

    let row = if let Some(row) = inserted {
        // The earlier proposal may have finished between routing and this
        // insert. Its command is committed before its row leaves the index.
        if let Some(key) = idempotency_key.filter(|_| recheck_routing) {
            if let Some(sequences) = lookup_idempotency_key(
                &mut *tx,
                &schema,
                &command_type.name,
                &authorised.client_id,
                key,
            )
            .await?
            {
                tx.rollback().await?;
                return Ok(ProposalWrite::Deduplicated(sequences));
            }
        }
        pin_held_blobs(
            &mut tx,
            bounded_context_name,
            BlobPinHolder::CommandProposal,
            &id,
            &blob_ids,
        )
        .await?;
        row
    } else {
        // A fresh statement sees a competing insert that committed while
        // the INSERT waited. If it has already finished, route again.
        let existing: Option<ProposalRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {PROPOSAL_COLUMNS} FROM {schema}.command_proposals
             WHERE command_type_name = $1 AND metadata_client_id = $2
               AND idempotency_key = $3 AND {OPEN_PROPOSAL_KEY_PREDICATE}"
        )))
        .bind(&command_type.name)
        .bind(&authorised.client_id)
        .bind(idempotency_key)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(row) = existing else {
            tx.rollback().await?;
            return Ok(ProposalWrite::Refresh);
        };
        row
    };
    tx.commit().await?;
    Ok(ProposalWrite::Proposed(Box::new(
        row.into_domain(command_type.bounded_context.clone()),
    )))
}

/// Marks pending proposals whose deadline has passed as expired, then
/// returns the ones matching `status`/`command_type_name`. The lazy update
/// keeps listing and approving consistent without a background sweeper.
pub async fn expire_command_proposals(
    pool: &Pool,
    bounded_context: &str,
    now: Timestamp,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.command_proposals SET status = 'expired' \
         WHERE status = 'pending' AND expires_at IS NOT NULL AND expires_at <= $1"
    )))
    .bind(now.to_sqlx())
    .execute(pool)
    .await?;
    Ok(())
}

/// `scope` is the reading admin's own `RoleAccessMapping.scope`: a
/// proposal outside it is left out, as `fetch_commands` leaves out a
/// command (`approvals::proposal_owner_scope_satisfied`).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_command_proposals(
    pool: &Pool,
    bounded_context: &str,
    status: Option<CommandProposalStatus>,
    command_type_name: Option<&str>,
    scope: Option<&str>,
    now: Timestamp,
) -> crate::error::Result<Vec<CommandProposal>> {
    expire_command_proposals(pool, bounded_context, now).await?;
    let schema = schema_ident(bounded_context);
    let rows: Vec<ProposalRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {PROPOSAL_COLUMNS} FROM {schema}.command_proposals \
         WHERE ($1::text IS NULL OR status = $1) \
           AND ($2::text IS NULL OR command_type_name = $2) \
         ORDER BY created_at, id"
    )))
    .bind(status.map(CommandProposalStatus::as_str))
    .bind(command_type_name)
    .fetch_all(pool)
    .await?;
    let bc = require_bounded_context(pool, bounded_context).await?;
    let mut scope_filter = ProposalScopeFilter::default();
    let mut visible = Vec::new();
    for proposal in rows.into_iter().map(|row| row.into_domain(bc.clone())) {
        if scope_filter
            .in_scope(pool, bounded_context, &proposal, scope)
            .await?
        {
            visible.push(proposal);
        }
    }
    Ok(visible)
}

/// `commandProposals(boundedContext:, after:)`'s own read: at most `limit`
/// proposals in [`list_command_proposals`]'s order (oldest first, the
/// order an approval queue is worked in), strictly after `after`. Like
/// `parkedDeliveries` (docs/architecture.md §86), proposals outside
/// `scope` are skipped and the page is filled from as many stored pages
/// as that takes, so a page shorter than `limit` means the end.
#[allow(
    clippy::too_many_arguments,
    reason = "one parameter per value the statement or scan uses"
)]
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_command_proposals_page(
    pool: &Pool,
    bounded_context: &str,
    status: Option<CommandProposalStatus>,
    command_type_name: Option<&str>,
    scope: Option<&str>,
    after: Option<&CommandProposalCursor>,
    limit: i64,
    now: Timestamp,
) -> crate::error::Result<Vec<CommandProposal>> {
    expire_command_proposals(pool, bounded_context, now).await?;
    let schema = schema_ident(bounded_context);
    let bc = require_bounded_context(pool, bounded_context).await?;
    let mut scope_filter = ProposalScopeFilter::default();
    let mut visible = Vec::new();
    let mut after = after.cloned();
    loop {
        let rows: Vec<ProposalRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT {PROPOSAL_COLUMNS} FROM {schema}.command_proposals \
             WHERE ($1::text IS NULL OR status = $1) \
               AND ($2::text IS NULL OR command_type_name = $2) \
               AND ($3::timestamptz IS NULL OR (created_at, id) > ($3, $4)) \
             ORDER BY created_at, id LIMIT $5"
        )))
        .bind(status.map(CommandProposalStatus::as_str))
        .bind(command_type_name)
        .bind(after.as_ref().map(|c| c.created_at).map(|t| t.to_sqlx()))
        .bind(after.as_ref().map(|c| c.id.clone()))
        .bind(limit)
        .fetch_all(pool)
        .await?;
        let exhausted = (rows.len() as i64) < limit;
        for proposal in rows.into_iter().map(|row| row.into_domain(bc.clone())) {
            after = Some(CommandProposalCursor::of(&proposal));
            if scope_filter
                .in_scope(pool, bounded_context, &proposal, scope)
                .await?
            {
                visible.push(proposal);
                if visible.len() as i64 == limit {
                    return Ok(visible);
                }
            }
        }
        if exhausted {
            return Ok(visible);
        }
    }
}

/// A position in [`list_command_proposals_page`]'s order. Like
/// [`ParkedDeliveryCursor`] it carries the position rather than naming a
/// row, so approving or rejecting the last proposal of a page - and with
/// a `status` filter, that moves it out of the listing - doesn't
/// invalidate the next page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandProposalCursor {
    pub created_at: Timestamp,
    pub id: String,
}

impl CommandProposalCursor {
    pub fn of(proposal: &CommandProposal) -> Self {
        Self {
            created_at: proposal.created_at,
            id: proposal.id.clone(),
        }
    }

    /// `{microseconds since the epoch}:{id}` - opaque to clients.
    pub fn encode(&self) -> String {
        format!("{}:{}", self.created_at.as_microsecond(), self.id)
    }

    pub fn decode(cursor: &str) -> Option<Self> {
        let (micros, id) = cursor.split_once(':')?;
        Some(Self {
            created_at: Timestamp::from_microsecond(micros.parse().ok()?).ok()?,
            id: id.to_string(),
        })
    }
}

/// Whether a proposal is inside an admin's owner scope, looking each
/// command type up once per listing.
#[derive(Default)]
pub(super) struct ProposalScopeFilter {
    pub(super) command_types: std::collections::HashMap<String, Option<CommandType>>,
}

impl ProposalScopeFilter {
    pub(super) async fn in_scope(
        &mut self,
        pool: &Pool,
        bounded_context: &str,
        proposal: &CommandProposal,
        scope: Option<&str>,
    ) -> crate::error::Result<bool> {
        if scope.is_none() {
            return Ok(true);
        }
        if !self.command_types.contains_key(&proposal.command_type_name) {
            let command_type =
                get_command_type(pool, bounded_context, &proposal.command_type_name).await?;
            self.command_types
                .insert(proposal.command_type_name.clone(), command_type);
        }
        // A type that no longer exists has no owner dimension left to
        // prove the proposal in scope: left out, failing closed.
        Ok(self.command_types[&proposal.command_type_name]
            .as_ref()
            .is_some_and(|command_type| {
                crate::approvals::proposal_owner_scope_satisfied(command_type, proposal, scope)
            }))
    }
}

pub(super) const PROPOSAL_COLUMNS: &str = "id, command_type_name, payload, metadata_version, \
    metadata_client_id, metadata_created_at, metadata_correlation_id, metadata_causation_id, \
    status, idempotency_key, created_at, expires_at, decided_at, decided_by_role_id, \
    rejection_reason, rejection_kind, command_external_id";

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_command_proposal(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<CommandProposal>> {
    let schema = schema_ident(bounded_context);
    let row: Option<ProposalRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, command_type_name, payload, metadata_version, metadata_client_id, \
                metadata_created_at, metadata_correlation_id, metadata_causation_id, status, \
                idempotency_key, created_at, expires_at, decided_at, decided_by_role_id, \
                rejection_reason, rejection_kind, command_external_id \
         FROM {schema}.command_proposals WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let bc = require_bounded_context(pool, bounded_context).await?;
    Ok(Some(row.into_domain(bc)))
}

#[derive(sqlx::FromRow)]
pub(super) struct ProposalRow {
    pub(super) id: String,
    pub(super) command_type_name: String,
    pub(super) payload: String,
    pub(super) metadata_version: i64,
    pub(super) metadata_client_id: String,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub(super) metadata_created_at: Timestamp,
    pub(super) metadata_correlation_id: Option<String>,
    pub(super) metadata_causation_id: Option<String>,
    pub(super) status: String,
    pub(super) idempotency_key: Option<String>,
    #[sqlx(try_from = "jiff_sqlx::Timestamp")]
    pub(super) created_at: Timestamp,
    #[sqlx(try_from = "NullableTimestamp")]
    pub(super) expires_at: Option<Timestamp>,
    #[sqlx(try_from = "NullableTimestamp")]
    pub(super) decided_at: Option<Timestamp>,
    pub(super) decided_by_role_id: Option<String>,
    pub(super) rejection_reason: Option<String>,
    pub(super) rejection_kind: Option<String>,
    pub(super) command_external_id: Option<String>,
}

impl ProposalRow {
    pub(super) fn into_domain(self, bounded_context: BoundedContext) -> CommandProposal {
        CommandProposal {
            id: self.id,
            bounded_context,
            command_type_name: self.command_type_name,
            payload: self.payload,
            metadata: Metadata {
                r#type: "Command".to_string(),
                version: self.metadata_version,
                client_id: self.metadata_client_id,
                created_at: self.metadata_created_at,
                correlation_id: self.metadata_correlation_id,
                causation_id: self.metadata_causation_id,
            },
            status: CommandProposalStatus::from_db(&self.status)
                .expect("command_proposals.status has a CHECK constraint"),
            idempotency_key: self.idempotency_key,
            created_at: self.created_at,
            expires_at: self.expires_at,
            decided_at: self.decided_at,
            decided_by_role_id: self.decided_by_role_id,
            rejection_reason: self.rejection_reason,
            rejection_kind: self.rejection_kind,
            command_id: self.command_external_id,
        }
    }
}

/// How long an `approving` claim may sit before another approver may take
/// it over. A claim normally lives for one `decide_and_submit_command`
/// call; one older than this was left behind by a crash, or by a failure
/// to record the outcome after the command committed. Taking it over is
/// safe even if the original approval is somehow still running: both run
/// under the same idempotency key ([`approval_idempotency_key`]), so the
/// second one is deduplicated under the bounded context's lock.
pub const APPROVAL_CLAIM_STALE_AFTER: jiff::SignedDuration = jiff::SignedDuration::from_mins(10);

/// The idempotency key an approval submits under: the proposer's own key,
/// or a reserved one derived from the proposal id.
pub fn approval_idempotency_key(proposal: &CommandProposal) -> String {
    proposal.idempotency_key.clone().unwrap_or_else(|| {
        format!(
            "{}{}",
            crate::event_store::RESERVED_APPROVAL_IDEMPOTENCY_KEY_PREFIX,
            proposal.id
        )
    })
}

/// Claims a proposal for `approver`: a `pending` one that has not expired,
/// or an `approving` one whose claim is older than
/// [`APPROVAL_CLAIM_STALE_AFTER`] (see there). The conditional `UPDATE` is
/// the lock: exactly one concurrent approver wins the claim.
///
/// If the proposal's command type no longer has an approval policy (it was
/// removed after the proposal was made), the proposal is still decidable by
/// any active admin of the bounded context, rather than stranded `pending`
/// with nobody able to approve or reject it.
pub async fn claim_command_proposal(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
    approver: &RoleAccessMapping,
    decision: crate::approvals::ProposalDecision,
    now: Timestamp,
) -> crate::error::Result<CommandProposal> {
    if approver.bounded_context.name != bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    expire_command_proposals(pool, bounded_context, now).await?;
    let not_found = || crate::approvals::Error::ProposalNotFound { id: id.to_string() };
    let proposal = get_command_proposal(pool, bounded_context, id)
        .await?
        .ok_or_else(not_found)?;
    // Outside the approver's owner scope reads as not found, the same
    // answer a missing id gets, so the scope is no existence oracle.
    let in_scope = approver.scope.is_none()
        || get_command_type(pool, bounded_context, &proposal.command_type_name)
            .await?
            .is_some_and(|command_type| {
                crate::approvals::proposal_owner_scope_satisfied(
                    &command_type,
                    &proposal,
                    approver.scope.as_deref(),
                )
            });
    if !in_scope {
        return Err(not_found().into());
    }
    let policy = get_command_approval_policy(pool, bounded_context, &proposal.command_type_name)
        .await?
        .unwrap_or_else(|| CommandApprovalPolicy::new(None, None));
    crate::approvals::check_approver(&policy, approver)?;
    if decision == crate::approvals::ProposalDecision::Approve {
        // Resolve the proposer to a Role, if it is one: a GraphQL
        // submission stamps `access_mapping.role.id`, while a REST token
        // or an internal path (deadline, route) stamps an id no Role
        // carries - `None`, so any admin may approve those. Matching on
        // `external_subject` (the human) rather than the Role id closes
        // the two-Roles-one-human gap; a proposer Role since revoked
        // still carries its subject, so it still refuses.
        let proposer_role = get_role(pool, &proposal.metadata.client_id).await?;
        crate::approvals::check_self_approval(&policy, proposer_role.as_ref(), approver)?;
    }
    let stale_cutoff = now - APPROVAL_CLAIM_STALE_AFTER;
    match proposal.status {
        CommandProposalStatus::Pending if proposal.is_expired_at(now) => {
            return Err(crate::approvals::Error::ProposalExpired.into())
        }
        CommandProposalStatus::Pending => {}
        CommandProposalStatus::Approving
            if proposal
                .decided_at
                .is_some_and(|claimed_at| claimed_at <= stale_cutoff) =>
        {
            tracing::warn!(
                bounded_context,
                proposal_id = id,
                "taking over a stale approval claim - a previous approval crashed or \
                 failed to record its outcome"
            );
        }
        CommandProposalStatus::Expired => {
            return Err(crate::approvals::Error::ProposalExpired.into())
        }
        status => {
            return Err(crate::approvals::Error::ProposalNotPending {
                status: status.as_str(),
            }
            .into())
        }
    }

    let schema = schema_ident(bounded_context);
    let claimed: Option<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.command_proposals \
         SET status = 'approving', decided_at = $2, decided_by_role_id = $3 \
         WHERE id = $1 AND ( \
             (status = 'pending' AND (expires_at IS NULL OR expires_at > $2)) \
             OR (status = 'approving' AND decided_at <= $4)) \
         RETURNING id"
    )))
    .bind(id)
    .bind(now.to_sqlx())
    .bind(&approver.role.id)
    .bind(stale_cutoff.to_sqlx())
    .fetch_optional(pool)
    .await?;
    if claimed.is_none() {
        return Err(crate::approvals::Error::ProposalNotPending { status: "claimed" }.into());
    }
    get_command_proposal(pool, bounded_context, id)
        .await?
        .ok_or_else(|| crate::approvals::Error::ProposalNotFound { id: id.to_string() }.into())
}

/// After [`claim_command_proposal`] took over a stale claim, settles what
/// the earlier approval already did, before anything new is decided:
/// - its command committed (the approval's idempotency key is recorded):
///   the proposal is finished `Approved`, and `Ok(true)` is returned;
/// - it did not, but the proposal's deadline has since passed: the
///   proposal is finished `Expired`, and `ProposalExpired` is returned;
/// - otherwise `Ok(false)`: nothing happened yet, decide as normal.
///
/// A fresh pending claim can also find a retained acceptance under a
/// caller key, for example when settling legacy duplicate proposals.
pub(super) async fn settle_reclaimed_proposal(
    pool: &Pool,
    bounded_context: &str,
    proposal: &CommandProposal,
    now: Timestamp,
) -> crate::error::Result<bool> {
    if idempotency_key_already_used(
        pool,
        bounded_context,
        &proposal.command_type_name,
        &proposal.metadata.client_id,
        &approval_idempotency_key(proposal),
    )
    .await?
    {
        finish_command_proposal(
            pool,
            bounded_context,
            &proposal.id,
            CommandProposalStatus::Approved,
            None,
            None,
            None,
            now,
        )
        .await?;
        return Ok(true);
    }
    if proposal.is_expired_at(now) {
        finish_command_proposal(
            pool,
            bounded_context,
            &proposal.id,
            CommandProposalStatus::Expired,
            None,
            None,
            None,
            now,
        )
        .await?;
        return Err(crate::approvals::Error::ProposalExpired.into());
    }
    Ok(false)
}

/// The payload approval hands `decide()`: the stored ciphertext revealed.
pub async fn reveal_command_proposal_payload(
    pool: &Pool,
    command_type: &CommandType,
    proposal: &CommandProposal,
    key_service: Option<&KeyService>,
) -> crate::error::Result<String> {
    let subjects = crate::event_store::try_sensitive_field_subjects(
        &command_type.sensitive_fields,
        &proposal.payload,
    )?;
    if subjects.is_empty() {
        return Ok(proposal.payload.clone());
    }
    let key_service = key_service.ok_or(encryption::Error::MasterKeyNotConfigured)?;
    let mut keys: std::collections::HashMap<(String, String), Option<DataKey>> =
        std::collections::HashMap::new();
    for (subject_key, subject_value) in subjects {
        let key = get_active_data_key(
            pool,
            &proposal.bounded_context.name,
            &subject_key,
            &subject_value,
            key_service,
        )
        .await?;
        keys.insert((subject_key, subject_value), key);
    }
    crate::event_store::reveal_sensitive_fields(
        &command_type.sensitive_fields,
        &proposal.payload,
        |subject_key, subject_value| {
            keys.get(&(subject_key.to_string(), subject_value.to_string()))
                .cloned()
                .flatten()
        },
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "one parameter per value the statement or scan uses"
)]
pub async fn finish_command_proposal(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
    status: CommandProposalStatus,
    rejection_reason: Option<&str>,
    rejection_kind: Option<&str>,
    command_external_id: Option<&str>,
    now: Timestamp,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.command_proposals SET status = $2, decided_at = $3, \
            rejection_reason = $4, rejection_kind = $5, command_external_id = $6 \
         WHERE id = $1 AND status = 'approving'"
    )))
    .bind(id)
    .bind(status.as_str())
    .bind(now.to_sqlx())
    .bind(rejection_reason)
    .bind(rejection_kind)
    .bind(command_external_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn release_command_proposal(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.command_proposals \
         SET status = 'pending', decided_at = NULL, decided_by_role_id = NULL \
         WHERE id = $1 AND status = 'approving'"
    )))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Whether a subject `proposal`'s sensitive fields are encrypted for had
/// a key destroyed at or after the proposal was made. Making it provisioned
/// (or reused) that subject's active key, and a subject has at most one
/// active key, so any destruction since means the key the payload was
/// encrypted under is gone - even if a later submission has provisioned a
/// fresh one (docs/architecture.md §221).
pub(super) async fn proposal_subject_forgotten(
    pool: &Pool,
    command_type: &CommandType,
    proposal: &CommandProposal,
) -> crate::error::Result<bool> {
    let schema = schema_ident(&proposal.bounded_context.name);
    for (subject_key, subject_value) in crate::event_store::try_sensitive_field_subjects(
        &command_type.sensitive_fields,
        &proposal.payload,
    )? {
        let destroyed: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT EXISTS (SELECT 1 FROM {schema}.encryption_keys \
             WHERE subject_key = $1 AND subject_value = $2 \
               AND status = 'destroyed' AND destroyed_at >= $3)"
        )))
        .bind(&subject_key)
        .bind(&subject_value)
        .bind(proposal.created_at.to_sqlx())
        .fetch_one(pool)
        .await?;
        if destroyed {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `forgetSubject`'s proposal half (docs/architecture.md §221): every
/// `pending` proposal in `bounded_context` whose sensitive fields are
/// encrypted for the subject is finished as `forgotten`. Once the key is
/// destroyed its payload can never be revealed, so it could never be
/// approved; left alone it would sit in the queue until someone rejected
/// it, or failed to approve it. Called before the key goes, like
/// [`forget_subject_in_deadlines`]. A proposal names the subject when its
/// payload does through its command type's sensitive fields
/// (`sensitive_field_subjects`) - the subject field itself is stored in
/// the clear - or, should that type be gone, when the subject value
/// appears anywhere in it. The payload is kept, encrypted as a command's
/// is, so the proposal renders as a command of a forgotten subject does.
/// An `approving` claim is past stopping and left alone, as a `firing`
/// deadline is: if it already revealed the payload, its submission
/// provisions a fresh key. Returns how many proposals were forgotten.
pub async fn forget_subject_in_command_proposals(
    pool: &Pool,
    bounded_context: &str,
    subject_key: &str,
    subject_value: &str,
    now: Timestamp,
) -> crate::error::Result<u64> {
    const PAGE: i64 = 1000;
    let schema = schema_ident(bounded_context);
    let mut sensitive_fields: std::collections::HashMap<String, Option<Vec<SensitiveField>>> =
        std::collections::HashMap::new();
    let mut forgotten = 0;
    let mut after = String::new();
    loop {
        let rows: Vec<(String, String, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id, command_type_name, payload FROM {schema}.command_proposals \
             WHERE status = 'pending' AND id > $1 ORDER BY id LIMIT $2"
        )))
        .bind(&after)
        .bind(PAGE)
        .fetch_all(pool)
        .await?;
        let Some((last_id, _, _)) = rows.last() else {
            break;
        };
        after = last_id.clone();
        for (id, command_type, payload) in &rows {
            if !sensitive_fields.contains_key(command_type) {
                let fields = get_command_type(pool, bounded_context, command_type)
                    .await?
                    .map(|ct| ct.sensitive_fields);
                sensitive_fields.insert(command_type.clone(), fields);
            }
            let parsed = serde_json::from_str::<serde_json::Value>(payload).ok();
            let names_subject = match (&sensitive_fields[command_type], &parsed) {
                (Some(fields), Some(_)) => {
                    crate::event_store::try_sensitive_field_subjects(fields, payload)?
                        .iter()
                        .any(|(key, value)| key == subject_key && value == subject_value)
                }
                (_, Some(json)) => json_mentions(json, subject_value),
                (_, None) => payload.contains(subject_value),
            };
            if names_subject {
                forgotten += sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {schema}.command_proposals SET status = 'forgotten', decided_at = $1 \
                     WHERE id = $2 AND status = 'pending'"
                )))
                .bind(now.to_sqlx())
                .bind(id)
                .execute(pool)
                .await?
                .rows_affected();
            }
        }
        if (rows.len() as i64) < PAGE {
            break;
        }
    }
    Ok(forgotten)
}

pub async fn reject_command_proposal(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
    approver: &RoleAccessMapping,
    reason: Option<&str>,
    now: Timestamp,
) -> crate::error::Result<CommandProposal> {
    let proposal = claim_command_proposal(
        pool,
        bounded_context,
        id,
        approver,
        crate::approvals::ProposalDecision::Reject,
        now,
    )
    .await?;
    // A taken-over stale claim whose command already committed is approved,
    // not rejectable - rejecting it would contradict the command that exists.
    if settle_reclaimed_proposal(pool, bounded_context, &proposal, now).await? {
        return Err(crate::approvals::Error::ProposalNotPending { status: "approved" }.into());
    }
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.command_proposals SET status = 'rejected', rejection_reason = $2, \
            decided_at = $3 WHERE id = $1 AND status = 'approving'"
    )))
    .bind(id)
    .bind(reason)
    .bind(now.to_sqlx())
    .execute(pool)
    .await?;
    get_command_proposal(pool, bounded_context, id)
        .await?
        .ok_or_else(|| crate::approvals::Error::ProposalNotFound { id: id.to_string() }.into())
}

/// The approval gate for skilj's own internal submissions - a fired
/// deadline (`fire_due_deadlines`) and a cross-context route
/// (`catch_up_cross_context_route_locked`). Both call
/// `decide_and_submit_command` directly rather than going through REST or
/// GraphQL, so without this an agent able to cause the *source* event could
/// get an approval-required command executed with no approver at all.
///
/// Returns `true` when `command_type` requires approval, and the occurrence
/// is then handled by this call: a proposal is created under the internal
/// caller's own `client_id` and reserved `idempotency_key` (or, after a
/// crash between a prior call and the caller's cursor/resolution update,
/// the existing open proposal is found again by that key; or, if that
/// proposal was already approved, nothing is proposed twice). Returns
/// `false` when the type has no approval policy, and the caller submits as
/// before.
///
/// A deadline's `DeadlineContext` is not carried onto the proposal: the
/// approved command runs at approval time, so its `decide_with` sees no
/// deadline - an approver deciding late is the approver's call.
#[allow(
    clippy::too_many_arguments,
    reason = "one parameter per value the statement or scan uses"
)]
pub async fn propose_if_approval_required(
    pool: &Pool,
    command_type: &CommandType,
    payload: &str,
    client_id: &str,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
    idempotency_key: &str,
    key_service: Option<&KeyService>,
    now: Timestamp,
) -> crate::error::Result<bool> {
    let authorised = crate::event_store::CommandAuthorised {
        command_type: command_type.clone(),
        payload: payload.to_string(),
        client_id: client_id.to_string(),
        correlation_id: correlation_id.map(str::to_string),
        causation_id: causation_id.map(str::to_string),
    };
    let outcome = route_approval_submission(
        pool,
        &authorised,
        Some(idempotency_key),
        key_service,
        PayloadLimits::default(),
        now,
    )
    .await?;
    Ok(!matches!(outcome, ApprovalSubmission::Ordinary))
}

/// What approving a proposal did: the proposal after its final update,
/// and the ordinary submission outcome (`Accepted`, `Rejected` by
/// `decide()`, or `Deduplicated` on a retry after a crash).
pub struct CommandApprovalOutcome {
    pub proposal: CommandProposal,
    pub outcome: SubmitCommandOutcome,
}

/// Runs the ordinary submission pipeline for a claimed proposal, with the
/// original submitter's own `client_id` and correlation/causation ids.
/// On an `Err` the claim is released back to `pending`, so a transient
/// failure is retryable; on a business rejection the proposal is recorded
/// rejected with `decide()`'s own reason.
#[allow(
    clippy::too_many_arguments,
    reason = "takes the engine's shared services (the pool, dispatchers and the like) as separate borrows, beside the call's own inputs"
)]
pub async fn approve_command_proposal(
    pool: &Pool,
    dispatcher: &dyn crate::plugin::CommandDispatcher,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    id: &str,
    approver: &RoleAccessMapping,
    key_service: Option<&KeyService>,
    now: Timestamp,
) -> crate::error::Result<CommandApprovalOutcome> {
    let proposal = claim_command_proposal(
        pool,
        bounded_context,
        id,
        approver,
        crate::approvals::ProposalDecision::Approve,
        now,
    )
    .await?;
    // A taken-over stale claim may already have produced its command; if so
    // it is marked approved here and the submission below deduplicates.
    settle_reclaimed_proposal(pool, bounded_context, &proposal, now).await?;
    let command_type =
        match get_command_type(pool, bounded_context, &proposal.command_type_name).await? {
            Some(command_type) => command_type,
            None => {
                release_command_proposal(pool, bounded_context, id).await?;
                return Err(crate::error::Error::row_not_found());
            }
        };
    let payload = match reveal_command_proposal_payload(pool, &command_type, &proposal, key_service)
        .await
    {
        Ok(payload) => payload,
        Err(e) => {
            // docs/architecture.md §221: a subject forgotten without
            // `forget_subject_in_command_proposals` (an older instance, or a
            // direct `destroy_encryption_key`) - the payload is gone for
            // good, so the proposal is finished rather than released to
            // fail again. Anything else, the check failing included,
            // releases the claim, so the approval stays retryable.
            match proposal_subject_forgotten(pool, &command_type, &proposal).await {
                Ok(true) => {
                    tracing::info!(
                        bounded_context,
                        proposal_id = id,
                        error = %e,
                        "a subject this proposal names was forgotten - recording it as forgotten"
                    );
                    finish_command_proposal(
                        pool,
                        bounded_context,
                        id,
                        CommandProposalStatus::Forgotten,
                        None,
                        None,
                        None,
                        now,
                    )
                    .await?;
                    return Err(crate::approvals::Error::ProposalSubjectForgotten.into());
                }
                Ok(false) => {}
                Err(check) => tracing::warn!(
                    bounded_context,
                    proposal_id = id,
                    error = %check,
                    "could not check whether a subject this proposal names was forgotten"
                ),
            }
            release_command_proposal(pool, bounded_context, id).await?;
            return Err(e);
        }
    };

    let idempotency_key = approval_idempotency_key(&proposal);
    let outcome = decide_and_submit_command(
        pool,
        dispatcher,
        projection_dispatcher,
        snapshot_dispatcher,
        broadcaster,
        event_cache,
        &command_type,
        &payload,
        &proposal.metadata.client_id,
        proposal.metadata.correlation_id.as_deref(),
        proposal.metadata.causation_id.as_deref(),
        key_service,
        now,
        Some(&idempotency_key),
        None,
    )
    .await;

    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(e) => {
            release_command_proposal(pool, bounded_context, id).await?;
            return Err(e);
        }
    };

    match &outcome {
        SubmitCommandOutcome::Accepted { command, .. } => {
            finish_command_proposal(
                pool,
                bounded_context,
                id,
                CommandProposalStatus::Approved,
                None,
                None,
                Some(command.id.as_str()),
                now,
            )
            .await?;
        }
        SubmitCommandOutcome::Rejected { reason, kind, .. } => {
            finish_command_proposal(
                pool,
                bounded_context,
                id,
                CommandProposalStatus::Rejected,
                Some(reason),
                Some(kind),
                None,
                now,
            )
            .await?;
        }
        SubmitCommandOutcome::Deduplicated { .. } => {
            finish_command_proposal(
                pool,
                bounded_context,
                id,
                CommandProposalStatus::Approved,
                None,
                None,
                None,
                now,
            )
            .await?;
        }
    }

    Ok(CommandApprovalOutcome {
        proposal: get_command_proposal(pool, bounded_context, id)
            .await?
            .ok_or_else(|| crate::approvals::Error::ProposalNotFound { id: id.to_string() })?,
        outcome,
    })
}

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_command_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<CommandType>> {
    use sqlx::FromRow;
    // The command type with its bounded context, hydrated, in one
    // statement (docs/architecture.md §196, §199): every command resolves
    // its type this way before the lock. `command_types` is named first so
    // it is locked before `roles`, in the order `DROP SCHEMA ... CASCADE`
    // takes them; the other order deadlocked with deleting this bounded
    // context (§251).
    let schema = schema_ident(bounded_context);
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {}, {} FROM {schema}.command_types ct \
         JOIN bounded_contexts bc ON bc.name = $1 {HYDRATED_BOUNDED_CONTEXT_JOINS} \
         WHERE ct.name = $2",
        hydrated_bounded_context_columns(),
        COMMAND_TYPE_COLUMNS
            .split(", ")
            .map(|column| format!("ct.{}", column.trim()))
            .collect::<Vec<_>>()
            .join(", ")
    )))
    .bind(bounded_context)
    .bind(name)
    .fetch_optional(pool)
    .await;
    let row = match row {
        Ok(Some(row)) => row,
        Ok(None) => return Ok(None),
        // The schema is gone with a hard-deleted bounded context, which
        // reads as no such command type, as it did when the bounded
        // context's row was read first.
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let bc = hydrated_bounded_context(&row)?;
    Ok(Some(CommandTypeRow::from_row(&row)?.into_domain(bc)))
}
