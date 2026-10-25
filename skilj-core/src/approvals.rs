//! Command approvals (issue #98): a command type can declare that
//! a submission is only a proposal, held until a human (a Role with an
//! active admin grant, optionally narrowed to one role name) approves it.
//!
//! `decide()` is deliberately not run when the proposal is created -
//! approval runs the ordinary submission pipeline, so the decision is
//! made against the state at approval time, not at proposal time. A
//! proposal rejected by `decide()` at approval time is recorded as
//! `Rejected`, with the rejection reason, rather than becoming an event.
//!
//! The policy is persisted separately from `CommandType` (a
//! `{schema}.command_approval_policies` row), so every existing
//! `CommandType` literal in tests and consumer code keeps working
//! unchanged. `plugin::CommandType::requires_approval()` and its siblings
//! are what a compiled-in type declares; `SkiljBuilder::build()`'s
//! reconciliation writes them into that table.

use crate::access_control::{AccessLevel, RoleAccessMapping, RoleStatus};
use crate::shared::Metadata;

/// See `CommandType::requires_approval()`'s own doc comment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandApprovalPolicy {
    /// The `Role.name` an approver must have, on top of an active admin
    /// grant. `None` means any active admin-level `RoleAccessMapping` on
    /// the bounded context may approve.
    pub approver_role: Option<String>,
    /// How long a proposal stays approvable, in seconds. `None` means no
    /// deadline. The value is copied onto each proposal's own `expires_at`
    /// when it is created.
    pub deadline_seconds: Option<i64>,
    /// Whether the account that proposed a command may also approve it.
    /// `false` by default: the point of an approval is a second pair of
    /// eyes, and an admin who submits through GraphQL would otherwise be
    /// able to wave their own proposal through - under any of their Roles.
    /// Turn it on for a type where that is acceptable (a single-user
    /// setup, say). Rejecting - withdrawing - one's own proposal is
    /// always allowed.
    pub allow_self_approval: bool,
}

impl CommandApprovalPolicy {
    /// A policy with self-approval off; see [`Self::allowing_self_approval`].
    pub fn new(approver_role: Option<String>, deadline_seconds: Option<i64>) -> Self {
        Self {
            approver_role,
            deadline_seconds,
            allow_self_approval: false,
        }
    }

    /// Whether the account that proposed a command may approve it too.
    pub fn allowing_self_approval(mut self, allow: bool) -> Self {
        self.allow_self_approval = allow;
        self
    }
}

/// Where a proposal is in its lifecycle. `Approving` is the short-lived
/// claim an approver takes before running the submission pipeline, so two
/// concurrent approvals cannot both accept the same proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommandProposalStatus {
    /// Waiting for an approver.
    Pending,
    /// Claimed by an approver whose submission is running.
    Approving,
    /// Approved, and the command submitted.
    Approved,
    /// Rejected by an approver, or by the command's own `decide()` at
    /// approval.
    Rejected,
    /// Its approval deadline passed first.
    Expired,
    /// A subject its sensitive fields are encrypted for was forgotten
    /// (`forgetSubject`) first, so it can never be approved
    /// (docs/architecture.md §221).
    Forgotten,
}

impl CommandProposalStatus {
    /// The lowercase name stored and served, e.g. `pending`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approving => "approving",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
            Self::Forgotten => "forgotten",
        }
    }

    /// Parses [`Self::as_str`]'s output; `None` for anything else.
    pub fn from_db(value: &str) -> Option<Self> {
        Some(match value {
            "pending" => Self::Pending,
            "approving" => Self::Approving,
            "approved" => Self::Approved,
            "rejected" => Self::Rejected,
            "expired" => Self::Expired,
            "forgotten" => Self::Forgotten,
            _ => return None,
        })
    }
}

/// One submitted command held for approval. `payload` is stored encrypted
/// exactly as a real `Command.payload` is, and rendered the same way
/// (`render_command`) before it reaches a reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandProposal {
    /// The proposal's id.
    pub id: String,
    /// The bounded context the command was submitted to.
    pub bounded_context: crate::event_store::BoundedContext,
    /// The command type submitted.
    pub command_type_name: String,
    /// The submitted payload, JSON, encrypted at rest as a command's is.
    pub payload: String,
    /// The submission's metadata; `client_id` is the proposer.
    pub metadata: Metadata,
    /// Where the proposal is in its lifecycle.
    pub status: CommandProposalStatus,
    /// The submission's idempotency key, applied when it is approved.
    pub idempotency_key: Option<String>,
    /// When it was proposed.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Its approval deadline; `None` for none.
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    /// When it was approved, rejected, expired or forgotten.
    pub decided_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The role that approved or rejected it.
    pub decided_by_role_id: Option<String>,
    /// Why it was rejected, by the approver or by `decide()`.
    pub rejection_reason: Option<String>,
    /// The rejection's machine-readable code.
    pub rejection_kind: Option<String>,
    /// The accepted command's own `Command.id` (`commands.external_id`),
    /// present once approval produced one.
    pub command_id: Option<String>,
}

impl CommandProposal {
    /// Whether `now` is at or past this proposal's own deadline. A
    /// proposal with no deadline is never expired.
    pub fn is_expired_at(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        self.expires_at.is_some_and(|expires_at| now >= expires_at)
    }
}

/// Whether `proposal` falls inside an admin's owner `scope` - the same
/// check `fetch_commands` applies to a real command
/// (`event_store::command_owner_scope_satisfied`), so an admin confined
/// to one owner can neither list nor decide another owner's proposals.
/// The tags are derived from the stored payload: a tag mapping may not
/// name a sensitive field, so encryption at rest leaves every field a
/// plain mapping reads untouched.
pub fn proposal_owner_scope_satisfied(
    command_type: &crate::event_store::CommandType,
    proposal: &CommandProposal,
    scope: Option<&str>,
) -> bool {
    if scope.is_none() || command_type.owner_tag_key.is_none() {
        return true;
    }
    crate::event_store::tag_owner_scope_satisfied(
        &crate::event_store::derive_plain_tags(&command_type.tag_mappings, &proposal.payload),
        command_type.owner_tag_key.as_deref(),
        scope,
    )
}

/// See `entity CommandProposal`'s approval gate. Reuses the access
/// control module's own rejections: an inactive grant is `GrantNotActive`,
/// one below admin `InsufficientAccessLevel`, and a role-name mismatch
/// this module's own `InsufficientApprovalRole`.
pub fn check_approver(
    policy: &CommandApprovalPolicy,
    access_mapping: &RoleAccessMapping,
) -> crate::error::Result<()> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if let Some(required) = &policy.approver_role {
        if &access_mapping.role.name != required {
            return Err(Error::InsufficientApprovalRole {
                required: required.clone(),
            }
            .into());
        }
    }
    Ok(())
}

/// What a claim on a proposal is for - self-approval only gates approving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProposalDecision {
    /// The claim is for approving.
    Approve,
    /// The claim is for rejecting.
    Reject,
}

/// The self-approval gate, applied to approving only - never to rejecting.
/// "Self" is the proposing human, not the proposing Role row: the caller
/// passes the Role the proposal's `metadata.client_id` resolves to (via
/// `db::get_role`), if any, and approval is refused when its
/// `external_subject` is the approver's own. Comparing subjects closes
/// the two-Roles-one-human gap a bare `client_id == role.id` check
/// leaves: revoking the proposing Role and approving under a fresh one
/// with the same subject is still self-approval. A REST token or an
/// internal path (deadline, route) resolves to no Role at all, so any
/// admin may approve those - by lookup, not by betting no token id ever
/// collides with a Role id.
pub fn check_self_approval(
    policy: &CommandApprovalPolicy,
    proposer_role: Option<&crate::access_control::Role>,
    approver: &RoleAccessMapping,
) -> crate::error::Result<()> {
    if policy.allow_self_approval {
        return Ok(());
    }
    if proposer_role
        .is_some_and(|proposer| proposer.external_subject == approver.role.external_subject)
    {
        return Err(Error::SelfApprovalNotAllowed.into());
    }
    Ok(())
}

/// Why a command proposal could not be approved or rejected.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// No such proposal, or one outside the caller's owner scope.
    #[error("no command proposal with id {id:?} exists in this bounded context")]
    ProposalNotFound {
        /// The id asked for.
        id: String,
    },

    /// The proposal was already decided, or is being approved.
    #[error("this command proposal is {status}, not pending approval")]
    ProposalNotPending {
        /// Its current status, as [`CommandProposalStatus::as_str`] names it.
        status: &'static str,
    },

    /// Its approval deadline passed.
    #[error("this command proposal's approval deadline has passed")]
    ProposalExpired,

    /// The approver's role isn't the one the command type requires.
    #[error("this command type must be approved by a Role named {required:?}")]
    InsufficientApprovalRole {
        /// The role name required.
        required: String,
    },

    /// An approval deadline of zero or fewer seconds.
    #[error("an approval policy's deadline must be a positive number of seconds")]
    InvalidApprovalPolicy,

    /// The proposer tried to approve their own command.
    #[error("the account that proposed this command may not approve it")]
    SelfApprovalNotAllowed,

    /// A subject the proposal's sensitive fields are encrypted for was
    /// forgotten, so its payload can no longer be read; approving it
    /// recorded it as forgotten (docs/architecture.md §221).
    #[error(
        "a subject this command proposal names was forgotten, so it can no longer be approved"
    )]
    ProposalSubjectForgotten,
}

impl Error {
    /// The stable code served for it, e.g. `command_proposal_not_found`.
    pub fn code(&self) -> &'static str {
        match self {
            Error::ProposalNotFound { .. } => "command_proposal_not_found",
            Error::ProposalNotPending { .. } => "command_proposal_not_pending",
            Error::ProposalExpired => "command_proposal_expired",
            Error::InsufficientApprovalRole { .. } => "insufficient_approval_role",
            Error::InvalidApprovalPolicy => "invalid_approval_policy",
            Error::SelfApprovalNotAllowed => "self_approval_not_allowed",
            Error::ProposalSubjectForgotten => "command_proposal_subject_forgotten",
        }
    }
}

impl crate::error::SkiljRejection for Error {
    fn code(&self) -> &str {
        Error::code(self)
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access_control::Role;

    fn role(id: &str, subject: &str) -> Role {
        Role {
            id: id.to_string(),
            external_subject: subject.to_string(),
            name: "Admin".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: chrono::DateTime::<chrono::Utc>::MIN_UTC,
            revoked_at: None,
        }
    }

    fn mapping(role: Role) -> RoleAccessMapping {
        RoleAccessMapping {
            role,
            bounded_context: crate::event_store::BoundedContext {
                name: "banking".to_string(),
                status: crate::event_store::BoundedContextStatus::Active,
                created_at: chrono::DateTime::<chrono::Utc>::MIN_UTC,
                created_by: crate::bootstrap::ContextCreator::SystemCreator,
                template: None,
            },
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            capabilities: Default::default(),
            delegable: Default::default(),
            status: RoleStatus::Active,
            created_at: chrono::DateTime::<chrono::Utc>::MIN_UTC,
            revoked_at: None,
        }
    }

    #[test]
    fn self_is_the_subject_not_the_role_row() {
        let policy = CommandApprovalPolicy::new(None, None);
        let approver = mapping(role("role-new", "alice"));
        // Same human under a fresh Role id: still refused.
        let proposer = role("role-old", "alice");
        assert!(check_self_approval(&policy, Some(&proposer), &approver).is_err());
        // A different human: allowed.
        let stranger = role("role-other", "bob");
        assert!(check_self_approval(&policy, Some(&stranger), &approver).is_ok());
        // No Role behind the proposal (REST token, deadline, route):
        // allowed.
        assert!(check_self_approval(&policy, None, &approver).is_ok());
        // Opt-in: the proposer themself may approve.
        let opt_in = CommandApprovalPolicy::new(None, None).allowing_self_approval(true);
        assert!(check_self_approval(&opt_in, Some(&role("role-new", "alice")), &approver).is_ok());
    }
}
