//! Command approvals (Codeberg issue #98): a command type can declare that
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
}

impl CommandApprovalPolicy {
    pub fn new(approver_role: Option<String>, deadline_seconds: Option<i64>) -> Self {
        Self {
            approver_role,
            deadline_seconds,
        }
    }
}

/// Where a proposal is in its lifecycle. `Approving` is the short-lived
/// claim an approver takes before running the submission pipeline, so two
/// concurrent approvals cannot both accept the same proposal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandProposalStatus {
    Pending,
    Approving,
    Approved,
    Rejected,
    Expired,
}

impl CommandProposalStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approving => "approving",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        Some(match value {
            "pending" => Self::Pending,
            "approving" => Self::Approving,
            "approved" => Self::Approved,
            "rejected" => Self::Rejected,
            "expired" => Self::Expired,
            _ => return None,
        })
    }
}

/// One submitted command held for approval. `payload` is stored encrypted
/// exactly as a real `Command.payload` is, and rendered the same way
/// (`render_command`) before it reaches a reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandProposal {
    pub id: String,
    pub bounded_context: crate::event_store::BoundedContext,
    pub command_type_name: String,
    pub payload: String,
    pub metadata: Metadata,
    pub status: CommandProposalStatus,
    pub idempotency_key: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub decided_at: Option<chrono::DateTime<chrono::Utc>>,
    pub decided_by_role_id: Option<String>,
    pub rejection_reason: Option<String>,
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("no command proposal with id {id:?} exists in this bounded context")]
    ProposalNotFound { id: String },

    #[error("this command proposal is {status}, not pending approval")]
    ProposalNotPending { status: &'static str },

    #[error("this command proposal's approval deadline has passed")]
    ProposalExpired,

    #[error("this command type must be approved by a Role named {required:?}")]
    InsufficientApprovalRole { required: String },

    #[error("an approval policy's deadline must be a positive number of seconds")]
    InvalidApprovalPolicy,
}

impl Error {
    pub fn code(&self) -> &'static str {
        match self {
            Error::ProposalNotFound { .. } => "command_proposal_not_found",
            Error::ProposalNotPending { .. } => "command_proposal_not_pending",
            Error::ProposalExpired => "command_proposal_expired",
            Error::InsufficientApprovalRole { .. } => "insufficient_approval_role",
            Error::InvalidApprovalPolicy => "invalid_approval_policy",
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
