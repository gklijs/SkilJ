//! `BootstrapSecret`, `ContextCreator`/`SystemCreator`; `CreateSuperadmin`,
//! `AddBoundedContext`, `ArchiveBoundedContext`, `ListBoundedContexts`;
//! the startup reconciliation loop (§1.5 - the reconciliation-loop entry
//! point and its authentication as a real, pre-existing Role both live
//! here). See docs/architecture.md §3.2.

use crate::error::SkiljRejection;

// TODO: entity BootstrapSecret, entity ContextCreator (+ SuperadminCreator
// / SystemCreator variants), the `admin`/`skilj` defaults, and the rules
// listed above - including the reconciliation loop itself
// (Skilj::builder().build(), per §1.5).

/// Library-level errors this module's own rules reject for.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("a superadmin Role already exists; the bootstrap secret can never be used again")]
    SuperadminAlreadyExists,

    #[error("the presented bootstrap secret doesn't match")]
    BootstrapSecretMismatch,

    #[error("a bounded context with this name already exists")]
    BoundedContextNameTaken,

    #[error("the admin bounded context can never be archived")]
    CannotArchiveAdminContext,

    #[error("this external_subject is already bound to another active Role")]
    ExternalSubjectAlreadyClaimed,
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::SuperadminAlreadyExists => "superadmin_already_exists",
            Error::BootstrapSecretMismatch => "bootstrap_secret_mismatch",
            Error::BoundedContextNameTaken => "bounded_context_name_taken",
            Error::CannotArchiveAdminContext => "cannot_archive_admin_context",
            Error::ExternalSubjectAlreadyClaimed => "external_subject_already_claimed",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}
