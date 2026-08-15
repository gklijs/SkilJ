//! `Projection`, `ProjectionRebuild`; `RegisterProjection`,
//! `RebuildProjection`, `DiscardProjectionRebuild`, `QueryProjection`;
//! `project()`, `read_projection()`, `await_projection_caught_up()`. See
//! docs/architecture.md §3.2.

use crate::error::SkiljRejection;

// TODO: entity Projection, entity ProjectionRebuild, and the rules
// listed above. `caught_up_to` on both entities is `i64` - see
// docs/architecture.md §2.2.1.

/// Library-level errors this module's own rules reject for.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("a projection may only consume EventTypes from its own bounded context")]
    EventTypeOutsideBoundedContext,

    #[error("this projection already has a rebuild pending or building")]
    RebuildAlreadyStaged,

    #[error("the requested wait_for_sequence was not reached in time")]
    CaughtUpTimeout,
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::EventTypeOutsideBoundedContext => "event_type_outside_bounded_context",
            Error::RebuildAlreadyStaged => "rebuild_already_staged",
            Error::CaughtUpTimeout => "caught_up_timeout",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}
