//! A worked example, built on the `skilj` facade crate exactly the way an
//! application would use it - not part of the library, and not reachable
//! from `skilj`/`skilj-core` in the other direction. Two bounded
//! contexts, chosen to show two different points on the same spectrum:
//!
//! - [`banking`]: withdrawing money only ever needs *one* account's own
//!   history to decide against. Two different accounts never share a
//!   tag, so their commands never contend, retry, or coordinate with
//!   each other at all - the case classic per-aggregate event sourcing
//!   (and `skilj`) both handle the same, easy way.
//! - [`courses`]: enrolling a student needs *two* facts checked
//!   atomically in one decision - "does this course still have a free
//!   place?" and "is this student still under their own course limit?"
//!   Those two facts live on what a classic one-stream-per-aggregate
//!   event store would call two different aggregates. There, this
//!   needs a saga or process manager: reserve a seat on the course
//!   aggregate, then check the student aggregate, then compensate if
//!   either step fails partway. Here, `EnrollStudentInCourse` just
//!   declares `tag_mappings` for both `student` and `course`;
//!   `decide()` receives the union of both histories in one
//!   synchronous call, and the bounded context's own commit lock
//!   (`skilj-core::db::submit_command`'s optimistic-then-locked retry -
//!   see docs/architecture.md §1.7/§2.2.2) makes the whole check-and-
//!   emit atomic with no saga at all. `skilj-demo/tests/courses.rs`
//!   proves this under real concurrency, not just logically.
//!
//! Every event/command/projection type in both modules is
//! `#[skilj::auto_register(BOUNDED_CONTEXT)]`-tagged (docs/architecture.md
//! §1.3.3), scoped to its own bounded context by that one macro argument -
//! each module declares its own `pub const BOUNDED_CONTEXT` exactly once,
//! at the top of the file, and the argument just points back at it; no
//! per-type `const BOUNDED_CONTEXT = ...` override anywhere - see
//! [`register`] below, `src/bin/server.rs` (a real runnable server), and
//! `tests/banking.rs`/`tests/courses.rs` (the integration-test suite that
//! doubles as this crate's own proof it behaves as documented).

pub mod banking;
pub mod courses;

/// Registers both bounded contexts onto `builder` in one call - every
/// `#[auto_register]`-tagged type in this crate finds its own bounded
/// context via its own `BOUNDED_CONTEXT` override, so there's nothing
/// left for this function to do per-module the way it used to (chaining
/// `banking::register`/`courses::register`, each scoping the builder by
/// hand via `.bounded_context(...)`).
pub fn register(builder: skilj::SkiljBuilder) -> skilj::SkiljBuilder {
    builder.auto_register()
}
