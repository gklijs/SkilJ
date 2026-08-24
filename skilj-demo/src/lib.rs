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
//! Both modules expose a `register(builder) -> builder` that chains their
//! own `.bounded_context(...)` onto a `skilj::SkiljBuilder` - see
//! [`register`] below, `src/bin/server.rs` (a real runnable server), and
//! `tests/banking.rs`/`tests/courses.rs` (the integration-test suite that
//! doubles as this crate's own proof it behaves as documented).

pub mod banking;
pub mod courses;

/// Registers both bounded contexts onto `builder`, in one place, so the
/// runnable server and the test suite can't drift from each other on
/// what gets registered - see each module's own `register()` for the
/// per-context detail.
pub fn register(builder: skilj::SkiljBuilder) -> skilj::SkiljBuilder {
    courses::register(banking::register(builder))
}
