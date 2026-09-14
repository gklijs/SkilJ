//! A given/when/then test harness for a skilj plugin author's own
//! `CommandType::decide()`/`Projection::project()` - see docs/architecture.md
//! §45 (Codeberg issue #19).
//!
//! **What this is not**: a way to test skilj's own engine. Computing a
//! command's own consistency tags from its payload, and filtering a
//! bounded context's full event history down to the `matching_events`
//! slice `decide()` actually sees, is real machinery
//! (`skilj_core::event_store::consistency_boundary_and_matching_events`)
//! that already has its own coverage, end to end, in `skilj-core`'s and
//! `skilj`'s own integration test suites (real Postgres, real DCB conflict
//! handling, real access control). Re-deriving that here, from a plugin
//! author's own test file, would just be a second, slower copy of those
//! same tests.
//!
//! What plugin authors actually need to test is narrower: *given* their
//! own domain events, does their own `decide()`/`project()` produce the
//! right decision/state? Both of those functions already take exactly the
//! typed slice this crate's "given events" are - `&[T::Event]`, the
//! bounded context's own generated event enum - so there's no raw
//! `Event`/tag/boundary plumbing to reconstruct at all: `GivenEvents` just
//! calls `decide()`/`project()` directly, in-process, with no database and
//! no HTTP round trip.
//!
//! ```rust,ignore
//! // AccountEvent is banking.rs's own generated bounded-context event enum
//! // (skilj-codegen, §17) - `.event(...)` takes it directly, the exact
//! // type `WithdrawMoney::decide()` itself is handed.
//! GivenEvents::<WithdrawMoney>::new()
//!     .event(AccountEvent::MoneyDeposited(MoneyDepositedPayload { account_id: "a".into(), amount: 100 }))
//!     .when(WithdrawMoneyPayload { account_id: "a".into(), amount: 150 })
//!     .then_rejected("insufficient_funds");
//! ```
//!
//! See `skilj-demo/tests/banking_fixture.rs` for this against real,
//! codegen'd `CommandType`/`Projection` impls.

pub mod command;
pub mod projection;

/// Shared by both `command::CommandOutcome` and `projection`'s state
/// assertion: renders a value as pretty-printed JSON for a mismatch
/// panic message, falling back to `Debug` on the (unreachable in
/// practice - every value here already round-tripped through
/// `serde_json`) chance serialization itself fails.
pub(crate) fn pretty(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| format!("{value:?}"))
}
