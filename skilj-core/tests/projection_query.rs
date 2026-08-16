//! Tests for the `ProjectionQuery` surface (`specs/skilj.allium`) -
//! propagated after `command_query.rs` (docs/architecture.md §9): rule
//! `QueryProjection`. This is the last of the three admin/read-facing
//! query surfaces (alongside `EventQuery`/`CommandQuery`) - unlike the
//! other two, `read_projection`/`await_projection_caught_up` are both
//! fully caller-supplied here rather than getting a trivial/deferred
//! split, since neither has an empty-case fallback to implement for real
//! (see `query_projection`'s own doc comment for why).
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 4 of 7 total.
//! Uncovered, with reason - see the doc comment at the bottom of this
//! file: `surface-actor`/`surface-exposure`/`surface-provides.ProjectionQuery`
//! (3) - the usual GraphQL-scaffolding gap.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::projections::{self, Projection};

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn bounded_context(status: BoundedContextStatus) -> BoundedContext {
    BoundedContext {
        name: "orders".into(),
        status,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
    }
}

fn access_mapping(status: RoleStatus, level: AccessLevel) -> RoleAccessMapping {
    RoleAccessMapping {
        role: Role {
            id: "role-1".into(),
            external_subject: "someone@example.com".into(),
            name: "Someone".into(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: timestamp(0),
            revoked_at: None,
        },
        bounded_context: bounded_context(BoundedContextStatus::Active),
        level,
        can_read_sensitive: false,
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn projection() -> Projection {
    Projection {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: "OrderSummary".into(),
        schema: "{}".into(),
        schema_version: 1,
        consumed_event_types: Vec::new(),
        sync: false,
        caught_up_to: Some(5),
    }
}

// ---------------------------------------------------------------------
// rule-success.QueryProjection / rule-failure.QueryProjection.{1,2,3}
// ---------------------------------------------------------------------

#[test]
fn query_projection_succeeds_and_returns_the_supplied_result_when_no_sequence_is_requested() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);
    let p = projection();

    let result = projections::query_projection(
        &mapping,
        &p,
        None,  // no wait_for_sequence
        false, // caught_up is irrelevant when nothing was requested
        r#"{"total":42}"#.into(),
    )
    .unwrap();

    assert_eq!(result, r#"{"total":42}"#);
}

/// A sync projection satisfies any sequence immediately, by construction
/// - reflected here in the caller-supplied `caught_up`, not re-derived.
#[test]
fn query_projection_succeeds_when_caught_up_to_the_requested_sequence() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    let p = projection();

    let result = projections::query_projection(
        &mapping,
        &p,
        Some(10),
        true, // await_projection_caught_up already resolved true
        r#"{"total":99}"#.into(),
    )
    .unwrap();

    assert_eq!(result, r#"{"total":99}"#);
}

/// Read level is enough - write and admin grants include it, since they
/// can already do strictly more (see the surface's own guarantee).
#[test]
fn query_projection_succeeds_for_every_access_level() {
    for level in [AccessLevel::Read, AccessLevel::Write, AccessLevel::Admin] {
        let mapping = access_mapping(RoleStatus::Active, level);
        let p = projection();

        let result = projections::query_projection(&mapping, &p, None, false, "ok".into()).unwrap();

        assert_eq!(result, "ok");
    }
}

/// rule-failure.QueryProjection.1 - `requires: access_mapping.status = active`.
#[test]
fn query_projection_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Read);
    let p = projection();

    let err = projections::query_projection(&mapping, &p, None, false, "x".into()).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.QueryProjection.2 - `requires: access_mapping.bounded_context = projection.bounded_context`.
#[test]
fn query_projection_rejects_a_mapping_scoped_to_a_different_bounded_context() {
    let mapping = RoleAccessMapping {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        ..access_mapping(RoleStatus::Active, AccessLevel::Read)
    };
    let p = projection();

    let err = projections::query_projection(&mapping, &p, None, false, "x".into()).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.QueryProjection.3 - `requires: wait_for_sequence = null or
/// await_projection_caught_up(projection, wait_for_sequence)`. Reported as
/// a timeout specifically, distinguishable from every other rejection
/// this rule can produce (see `ReadYourWritesWhenRequested`).
#[test]
fn query_projection_rejects_with_a_distinguishable_timeout_when_not_caught_up_in_time() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);
    let p = projection();

    let err = projections::query_projection(
        &mapping,
        &p,
        Some(10),
        false, // await_projection_caught_up resolved false (timed out)
        "x".into(),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        projections::Error::ProjectionCaughtUpTimedOut.code()
    );
}

/// A query naming no sequence makes no read-your-writes promise and
/// never times out, regardless of `caught_up` - the check is vacuous
/// when `wait_for_sequence` is `None`.
#[test]
fn query_projection_never_times_out_when_no_sequence_was_requested() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);
    let p = projection();

    let result =
        projections::query_projection(&mapping, &p, None, false, "whatever".into()).unwrap();

    assert_eq!(result, "whatever");
}

// ---------------------------------------------------------------------
// surface-actor/surface-exposure/surface-provides.ProjectionQuery - uncovered
// ---------------------------------------------------------------------
//
// GraphQL-scaffolding gap, same as every prior surface's deferred set
// (see command_processing.rs's header comment) - no resolver/schema
// wiring in skilj-graphql yet.
