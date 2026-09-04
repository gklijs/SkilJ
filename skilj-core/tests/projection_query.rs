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
//! filtered to this pass's source constructs): 5 of 8 total -
//! `rule-failure.QueryProjection.4` (cross-tenant projection read fix,
//! docs/architecture.md's own write-up of this pass) added alongside the
//! original 4. Uncovered, with reason - see the doc comment at the
//! bottom of this file: `surface-actor`/`surface-exposure`/
//! `surface-provides.ProjectionQuery` (3) - the usual GraphQL-scaffolding
//! gap.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::projections::{self, Projection, ProjectionAccessScope};

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
        template: None,
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
        scope: None,
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

/// No owner dimension, no required team - the pre-issue-#17 baseline
/// every test in this file that isn't specifically about one of those
/// two checks exercises.
fn no_access_restriction() -> ProjectionAccessScope<'static> {
    ProjectionAccessScope {
        declares_owner: false,
        instance_owner: None,
        team_only: None,
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
        "",
        None,  // no wait_for_sequence
        false, // caught_up is irrelevant when nothing was requested
        no_access_restriction(),
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
        "",
        Some(10),
        true, // await_projection_caught_up already resolved true
        no_access_restriction(),
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

        let result = projections::query_projection(
            &mapping,
            &p,
            "",
            None,
            false,
            no_access_restriction(),
            "ok".into(),
        )
        .unwrap();

        assert_eq!(result, "ok");
    }
}

/// rule-failure.QueryProjection.1 - `requires: access_mapping.status = active`.
#[test]
fn query_projection_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Read);
    let p = projection();

    let err = projections::query_projection(
        &mapping,
        &p,
        "",
        None,
        false,
        no_access_restriction(),
        "x".into(),
    )
    .unwrap_err();

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

    let err = projections::query_projection(
        &mapping,
        &p,
        "",
        None,
        false,
        no_access_restriction(),
        "x".into(),
    )
    .unwrap_err();

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
        "",
        Some(10),
        false, // await_projection_caught_up resolved false (timed out)
        no_access_restriction(),
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

    let result = projections::query_projection(
        &mapping,
        &p,
        "",
        None,
        false,
        no_access_restriction(),
        "whatever".into(),
    )
    .unwrap();

    assert_eq!(result, "whatever");
}

// ---------------------------------------------------------------------
// rule-success.QueryProjection / rule-failure.QueryProjection.4 -
// `requires: owner_scope_satisfied(projection, instance_key, access_mapping)`
// (cross-tenant projection read fix, docs/architecture.md's own write-up
// of this pass)
// ---------------------------------------------------------------------

/// A grant with no `scope` is unrestricted, exactly as before this check
/// existed - regardless of whether the projection declares an owner
/// dimension or what the instance's own owner is.
#[test]
fn query_projection_succeeds_when_the_grant_has_no_scope() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Read);
    let p = projection();

    let result = projections::query_projection(
        &mapping,
        &p,
        "company-a",
        None,
        false,
        ProjectionAccessScope {
            declares_owner: true, // projection declares an owner dimension
            instance_owner: Some("company-b"),
            team_only: None,
        },
        "x".into(),
    )
    .unwrap();

    assert_eq!(result, "x");
}

/// A scoped grant querying a projection that declares no owner dimension
/// at all is unaffected by `scope` regardless of its value.
#[test]
fn query_projection_succeeds_when_the_projection_declares_no_owner_dimension() {
    let mapping = RoleAccessMapping {
        scope: Some("company-a".into()),
        ..access_mapping(RoleStatus::Active, AccessLevel::Read)
    };
    let p = projection();

    let result = projections::query_projection(
        &mapping,
        &p,
        "",
        None,
        false,
        no_access_restriction(), // no owner dimension declared
        "x".into(),
    )
    .unwrap();

    assert_eq!(result, "x");
}

/// A scoped grant querying an owner-declaring projection succeeds when
/// the instance's own derived owner matches.
#[test]
fn query_projection_succeeds_when_the_instance_owner_matches_the_grants_scope() {
    let mapping = RoleAccessMapping {
        scope: Some("company-a".into()),
        ..access_mapping(RoleStatus::Active, AccessLevel::Read)
    };
    let p = projection();

    let result = projections::query_projection(
        &mapping,
        &p,
        "ticket-1",
        None,
        false,
        ProjectionAccessScope {
            declares_owner: true,
            instance_owner: Some("company-a"),
            team_only: None,
        },
        "x".into(),
    )
    .unwrap();

    assert_eq!(result, "x");
}

/// rule-failure.QueryProjection.4 - a scoped grant querying an
/// owner-declaring projection is rejected when the instance's own
/// derived owner belongs to someone else. The concrete cross-tenant leak
/// this whole pass fixes: before it, `require_read_mapping`'s
/// any-active-mapping check was the only thing gating this query.
#[test]
fn query_projection_rejects_an_instance_owned_by_a_different_scope() {
    let mapping = RoleAccessMapping {
        scope: Some("company-a".into()),
        ..access_mapping(RoleStatus::Active, AccessLevel::Read)
    };
    let p = projection();

    let err = projections::query_projection(
        &mapping,
        &p,
        "ticket-1",
        None,
        false,
        ProjectionAccessScope {
            declares_owner: true,
            instance_owner: Some("company-b"),
            team_only: None,
        },
        "x".into(),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantScopeMismatch.code());
}

/// rule-failure.QueryProjection.4, the fail-closed half: an instance
/// nothing has established an owner for yet (a never-touched key, or a
/// row that predates the projection declaring an owner dimension) is
/// treated as unproven, not as "no conflict" - a scoped grant is
/// rejected exactly as it would be for a proven mismatch.
#[test]
fn query_projection_rejects_an_unestablished_owner_for_a_scoped_grant() {
    let mapping = RoleAccessMapping {
        scope: Some("company-a".into()),
        ..access_mapping(RoleStatus::Active, AccessLevel::Read)
    };
    let p = projection();

    let err = projections::query_projection(
        &mapping,
        &p,
        "ticket-1",
        None,
        false,
        ProjectionAccessScope {
            declares_owner: true,
            instance_owner: None, // no owner established yet
            team_only: None,
        },
        "x".into(),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantScopeMismatch.code());
}

// ---------------------------------------------------------------------
// surface-actor/surface-exposure/surface-provides.ProjectionQuery - uncovered
// ---------------------------------------------------------------------
//
// GraphQL-scaffolding gap, same as every prior surface's deferred set
// (see command_processing.rs's header comment) - no resolver/schema
// wiring in skilj-graphql yet.
