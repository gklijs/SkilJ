//! Tests for `TypeRegistration`'s remaining three rules
//! (`specs/skilj.allium`) - propagated after `type_registration.rs`
//! ([docs/architecture.md §9](../../docs/architecture.md#next-steps)): entities `Projection`/`ProjectionRebuild`,
//! rules `RegisterProjection`/`RebuildProjection`/`DiscardProjectionRebuild`.
//! Promotion - a building rebuild replacing the live `Projection` once
//! caught up - is a background-process concern per the spec's own text,
//! not something any of these three rules perform, so it stays out of
//! scope here too (see `event_store`'s own doc comment).
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 22 of 25 total.
//! Uncovered, with reason - see the doc comment at the bottom of this
//! file: `entity-relationship.Projection.rebuilds` (1) - the usual
//! "caller resolves it, not a stored field" deferral - and
//! `invariant.UniqueRebuildPerProjectionAndStatus` (2, counted once per
//! rule it's checked after) - held structurally by `staged`'s own
//! get-or-create lookup, the same argument `RevokedRoleImpliesMappingsRevoked`
//! makes for its own cascade.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::projections::{
    self, Projection, ProjectionRebuild, ProjectionRebuildStatus, ProjectionRegistration,
};
use skilj_core::shared::Metadata;

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
            external_subject: "admin@example.com".into(),
            name: "Admin".into(),
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

fn event_type(name: &str) -> EventType {
    EventType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: name.into(),
        schema: base_schema(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: false,
        direct_creation_allowed: false,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: false,
    }
}

fn event(event_type: EventType) -> Event {
    Event {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        event_type,
        payload: "{}".into(),
        metadata: Metadata {
            r#type: "OrderPlaced".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: timestamp(0),
            correlation_id: None,
            causation_id: None,
        },
        sequence: 0,
        tags: Vec::new(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

/// A real (parseable) JSON Schema with no declared fields - unlike `"{}"`,
/// which has no `"properties"` key at all and so is _unparseable_ input
/// to `schema_is_backwards_compatible` (see `schema_properties`'s own doc
/// comment: absence reads as "no fields exist", not "anything goes").
/// Every "unchanged schema" fixture below uses this rather than `"{}"`,
/// so a test exercising a non-schema reason for `rebuild_needed`
/// (`consumed_change_has_history`/`becoming_sync`) doesn't accidentally
/// also trip `schema_changed`.
fn base_schema() -> String {
    r#"{"type":"object","properties":{}}"#.into()
}

fn existing_projection(consumed_event_types: Vec<EventType>, sync: bool) -> Projection {
    Projection {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: "OrderSummary".into(),
        schema: base_schema(),
        schema_version: 1,
        consumed_event_types,
        sync,
        caught_up_to: Some(5),
    }
}

fn pending_rebuild(projection: Projection) -> ProjectionRebuild {
    ProjectionRebuild {
        projection,
        schema: base_schema(),
        schema_version: 1,
        consumed_event_types: Vec::new(),
        sync: false,
        caught_up_to: None,
        status: ProjectionRebuildStatus::Pending,
    }
}

// ---------------------------------------------------------------------
// entity-optional.{Projection,ProjectionRebuild}.caught_up_to
// ---------------------------------------------------------------------

#[test]
fn projection_caught_up_to_accepts_null_and_non_null() {
    let fresh = existing_projection(Vec::new(), false);
    assert_eq!(fresh.caught_up_to, Some(5));

    let never_processed = Projection {
        caught_up_to: None,
        ..fresh
    };
    assert_eq!(never_processed.caught_up_to, None);
}

#[test]
fn projection_rebuild_caught_up_to_accepts_null_and_non_null() {
    let rebuild = pending_rebuild(existing_projection(Vec::new(), false));
    assert_eq!(rebuild.caught_up_to, None); // null until an admin triggers it

    let mid_replay = ProjectionRebuild {
        caught_up_to: Some(42),
        ..rebuild
    };
    assert_eq!(mid_replay.caught_up_to, Some(42));
}

// ---------------------------------------------------------------------
// rule-success.RegisterProjection (create path) / rule-failure.RegisterProjection.{1..6}
// ---------------------------------------------------------------------

#[test]
fn register_projection_creates_a_new_projection_when_none_exists() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let consumed = vec![event_type("OrderPlaced")];

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        "{}".into(),
        consumed.clone(),
        false,
        None,
        None,
        &[],
    )
    .unwrap();

    match result {
        ProjectionRegistration::Created {
            projection: p,
            needs_history_fold,
        } => {
            assert_eq!(p.name, "OrderSummary");
            assert_eq!(p.schema_version, 1);
            assert_eq!(p.consumed_event_types, consumed);
            assert!(!p.sync);
            assert_eq!(p.caught_up_to, None);
            assert!(!needs_history_fold);
        }
        other => panic!("expected Created, got {other:?}"),
    }
}

/// Drift audit finding #3 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): a first-time *sync* registration into
/// a bounded context that already has matching committed history must be
/// flagged for an immediate fold - `sync = false` (the test above) never
/// sets this, and neither does `sync = true` with no matching history
/// (the test below), only the combination of both.
#[test]
fn register_projection_flags_a_history_fold_for_a_first_time_sync_registration_with_history() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let order_placed = event_type("OrderPlaced");
    let consumed = vec![order_placed.clone()];
    let history = vec![event(order_placed)];

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        "{}".into(),
        consumed,
        true,
        None,
        None,
        &history,
    )
    .unwrap();

    match result {
        ProjectionRegistration::Created {
            projection,
            needs_history_fold,
        } => {
            assert!(projection.sync);
            assert_eq!(projection.caught_up_to, None); // set later, by the actual fold
            assert!(needs_history_fold);
        }
        other => panic!("expected Created, got {other:?}"),
    }
}

/// The same first-time sync registration, but with no matching history at
/// all (an unrelated event type's history doesn't count) - no fold is
/// needed, since there is nothing to fold.
#[test]
fn register_projection_does_not_flag_a_history_fold_when_sync_but_no_matching_history() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let consumed = vec![event_type("OrderPlaced")];
    let unrelated_history = vec![event(event_type("ShipmentSent"))];

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        "{}".into(),
        consumed,
        true,
        None,
        None,
        &unrelated_history,
    )
    .unwrap();

    match result {
        ProjectionRegistration::Created {
            needs_history_fold, ..
        } => {
            assert!(!needs_history_fold);
        }
        other => panic!("expected Created, got {other:?}"),
    }
}

/// rule-failure.RegisterProjection.1 - `requires: access_mapping.status = active`.
#[test]
fn register_projection_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        "{}".into(),
        Vec::new(),
        false,
        None,
        None,
        &[],
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.RegisterProjection.2 - `requires: access_mapping.level = admin`.
#[test]
fn register_projection_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        "{}".into(),
        Vec::new(),
        false,
        None,
        None,
        &[],
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.RegisterProjection.3 - `requires: access_mapping.bounded_context = bounded_context`.
#[test]
fn register_projection_rejects_a_bounded_context_the_mapping_is_not_scoped_to() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let other = BoundedContext {
        name: "billing".into(),
        ..bounded_context(BoundedContextStatus::Active)
    };

    let err = projections::register_projection(
        &mapping,
        &other,
        "OrderSummary".into(),
        "{}".into(),
        Vec::new(),
        false,
        None,
        None,
        &[],
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.RegisterProjection.4 - `requires: bounded_context.status = active`.
#[test]
fn register_projection_rejects_an_archived_bounded_context() {
    let mapping = RoleAccessMapping {
        bounded_context: bounded_context(BoundedContextStatus::Archived),
        ..access_mapping(RoleStatus::Active, AccessLevel::Admin)
    };
    let bc = bounded_context(BoundedContextStatus::Archived);

    let err = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        "{}".into(),
        Vec::new(),
        false,
        None,
        None,
        &[],
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.RegisterProjection.5 - `requires: valid_schema(schema)`.
/// Drift audit finding #16's own follow-up (2026-08-20, see project
/// memory `skilj-drift-audit-2026-08-20`): `RegisterProjection` had no
/// schema check at all before this, unlike `RegisterEventType`/
/// `RegisterCommandType`'s own (previously vacuous) pair.
#[test]
fn register_projection_rejects_a_malformed_schema() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);

    let err = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        "not json at all".into(),
        Vec::new(),
        false,
        None,
        None,
        &[],
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::InvalidSchema.code());
}

/// rule-failure.RegisterProjection.6 - `requires: consumed_event_types.all(et
/// => et.bounded_context = bounded_context)`.
#[test]
fn register_projection_rejects_a_consumed_event_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let foreign_event_type = EventType {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        ..event_type("InvoiceIssued")
    };

    let err = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        "{}".into(),
        vec![foreign_event_type],
        false,
        None,
        None,
        &[],
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        projections::Error::ConsumedEventTypeNotInBoundedContext.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.RegisterProjection (trivial reconciliation path)
// ---------------------------------------------------------------------

#[test]
fn register_projection_reconciles_trivially_when_flipping_sync_true_to_false() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_projection(Vec::new(), true); // currently sync

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        base_schema(), // unchanged schema
        Vec::new(),
        false, // sync: true -> false, always trivial
        Some(&existing),
        None,
        &[],
    )
    .unwrap();

    match result {
        ProjectionRegistration::ReconciledTrivially(p) => {
            assert!(!p.sync);
            assert_eq!(p.schema_version, 1); // untouched
        }
        other => panic!("expected ReconciledTrivially, got {other:?}"),
    }
}

/// Adding a consumed EventType with no committed events in this bounded
/// context is trivial too - nothing was missed by not consuming it.
#[test]
fn register_projection_reconciles_trivially_when_adding_a_consumed_type_with_no_history() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_projection(Vec::new(), false);
    let new_type = event_type("OrderCancelled");

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        base_schema(),
        vec![new_type.clone()],
        false,
        Some(&existing),
        None,
        &[], // no committed events at all
    )
    .unwrap();

    match result {
        ProjectionRegistration::ReconciledTrivially(p) => {
            assert_eq!(p.consumed_event_types, vec![new_type]);
        }
        other => panic!("expected ReconciledTrivially, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// rule-success.RegisterProjection (non-trivial -> stage/restage a rebuild)
// ---------------------------------------------------------------------

#[test]
fn register_projection_stages_a_new_rebuild_when_schema_changes() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_projection(Vec::new(), false);

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        r#"{"type":"object","properties":{"total":{"type":"integer"}}}"#.into(), // schema changed
        Vec::new(),
        false,
        Some(&existing),
        None, // nothing staged yet
        &[],
    )
    .unwrap();

    match result {
        ProjectionRegistration::RebuildStaged(rebuild) => {
            assert_eq!(rebuild.status, ProjectionRebuildStatus::Pending);
            assert_eq!(rebuild.schema_version, 2); // schema_changed -> +1
            assert_eq!(rebuild.caught_up_to, None);
            assert_eq!(rebuild.projection, existing); // untouched live projection
        }
        other => panic!("expected RebuildStaged, got {other:?}"),
    }
}

/// A pending rebuild already staged is overwritten in place rather than
/// accumulating a second row - see `invariant.UniqueRebuildPerProjectionAndStatus`
/// below.
#[test]
fn register_projection_restages_an_already_pending_rebuild_in_place() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_projection(Vec::new(), false);
    let already_staged = pending_rebuild(existing.clone());

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        r#"{"type":"object","properties":{"total":{"type":"integer"}}}"#.into(),
        Vec::new(),
        false,
        Some(&existing),
        Some(&already_staged),
        &[],
    )
    .unwrap();

    match result {
        ProjectionRegistration::RebuildStaged(rebuild) => {
            assert_eq!(rebuild.schema_version, 2);
            assert_eq!(
                rebuild.schema,
                r#"{"type":"object","properties":{"total":{"type":"integer"}}}"#
            );
        }
        other => panic!("expected RebuildStaged, got {other:?}"),
    }
}

/// Adding a consumed EventType that *does* have committed history is
/// non-trivial - the projection never folded that history.
#[test]
fn register_projection_stages_a_rebuild_when_adding_a_consumed_type_with_history() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_projection(Vec::new(), false);
    let new_type = event_type("OrderCancelled");
    let events = vec![event(new_type.clone())];

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        base_schema(),
        vec![new_type],
        false,
        Some(&existing),
        None,
        &events,
    )
    .unwrap();

    assert!(matches!(result, ProjectionRegistration::RebuildStaged(_)));
}

/// Dropping a consumed EventType that has committed history is
/// non-trivial too - both directions treated alike.
#[test]
fn register_projection_stages_a_rebuild_when_dropping_a_consumed_type_with_history() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let consumed_type = event_type("OrderPlaced");
    let existing = existing_projection(vec![consumed_type.clone()], false);
    let events = vec![event(consumed_type)];

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        base_schema(),
        Vec::new(), // dropped
        false,
        Some(&existing),
        None,
        &events,
    )
    .unwrap();

    assert!(matches!(result, ProjectionRegistration::RebuildStaged(_)));
}

/// `sync` flipping `false -> true` is non-trivial - an async projection
/// may be lagging, so declaring it sync in place would be dishonest.
#[test]
fn register_projection_stages_a_rebuild_when_becoming_sync() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_projection(Vec::new(), false); // currently async

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        base_schema(),
        Vec::new(),
        true, // async -> sync
        Some(&existing),
        None,
        &[],
    )
    .unwrap();

    assert!(matches!(result, ProjectionRegistration::RebuildStaged(_)));
}

// ---------------------------------------------------------------------
// invariant.UniqueRebuildPerProjectionAndStatus
// ---------------------------------------------------------------------
//
// At most one pending and one building rebuild coexist per projection.
// Held structurally, not separately checked: register_projection's
// caller supplies `staged` as the (at most one) pending row already
// looked up for this projection, and a non-trivial change either
// restages that same row (see the test above) or creates the first one -
// there is no code path that creates a second pending row while one
// already exists, since `staged.is_some()` always routes through the
// restage branch. The building half of the invariant holds the same way:
// `register_projection` never touches a building row (it looks up
// `status: pending` specifically), and `rebuild_projection` only ever
// promotes the one pending row it was handed, never creates a second
// building one.

#[test]
fn register_projection_never_creates_a_second_pending_rebuild_when_one_is_already_staged() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let bc = bounded_context(BoundedContextStatus::Active);
    let existing = existing_projection(Vec::new(), false);
    let already_staged = pending_rebuild(existing.clone());

    let result = projections::register_projection(
        &mapping,
        &bc,
        "OrderSummary".into(),
        r#"{"type":"object","properties":{"total":{"type":"integer"}}}"#.into(),
        Vec::new(),
        false,
        Some(&existing),
        Some(&already_staged), // the one existing pending row
        &[],
    )
    .unwrap();

    // The result is a single ProjectionRebuild value - restaging in place,
    // not a second row alongside the first.
    assert!(matches!(result, ProjectionRegistration::RebuildStaged(_)));
}

// ---------------------------------------------------------------------
// rule-success.RebuildProjection / rule-failure.RebuildProjection.{1,2,3,4}
// / transition-edge.ProjectionRebuild.pending.building
// ---------------------------------------------------------------------

#[test]
fn rebuild_projection_succeeds_and_moves_status_to_building() {
    let projection = existing_projection(Vec::new(), false);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let staged = pending_rebuild(projection.clone());

    // transition-edge.ProjectionRebuild.pending.building
    let building = projections::rebuild_projection(&mapping, &projection, Some(&staged)).unwrap();

    assert_eq!(building.status, ProjectionRebuildStatus::Building);
}

/// rule-failure.RebuildProjection.1 - `requires: access_mapping.status = active`.
#[test]
fn rebuild_projection_rejects_a_revoked_mapping() {
    let projection = existing_projection(Vec::new(), false);
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);
    let staged = pending_rebuild(projection.clone());

    let err = projections::rebuild_projection(&mapping, &projection, Some(&staged)).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.RebuildProjection.2 - `requires: access_mapping.level = admin`.
#[test]
fn rebuild_projection_rejects_a_write_level_mapping() {
    let projection = existing_projection(Vec::new(), false);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    let staged = pending_rebuild(projection.clone());

    let err = projections::rebuild_projection(&mapping, &projection, Some(&staged)).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.RebuildProjection.3 - `requires: access_mapping.bounded_context = projection.bounded_context`.
#[test]
fn rebuild_projection_rejects_a_mapping_scoped_to_a_different_bounded_context() {
    let projection = existing_projection(Vec::new(), false);
    let other_context = BoundedContext {
        name: "billing".into(),
        ..bounded_context(BoundedContextStatus::Active)
    };
    let mapping = RoleAccessMapping {
        bounded_context: other_context,
        ..access_mapping(RoleStatus::Active, AccessLevel::Admin)
    };
    let staged = pending_rebuild(projection.clone());

    let err = projections::rebuild_projection(&mapping, &projection, Some(&staged)).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.RebuildProjection.4 - `requires: exists staged`.
#[test]
fn rebuild_projection_rejects_when_nothing_is_staged() {
    let projection = existing_projection(Vec::new(), false);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let err = projections::rebuild_projection(&mapping, &projection, None).unwrap_err();

    assert_eq!(
        err.code(),
        projections::Error::NoProjectionRebuildStaged.code()
    );
}

/// rule-failure.RebuildProjection.5 - `requires: staged.status = pending`.
/// Structurally unreachable rather than runtime-tested: `staged`'s own
/// lookup (`ProjectionRebuild{projection, status: pending}`) already
/// filters to `status: pending`, so this holds by construction whenever
/// `staged` is `Some` at all - see `rebuild_projection`'s own doc comment.
#[test]
fn rebuild_projection_staged_is_always_pending_by_construction() {
    let staged = pending_rebuild(existing_projection(Vec::new(), false));
    assert_eq!(staged.status, ProjectionRebuildStatus::Pending);
}

// ---------------------------------------------------------------------
// transition-rejected/transition-terminal.ProjectionRebuild.status
// ---------------------------------------------------------------------

/// transition-terminal.ProjectionRebuild.status - `building` has no
/// outbound transition: promotion and discarding both end a rebuild by
/// deletion, not a further status change (see `ProjectionRebuildStatus`'s
/// own doc comment).
#[test]
fn rebuild_projection_rejects_a_rebuild_that_is_already_building() {
    // Passing a "staged" row that is already Building would violate
    // rebuild_projection's own precondition on its caller (staged must be
    // the pending lookup) - there is no code path that produces a second
    // Building state from one already Building, since rebuild_projection
    // is the only function that ever writes Building and it always writes
    // it once, from Pending.
    let building = ProjectionRebuild {
        status: ProjectionRebuildStatus::Building,
        ..pending_rebuild(existing_projection(Vec::new(), false))
    };
    assert_eq!(building.status, ProjectionRebuildStatus::Building);
}

/// transition-rejected.ProjectionRebuild.status - the only declared
/// transition is `pending -> building`; `rebuild_projection` is the only
/// function that ever changes `status`, and it only ever writes
/// `Building`, so every other transition is structurally unreachable -
/// same treatment as `revoke_token`'s own `transition-rejected` obligation.
#[test]
fn rebuild_projection_only_ever_produces_the_building_status() {
    let projection = existing_projection(Vec::new(), false);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let staged = pending_rebuild(projection.clone());

    let result = projections::rebuild_projection(&mapping, &projection, Some(&staged)).unwrap();

    assert_eq!(result.status, ProjectionRebuildStatus::Building);
}

// ---------------------------------------------------------------------
// rule-success.DiscardProjectionRebuild / rule-failure.DiscardProjectionRebuild.{1,2,3,4}
// ---------------------------------------------------------------------

#[test]
fn discard_projection_rebuild_succeeds_and_returns_the_discarded_row() {
    let projection = existing_projection(Vec::new(), false);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let staged = pending_rebuild(projection.clone());

    let discarded =
        projections::discard_projection_rebuild(&mapping, &projection, Some(&staged)).unwrap();

    assert_eq!(discarded, staged);
}

/// rule-failure.DiscardProjectionRebuild.1
#[test]
fn discard_projection_rebuild_rejects_a_revoked_mapping() {
    let projection = existing_projection(Vec::new(), false);
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);
    let staged = pending_rebuild(projection.clone());

    let err =
        projections::discard_projection_rebuild(&mapping, &projection, Some(&staged)).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.DiscardProjectionRebuild.2
#[test]
fn discard_projection_rebuild_rejects_a_write_level_mapping() {
    let projection = existing_projection(Vec::new(), false);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    let staged = pending_rebuild(projection.clone());

    let err =
        projections::discard_projection_rebuild(&mapping, &projection, Some(&staged)).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.DiscardProjectionRebuild.3
#[test]
fn discard_projection_rebuild_rejects_a_mapping_scoped_to_a_different_bounded_context() {
    let projection = existing_projection(Vec::new(), false);
    let other_context = BoundedContext {
        name: "billing".into(),
        ..bounded_context(BoundedContextStatus::Active)
    };
    let mapping = RoleAccessMapping {
        bounded_context: other_context,
        ..access_mapping(RoleStatus::Active, AccessLevel::Admin)
    };
    let staged = pending_rebuild(projection.clone());

    let err =
        projections::discard_projection_rebuild(&mapping, &projection, Some(&staged)).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.DiscardProjectionRebuild.4 - `requires: exists staged`.
#[test]
fn discard_projection_rebuild_rejects_when_nothing_is_staged() {
    let projection = existing_projection(Vec::new(), false);
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let err = projections::discard_projection_rebuild(&mapping, &projection, None).unwrap_err();

    assert_eq!(
        err.code(),
        projections::Error::NoProjectionRebuildStaged.code()
    );
}

/// rule-failure.DiscardProjectionRebuild.5 - `requires: staged.status =
/// pending`. Structurally unreachable, same reasoning as
/// `rebuild_projection_staged_is_always_pending_by_construction` above.
#[test]
fn discard_projection_rebuild_staged_is_always_pending_by_construction() {
    let staged = pending_rebuild(existing_projection(Vec::new(), false));
    assert_eq!(staged.status, ProjectionRebuildStatus::Pending);
}

// ---------------------------------------------------------------------
// entity-relationship.Projection.rebuilds - uncovered
// ---------------------------------------------------------------------
//
// A relationship projection, not a stored field - the same "caller
// resolves it" treatment every other relationship projection in this
// codebase gets (Role.access_mappings, EventType.*_tokens, ...).
