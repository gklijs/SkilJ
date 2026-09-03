//! `Projection`, `ProjectionRebuild`; `RegisterProjection`,
//! `RebuildProjection`, `DiscardProjectionRebuild`, `QueryProjection`;
//! `project()`, `read_projection()`, `await_projection_caught_up()`. See
//! docs/architecture.md §3.2.

use crate::access_control::{AccessLevel, RoleAccessMapping, RoleStatus};
use crate::error::SkiljRejection;
use crate::event_store::{BoundedContext, BoundedContextStatus, Event, EventType};

// `project()` - the plugged-in, per-projection fold black box (see the
// note above the rules in specs/skilj.allium) - is `plugin::
// ProjectionDispatcher::project`, called both by the sync inline path
// (`db::insert_event_and_update_sync_projections`) and the async
// background consumer (`db::catch_up_bounded_context`), which also
// drives a building `ProjectionRebuild`'s own replay and its automatic
// promotion once caught up - see that function's own doc comment.
// `await_projection_caught_up()` is still fully caller-supplied in
// `query_projection` below - see its own doc comment for why (the same
// "black box in the same register as decide()" treatment
// `process_command`'s `decision` gets). `read_projection()` is real now -
// see its own doc comment - but `query_projection` itself stays
// unchanged: the resolver calls `read_projection` first and hands
// `query_projection` the already-decrypted result, the identical
// "pure function takes what the caller already resolved" shape
// `event_store::query_events` has relative to `render_event`.
//
// This module was originally scaffolded with its own placeholder `Error`
// (`EventTypeOutsideBoundedContext`/`RebuildAlreadyStaged`/
// `CaughtUpTimeout`) ahead of the rules themselves being worked out in
// full. `Projection`/`ProjectionRebuild` and the four rules below were
// first built directly in `event_store` instead - missing this module
// entirely - then moved here once that mismatch against
// docs/architecture.md's own module map was caught; `event_store`'s
// `schema_is_backwards_compatible` is reused across the module boundary
// the same way `bootstrap` already reuses `event_store::Error::
// BoundedContextArchived`. The placeholder `Error` variants didn't
// survive the move unchanged - `RebuildAlreadyStaged` in particular
// named a rejection no rule actually has (restaging an already-pending
// rebuild is `RegisterProjection`'s success path, not an error) - so
// they were replaced with the three the rules actually need, matching
// the naming convention `event_store`'s own `*NotInBoundedContext`
// errors already use.

/// Library-level errors this module's own rules reject for.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("a projection may only consume EventTypes from its own bounded context")]
    ConsumedEventTypeNotInBoundedContext,

    #[error("no pending ProjectionRebuild is staged for this projection")]
    NoProjectionRebuildStaged,

    /// Distinguishable from every other rejection this rule can produce
    /// (see `ReadYourWritesWhenRequested`) - a caller retries a timeout,
    /// unlike a permanent rejection such as a revoked grant.
    #[error(
        "the projection did not catch up to the requested sequence within the configured wait"
    )]
    ProjectionCaughtUpTimedOut,
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::ConsumedEventTypeNotInBoundedContext => {
                "consumed_event_type_not_in_bounded_context"
            }
            Error::NoProjectionRebuildStaged => "no_projection_rebuild_staged",
            Error::ProjectionCaughtUpTimedOut => "projection_caught_up_timed_out",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

/// See `entity Projection`. `rebuilds` (a relationship projection, not a
/// stored field - the same treatment `EventType`'s `*_tokens` get) is
/// omitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projection {
    pub bounded_context: BoundedContext,
    pub name: String,
    pub schema: String,
    pub schema_version: i64,
    pub consumed_event_types: Vec<EventType>,
    pub sync: bool,
    pub caught_up_to: Option<i64>,
}

/// See `entity ProjectionRebuild`'s `status` field/transition graph.
/// `building` is terminal - see the entity's own doc comment: promotion
/// and discarding both end a rebuild by making the row cease to exist,
/// neither is a further `status` transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionRebuildStatus {
    Pending,
    Building,
}

/// See `entity ProjectionRebuild`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionRebuild {
    pub projection: Projection,
    pub schema: String,
    pub schema_version: i64,
    pub consumed_event_types: Vec<EventType>,
    pub sync: bool,
    pub caught_up_to: Option<i64>,
    pub status: ProjectionRebuildStatus,
}

/// The outcome of `register_projection` below - the spec's own three-way
/// `ensures` branch (`Projection.created`, staging/restaging a
/// `ProjectionRebuild`, or reconciling the live `Projection` in place),
/// made explicit the same way `event_store::EventTypeRegistration` makes
/// `RegisterEventType`'s two-way branch explicit. `RebuildStaged` doesn't
/// separately distinguish "a new row was staged" from "an already-staged
/// row was restaged" - unlike the created/updated split on the other two,
/// that distinction isn't its own obligation here (`allium plan` has no
/// `rule-entity-creation` for `RegisterProjection` - its `ensures` block
/// is a three-way conditional, not a single `.created()` clause), and the
/// returned `ProjectionRebuild`'s own fields already show what a caller
/// needs either way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionRegistration {
    Created {
        projection: Projection,
        /// `true` only for a first-time registration where `sync` is
        /// true and the bounded context already has committed events
        /// for at least one of `consumed_event_types` - drift audit
        /// finding #3 (2026-08-20, see project memory
        /// `skilj-drift-audit-2026-08-20`): a sync projection has no
        /// periodic catch-up of its own (`db::catch_up_bounded_context`
        /// filters to `!p.sync`, deliberately - a sync projection is
        /// meant to be kept current inline, by
        /// `insert_event_and_update_sync_projections`, not polled), so
        /// without this flag a brand-new sync projection registered
        /// into a context with pre-existing matching history would
        /// permanently miss it: nothing ever folds it in, since only
        /// *future* events reach the sync inline path. The caller must
        /// respond by folding that history in immediately, before the
        /// projection is considered caught up - see
        /// `db::fold_history_into_new_sync_projection`. Safe to do
        /// synchronously, unlike a re-registration's `RebuildStaged`
        /// two-step (stage now, replay later, promote atomically): a
        /// brand-new projection has no live readers yet to protect from
        /// an in-progress fold.
        needs_history_fold: bool,
    },
    RebuildStaged(ProjectionRebuild),
    ReconciledTrivially(Projection),
}

/// See `rule RegisterProjection`. `existing` is `Projection{bounded_context,
/// name}` and `staged` is `ProjectionRebuild{projection: existing, status:
/// pending}`, both as already looked up by the caller - the same get-or-
/// create lookup treatment `event_store::register_event_type`'s `existing`
/// and `event_store::consume_events`' `existing_cursor` get.
/// `bounded_context_events` is every `Event` in `bounded_context` this
/// engine currently knows of, for `consumed_change_has_history` - the one
/// check in this function that needs more than the two looked-up rows,
/// the same full-snapshot treatment `access_control::create_role`'s
/// `existing_roles` gets elsewhere.
#[allow(clippy::too_many_arguments)]
pub fn register_projection(
    access_mapping: &RoleAccessMapping,
    bounded_context: &BoundedContext,
    name: String,
    schema: String,
    consumed_event_types: Vec<EventType>,
    sync: bool,
    existing: Option<&Projection>,
    staged: Option<&ProjectionRebuild>,
    bounded_context_events: &[Event],
) -> crate::error::Result<ProjectionRegistration> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if &access_mapping.bounded_context != bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if bounded_context.status != BoundedContextStatus::Active {
        return Err(crate::event_store::Error::BoundedContextArchived.into());
    }
    // Drift audit finding #16's own follow-up (2026-08-20, see project
    // memory `skilj-drift-audit-2026-08-20`): RegisterProjection had no
    // schema check at all, unlike RegisterEventType/RegisterCommandType -
    // not even the vacuous kind those two had before this same finding's
    // fix, since this rule never had a `valid_tag_mappings`/
    // `valid_sensitive_fields` pair to begin with. Worse than what the
    // finding originally covered: a projection's schema has no
    // `valid_payload`-style backstop later either (its real consumer is
    // the generated GraphQL type, not a runtime payload check), so a
    // malformed one could persist indefinitely with no eventual
    // rejection at all. Reuses `event_store::Error::InvalidSchema` rather
    // than a duplicate variant here, the same cross-module reuse
    // `BoundedContextArchived` just above already does.
    if !crate::event_store::valid_schema(&schema) {
        return Err(crate::event_store::Error::InvalidSchema.into());
    }
    if !consumed_event_types
        .iter()
        .all(|et| &et.bounded_context == bounded_context)
    {
        return Err(Error::ConsumedEventTypeNotInBoundedContext.into());
    }

    let Some(existing) = existing else {
        let needs_history_fold = sync
            && consumed_event_types
                .iter()
                .any(|et| bounded_context_events.iter().any(|e| &e.event_type == et));
        return Ok(ProjectionRegistration::Created {
            projection: Projection {
                bounded_context: bounded_context.clone(),
                name,
                schema,
                schema_version: 1,
                consumed_event_types,
                sync,
                caught_up_to: None,
            },
            needs_history_fold,
        });
    };

    if !crate::event_store::schema_is_backwards_compatible(&existing.schema, &schema) {
        return Err(crate::event_store::Error::SchemaIncompatible.into());
    }

    let schema_changed = existing.schema != schema;
    let changed_event_types = existing
        .consumed_event_types
        .iter()
        .filter(|et| !consumed_event_types.contains(et))
        .chain(
            consumed_event_types
                .iter()
                .filter(|et| !existing.consumed_event_types.contains(et)),
        );
    let consumed_change_has_history = changed_event_types
        .into_iter()
        .any(|et| bounded_context_events.iter().any(|e| &e.event_type == et));
    let becoming_sync = !existing.sync && sync;
    let rebuild_needed = schema_changed || consumed_change_has_history || becoming_sync;

    let new_schema_version = if schema_changed {
        existing.schema_version + 1
    } else {
        existing.schema_version
    };

    if !rebuild_needed {
        // A trivial change, by definition one the schema did not take part
        // in (schema_changed is itself enough to make a registration
        // non-trivial), so neither schema nor schema_version is touched.
        return Ok(ProjectionRegistration::ReconciledTrivially(Projection {
            consumed_event_types,
            sync,
            ..existing.clone()
        }));
    }

    Ok(ProjectionRegistration::RebuildStaged(match staged {
        Some(staged) => ProjectionRebuild {
            schema,
            schema_version: new_schema_version,
            consumed_event_types,
            sync,
            caught_up_to: None,
            ..staged.clone()
        },
        None => ProjectionRebuild {
            projection: existing.clone(),
            schema,
            schema_version: new_schema_version,
            consumed_event_types,
            sync,
            caught_up_to: None,
            status: ProjectionRebuildStatus::Pending,
        },
    }))
}

/// See `rule RebuildProjection`. `staged` is `ProjectionRebuild{projection,
/// status: pending}` as already looked up by the caller - since that
/// lookup is itself filtered to `status: pending`, `staged.status =
/// pending` (the spec's own second `requires`) holds true by construction
/// whenever `staged` is `Some` at all, the same "derived, not a separate
/// parameter" treatment `event_store::authorise_command_trigger`'s
/// `token.command_type = command_type` gets.
pub fn rebuild_projection(
    access_mapping: &RoleAccessMapping,
    projection: &Projection,
    staged: Option<&ProjectionRebuild>,
) -> crate::error::Result<ProjectionRebuild> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if access_mapping.bounded_context != projection.bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    let Some(staged) = staged else {
        return Err(Error::NoProjectionRebuildStaged.into());
    };

    Ok(ProjectionRebuild {
        status: ProjectionRebuildStatus::Building,
        ..staged.clone()
    })
}

/// See `rule DiscardProjectionRebuild`. Same lookup treatment as
/// `rebuild_projection`'s `staged` above. Returns the discarded row
/// rather than `()`: the spec's own `ensures` is a deletion (`not exists
/// staged`), which this pure function can't perform itself (see
/// `access_control::revoke_role`'s cascade for the same "hands back what
/// changed" shape applied to an update rather than a delete) - the caller
/// is the one that removes the row this confirms discarding.
pub fn discard_projection_rebuild(
    access_mapping: &RoleAccessMapping,
    projection: &Projection,
    staged: Option<&ProjectionRebuild>,
) -> crate::error::Result<ProjectionRebuild> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(crate::access_control::Error::InsufficientAccessLevel.into());
    }
    if access_mapping.bounded_context != projection.bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    let Some(staged) = staged else {
        return Err(Error::NoProjectionRebuildStaged.into());
    };

    Ok(staged.clone())
}

/// `read_projection`'s own real branch - see `crate::encryption::decrypt_ciphertext_leaves`'s
/// own doc comment for the full design and why it's automatic rather than
/// a declared `Projection.sensitive_fields`. `data_keys` is the caller's
/// own pre-resolution (`db::list_active_data_keys_for_subject_value`,
/// gated on `event_store::sensitive_field_is_granted` first) - empty
/// means either the caller isn't granted for this instance's own subject,
/// or nothing sensitive was ever found for it; either way `state_json` is
/// returned verbatim, not even re-parsed. Non-empty means at least one
/// key was resolved for this instance's own `key` - every string leaf,
/// at any depth, is tried against each.
pub fn read_projection(state_json: &str, data_keys: &[crate::encryption::DataKey]) -> String {
    if data_keys.is_empty() {
        return state_json.to_string();
    }
    let Ok(mut value) = serde_json::from_str(state_json) else {
        // Not valid JSON - can't happen for a real projection_state row
        // (always written by serde_json::to_string), but returning it
        // verbatim rather than panicking matches every other black box's
        // own "malformed input is left alone, never a crash" treatment.
        return state_json.to_string();
    };
    crate::encryption::decrypt_ciphertext_leaves(&mut value, data_keys);
    serde_json::to_string(&value).expect("re-serialising a parsed JSON Value is infallible")
}

/// See `rule QueryProjection`. `read_projection(projection, key, access_mapping)`
/// and `await_projection_caught_up(projection, wait_for_sequence)` are
/// both black boxes "in the same register as `decide()`" per the spec's
/// own text above rule `CreateExternalEvent` - unlike `event_store::
/// render_event`/`render_command`, there is no trivial/empty case to
/// implement for real here (`Projection` carries no queryable content of
/// its own to fall back to unchanged - that content is exactly what the
/// black box computes), so both are caller-supplied outputs, the same
/// treatment `event_store::process_command`'s `decision` gets. `caught_up`
/// is only consulted when `wait_for_sequence` is `Some` - the spec's own
/// `wait_for_sequence = null or await_projection_caught_up(...)`
/// short-circuit, owned here rather than pushed onto the caller, the same
/// "this function resolves its own null-coalescing" treatment
/// `event_store::fetch_events`'s `after_sequence ?? -1` gets. A sync
/// projection satisfies any sequence immediately, by construction (see
/// the note above the rules) - reflected in whatever `caught_up` the
/// caller computed, not re-derived here.
///
/// `key` (the rule's own `instance_key = key ?? ""`, already resolved by
/// the caller - the identical null-coalescing treatment `wait_for_sequence`
/// itself gets) is accepted here purely for trigger-parameter parity with
/// `when: QueryProjection(access_mapping, projection, key?, wait_for_sequence?)` -
/// this function's own `requires` clauses don't gate on it (which
/// instance to read is already baked into `read_projection_result` by
/// the time it's handed in), but `ProjectionDelivered`'s own `ensures`
/// carries it alongside `result`, so the caller echoes it back in its own
/// response shape.
///
/// `owner_scope_satisfied(projection, key, access_mapping)` (cross-tenant
/// projection read fix, docs/architecture.md's own write-up of this
/// pass): `projection_declares_owner` is whether this projection
/// registered an `OWNER_TAG_KEY` at all (from `ProjectionDispatcher::
/// owner_tag_key`, the caller's own already-resolved lookup); `instance_owner`
/// is the queried instance's own stored `owner` column, or `None` for a
/// row that doesn't exist yet or has never had one derived. When the
/// projection declares no owner dimension, `access_mapping.scope` is
/// irrelevant here regardless of its own value - it only ever restricts
/// an owner-declaring projection. When it does, and `access_mapping.scope`
/// is `Some`, the query is rejected unless `instance_owner` is `Some` and
/// equal to it - fail-closed: an instance whose ownership can't be
/// affirmatively proven (including one nothing has touched yet) is
/// treated the same as a proven mismatch, not the same as a proven
/// match.
#[allow(clippy::too_many_arguments)]
pub fn query_projection(
    access_mapping: &RoleAccessMapping,
    projection: &Projection,
    _key: &str,
    wait_for_sequence: Option<i64>,
    caught_up: bool,
    projection_declares_owner: bool,
    instance_owner: Option<&str>,
    read_projection_result: String,
) -> crate::error::Result<String> {
    if access_mapping.status != RoleStatus::Active {
        return Err(crate::access_control::Error::GrantNotActive.into());
    }
    if access_mapping.bounded_context != projection.bounded_context {
        return Err(crate::access_control::Error::GrantBoundedContextMismatch.into());
    }
    if projection_declares_owner {
        if let Some(scope) = &access_mapping.scope {
            if instance_owner != Some(scope.as_str()) {
                return Err(crate::access_control::Error::GrantScopeMismatch.into());
            }
        }
    }
    if wait_for_sequence.is_some() && !caught_up {
        return Err(Error::ProjectionCaughtUpTimedOut.into());
    }

    Ok(read_projection_result)
}
