//! `surface TypeRegistration` - `registerEventType`, `registerCommandType`,
//! `registerProjection`, `rebuildProjection`, `discardProjectionRebuild`,
//! plus a `projections(boundedContext:)` query satisfying the surface's
//! own `exposes: for projection in bounded_context.projections` (see
//! `gql_types::ProjectionWithRebuild`'s own doc comment for why a
//! projection's rebuilds are modelled as two nullable fields there, not a
//! list, despite the spec's own loop phrasing) and a
//! `scheduledEventTypes(boundedContext:)` query satisfying `exposes: for
//! scheduled_type in bounded_context.event_types where
//! system_triggered_allowed = true`. `AdminAccess`-gated -
//! `require_admin_mapping` resolves the actor; every pure function this
//! module calls re-checks the grant's status/level/scope for real.
//!
//! `RegisterProjection`/`RebuildProjection`/`DiscardProjectionRebuild`
//! each look up `staged` as `ProjectionRebuild{..., status: pending}` per
//! their own `let` in specs/skilj.allium - never `building`, which
//! `db::get_projection_rebuild`'s own `status` parameter makes explicit
//! at every call site below rather than leaving it to an unfiltered
//! lookup's luck.

use super::{
    not_found, parse_missed_occurrence_policy, parse_private_fields, parse_sensitive_fields,
    parse_tag_mappings, require_admin_mapping,
};
use crate::error::to_graphql_error;
use crate::gql_types::{ProjectionRegistrationResult, ProjectionWithRebuild};
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::projections::{ProjectionRebuildStatus, ProjectionRegistration};

/// `registerEventType(boundedContext: String!, name: String!, schema: String!, tagMappings: [TagMappingInput!]!, ownerTagKey: String, sensitiveFields: [SensitiveFieldInput!]!, externalCreationAllowed: Boolean!, directCreationAllowed: Boolean!, systemTriggeredAllowed: Boolean!, eventReadAllowed: Boolean!, systemTriggeredSchedule: String, missedOccurrencePolicy: MissedOccurrencePolicy): EventType!`
///
/// `ownerTagKey` (cross-tenant read fix, docs/architecture.md's own
/// write-up of these passes): names which of `tagMappings`' own keys is
/// this type's owner dimension - `null`/omitted (every type registered
/// before this argument existed) means no owner dimension, unaffected by
/// any grant's `scope` regardless of its value. Validated by
/// `valid_owner_tag_key` - see `RegisterEventType` in specs/skilj.allium.
pub fn register_event_type_field(n: &Naming) -> Field {
    Field::new(
        n.root("registerEventType"),
        TypeRef::named_nn(n.ty("EventType")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let name = ctx.args.try_get("name")?.string()?.to_string();
                let schema = ctx.args.try_get("schema")?.string()?.to_string();
                let tag_mappings = parse_tag_mappings(&ctx.args.try_get("tagMappings")?)?;
                let owner_tag_key = ctx
                    .args
                    .get("ownerTagKey")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?;
                let sensitive_fields =
                    parse_sensitive_fields(&ctx.args.try_get("sensitiveFields")?)?;
                let private_fields = parse_private_fields(&ctx.args.try_get("privateFields")?)?;
                let external_creation_allowed =
                    ctx.args.try_get("externalCreationAllowed")?.boolean()?;
                let direct_creation_allowed =
                    ctx.args.try_get("directCreationAllowed")?.boolean()?;
                let system_triggered_allowed =
                    ctx.args.try_get("systemTriggeredAllowed")?.boolean()?;
                let event_read_allowed = ctx.args.try_get("eventReadAllowed")?.boolean()?;
                let system_triggered_schedule = ctx
                    .args
                    .get("systemTriggeredSchedule")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?;
                let missed_occurrence_policy = match ctx
                    .args
                    .get("missedOccurrencePolicy")
                    .filter(|v| !v.is_null())
                {
                    Some(v) => Some(parse_missed_occurrence_policy(v.enum_name()?)),
                    None => None,
                };

                let bounded_context =
                    skilj_core::db::get_bounded_context(&state.pool, &bounded_context_name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("BoundedContext", &bounded_context_name))?;
                let existing =
                    skilj_core::db::get_event_type(&state.pool, &bounded_context_name, &name)
                        .await
                        .map_err(to_graphql_error)?;

                let registration = skilj_core::event_store::register_event_type(
                    &access_mapping,
                    &bounded_context,
                    name,
                    schema,
                    tag_mappings,
                    owner_tag_key,
                    sensitive_fields,
                    private_fields,
                    external_creation_allowed,
                    direct_creation_allowed,
                    system_triggered_allowed,
                    system_triggered_schedule,
                    missed_occurrence_policy,
                    event_read_allowed,
                    existing.as_ref(),
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::upsert_event_type(&state.pool, registration.event_type())
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(
                    registration.event_type().clone(),
                )))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
    .argument(InputValue::new(
        "schema",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "tagMappings",
        TypeRef::named_nn_list_nn(n.ty("TagMappingInput")),
    ))
    .argument(InputValue::new(
        "ownerTagKey",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "sensitiveFields",
        TypeRef::named_nn_list_nn(n.ty("SensitiveFieldInput")),
    ))
    .argument(InputValue::new(
        "privateFields",
        TypeRef::named_nn_list_nn(n.ty("PrivateFieldInput")),
    ))
    .argument(InputValue::new(
        "externalCreationAllowed",
        TypeRef::named_nn(TypeRef::BOOLEAN),
    ))
    .argument(InputValue::new(
        "directCreationAllowed",
        TypeRef::named_nn(TypeRef::BOOLEAN),
    ))
    .argument(InputValue::new(
        "systemTriggeredAllowed",
        TypeRef::named_nn(TypeRef::BOOLEAN),
    ))
    .argument(InputValue::new(
        "eventReadAllowed",
        TypeRef::named_nn(TypeRef::BOOLEAN),
    ))
    .argument(InputValue::new(
        "systemTriggeredSchedule",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "missedOccurrencePolicy",
        TypeRef::named(n.ty("MissedOccurrencePolicy")),
    ))
}

/// `registerCommandType(boundedContext: String!, name: String!, schema: String!, tagMappings: [TagMappingInput!]!, ownerTagKey: String, sensitiveFields: [SensitiveFieldInput!]!, restTriggerAllowed: Boolean!): CommandType!`
///
/// `ownerTagKey` - see `registerEventType`'s own doc comment on
/// `ownerTagKey`, identical role for `CommandType`/`FetchCommands`
/// (cross-tenant read fix, docs/architecture.md's own write-up of these
/// passes).
pub fn register_command_type_field(n: &Naming) -> Field {
    Field::new(
        n.root("registerCommandType"),
        TypeRef::named_nn(n.ty("CommandType")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let name = ctx.args.try_get("name")?.string()?.to_string();
                let schema = ctx.args.try_get("schema")?.string()?.to_string();
                let tag_mappings = parse_tag_mappings(&ctx.args.try_get("tagMappings")?)?;
                let owner_tag_key = ctx
                    .args
                    .get("ownerTagKey")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?;
                let sensitive_fields =
                    parse_sensitive_fields(&ctx.args.try_get("sensitiveFields")?)?;
                let private_fields = parse_private_fields(&ctx.args.try_get("privateFields")?)?;
                let rest_trigger_allowed = ctx.args.try_get("restTriggerAllowed")?.boolean()?;

                let bounded_context =
                    skilj_core::db::get_bounded_context(&state.pool, &bounded_context_name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("BoundedContext", &bounded_context_name))?;
                let existing =
                    skilj_core::db::get_command_type(&state.pool, &bounded_context_name, &name)
                        .await
                        .map_err(to_graphql_error)?;

                let registration = skilj_core::event_store::register_command_type(
                    &access_mapping,
                    &bounded_context,
                    name,
                    schema,
                    tag_mappings,
                    owner_tag_key,
                    sensitive_fields,
                    private_fields,
                    rest_trigger_allowed,
                    existing.as_ref(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::upsert_command_type(&state.pool, registration.command_type())
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(
                    registration.command_type().clone(),
                )))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
    .argument(InputValue::new(
        "schema",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "tagMappings",
        TypeRef::named_nn_list_nn(n.ty("TagMappingInput")),
    ))
    .argument(InputValue::new(
        "ownerTagKey",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "sensitiveFields",
        TypeRef::named_nn_list_nn(n.ty("SensitiveFieldInput")),
    ))
    .argument(InputValue::new(
        "privateFields",
        TypeRef::named_nn_list_nn(n.ty("PrivateFieldInput")),
    ))
    .argument(InputValue::new(
        "restTriggerAllowed",
        TypeRef::named_nn(TypeRef::BOOLEAN),
    ))
}

/// `registerProjection(boundedContext: String!, name: String!, schema: String!, consumedEventTypes: [String!]!, sync: Boolean!): ProjectionRegistrationResult!`
pub fn register_projection_field(n: &Naming) -> Field {
    Field::new(
        n.root("registerProjection"),
        TypeRef::named_nn(n.ty("ProjectionRegistrationResult")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let name = ctx.args.try_get("name")?.string()?.to_string();
                let schema = ctx.args.try_get("schema")?.string()?.to_string();
                let sync = ctx.args.try_get("sync")?.boolean()?;

                // A repeated name is refused before any is looked up
                // (docs/architecture.md §145).
                let consumed_names = ctx
                    .args
                    .try_get("consumedEventTypes")?
                    .list()?
                    .iter()
                    .map(|item| item.string().map(str::to_string))
                    .collect::<async_graphql::Result<Vec<_>>>()?;
                skilj_core::projections::check_consumed_event_type_names(
                    consumed_names.iter().map(String::as_str),
                )
                .map_err(to_graphql_error)?;
                let mut consumed_event_types = Vec::new();
                for event_type_name in consumed_names {
                    let event_type = skilj_core::db::get_event_type(
                        &state.pool,
                        &bounded_context_name,
                        &event_type_name,
                    )
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("EventType", &event_type_name))?;
                    consumed_event_types.push(event_type);
                }

                let bounded_context =
                    skilj_core::db::get_bounded_context(&state.pool, &bounded_context_name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("BoundedContext", &bounded_context_name))?;
                let existing =
                    skilj_core::db::get_projection(&state.pool, &bounded_context_name, &name)
                        .await
                        .map_err(to_graphql_error)?;
                let staged = skilj_core::db::get_projection_rebuild(
                    &state.pool,
                    &bounded_context_name,
                    &name,
                    ProjectionRebuildStatus::Pending,
                )
                .await
                .map_err(to_graphql_error)?;
                let bounded_context_events = skilj_core::db::witness_events_of_types(
                    &state.pool,
                    &bounded_context_name,
                    &consumed_event_types,
                )
                .await
                .map_err(to_graphql_error)?;

                let registration = skilj_core::projections::register_projection(
                    &access_mapping,
                    &bounded_context,
                    name.clone(),
                    schema,
                    consumed_event_types,
                    sync,
                    existing.as_ref(),
                    staged.as_ref(),
                    &bounded_context_events,
                )
                .map_err(to_graphql_error)?;

                let result = match registration {
                    ProjectionRegistration::Created {
                        projection,
                        needs_history_fold,
                    } => {
                        // docs/architecture.md §113: stored and backfilled
                        // so no concurrent write is folded out of order or
                        // missed.
                        let projection = skilj_core::db::create_projection(
                            &state.pool,
                            &projection,
                            needs_history_fold,
                            state.projection_dispatcher.as_ref(),
                        )
                        .await
                        .map_err(to_graphql_error)?;
                        ProjectionRegistrationResult {
                            outcome: "CREATED",
                            projection: Some(ProjectionWithRebuild {
                                projection,
                                pending_rebuild: None,
                                building_rebuild: None,
                            }),
                            rebuild: None,
                        }
                    }
                    ProjectionRegistration::ReconciledTrivially(projection) => {
                        skilj_core::db::upsert_projection(&state.pool, &projection)
                            .await
                            .map_err(to_graphql_error)?;
                        ProjectionRegistrationResult {
                            outcome: "RECONCILED_TRIVIALLY",
                            projection: Some(ProjectionWithRebuild {
                                projection,
                                pending_rebuild: None,
                                building_rebuild: None,
                            }),
                            rebuild: None,
                        }
                    }
                    ProjectionRegistration::RebuildStaged(rebuild) => {
                        skilj_core::db::upsert_projection_rebuild(&state.pool, &rebuild)
                            .await
                            .map_err(to_graphql_error)?;
                        ProjectionRegistrationResult {
                            outcome: "REBUILD_STAGED",
                            projection: None,
                            rebuild: Some(rebuild),
                        }
                    }
                };

                Ok(Some(FieldValue::owned_any(result)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
    .argument(InputValue::new(
        "schema",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "consumedEventTypes",
        TypeRef::named_nn_list_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("sync", TypeRef::named_nn(TypeRef::BOOLEAN)))
}

/// `rebuildProjection(boundedContext: String!, name: String!): ProjectionRebuild!`
pub fn rebuild_projection_field(n: &Naming) -> Field {
    Field::new(
        n.root("rebuildProjection"),
        TypeRef::named_nn(n.ty("ProjectionRebuild")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let name = ctx.args.try_get("name")?.string()?.to_string();

                let projection =
                    skilj_core::db::get_projection(&state.pool, &bounded_context_name, &name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("Projection", &name))?;
                let staged = skilj_core::db::get_projection_rebuild(
                    &state.pool,
                    &bounded_context_name,
                    &name,
                    ProjectionRebuildStatus::Pending,
                )
                .await
                .map_err(to_graphql_error)?;

                let rebuild = skilj_core::projections::rebuild_projection(
                    &access_mapping,
                    &projection,
                    staged.as_ref(),
                )
                .map_err(to_graphql_error)?;
                // Not `upsert_projection_rebuild` - this is a status
                // *transition* (pending becoming building), not a
                // same-status restage; see
                // `transition_projection_rebuild_to_building`'s own doc
                // comment for why that distinction matters.
                skilj_core::db::transition_projection_rebuild_to_building(&state.pool, &rebuild)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(rebuild)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
}

/// `discardProjectionRebuild(boundedContext: String!, name: String!): ProjectionRebuild!`,
/// returning the just-discarded row (see `discard_projection_rebuild`'s
/// own doc comment for why it hands that back rather than `()`).
pub fn discard_projection_rebuild_field(n: &Naming) -> Field {
    Field::new(
        n.root("discardProjectionRebuild"),
        TypeRef::named_nn(n.ty("ProjectionRebuild")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let name = ctx.args.try_get("name")?.string()?.to_string();

                let projection =
                    skilj_core::db::get_projection(&state.pool, &bounded_context_name, &name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("Projection", &name))?;
                let staged = skilj_core::db::get_projection_rebuild(
                    &state.pool,
                    &bounded_context_name,
                    &name,
                    ProjectionRebuildStatus::Pending,
                )
                .await
                .map_err(to_graphql_error)?;

                let discarded = skilj_core::projections::discard_projection_rebuild(
                    &access_mapping,
                    &projection,
                    staged.as_ref(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::delete_projection_rebuild(
                    &state.pool,
                    &bounded_context_name,
                    &name,
                    ProjectionRebuildStatus::Pending,
                )
                .await
                .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(discarded)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
}

/// `projections(boundedContext: String!): [Projection!]!` - satisfies
/// `TypeRegistration`'s own `exposes: for projection in
/// bounded_context.projections`.
pub fn projections_field(n: &Naming) -> Field {
    Field::new(
        n.root("projections"),
        TypeRef::named_nn_list_nn(n.ty("Projection")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let projections = skilj_core::db::list_projections_for_bounded_context(
                    &state.pool,
                    &bounded_context_name,
                )
                .await
                .map_err(to_graphql_error)?;

                let mut with_rebuilds = Vec::with_capacity(projections.len());
                for projection in projections {
                    let pending_rebuild = skilj_core::db::get_projection_rebuild(
                        &state.pool,
                        &bounded_context_name,
                        &projection.name,
                        ProjectionRebuildStatus::Pending,
                    )
                    .await
                    .map_err(to_graphql_error)?;
                    let building_rebuild = skilj_core::db::get_projection_rebuild(
                        &state.pool,
                        &bounded_context_name,
                        &projection.name,
                        ProjectionRebuildStatus::Building,
                    )
                    .await
                    .map_err(to_graphql_error)?;
                    with_rebuilds.push(FieldValue::owned_any(ProjectionWithRebuild {
                        projection,
                        pending_rebuild,
                        building_rebuild,
                    }));
                }
                Ok(Some(FieldValue::list(with_rebuilds)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}

/// Shared by `scheduled_event_types_field`/`event_types_field`/
/// `command_types_field` immediately below - all three resolve
/// `boundedContext` → `require_admin_mapping` → call one `db::list_*`
/// function → return the unwrapped list. `projections_field` above is a
/// genuinely different shape (it layers a per-projection rebuild-status
/// lookup on top) and is left as its own hand-written body.
macro_rules! list_for_bounded_context_field {
    ($n:expr, $field_name:literal, $return_type:literal, $list_fn:path) => {
        Field::new(
            $n.root($field_name),
            TypeRef::named_nn_list_nn($n.ty($return_type)),
            |ctx| {
                FieldFuture::new(async move {
                    let state = ctx.data::<GraphqlState>()?;
                    let bounded_context_name =
                        ctx.args.try_get("boundedContext")?.string()?.to_string();
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                    let items = $list_fn(&state.pool, &bounded_context_name)
                        .await
                        .map_err(to_graphql_error)?;

                    Ok(Some(FieldValue::list(
                        items.into_iter().map(FieldValue::owned_any),
                    )))
                })
            },
        )
        .argument(InputValue::new(
            "boundedContext",
            TypeRef::named_nn(TypeRef::STRING),
        ))
    };
}

/// `scheduledEventTypes(boundedContext: String!): [EventType!]!` -
/// satisfies `TypeRegistration`'s own `exposes: for scheduled_type in
/// bounded_context.event_types where system_triggered_allowed = true`
/// (`@guarantee ScheduleStateIsShared`'s own admin visibility: policy,
/// `lastFiredAt`, `schedulePosition`, all already on `EventType` itself -
/// see `gql_types::event_type_object` - so this is only ever the
/// filtered listing, nothing new on the type).
pub fn scheduled_event_types_field(n: &Naming) -> Field {
    list_for_bounded_context_field!(
        n,
        "scheduledEventTypes",
        "EventType",
        skilj_core::db::list_scheduled_event_types
    )
}

/// `eventTypes(boundedContext: String!): [EventType!]!` - satisfies
/// `TypeRegistration`'s own `exposes: for event_type in
/// bounded_context.event_types` (Codeberg issue #6's "5a": the
/// self-describing surface a real command/event type picker needs -
/// `scheduledEventTypes` above only ever returns the scheduled subset).
/// `db::list_event_types_for_bounded_context` is the identical query
/// unfiltered - same `AdminAccess` gate, same shape.
pub fn event_types_field(n: &Naming) -> Field {
    list_for_bounded_context_field!(
        n,
        "eventTypes",
        "EventType",
        skilj_core::db::list_event_types_for_bounded_context
    )
}

/// `commandTypes(boundedContext: String!): [CommandType!]!` -
/// `CommandType`'s own equivalent of `event_types_field` immediately
/// above, satisfying `exposes: for command_type in
/// bounded_context.command_types`.
pub fn command_types_field(n: &Naming) -> Field {
    list_for_bounded_context_field!(
        n,
        "commandTypes",
        "CommandType",
        skilj_core::db::list_command_types_for_bounded_context
    )
}
