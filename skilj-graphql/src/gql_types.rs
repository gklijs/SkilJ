//! The static GraphQL types Phase 1's admin-console surfaces share -
//! `Role`, `BoundedContext` (+ its nested `ContextCreator`/
//! `RoleAccessMapping`), the three enums, and `CreatedSuperadmin` (the
//! deliberately narrower payload `createSuperadmin` itself returns - see
//! its own doc comment). Every one of these is built against
//! `async_graphql::dynamic`'s own `Object`/`Field`/`Enum` API (§5.1) -
//! there is no bridge from `#[derive(SimpleObject)]`-style static types
//! into a `dynamic::Schema` in this version of `async-graphql`, so this
//! module can't use *that* derive macro the way `skilj-core::plugin`'s
//! own `schemars`-derived payload schemas do. Most `Object` builders
//! below use `skilj_macros::gql_object!` instead (docs/architecture.md
//! §1.3.2) - a bespoke local macro filling the gap that missing bridge
//! leaves, not a reimplementation of it; a few with a field shape it
//! doesn't cover (a nullable list of plain scalars, say) stay hand-built
//! against `scalar_field`/`object_field`/`list_field` directly, or a raw
//! `Field::new(...)` for the rare field neither covers.
//!
//! Every object's fields resolve synchronously off an already-loaded
//! Rust value (`ctx.parent_value.try_downcast_ref::<T>()`) rather than
//! making their own database calls - the query/mutation resolvers in
//! `resolvers/` load everything a response needs up front (including,
//! for `BoundedContext.accessMappings`, a second query resolved before
//! construction - see `BoundedContextWithMappings`), so no nested field
//! here ever needs `ctx.data::<Pool>()` for itself.

use async_graphql::dynamic::{
    Enum, Field, FieldFuture, FieldValue, InputObject, InputValue, Object, ResolverContext,
    TypeRef, Union,
};
use async_graphql::Value;
use skilj_core::access_control::{
    AccessLevel, CommandToken, DirectCreationToken, EventReadStartPosition, EventReadToken,
    ExternalEventToken, PrivateFieldGrant, Role, RoleAccessMapping, RoleStatus, TokenStatus,
};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, CommandType, EncryptionKey, EncryptionKeyStatus,
    EventType, MissedOccurrencePolicy,
};
use skilj_core::projections::{Projection, ProjectionRebuild, ProjectionRebuildStatus};
use skilj_core::shared::{PrivateField, PrivateFieldKind, SensitiveField, TagMapping};
use skilj_macros::gql_object;

/// A scalar (or nullable-scalar) field resolved synchronously from the
/// parent value alone - the common case for every leaf field below.
fn scalar_field<T, F>(name: &'static str, ty: TypeRef, resolve: F) -> Field
where
    T: Send + Sync + 'static,
    F: Fn(&T) -> Value + Clone + Send + Sync + 'static,
{
    Field::new(name, ty, move |ctx: ResolverContext| {
        let resolve = resolve.clone();
        FieldFuture::new(async move {
            let parent = ctx.parent_value.try_downcast_ref::<T>()?;
            Ok(Some(resolve(parent)))
        })
    })
}

/// A field resolving to a nested object type - `resolve` produces the
/// child object's own backing Rust value, boxed via `FieldValue::owned_any`
/// exactly as `object.rs`'s own doc example does.
fn object_field<T, R, F>(name: &'static str, ty: TypeRef, resolve: F) -> Field
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: Fn(&T) -> Option<R> + Clone + Send + Sync + 'static,
{
    Field::new(name, ty, move |ctx: ResolverContext| {
        let resolve = resolve.clone();
        FieldFuture::new(async move {
            let parent = ctx.parent_value.try_downcast_ref::<T>()?;
            Ok(resolve(parent).map(FieldValue::owned_any))
        })
    })
}

/// A field resolving to a non-null list of nested objects.
fn list_field<T, R, F>(name: &'static str, ty: TypeRef, resolve: F) -> Field
where
    T: Send + Sync + 'static,
    R: Send + Sync + 'static,
    F: Fn(&T) -> Vec<R> + Clone + Send + Sync + 'static,
{
    Field::new(name, ty, move |ctx: ResolverContext| {
        let resolve = resolve.clone();
        FieldFuture::new(async move {
            let parent = ctx.parent_value.try_downcast_ref::<T>()?;
            let items: Vec<FieldValue> = resolve(parent)
                .into_iter()
                .map(FieldValue::owned_any)
                .collect();
            Ok(Some(FieldValue::list(items)))
        })
    })
}

fn optional_timestamp(t: Option<chrono::DateTime<chrono::Utc>>) -> Value {
    match t {
        Some(t) => Value::from(t.to_rfc3339()),
        None => Value::Null,
    }
}

pub fn role_status_name(status: RoleStatus) -> &'static str {
    match status {
        RoleStatus::Active => "ACTIVE",
        RoleStatus::Revoked => "REVOKED",
    }
}

pub fn access_level_name(level: AccessLevel) -> &'static str {
    match level {
        AccessLevel::Read => "READ",
        AccessLevel::Write => "WRITE",
        AccessLevel::Admin => "ADMIN",
    }
}

pub fn bounded_context_status_name(status: BoundedContextStatus) -> &'static str {
    match status {
        BoundedContextStatus::Active => "ACTIVE",
        BoundedContextStatus::Archived => "ARCHIVED",
    }
}

/// `enum RoleStatus`. `parse_role_status`/etc. below have no counterpart
/// yet - Phase 1 never takes a status as a mutation argument, only ever
/// returns one.
pub fn role_status_enum() -> async_graphql::dynamic::Enum {
    async_graphql::dynamic::Enum::new("RoleStatus")
        .item("ACTIVE")
        .item("REVOKED")
}

pub fn access_level_enum() -> async_graphql::dynamic::Enum {
    async_graphql::dynamic::Enum::new("AccessLevel")
        .item("READ")
        .item("WRITE")
        .item("ADMIN")
}

pub fn bounded_context_status_enum() -> async_graphql::dynamic::Enum {
    async_graphql::dynamic::Enum::new("BoundedContextStatus")
        .item("ACTIVE")
        .item("ARCHIVED")
}

/// `enum MissedOccurrencePolicy` - `registerEventType`'s own
/// `missedOccurrencePolicy` argument type, and `EventType.
/// missedOccurrencePolicy`'s own output type below. See
/// `resolvers::parse_missed_occurrence_policy` for the input-side
/// mapping and `missed_occurrence_policy_name` for the output-side one.
pub fn missed_occurrence_policy_enum() -> async_graphql::dynamic::Enum {
    async_graphql::dynamic::Enum::new("MissedOccurrencePolicy")
        .item("SKIP")
        .item("FIRE_ONCE")
        .item("REPLAY_BACKLOG")
}

pub fn missed_occurrence_policy_name(policy: MissedOccurrencePolicy) -> &'static str {
    match policy {
        MissedOccurrencePolicy::Skip => "SKIP",
        MissedOccurrencePolicy::FireOnce => "FIRE_ONCE",
        MissedOccurrencePolicy::ReplayBacklog => "REPLAY_BACKLOG",
    }
}

/// `enum EventReadStartPosition` - `createEventReadToken`'s own
/// `startFrom` argument type, and `EventReadToken.startFrom`'s own
/// output type below (docs/architecture.md's own write-up of this
/// pass). See `resolvers::parse_event_read_start_position` for the
/// input-side mapping and `event_read_start_position_name` for the
/// output-side one.
pub fn event_read_start_position_enum() -> Enum {
    Enum::new("EventReadStartPosition")
        .item("BEGINNING")
        .item("LATEST")
        .item("AT_SEQUENCE")
        .item("AT_TIME")
}

pub fn event_read_start_position_name(position: EventReadStartPosition) -> &'static str {
    match position {
        EventReadStartPosition::Beginning => "BEGINNING",
        EventReadStartPosition::Latest => "LATEST",
        EventReadStartPosition::AtSequence => "AT_SEQUENCE",
        EventReadStartPosition::AtTime => "AT_TIME",
    }
}

/// `entity Role`, minus the relationship projection `access_mappings`
/// (resolved separately, per surface, the same "caller resolves it, not
/// a stored field" treatment every relationship projection gets
/// elsewhere in this codebase).
pub fn role_object() -> Object {
    gql_object!(Role => "Role" {
        scalar "id": TypeRef::named_nn(TypeRef::ID) => |r| Value::from(r.id.clone()),
        scalar "externalSubject": TypeRef::named_nn(TypeRef::STRING) => |r| Value::from(r.external_subject.clone()),
        scalar "name": TypeRef::named_nn(TypeRef::STRING) => |r| Value::from(r.name.clone()),
        scalar "superadmin": TypeRef::named_nn(TypeRef::BOOLEAN) => |r| Value::from(r.superadmin),
        scalar "status": TypeRef::named_nn("RoleStatus") => |r| Value::from(role_status_name(r.status)),
        scalar "createdAt": TypeRef::named_nn(TypeRef::STRING) => |r| Value::from(r.created_at.to_rfc3339()),
        scalar "revokedAt": TypeRef::named(TypeRef::STRING) => |r| optional_timestamp(r.revoked_at),
    })
}

/// `createSuperadmin`'s own deliberately narrow return type -
/// `NoCredentialIssued` (`surface SuperadminBootstrap`): "hands back only
/// the created Role's own id, name and external_subject". A dedicated
/// type rather than reusing `Role` so that guarantee is structural, not
/// a convention a resolver could accidentally violate by returning more.
pub fn created_superadmin_object() -> Object {
    gql_object!(Role => "CreatedSuperadmin" {
        scalar "id": TypeRef::named_nn(TypeRef::ID) => |r| Value::from(r.id.clone()),
        scalar "name": TypeRef::named_nn(TypeRef::STRING) => |r| Value::from(r.name.clone()),
        scalar "externalSubject": TypeRef::named_nn(TypeRef::STRING) => |r| Value::from(r.external_subject.clone()),
    })
}

/// `entity RoleAccessMapping`, minus `bounded_context` - every place
/// this object appears it's already nested under the bounded context it
/// belongs to (see `surface BoundedContextDirectory`'s own `exposes`
/// list, which never repeats it either).
pub fn role_access_mapping_object() -> Object {
    gql_object!(RoleAccessMapping => "RoleAccessMapping" {
        object "role": TypeRef::named_nn("Role") => |m| Some(m.role.clone()),
        scalar "level": TypeRef::named_nn("AccessLevel") => |m| Value::from(access_level_name(m.level)),
        scalar "canReadSensitive": TypeRef::named_nn(TypeRef::BOOLEAN) => |m| Value::from(m.can_read_sensitive),
        scalar "scope": TypeRef::named(TypeRef::STRING) => |m| optional_string(m.scope.clone()),
        scalar "status": TypeRef::named_nn("RoleStatus") => |m| Value::from(role_status_name(m.status)),
    })
}

/// `entity ContextCreator`. `kind` stands in for the Rust sum type's own
/// variant tag (the same "the enum variant tag is the field" treatment
/// the Rust side already gives it - see `ContextCreator`'s own doc
/// comment); `role` is present only for `SUPERADMIN`.
pub fn context_creator_object() -> Object {
    gql_object!(ContextCreator => "ContextCreator" {
        scalar "kind": TypeRef::named_nn(TypeRef::STRING) => |c| {
            Value::from(match c {
                ContextCreator::SuperadminCreator { .. } => "SUPERADMIN",
                ContextCreator::SystemCreator => "SYSTEM",
            })
        },
        object "role": TypeRef::named("Role") => |c| match c {
            ContextCreator::SuperadminCreator { role } => Some(role.clone()),
            ContextCreator::SystemCreator => None,
        },
    })
}

/// `BoundedContext` plus its own currently-active `RoleAccessMapping`s -
/// built once, up front, by every resolver that returns a
/// `BoundedContext` (see `resolvers::load_bounded_context_with_mappings`),
/// since `BoundedContext` itself carries no such relationship as a
/// stored field (the same "caller resolves it" treatment every
/// relationship projection gets elsewhere in this codebase).
pub struct BoundedContextWithMappings {
    pub context: BoundedContext,
    pub access_mappings: Vec<RoleAccessMapping>,
}

/// `entity BoundedContext`, plus the `access_mappings` relationship
/// `surface BoundedContextDirectory`'s own `exposes` list asks for.
pub fn bounded_context_object() -> Object {
    gql_object!(BoundedContextWithMappings => "BoundedContext" {
        scalar "name": TypeRef::named_nn(TypeRef::STRING) => |bc| Value::from(bc.context.name.clone()),
        scalar "status": TypeRef::named_nn("BoundedContextStatus") => |bc| {
            Value::from(bounded_context_status_name(bc.context.status))
        },
        scalar "createdAt": TypeRef::named_nn(TypeRef::STRING) => |bc| Value::from(bc.context.created_at.to_rfc3339()),
        object "createdBy": TypeRef::named_nn("ContextCreator") => |bc| Some(bc.context.created_by.clone()),
        list "accessMappings": TypeRef::named_nn_list_nn("RoleAccessMapping") => |bc| bc.access_mappings.clone(),
    })
}

// ---------------------------------------------------------------------
// Phase 2 additions: TypeRegistration, EventTypeAdminOperations,
// CommandTypeAdminOperations, TokenRevocation.
// ---------------------------------------------------------------------

pub fn access_token_status_name(status: TokenStatus) -> &'static str {
    match status {
        TokenStatus::Active => "ACTIVE",
        TokenStatus::Revoked => "REVOKED",
    }
}

pub fn access_token_status_enum() -> Enum {
    Enum::new("AccessTokenStatus")
        .item("ACTIVE")
        .item("REVOKED")
}

pub fn projection_rebuild_status_name(status: ProjectionRebuildStatus) -> &'static str {
    match status {
        ProjectionRebuildStatus::Pending => "PENDING",
        ProjectionRebuildStatus::Building => "BUILDING",
    }
}

pub fn projection_rebuild_status_enum() -> Enum {
    Enum::new("ProjectionRebuildStatus")
        .item("PENDING")
        .item("BUILDING")
}

/// `value TagMapping`. `TagMappingInput` (used by
/// `registerEventType`/`registerCommandType`'s own arguments) mirrors it
/// field-for-field, plain scalars only - no bridge needed between the two
/// beyond `resolvers::type_registration`'s own parsing.
pub fn tag_mapping_object() -> Object {
    gql_object!(TagMapping => "TagMapping" {
        scalar "key": TypeRef::named_nn(TypeRef::STRING) => |t| Value::from(t.key.clone()),
        scalar "field": TypeRef::named_nn(TypeRef::STRING) => |t| Value::from(t.field.clone()),
    })
}

pub fn tag_mapping_input() -> InputObject {
    InputObject::new("TagMappingInput")
        .field(InputValue::new("key", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new("field", TypeRef::named_nn(TypeRef::STRING)))
}

/// `value SensitiveField`. See `tag_mapping_object`'s own doc comment -
/// same input/output split.
pub fn sensitive_field_object() -> Object {
    gql_object!(SensitiveField => "SensitiveField" {
        scalar "field": TypeRef::named_nn(TypeRef::STRING) => |s| Value::from(s.field.clone()),
        scalar "subjectKey": TypeRef::named_nn(TypeRef::STRING) => |s| Value::from(s.subject_key.clone()),
        scalar "subjectField": TypeRef::named_nn(TypeRef::STRING) => |s| Value::from(s.subject_field.clone()),
    })
}

pub fn sensitive_field_input() -> InputObject {
    InputObject::new("SensitiveFieldInput")
        .field(InputValue::new("field", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new(
            "subjectKey",
            TypeRef::named_nn(TypeRef::STRING),
        ))
        .field(InputValue::new(
            "subjectField",
            TypeRef::named_nn(TypeRef::STRING),
        ))
}

/// `enum PrivateFieldKind`.
pub fn private_field_kind_enum() -> Enum {
    Enum::new("PrivateFieldKind")
        .item("OWN")
        .item("TEAM")
        .item("ADDRESSED")
}

fn private_field_kind_name(kind: PrivateFieldKind) -> &'static str {
    match kind {
        PrivateFieldKind::Own => "OWN",
        PrivateFieldKind::Team => "TEAM",
        PrivateFieldKind::Addressed => "ADDRESSED",
    }
}

/// `value PrivateField`. See `tag_mapping_object`'s own doc comment -
/// same input/output split.
pub fn private_field_object() -> Object {
    gql_object!(PrivateField => "PrivateField" {
        scalar "field": TypeRef::named_nn(TypeRef::STRING) => |p| Value::from(p.field.clone()),
        scalar "kind": TypeRef::named_nn("PrivateFieldKind") => |p| Value::from(private_field_kind_name(p.kind)),
        scalar "team": TypeRef::named(TypeRef::STRING) => |p| optional_string(p.team.clone()),
        scalar "addresseeField": TypeRef::named(TypeRef::STRING) => |p| optional_string(p.addressee_field.clone()),
    })
}

pub fn private_field_input() -> InputObject {
    InputObject::new("PrivateFieldInput")
        .field(InputValue::new("field", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new(
            "kind",
            TypeRef::named_nn("PrivateFieldKind"),
        ))
        .field(InputValue::new("team", TypeRef::named(TypeRef::STRING)))
        .field(InputValue::new(
            "addresseeField",
            TypeRef::named(TypeRef::STRING),
        ))
}

/// `entity PrivateFieldGrant`, minus `boundedContext` - the same "already
/// nested under the context it belongs to" omission
/// `role_access_mapping_object` makes. `eventSequence`/`commandId` are
/// this crate's own wire names for the Rust-side `event_sequence`/
/// `command_id` identity fields - see `access_control::PrivateFieldGrant`'s
/// own doc comment for why those stand in for the spec's `event: Event?`/
/// `command: Command?`.
pub fn private_field_grant_object() -> Object {
    gql_object!(PrivateFieldGrant => "PrivateFieldGrant" {
        scalar "id": TypeRef::named_nn(TypeRef::ID) => |g| Value::from(g.id.clone()),
        object "grantor": TypeRef::named_nn("Role") => |g| Some(g.grantor.clone()),
        object "grantee": TypeRef::named_nn("Role") => |g| Some(g.grantee.clone()),
        scalar "eventSequence": TypeRef::named(TypeRef::INT) => |g| match g.event_sequence {
            Some(seq) => Value::from(seq),
            None => Value::Null,
        },
        scalar "commandId": TypeRef::named(TypeRef::ID) => |g| optional_string(g.command_id.clone()),
        scalar "status": TypeRef::named_nn("AccessTokenStatus") => |g| Value::from(access_token_status_name(g.status)),
        scalar "createdAt": TypeRef::named_nn(TypeRef::STRING) => |g| Value::from(g.created_at.to_rfc3339()),
        scalar "revokedAt": TypeRef::named(TypeRef::STRING) => |g| optional_timestamp(g.revoked_at),
    })
}

/// `entity EventType`, minus the relationship projections
/// (`*_tokens`) - the same "caller resolves it, not a stored field"
/// treatment every relationship projection gets elsewhere in this
/// codebase.
pub fn event_type_object() -> Object {
    gql_object!(EventType => "EventType" {
        scalar "name": TypeRef::named_nn(TypeRef::STRING) => |et| Value::from(et.name.clone()),
        scalar "schema": TypeRef::named_nn(TypeRef::STRING) => |et| Value::from(et.schema.clone()),
        scalar "schemaVersion": TypeRef::named_nn(TypeRef::INT) => |et| Value::from(et.schema_version),
        list "tagMappings": TypeRef::named_nn_list_nn("TagMapping") => |et| et.tag_mappings.clone(),
        scalar "ownerTagKey": TypeRef::named(TypeRef::STRING) => |et| optional_string(et.owner_tag_key.clone()),
        list "sensitiveFields": TypeRef::named_nn_list_nn("SensitiveField") => |et| et.sensitive_fields.clone(),
        scalar "externalCreationAllowed": TypeRef::named_nn(TypeRef::BOOLEAN) => |et| Value::from(et.external_creation_allowed),
        scalar "directCreationAllowed": TypeRef::named_nn(TypeRef::BOOLEAN) => |et| Value::from(et.direct_creation_allowed),
        scalar "systemTriggeredAllowed": TypeRef::named_nn(TypeRef::BOOLEAN) => |et| Value::from(et.system_triggered_allowed),
        scalar "systemTriggeredSchedule": TypeRef::named(TypeRef::STRING) => |et| match &et.system_triggered_schedule {
            Some(s) => Value::from(s.clone()),
            None => Value::Null,
        },
        scalar "missedOccurrencePolicy": TypeRef::named("MissedOccurrencePolicy") => |et| match et.missed_occurrence_policy {
            Some(policy) => Value::from(missed_occurrence_policy_name(policy)),
            None => Value::Null,
        },
        scalar "schedulePosition": TypeRef::named(TypeRef::STRING) => |et| optional_timestamp(et.schedule_position),
        scalar "lastFiredAt": TypeRef::named(TypeRef::STRING) => |et| optional_timestamp(et.last_fired_at),
        scalar "eventReadAllowed": TypeRef::named_nn(TypeRef::BOOLEAN) => |et| Value::from(et.event_read_allowed),
        list "privateFields": TypeRef::named_nn_list_nn("PrivateField") => |et| et.private_fields.clone(),
    })
}

/// `entity CommandType`, same treatment as `event_type_object` above.
pub fn command_type_object() -> Object {
    gql_object!(CommandType => "CommandType" {
        scalar "name": TypeRef::named_nn(TypeRef::STRING) => |ct| Value::from(ct.name.clone()),
        scalar "schema": TypeRef::named_nn(TypeRef::STRING) => |ct| Value::from(ct.schema.clone()),
        scalar "schemaVersion": TypeRef::named_nn(TypeRef::INT) => |ct| Value::from(ct.schema_version),
        list "tagMappings": TypeRef::named_nn_list_nn("TagMapping") => |ct| ct.tag_mappings.clone(),
        scalar "ownerTagKey": TypeRef::named(TypeRef::STRING) => |ct| optional_string(ct.owner_tag_key.clone()),
        list "sensitiveFields": TypeRef::named_nn_list_nn("SensitiveField") => |ct| ct.sensitive_fields.clone(),
        scalar "restTriggerAllowed": TypeRef::named_nn(TypeRef::BOOLEAN) => |ct| Value::from(ct.rest_trigger_allowed),
        list "privateFields": TypeRef::named_nn_list_nn("PrivateField") => |ct| ct.private_fields.clone(),
    })
}

/// `entity ProjectionRebuild`.
pub fn projection_rebuild_object() -> Object {
    gql_object!(ProjectionRebuild => "ProjectionRebuild" {
        scalar "schema": TypeRef::named_nn(TypeRef::STRING) => |r| Value::from(r.schema.clone()),
        scalar "schemaVersion": TypeRef::named_nn(TypeRef::INT) => |r| Value::from(r.schema_version),
        list "consumedEventTypes": TypeRef::named_nn_list_nn("EventType") => |r| r.consumed_event_types.clone(),
        scalar "sync": TypeRef::named_nn(TypeRef::BOOLEAN) => |r| Value::from(r.sync),
        scalar "caughtUpTo": TypeRef::named(TypeRef::INT) => |r| match r.caught_up_to {
            Some(seq) => Value::from(seq),
            None => Value::Null,
        },
        scalar "status": TypeRef::named_nn("ProjectionRebuildStatus") => |r| Value::from(projection_rebuild_status_name(r.status)),
    })
}

/// `entity Projection`. `pending_rebuild`/`building_rebuild` are two
/// nullable fields, not the spec's own `exposes: ... for rebuild in
/// projection.rebuilds` loop phrasing rendered as a `[ProjectionRebuild!]!`
/// list - invariant `UniqueRebuildPerProjectionAndStatus` caps
/// `rebuilds` at exactly one per status, never an open-ended set, so a
/// list would just be an unindexed, always-length-≤2 collection a caller
/// has to search for the row it actually wants. Two named fields say
/// which is which for free - the same "deliberately coarse on the wire
/// contract" simplification `TypeRegistration`'s own guidance already
/// sanctions elsewhere. (Previously a single `rebuild` field, on the
/// wrong assumption that `ProjectionRebuild` was keyed 1:1 by
/// `(bounded_context, projection_name)` - the drift audit's own finding
/// on `UniqueRebuildPerProjectionAndStatus`'s "the two coexisting is the
/// deliberate case" catching that a pending and a building rebuild can
/// be live at once, which the old single field had no way to show both
/// of.)
#[derive(Clone)]
pub struct ProjectionWithRebuild {
    pub projection: Projection,
    pub pending_rebuild: Option<ProjectionRebuild>,
    pub building_rebuild: Option<ProjectionRebuild>,
}

pub fn projection_object() -> Object {
    gql_object!(ProjectionWithRebuild => "Projection" {
        scalar "name": TypeRef::named_nn(TypeRef::STRING) => |p| Value::from(p.projection.name.clone()),
        scalar "schema": TypeRef::named_nn(TypeRef::STRING) => |p| Value::from(p.projection.schema.clone()),
        scalar "schemaVersion": TypeRef::named_nn(TypeRef::INT) => |p| Value::from(p.projection.schema_version),
        list "consumedEventTypes": TypeRef::named_nn_list_nn("EventType") => |p| p.projection.consumed_event_types.clone(),
        scalar "sync": TypeRef::named_nn(TypeRef::BOOLEAN) => |p| Value::from(p.projection.sync),
        scalar "caughtUpTo": TypeRef::named(TypeRef::INT) => |p| match p.projection.caught_up_to {
            Some(seq) => Value::from(seq),
            None => Value::Null,
        },
        object "pendingRebuild": TypeRef::named("ProjectionRebuild") => |p| p.pending_rebuild.clone(),
        object "buildingRebuild": TypeRef::named("ProjectionRebuild") => |p| p.building_rebuild.clone(),
    })
}

/// `RegisterProjection`'s own three-way outcome
/// (`event_store::ProjectionRegistration`'s `Created`/`RebuildStaged`/
/// `ReconciledTrivially`), made explicit on the wire rather than
/// collapsed - `outcome` names which of `projection`/`rebuild` is
/// populated (`Created`/`ReconciledTrivially` fill `projection`,
/// `RebuildStaged` fills `rebuild`; the other is always null).
pub struct ProjectionRegistrationResult {
    pub outcome: &'static str,
    pub projection: Option<ProjectionWithRebuild>,
    pub rebuild: Option<ProjectionRebuild>,
}

pub fn projection_registration_result_object() -> Object {
    gql_object!(ProjectionRegistrationResult => "ProjectionRegistrationResult" {
        scalar "outcome": TypeRef::named_nn(TypeRef::STRING) => |r| Value::from(r.outcome),
        object "projection": TypeRef::named("Projection") => |r| r.projection.clone(),
        object "rebuild": TypeRef::named("ProjectionRebuild") => |r| r.rebuild.clone(),
    })
}

/// Codeberg issue #21 - see `skilj_core::db::ParkedDeliveryKind`'s own
/// doc comment for what each variant means and which other
/// `ParkedDelivery` fields it implies are populated.
pub fn parked_delivery_kind_name(kind: skilj_core::db::ParkedDeliveryKind) -> &'static str {
    match kind {
        skilj_core::db::ParkedDeliveryKind::CrossContextRoute => "CROSS_CONTEXT_ROUTE",
        skilj_core::db::ParkedDeliveryKind::ExternalEvent => "EXTERNAL_EVENT",
        skilj_core::db::ParkedDeliveryKind::CommandTrigger => "COMMAND_TRIGGER",
    }
}

pub fn parked_delivery_kind_enum() -> Enum {
    Enum::new("ParkedDeliveryKind")
        .item("CROSS_CONTEXT_ROUTE")
        .item("EXTERNAL_EVENT")
        .item("COMMAND_TRIGGER")
}

/// `entity ParkedDelivery` (no spec entity backs this - see
/// `skilj_core::db::ParkedDelivery`'s own doc comment). `requestJson` is
/// `Value::from(serde_json::to_string(...))`, the same "deliberately
/// coarse on the wire contract" `String`-carrying-JSON treatment
/// `EventDto.payload`/`CommandDto.payload` already use, rather than a
/// native GraphQL JSON scalar this schema has never needed elsewhere.
/// `targetBoundedContext`/`targetCommandType` are populated only for
/// `kind: CROSS_CONTEXT_ROUTE`; `accessTokenId` only for `EXTERNAL_EVENT`/
/// `COMMAND_TRIGGER` - see `ParkedDeliveryKind`'s own doc comment for the
/// full split.
pub fn parked_delivery_object() -> Object {
    gql_object!(skilj_core::db::ParkedDelivery => "ParkedDelivery" {
        scalar "id": TypeRef::named_nn(TypeRef::STRING) => |d| Value::from(d.id.clone()),
        scalar "source": TypeRef::named_nn(TypeRef::STRING) => |d| Value::from(d.source.clone()),
        scalar "kind": TypeRef::named_nn("ParkedDeliveryKind") => |d| Value::from(parked_delivery_kind_name(d.kind)),
        scalar "identifier": TypeRef::named_nn(TypeRef::STRING) => |d| Value::from(d.identifier.clone()),
        scalar "accessTokenId": TypeRef::named(TypeRef::STRING) => |d| optional_string(d.access_token_id.clone()),
        scalar "targetBoundedContext": TypeRef::named(TypeRef::STRING) => |d| optional_string(d.target_bounded_context.clone()),
        scalar "targetCommandType": TypeRef::named(TypeRef::STRING) => |d| optional_string(d.target_command_type.clone()),
        scalar "requestJson": TypeRef::named_nn(TypeRef::STRING) => |d| Value::from(
            serde_json::to_string(&d.request_json).unwrap_or_default()
        ),
        scalar "error": TypeRef::named_nn(TypeRef::STRING) => |d| Value::from(d.error.clone()),
        scalar "attemptCount": TypeRef::named_nn(TypeRef::INT) => |d| Value::from(d.attempt_count),
        scalar "firstFailedAt": TypeRef::named_nn(TypeRef::STRING) => |d| Value::from(d.first_failed_at.to_rfc3339()),
        scalar "lastFailedAt": TypeRef::named_nn(TypeRef::STRING) => |d| Value::from(d.last_failed_at.to_rfc3339()),
    })
}

/// One `Object` per `AccessToken` variant, sharing the same base fields
/// (`id`/`secret`/`status`/`createdAt`/`revokedAt`/`scope`) plus one
/// type-specific nested field - the same "the enum variant tag is the
/// GraphQL type" treatment the `AccessToken` union below relies on.
/// `scope` used to be `EventReadToken`'s alone (cross-tenant read fix,
/// docs/architecture.md's own write-up of these passes) but was never
/// actually exposed here even for that one kind - a real admin read-back
/// gap this macro closes for all four at once, the write-side pass
/// giving `ExternalEventToken`/`DirectCreationToken`/`CommandToken` the
/// same Rust field made it obviously wrong to leave any of the four
/// unreadable, the same "why leave three gaps when one fix closes all
/// four" reasoning `[§26](../../docs/architecture.md#admin-read-back-owner-tag-key)`'s own admin read-back fix already applied to
/// `owner_tag_key`.
macro_rules! token_object {
    ($object_name:literal, $rust_type:ty, $scoped_field_name:literal, $scoped_type:literal, $scoped_accessor:expr) => {
        Object::new($object_name)
            .field(scalar_field(
                "id",
                TypeRef::named_nn(TypeRef::ID),
                |t: &$rust_type| Value::from(t.id.clone()),
            ))
            .field(scalar_field(
                "secret",
                TypeRef::named_nn(TypeRef::STRING),
                |t: &$rust_type| Value::from(t.secret.clone()),
            ))
            .field(scalar_field(
                "status",
                TypeRef::named_nn("AccessTokenStatus"),
                |t: &$rust_type| Value::from(access_token_status_name(t.status)),
            ))
            .field(scalar_field(
                "createdAt",
                TypeRef::named_nn(TypeRef::STRING),
                |t: &$rust_type| Value::from(t.created_at.to_rfc3339()),
            ))
            .field(scalar_field(
                "revokedAt",
                TypeRef::named(TypeRef::STRING),
                |t: &$rust_type| optional_timestamp(t.revoked_at),
            ))
            .field(scalar_field(
                "scope",
                TypeRef::named(TypeRef::STRING),
                |t: &$rust_type| optional_string(t.scope.clone()),
            ))
            .field(object_field(
                $scoped_field_name,
                TypeRef::named_nn($scoped_type),
                $scoped_accessor,
            ))
    };
}

pub fn external_event_token_object() -> Object {
    token_object!(
        "ExternalEventToken",
        ExternalEventToken,
        "eventType",
        "EventType",
        |t: &ExternalEventToken| Some(t.event_type.clone())
    )
}

pub fn direct_creation_token_object() -> Object {
    token_object!(
        "DirectCreationToken",
        DirectCreationToken,
        "eventType",
        "EventType",
        |t: &DirectCreationToken| Some(t.event_type.clone())
    )
}

/// The one field `token_object!` doesn't give this type - `startFrom`,
/// appended directly rather than folded into the macro since it's the
/// only one of the four token kinds that has it (`EventReadToken.
/// start_from`'s own doc comment).
pub fn event_read_token_object() -> Object {
    token_object!(
        "EventReadToken",
        EventReadToken,
        "eventType",
        "EventType",
        |t: &EventReadToken| Some(t.event_type.clone())
    )
    .field(scalar_field(
        "startFrom",
        TypeRef::named_nn("EventReadStartPosition"),
        |t: &EventReadToken| Value::from(event_read_start_position_name(t.start_from)),
    ))
    .field(scalar_field(
        "startAtSequence",
        TypeRef::named(TypeRef::INT),
        |t: &EventReadToken| match t.start_at_sequence {
            Some(seq) => Value::from(seq),
            None => Value::Null,
        },
    ))
    .field(scalar_field(
        "startAtTime",
        TypeRef::named(TypeRef::STRING),
        |t: &EventReadToken| optional_timestamp(t.start_at_time),
    ))
}

pub fn command_token_object() -> Object {
    token_object!(
        "CommandToken",
        CommandToken,
        "commandType",
        "CommandType",
        |t: &CommandToken| Some(t.command_type.clone())
    )
}

/// `entity AccessToken`'s `purpose` sum type - a GraphQL union over the
/// four variant objects above, the same role the Rust
/// `access_control::AccessToken` enum plays. `RevokeToken`'s own return
/// type: the one place this crate needs to hand back "any `AccessToken`
/// variant" polymorphically, mirroring why `access_control::AccessToken`
/// exists as its own Rust type in the first place.
pub fn access_token_union() -> Union {
    Union::new("AccessToken")
        .possible_type("ExternalEventToken")
        .possible_type("DirectCreationToken")
        .possible_type("EventReadToken")
        .possible_type("CommandToken")
}

// ---------------------------------------------------------------------
// Phase 3 additions: EventQuery, CommandQuery, CommandSubmission.
// ---------------------------------------------------------------------

pub fn tag_input() -> InputObject {
    InputObject::new("TagInput")
        .field(InputValue::new("key", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new("value", TypeRef::named(TypeRef::STRING)))
}

/// `queryEvents`' own per-row shape - `event_store::query_events`'s
/// return type is `Vec<(i64, String)>` (sequence paired with the
/// rendered payload, see that function's own doc comment for why the
/// sequence travels with it now), so `(i64, String)` is this object's
/// parent value directly - no wrapper struct needed.
pub fn queried_event_object() -> Object {
    gql_object!((i64, String) => "QueriedEvent" {
        scalar "sequence": TypeRef::named_nn(TypeRef::INT) => |e| Value::from(e.0),
        scalar "payload": TypeRef::named_nn(TypeRef::STRING) => |e| Value::from(e.1.clone()),
    })
}

/// `fetchCommands`' per-command shape: `(id, createdAt, rendered payload)`.
/// `id` is the command's own `Command.id` - what a caller passes back as
/// `afterCommandId` to get the next page (rule `FetchCommands`'
/// `after_command`).
pub fn queried_command_object() -> Object {
    gql_object!((String, String, String) => "QueriedCommand" {
        scalar "id": TypeRef::named_nn(TypeRef::STRING) => |c| Value::from(c.0.clone()),
        scalar "createdAt": TypeRef::named_nn(TypeRef::STRING) => |c| Value::from(c.1.clone()),
        scalar "payload": TypeRef::named_nn(TypeRef::STRING) => |c| Value::from(c.2.clone()),
    })
}

/// `SubmitCommandResult.matchingEvents`' own per-row shape (Codeberg
/// issue #7's DCB conflict visualizer) - a real `Event`, not
/// `QueriedEvent`'s `(i64, String)` pair, since `eventTypeName` matters
/// here in a way it doesn't for `queryEvents` (a caller there already
/// filtered by event type; a rejection's matching set can span several).
/// Deliberately narrow - `sequence`/`eventTypeName`/`payload` only,
/// enough to actually debug a conflict without inventing a bigger wire
/// type than the issue asks for.
pub fn matching_event_object() -> Object {
    use skilj_core::event_store::Event;

    gql_object!(Event => "MatchingEvent" {
        scalar "sequence": TypeRef::named_nn(TypeRef::INT) => |e| Value::from(e.sequence),
        scalar "eventTypeName": TypeRef::named_nn(TypeRef::STRING) => |e| Value::from(e.event_type.name.clone()),
        scalar "payload": TypeRef::named_nn(TypeRef::STRING) => |e| Value::from(e.payload.clone()),
    })
}

/// `entity Event`'s `origin` field - a flat "kind + nullable per-variant
/// fields" shape, the same treatment `ContextCreator` already got in
/// Phase 1, not a GraphQL union: nothing here needs the return type
/// itself to vary structurally the way `TokenRevocation`'s `AccessToken`
/// does.
pub fn event_origin_object() -> Object {
    use skilj_core::event_store::EventOrigin;

    gql_object!(EventOrigin => "EventOrigin" {
        scalar "kind": TypeRef::named_nn(TypeRef::STRING) => |o| {
            Value::from(match o {
                EventOrigin::ExternalTriggered { .. } => "EXTERNAL_TRIGGERED",
                EventOrigin::DirectlyCreated => "DIRECTLY_CREATED",
                EventOrigin::SystemTriggered => "SYSTEM_TRIGGERED",
                EventOrigin::CommandTriggered { .. } => "COMMAND_TRIGGERED",
            })
        },
        scalar "sourceContent": TypeRef::named(TypeRef::STRING) => |o| match o {
            EventOrigin::ExternalTriggered { source_content, .. } => {
                Value::from(source_content.clone())
            }
            _ => Value::Null,
        },
        scalar "sourceContext": TypeRef::named(TypeRef::STRING) => |o| match o {
            EventOrigin::ExternalTriggered { source_context, .. } => {
                optional_string(source_context.clone())
            }
            _ => Value::Null,
        },
        object "triggeringCommandType": TypeRef::named("CommandType") => |o| match o {
            EventOrigin::CommandTriggered { command } => Some(command.command_type.clone()),
            _ => None,
        },
        scalar "triggeringCommandPayload": TypeRef::named(TypeRef::STRING) => |o| match o {
            EventOrigin::CommandTriggered { command } => Value::from(command.payload.clone()),
            _ => Value::Null,
        },
    })
}

fn optional_string(s: Option<String>) -> Value {
    match s {
        Some(s) => Value::from(s),
        None => Value::Null,
    }
}

/// `InspectEvent`'s own `exposes:` list - `event.metadata.created_at`,
/// `event.origin` - nothing more. The parent value is a plain
/// `event_store::Event` directly; no wrapper struct needed.
pub fn event_meta_object() -> Object {
    use skilj_core::event_store::Event;

    gql_object!(Event => "EventMeta" {
        scalar "createdAt": TypeRef::named_nn(TypeRef::STRING) => |e| Value::from(e.metadata.created_at.to_rfc3339()),
        // Codeberg issue #18. `correlationId` is null only for a record
        // written before this field existed.
        scalar "correlationId": TypeRef::named(TypeRef::STRING) => |e| optional_string(e.metadata.correlation_id.clone()),
        scalar "causationId": TypeRef::named(TypeRef::STRING) => |e| optional_string(e.metadata.causation_id.clone()),
        object "origin": TypeRef::named_nn("EventOrigin") => |e| Some(e.origin.clone()),
    })
}

/// `rule InspectEvent`'s own `ensures` - `event` and `rendered_payload`
/// delivered alongside each other (see `event_store::EventInspected`'s
/// own doc comment, which this mirrors on the wire).
pub struct InspectedEventData {
    pub event: skilj_core::event_store::Event,
    pub rendered_payload: String,
}

pub fn inspected_event_object() -> Object {
    gql_object!(InspectedEventData => "InspectedEvent" {
        object "event": TypeRef::named_nn("EventMeta") => |i| Some(i.event.clone()),
        scalar "renderedPayload": TypeRef::named_nn(TypeRef::STRING) => |i| Value::from(i.rendered_payload.clone()),
    })
}

/// `inspectSnapshot`'s own response shape ([docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)) -
/// a real, stored `{schema}.snapshots` row, resolved by
/// `resolvers::snapshot_query`. `state` is the raw JSON a `Snapshot`
/// impl's own `fold()` produced, not decoded any further here - this
/// endpoint has no compiled `Snapshot::State` type to decode it into,
/// only the in-process `SnapshotDispatcher` registry's own `tag_key`/
/// `version` (see that resolver's own doc comment).
pub struct InspectedSnapshotData {
    pub tag_key: String,
    pub tag_value: String,
    pub version: i64,
    pub as_of_sequence: i64,
    pub state: String,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

pub fn inspected_snapshot_object() -> Object {
    gql_object!(InspectedSnapshotData => "InspectedSnapshot" {
        scalar "tagKey": TypeRef::named_nn(TypeRef::STRING) => |i| Value::from(i.tag_key.clone()),
        scalar "tagValue": TypeRef::named_nn(TypeRef::STRING) => |i| Value::from(i.tag_value.clone()),
        scalar "version": TypeRef::named_nn(TypeRef::INT) => |i| Value::from(i.version),
        scalar "asOfSequence": TypeRef::named_nn(TypeRef::INT) => |i| Value::from(i.as_of_sequence),
        scalar "state": TypeRef::named_nn(TypeRef::STRING) => |i| Value::from(i.state.clone()),
        scalar "updatedAt": TypeRef::named_nn(TypeRef::STRING) => |i| Value::from(i.updated_at.to_rfc3339()),
    })
}

/// `SubmitCommand`'s own response shape - docs/architecture.md §5.4's
/// already-committed `SubmitCommandPayload`: business rejections surface
/// as ordinary typed data here, never through GraphQL's real `errors`
/// array (that's reserved for library-level errors, rendered through
/// `to_graphql_error` as everywhere else in this crate).
pub struct SubmitCommandResult {
    pub accepted: bool,
    pub triggered_event_sequences: Option<Vec<i64>>,
    pub rejection_reason: Option<String>,
    pub rejection_kind: Option<String>,
    // Codeberg issue #7's DCB conflict visualizer - `Some` only on
    // rejection (see the resolver's own construction of this struct for
    // why `Accepted` gets `None`), the tag-scoped set `decide()`
    // actually evaluated against for the rejection that governed.
    pub matching_events: Option<Vec<skilj_core::event_store::Event>>,
    // Codeberg issue #12: `true` only when an `idempotencyKey` was given
    // and it matched a prior `Accepted` outcome - `triggered_event_sequences`
    // is that prior outcome's, not a fresh decision. Always `false` when
    // no key was given, matching today's behaviour exactly.
    pub deduplicated: bool,
    // Codeberg issue #18: the resulting Command's own correlation_id -
    // the caller's, if it supplied one, or generated server-side
    // otherwise. `None` for a rejection (nothing was stored) and for a
    // deduplicated outcome (the cached prior answer doesn't carry it -
    // see the resolver's own construction of this struct).
    pub correlation_id: Option<String>,
}

pub fn submit_command_payload_object() -> Object {
    // `triggeredEventSequences` is a nullable *list of scalars*, and
    // `matchingEvents` a nullable *list of objects* -
    // `scalar_field`/`object_field`/`list_field` (and so `gql_object!`
    // itself) only cover a bare scalar, a nullable nested object, and a
    // non-null list of nested objects; neither shape fits, so both stay
    // hand-written `Field::new(...)`s, appended after the macro-generated
    // ones below.
    gql_object!(SubmitCommandResult => "SubmitCommandPayload" {
        scalar "accepted": TypeRef::named_nn(TypeRef::BOOLEAN) => |r| Value::from(r.accepted),
        scalar "rejectionReason": TypeRef::named(TypeRef::STRING) => |r| optional_string(r.rejection_reason.clone()),
        scalar "rejectionKind": TypeRef::named(TypeRef::STRING) => |r| optional_string(r.rejection_kind.clone()),
        scalar "deduplicated": TypeRef::named_nn(TypeRef::BOOLEAN) => |r| Value::from(r.deduplicated),
        scalar "correlationId": TypeRef::named(TypeRef::STRING) => |r| optional_string(r.correlation_id.clone()),
    })
    .field(Field::new(
        "triggeredEventSequences",
        TypeRef::named_list(TypeRef::INT),
        |ctx: ResolverContext| {
            FieldFuture::new(async move {
                let parent = ctx.parent_value.try_downcast_ref::<SubmitCommandResult>()?;
                Ok(parent.triggered_event_sequences.as_ref().map(|sequences| {
                    FieldValue::list(sequences.iter().map(|s| FieldValue::value(Value::from(*s))))
                }))
            })
        },
    ))
    .field(Field::new(
        "matchingEvents",
        TypeRef::named_list("MatchingEvent"),
        |ctx: ResolverContext| {
            FieldFuture::new(async move {
                let parent = ctx.parent_value.try_downcast_ref::<SubmitCommandResult>()?;
                Ok(parent.matching_events.as_ref().map(|events| {
                    FieldValue::list(events.iter().cloned().map(FieldValue::owned_any))
                }))
            })
        },
    ))
}

pub fn encryption_key_status_name(status: EncryptionKeyStatus) -> &'static str {
    match status {
        EncryptionKeyStatus::Active => "ACTIVE",
        EncryptionKeyStatus::Destroyed => "DESTROYED",
    }
}

pub fn encryption_key_status_enum() -> Enum {
    Enum::new("EncryptionKeyStatus")
        .item("ACTIVE")
        .item("DESTROYED")
}

/// `entity EncryptionKey`, minus `bounded_context` (already implied by
/// the query that scoped the lookup, the same treatment `RoleAccessMapping`
/// gives its own `bounded_context` field) and any key material - that
/// never crosses this boundary at all, see `skilj_core::encryption`'s own
/// doc comment.
pub fn encryption_key_object() -> Object {
    gql_object!(EncryptionKey => "EncryptionKey" {
        scalar "subjectKey": TypeRef::named_nn(TypeRef::STRING) => |k| Value::from(k.subject_key.clone()),
        scalar "subjectValue": TypeRef::named_nn(TypeRef::STRING) => |k| Value::from(k.subject_value.clone()),
        scalar "status": TypeRef::named_nn("EncryptionKeyStatus") => |k| Value::from(encryption_key_status_name(k.status)),
        scalar "createdAt": TypeRef::named_nn(TypeRef::STRING) => |k| Value::from(k.created_at.to_rfc3339()),
        scalar "destroyedAt": TypeRef::named(TypeRef::STRING) => |k| optional_timestamp(k.destroyed_at),
    })
}

/// `value Filter`'s own `operator` field. `resolvers::event_subscription`'s
/// own `eventsByType(filters: ...)` is this type's only user - real wire
/// shape (matching `CreateEventTypeSubscription`'s own signature
/// faithfully) and real behaviour end to end now that `matches_filters`/
/// `valid_filters` are real.
pub fn filter_operator_enum() -> Enum {
    Enum::new("FilterOperator")
        .item("EQUALS")
        .item("CONTAINS")
        .item("IS_LIKE")
        .item("GREATER_THAN")
        .item("LESS_THAN")
        .item("NEAR")
        .item("SIMILAR_COLOR")
        .item("IN_SUBNET")
        .item("IN")
}

pub fn filter_input() -> InputObject {
    InputObject::new("FilterInput")
        .field(InputValue::new("field", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new(
            "operator",
            TypeRef::named_nn("FilterOperator"),
        ))
        .field(InputValue::new("value", TypeRef::named_nn(TypeRef::STRING)))
}
