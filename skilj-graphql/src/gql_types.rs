//! The static GraphQL types Phase 1's admin-console surfaces share -
//! `Role`, `BoundedContext` (+ its nested `ContextCreator`/
//! `RoleAccessMapping`), the three enums, and `CreatedSuperadmin` (the
//! deliberately narrower payload `createSuperadmin` itself returns - see
//! its own doc comment). Every one of these is built directly against
//! `async_graphql::dynamic`'s own `Object`/`Field`/`Enum` API (§5.1) -
//! there is no bridge from `#[derive(SimpleObject)]`-style static types
//! into a `dynamic::Schema` in this version of `async-graphql`, so
//! nothing here uses derive macros at all, unlike `skilj-core::plugin`'s
//! own `schemars`-derived payload schemas.
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
    AccessLevel, CommandToken, DirectCreationToken, EventReadToken, ExternalEventToken, Role,
    RoleAccessMapping, RoleStatus, TokenStatus,
};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, CommandType, EventType};
use skilj_core::projections::{Projection, ProjectionRebuild, ProjectionRebuildStatus};
use skilj_core::shared::{SensitiveField, TagMapping};

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

/// `entity Role`, minus the relationship projection `access_mappings`
/// (resolved separately, per surface, the same "caller resolves it, not
/// a stored field" treatment every relationship projection gets
/// elsewhere in this codebase).
pub fn role_object() -> Object {
    Object::new("Role")
        .field(scalar_field(
            "id",
            TypeRef::named_nn(TypeRef::ID),
            |r: &Role| Value::from(r.id.clone()),
        ))
        .field(scalar_field(
            "externalSubject",
            TypeRef::named_nn(TypeRef::STRING),
            |r: &Role| Value::from(r.external_subject.clone()),
        ))
        .field(scalar_field(
            "name",
            TypeRef::named_nn(TypeRef::STRING),
            |r: &Role| Value::from(r.name.clone()),
        ))
        .field(scalar_field(
            "superadmin",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |r: &Role| Value::from(r.superadmin),
        ))
        .field(scalar_field(
            "status",
            TypeRef::named_nn("RoleStatus"),
            |r: &Role| Value::from(role_status_name(r.status)),
        ))
        .field(scalar_field(
            "createdAt",
            TypeRef::named_nn(TypeRef::STRING),
            |r: &Role| Value::from(r.created_at.to_rfc3339()),
        ))
        .field(scalar_field(
            "revokedAt",
            TypeRef::named(TypeRef::STRING),
            |r: &Role| optional_timestamp(r.revoked_at),
        ))
}

/// `createSuperadmin`'s own deliberately narrow return type -
/// `NoCredentialIssued` (`surface SuperadminBootstrap`): "hands back only
/// the created Role's own id, name and external_subject". A dedicated
/// type rather than reusing `Role` so that guarantee is structural, not
/// a convention a resolver could accidentally violate by returning more.
pub fn created_superadmin_object() -> Object {
    Object::new("CreatedSuperadmin")
        .field(scalar_field(
            "id",
            TypeRef::named_nn(TypeRef::ID),
            |r: &Role| Value::from(r.id.clone()),
        ))
        .field(scalar_field(
            "name",
            TypeRef::named_nn(TypeRef::STRING),
            |r: &Role| Value::from(r.name.clone()),
        ))
        .field(scalar_field(
            "externalSubject",
            TypeRef::named_nn(TypeRef::STRING),
            |r: &Role| Value::from(r.external_subject.clone()),
        ))
}

/// `entity RoleAccessMapping`, minus `bounded_context` - every place
/// this object appears it's already nested under the bounded context it
/// belongs to (see `surface BoundedContextDirectory`'s own `exposes`
/// list, which never repeats it either).
pub fn role_access_mapping_object() -> Object {
    Object::new("RoleAccessMapping")
        .field(object_field(
            "role",
            TypeRef::named_nn("Role"),
            |m: &RoleAccessMapping| Some(m.role.clone()),
        ))
        .field(scalar_field(
            "level",
            TypeRef::named_nn("AccessLevel"),
            |m: &RoleAccessMapping| Value::from(access_level_name(m.level)),
        ))
        .field(scalar_field(
            "canReadSensitive",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |m: &RoleAccessMapping| Value::from(m.can_read_sensitive),
        ))
        .field(scalar_field(
            "status",
            TypeRef::named_nn("RoleStatus"),
            |m: &RoleAccessMapping| Value::from(role_status_name(m.status)),
        ))
}

/// `entity ContextCreator`. `kind` stands in for the Rust sum type's own
/// variant tag (the same "the enum variant tag is the field" treatment
/// the Rust side already gives it - see `ContextCreator`'s own doc
/// comment); `role` is present only for `SUPERADMIN`.
pub fn context_creator_object() -> Object {
    Object::new("ContextCreator")
        .field(scalar_field(
            "kind",
            TypeRef::named_nn(TypeRef::STRING),
            |c: &ContextCreator| {
                Value::from(match c {
                    ContextCreator::SuperadminCreator { .. } => "SUPERADMIN",
                    ContextCreator::SystemCreator => "SYSTEM",
                })
            },
        ))
        .field(object_field(
            "role",
            TypeRef::named("Role"),
            |c: &ContextCreator| match c {
                ContextCreator::SuperadminCreator { role } => Some(role.clone()),
                ContextCreator::SystemCreator => None,
            },
        ))
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
    Object::new("BoundedContext")
        .field(scalar_field(
            "name",
            TypeRef::named_nn(TypeRef::STRING),
            |bc: &BoundedContextWithMappings| Value::from(bc.context.name.clone()),
        ))
        .field(scalar_field(
            "status",
            TypeRef::named_nn("BoundedContextStatus"),
            |bc: &BoundedContextWithMappings| {
                Value::from(bounded_context_status_name(bc.context.status))
            },
        ))
        .field(scalar_field(
            "createdAt",
            TypeRef::named_nn(TypeRef::STRING),
            |bc: &BoundedContextWithMappings| Value::from(bc.context.created_at.to_rfc3339()),
        ))
        .field(object_field(
            "createdBy",
            TypeRef::named_nn("ContextCreator"),
            |bc: &BoundedContextWithMappings| Some(bc.context.created_by.clone()),
        ))
        .field(list_field(
            "accessMappings",
            TypeRef::named_nn_list_nn("RoleAccessMapping"),
            |bc: &BoundedContextWithMappings| bc.access_mappings.clone(),
        ))
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
    Object::new("TagMapping")
        .field(scalar_field(
            "key",
            TypeRef::named_nn(TypeRef::STRING),
            |t: &TagMapping| Value::from(t.key.clone()),
        ))
        .field(scalar_field(
            "field",
            TypeRef::named_nn(TypeRef::STRING),
            |t: &TagMapping| Value::from(t.field.clone()),
        ))
}

pub fn tag_mapping_input() -> InputObject {
    InputObject::new("TagMappingInput")
        .field(InputValue::new("key", TypeRef::named_nn(TypeRef::STRING)))
        .field(InputValue::new("field", TypeRef::named_nn(TypeRef::STRING)))
}

/// `value SensitiveField`. See `tag_mapping_object`'s own doc comment -
/// same input/output split.
pub fn sensitive_field_object() -> Object {
    Object::new("SensitiveField")
        .field(scalar_field(
            "field",
            TypeRef::named_nn(TypeRef::STRING),
            |s: &SensitiveField| Value::from(s.field.clone()),
        ))
        .field(scalar_field(
            "subjectKey",
            TypeRef::named_nn(TypeRef::STRING),
            |s: &SensitiveField| Value::from(s.subject_key.clone()),
        ))
        .field(scalar_field(
            "subjectField",
            TypeRef::named_nn(TypeRef::STRING),
            |s: &SensitiveField| Value::from(s.subject_field.clone()),
        ))
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

/// `entity EventType`, minus the relationship projections
/// (`*_tokens`) - the same "caller resolves it, not a stored field"
/// treatment every relationship projection gets elsewhere in this
/// codebase.
pub fn event_type_object() -> Object {
    Object::new("EventType")
        .field(scalar_field(
            "name",
            TypeRef::named_nn(TypeRef::STRING),
            |et: &EventType| Value::from(et.name.clone()),
        ))
        .field(scalar_field(
            "schema",
            TypeRef::named_nn(TypeRef::STRING),
            |et: &EventType| Value::from(et.schema.clone()),
        ))
        .field(scalar_field(
            "schemaVersion",
            TypeRef::named_nn(TypeRef::INT),
            |et: &EventType| Value::from(et.schema_version),
        ))
        .field(list_field(
            "tagMappings",
            TypeRef::named_nn_list_nn("TagMapping"),
            |et: &EventType| et.tag_mappings.clone(),
        ))
        .field(list_field(
            "sensitiveFields",
            TypeRef::named_nn_list_nn("SensitiveField"),
            |et: &EventType| et.sensitive_fields.clone(),
        ))
        .field(scalar_field(
            "externalCreationAllowed",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |et: &EventType| Value::from(et.external_creation_allowed),
        ))
        .field(scalar_field(
            "directCreationAllowed",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |et: &EventType| Value::from(et.direct_creation_allowed),
        ))
        .field(scalar_field(
            "systemTriggeredAllowed",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |et: &EventType| Value::from(et.system_triggered_allowed),
        ))
        .field(scalar_field(
            "systemTriggeredSchedule",
            TypeRef::named(TypeRef::STRING),
            |et: &EventType| match &et.system_triggered_schedule {
                Some(s) => Value::from(s.clone()),
                None => Value::Null,
            },
        ))
        .field(scalar_field(
            "eventReadAllowed",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |et: &EventType| Value::from(et.event_read_allowed),
        ))
}

/// `entity CommandType`, same treatment as `event_type_object` above.
pub fn command_type_object() -> Object {
    Object::new("CommandType")
        .field(scalar_field(
            "name",
            TypeRef::named_nn(TypeRef::STRING),
            |ct: &CommandType| Value::from(ct.name.clone()),
        ))
        .field(scalar_field(
            "schema",
            TypeRef::named_nn(TypeRef::STRING),
            |ct: &CommandType| Value::from(ct.schema.clone()),
        ))
        .field(scalar_field(
            "schemaVersion",
            TypeRef::named_nn(TypeRef::INT),
            |ct: &CommandType| Value::from(ct.schema_version),
        ))
        .field(list_field(
            "tagMappings",
            TypeRef::named_nn_list_nn("TagMapping"),
            |ct: &CommandType| ct.tag_mappings.clone(),
        ))
        .field(list_field(
            "sensitiveFields",
            TypeRef::named_nn_list_nn("SensitiveField"),
            |ct: &CommandType| ct.sensitive_fields.clone(),
        ))
        .field(scalar_field(
            "restTriggerAllowed",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |ct: &CommandType| Value::from(ct.rest_trigger_allowed),
        ))
}

/// `entity ProjectionRebuild`.
pub fn projection_rebuild_object() -> Object {
    Object::new("ProjectionRebuild")
        .field(scalar_field(
            "schema",
            TypeRef::named_nn(TypeRef::STRING),
            |r: &ProjectionRebuild| Value::from(r.schema.clone()),
        ))
        .field(scalar_field(
            "schemaVersion",
            TypeRef::named_nn(TypeRef::INT),
            |r: &ProjectionRebuild| Value::from(r.schema_version),
        ))
        .field(list_field(
            "consumedEventTypes",
            TypeRef::named_nn_list_nn("EventType"),
            |r: &ProjectionRebuild| r.consumed_event_types.clone(),
        ))
        .field(scalar_field(
            "sync",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |r: &ProjectionRebuild| Value::from(r.sync),
        ))
        .field(scalar_field(
            "caughtUpTo",
            TypeRef::named(TypeRef::INT),
            |r: &ProjectionRebuild| match r.caught_up_to {
                Some(seq) => Value::from(seq),
                None => Value::Null,
            },
        ))
        .field(scalar_field(
            "status",
            TypeRef::named_nn("ProjectionRebuildStatus"),
            |r: &ProjectionRebuild| Value::from(projection_rebuild_status_name(r.status)),
        ))
}

/// `entity Projection`. `rebuild` is nullable - `ProjectionRebuild` is
/// keyed 1:1 by `(bounded_context, projection_name)` (see the migration's
/// own doc comment), so despite the spec's own `exposes: ... for rebuild
/// in projection.rebuilds` loop phrasing, there is never more than one at
/// a time; modelled here as a nullable field rather than a list, the
/// wire-shape simplification `TypeRegistration`'s own guidance explicitly
/// leaves open ("deliberately coarse on the wire contract").
#[derive(Clone)]
pub struct ProjectionWithRebuild {
    pub projection: Projection,
    pub rebuild: Option<ProjectionRebuild>,
}

pub fn projection_object() -> Object {
    Object::new("Projection")
        .field(scalar_field(
            "name",
            TypeRef::named_nn(TypeRef::STRING),
            |p: &ProjectionWithRebuild| Value::from(p.projection.name.clone()),
        ))
        .field(scalar_field(
            "schema",
            TypeRef::named_nn(TypeRef::STRING),
            |p: &ProjectionWithRebuild| Value::from(p.projection.schema.clone()),
        ))
        .field(scalar_field(
            "schemaVersion",
            TypeRef::named_nn(TypeRef::INT),
            |p: &ProjectionWithRebuild| Value::from(p.projection.schema_version),
        ))
        .field(list_field(
            "consumedEventTypes",
            TypeRef::named_nn_list_nn("EventType"),
            |p: &ProjectionWithRebuild| p.projection.consumed_event_types.clone(),
        ))
        .field(scalar_field(
            "sync",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |p: &ProjectionWithRebuild| Value::from(p.projection.sync),
        ))
        .field(scalar_field(
            "caughtUpTo",
            TypeRef::named(TypeRef::INT),
            |p: &ProjectionWithRebuild| match p.projection.caught_up_to {
                Some(seq) => Value::from(seq),
                None => Value::Null,
            },
        ))
        .field(object_field(
            "rebuild",
            TypeRef::named("ProjectionRebuild"),
            |p: &ProjectionWithRebuild| p.rebuild.clone(),
        ))
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
    Object::new("ProjectionRegistrationResult")
        .field(scalar_field(
            "outcome",
            TypeRef::named_nn(TypeRef::STRING),
            |r: &ProjectionRegistrationResult| Value::from(r.outcome),
        ))
        .field(object_field(
            "projection",
            TypeRef::named("Projection"),
            |r: &ProjectionRegistrationResult| r.projection.clone(),
        ))
        .field(object_field(
            "rebuild",
            TypeRef::named("ProjectionRebuild"),
            |r: &ProjectionRegistrationResult| r.rebuild.clone(),
        ))
}

/// One `Object` per `AccessToken` variant, sharing the same base fields
/// (`id`/`secret`/`status`/`createdAt`/`revokedAt`) plus one type-specific
/// nested field - the same "the enum variant tag is the GraphQL type"
/// treatment the `AccessToken` union below relies on.
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

pub fn event_read_token_object() -> Object {
    token_object!(
        "EventReadToken",
        EventReadToken,
        "eventType",
        "EventType",
        |t: &EventReadToken| Some(t.event_type.clone())
    )
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
    Object::new("QueriedEvent")
        .field(scalar_field(
            "sequence",
            TypeRef::named_nn(TypeRef::INT),
            |e: &(i64, String)| Value::from(e.0),
        ))
        .field(scalar_field(
            "payload",
            TypeRef::named_nn(TypeRef::STRING),
            |e: &(i64, String)| Value::from(e.1.clone()),
        ))
}

/// `entity Event`'s `origin` field - a flat "kind + nullable per-variant
/// fields" shape, the same treatment `ContextCreator` already got in
/// Phase 1, not a GraphQL union: nothing here needs the return type
/// itself to vary structurally the way `TokenRevocation`'s `AccessToken`
/// does.
pub fn event_origin_object() -> Object {
    use skilj_core::event_store::EventOrigin;

    Object::new("EventOrigin")
        .field(scalar_field(
            "kind",
            TypeRef::named_nn(TypeRef::STRING),
            |o: &EventOrigin| {
                Value::from(match o {
                    EventOrigin::ExternalTriggered { .. } => "EXTERNAL_TRIGGERED",
                    EventOrigin::DirectlyCreated => "DIRECTLY_CREATED",
                    EventOrigin::SystemTriggered => "SYSTEM_TRIGGERED",
                    EventOrigin::CommandTriggered { .. } => "COMMAND_TRIGGERED",
                })
            },
        ))
        .field(scalar_field(
            "sourceContent",
            TypeRef::named(TypeRef::STRING),
            |o: &EventOrigin| match o {
                EventOrigin::ExternalTriggered { source_content, .. } => {
                    Value::from(source_content.clone())
                }
                _ => Value::Null,
            },
        ))
        .field(scalar_field(
            "sourceContext",
            TypeRef::named(TypeRef::STRING),
            |o: &EventOrigin| match o {
                EventOrigin::ExternalTriggered { source_context, .. } => {
                    optional_string(source_context.clone())
                }
                _ => Value::Null,
            },
        ))
        .field(object_field(
            "triggeringCommandType",
            TypeRef::named("CommandType"),
            |o: &EventOrigin| match o {
                EventOrigin::CommandTriggered { command } => Some(command.command_type.clone()),
                _ => None,
            },
        ))
        .field(scalar_field(
            "triggeringCommandPayload",
            TypeRef::named(TypeRef::STRING),
            |o: &EventOrigin| match o {
                EventOrigin::CommandTriggered { command } => Value::from(command.payload.clone()),
                _ => Value::Null,
            },
        ))
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

    Object::new("EventMeta")
        .field(scalar_field(
            "createdAt",
            TypeRef::named_nn(TypeRef::STRING),
            |e: &Event| Value::from(e.metadata.created_at.to_rfc3339()),
        ))
        .field(object_field(
            "origin",
            TypeRef::named_nn("EventOrigin"),
            |e: &Event| Some(e.origin.clone()),
        ))
}

/// `rule InspectEvent`'s own `ensures` - `event` and `rendered_payload`
/// delivered alongside each other (see `event_store::EventInspected`'s
/// own doc comment, which this mirrors on the wire).
pub struct InspectedEventData {
    pub event: skilj_core::event_store::Event,
    pub rendered_payload: String,
}

pub fn inspected_event_object() -> Object {
    Object::new("InspectedEvent")
        .field(object_field(
            "event",
            TypeRef::named_nn("EventMeta"),
            |i: &InspectedEventData| Some(i.event.clone()),
        ))
        .field(scalar_field(
            "renderedPayload",
            TypeRef::named_nn(TypeRef::STRING),
            |i: &InspectedEventData| Value::from(i.rendered_payload.clone()),
        ))
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
}

pub fn submit_command_payload_object() -> Object {
    Object::new("SubmitCommandPayload")
        .field(scalar_field(
            "accepted",
            TypeRef::named_nn(TypeRef::BOOLEAN),
            |r: &SubmitCommandResult| Value::from(r.accepted),
        ))
        .field(Field::new(
            "triggeredEventSequences",
            TypeRef::named_list(TypeRef::INT),
            |ctx: ResolverContext| {
                FieldFuture::new(async move {
                    let parent = ctx.parent_value.try_downcast_ref::<SubmitCommandResult>()?;
                    Ok(parent.triggered_event_sequences.as_ref().map(|sequences| {
                        FieldValue::list(
                            sequences.iter().map(|s| FieldValue::value(Value::from(*s))),
                        )
                    }))
                })
            },
        ))
        .field(scalar_field(
            "rejectionReason",
            TypeRef::named(TypeRef::STRING),
            |r: &SubmitCommandResult| optional_string(r.rejection_reason.clone()),
        ))
        .field(scalar_field(
            "rejectionKind",
            TypeRef::named(TypeRef::STRING),
            |r: &SubmitCommandResult| optional_string(r.rejection_kind.clone()),
        ))
}
