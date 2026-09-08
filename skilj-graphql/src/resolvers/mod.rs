//! One module per GraphQL surface, matching `specs/skilj.allium`'s
//! surfaces section. Phase 1 ([docs/architecture.md §8](../../../docs/architecture.md#open-for-a-future-pass) item 5's own plan)
//! covers the superadmin admin console - the six static-typed,
//! `Superadmin`-facing surfaces: `superadmin_bootstrap`,
//! `access_management`, `bounded_context_creation`,
//! `bounded_context_directory`, `bounded_context_archival`,
//! `bounded_context_deletion`. Phase 2 adds four more static-typed
//! surfaces, this time `AdminAccess`-facing (a caller's own per-bounded-
//! context grant, not `Superadmin`): `type_registration`,
//! `event_type_admin_operations`, `command_type_admin_operations`,
//! `token_revocation`. `subject_erasure` (`ForgetSubject`) wasn't built in
//! this phase - `EncryptionKey` had no persistence at all yet - but is now
//! (its own later pass, once `protect_sensitive_fields`/`skilj_core::encryption`
//! existed for real - see `docs/architecture.md`'s write-up of that pass).
//!
//! Phase 3 adds three of the five dynamic per-bounded-context business
//! surfaces: `event_query` (`EventQuery`), `command_query`
//! (`CommandQuery`), `command_submission` (`CommandSubmission` - see its
//! own doc comment for why this is the highest-value of the three:
//! `CommandDispatcher::required_role`, §1.3.1, finally gets a real
//! caller). `ProjectionQuery`/`EventSubscription` stayed out of Phase 3 -
//! both were blocked on genuine prerequisites at the time (`project()`
//! didn't exist yet; neither did an event-delivery mechanism for
//! `EventSubscription`) - both are built now, each its own later pass,
//! described just below.
//!
//! `projection_query` (`ProjectionQuery`) is its own later pass, once
//! `project()` existed both sync and async ([§8](../../../docs/architecture.md#open-for-a-future-pass) item 6) - see
//! `crate::projection_types` for the JSON-Schema→GraphQL-type generation
//! mechanism it needed (§5.1's own previously-deferred piece) and this
//! module's own `require_read_mapping` for why it's gated differently
//! from every prior dynamic surface (`ReadAccess`, not `AdminAccess`).
//!
//! `event_subscription` (`EventSubscription`) closes out [§8](../../../docs/architecture.md#open-for-a-future-pass)/[§9](../../../docs/architecture.md#next-steps) - the
//! last surface, once `skilj_core::event_store::EventBroadcaster` gave
//! it a real-time delivery mechanism. Reuses `require_read_mapping`
//! (same `ReadAccess` facing as `projection_query`) and this module's own
//! `parse_filters`.
//!
//! `event_query`/`command_query`/`event_subscription` all also do real
//! decrypt-on-read now (a later pass still, once `render_event`/
//! `render_command` grew a real non-trivial branch) - see this module's
//! own `resolve_read_data_keys` for the shared pre-resolution step every
//! one of them calls before its own `event_store` function.
//!
//! Every resolver here is a thin wrapper: resolve the caller (except
//! `createSuperadmin`, the one surface with none), load whatever state
//! the pure function needs via `skilj_core::db`, call it, persist the
//! result, render it as the GraphQL response. Library-level errors
//! render through `code()`/`message()` into `async-graphql`'s real
//! error/extensions via `crate::error::to_graphql_error` (§5.4).
//! `command_submission` is the one exception with a
//! `CommandDecision`-shaped rejection path - business rejections there
//! render as ordinary typed data (§5.4), never a GraphQL error.

pub mod access_management;
pub mod bounded_context_archival;
pub mod bounded_context_creation;
pub mod bounded_context_deletion;
pub mod bounded_context_directory;
pub mod bounded_context_templating;
pub mod command_query;
pub mod command_submission;
pub mod command_type_admin_operations;
pub mod event_query;
pub mod event_subscription;
pub mod event_type_admin_operations;
pub mod private_field_grant_management;
pub mod projection_query;
pub mod snapshot_query;
pub mod subject_erasure;
pub mod superadmin_bootstrap;
pub mod token_revocation;
pub mod type_registration;

use crate::error::to_graphql_error;
use crate::gql_types::BoundedContextWithMappings;
use async_graphql::dynamic::{ResolverContext, ValueAccessor};
use async_graphql::ErrorExtensions;
use skilj_core::access_control::{
    AccessLevel, EventReadStartPosition, PrivateFieldGrant, Role, RoleAccessMapping, RoleStatus,
};
use skilj_core::db::Pool;
use skilj_core::encryption::{DataKey, EncryptionMasterKey};
use skilj_core::event_store::MissedOccurrencePolicy;
use skilj_core::shared::{
    Filter, FilterOperator, PrivateField, PrivateFieldKind, SensitiveField, TagMapping,
};

/// The resolved caller for every gated resolver in this crate except
/// `createSuperadmin` - `Err` (an "unauthenticated" GraphQL error) when
/// no bearer JWT resolved to a `Role` at all. See `auth::resolve_role`'s
/// own doc comment for why that's `ctx.data::<Option<Role>>()` rather
/// than a hard requirement baked into every request: `createSuperadmin`
/// needs exactly the `None` case to stay reachable at all.
pub fn require_caller(ctx: &ResolverContext) -> async_graphql::Result<Role> {
    ctx.data::<Option<Role>>()?.clone().ok_or_else(|| {
        async_graphql::Error::new(
            "this mutation needs a caller identity - present a bearer JWT that resolves to an \
             active Role",
        )
        .extend_with(|_, ext| ext.set("code", "unauthenticated"))
    })
}

/// The pre-resolution step every caller of `event_store::render_event`/
/// `render_command` (via `query_events`/`inspect_event`/`fetch_commands`/
/// `deliver_to_subscriptions`) needs before it can decrypt anything - a
/// thin async wrapper around `db::resolve_data_keys_for_reading`,
/// converting its error into a GraphQL one via `to_graphql_error`. Called
/// once per event/command being rendered, accumulating into the same
/// `resolved` map across the whole batch - a subject shared across many
/// rows resolves once, the identical shape `db::resolve_encryption_keys`
/// already has on the write side.
pub async fn resolve_read_data_keys(
    pool: &Pool,
    bounded_context: &str,
    sensitive_fields: &[SensitiveField],
    payload: &str,
    access_mapping: &RoleAccessMapping,
    master_key: Option<&EncryptionMasterKey>,
    resolved: &mut std::collections::HashMap<(String, String), DataKey>,
) -> async_graphql::Result<()> {
    skilj_core::db::resolve_data_keys_for_reading(
        pool,
        bounded_context,
        sensitive_fields,
        payload,
        access_mapping,
        master_key,
        resolved,
    )
    .await
    .map_err(to_graphql_error)
}

/// `BoundedContext` plus its own active `RoleAccessMapping`s, loaded
/// together - see `gql_types::BoundedContextWithMappings`'s own doc
/// comment for why every resolver returning a `BoundedContext` builds
/// this rather than the bare entity. `None` when no such context exists.
pub async fn load_bounded_context_with_mappings(
    pool: &Pool,
    name: &str,
) -> skilj_core::error::Result<Option<BoundedContextWithMappings>> {
    let Some(context) = skilj_core::db::get_bounded_context(pool, name).await? else {
        return Ok(None);
    };
    let access_mappings = skilj_core::db::list_role_access_mappings(pool)
        .await?
        .into_iter()
        .filter(|m| m.bounded_context.name == name && m.status == RoleStatus::Active)
        .collect();
    Ok(Some(BoundedContextWithMappings {
        context,
        access_mappings,
    }))
}

/// The caller's own active, admin-level `RoleAccessMapping` on
/// `bounded_context_name` - what every Phase 2 surface faces
/// (`AdminAccess`, not `Superadmin` - `TypeRegistration`/
/// `EventTypeAdminOperations`/`CommandTypeAdminOperations`/
/// `TokenRevocation` all `facing access_mapping: AdminAccess`). No
/// mapping at all collapses into the same rejection a revoked one gets -
/// the pure functions these resolvers call can't tell "missing" from
/// "revoked" either, since both would arrive as `GrantNotActive` from
/// their own `access_mapping.status` check once a real (revoked) row
/// exists; this is the "no row at all" case that has none to pass in,
/// the same treatment `bounded_context_archival` already gives it.
pub async fn require_admin_mapping(
    ctx: &ResolverContext<'_>,
    pool: &Pool,
    bounded_context_name: &str,
) -> async_graphql::Result<RoleAccessMapping> {
    let caller = require_caller(ctx)?;
    skilj_core::db::get_active_role_access_mapping(pool, &caller.id, bounded_context_name)
        .await
        .map_err(to_graphql_error)?
        .filter(|m| m.level == AccessLevel::Admin)
        .ok_or_else(|| to_graphql_error(skilj_core::access_control::Error::GrantNotActive))
}

/// The caller's own active `RoleAccessMapping` on `bounded_context_name`,
/// at *any* level - what `ProjectionQuery` faces (`ReadAccess`, not
/// `AdminAccess`): "Read level is enough - write and admin grants
/// include it, since they can already do strictly more" (the surface's
/// own `GrantScopedToBoundedContext` guarantee). No level filter at all,
/// unlike `require_admin_mapping` - matching `query_projection`
/// (`skilj-core::projections`) itself, which imposes none either. Same
/// "no mapping at all collapses into the same rejection a revoked one
/// gets" treatment as `require_admin_mapping`.
///
/// Deliberately bounded-context-level only, same as it always was: this
/// resolves *whether the caller may query the bounded context at all*,
/// not *which instances within it*. The finer-grained check -
/// `RoleAccessMapping.scope` against a queried instance's own derived
/// owner, for a projection that declares one - is `projections::
/// query_projection`'s own job, applied per query in `projection_query`'s
/// resolver (cross-tenant projection read fix, docs/architecture.md's
/// own write-up of this pass). The mapping this function returns already
/// carries `scope`; it is only enforced downstream.
pub async fn require_read_mapping(
    ctx: &ResolverContext<'_>,
    pool: &Pool,
    bounded_context_name: &str,
) -> async_graphql::Result<RoleAccessMapping> {
    let caller = require_caller(ctx)?;
    skilj_core::db::get_active_role_access_mapping(pool, &caller.id, bounded_context_name)
        .await
        .map_err(to_graphql_error)?
        .ok_or_else(|| to_graphql_error(skilj_core::access_control::Error::GrantNotActive))
}

/// Parses a `[TagMappingInput!]!` argument into `Vec<TagMapping>` -
/// shared by `registerEventType`/`registerCommandType`.
pub fn parse_tag_mappings(value: &ValueAccessor) -> async_graphql::Result<Vec<TagMapping>> {
    let mut mappings = Vec::new();
    for item in value.list()?.iter() {
        let obj = item.object()?;
        mappings.push(TagMapping {
            key: obj.try_get("key")?.string()?.to_string(),
            field: obj.try_get("field")?.string()?.to_string(),
        });
    }
    Ok(mappings)
}

/// Parses a `[SensitiveFieldInput!]!` argument into `Vec<SensitiveField>` -
/// shared by `registerEventType`/`registerCommandType`.
pub fn parse_sensitive_fields(value: &ValueAccessor) -> async_graphql::Result<Vec<SensitiveField>> {
    let mut fields = Vec::new();
    for item in value.list()?.iter() {
        let obj = item.object()?;
        fields.push(SensitiveField {
            field: obj.try_get("field")?.string()?.to_string(),
            subject_key: obj.try_get("subjectKey")?.string()?.to_string(),
            subject_field: obj.try_get("subjectField")?.string()?.to_string(),
        });
    }
    Ok(fields)
}

/// Parses a `[PrivateFieldInput!]!` argument into `Vec<PrivateField>` -
/// shared by `registerEventType`/`registerCommandType`, the same role
/// `parse_sensitive_fields` plays for `sensitive_fields`.
pub fn parse_private_fields(value: &ValueAccessor) -> async_graphql::Result<Vec<PrivateField>> {
    let mut fields = Vec::new();
    for item in value.list()?.iter() {
        let obj = item.object()?;
        let kind = match obj.try_get("kind")?.enum_name()? {
            "TEAM" => PrivateFieldKind::Team,
            "ADDRESSED" => PrivateFieldKind::Addressed,
            _ => PrivateFieldKind::Own,
        };
        let team = obj
            .get("team")
            .filter(|v| !v.is_null())
            .map(|v| v.string().map(str::to_string))
            .transpose()?;
        let addressee_field = obj
            .get("addresseeField")
            .filter(|v| !v.is_null())
            .map(|v| v.string().map(str::to_string))
            .transpose()?;
        fields.push(PrivateField {
            field: obj.try_get("field")?.string()?.to_string(),
            kind,
            team,
            addressee_field,
        });
    }
    Ok(fields)
}

/// Every active `PrivateFieldGrant` in `bounded_context_name` - the
/// shared snapshot `query_events`/`inspect_event`/`fetch_commands`/
/// `deliver_to_subscriptions`'s own callers all pre-load once per request
/// and pass straight through to `render_event`/`render_command` (see
/// those functions' own doc comments for why one list serves every
/// caller/subscriber alike, filtered internally by grantee).
pub async fn load_private_field_grants(
    pool: &Pool,
    bounded_context_name: &str,
) -> async_graphql::Result<Vec<PrivateFieldGrant>> {
    skilj_core::db::list_private_field_grants_for_context(pool, bounded_context_name)
        .await
        .map_err(to_graphql_error)
}

/// Parses a `[FilterInput!]` argument into `Vec<Filter>` - used by
/// `event_subscription::events_by_type_field`'s own `filters` argument.
/// Real wire-shape (matching `CreateEventTypeSubscription`'s own
/// signature faithfully) and, now that `matches_filters`/`valid_filters`
/// are real, real behaviour end to end - the output flows straight into
/// `create_event_type_subscription`, whose own `valid_filters` call does
/// the rejection for real.
pub fn parse_filters(value: &ValueAccessor) -> async_graphql::Result<Vec<Filter>> {
    let mut filters = Vec::new();
    for item in value.list()?.iter() {
        let obj = item.object()?;
        let operator = match obj.try_get("operator")?.enum_name()? {
            "EQUALS" => FilterOperator::Equals,
            "CONTAINS" => FilterOperator::Contains,
            "IS_LIKE" => FilterOperator::IsLike,
            "GREATER_THAN" => FilterOperator::GreaterThan,
            "NEAR" => FilterOperator::Near,
            "SIMILAR_COLOR" => FilterOperator::SimilarColor,
            "IN_SUBNET" => FilterOperator::InSubnet,
            "IN" => FilterOperator::In,
            _ => FilterOperator::LessThan,
        };
        filters.push(Filter {
            field: obj.try_get("field")?.string()?.to_string(),
            operator,
            value: obj.try_get("value")?.string()?.to_string(),
        });
    }
    Ok(filters)
}

/// `MissedOccurrencePolicy` GraphQL enum name -> domain value - the same
/// `match ... => variant, _ => last variant` shape `parse_filters`'s own
/// `operator` match already uses. `registerEventType`'s own
/// `missedOccurrencePolicy` argument is the only caller - deliberately
/// no default anywhere upstream of this (see `EventType::
/// missed_occurrence_policy`'s own doc comment: the user's own call was
/// "I rather not have a default"), so this only ever runs against a
/// value the GraphQL enum type itself already validated as one of the
/// three real names.
pub fn parse_missed_occurrence_policy(name: &str) -> MissedOccurrencePolicy {
    match name {
        "SKIP" => MissedOccurrencePolicy::Skip,
        "FIRE_ONCE" => MissedOccurrencePolicy::FireOnce,
        _ => MissedOccurrencePolicy::ReplayBacklog,
    }
}

/// `EventReadStartPosition` GraphQL enum name -> domain value - same
/// shape and reasoning as `parse_missed_occurrence_policy` above.
/// `createEventReadToken`'s own `startFrom` argument is the only caller;
/// omitted entirely (`None`, not one of these two names) is handled at
/// the call site, before this ever runs - see
/// `create_event_read_token_field`'s own doc comment.
pub fn parse_event_read_start_position(name: &str) -> EventReadStartPosition {
    match name {
        "LATEST" => EventReadStartPosition::Latest,
        _ => EventReadStartPosition::Beginning,
    }
}

/// A `String` argument this crate treats as an entity name/id lookup key
/// that turned out to match nothing - not a `skilj_core::Error` (no rule
/// ever ran; there was nothing to run it against), so not something
/// `to_graphql_error` can render. Every resolver that looks one up by a
/// caller-supplied argument (a `Role` by id, a `BoundedContext` by name,
/// a `RoleAccessMapping` by its `(role, bounded_context)` pair) uses this
/// for the "doesn't exist" case.
pub fn not_found(entity: &str, key: &str) -> async_graphql::Error {
    async_graphql::Error::new(format!("no {entity} matches {key:?}"))
        .extend_with(|_, ext| ext.set("code", format!("{entity}_not_found")))
}

/// Shared by `event_type_admin_operations`/`command_type_admin_operations` -
/// three of the four `create*Token` mutations (`createExternalEventToken`,
/// `createDirectCreationToken`, `createCommandToken`) differ only in
/// which type they resolve (`EventType`/`CommandType`, hence
/// `$type_arg_name`/`$type_label`/`$get_type_fn`) and which
/// `access_control::create_*_token`/`db::insert_*_token` pair they call.
/// Originally two separate, near-identical macros/hand-written bodies
/// (one file's own doc comment used to say so explicitly) - unified here
/// since `command_type_admin_operations` only ever needed the identical
/// shape with different type parameters, not a genuinely different one.
///
/// `createEventReadToken` briefly joined this macro too, once the
/// cross-tenant read/write fix gave every `create_*_token` function an
/// identical `scope: Option<String>` parameter in the same position
/// (docs/architecture.md's own write-up of these passes) - but the "new
/// subscriber replays all history" fix (a later pass) gave
/// `create_event_read_token` a `start_from` parameter none of its three
/// siblings share, breaking the "identical parameter list" premise this
/// macro depends on. It has stayed hand-written since - see
/// `event_type_admin_operations::create_event_read_token_field`'s own
/// doc comment.
macro_rules! create_type_token_field {
    (
        $field_name:literal,
        $return_type:literal,
        $type_arg_name:literal,
        $type_label:literal,
        $get_type_fn:path,
        $create_fn:path,
        $insert_fn:path
    ) => {
        ::async_graphql::dynamic::Field::new(
            $field_name,
            ::async_graphql::dynamic::TypeRef::named_nn($return_type),
            |ctx| {
                ::async_graphql::dynamic::FieldFuture::new(async move {
                    let state = ctx.data::<$crate::GraphqlState>()?;
                    let bounded_context_name =
                        ctx.args.try_get("boundedContext")?.string()?.to_string();
                    let access_mapping = $crate::resolvers::require_admin_mapping(
                        &ctx,
                        &state.pool,
                        &bounded_context_name,
                    )
                    .await?;
                    let type_name = ctx.args.try_get($type_arg_name)?.string()?.to_string();
                    let scope = ctx
                        .args
                        .get("scope")
                        .filter(|v| !v.is_null())
                        .map(|v| v.string().map(str::to_string))
                        .transpose()?;

                    let target_type = $get_type_fn(&state.pool, &bounded_context_name, &type_name)
                        .await
                        .map_err($crate::error::to_graphql_error)?
                        .ok_or_else(|| $crate::resolvers::not_found($type_label, &type_name))?;

                    let token = $create_fn(
                        &access_mapping,
                        &target_type,
                        ::skilj_core::shared::generate_token_id(),
                        ::skilj_core::shared::generate_token_secret(),
                        scope,
                        chrono::Utc::now(),
                    )
                    .map_err($crate::error::to_graphql_error)?;
                    $insert_fn(&state.pool, &token)
                        .await
                        .map_err($crate::error::to_graphql_error)?;

                    Ok(Some(::async_graphql::dynamic::FieldValue::owned_any(token)))
                })
            },
        )
        .argument(::async_graphql::dynamic::InputValue::new(
            "boundedContext",
            ::async_graphql::dynamic::TypeRef::named_nn(::async_graphql::dynamic::TypeRef::STRING),
        ))
        .argument(::async_graphql::dynamic::InputValue::new(
            $type_arg_name,
            ::async_graphql::dynamic::TypeRef::named_nn(::async_graphql::dynamic::TypeRef::STRING),
        ))
        .argument(::async_graphql::dynamic::InputValue::new(
            "scope",
            ::async_graphql::dynamic::TypeRef::named(::async_graphql::dynamic::TypeRef::STRING),
        ))
    };
}
pub(crate) use create_type_token_field;
