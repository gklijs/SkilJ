//! Builds the one unified GraphQL schema - see docs/architecture.md
//! §5.1. `build()` is still only ever called once, at
//! `Skilj::graphql_router()` time, even now that `ProjectionQuery` gives
//! it genuinely data-dependent content (`projection_types::build`) -
//! `SchemaRegistry` below stays a stub, deliberately (see the plan at
//! `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`): a
//! projection registered after `graphql_router()` was called won't gain
//! a `ProjectionResult` member until the process restarts. It becomes
//! real once something actually needs the schema to reflect registration
//! changes live, without a restart (§5.1's own "rebuilds the entire
//! schema from scratch behind `ArcSwap<Schema>`").

use crate::gql_types;
use crate::projection_types;
use crate::resolvers;
use crate::GraphqlState;
use arc_swap::ArcSwap;
use async_graphql::dynamic::{Object, Schema, Subscription};
use std::sync::Arc;

pub struct SchemaRegistry {
    current: ArcSwap<Schema>,
}

impl SchemaRegistry {
    pub fn current(&self) -> Arc<Schema> {
        self.current.load_full()
    }

    // TODO (Phase 3): build(), walking every registered bounded context's
    // EventType/CommandType/Projection JSON Schemas into
    // async_graphql::dynamic::Object/Field/TypeRef definitions (§5.1),
    // nested under a per-bounded-context field on Query/Mutation/
    // Subscription (§5.2), and rebuild()/swap() called whenever
    // registration changes.
}

/// Builds the real schema - see this module's own doc comment for why
/// this is a plain function today, not `SchemaRegistry::build()`. `state`
/// is baked into the schema's own global `.data()` (not per-request -
/// `pool`/`bootstrap_secret`/`identity` never change once built), so
/// every resolver reaches it via `ctx.data::<GraphqlState>()`. The
/// per-request caller `Role` is the one thing that *does* vary per
/// request - `graphql_handler` injects that separately, via
/// `async_graphql::Request::data`, not here.
///
/// `async`, returning `Result`: `ProjectionQuery`'s own per-projection
/// types (`crate::projection_types::build`) need to list every
/// registered projection across every bounded context, a real query this
/// function didn't need to make before - built once, here, not
/// rebuilt when registration changes later (§5.1's aspirational
/// `SchemaRegistry` stays a stub - see its own doc comment). `None` from
/// `projection_types::build` (nothing registered anywhere yet) omits the
/// `projection` field and the `ProjectionResult` union entirely, rather
/// than registering an invalid zero-member union.
pub async fn build(state: GraphqlState) -> skilj_core::error::Result<Schema> {
    let projection_types = projection_types::build(&state.pool).await?;

    let mut query = Object::new("Query")
        .field(resolvers::bounded_context_directory::field())
        .field(resolvers::type_registration::projections_field())
        .field(resolvers::type_registration::scheduled_event_types_field())
        .field(resolvers::event_query::query_events_field())
        .field(resolvers::event_query::count_events_field())
        .field(resolvers::event_query::inspect_event_field())
        .field(resolvers::command_query::fetch_commands_field())
        .field(resolvers::projection_query::schema_field());
    if projection_types.is_some() {
        query = query.field(resolvers::projection_query::field());
    }
    let mutation = Object::new("Mutation")
        .field(resolvers::superadmin_bootstrap::field())
        .field(resolvers::access_management::create_role_field())
        .field(resolvers::access_management::revoke_role_field())
        .field(resolvers::access_management::grant_role_access_mapping_field())
        .field(resolvers::access_management::revoke_role_access_mapping_field())
        .field(resolvers::bounded_context_creation::field())
        .field(resolvers::bounded_context_archival::field())
        .field(resolvers::bounded_context_deletion::field())
        .field(resolvers::type_registration::register_event_type_field())
        .field(resolvers::type_registration::register_command_type_field())
        .field(resolvers::type_registration::register_projection_field())
        .field(resolvers::type_registration::rebuild_projection_field())
        .field(resolvers::type_registration::discard_projection_rebuild_field())
        .field(resolvers::event_type_admin_operations::create_external_event_token_field())
        .field(resolvers::event_type_admin_operations::create_direct_creation_token_field())
        .field(resolvers::event_type_admin_operations::create_event_read_token_field())
        .field(resolvers::command_type_admin_operations::create_command_token_field())
        .field(resolvers::token_revocation::revoke_token_field())
        .field(resolvers::command_submission::submit_command_field())
        .field(resolvers::subject_erasure::field());
    let subscription = Subscription::new("Subscription")
        .field(resolvers::event_subscription::all_events_field())
        .field(resolvers::event_subscription::events_by_type_field());

    let mut builder = Schema::build(
        query.type_name(),
        Some(mutation.type_name()),
        Some(subscription.type_name()),
    )
    .register(query)
    .register(mutation)
    .register(subscription)
    .register(gql_types::filter_input())
    .register(gql_types::filter_operator_enum())
    .register(gql_types::role_object())
    .register(gql_types::created_superadmin_object())
    .register(gql_types::role_access_mapping_object())
    .register(gql_types::context_creator_object())
    .register(gql_types::bounded_context_object())
    .register(gql_types::role_status_enum())
    .register(gql_types::access_level_enum())
    .register(gql_types::bounded_context_status_enum())
    .register(gql_types::missed_occurrence_policy_enum())
    .register(gql_types::access_token_status_enum())
    .register(gql_types::projection_rebuild_status_enum())
    .register(gql_types::tag_mapping_object())
    .register(gql_types::tag_mapping_input())
    .register(gql_types::sensitive_field_object())
    .register(gql_types::sensitive_field_input())
    .register(gql_types::event_type_object())
    .register(gql_types::command_type_object())
    .register(gql_types::projection_object())
    .register(gql_types::projection_rebuild_object())
    .register(gql_types::projection_registration_result_object())
    .register(gql_types::external_event_token_object())
    .register(gql_types::direct_creation_token_object())
    .register(gql_types::event_read_token_object())
    .register(gql_types::command_token_object())
    .register(gql_types::access_token_union())
    .register(gql_types::tag_input())
    .register(gql_types::queried_event_object())
    .register(gql_types::event_origin_object())
    .register(gql_types::event_meta_object())
    .register(gql_types::inspected_event_object())
    .register(gql_types::submit_command_payload_object())
    .register(gql_types::encryption_key_object())
    .register(gql_types::encryption_key_status_enum());

    // ProjectionQuery's own per-projection types (§5.1) - data-dependent,
    // unlike everything registered above, which is why this is the one
    // part of this function that can genuinely fail at runtime (see the
    // .expect() below, which no longer covers this).
    if let Some((objects, union)) = projection_types {
        for object in objects {
            builder = builder.register(object);
        }
        builder = builder.register(union);
    }

    Ok(builder.data(state).finish().expect(
        "every type registered above this point is fixed at compile time; every one \
         registered from projection_types is named via projection_types::graphql_type_name, \
         the single source both registration and resolution agree on - a SchemaError here \
         would mean a bug in one of those two modules, not a runtime condition",
    ))
}
