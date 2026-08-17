//! Builds the one unified GraphQL schema - see docs/architecture.md
//! §5.1. Phase 1 (the plan at
//! `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`) only ever
//! calls `build()` once, at `Skilj::graphql_router()` time: none of its
//! six surfaces depend on any per-bounded-context dynamic type, so
//! there's nothing yet that would ever need a rebuild. `SchemaRegistry`
//! below stays a stub for that reason - it becomes real once Phase 3's
//! dynamic, per-context types exist to rebuild the schema for when
//! registration changes (§5.1's own "rebuilds the entire schema from
//! scratch behind `ArcSwap<Schema>`").

use crate::gql_types;
use crate::resolvers;
use crate::GraphqlState;
use arc_swap::ArcSwap;
use async_graphql::dynamic::{Object, Schema};
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

/// Builds the real, Phase-1 schema: the superadmin admin console - see
/// this module's own doc comment for why this is a plain function today,
/// not `SchemaRegistry::build()`. `state` is baked into the schema's own
/// global `.data()` (not per-request - `pool`/`bootstrap_secret`/
/// `identity` never change once built), so every resolver reaches it via
/// `ctx.data::<GraphqlState>()`. The per-request caller `Role` is the one
/// thing that *does* vary per request - `graphql_handler` injects that
/// separately, via `async_graphql::Request::data`, not here.
pub fn build(state: GraphqlState) -> Schema {
    let query = Object::new("Query")
        .field(resolvers::bounded_context_directory::field())
        .field(resolvers::type_registration::projections_field())
        .field(resolvers::event_query::query_events_field())
        .field(resolvers::event_query::count_events_field())
        .field(resolvers::event_query::inspect_event_field())
        .field(resolvers::command_query::fetch_commands_field());
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
        .field(resolvers::command_submission::submit_command_field());

    Schema::build(query.type_name(), Some(mutation.type_name()), None)
        .register(query)
        .register(mutation)
        .register(gql_types::role_object())
        .register(gql_types::created_superadmin_object())
        .register(gql_types::role_access_mapping_object())
        .register(gql_types::context_creator_object())
        .register(gql_types::bounded_context_object())
        .register(gql_types::role_status_enum())
        .register(gql_types::access_level_enum())
        .register(gql_types::bounded_context_status_enum())
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
        .data(state)
        .finish()
        .expect(
            "Phase 1/2/3's schema shape is fixed at compile time, never data-dependent - a \
             SchemaError here would mean two fields/types collide or a TypeRef names something \
             unregistered, a bug in this module, not a runtime condition",
        )
}
