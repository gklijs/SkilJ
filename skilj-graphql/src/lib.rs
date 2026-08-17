//! SkilJ's GraphQL surface. Depends on `skilj-core`, never the other way
//! round - see docs/architecture.md §3.1. Phases 1/2 (docs/
//! architecture.md §8 item 5's own plan): the superadmin admin console
//! plus the four `AdminAccess`-gated static surfaces. Phase 3 adds
//! `EventQuery`/`CommandQuery`/`CommandSubmission` - see
//! `resolvers::command_submission`'s own doc comment for why
//! `ProjectionQuery`/`EventSubscription` stay out of this phase too.

pub mod auth;
mod error;
pub mod gql_types;
pub mod resolvers;
pub mod schema;

use async_graphql::dynamic::Schema;
use skilj_core::bootstrap::BootstrapSecret;
use skilj_core::db::Pool;
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher};
use std::sync::Arc;

/// Everything a GraphQL request needs, baked into the schema's own
/// global `.data()` at `router()` time (see `schema::build`'s own doc
/// comment for why this is schema-level data, not per-request) - built
/// once by `skilj`'s own `Skilj::graphql_router()`, the one real caller.
/// `Clone`-cheap: `Pool` is internally `Arc`-wrapped already,
/// `BootstrapSecret` is a small owned struct, `auth::Identity` wraps its
/// own `Arc<JwksCache>`, both dispatchers are already `Arc`-wrapped.
#[derive(Clone)]
pub struct GraphqlState {
    pub pool: Pool,
    pub bootstrap_secret: Option<BootstrapSecret>,
    pub identity: Option<auth::Identity>,
    /// `submitCommand`'s own bridge into the right bounded context's
    /// typed `decide()` - the identical `Arc<dyn CommandDispatcher>`
    /// `Skilj::rest_router()` already hands `skilj-rest`'s own
    /// `CommandTrigger` route (§1.7/§8 item 4), reused here rather than
    /// building a second registry.
    pub dispatcher: Arc<dyn CommandDispatcher>,
    /// `submitCommand`'s own bridge into `project()` for the events it
    /// produces (§8 item 6) - the identical `Arc<dyn ProjectionDispatcher>`
    /// `Skilj::rest_router()` hands its own event-creation routes, reused
    /// here for the same reason `dispatcher` above is.
    pub projection_dispatcher: Arc<dyn ProjectionDispatcher>,
}

/// Builds the schema from `state` and mounts it as a fresh `axum::Router`
/// at `POST /graphql` - the one real caller is `Skilj::graphql_router()`.
pub fn router(state: GraphqlState) -> axum::Router {
    let schema = schema::build(state.clone());
    axum::Router::new()
        .route("/graphql", axum::routing::post(graphql_handler))
        .with_state((schema, state))
}

/// Resolves the caller (`auth::resolve_role`) before ever executing the
/// GraphQL request: a header that's missing resolves to `None` (fine -
/// `createSuperadmin` is the only field that needs no caller at all,
/// each resolver checks for itself via `resolvers::require_caller`), but
/// one that's *present and invalid* rejects the whole request outright,
/// as a top-level GraphQL error, without ever reaching `schema.execute` -
/// the same "a wrong credential is a hard stop" treatment `skilj-rest`'s
/// own bearer extractor gives a malformed one.
async fn graphql_handler(
    axum::extract::State((schema, state)): axum::extract::State<(Schema, GraphqlState)>,
    headers: axum::http::HeaderMap,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    let response = match auth::resolve_role(&headers, state.identity.as_ref(), &state.pool).await {
        Ok(role) => schema.execute(req.into_inner().data(role)).await,
        Err(err) => async_graphql::Response::from_errors(vec![
            err.into_server_error(async_graphql::Pos::default())
        ]),
    };
    response.into()
}
