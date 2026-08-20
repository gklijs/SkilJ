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
pub mod projection_types;
pub mod resolvers;
pub mod schema;

use async_graphql::dynamic::Schema;
use skilj_core::access_control::RevocationBroadcaster;
use skilj_core::bootstrap::BootstrapSecret;
use skilj_core::db::Pool;
use skilj_core::encryption::EncryptionMasterKey;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::EventBroadcaster;
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher};
use std::sync::Arc;
use std::time::Duration;

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
    /// `ProjectionQuery`'s own `wait_for_sequence` timeout - "a process-
    /// start configuration knob, not a per-query argument or a fixed
    /// value" (the spec's own guidance above `rule QueryProjection`), the
    /// same register `SkiljBuilder::async_projection_poll_interval`
    /// already lives in. See `resolvers::projection_query::wait_until_caught_up`.
    pub projection_query_wait_timeout: Duration,
    /// `submitCommand`'s own bridge into `protect_sensitive_fields` for
    /// the events/command it produces (§SubjectErasure) - the identical
    /// `Option<EncryptionMasterKey>` `Skilj::rest_router()` hands its own
    /// event-creation routes, reused here for the same reason
    /// `dispatcher`/`projection_dispatcher` above are.
    pub encryption_master_key: Option<EncryptionMasterKey>,
    /// `EventSubscription`'s own real-time delivery source
    /// (`resolvers::event_subscription`) - the identical `EventBroadcaster`
    /// `Skilj::rest_router()` hands its own event-creation routes to
    /// publish into, reused here for the same reason `dispatcher`/
    /// `projection_dispatcher` above are: one shared, process-wide
    /// broadcaster, not a second one.
    pub event_broadcaster: EventBroadcaster,
    /// `EventSubscription`'s own second broadcast channel (drift audit
    /// finding #4) - see `access_control::RevocationBroadcaster`'s own
    /// doc comment for the full design: `resolvers::access_management`
    /// publishes here on every real revocation, `resolvers::event_subscription`
    /// closes a matching live connection the instant one arrives, rather
    /// than only at its own next delivered event.
    pub revocation_broadcaster: RevocationBroadcaster,
    /// `QueryEvents`/`CountEvents`/`InspectEvent`'s and `submitCommand`'s
    /// own DCB pre-check's real read path (drift audit finding #8) - the
    /// identical `EventCache` `Skilj::rest_router()` hands its own
    /// event-creation and `FetchEvents`/`ConsumeEvents` routes, reused
    /// here for the same reason `event_broadcaster` above is: one
    /// shared, process-wide cache, not a second one.
    pub event_cache: EventCache,
}

/// Builds the schema from `state` and mounts it as a fresh `axum::Router`
/// at `/graphql` - the one real caller is `Skilj::graphql_router()`.
/// `async`, returning `Result`, since `schema::build` now needs to list
/// every registered projection to generate `ProjectionQuery`'s own
/// per-projection types (§5.1) - a real I/O failure mode `schema::build`'s
/// own `.expect(...)` deliberately doesn't cover (that stays for a
/// genuine schema-shape bug, never a runtime condition).
///
/// One path, method-routed: `POST` still goes to `graphql_handler`
/// (queries/mutations); `GET` with the right `Upgrade`/`Sec-WebSocket-Protocol`
/// headers goes to `graphql_ws_handler` (subscriptions) - axum's own
/// documented idiom for exactly this "one path, a handler or a raw
/// `Service` depending on method" case, confirmed against the installed
/// crate source, not improvised. A GraphQL client's own protocol
/// negotiation picks the right one; nothing here needs a second path.
pub async fn router(state: GraphqlState) -> skilj_core::error::Result<axum::Router> {
    let schema = schema::build(state.clone()).await?;
    Ok(axum::Router::new()
        .route(
            "/graphql",
            axum::routing::post(graphql_handler).get(graphql_ws_handler),
        )
        .with_state((schema, state)))
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

/// Upgrades to a GraphQL-over-websocket connection for `EventSubscription`.
/// Built directly against `async_graphql_axum::{GraphQLProtocol,
/// GraphQLWebSocket}` rather than the higher-level `GraphQLSubscription`
/// service wrapper - that wrapper builds its own `GraphQLWebSocket`
/// internally with no way to reach `on_connection_init`, and the caller
/// identity has to come from there (the graphql-ws protocol's own place
/// for it - there is no per-message header on an already-established
/// websocket). A `connection_init` payload with no recognisable bearer
/// credential resolves to no caller (`Ok(None)`, matching
/// `auth::resolve_role`'s own "missing = fine" case - some subscriptions
/// may need no caller); one that's present but invalid rejects the whole
/// connection before any subscription starts, via `on_connection_init`'s
/// own `Err` path - the same "wrong credential is a hard stop" treatment
/// `graphql_handler` already gives the header case.
async fn graphql_ws_handler(
    axum::extract::State((schema, state)): axum::extract::State<(Schema, GraphqlState)>,
    protocol: async_graphql_axum::GraphQLProtocol,
    upgrade: axum::extract::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    upgrade
        .protocols(async_graphql::http::ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| {
            async_graphql_axum::GraphQLWebSocket::new(socket, schema, protocol)
                .on_connection_init(move |payload| {
                    let state = state.clone();
                    async move {
                        let role = auth::resolve_role_from_connection_init(
                            &payload,
                            state.identity.as_ref(),
                            &state.pool,
                        )
                        .await?;
                        let mut data = async_graphql::Data::default();
                        data.insert(role);
                        Ok(data)
                    }
                })
                .serve()
        })
}
