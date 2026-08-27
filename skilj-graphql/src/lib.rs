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

use opentelemetry::metrics::Histogram;
use skilj_core::access_control::RevocationBroadcaster;
use skilj_core::bootstrap::BootstrapSecret;
use skilj_core::db::Pool;
use skilj_core::encryption::EncryptionMasterKey;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::EventBroadcaster;
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher};
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

/// See `skilj-core::db`'s own `meter()`/`LazyLock` doc comment for the
/// `global::meter()` snapshot-binding caveat this is subject to too -
/// only ever touched from inside `trace_request` below.
static REQUEST_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    opentelemetry::global::meter("skilj-graphql")
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of HTTP requests served by skilj-graphql.")
        .build()
});

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

/// Mounts a fresh `axum::Router` at `/graphql` against an already-built
/// `registry` - the one real caller is `Skilj::graphql_router()`, which
/// shares the one `SchemaRegistry` `SkiljBuilder::build()` built (and
/// `skilj::cross_instance` keeps rebuilding) rather than building a new
/// one per call, so every call sees the same live-updating schema.
///
/// One path, method-routed: `POST` still goes to `graphql_handler`
/// (queries/mutations); `GET` with the right `Upgrade`/`Sec-WebSocket-Protocol`
/// headers goes to `graphql_ws_handler` (subscriptions) - axum's own
/// documented idiom for exactly this "one path, a handler or a raw
/// `Service` depending on method" case, confirmed against the installed
/// crate source, not improvised. A GraphQL client's own protocol
/// negotiation picks the right one; nothing here needs a second path.
pub async fn router(
    registry: Arc<schema::SchemaRegistry>,
    state: GraphqlState,
) -> skilj_core::error::Result<axum::Router> {
    Ok(axum::Router::new()
        .route(
            "/graphql",
            axum::routing::post(graphql_handler).get(graphql_ws_handler),
        )
        .layer(axum::middleware::from_fn(trace_request))
        .with_state((registry, state)))
}

/// One request-level span per GraphQL call (both the `POST` query/mutation
/// path and the WS upgrade), its parent set from an incoming W3C
/// `traceparent`/`tracestate` header pair if present - a safe no-op when
/// the consuming application never registers a real propagator.
/// Identical to `skilj-rest::routes::trace_request`; see that one's doc
/// comment for the full reasoning, including why this is a hand-rolled
/// `axum::middleware::from_fn` rather than `tower_http::trace::TraceLayer`.
/// Duplicated rather than shared since there's no crate both `skilj-rest`
/// and `skilj-graphql` already depend on that this small a helper would
/// justify adding.
async fn trace_request(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use tracing::Instrument;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let parent_cx = opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&opentelemetry_http::HeaderExtractor(request.headers()))
    });
    // Captured as owned values before `request` moves into `next.run(...)`
    // below - see `skilj-rest::routes::trace_request`'s own doc comment
    // on why `http.route` is the bare path.
    let method = request.method().to_string();
    let route = request.uri().path().to_string();
    let span = tracing::info_span!(
        "request",
        method = %request.method(),
        uri = %request.uri(),
        status = tracing::field::Empty,
        // See `skilj-rest::routes::trace_request`'s own doc comment for
        // why this well-known field name, not `OpenTelemetrySpanExt::set_status`
        // directly.
        otel.status_description = tracing::field::Empty,
    );
    // `Err` here just means no `tracing-opentelemetry` layer is installed
    // in this process - nothing to propagate into, so nothing to do.
    let _ = span.set_parent(parent_cx);

    async move {
        let start = std::time::Instant::now();
        let response = next.run(request).await;
        let status = response.status();
        let span = tracing::Span::current();
        span.record("status", status.as_u16());
        // Limited value on the `POST /graphql` path specifically -
        // `async_graphql_axum::GraphQLResponse` always renders as HTTP
        // 200 regardless of GraphQL-level errors, per the GraphQL-over-
        // HTTP convention (§5.4's own "a rejection renders identically"
        // treatment extends the same idea to real errors) - but still
        // correct for the WS upgrade path, and costs nothing when it
        // never fires.
        if status.is_server_error() {
            span.record("otel.status_description", status.to_string());
            tracing::error!(
                status = status.as_u16(),
                "request failed with a server error"
            );
        }
        REQUEST_DURATION.record(
            start.elapsed().as_secs_f64(),
            &[
                opentelemetry::KeyValue::new("http.request.method", method),
                opentelemetry::KeyValue::new("http.route", route),
                opentelemetry::KeyValue::new("http.response.status_code", status.as_u16() as i64),
            ],
        );
        response
    }
    .instrument(span)
    .await
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
    axum::extract::State((registry, state)): axum::extract::State<(
        Arc<schema::SchemaRegistry>,
        GraphqlState,
    )>,
    headers: axum::http::HeaderMap,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    // The live schema, read fresh for this one request - a concurrent
    // `SchemaRegistry::rebuild` (another registration change, on this
    // instance or another) never affects a request already in flight,
    // since this is an `Arc` snapshot, not a lock.
    let schema = registry.current();
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
    axum::extract::State((registry, state)): axum::extract::State<(
        Arc<schema::SchemaRegistry>,
        GraphqlState,
    )>,
    protocol: async_graphql_axum::GraphQLProtocol,
    upgrade: axum::extract::WebSocketUpgrade,
) -> impl axum::response::IntoResponse {
    // `Schema` is `Clone`-cheap (its own doc comment: internally
    // `Arc`-wrapped) - a subscription's whole lifetime uses whichever
    // schema was live at connection time, the same "one snapshot, no
    // torn reads" treatment `graphql_handler` gets, just held for
    // longer.
    let schema = (*registry.current()).clone();
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
