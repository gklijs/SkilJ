//! SkilJ's GraphQL surface. Depends on `skilj-core`, never the other way
//! round - see docs/architecture.md §3.1. Phases 1/2 (docs/
//! [architecture.md §8](../../docs/architecture.md#open-for-a-future-pass) item 5's own plan): the superadmin admin console
//! plus the four `AdminAccess`-gated static surfaces. Phase 3 adds
//! `EventQuery`/`CommandQuery`/`CommandSubmission` - see
//! `resolvers::command_submission`'s own doc comment for why
//! `ProjectionQuery`/`EventSubscription` stay out of this phase too.

pub mod auth;
mod error;
pub mod federation;
pub mod gql_types;
pub mod limits;
pub mod naming;
pub mod projection_types;
pub mod resolvers;
pub mod schema;

use opentelemetry::metrics::Histogram;
use skilj_core::access_control::RevocationBroadcaster;
use skilj_core::db::Pool;
use skilj_core::encryption::EncryptionMasterKey;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::EventBroadcaster;
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher, SnapshotDispatcher};
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
    /// How many `retryParkedDelivery` redrives may run at once, shared by
    /// every `GraphqlState` of one process. Each holds its per-row lock on
    /// a connection of its own while the redrive needs more, so without a
    /// bound, parallel retries could take every connection and wait on
    /// each other for one more (docs/architecture.md §117).
    pub parked_delivery_retry_permits: Arc<tokio::sync::Semaphore>,
    /// This process's bootstrap secret, shared with `skilj::Skilj` and every
    /// other `GraphqlState` built from it, so a claim through any of them
    /// consumes it for all (docs/architecture.md §109).
    pub bootstrap: skilj_core::bootstrap::BootstrapGate,
    pub identity: Option<auth::Identity>,
    /// `submitCommand`'s own bridge into the right bounded context's
    /// typed `decide()` - the identical `Arc<dyn CommandDispatcher>`
    /// `Skilj::rest_router()` already hands `skilj-rest`'s own
    /// `CommandTrigger` route (§1.7/[§8](../../docs/architecture.md#open-for-a-future-pass) item 4), reused here rather than
    /// building a second registry.
    pub dispatcher: Arc<dyn CommandDispatcher>,
    /// `submitCommand`'s own bridge into `project()` for the events it
    /// produces ([§8](../../docs/architecture.md#open-for-a-future-pass) item 6) - the identical `Arc<dyn ProjectionDispatcher>`
    /// `Skilj::rest_router()` hands its own event-creation routes, reused
    /// here for the same reason `dispatcher` above is.
    pub projection_dispatcher: Arc<dyn ProjectionDispatcher>,
    /// `inspectSnapshot`'s own bridge into `Snapshot::fold`'s registered
    /// `tag_key`/`version`/`default_state` ([docs/architecture.md §19](../../docs/architecture.md#optional-snapshotting-matching-events)) -
    /// the identical `Arc<dyn SnapshotDispatcher>` `Skilj`'s own
    /// background snapshot catch-up task holds, reused here for the same
    /// reason `dispatcher`/`projection_dispatcher` above are.
    pub snapshot_dispatcher: Arc<dyn SnapshotDispatcher>,
    /// `ProjectionQuery`'s own `wait_for_sequence` timeout - "a process-
    /// start configuration knob, not a per-query argument or a fixed
    /// value" (the spec's own guidance above `rule QueryProjection`), the
    /// same register `SkiljBuilder::async_projection_poll_interval`
    /// already lives in. See `resolvers::projection_query::wait_until_caught_up`.
    pub projection_query_wait_timeout: Duration,
    /// `config.max_events_per_read` - the most events one `queryEvents`
    /// returns. See `skilj::SkiljBuilder::max_events_per_read`.
    pub max_events_per_read: usize,
    /// Per-request bounds on `/graphql` - see [`limits::GraphqlLimits`].
    pub limits: limits::GraphqlLimits,
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
    /// `createBoundedContextFromTemplate`'s own synchronous refresh
    /// target (ultra-review bug_001) - the identical `TemplateCache`
    /// every dispatcher above already consults for template resolution,
    /// reused here so the resolver can close the same-instance race a
    /// refresh-only-via-cross-instance-NOTIFY design left open: calling
    /// `.refresh(&state.pool)` right after committing a new tenant means
    /// an immediate `submitCommand` on this same instance sees it, not
    /// just other instances once their own listener task catches up.
    /// See `skilj_core::template_cache`'s own doc comment for the full
    /// design and why this lives in `skilj-core`, not here or in the
    /// `skilj` facade crate that first held it.
    pub template_cache: skilj_core::template_cache::TemplateCache,
    /// `submitCommand`'s and parked-delivery redrive's own real,
    /// externally-triggered submission volume - exactly what
    /// `CommandBatcher` exists to coalesce into fewer bounded-context
    /// lock acquisitions (Codeberg issue #32, round two) - the identical
    /// `CommandBatcher` `Skilj::rest_router()` hands `skilj-rest`'s own
    /// `CommandTrigger` route, reused here for the same reason
    /// `dispatcher`/`event_broadcaster` above are: one shared, process-
    /// wide batcher, not a second one that would only ever coalesce
    /// this surface's own traffic against itself.
    pub command_batcher: skilj_core::command_batcher::CommandBatcher,
    /// The prefix every type and root field of the schema gets
    /// (docs/architecture.md §194) - read at build time, and by the
    /// resolvers that name a type at run time (a union member, say).
    pub naming: naming::Naming,
    /// Set when `/graphql` is a federation subgraph (docs/architecture.md
    /// §194): what it publishes. `naming` is its prefix's.
    pub federation: Option<Arc<federation::FederationOptions>>,
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
    let max_body = state.limits.max_request_body_bytes;
    Ok(axum::Router::new()
        .route(
            "/graphql",
            axum::routing::post(graphql_handler).get(graphql_ws_handler),
        )
        .layer(axum::middleware::from_fn(move |request, next| {
            limits::limit_request_body(max_body, request, next)
        }))
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
    let response = match auth::resolve_role(&headers, state.identity.as_ref(), &state.pool).await {
        Ok(role) => execute_as(&registry, &state, role, req.into_inner()).await,
        Err(err) => async_graphql::Response::from_errors(vec![
            err.into_server_error(async_graphql::Pos::default())
        ]),
    };
    response.into()
}

/// Runs `request` against the schema `caller` is served
/// (`SchemaRegistry::for_caller`, docs/architecture.md §138), read fresh
/// for this one request - a concurrent `SchemaRegistry::rebuild` never
/// affects a request already in flight, since it's an `Arc` snapshot,
/// not a lock. A caller with no credential gets no introspection at all.
async fn execute_as(
    registry: &schema::SchemaRegistry,
    state: &GraphqlState,
    caller: Option<skilj_core::access_control::Role>,
    mut request: async_graphql::Request,
) -> async_graphql::Response {
    // docs/architecture.md §194: a subgraph's `_service` is the published
    // description, whoever asks - not the caller's own schema.
    if state.federation.is_some()
        && federation::is_service_request(&request.query, request.operation_name.as_deref())
    {
        return match registry.published_sdl(state).await {
            Ok(sdl) => {
                let sdl = sdl.map(|sdl| sdl.to_string()).unwrap_or_default();
                federation::service_schema(sdl).execute(request).await
            }
            Err(e) => async_graphql::Response::from_errors(vec![
                error::to_graphql_error(e).into_server_error(async_graphql::Pos::default())
            ]),
        };
    }
    let schema = match registry.for_caller(state, caller.as_ref()).await {
        Ok(schema) => schema,
        Err(e) => {
            return async_graphql::Response::from_errors(vec![
                error::to_graphql_error(e).into_server_error(async_graphql::Pos::default())
            ])
        }
    };
    if caller.is_none() {
        request = request.disable_introspection();
    }
    schema.execute(request.data(caller)).await
}

/// The websocket's executor: each operation runs against the schema its
/// connection's caller is served (docs/architecture.md §138). The caller
/// is only known once `connection_init` has been handled, after the
/// websocket is set up, so it's filled in then; choosing per operation
/// also means a subscription started later on a long-lived connection
/// sees the schema as it is by then.
#[derive(Clone)]
struct CallerExecutor {
    registry: Arc<schema::SchemaRegistry>,
    state: GraphqlState,
    caller: Arc<std::sync::OnceLock<Option<skilj_core::access_control::Role>>>,
}

impl async_graphql::Executor for CallerExecutor {
    async fn execute(&self, request: async_graphql::Request) -> async_graphql::Response {
        let caller = self.caller.get().cloned().flatten();
        execute_as(&self.registry, &self.state, caller, request).await
    }

    fn execute_stream(
        &self,
        request: async_graphql::Request,
        session_data: Option<Arc<async_graphql::Data>>,
    ) -> async_graphql::futures_util::stream::BoxStream<'static, async_graphql::Response> {
        use async_graphql::futures_util::{stream, StreamExt};
        let this = self.clone();
        stream::once(async move {
            let caller = this.caller.get().cloned().flatten();
            match this.registry.for_caller(&this.state, caller.as_ref()).await {
                Ok(schema) => {
                    let request = if caller.is_none() {
                        request.disable_introspection()
                    } else {
                        request
                    };
                    async_graphql::Executor::execute_stream(&*schema, request, session_data)
                }
                Err(e) => stream::once(async move {
                    async_graphql::Response::from_errors(vec![
                        error::to_graphql_error(e).into_server_error(async_graphql::Pos::default())
                    ])
                })
                .boxed(),
            }
        })
        .flatten()
        .boxed()
    }
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
    // The same cap `POST /graphql` bodies get (docs/architecture.md §73):
    // each websocket message is a GraphQL document too, and it arrives
    // before (or without) any credential - axum's own default would
    // otherwise accept 64 MiB per message.
    let max_message = state.limits.max_request_body_bytes;
    upgrade
        .max_message_size(max_message)
        .max_frame_size(max_message)
        .protocols(async_graphql::http::ALL_WEBSOCKET_PROTOCOLS)
        .on_upgrade(move |socket| serve_websocket(socket, registry, protocol, state))
}

/// Close code sent when the connection's credential expires
/// (docs/architecture.md §135) - graphql-ws's own "Forbidden".
pub const CREDENTIAL_EXPIRED_CLOSE_CODE: u16 = 4403;

/// Close code sent when no `connection_init` arrives within
/// `GraphqlLimits::websocket_init_timeout` (docs/architecture.md §136) -
/// graphql-ws's own "Connection initialisation timeout".
pub const INIT_TIMEOUT_CLOSE_CODE: u16 = 4408;

/// Why [`serve_websocket`] closes a connection itself.
enum ServerClose {
    InitTimeout,
    CredentialExpired,
}

/// One GraphQL websocket connection. On top of what
/// `GraphQLWebSocket::serve` does, this bounds its lifetime:
///
/// - The JWT presented in `connection_init` is only verified then, but
///   the connection - and every subscription started on it later - can
///   outlive it by hours. It is closed with
///   [`CREDENTIAL_EXPIRED_CLOSE_CODE`] once the token's `exp` (plus
///   verification leeway) passes; a client reconnects with a fresh token
///   (docs/architecture.md §135).
/// - A connection that never sends `connection_init` is closed with
///   [`INIT_TIMEOUT_CLOSE_CODE`], and one whose peer has gone away
///   without closing (answering no pings) is dropped, instead of either
///   being held open forever (docs/architecture.md §136).
///
/// `GraphQLWebSocket::serve` owns the socket's sink, so its output goes
/// through a channel and is forwarded here, which leaves this loop able
/// to send pings and close frames itself.
async fn serve_websocket(
    socket: axum::extract::ws::WebSocket,
    registry: Arc<schema::SchemaRegistry>,
    protocol: async_graphql_axum::GraphQLProtocol,
    state: GraphqlState,
) {
    use async_graphql::futures_util::{sink, StreamExt};
    use axum::extract::ws::{CloseFrame, Message};

    let limits = state.limits;
    let opened_at = tokio::time::Instant::now();
    // Milliseconds after `opened_at` that the last frame of any kind -
    // pongs included - arrived from the peer.
    let last_received = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (mut ws_sink, ws_stream) = socket.split();
    let ws_stream = ws_stream.inspect({
        let last_received = last_received.clone();
        move |_| {
            last_received.store(
                opened_at.elapsed().as_millis() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    });
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<Message>(16);
    let out_sink = sink::unfold(out_tx, |out_tx, message: Message| async move {
        out_tx.send(message).await.map(|()| out_tx)
    });
    // Sent the moment `connection_init` arrives.
    let (init_received_tx, init_received_rx) = tokio::sync::oneshot::channel::<()>();
    // Sent once `connection_init` authenticates with a JWT; dropped
    // unsent for an anonymous or refused connection, which never expires.
    let (valid_until_tx, valid_until_rx) = tokio::sync::oneshot::channel::<std::time::SystemTime>();

    let executor = CallerExecutor {
        registry,
        state: state.clone(),
        caller: Default::default(),
    };
    let caller = executor.caller.clone();

    let serve = async_graphql_axum::GraphQLWebSocket::new_with_pair(
        out_sink, ws_stream, executor, protocol,
    )
    .on_connection_init(move |payload| {
        let _ = init_received_tx.send(());
        async move {
            let credential = auth::resolve_role_from_connection_init(
                &payload,
                state.identity.as_ref(),
                &state.pool,
            )
            .await?;
            let mut data = async_graphql::Data::default();
            let role = credential.map(|(role, valid_until)| {
                let _ = valid_until_tx.send(valid_until);
                role
            });
            let _ = caller.set(role.clone());
            data.insert(role);
            // docs/architecture.md §74 - this connection's own
            // running-subscription count.
            data.insert(limits::ConnectionSubscriptions::default());
            Ok(data)
        }
    })
    .serve();
    let server_close = async move {
        if tokio::time::timeout(limits.websocket_init_timeout, init_received_rx)
            .await
            .is_err()
        {
            return ServerClose::InitTimeout;
        }
        match valid_until_rx.await {
            Ok(valid_until) => {
                let remaining = valid_until
                    .duration_since(std::time::SystemTime::now())
                    .unwrap_or_default();
                tokio::time::sleep(remaining).await;
                ServerClose::CredentialExpired
            }
            Err(_) => std::future::pending().await,
        }
    };
    tokio::pin!(serve, server_close);
    let mut ping = limits.websocket_ping_interval.map(|interval| {
        let mut ping = tokio::time::interval_at(opened_at + interval, interval);
        ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ping
    });
    // A peer silent this long is gone. Also bounds each send: a gone
    // peer stops reading, and once the socket's buffers fill a send
    // would otherwise wait out TCP's own retransmission timeout.
    let dead_after = limits.websocket_ping_interval.map(|interval| interval * 2);

    let mut serving = true;
    loop {
        tokio::select! {
            () = &mut serve, if serving => serving = false,
            message = out_rx.recv() => match message {
                Some(message) => {
                    if !send_bounded(&mut ws_sink, message, dead_after).await {
                        return;
                    }
                }
                // `serve` finished and everything it sent is forwarded.
                None => return,
            },
            () = next_ping(&mut ping) => {
                let dead_after = dead_after.unwrap_or_default();
                let silent_for = opened_at.elapsed().saturating_sub(std::time::Duration::from_millis(
                    last_received.load(std::sync::atomic::Ordering::Relaxed),
                ));
                if silent_for >= dead_after {
                    // Nothing to say goodbye to: the peer isn't reading.
                    return;
                }
                if !send_bounded(&mut ws_sink, Message::Ping(Default::default()), Some(dead_after)).await {
                    return;
                }
            }
            reason = &mut server_close => {
                let (code, reason) = match reason {
                    ServerClose::InitTimeout => {
                        (INIT_TIMEOUT_CLOSE_CODE, "connection initialisation timeout")
                    }
                    ServerClose::CredentialExpired => {
                        (CREDENTIAL_EXPIRED_CLOSE_CODE, "credential expired")
                    }
                };
                let close = Message::Close(Some(CloseFrame {
                    code,
                    reason: reason.into(),
                }));
                send_bounded(&mut ws_sink, close, dead_after).await;
                return;
            }
        }
    }
}

/// Sends `message`, giving up after `limit`. `false` when the socket is
/// closed or the send timed out.
async fn send_bounded(
    sink: &mut async_graphql::futures_util::stream::SplitSink<
        axum::extract::ws::WebSocket,
        axum::extract::ws::Message,
    >,
    message: axum::extract::ws::Message,
    limit: Option<std::time::Duration>,
) -> bool {
    use async_graphql::futures_util::SinkExt;
    match limit {
        Some(limit) => matches!(
            tokio::time::timeout(limit, sink.send(message)).await,
            Ok(Ok(()))
        ),
        None => sink.send(message).await.is_ok(),
    }
}

/// The next ping tick, or never when pings are off.
async fn next_ping(ping: &mut Option<tokio::time::Interval>) {
    match ping {
        Some(ping) => {
            ping.tick().await;
        }
        None => std::future::pending().await,
    }
}
