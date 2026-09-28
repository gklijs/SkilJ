//! The one thing this crate actually is: a GraphQL client for whatever
//! `skilj-graphql` endpoint the operator points it at (see this crate's
//! own doc comment on why it depends on no other skilj crate at all -
//! this module only ever speaks the wire protocol). Two halves:
//! `Client::request` for queries/mutations (plain HTTP POST), and
//! `spawn_subscription` for the `graphql-transport-ws` protocol
//! `EventSubscription` uses - a real client for it already exists as a
//! test fixture in `skilj/tests/event_subscription.rs`; this is that
//! same wire shape, promoted from a test-only helper to something this
//! crate depends on for real.
//!
//! GraphQL responses are handled as raw `serde_json::Value`, indexed
//! dynamically - not a codegen client (`graphql_client`/`cynic`), since
//! `skilj-graphql`'s own schema is dynamic and grows per bounded context
//! (docs/architecture.md §5.1), so there's no fixed schema file to
//! codegen a typed client against. The same style every test in the rest
//! of this workspace already uses for GraphQL responses.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::fmt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

/// One entry of a GraphQL response's `errors[]` array, kept structured
/// rather than a plain string - `code`/`trace_id` are both extensions
/// every rejection in this codebase's own error shape carries (§4.2, and
/// the more recent observability pass's `traceId` addition), and the UI
/// wants to show both: `code` as the short, stable reason, `trace_id` as
/// something the operator can hand to whoever runs the collector.
#[derive(Debug, Clone)]
pub struct GraphQlError {
    pub message: String,
    pub code: Option<String>,
    pub trace_id: Option<String>,
}

impl fmt::Display for GraphQlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)?;
        if let Some(code) = &self.code {
            write!(f, " ({code})")?;
        }
        if let Some(trace_id) = &self.trace_id {
            write!(f, " [trace {trace_id}]")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum ClientError {
    Http(reqwest::Error),
    Graphql(Vec<GraphQlError>),
    /// The response had neither a `data` nor a (non-empty) `errors` -
    /// not a shape any GraphQL server should produce, but this crate
    /// talks to whatever endpoint it's pointed at, not only ones this
    /// workspace controls.
    MalformedResponse(String),
    WebSocket(tokio_tungstenite::tungstenite::Error),
    /// [`spawn_live_events`]'s subscription ended - the server completed
    /// it, or it failed - and is being re-established, resuming after
    /// `resuming_after` when that's known.
    SubscriptionEnded {
        resuming_after: Option<i64>,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Http(e) => write!(f, "HTTP error: {e}"),
            ClientError::Graphql(errors) => {
                let joined: Vec<String> = errors.iter().map(|e| e.to_string()).collect();
                write!(f, "GraphQL error: {}", joined.join("; "))
            }
            ClientError::MalformedResponse(msg) => write!(f, "malformed GraphQL response: {msg}"),
            ClientError::WebSocket(e) => write!(f, "websocket error: {e}"),
            ClientError::SubscriptionEnded {
                resuming_after: Some(sequence),
            } => write!(
                f,
                "subscription ended - reconnecting from sequence {sequence}"
            ),
            ClientError::SubscriptionEnded {
                resuming_after: None,
            } => write!(f, "subscription ended - reconnecting"),
        }
    }
}

impl std::error::Error for ClientError {}

/// A query/mutation client for one `skilj-graphql` endpoint, authenticated
/// as one Role's bearer JWT - the same credential presented however the
/// operator obtained it (see this crate's own doc comment on why this
/// never handles login itself).
pub struct Client {
    http: reqwest::Client,
    endpoint: reqwest::Url,
    token: String,
}

impl Client {
    pub fn new(endpoint: reqwest::Url, token: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            endpoint,
            token,
        }
    }

    /// Runs one query or mutation, returning the response's `data` on
    /// success. `variables` is `Value::Null` for an operation with none.
    pub async fn request(&self, query: &str, variables: Value) -> Result<Value, ClientError> {
        let body = json!({ "query": query, "variables": variables });
        let response = self
            .http
            .post(self.endpoint.clone())
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .map_err(ClientError::Http)?;
        let parsed: Value = response.json().await.map_err(ClientError::Http)?;
        extract_data(parsed)
    }
}

fn extract_data(response: Value) -> Result<Value, ClientError> {
    if let Some(errors) = response.get("errors").and_then(Value::as_array) {
        if !errors.is_empty() {
            return Err(ClientError::Graphql(
                errors.iter().map(parse_error).collect(),
            ));
        }
    }
    response.get("data").cloned().ok_or_else(|| {
        ClientError::MalformedResponse("response had neither data nor errors".into())
    })
}

fn parse_error(value: &Value) -> GraphQlError {
    GraphQlError {
        message: value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("(no message)")
            .to_string(),
        code: value
            .pointer("/extensions/code")
            .and_then(Value::as_str)
            .map(String::from),
        trace_id: value
            .pointer("/extensions/traceId")
            .and_then(Value::as_str)
            .map(String::from),
    }
}

/// Rewrites an `http(s)://.../graphql` endpoint into its `ws(s)://` twin -
/// `skilj-graphql` mounts both the query/mutation handler and the
/// websocket upgrade on the identical path (`Skilj::graphql_router()`'s
/// own doc comment), so this is the one thing that has to change.
pub fn to_websocket_url(endpoint: &reqwest::Url) -> reqwest::Url {
    let mut ws = endpoint.clone();
    let scheme = match endpoint.scheme() {
        "https" => "wss",
        _ => "ws",
    };
    ws.set_scheme(scheme)
        .expect("http(s)/ws(s) are both non-special-cased schemes to swap");
    ws
}

/// Opens a `graphql-transport-ws` subscription in a spawned task,
/// streaming each delivered `payload.data` back over the returned
/// channel - one `Err` (from a websocket-level failure, a GraphQL error
/// message, or an unexpected close) ends the stream; the caller decides
/// whether to retry. Wire shape - `connection_init`/`connection_ack`,
/// then `subscribe`/`next` - verified against a real server in
/// `skilj/tests/event_subscription.rs` already; `tests/subscription.rs`
/// in this crate verifies this client's own half of it against a small
/// local mock.
pub fn spawn_subscription(
    ws_endpoint: reqwest::Url,
    token: String,
    query: String,
    variables: Value,
) -> mpsc::UnboundedReceiver<Result<Value, ClientError>> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        if let Err(e) = run_subscription(ws_endpoint, token, query, variables, &tx).await {
            let _ = tx.send(Err(e));
        }
    });
    rx
}

/// The live `allEvents` feed the TUI shows - resumable: `$from` is the
/// last sequence already received (`null` on the first connection).
/// `QueriedEvent` (`allEvents`'s return type) only has `sequence`/
/// `payload` - no `eventType` field, confirmed against a real running
/// skilj-demo server, not assumed from the schema-builder source alone.
pub const LIVE_EVENTS_QUERY: &str = "subscription($bc: String!, $from: Int) { \
    allEvents(boundedContext: $bc, fromSequence: $from) { sequence payload } \
}";

/// [`spawn_subscription`] for [`LIVE_EVENTS_QUERY`], kept alive
/// (docs/architecture.md §106). The server ends a subscription on
/// purpose - it fell behind (`subscription_lagged`), a missed
/// cross-instance notification left a gap, the connection dropped - and
/// expects the client to resubscribe from the last sequence it received,
/// which then delivers everything since first. So this does exactly that:
/// whenever the subscription ends it sends
/// [`ClientError::SubscriptionEnded`] and resubscribes, with `fromSequence`
/// set to the last `allEvents.sequence` delivered, backing off from
/// `initial_delay` up to `max_delay` while connecting keeps failing. A
/// `resume_span_too_large` refusal (too far behind to replay) resumes from
/// now instead, after reporting it. Ends only when the receiver is dropped.
pub fn spawn_live_events(
    ws_endpoint: reqwest::Url,
    token: String,
    bounded_context: String,
    initial_delay: std::time::Duration,
    max_delay: std::time::Duration,
) -> mpsc::UnboundedReceiver<Result<Value, ClientError>> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut last_sequence: Option<i64> = None;
        let mut delay = initial_delay;
        loop {
            let mut inner = spawn_subscription(
                ws_endpoint.clone(),
                token.clone(),
                LIVE_EVENTS_QUERY.to_string(),
                json!({ "bc": bounded_context, "from": last_sequence }),
            );
            while let Some(item) = inner.recv().await {
                match &item {
                    Ok(data) => {
                        if let Some(sequence) =
                            data.pointer("/allEvents/sequence").and_then(Value::as_i64)
                        {
                            last_sequence = Some(sequence);
                        }
                        delay = initial_delay;
                    }
                    Err(ClientError::Graphql(errors))
                        if errors
                            .iter()
                            .any(|e| e.code.as_deref() == Some("resume_span_too_large")) =>
                    {
                        last_sequence = None;
                    }
                    Err(_) => {}
                }
                if tx.send(item).is_err() {
                    return;
                }
            }
            if tx
                .send(Err(ClientError::SubscriptionEnded {
                    resuming_after: last_sequence,
                }))
                .is_err()
            {
                return;
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(max_delay);
        }
    });
    rx
}

async fn run_subscription(
    ws_endpoint: reqwest::Url,
    token: String,
    query: String,
    variables: Value,
    tx: &mpsc::UnboundedSender<Result<Value, ClientError>>,
) -> Result<(), ClientError> {
    let mut request = ws_endpoint
        .to_string()
        .into_client_request()
        .map_err(ClientError::WebSocket)?;
    request.headers_mut().insert(
        "sec-websocket-protocol",
        "graphql-transport-ws"
            .parse()
            .expect("a fixed protocol-name string is always a valid header value"),
    );
    let (mut ws, _response) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(ClientError::WebSocket)?;

    send_json(
        &mut ws,
        json!({
            "type": "connection_init",
            "payload": { "Authorization": format!("Bearer {token}") },
        }),
    )
    .await?;
    let ack = recv_json(&mut ws).await?;
    if ack.get("type").and_then(Value::as_str) != Some("connection_ack") {
        return Err(ClientError::MalformedResponse(format!(
            "expected connection_ack, got {ack:?}"
        )));
    }

    send_json(
        &mut ws,
        json!({
            "id": "1",
            "type": "subscribe",
            "payload": { "query": query, "variables": variables },
        }),
    )
    .await?;

    loop {
        let message = recv_json(&mut ws).await?;
        match message.get("type").and_then(Value::as_str) {
            // A resolver error arrives as a `next` carrying `errors` (and
            // `data: null`), followed by the server's own `complete` -
            // not as a protocol-level `error` message (docs/architecture.md
            // §106).
            Some("next")
                if message
                    .pointer("/payload/errors")
                    .and_then(Value::as_array)
                    .is_some_and(|errors| !errors.is_empty()) =>
            {
                let errors = message
                    .pointer("/payload/errors")
                    .and_then(Value::as_array)
                    .map(|errs| errs.iter().map(parse_error).collect())
                    .unwrap_or_default();
                return Err(ClientError::Graphql(errors));
            }
            Some("next") => {
                let data = message.pointer("/payload/data").cloned().ok_or_else(|| {
                    ClientError::MalformedResponse(format!(
                        "next with no payload.data: {message:?}"
                    ))
                })?;
                if tx.send(Ok(data)).is_err() {
                    return Ok(()); // the receiving end (the app) hung up - nothing left to do
                }
            }
            Some("error") => {
                let errors = message
                    .pointer("/payload")
                    .and_then(Value::as_array)
                    .map(|errs| errs.iter().map(parse_error).collect())
                    .unwrap_or_default();
                return Err(ClientError::Graphql(errors));
            }
            Some("complete") => return Ok(()),
            Some("ping") => {
                send_json(&mut ws, json!({ "type": "pong" })).await?;
            }
            _ => {
                return Err(ClientError::MalformedResponse(format!(
                    "unexpected message: {message:?}"
                )))
            }
        }
    }
}

type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn send_json(ws: &mut WsStream, value: Value) -> Result<(), ClientError> {
    ws.send(Message::text(value.to_string()))
        .await
        .map_err(ClientError::WebSocket)
}

async fn recv_json(ws: &mut WsStream) -> Result<Value, ClientError> {
    match ws.next().await {
        None => Err(ClientError::WebSocket(
            tokio_tungstenite::tungstenite::Error::ConnectionClosed,
        )),
        Some(Err(e)) => Err(ClientError::WebSocket(e)),
        Some(Ok(Message::Text(text))) => serde_json::from_str(&text)
            .map_err(|e| ClientError::MalformedResponse(format!("invalid JSON: {e}"))),
        Some(Ok(other)) => Err(ClientError::MalformedResponse(format!(
            "unexpected websocket message: {other:?}"
        ))),
    }
}
