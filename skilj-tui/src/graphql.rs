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

pub use crate::token::TokenSource;

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
    /// The server closed the websocket because the token it was opened
    /// with expired (close code 4403, docs/architecture.md §135).
    CredentialExpired,
    /// The server refused `connection_init` - closed the websocket
    /// instead of acknowledging it; `reason` is the server's.
    InitRefused(String),
    /// The server closed the websocket for another reason.
    Closed {
        code: u16,
        reason: String,
    },
    /// `--token-command` failed (docs/architecture.md §137).
    TokenCommand(String),
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
            ClientError::CredentialExpired => write!(f, "the token expired"),
            ClientError::InitRefused(reason) => write!(f, "connection refused: {reason}"),
            ClientError::Closed { code, reason } => {
                write!(f, "connection closed by the server ({code}): {reason}")
            }
            ClientError::TokenCommand(msg) => write!(f, "token command failed: {msg}"),
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
    token: TokenSource,
    /// See [`Client::with_prefix`].
    prefix: String,
}

impl Client {
    /// A client for a token that is never replaced.
    pub fn new(endpoint: reqwest::Url, token: String) -> Self {
        Self::with_token_source(endpoint, TokenSource::fixed(token))
    }

    /// A client whose token is refreshed when the server refuses it as
    /// no longer valid (docs/architecture.md §137).
    pub fn with_token_source(endpoint: reqwest::Url, token: TokenSource) -> Self {
        Self {
            http: reqwest::Client::new(),
            endpoint,
            token,
            prefix: String::new(),
        }
    }

    /// For a skilj whose `/graphql` is a federation subgraph with a name
    /// prefix (docs/architecture.md §194): every request's root fields
    /// are sent prefixed, aliased back to their own names so responses
    /// read the same ([`with_prefix`]).
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Runs one query or mutation, returning the response's `data` on
    /// success. `variables` is `Value::Null` for an operation with none.
    /// A refusal of the token itself (`jwt_verification_failed` - an
    /// expired token among others) is retried once with a refreshed
    /// token, when the token source can refresh.
    pub async fn request(&self, query: &str, variables: Value) -> Result<Value, ClientError> {
        let body = json!({ "query": with_prefix(query, &self.prefix), "variables": variables });
        let token = self.token.current().await;
        let result = self.send(&body, &token).await;
        match result {
            Err(ClientError::Graphql(ref errors)) if is_token_refused(errors) => {
                match self.token.refresh(&token).await? {
                    Some(fresh) => self.send(&body, &fresh).await,
                    None => result,
                }
            }
            other => other,
        }
    }

    async fn send(&self, body: &Value, token: &str) -> Result<Value, ClientError> {
        let response = self
            .http
            .post(self.endpoint.clone())
            .bearer_auth(token)
            .json(body)
            .send()
            .await
            .map_err(ClientError::Http)?;
        let parsed: Value = response.json().await.map_err(ClientError::Http)?;
        extract_data(parsed)
    }
}

/// `document`, one of this crate's own operations, with each of its root
/// fields renamed the way a skilj with name prefix `prefix` names them
/// (docs/architecture.md §194: `ledger` turns `queryEvents` into
/// `ledgerQueryEvents`), and aliased back to its own name, so the
/// response has the keys it would have without a prefix. Unchanged when
/// `prefix` is empty. A root field is a name at the top level of the
/// operation's selection set; `__typename`, fragments and anything inside
/// arguments or a nested selection are left alone. Only meant for the
/// documents written in this crate, which use no fragments at the top
/// level.
pub fn with_prefix(document: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        return document.to_string();
    }
    let mut out = String::with_capacity(document.len() + 64);
    let mut chars = document.char_indices().peekable();
    let (mut braces, mut parens) = (0usize, 0usize);
    let mut in_string = false;
    while let Some((i, c)) = chars.next() {
        if in_string {
            out.push(c);
            if c == '\\' {
                if let Some((_, escaped)) = chars.next() {
                    out.push(escaped);
                }
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => in_string = true,
            '{' => braces += 1,
            '}' => braces = braces.saturating_sub(1),
            '(' => parens += 1,
            ')' => parens = parens.saturating_sub(1),
            _ => {}
        }
        let starts_name = (c.is_ascii_alphabetic() || c == '_')
            && !document[..i]
                .ends_with(|p: char| p.is_ascii_alphanumeric() || p == '_' || p == '$');
        if braces == 1 && parens == 0 && starts_name {
            let end = document[i..]
                .find(|n: char| !(n.is_ascii_alphanumeric() || n == '_'))
                .map_or(document.len(), |len| i + len);
            let name = &document[i..end];
            let rest = document[end..].trim_start();
            if !name.starts_with("__") && name != "on" && !rest.starts_with(':') {
                let mut first = prefix.chars();
                let lower: String = first
                    .next()
                    .map(|f| f.to_ascii_lowercase())
                    .into_iter()
                    .chain(first)
                    .collect();
                let mut field = name.chars();
                let upper: String = field
                    .next()
                    .map(|f| f.to_ascii_uppercase())
                    .into_iter()
                    .chain(field)
                    .collect();
                if document[..i].trim_end().ends_with(':') {
                    // Already aliased: only the field is renamed.
                    out.push_str(&format!("{lower}{upper}"));
                } else {
                    out.push_str(&format!("{name}: {lower}{upper}"));
                }
                while chars.peek().is_some_and(|(j, _)| *j < end) {
                    chars.next();
                }
                continue;
            }
        }
        out.push(c);
    }
    out
}

/// Whether the server refused the token itself, rather than the
/// operation.
fn is_token_refused(errors: &[GraphQlError]) -> bool {
    errors
        .iter()
        .any(|e| e.code.as_deref() == Some("jwt_verification_failed"))
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

/// [`LIVE_EVENTS_QUERY`] with the database epoch `$from` was read in
/// (docs/architecture.md §176), for a server that has one: after a
/// failover or restore it's refused with `epoch_changed` rather than
/// resumed from a sequence the new history may have reused.
pub const LIVE_EVENTS_IN_EPOCH_QUERY: &str =
    "subscription($bc: String!, $from: Int, $epoch: String) { \
    allEvents(boundedContext: $bc, fromSequence: $from, epoch: $epoch) { sequence payload } \
}";

/// The server's current database epoch (`{ epoch }`), or `None` when it
/// can't be had - a server from before §176 has no such field, and the
/// feed then resumes without one, as it always did.
async fn fetch_epoch(ws_endpoint: reqwest::Url, token: String, prefix: &str) -> Option<String> {
    let mut rx = spawn_subscription(
        ws_endpoint,
        token,
        with_prefix("{ epoch }", prefix),
        json!({}),
    );
    match rx.recv().await? {
        Ok(data) => data
            .get("epoch")
            .and_then(Value::as_str)
            .map(str::to_string),
        Err(_) => None,
    }
}

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
/// `resume_span_too_large` refusal (too far behind to replay) or a
/// `from_sequence_not_committed` one (the bounded context was recreated,
/// its sequences starting over - §148) resumes from now instead, after
/// reporting it, and so does `epoch_changed` (a failover or restore since
/// the last sequence was read - §176): whenever it starts from now it
/// reads the server's epoch first and resumes in it. Ends only when the
/// receiver is dropped.
///
/// `prefix` is the server's name prefix, as [`Client::with_prefix`]
/// takes it; empty for none.
///
/// When the server closes the connection because the token expired, or
/// refuses `connection_init`, the token is refreshed first if `token`
/// can be (docs/architecture.md §137); after an expiry the reconnect is
/// immediate.
pub fn spawn_live_events(
    ws_endpoint: reqwest::Url,
    token: impl Into<TokenSource>,
    prefix: String,
    bounded_context: String,
    initial_delay: std::time::Duration,
    max_delay: std::time::Duration,
) -> mpsc::UnboundedReceiver<Result<Value, ClientError>> {
    let token = token.into();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut last_sequence: Option<i64> = None;
        let mut epoch: Option<String> = None;
        let mut delay = initial_delay;
        loop {
            let used_token = token.current().await;
            // Starting from now: the epoch the sequences about to arrive
            // are read in, sent back with the last of them on a resume.
            if last_sequence.is_none() {
                epoch = fetch_epoch(ws_endpoint.clone(), used_token.clone(), &prefix).await;
            }
            let (query, variables) = match &epoch {
                Some(epoch) => (
                    LIVE_EVENTS_IN_EPOCH_QUERY,
                    json!({ "bc": bounded_context, "from": last_sequence, "epoch": epoch }),
                ),
                None => (
                    LIVE_EVENTS_QUERY,
                    json!({ "bc": bounded_context, "from": last_sequence }),
                ),
            };
            let mut inner = spawn_subscription(
                ws_endpoint.clone(),
                used_token.clone(),
                with_prefix(query, &prefix),
                variables,
            );
            let mut expired = false;
            let mut refused = false;
            while let Some(item) = inner.recv().await {
                match &item {
                    Err(ClientError::CredentialExpired) => expired = true,
                    Err(ClientError::InitRefused(_)) => refused = true,
                    Ok(data) => {
                        if let Some(sequence) =
                            data.pointer("/allEvents/sequence").and_then(Value::as_i64)
                        {
                            last_sequence = Some(sequence);
                        }
                        delay = initial_delay;
                    }
                    // Resuming from `last_sequence` is refused for good:
                    // too far behind to replay, or - `from_sequence_not_committed`
                    // - past the latest sequence, the bounded context having
                    // been deleted and recreated (docs/architecture.md
                    // §148), or - `epoch_changed` - read before a failover
                    // or restore (§176). Retried as it was, it would be
                    // refused on every reconnect; it resumes from now
                    // instead.
                    Err(ClientError::Graphql(errors))
                        if errors.iter().any(|e| {
                            matches!(
                                e.code.as_deref(),
                                Some(
                                    "resume_span_too_large"
                                        | "from_sequence_not_committed"
                                        | "epoch_changed"
                                )
                            )
                        }) =>
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
            let refreshed = if (expired || refused) && token.can_refresh() {
                match token.refresh(&used_token).await {
                    Ok(_) => true,
                    Err(e) => {
                        if tx.send(Err(e)).is_err() {
                            return;
                        }
                        false
                    }
                }
            } else {
                false
            };
            if expired && refreshed {
                // An expiry is routine, not a failure to back off from.
                delay = initial_delay;
                continue;
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
    let ack = match recv_json(&mut ws).await {
        Err(ClientError::Closed { reason, .. }) => return Err(ClientError::InitRefused(reason)),
        other => other?,
    };
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
    loop {
        return match ws.next().await {
            None => Err(ClientError::WebSocket(
                tokio_tungstenite::tungstenite::Error::ConnectionClosed,
            )),
            Some(Err(e)) => Err(ClientError::WebSocket(e)),
            Some(Ok(Message::Text(text))) => serde_json::from_str(&text)
                .map_err(|e| ClientError::MalformedResponse(format!("invalid JSON: {e}"))),
            // The server pings to find dead connections; tungstenite
            // answers on its own (docs/architecture.md §136).
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
            Some(Ok(Message::Close(frame))) => Err(match frame {
                Some(frame) if u16::from(frame.code) == 4403 => ClientError::CredentialExpired,
                Some(frame) => ClientError::Closed {
                    code: frame.code.into(),
                    reason: frame.reason.to_string(),
                },
                None => {
                    ClientError::WebSocket(tokio_tungstenite::tungstenite::Error::ConnectionClosed)
                }
            }),
            Some(Ok(other)) => Err(ClientError::MalformedResponse(format!(
                "unexpected websocket message: {other:?}"
            ))),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::with_prefix;

    #[test]
    fn root_fields_are_prefixed_and_aliased_back() {
        assert_eq!(with_prefix("{ epoch }", ""), "{ epoch }");
        assert_eq!(with_prefix("{ epoch }", "ledger"), "{ epoch: ledgerEpoch }");
        assert_eq!(
            with_prefix(
                "query($bc: String!) { eventTypes(boundedContext: $bc) { name schema } }",
                "Ledger"
            ),
            "query($bc: String!) { eventTypes: ledgerEventTypes(boundedContext: $bc) { name schema } }"
        );
        assert_eq!(
            with_prefix(
                "mutation($p: String!) { submitCommand(boundedContext: \"x\", payload: $p) { \
                 accepted } __typename }",
                "ledger"
            ),
            "mutation($p: String!) { submitCommand: ledgerSubmitCommand(boundedContext: \"x\", \
             payload: $p) { accepted } __typename }"
        );
        assert_eq!(
            with_prefix(
                "query { projection(name: \"a\") { __typename ... on X { total } } }",
                "ledger"
            ),
            "query { projection: ledgerProjection(name: \"a\") { __typename ... on X { total } } }"
        );
        // Already aliased: the alias is kept, the field renamed.
        assert_eq!(with_prefix("{ e: epoch }", "ledger"), "{ e: ledgerEpoch }");
    }
}
