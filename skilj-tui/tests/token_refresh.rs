//! `--token-command` (docs/architecture.md §137): a token the server
//! refuses as expired is replaced by running the command again - on the
//! live feed (a 4403 close) and on queries (`jwt_verification_failed`) -
//! and several refusals at once run it once.
#![cfg(unix)]

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use serde_json::{json, Value};
use skilj_tui::graphql::{spawn_live_events, Client, ClientError, TokenSource};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A token command whose output is whatever `token` holds, logging each
/// run to `runs`.
struct TokenCommand {
    dir: PathBuf,
}

impl TokenCommand {
    fn new(initial: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "skilj-tui-token-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let command = Self { dir };
        command.set(initial);
        command
    }

    fn set(&self, token: &str) {
        std::fs::write(self.dir.join("token"), token).unwrap();
    }

    fn command(&self) -> String {
        format!(
            "echo run >> '{0}/runs'; cat '{0}/token'",
            self.dir.display()
        )
    }

    fn runs(&self) -> usize {
        std::fs::read_to_string(self.dir.join("runs"))
            .map(|s| s.lines().count())
            .unwrap_or(0)
    }
}

impl Drop for TokenCommand {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// --- live feed ---

#[derive(Clone, Default)]
struct WsState {
    /// Each connection's `connection_init` token and `subscribe` variables.
    connections: Arc<Mutex<Vec<(String, Value)>>>,
}

async fn serve_ws(state: WsState) -> reqwest::Url {
    let app = axum::Router::new()
        .route("/graphql", axum::routing::get(handle_upgrade))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("ws://{addr}/graphql").parse().unwrap()
}

async fn handle_upgrade(State(state): State<WsState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.protocols(["graphql-transport-ws"])
        .on_upgrade(move |socket| run_ws(socket, state))
}

async fn send(socket: &mut WebSocket, value: Value) {
    socket.send(Message::text(value.to_string())).await.unwrap();
}

/// Serves "old" one event and then closes the way skilj does when a
/// token expires; serves "new" another event and stays open.
async fn run_ws(mut socket: WebSocket, state: WsState) {
    let Some(Ok(Message::Text(init))) = socket.recv().await else {
        return;
    };
    let init: Value = serde_json::from_str(&init).unwrap();
    let token = init["payload"]["Authorization"]
        .as_str()
        .unwrap()
        .trim_start_matches("Bearer ")
        .to_string();
    send(&mut socket, json!({ "type": "connection_ack" })).await;
    let Some(Ok(Message::Text(subscribe))) = socket.recv().await else {
        return;
    };
    let subscribe: Value = serde_json::from_str(&subscribe).unwrap();
    state
        .connections
        .lock()
        .unwrap()
        .push((token.clone(), subscribe["payload"]["variables"].clone()));

    let sequence = if token == "old" { 5 } else { 6 };
    send(
        &mut socket,
        json!({ "id": "1", "type": "next",
                "payload": { "data": { "allEvents": { "sequence": sequence, "payload": "{}" } } } }),
    )
    .await;
    if token == "old" {
        let _ = socket
            .send(Message::Close(Some(CloseFrame {
                code: 4403,
                reason: "credential expired".into(),
            })))
            .await;
    } else {
        let _ = socket.recv().await;
    }
}

async fn recv(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<Result<Value, ClientError>>,
) -> Result<Value, ClientError> {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out")
        .expect("the live feed ended")
}

#[tokio::test]
async fn an_expired_token_on_the_live_feed_is_refreshed_and_the_feed_resumes() {
    let command = TokenCommand::new("old");
    let token = TokenSource::from_command(command.command()).await.unwrap();
    command.set("new");
    let state = WsState::default();
    let ws_endpoint = serve_ws(state.clone()).await;
    let mut rx = spawn_live_events(
        ws_endpoint,
        token.clone(),
        "banking".to_string(),
        // Long enough that a reconnect only arrives in time if it
        // skipped the backoff, as an expiry should.
        Duration::from_secs(30),
        Duration::from_secs(30),
    );

    assert_eq!(
        recv(&mut rx).await.unwrap(),
        json!({ "allEvents": { "sequence": 5, "payload": "{}" } })
    );
    assert!(
        matches!(recv(&mut rx).await, Err(ClientError::CredentialExpired)),
        "the expiry is reported"
    );
    assert!(matches!(
        recv(&mut rx).await,
        Err(ClientError::SubscriptionEnded {
            resuming_after: Some(5)
        })
    ));
    assert_eq!(
        recv(&mut rx).await.unwrap(),
        json!({ "allEvents": { "sequence": 6, "payload": "{}" } })
    );

    let connections = state.connections.lock().unwrap().clone();
    assert_eq!(connections[0].0, "old");
    assert_eq!(connections[1].0, "new", "reconnected with the fresh token");
    assert_eq!(connections[1].1, json!({ "bc": "banking", "from": 5 }));
    assert_eq!(token.current().await, "new", "shared with queries too");
    assert_eq!(command.runs(), 2, "at startup, then once for the expiry");
}

// --- queries ---

/// Answers "new" with data and anything else as skilj answers an expired
/// token; records each request's token.
async fn serve_http(seen: Arc<Mutex<Vec<String>>>) -> reqwest::Url {
    let app = axum::Router::new().route(
        "/graphql",
        axum::routing::post(move |headers: axum::http::HeaderMap| {
            let seen = seen.clone();
            async move {
                let token = headers
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .trim_start_matches("Bearer ")
                    .to_string();
                seen.lock().unwrap().push(token.clone());
                axum::Json(if token == "new" {
                    json!({ "data": { "ok": true } })
                } else {
                    json!({ "data": null, "errors": [{
                        "message": "JWT verification failed: ExpiredSignature",
                        "extensions": { "code": "jwt_verification_failed" },
                    }] })
                })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}/graphql").parse().unwrap()
}

#[tokio::test]
async fn a_query_refused_for_its_token_is_retried_once_with_a_fresh_one() {
    let command = TokenCommand::new("old");
    let token = TokenSource::from_command(command.command()).await.unwrap();
    command.set("new");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let client = Client::with_token_source(serve_http(seen.clone()).await, token);

    let data = client.request("{ ok }", Value::Null).await.unwrap();
    assert_eq!(data, json!({ "ok": true }));
    assert_eq!(*seen.lock().unwrap(), ["old", "new"]);

    // A command still printing a refused token: one retry, then the
    // refusal is the answer - no loop.
    command.set("stale-again");
    seen.lock().unwrap().clear();
    let client = Client::with_token_source(
        serve_http(seen.clone()).await,
        TokenSource::from_command(command.command()).await.unwrap(),
    );
    match client.request("{ ok }", Value::Null).await {
        Err(ClientError::Graphql(errors)) => {
            assert_eq!(errors[0].code.as_deref(), Some("jwt_verification_failed"))
        }
        other => panic!("expected the refusal, got {other:?}"),
    }
    assert_eq!(seen.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_fixed_token_is_not_retried() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let client = Client::new(serve_http(seen.clone()).await, "old".to_string());
    assert!(client.request("{ ok }", Value::Null).await.is_err());
    assert_eq!(seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn refusals_at_once_run_the_command_once() {
    let command = TokenCommand::new("old");
    let token = TokenSource::from_command(command.command()).await.unwrap();
    command.set("new");

    let refreshes = (0..8).map(|_| {
        let token = token.clone();
        tokio::spawn(async move { token.refresh("old").await.unwrap() })
    });
    for refresh in refreshes {
        assert_eq!(refresh.await.unwrap().as_deref(), Some("new"));
    }
    assert_eq!(command.runs(), 2, "at startup, then once for all eight");
}
