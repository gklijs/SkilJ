//! `graphql::spawn_live_events` against a mock `graphql-transport-ws`
//! server (docs/architecture.md §106): the first connection delivers one
//! event and then ends the subscription the way the server does when it
//! can't continue without a gap - a `next` carrying
//! `errors: [{ code: "subscription_lagged" }]`, then `complete`. The client
//! must report that, reconnect, and resubscribe with `fromSequence` set to
//! the last sequence it delivered.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use serde_json::{json, Value};
use skilj_tui::graphql::{spawn_live_events, ClientError};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Default)]
struct MockState {
    connections: Arc<AtomicUsize>,
    /// Each `subscribe`'s `variables`, in order.
    subscriptions: Arc<Mutex<Vec<Value>>>,
    /// The code the first connection's subscription ends with -
    /// `subscription_lagged` when `None`.
    end_code: Option<&'static str>,
}

async fn serve(state: MockState) -> reqwest::Url {
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

async fn handle_upgrade(State(state): State<MockState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.protocols(["graphql-transport-ws"])
        .on_upgrade(move |socket| run(socket, state))
}

async fn send(socket: &mut WebSocket, value: Value) {
    socket.send(Message::text(value.to_string())).await.unwrap();
}

async fn run(mut socket: WebSocket, state: MockState) {
    let Some(Ok(Message::Text(_init))) = socket.recv().await else {
        return;
    };
    send(&mut socket, json!({ "type": "connection_ack" })).await;
    let Some(Ok(Message::Text(subscribe))) = socket.recv().await else {
        return;
    };
    let subscribe: Value = serde_json::from_str(&subscribe).unwrap();
    // The feed reads the database epoch before starting from now
    // (docs/architecture.md §176); not a subscription of its own.
    if subscribe["payload"]["query"] == "{ epoch }" {
        send(
            &mut socket,
            json!({ "id": "1", "type": "next", "payload": { "data": { "epoch": "e1" } } }),
        )
        .await;
        send(&mut socket, json!({ "id": "1", "type": "complete" })).await;
        return;
    }
    let connection = state.connections.fetch_add(1, Ordering::SeqCst);
    state
        .subscriptions
        .lock()
        .unwrap()
        .push(subscribe["payload"]["variables"].clone());

    if connection == 0 {
        send(
            &mut socket,
            json!({ "id": "1", "type": "next",
                    "payload": { "data": { "allEvents": { "sequence": 5, "payload": "{}" } } } }),
        )
        .await;
        send(
            &mut socket,
            json!({ "id": "1", "type": "next",
                    "payload": { "data": null, "errors": [{
                        "message": "ended",
                        "extensions": { "code": state.end_code.unwrap_or("subscription_lagged") },
                    }] } }),
        )
        .await;
        send(&mut socket, json!({ "id": "1", "type": "complete" })).await;
    } else {
        send(
            &mut socket,
            json!({ "id": "1", "type": "next",
                    "payload": { "data": { "allEvents": { "sequence": 6, "payload": "{}" } } } }),
        )
        .await;
        // Hold the connection open.
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
async fn a_subscription_the_server_ends_is_resumed_from_the_last_sequence() {
    let state = MockState::default();
    let ws_endpoint = serve(state.clone()).await;
    let mut rx = spawn_live_events(
        ws_endpoint,
        "test-jwt".to_string(),
        String::new(),
        "banking".to_string(),
        Duration::from_millis(10),
        Duration::from_millis(50),
    );

    assert_eq!(
        recv(&mut rx).await.unwrap(),
        json!({ "allEvents": { "sequence": 5, "payload": "{}" } })
    );
    match recv(&mut rx).await {
        Err(ClientError::Graphql(errors)) => {
            assert_eq!(errors[0].code.as_deref(), Some("subscription_lagged"))
        }
        other => panic!("expected the lagged error, got {other:?}"),
    }
    match recv(&mut rx).await {
        Err(ClientError::SubscriptionEnded { resuming_after }) => {
            assert_eq!(resuming_after, Some(5))
        }
        other => panic!("expected SubscriptionEnded, got {other:?}"),
    }
    assert_eq!(
        recv(&mut rx).await.unwrap(),
        json!({ "allEvents": { "sequence": 6, "payload": "{}" } })
    );

    let subscriptions = state.subscriptions.lock().unwrap().clone();
    assert_eq!(
        subscriptions[0],
        json!({ "bc": "banking", "from": null, "epoch": "e1" })
    );
    assert_eq!(
        subscriptions[1],
        json!({ "bc": "banking", "from": 5, "epoch": "e1" })
    );
}

/// A refusal to resume from the last sequence - too far behind
/// (`resume_span_too_large`), or past the latest sequence because the
/// bounded context was recreated (`from_sequence_not_committed`,
/// docs/architecture.md §148), or read before a failover
/// (`epoch_changed`, §176) - is refused on every retry, so the feed
/// resumes from now (`from: null`, in the epoch read again) instead of
/// repeating it forever.
#[tokio::test]
async fn a_refused_resume_starts_again_from_now() {
    for code in [
        "resume_span_too_large",
        "from_sequence_not_committed",
        "epoch_changed",
    ] {
        let state = MockState {
            end_code: Some(code),
            ..MockState::default()
        };
        let ws_endpoint = serve(state.clone()).await;
        let mut rx = spawn_live_events(
            ws_endpoint,
            "test-jwt".to_string(),
            String::new(),
            "banking".to_string(),
            Duration::from_millis(10),
            Duration::from_millis(50),
        );
        recv(&mut rx).await.unwrap();
        match recv(&mut rx).await {
            Err(ClientError::Graphql(errors)) => assert_eq!(errors[0].code.as_deref(), Some(code)),
            other => panic!("expected the {code} error, got {other:?}"),
        }
        match recv(&mut rx).await {
            Err(ClientError::SubscriptionEnded { resuming_after }) => {
                assert_eq!(resuming_after, None, "{code}")
            }
            other => panic!("expected SubscriptionEnded, got {other:?}"),
        }
        recv(&mut rx).await.unwrap();
        let subscriptions = state.subscriptions.lock().unwrap().clone();
        assert_eq!(
            subscriptions[1],
            json!({ "bc": "banking", "from": null, "epoch": "e1" }),
            "{code}"
        );
    }
}
