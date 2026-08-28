//! Verifies `graphql::spawn_subscription`'s own half of the
//! `graphql-transport-ws` handshake against a small local mock websocket
//! server - `connection_init`/`connection_ack`, `subscribe`, then a
//! `next` message's `payload.data` delivered over the returned channel.
//! The real server side of this same protocol is already proven against
//! a real `skilj-graphql` schema in `skilj/tests/event_subscription.rs`;
//! this is this crate's own client half, verified independently.

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::IntoResponse;
use serde_json::{json, Value};
use skilj_tui::graphql::spawn_subscription;

async fn serve_mock_subscription_server() -> reqwest::Url {
    let app = axum::Router::new().route("/graphql", axum::routing::get(handle_upgrade));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("ws://{addr}/graphql").parse().unwrap()
}

async fn handle_upgrade(ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.protocols(["graphql-transport-ws"])
        .on_upgrade(run_mock_protocol)
}

/// Plays the server's own half of the handshake by hand - not
/// `async-graphql`'s real implementation (this crate deliberately
/// depends on no `skilj-*` crate, `async-graphql` included), just enough
/// of the wire protocol to prove the client drives it correctly:
/// `connection_init` -> `connection_ack`, `subscribe` -> one `next`,
/// then a `complete`.
async fn run_mock_protocol(mut socket: WebSocket) {
    let Some(Ok(Message::Text(init))) = socket.recv().await else {
        return;
    };
    let init: Value = serde_json::from_str(&init).unwrap();
    assert_eq!(init["type"], "connection_init");
    assert_eq!(init["payload"]["Authorization"], "Bearer test-jwt");

    socket
        .send(Message::text(
            json!({ "type": "connection_ack" }).to_string(),
        ))
        .await
        .unwrap();

    let Some(Ok(Message::Text(subscribe))) = socket.recv().await else {
        return;
    };
    let subscribe: Value = serde_json::from_str(&subscribe).unwrap();
    assert_eq!(subscribe["type"], "subscribe");
    assert_eq!(subscribe["payload"]["variables"]["bc"], "banking");

    socket
        .send(Message::text(
            json!({
                "id": "1",
                "type": "next",
                "payload": { "data": { "allEvents": { "sequence": 42 } } },
            })
            .to_string(),
        ))
        .await
        .unwrap();

    socket
        .send(Message::text(
            json!({ "id": "1", "type": "complete" }).to_string(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn drives_the_handshake_and_delivers_one_next_payload() {
    let ws_endpoint = serve_mock_subscription_server().await;

    let mut rx = spawn_subscription(
        ws_endpoint,
        "test-jwt".to_string(),
        "subscription($bc: String!) { allEvents(boundedContext: $bc) { sequence } }".to_string(),
        json!({ "bc": "banking" }),
    );

    let delivered = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for the next payload")
        .expect("the channel should not have closed before delivering anything")
        .expect("the delivered payload should be Ok");

    assert_eq!(delivered, json!({ "allEvents": { "sequence": 42 } }));

    // The mock server sends `complete` right after - the channel should
    // end cleanly (`None`), not with a trailing `Err`.
    let after = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for the channel to close");
    assert!(
        after.is_none(),
        "expected the channel to close after \"complete\", got {after:?}"
    );
}
