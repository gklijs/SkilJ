//! `post_inbound_batch` (docs/architecture.md §227) against a mock skilj:
//! how many messages it reports done, and how it adapts when skilj refuses
//! a batch.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};
use skilj_bridge::{
    post_inbound_batch, InboundAction, InboundBatcher, InboundBatching, InboundItem, InboundMapping,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Mock {
    /// Every batch call's item count.
    calls: Vec<usize>,
    /// The first item with this payload `n` fails, and the rest of its
    /// batch is not processed.
    fail_n: Option<i64>,
    /// Batches larger than this are refused with `batch_too_large`.
    max_items: Option<usize>,
    /// The `idempotencyKey` of every processed item.
    keys: Vec<Option<String>>,
}

type Shared = Arc<Mutex<Mock>>;

async fn trigger_batch(
    State(mock): State<Shared>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    assert_eq!(body["stopAtFirstFailure"], true);
    let commands = body["commands"].as_array().unwrap();
    let mut mock = mock.lock().unwrap();
    mock.calls.push(commands.len());
    if mock.max_items.is_some_and(|max| commands.len() > max) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "code": "batch_too_large", "message": "too large" })),
        );
    }
    let mut failed = false;
    let mut outcomes = Vec::new();
    for command in commands {
        if failed {
            outcomes.push(json!({ "error": { "status": 409, "code": "not_processed" } }));
        } else if Some(command["payload"]["n"].as_i64().unwrap()) == mock.fail_n {
            failed = true;
            outcomes.push(json!({ "error": { "status": 500, "code": "database_error" } }));
        } else {
            mock.keys
                .push(command["idempotencyKey"].as_str().map(str::to_string));
            outcomes.push(json!({ "accepted": true, "triggeredEventSequences": [1] }));
        }
    }
    (StatusCode::OK, Json(json!({ "outcomes": outcomes })))
}

async fn serve(mock: Mock, with_batch_route: bool) -> (String, Shared) {
    let shared: Shared = Arc::new(Mutex::new(mock));
    let mut app = Router::new();
    if with_batch_route {
        app = app.route("/v1/commands/trigger/batch", post(trigger_batch));
    }
    let app = app.with_state(shared.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, shared)
}

fn mapping() -> InboundMapping {
    InboundMapping {
        credential: "id.secret".to_string(),
        action: InboundAction::Trigger {
            command_type: "ShipOrder".to_string(),
        },
    }
}

fn bodies(count: i64) -> Vec<Value> {
    (0..count)
        .map(|n| json!({ "payload": { "n": n } }))
        .collect()
}

/// `post_inbound_batch` over `bodies`, the n-th keyed `"key-{n}"`.
async fn send(url: &str, batcher: &InboundBatcher, bodies: &[Value]) -> usize {
    let keys: Vec<String> = (0..bodies.len()).map(|n| format!("key-{n}")).collect();
    let items: Vec<InboundItem<'_>> = bodies
        .iter()
        .zip(&keys)
        .map(|(body, key)| InboundItem {
            body,
            idempotency_key: Some(key),
        })
        .collect();
    post_inbound_batch(&reqwest::Client::new(), url, &mapping(), batcher, &items).await
}

#[tokio::test]
async fn every_message_is_done_in_batches_of_at_most_max_items() {
    let (url, mock) = serve(Mock::default(), true).await;
    let batcher = InboundBatcher::new(InboundBatching {
        max_items: 4,
        ..InboundBatching::default()
    });

    assert_eq!(send(&url, &batcher, &bodies(10)).await, 10);
    let mock = mock.lock().unwrap();
    assert_eq!(mock.calls, vec![4, 4, 2]);
    let expected: Vec<Option<String>> = (0..10).map(|n| Some(format!("key-{n}"))).collect();
    assert_eq!(mock.keys, expected);
}

#[tokio::test]
async fn it_stops_at_the_first_message_that_is_not_done() {
    let (url, mock) = serve(
        Mock {
            fail_n: Some(5),
            ..Mock::default()
        },
        true,
    )
    .await;
    let batcher = InboundBatcher::new(InboundBatching {
        max_items: 4,
        ..InboundBatching::default()
    });

    // 0-3 in the first call; 4 done and 5 failed in the second, 6-7 not
    // processed: the caller deals with 5 alone, then sends 6 on.
    assert_eq!(send(&url, &batcher, &bodies(8)).await, 5);
    assert_eq!(mock.lock().unwrap().calls, vec![4, 4]);
}

#[tokio::test]
async fn a_lone_message_is_left_to_the_single_route() {
    let (url, mock) = serve(Mock::default(), true).await;
    let batcher = InboundBatcher::new(InboundBatching::default());

    assert_eq!(send(&url, &batcher, &bodies(1)).await, 0);
    assert!(mock.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn batching_off_sends_no_batch() {
    let (url, mock) = serve(Mock::default(), true).await;
    let batcher = InboundBatcher::new(InboundBatching::off());

    assert_eq!(send(&url, &batcher, &bodies(5)).await, 0);
    assert!(mock.lock().unwrap().calls.is_empty());
}

#[tokio::test]
async fn a_skilj_without_the_batch_route_turns_batching_off() {
    let (url, _mock) = serve(Mock::default(), false).await;
    let batcher = InboundBatcher::new(InboundBatching::default());

    assert_eq!(send(&url, &batcher, &bodies(5)).await, 0);
    assert_eq!(batcher.max_items(), 1);
}

#[tokio::test]
async fn a_batch_too_large_is_halved_until_skilj_takes_it() {
    let (url, mock) = serve(
        Mock {
            max_items: Some(3),
            ..Mock::default()
        },
        true,
    )
    .await;
    let batcher = InboundBatcher::new(InboundBatching {
        max_items: 10,
        ..InboundBatching::default()
    });

    assert_eq!(send(&url, &batcher, &bodies(10)).await, 10);
    assert_eq!(batcher.max_items(), 2);
    // 10 refused, 5 refused, then pairs.
    assert_eq!(mock.lock().unwrap().calls, vec![10, 5, 2, 2, 2, 2, 2]);
}

#[tokio::test]
async fn an_unreachable_skilj_leaves_everything_to_the_single_route() {
    let batcher = InboundBatcher::new(InboundBatching::default());
    // Nothing listens on port 9 (discard) here.
    assert_eq!(send("http://127.0.0.1:9", &batcher, &bodies(5)).await, 0);
    // A failed call is not a reason to stop batching.
    assert_eq!(batcher.max_items(), 100);
}
