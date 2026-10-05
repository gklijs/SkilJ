//! `outbound_cycle` against a small mock of skilj's consume/ack routes and
//! a fake broker, so what gets delivered and what gets acknowledged can be
//! asserted call by call. The mock refuses an acknowledgement that moves
//! the cursor backwards with 409, as the real server does
//! (docs/architecture.md §75) - an earlier mock accepted any order, which
//! is how the bug in §169 went unnoticed.

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::{json, Value};
use skilj_bridge::{
    outbound_cycle, owns_partition, ConsumedEvent, OutboundRetryState, OutboundSink,
    OutboundTarget, SkiljError,
};
use skilj_retry::RetryPolicy;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const EVENT_TYPE: &str = "OrderPlaced";

#[derive(Default)]
struct Mock {
    events: Vec<Value>,
    cursor: i64,
    acks: Vec<i64>,
    consumes: usize,
    fail_acks: usize,
    /// The database epoch consume serves under, and an ack must name if it
    /// names one (docs/architecture.md §176).
    epoch: String,
    /// Becomes `epoch` right after the next consume - a failover between
    /// a consume and its acknowledgement.
    fail_over_to: Option<String>,
}

type Shared = Arc<Mutex<Mock>>;

async fn consume(State(mock): State<Shared>) -> Json<Value> {
    let mut mock = mock.lock().unwrap();
    mock.consumes += 1;
    let cursor = mock.cursor;
    let events: Vec<Value> = mock
        .events
        .iter()
        .filter(|e| e["sequence"].as_i64().unwrap() > cursor)
        .cloned()
        .collect();
    let epoch = mock.epoch.clone();
    if let Some(next) = mock.fail_over_to.take() {
        mock.epoch = next;
    }
    Json(json!({ "events": events, "eventTypeName": EVENT_TYPE, "epoch": epoch }))
}

async fn ack(State(mock): State<Shared>, Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
    let mut mock = mock.lock().unwrap();
    if mock.fail_acks > 0 {
        mock.fail_acks -= 1;
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(Value::Null));
    }
    if body["epoch"]
        .as_str()
        .is_some_and(|epoch| epoch != mock.epoch)
    {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "code": "epoch_changed" })),
        );
    }
    let sequence = body["sequence"].as_i64().unwrap();
    if sequence < mock.cursor {
        return (StatusCode::CONFLICT, Json(Value::Null));
    }
    mock.cursor = sequence;
    mock.acks.push(sequence);
    (StatusCode::OK, Json(json!({})))
}

async fn serve(events: Vec<Value>) -> (String, Shared) {
    let mock: Shared = Arc::new(Mutex::new(Mock {
        events,
        cursor: -1,
        epoch: "epoch-1".to_string(),
        ..Mock::default()
    }));
    let app = Router::new()
        .route("/v1/events/consume", get(consume))
        .route("/v1/events/consume/ack", post(ack))
        .with_state(mock.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), mock)
}

fn event(sequence: i64, order: &str) -> Value {
    json!({
        "sequence": sequence,
        "eventType": EVENT_TYPE,
        "payload": { "orderId": order },
        "tags": [{ "key": "order", "value": order }],
    })
}

#[derive(Debug)]
enum TestError {
    Skilj(SkiljError),
    Broker(i64),
}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestError::Skilj(e) => write!(f, "{e}"),
            TestError::Broker(sequence) => write!(f, "broker refused {sequence}"),
        }
    }
}

impl From<SkiljError> for TestError {
    fn from(e: SkiljError) -> Self {
        TestError::Skilj(e)
    }
}

/// Records every delivery; refuses a sequence while it has failures left
/// for it (`u32::MAX` for "always").
#[derive(Default)]
struct FakeBroker {
    delivered: Vec<i64>,
    failures: HashMap<i64, u32>,
}

impl OutboundSink for FakeBroker {
    type Error = TestError;

    fn broker_name(&self) -> &'static str {
        "the fake broker"
    }

    async fn deliver(&mut self, event: &ConsumedEvent) -> Result<(), TestError> {
        if let Some(remaining) = self.failures.get_mut(&event.sequence) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(TestError::Broker(event.sequence));
            }
        }
        self.delivered.push(event.sequence);
        Ok(())
    }
}

fn target(partition: Option<(u32, u32)>) -> OutboundTarget<'static> {
    OutboundTarget {
        credential: "read-token",
        event_type: EVENT_TYPE,
        partition,
        key_tag_key: Some("order"),
    }
}

/// Retries immediately, so a second cycle can run straight away.
fn retry_at_once(max_attempts: u32) -> RetryPolicy {
    RetryPolicy::bounded(Duration::ZERO, 1.0, Duration::ZERO, max_attempts)
}

async fn cycle(
    base_url: &str,
    target: &OutboundTarget<'_>,
    broker: &mut FakeBroker,
    policy: &RetryPolicy,
    retry_state: &mut Option<OutboundRetryState>,
) -> usize {
    outbound_cycle(
        &skilj_bridge::http_client(),
        base_url,
        target,
        broker,
        policy,
        retry_state,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn one_acknowledgement_covers_a_contiguous_run() {
    let (base_url, mock) = serve((1..=5).map(|s| event(s, "o-1")).collect()).await;
    let mut broker = FakeBroker::default();
    let mut retry_state = None;

    let served = cycle(
        &base_url,
        &target(None),
        &mut broker,
        &retry_at_once(5),
        &mut retry_state,
    )
    .await;

    assert_eq!(served, 5);
    assert_eq!(broker.delivered, [1, 2, 3, 4, 5]);
    assert_eq!(
        mock.lock().unwrap().acks,
        [5],
        "one call for the whole page"
    );
    assert!(retry_state.is_none());
}

#[tokio::test]
async fn a_failed_delivery_acknowledges_the_run_before_it_and_nothing_past_it() {
    let (base_url, mock) = serve((1..=5).map(|s| event(s, "o-1")).collect()).await;
    let mut broker = FakeBroker {
        failures: HashMap::from([(3, 1)]),
        ..FakeBroker::default()
    };
    let mut retry_state = None;
    let policy = retry_at_once(5);

    let served = cycle(
        &base_url,
        &target(None),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;
    assert_eq!(served, 2);
    assert_eq!(broker.delivered, [1, 2]);
    assert_eq!(mock.lock().unwrap().acks, [2]);
    assert!(retry_state.is_some());

    let served = cycle(
        &base_url,
        &target(None),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;
    assert_eq!(served, 3);
    assert_eq!(broker.delivered, [1, 2, 3, 4, 5]);
    assert_eq!(mock.lock().unwrap().acks, [2, 5]);
    assert!(retry_state.is_none());
}

/// Codeberg issue #39 / §169: events another partition owns join the run,
/// but the run is never acknowledged past an owned event that failed.
#[tokio::test]
async fn a_partition_skip_never_acknowledges_past_a_failed_owned_event() {
    let partition = Some((0, 2));
    let keys: Vec<String> = (0..64).map(|i| format!("o-{i}")).collect();
    let owned = keys
        .iter()
        .find(|k| owns_partition(partition, Some(k)))
        .unwrap();
    let other = keys
        .iter()
        .find(|k| !owns_partition(partition, Some(k)))
        .unwrap();
    let (base_url, mock) = serve(vec![
        event(1, other),
        event(2, owned),
        event(3, other),
        event(4, owned),
        event(5, other),
    ])
    .await;
    let mut broker = FakeBroker {
        failures: HashMap::from([(4, 1)]),
        ..FakeBroker::default()
    };
    let mut retry_state = None;
    let policy = retry_at_once(5);

    let served = cycle(
        &base_url,
        &target(partition),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;
    assert_eq!(served, 1);
    assert_eq!(broker.delivered, [2]);
    assert_eq!(
        mock.lock().unwrap().acks,
        [3],
        "up to the failed event, not past it"
    );

    let served = cycle(
        &base_url,
        &target(partition),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;
    assert_eq!(served, 1);
    assert_eq!(broker.delivered, [2, 4]);
    assert_eq!(mock.lock().unwrap().acks, [3, 5]);
}

/// A failed acknowledgement is charged to the run's first event: the whole
/// run is delivered again, and an exhausted policy gives up by
/// acknowledging anyway - counted as not served, like any give-up.
#[tokio::test]
async fn a_failed_acknowledgement_redelivers_the_run_then_gives_up() {
    let (base_url, mock) = serve((1..=3).map(|s| event(s, "o-1")).collect()).await;
    mock.lock().unwrap().fail_acks = 2;
    let mut broker = FakeBroker::default();
    let mut retry_state = None;
    let policy = retry_at_once(2);

    let served = cycle(
        &base_url,
        &target(None),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;
    assert_eq!(served, 0);
    assert!(mock.lock().unwrap().acks.is_empty());
    assert!(retry_state.is_some());

    let served = cycle(
        &base_url,
        &target(None),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;
    assert_eq!(served, 0);
    assert_eq!(broker.delivered, [1, 2, 3, 1, 2, 3]);
    assert_eq!(mock.lock().unwrap().acks, [3]);
    assert!(retry_state.is_none());
}

/// An event the policy gives up on is acknowledged along with the run it
/// is part of, and the events behind it are still delivered this cycle.
#[tokio::test]
async fn an_event_given_up_on_is_acknowledged_with_the_rest_of_its_run() {
    let (base_url, mock) = serve((1..=4).map(|s| event(s, "o-1")).collect()).await;
    let mut broker = FakeBroker {
        failures: HashMap::from([(2, u32::MAX)]),
        ..FakeBroker::default()
    };
    let mut retry_state = None;

    let served = cycle(
        &base_url,
        &target(None),
        &mut broker,
        &retry_at_once(1),
        &mut retry_state,
    )
    .await;

    assert_eq!(served, 3);
    assert_eq!(broker.delivered, [1, 3, 4]);
    assert_eq!(mock.lock().unwrap().acks, [1, 4]);
    assert!(retry_state.is_none());
}

#[tokio::test]
async fn a_cycle_in_backoff_does_not_even_consume() {
    let (base_url, mock) = serve(vec![event(1, "o-1")]).await;
    let mut broker = FakeBroker {
        failures: HashMap::from([(1, u32::MAX)]),
        ..FakeBroker::default()
    };
    let mut retry_state = None;
    let policy = RetryPolicy::bounded(Duration::from_secs(60), 1.0, Duration::from_secs(60), 5);

    cycle(
        &base_url,
        &target(None),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;
    let served = cycle(
        &base_url,
        &target(None),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;

    assert_eq!(served, 0);
    assert_eq!(mock.lock().unwrap().consumes, 1);
    assert!(mock.lock().unwrap().acks.is_empty());
}

/// docs/architecture.md §176: a failover between a consume and its
/// acknowledgement. The acknowledgement is refused with `epoch_changed`;
/// that's neither retried in backoff nor given up on by acknowledging
/// anyway (which would be refused again). The next cycle consumes from the
/// cursor afresh and acknowledges under the new epoch.
#[tokio::test]
async fn an_epoch_change_drops_the_batch_and_consumes_again() {
    let (base_url, mock) = serve((1..=3).map(|s| event(s, "o-1")).collect()).await;
    mock.lock().unwrap().fail_over_to = Some("epoch-2".to_string());
    let mut broker = FakeBroker::default();
    let mut retry_state = None;
    let policy = retry_at_once(1);

    let served = cycle(
        &base_url,
        &target(None),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;
    assert_eq!(served, 0, "nothing was acknowledged");
    assert!(retry_state.is_none(), "not a failure to back off from");
    assert!(mock.lock().unwrap().acks.is_empty());

    let served = cycle(
        &base_url,
        &target(None),
        &mut broker,
        &policy,
        &mut retry_state,
    )
    .await;
    assert_eq!(served, 3);
    assert_eq!(broker.delivered, [1, 2, 3, 1, 2, 3], "at-least-once");
    assert_eq!(mock.lock().unwrap().acks, [3]);
}
