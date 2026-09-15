//! End-to-end proof of `skilj_nats::produce_once`/`dispatch_inbound_message`
//! against a real, ephemeral NATS server with JetStream enabled
//! (`testcontainers_modules::nats`), neither mocked at the protocol
//! level it matters for. The skilj *side* is a small local mock server,
//! the identical "wire protocol only, no real skilj crate" shape
//! `skilj-kafka/tests/kafka_bridge.rs`/`skilj-amqp/tests/amqp_bridge.rs`
//! already use - skilj's own wire contracts are each already
//! exhaustively tested elsewhere; this crate's own job is proving it
//! calls them correctly with real JetStream deliveries on the other
//! end.
//!
//! One shared, ephemeral server for the whole file (`OnceCell`) from
//! the start - the lesson `skilj-kafka/tests/kafka_bridge.rs` only
//! learned after a first draft started one container per test and
//! starved this sandbox's Docker daemon under `cargo test`'s default
//! parallelism, applied here from the beginning instead of repeated.
//!
//! Needs a reachable Docker daemon - see CONTRIBUTING.md's own note on
//! the `DOCKER_HOST` quirk this can hit on WSL. Skips gracefully, the
//! same tolerance every other real-external-dependency test in this
//! workspace already has, if Docker isn't reachable at all.

use async_nats::jetstream::{self, consumer::pull};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::TryStreamExt;
use serde_json::{json, Value};
use skilj_nats::{
    dispatch_inbound_message, produce_once, run_inbound, InboundAction, InboundMapping,
    InboundMessageMeta, OutboundMapping, PullConsumer,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for these tests")
    })
}

// --- provisioning: one shared, ephemeral NATS+JetStream server for the
// whole file - see this file's own doc comment for why.

struct TestNats {
    url: String,
    _container: ContainerAsync<Nats>,
}

static TEST_NATS: tokio::sync::OnceCell<Option<TestNats>> = tokio::sync::OnceCell::const_new();

async fn test_nats() -> Option<&'static str> {
    TEST_NATS
        .get_or_init(provision_nats)
        .await
        .as_ref()
        .map(|n| n.url.as_str())
}

async fn provision_nats() -> Option<TestNats> {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = match Nats::default().with_cmd(&cmd).start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!(
                "skipping: starting the ephemeral NATS container failed \
                 (no reachable Docker daemon, most likely - see CONTRIBUTING.md's \
                 own DOCKER_HOST note): {e}"
            );
            return None;
        }
    };
    let host = match container.get_host().await {
        Ok(host) => host,
        Err(e) => {
            eprintln!("skipping: getting the NATS container's own host failed: {e}");
            return None;
        }
    };
    let port = match container.get_host_port_ipv4(4222).await {
        Ok(port) => port,
        Err(e) => {
            eprintln!("skipping: getting the NATS container's own mapped port failed: {e}");
            return None;
        }
    };
    Some(TestNats {
        url: format!("{host}:{port}"),
        _container: container,
    })
}

/// A fresh JetStream context with its own, uniquely-named stream -
/// every test gets its own stream (subjects like `"{stream_name}.>"`)
/// so nothing they publish/consume interferes with another test's own
/// traffic on the same shared server.
async fn jetstream_with_stream(url: &str, stream_name: &str) -> jetstream::Context {
    let client = async_nats::ConnectOptions::default()
        .connect(url)
        .await
        .expect("connecting to the ephemeral NATS server must succeed");
    let jetstream = jetstream::new(client);
    jetstream
        .create_stream(jetstream::stream::Config {
            name: stream_name.to_string(),
            subjects: vec![format!("{stream_name}.>")],
            ..Default::default()
        })
        .await
        .expect("creating a fresh stream must succeed");
    jetstream
}

async fn pull_consumer(jetstream: &jetstream::Context, stream_name: &str) -> PullConsumer {
    let stream = jetstream
        .get_stream(stream_name)
        .await
        .expect("the stream must already exist");
    stream
        .create_consumer(pull::Config {
            durable_name: Some("test-consumer".to_string()),
            ..Default::default()
        })
        .await
        .expect("creating a pull consumer must succeed")
}

fn unique_name(prefix: &str) -> String {
    format!(
        "{prefix}{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

// --- mock skilj server: consume/ack (outbound), external/trigger (inbound) ---

type TriggerRequest = (Value, Option<String>);

#[derive(Clone, Default)]
struct MockSkiljState {
    queues: Arc<Mutex<std::collections::HashMap<String, std::collections::VecDeque<Value>>>>,
    event_types: Arc<Mutex<std::collections::HashMap<String, String>>>,
    acked: Arc<Mutex<Vec<i64>>>,
    external_requests: Arc<Mutex<Vec<Value>>>,
    trigger_requests: Arc<Mutex<Vec<TriggerRequest>>>,
    /// Codeberg issue #21 - `POST /v1/events/external` returns a 500
    /// while this is `> 0`, decrementing it each time - the deterministic
    /// "the target keeps failing" trigger
    /// `an_inbound_message_parks_and_reports_after_exhausting_retries`
    /// needs.
    fail_external_requests: Arc<Mutex<usize>>,
    /// Same idea, for `POST /v1/events/consume/ack` -
    /// `an_outbound_event_is_skipped_after_exhausting_a_configured_retry_cap`'s
    /// own deterministic trigger.
    fail_acks: Arc<Mutex<usize>>,
    /// Every `POST /v1/parked-deliveries` body this mock ever received.
    parked_deliveries: Arc<Mutex<Vec<Value>>>,
}

async fn get_events_consume(
    State(state): State<MockSkiljState>,
    headers: HeaderMap,
) -> Json<Value> {
    let token = bearer_token(&headers);
    // Codeberg issue #21 - peeks rather than drains, so a manual-ack
    // caller that fails to ack (`produce_once`'s own retry tests) gets
    // the identical still-unacked event(s) back on its next GET, the
    // real semantics `GET /v1/events/consume?mode=manual` has. Removal
    // happens only in `post_events_consume_ack` below, once an ack
    // actually succeeds.
    let events: Vec<Value> = state
        .queues
        .lock()
        .unwrap()
        .get(&token)
        .map(|q| q.iter().cloned().collect())
        .unwrap_or_default();
    let event_type_name = state
        .event_types
        .lock()
        .unwrap()
        .get(&token)
        .cloned()
        .unwrap_or_default();
    Json(json!({ "events": events, "eventTypeName": event_type_name }))
}

async fn post_events_consume_ack(
    State(state): State<MockSkiljState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    {
        let mut remaining = state.fail_acks.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }
    let sequence = body["sequence"].as_i64().unwrap();
    state.acked.lock().unwrap().push(sequence);
    let token = bearer_token(&headers);
    if let Some(q) = state.queues.lock().unwrap().get_mut(&token) {
        q.retain(|e| e["sequence"].as_i64().unwrap_or(i64::MIN) > sequence);
    }
    StatusCode::OK
}

async fn post_events_external(
    State(state): State<MockSkiljState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    {
        let mut remaining = state.fail_external_requests.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(Value::Null));
        }
    }
    state.external_requests.lock().unwrap().push(body);
    (
        StatusCode::CREATED,
        Json(json!({ "sequence": 1, "redelivered": false })),
    )
}

async fn post_parked_deliveries(
    State(state): State<MockSkiljState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state.parked_deliveries.lock().unwrap().push(body);
    (StatusCode::CREATED, Json(json!({ "id": "parked-1" })))
}

async fn post_commands_trigger(
    State(state): State<MockSkiljState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    state
        .trigger_requests
        .lock()
        .unwrap()
        .push((body, idempotency_key));
    (
        StatusCode::OK,
        Json(json!({ "accepted": true, "triggeredEventSequences": [1], "deduplicated": false })),
    )
}

fn enqueue(
    state: &MockSkiljState,
    token: &str,
    event_type: &str,
    events: impl Into<std::collections::VecDeque<Value>>,
) {
    state
        .event_types
        .lock()
        .unwrap()
        .insert(token.to_string(), event_type.to_string());
    state
        .queues
        .lock()
        .unwrap()
        .insert(token.to_string(), events.into());
}

fn bearer_token(headers: &HeaderMap) -> String {
    headers
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap()
        .strip_prefix("Bearer ")
        .unwrap()
        .to_string()
}

async fn serve_mock_skilj(state: MockSkiljState) -> String {
    let app = Router::new()
        .route("/v1/events/consume", get(get_events_consume))
        .route("/v1/events/consume/ack", post(post_events_consume_ack))
        .route("/v1/events/external", post(post_events_external))
        .route("/v1/commands/trigger", post(post_commands_trigger))
        .route("/v1/parked-deliveries", post(post_parked_deliveries))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// The outbound scenario: a skilj `OrderPlaced` event, tagged with a
/// real `order` DCB tag, gets published to a real JetStream subject
/// with that tag's own value as the `Skilj-Correlation-Key` header, and
/// `"{bounded_context}:{sequence}"` as `Nats-Msg-Id` - proving
/// `produce_once` actually talks to a real JetStream server correctly.
/// Consumed back with a real pull consumer to verify the payload and
/// headers round-trip exactly, and the mock skilj server's own
/// `/consume/ack` was called only *after* the publish succeeded.
#[test]
fn an_order_placed_event_is_published_with_its_own_tag_as_correlation_header() {
    runtime().block_on(async {
        let Some(url) = test_nats().await else {
            return;
        };
        let stream_name = unique_name("ORDERS");
        let jetstream = jetstream_with_stream(url, &stream_name).await;
        let consumer = pull_consumer(&jetstream, &stream_name).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let token = "read-token".to_string();
        enqueue(
            &mock_state,
            &token,
            "OrderPlaced",
            [json!({
                "sequence": 7,
                "eventType": "OrderPlaced",
                "payload": { "orderId": "o-42" },
                "tags": [{ "key": "order", "value": "o-42" }],
                "metadata": { "correlationId": "corr-42", "causationId": null },
            })],
        );

        let mapping = OutboundMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token,
            subject: format!("{stream_name}.orders"),
            correlation_tag_key: Some("order".to_string()),
        };
        let http = reqwest::Client::new();
        let retry_policy = skilj_retry::RetryPolicy::default();
        let mut retry_state = None;
        let served = produce_once(
            &http,
            &skilj_base_url,
            &jetstream,
            "banking",
            &mapping,
            &retry_policy,
            &mut retry_state,
        )
        .await
        .unwrap();
        assert_eq!(served, 1);

        let mut messages = consumer.messages().await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(15), messages.try_next())
            .await
            .expect("must receive the published message within 15s")
            .unwrap()
            .expect("stream must not have ended");
        message.ack().await.unwrap();

        let headers = message.headers.as_ref().unwrap();
        assert_eq!(
            headers.get("Skilj-Correlation-Key").map(|v| v.to_string()),
            Some("o-42".to_string())
        );
        assert_eq!(
            headers.get("Nats-Msg-Id").map(|v| v.to_string()),
            Some("banking:7".to_string())
        );
        // Codeberg issue #18 - the event's own correlation_id round-trips
        // as a distinct `Skilj-Correlation-Id` header, not to be confused
        // with `Skilj-Correlation-Key` above (an unrelated, pre-existing
        // DCB-tag-derived header). No `Skilj-Causation-Id` header at all -
        // `causationId: null` on the consumed event means nothing is
        // sent, not an empty-string header.
        assert_eq!(
            headers.get("Skilj-Correlation-Id").map(|v| v.to_string()),
            Some("corr-42".to_string())
        );
        assert!(headers.get("Skilj-Causation-Id").is_none());
        let payload: Value = serde_json::from_slice(&message.payload).unwrap();
        assert_eq!(payload, json!({ "orderId": "o-42" }));

        assert_eq!(
            mock_state.acked.lock().unwrap().as_slice(),
            &[7],
            "skilj must have been acked for sequence 7 after the real JetStream publish succeeded"
        );
    });
}

/// The inbound `Record` scenario: a real message published to a real
/// stream, consumed back with a real pull consumer, and dispatched
/// through `dispatch_inbound_message` exactly as `run_inbound` would -
/// proving the `dedupe` partition key/sequence sent to skilj are
/// genuinely read from this message's own real, broker-assigned
/// `(stream, stream_sequence)`, not synthesised - and, unlike AMQP,
/// always present (never optional) for any real JetStream delivery.
#[test]
fn an_inbound_record_message_carries_its_own_real_stream_and_sequence_as_dedupe() {
    runtime().block_on(async {
        let Some(url) = test_nats().await else {
            return;
        };
        let stream_name = unique_name("ORDERSIN");
        let jetstream = jetstream_with_stream(url, &stream_name).await;
        let consumer = pull_consumer(&jetstream, &stream_name).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        jetstream
            .publish(format!("{stream_name}.in"), r#"{"orderId":"o-1"}"#.into())
            .await
            .unwrap()
            .await
            .unwrap();

        let mut messages = consumer.messages().await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(15), messages.try_next())
            .await
            .expect("must receive the published message within 15s")
            .unwrap()
            .expect("stream must not have ended");

        let meta = InboundMessageMeta::from_message(&message).unwrap();

        let mapping = InboundMapping {
            credential: "external-token".to_string(),
            action: InboundAction::Record {
                event_type: "OrderPlaced".to_string(),
            },
        };
        let http = reqwest::Client::new();
        dispatch_inbound_message(&http, &skilj_base_url, &mapping, &meta, &message.payload)
            .await
            .unwrap();
        message.ack().await.unwrap();

        let requests = mock_state.external_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["payload"], json!({ "orderId": "o-1" }));
        assert_eq!(requests[0]["dedupe"]["partitionKey"], json!(stream_name));
        assert_eq!(requests[0]["dedupe"]["sequence"], json!(1));
    });
}

/// Codeberg issue #18 - the inbound half of the same round trip the
/// outbound test above already proves: a real JetStream message carrying
/// `Skilj-Correlation-Id`/`Skilj-Causation-Id` headers gets them read
/// back (`InboundMessageMeta::from_message`) and forwarded as
/// `correlationId`/`causationId` on the `POST /v1/events/external` body
/// `dispatch_inbound_message` sends - not left for skilj to generate a
/// fresh one, which is what would happen if this bridge silently dropped
/// them.
#[test]
fn an_inbound_record_message_forwards_its_own_correlation_and_causation_headers() {
    runtime().block_on(async {
        let Some(url) = test_nats().await else {
            return;
        };
        let stream_name = unique_name("ORDERSINCORR");
        let jetstream = jetstream_with_stream(url, &stream_name).await;
        let consumer = pull_consumer(&jetstream, &stream_name).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Skilj-Correlation-Id", "upstream-corr-1");
        headers.insert("Skilj-Causation-Id", "upstream-cause-1");
        jetstream
            .publish_with_headers(
                format!("{stream_name}.in"),
                headers,
                r#"{"orderId":"o-2"}"#.into(),
            )
            .await
            .unwrap()
            .await
            .unwrap();

        let mut messages = consumer.messages().await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(15), messages.try_next())
            .await
            .expect("must receive the published message within 15s")
            .unwrap()
            .expect("stream must not have ended");

        let meta = InboundMessageMeta::from_message(&message).unwrap();

        let mapping = InboundMapping {
            credential: "external-token".to_string(),
            action: InboundAction::Record {
                event_type: "OrderPlaced".to_string(),
            },
        };
        let http = reqwest::Client::new();
        dispatch_inbound_message(&http, &skilj_base_url, &mapping, &meta, &message.payload)
            .await
            .unwrap();
        message.ack().await.unwrap();

        let requests = mock_state.external_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["correlationId"], json!("upstream-corr-1"));
        assert_eq!(requests[0]["causationId"], json!("upstream-cause-1"));
    });
}

/// The inbound `Trigger` scenario: a real message published with a real
/// `Nats-Msg-Id` header, dispatched as a command trigger - proving
/// `Idempotency-Key` is derived from that message's own real
/// `Nats-Msg-Id`, not a synthesised value.
#[test]
fn an_inbound_trigger_message_derives_its_idempotency_key_from_its_own_real_msg_id() {
    runtime().block_on(async {
        let Some(url) = test_nats().await else {
            return;
        };
        let stream_name = unique_name("WITHDRAWIN");
        let jetstream = jetstream_with_stream(url, &stream_name).await;
        let consumer = pull_consumer(&jetstream, &stream_name).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", "real-msg-id-456");
        jetstream
            .publish_with_headers(
                format!("{stream_name}.in"),
                headers,
                r#"{"amount":20}"#.into(),
            )
            .await
            .unwrap()
            .await
            .unwrap();

        let mut messages = consumer.messages().await.unwrap();
        let message = tokio::time::timeout(Duration::from_secs(15), messages.try_next())
            .await
            .expect("must receive the published message within 15s")
            .unwrap()
            .expect("stream must not have ended");

        let meta = InboundMessageMeta::from_message(&message).unwrap();

        let mapping = InboundMapping {
            credential: "command-token".to_string(),
            action: InboundAction::Trigger {
                command_type: "WithdrawMoney".to_string(),
            },
        };
        let http = reqwest::Client::new();
        dispatch_inbound_message(&http, &skilj_base_url, &mapping, &meta, &message.payload)
            .await
            .unwrap();
        message.ack().await.unwrap();

        let requests = mock_state.trigger_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0["payload"], json!({ "amount": 20 }));
        assert_eq!(requests[0].1.as_deref(), Some("real-msg-id-456"));
    });
}

/// Codeberg issue #21 - a message that keeps failing to dispatch is
/// retried with backoff (the mock's own `fail_external_requests` toggle
/// makes `POST /v1/events/external` fail exactly twice), then reported
/// to skilj's own `POST /v1/parked-deliveries` and the message acked
/// anyway once `retry_policy` (`max_attempts: 2`) exhausts -
/// `run_inbound` itself, not `dispatch_inbound_message` directly, since
/// the retry loop lives there.
#[test]
fn an_inbound_message_parks_and_reports_after_exhausting_retries() {
    runtime().block_on(async {
        let Some(url) = test_nats().await else {
            return;
        };
        let stream_name = unique_name("ORDERSPARK");
        let jetstream = jetstream_with_stream(url, &stream_name).await;
        let consumer = pull_consumer(&jetstream, &stream_name).await;

        let mock_state = MockSkiljState::default();
        *mock_state.fail_external_requests.lock().unwrap() = 2;
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        jetstream
            .publish(
                format!("{stream_name}.in"),
                r#"{"orderId":"o-parked"}"#.into(),
            )
            .await
            .unwrap()
            .await
            .unwrap();

        let mapping = InboundMapping {
            credential: "external-token".to_string(),
            action: InboundAction::Record {
                event_type: "OrderPlaced".to_string(),
            },
        };
        let http = reqwest::Client::new();
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(10),
            1.0,
            Duration::from_millis(10),
            2,
        );
        tokio::spawn(async move {
            run_inbound(&consumer, &http, &skilj_base_url, &mapping, &retry_policy).await;
        });

        // 20s budget, not 5s - see skilj-kafka's own identical test for
        // why real broker connection/session setup needs more slack than
        // this test's own retry-policy math alone.
        let mut parked = None;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let reports = mock_state.parked_deliveries.lock().unwrap();
            if let Some(p) = reports.first() {
                parked = Some(p.clone());
                break;
            }
        }
        let parked =
            parked.expect("run_inbound must have reported a parked delivery within the timeout");
        assert_eq!(parked["source"], json!("nats-inbound"));
        assert_eq!(parked["kind"], json!("external_event"));
        assert_eq!(parked["attemptCount"], json!(2));
        assert_eq!(
            parked["request"]["payload"],
            json!({ "orderId": "o-parked" })
        );
        assert_eq!(parked["identifier"], json!(format!("{stream_name}:1")));
        assert_eq!(
            mock_state.external_requests.lock().unwrap().len(),
            0,
            "every attempt failed, so a real event must never have been created"
        );
    });
}

/// Codeberg issue #21 - the outbound direction's own shape: unlike
/// inbound, exhausting a *configured* `retry_policy` (default is
/// `RetryPolicy::unbounded` - this test opts into a bounded one
/// specifically to exercise the cap) skips the event instead of parking
/// it - acknowledged to skilj without ever successfully publishing it,
/// no `POST /v1/parked-deliveries` call at all. `fail_acks` (not a
/// publish failure) is this test's own deterministic trigger - see
/// `skilj_kafka`'s own identical test for why that still exercises the
/// real skip path even though the event *is* re-published to JetStream
/// on each retry (an existing, pre-issue-#21 property of retrying a
/// combined publish+ack step as one unit, not something this pass
/// changes) - JetStream's own server-side dedup on the repeated
/// `Nats-Msg-Id` means those retried publishes don't even create
/// duplicate messages here, unlike Kafka's own equivalent test.
#[test]
fn an_outbound_event_is_skipped_after_exhausting_a_configured_retry_cap() {
    runtime().block_on(async {
        let Some(url) = test_nats().await else {
            return;
        };
        let stream_name = unique_name("ORDERSSKIP");
        let jetstream = jetstream_with_stream(url, &stream_name).await;

        let mock_state = MockSkiljState::default();
        *mock_state.fail_acks.lock().unwrap() = 2;
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let token = "read-token-skip".to_string();
        enqueue(
            &mock_state,
            &token,
            "OrderPlaced",
            [json!({
                "sequence": 7,
                "eventType": "OrderPlaced",
                "payload": { "orderId": "o-skip" },
                "tags": [{ "key": "order", "value": "o-skip" }],
                "metadata": { "correlationId": null, "causationId": null },
            })],
        );

        let mapping = OutboundMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token,
            subject: format!("{stream_name}.skip"),
            correlation_tag_key: Some("order".to_string()),
        };
        let http = reqwest::Client::new();
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(10),
            1.0,
            Duration::from_millis(10),
            2,
        );
        let mut retry_state = None;

        // Cycle 1: publish succeeds, ack fails (1/2 of the fail budget) -
        // not yet exhausted, so this cycle stops here without skipping.
        let served = produce_once(
            &http,
            &skilj_base_url,
            &jetstream,
            "banking",
            &mapping,
            &retry_policy,
            &mut retry_state,
        )
        .await
        .unwrap();
        assert_eq!(served, 0);
        assert!(retry_state.is_some());
        assert!(mock_state.acked.lock().unwrap().is_empty());

        tokio::time::sleep(Duration::from_millis(20)).await;

        // Cycle 2: backoff has elapsed - publish succeeds again, ack
        // fails again (2/2), which exhausts `max_attempts: 2`. The event
        // is skipped: acknowledged directly, without a 3rd publish.
        let served = produce_once(
            &http,
            &skilj_base_url,
            &jetstream,
            "banking",
            &mapping,
            &retry_policy,
            &mut retry_state,
        )
        .await
        .unwrap();
        assert_eq!(
            served, 0,
            "a skipped event is not counted as served - it was never actually delivered"
        );
        assert!(retry_state.is_none());
        assert_eq!(mock_state.acked.lock().unwrap().as_slice(), &[7]);
        assert!(
            mock_state.parked_deliveries.lock().unwrap().is_empty(),
            "outbound gives up by skipping, never by parking"
        );
    });
}
