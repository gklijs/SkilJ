//! End-to-end proof of `skilj_kafka::produce_once`/`dispatch_inbound_message`
//! against a real, ephemeral Kafka broker (`testcontainers_modules::kafka`,
//! KRaft mode, no ZooKeeper - dev-dependency only, see this crate's own
//! Cargo.toml), neither mocked at the protocol level it matters for. The
//! skilj *side* is a small local mock server, the same "wire protocol
//! only, no real skilj crate" treatment `skilj-temporal/tests/temporal_bridge.rs`
//! already gives its own Temporal-facing half - skilj's own `GET
//! /v1/events/consume`/`POST /v1/events/consume/ack`/`POST
//! /v1/events/external`/`POST /v1/commands/trigger` contracts are each
//! already exhaustively tested against a real server elsewhere; this
//! crate's own job is proving it calls those contracts correctly with
//! real Kafka messages on the other end, not re-proving skilj's own
//! implementation of them.
//!
//! Needs a reachable Docker daemon - see CONTRIBUTING.md's own note on
//! the `DOCKER_HOST` quirk this can hit on WSL (`unset DOCKER_HOST`
//! before running these tests, if `docker version` doesn't already
//! work). Skips gracefully, the same tolerance every other
//! real-external-dependency test in this workspace already has, if
//! Docker isn't reachable at all.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, Message};
use serde_json::{json, Value};
use skilj_kafka::{
    dispatch_inbound_message, produce_once, InboundAction, InboundMapping, OutboundMapping,
};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers_modules::kafka::apache;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for these tests")
    })
}

// --- provisioning: one shared, ephemeral Kafka container for the whole
// file, the same `OnceCell`-backed "provision once, every test reuses
// it" shape every embedded-Postgres-backed test suite in this workspace
// already uses (see e.g. `skilj-core/tests/submit_command.rs`'s own
// `TEST_DB`/`test_pool`) - one container per *test*, not per file, is
// what a first draft of this file did, and running three of them
// concurrently (`cargo test`'s own default parallelism) genuinely
// starved this sandbox's own Docker daemon (`OperationTimedOut` on
// topic creation, confirmed by testing - not assumed - across repeated
// runs). Each test still gets its own uniquely-named topic
// (`unique_topic`), so sharing one broker introduces no cross-test
// interference.

struct TestKafka {
    bootstrap_servers: String,
    // Never read directly - kept alive for as long as `TEST_KAFKA`
    // itself is, which is the whole test binary's lifetime, the same
    // "held by the static, dropped at process exit" treatment
    // `postgresql_embedded::PostgreSQL` gets in every other test file's
    // own `TestDb`.
    _container: testcontainers_modules::testcontainers::ContainerAsync<apache::Kafka>,
}

static TEST_KAFKA: tokio::sync::OnceCell<Option<TestKafka>> = tokio::sync::OnceCell::const_new();

/// `None` (skip) if Docker isn't reachable here, the same tolerance this
/// workspace's own embedded-Postgres/ephemeral-Temporal tests already
/// have for their own external dependency.
async fn test_kafka() -> Option<&'static str> {
    TEST_KAFKA
        .get_or_init(provision_kafka)
        .await
        .as_ref()
        .map(|k| k.bootstrap_servers.as_str())
}

async fn provision_kafka() -> Option<TestKafka> {
    let node = match apache::Kafka::default().start().await {
        Ok(node) => node,
        Err(e) => {
            eprintln!(
                "skipping: starting the ephemeral Kafka container failed \
                 (no reachable Docker daemon, most likely - see CONTRIBUTING.md's \
                 own DOCKER_HOST note): {e}"
            );
            return None;
        }
    };
    let bootstrap_servers = match node.get_host_port_ipv4(apache::KAFKA_PORT).await {
        Ok(port) => format!("127.0.0.1:{port}"),
        Err(e) => {
            eprintln!("skipping: getting the Kafka container's own mapped port failed: {e}");
            return None;
        }
    };
    Some(TestKafka {
        bootstrap_servers,
        _container: node,
    })
}

// --- mock skilj server: consume/ack (outbound), external/trigger (inbound) ---

/// One `POST /v1/commands/trigger` body this mock ever received,
/// paired with its own `Idempotency-Key` header.
type TriggerRequest = (Value, Option<String>);

#[derive(Clone, Default)]
struct MockSkiljState {
    /// Keyed by bearer credential - simulates an `EventReadToken`'s own
    /// fixed scope, the same shape `skilj-temporal`'s own mock uses.
    queues: Arc<Mutex<std::collections::HashMap<String, VecDeque<Value>>>>,
    event_types: Arc<Mutex<std::collections::HashMap<String, String>>>,
    acked: Arc<Mutex<Vec<i64>>>,
    /// Every `POST /v1/events/external` body this mock ever received,
    /// in order - what the inbound `Record` tests assert against.
    external_requests: Arc<Mutex<Vec<Value>>>,
    /// In order - what the inbound `Trigger` tests assert against.
    trigger_requests: Arc<Mutex<Vec<TriggerRequest>>>,
}

async fn get_events_consume(
    State(state): State<MockSkiljState>,
    headers: HeaderMap,
) -> Json<Value> {
    let token = bearer_token(&headers);
    let events: Vec<Value> = state
        .queues
        .lock()
        .unwrap()
        .get_mut(&token)
        .map(|q| q.drain(..).collect())
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
    Json(body): Json<Value>,
) -> StatusCode {
    state
        .acked
        .lock()
        .unwrap()
        .push(body["sequence"].as_i64().unwrap());
    StatusCode::OK
}

async fn post_events_external(
    State(state): State<MockSkiljState>,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state.external_requests.lock().unwrap().push(body);
    (
        StatusCode::CREATED,
        Json(json!({ "sequence": 1, "redelivered": false })),
    )
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
    events: impl Into<VecDeque<Value>>,
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
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// `consumer.recv()`, tolerating the transient `UnknownTopicOrPartition`
/// a subscription made *before* auto-topic-creation has finished
/// propagating can legitimately surface once or twice - a real consume
/// loop (`run_inbound`) would log an error like this and simply call
/// `recv()` again, not treat it as fatal, so this test helper does the
/// same rather than failing on the first transient hiccup within the
/// overall timeout.
async fn recv_within(
    consumer: &StreamConsumer,
    timeout: Duration,
) -> rdkafka::message::BorrowedMessage<'_> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            remaining > Duration::ZERO,
            "no message received within {timeout:?}"
        );
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Ok(msg)) => return msg,
            Ok(Err(e)) => eprintln!("transient consume error, retrying: {e}"),
            Err(_) => panic!("no message received within {timeout:?}"),
        }
    }
}

/// Explicitly creates `topic` (one partition, no replication - a
/// single-broker test container) and waits for the create to actually
/// land, rather than relying on the broker's own auto-topic-creation on
/// first produce/subscribe - auto-creation's own metadata-propagation
/// delay is real and variable (observed directly: a `produce_once`
/// call using the library's own recommended timeout hit
/// `MessageTimedOut` against a topic that had never been created before,
/// on a freshly-started container), and a real deployment would
/// pre-provision its own topics rather than lean on auto-creation
/// anyway - proactively creating it here is more realistic, not just
/// more convenient for the test.
async fn create_topic(bootstrap_servers: &str, topic: &str) {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .unwrap();
    let new_topic = NewTopic::new(topic, 1, TopicReplication::Fixed(1));
    // The library's own default admin timeout (~5s) is tight when
    // several of this file's own tests each start their own separate
    // container concurrently (`cargo test`'s default parallelism) in a
    // resource-constrained environment - a longer one here is cheap and
    // makes this real, worth doing regardless of how much of the
    // observed flakiness it alone accounts for.
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
    admin.create_topics([&new_topic], &opts).await.unwrap();
}

fn unique_topic(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// The outbound scenario: a skilj `OrderPlaced` event, tagged with a
/// real `order` DCB tag, gets produced to a real Kafka topic with that
/// tag's own value as the message key - proving `produce_once` actually
/// talks to Kafka correctly, not just that it builds the right internal
/// `FutureRecord` value. Consumed back with a real `StreamConsumer` to
/// verify the payload and key round-trip exactly, and the mock skilj
/// server's own `/consume/ack` was called with the right sequence only
/// *after* the produce succeeded.
#[test]
fn an_order_placed_event_is_produced_to_kafka_with_its_own_tag_as_the_key() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders");
        create_topic(bootstrap_servers, &topic).await;

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
            })],
        );

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();

        let mapping = OutboundMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token,
            topic: topic.clone(),
            key_tag_key: Some("order".to_string()),
        };
        let http = reqwest::Client::new();
        let served = produce_once(&http, &skilj_base_url, &producer, &mapping)
            .await
            .unwrap();
        assert_eq!(served, 1);

        let msg = recv_within(&consumer, Duration::from_secs(15)).await;
        assert_eq!(msg.key(), Some("o-42".as_bytes()));
        let payload: Value =
            serde_json::from_slice(msg.payload().expect("message must have a payload")).unwrap();
        assert_eq!(payload, json!({ "orderId": "o-42" }));

        assert_eq!(
            mock_state.acked.lock().unwrap().as_slice(),
            &[7],
            "skilj must have been acked for sequence 7 after the real Kafka produce succeeded"
        );
    });
}

/// The inbound `Record` scenario: a real message produced to a real
/// Kafka topic, consumed back with a real `StreamConsumer`, and
/// dispatched through `dispatch_inbound_message` exactly as `run_inbound`
/// would - proving the `dedupe` partition key/sequence sent to skilj are
/// genuinely derived from this message's own real topic/partition/offset,
/// not synthesised.
#[test]
fn an_inbound_record_message_carries_its_own_real_partition_and_offset_as_dedupe() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-in");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-inbound")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();

        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(r#"{"orderId":"o-1"}"#)
                    .key("k"),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();

        let msg = recv_within(&consumer, Duration::from_secs(15)).await;

        let mapping = InboundMapping {
            credential: "external-token".to_string(),
            action: InboundAction::Record {
                event_type: "OrderPlaced".to_string(),
            },
        };
        let http = reqwest::Client::new();
        dispatch_inbound_message(
            &http,
            &skilj_base_url,
            &mapping,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.payload().unwrap(),
        )
        .await
        .unwrap();

        let requests = mock_state.external_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["payload"], json!({ "orderId": "o-1" }));
        assert_eq!(
            requests[0]["dedupe"]["partitionKey"],
            json!(format!("{topic}:{}", msg.partition()))
        );
        assert_eq!(requests[0]["dedupe"]["sequence"], json!(msg.offset()));
    });
}

/// The inbound `Trigger` scenario: the same real message, this time
/// dispatched as a command trigger - proving `Idempotency-Key` is
/// derived from the identical real partition/offset, in the
/// `"{topic}:{partition}:{offset}"` shape [`docs/architecture.md` §40](../../docs/architecture.md#skilj-kafka-bridge)
/// documents.
#[test]
fn an_inbound_trigger_message_derives_its_idempotency_key_from_its_own_real_offset() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("withdrawals-in");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-trigger")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();

        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(r#"{"amount":20}"#)
                    .key("k"),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();

        let msg = recv_within(&consumer, Duration::from_secs(15)).await;
        let expected_key = format!("{topic}:{}:{}", msg.partition(), msg.offset());

        let mapping = InboundMapping {
            credential: "command-token".to_string(),
            action: InboundAction::Trigger {
                command_type: "WithdrawMoney".to_string(),
            },
        };
        let http = reqwest::Client::new();
        dispatch_inbound_message(
            &http,
            &skilj_base_url,
            &mapping,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.payload().unwrap(),
        )
        .await
        .unwrap();

        let requests = mock_state.trigger_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0["payload"], json!({ "amount": 20 }));
        assert_eq!(requests[0].1.as_deref(), Some(expected_key.as_str()));
    });
}
