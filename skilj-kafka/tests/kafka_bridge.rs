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
    dispatch_inbound_message, header_str, produce_once, run_inbound, run_inbound_until,
    run_outbound_until, InboundAction, InboundMapping, OutboundMapping,
};
use std::collections::{HashMap, VecDeque};
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
    // itself is, which is the whole test binary's lifetime. A static is
    // never dropped, so the container is removed by the watchdog
    // `provision_kafka` registers, not by this handle.
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
        Ok(node) => {
            skilj_test_support::remove_container_on_exit(node.id());
            node
        }
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
    /// The highest sequence acknowledged so far per bearer credential -
    /// a lower one is refused with 409, the real server's own
    /// `acknowledgement_regresses` (a read cursor never moves backwards).
    ack_cursors: Arc<Mutex<HashMap<String, i64>>>,
    /// Every `POST /v1/events/external` body this mock ever received,
    /// in order - what the inbound `Record` tests assert against.
    external_requests: Arc<Mutex<Vec<Value>>>,
    /// In order - what the inbound `Trigger` tests assert against.
    trigger_requests: Arc<Mutex<Vec<TriggerRequest>>>,
    /// Codeberg issue #21 - `POST /v1/events/external` returns a 500
    /// while this is `> 0`, decrementing it each time - the deterministic
    /// "the target keeps failing" trigger `an_inbound_message_parks_after_exhausting_retries`
    /// needs.
    fail_external_requests: Arc<Mutex<usize>>,
    /// Same idea, for `POST /v1/events/consume/ack` -
    /// `an_outbound_event_is_skipped_after_exhausting_a_configured_retry_cap`'s
    /// own deterministic trigger.
    fail_acks: Arc<Mutex<usize>>,
    /// Same idea, for `POST /v1/commands/trigger`; the `Idempotency-Key`
    /// each refused attempt carried goes into `failed_trigger_keys`.
    fail_trigger_requests: Arc<Mutex<usize>>,
    failed_trigger_keys: Arc<Mutex<Vec<Option<String>>>>,
    /// `POST /v1/events/external` returns a 500 for a payload whose
    /// `orderId` is in here, for as long as it is - a message that keeps
    /// failing while others succeed.
    fail_order_ids: Arc<Mutex<std::collections::HashSet<String>>>,
    /// How many requests `fail_order_ids` refused.
    refused_order_requests: Arc<Mutex<usize>>,
    /// Every `POST /v1/parked-deliveries` body this mock ever received.
    parked_deliveries: Arc<Mutex<Vec<Value>>>,
    /// Same idea again, for `POST /v1/parked-deliveries` itself - the
    /// report of a message that exhausted its retries also failing.
    fail_parked_reports: Arc<Mutex<usize>>,
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
    // actually succeeds - the same "only ack advances the cursor"
    // property the real server has.
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
    let token = bearer_token(&headers);
    {
        let mut cursors = state.ack_cursors.lock().unwrap();
        if cursors.get(&token).is_some_and(|&cursor| sequence < cursor) {
            return StatusCode::CONFLICT;
        }
        cursors.insert(token.clone(), sequence);
    }
    state.acked.lock().unwrap().push(sequence);
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
    if body["payload"]["orderId"]
        .as_str()
        .is_some_and(|id| state.fail_order_ids.lock().unwrap().contains(id))
    {
        *state.refused_order_requests.lock().unwrap() += 1;
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(Value::Null));
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
    {
        let mut remaining = state.fail_parked_reports.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({})));
        }
    }
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
    {
        let mut remaining = state.fail_trigger_requests.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            state
                .failed_trigger_keys
                .lock()
                .unwrap()
                .push(idempotency_key);
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(Value::Null));
        }
    }
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
        .route("/v1/parked-deliveries", post(post_parked_deliveries))
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
    create_topic_with_partitions(bootstrap_servers, topic, 1).await;
}

/// [`create_topic`] with `partitions` partitions.
async fn create_topic_with_partitions(bootstrap_servers: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .unwrap();
    let new_topic = NewTopic::new(topic, partitions, TopicReplication::Fixed(1));
    // The library's own default admin timeout (~5s) is tight when
    // several of this file's own tests each start their own separate
    // container concurrently (`cargo test`'s default parallelism) in a
    // resource-constrained environment - a longer one here is cheap and
    // makes this real, worth doing regardless of how much of the
    // observed flakiness it alone accounts for.
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(30)));
    // A freshly started container accepts connections before its broker
    // can serve admin requests, and under a full-workspace test run that
    // gap can outlast one timeout - every test's first create then failed
    // together with `OperationTimedOut`. Retry with backoff; a timed-out
    // attempt may still have been applied, so "already exists" is success.
    let mut backoff = Duration::from_millis(500);
    for attempt in 1..=6 {
        let outcome = match admin.create_topics([&new_topic], &opts).await {
            Ok(results) => match results.into_iter().next() {
                Some(Ok(_)) => return,
                Some(Err((_, rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists))) => return,
                Some(Err((_, code))) => format!("{code:?}"),
                None => "no result for the topic".to_string(),
            },
            Err(e) => e.to_string(),
        };
        assert!(
            attempt < 6,
            "creating topic {topic} failed after {attempt} attempts: {outcome}"
        );
        eprintln!("creating topic {topic} failed (attempt {attempt}), retrying: {outcome}");
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(8));
    }
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
                "metadata": { "correlationId": "corr-42", "causationId": null },
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
            partition: None,
        };
        let http = skilj_kafka::http_client();
        let retry_policy = skilj_retry::RetryPolicy::default();
        let mut retry_state = None;
        let served = produce_once(
            &http,
            &skilj_base_url,
            &producer,
            &mapping,
            &retry_policy,
            &mut retry_state,
        )
        .await
        .unwrap();
        assert_eq!(served, 1);

        let msg = recv_within(&consumer, Duration::from_secs(15)).await;
        assert_eq!(msg.key(), Some("o-42".as_bytes()));
        let payload: Value =
            serde_json::from_slice(msg.payload().expect("message must have a payload")).unwrap();
        assert_eq!(payload, json!({ "orderId": "o-42" }));
        // Codeberg issue #18 - the event's own correlation_id round-trips
        // as a real Kafka header, distinct from `key` above (which is
        // derived from the unrelated `order` DCB tag). No causation_id
        // header at all - `causationId: null` on the consumed event
        // means nothing is sent, not an empty-string header.
        assert_eq!(
            header_str(msg.headers(), "Skilj-Correlation-Id"),
            Some("corr-42")
        );
        assert_eq!(header_str(msg.headers(), "Skilj-Causation-Id"), None);

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
        let http = skilj_kafka::http_client();
        dispatch_inbound_message(
            &http,
            &skilj_base_url,
            &mapping,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.payload().unwrap(),
            None,
            None,
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

/// Codeberg issue #18 - the inbound half of the same round trip the
/// outbound test above already proves: a real Kafka message carrying
/// `Skilj-Correlation-Id`/`Skilj-Causation-Id` headers gets them
/// extracted (`header_str`) and forwarded as `correlationId`/`causationId`
/// on the `POST /v1/events/external` body `dispatch_inbound_message`
/// sends - not left for skilj to generate a fresh one, which is what
/// would happen if this bridge silently dropped them.
#[test]
fn an_inbound_record_message_forwards_its_own_correlation_and_causation_headers() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-in-correlated");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-inbound-correlated")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();

        let headers = rdkafka::message::OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: "Skilj-Correlation-Id",
                value: Some("upstream-corr-1"),
            })
            .insert(rdkafka::message::Header {
                key: "Skilj-Causation-Id",
                value: Some("upstream-cause-1"),
            });
        producer
            .send(
                FutureRecord::to(&topic)
                    .payload(r#"{"orderId":"o-2"}"#)
                    .key("k")
                    .headers(headers),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();

        let msg = recv_within(&consumer, Duration::from_secs(15)).await;
        let correlation_id = header_str(msg.headers(), "Skilj-Correlation-Id");
        let causation_id = header_str(msg.headers(), "Skilj-Causation-Id");

        let mapping = InboundMapping {
            credential: "external-token".to_string(),
            action: InboundAction::Record {
                event_type: "OrderPlaced".to_string(),
            },
        };
        let http = skilj_kafka::http_client();
        dispatch_inbound_message(
            &http,
            &skilj_base_url,
            &mapping,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.payload().unwrap(),
            correlation_id,
            causation_id,
        )
        .await
        .unwrap();

        let requests = mock_state.external_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["correlationId"], json!("upstream-corr-1"));
        assert_eq!(requests[0]["causationId"], json!("upstream-cause-1"));
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
        let http = skilj_kafka::http_client();
        dispatch_inbound_message(
            &http,
            &skilj_base_url,
            &mapping,
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.payload().unwrap(),
            None,
            None,
        )
        .await
        .unwrap();

        let requests = mock_state.trigger_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0["payload"], json!({ "amount": 20 }));
        assert_eq!(requests[0].1.as_deref(), Some(expected_key.as_str()));
    });
}

/// Codeberg issue #21 - a message that keeps failing to dispatch is
/// retried with backoff (the mock's own `fail_external_requests` toggle
/// makes `POST /v1/events/external` fail exactly twice), then reported
/// to skilj's own `POST /v1/parked-deliveries` and the offset committed
/// anyway once `retry_policy` (`max_attempts: 2`) exhausts - `run_inbound`
/// itself, not `dispatch_inbound_message` directly, since the retry loop
/// lives there.
#[test]
fn an_inbound_message_parks_and_reports_after_exhausting_retries() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-in-parking");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        *mock_state.fail_external_requests.lock().unwrap() = 2;
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-parking")
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
                    .payload(r#"{"orderId":"o-parked"}"#)
                    .key("k"),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();

        let mut mappings = HashMap::new();
        mappings.insert(
            topic.clone(),
            InboundMapping {
                credential: "external-token".to_string(),
                action: InboundAction::Record {
                    event_type: "OrderPlaced".to_string(),
                },
            },
        );

        let http = skilj_kafka::http_client();
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(10),
            1.0,
            Duration::from_millis(10),
            2,
        );
        tokio::spawn(async move {
            run_inbound(&consumer, &http, &skilj_base_url, &mappings, &retry_policy).await;
        });

        // 20s budget, not 5s - a brand-new consumer group's first
        // `recv()` blocks on a real partition-assignment rebalance
        // first, the identical latency `recv_within`'s own 15s timeout
        // elsewhere in this file already exists to tolerate; this test
        // waits on that *plus* two full retry-policy attempts on top.
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
        assert_eq!(parked["source"], json!("kafka-inbound"));
        assert_eq!(parked["kind"], json!("external_event"));
        assert_eq!(parked["attemptCount"], json!(2));
        assert_eq!(
            parked["request"]["payload"],
            json!({ "orderId": "o-parked" })
        );
        assert_eq!(
            mock_state.external_requests.lock().unwrap().len(),
            0,
            "every attempt failed, so a real event must never have been created"
        );
    });
}

/// The `Trigger` counterpart: the parked report carries the
/// `Idempotency-Key` the failing attempts were sent with, so
/// `retryParkedDelivery` can redrive under it and dedupe against an
/// attempt that committed without the bridge hearing back.
#[test]
fn a_parked_trigger_message_reports_the_idempotency_key_it_was_sent_with() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("trigger-in-parking");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        *mock_state.fail_trigger_requests.lock().unwrap() = 2;
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-trigger-parking")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();
        producer
            .send(
                FutureRecord::to(&topic).payload(r#"{"amount":5}"#).key("k"),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();

        let mut mappings = HashMap::new();
        mappings.insert(
            topic.clone(),
            InboundMapping {
                credential: "command-token".to_string(),
                action: InboundAction::Trigger {
                    command_type: "Deposit".to_string(),
                },
            },
        );
        let http = skilj_kafka::http_client();
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(10),
            1.0,
            Duration::from_millis(10),
            2,
        );
        tokio::spawn(async move {
            run_inbound(&consumer, &http, &skilj_base_url, &mappings, &retry_policy).await;
        });

        // See `an_inbound_message_parks_and_reports_after_exhausting_retries`
        // for the 20s budget.
        let mut parked = None;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Some(p) = mock_state.parked_deliveries.lock().unwrap().first() {
                parked = Some(p.clone());
                break;
            }
        }
        let parked =
            parked.expect("run_inbound must have reported a parked delivery within the timeout");
        assert_eq!(parked["kind"], json!("command_trigger"));
        let sent = mock_state.failed_trigger_keys.lock().unwrap().clone();
        assert_eq!(sent.len(), 2);
        let sent_key = sent[0]
            .clone()
            .expect("a Trigger request always carries a key");
        assert_eq!(sent[1].as_deref(), Some(sent_key.as_str()));
        assert!(sent_key.starts_with(&format!("{topic}:")), "{sent_key}");
        assert_eq!(parked["idempotencyKey"], json!(sent_key));
        assert!(parked["request"].get("idempotencyKey").is_none());
    });
}

/// Codeberg issue #21 - the outbound direction's own shape: unlike
/// inbound, exhausting a *configured* `retry_policy` (default is
/// `RetryPolicy::unbounded` - this test opts into a bounded one
/// specifically to exercise the cap) skips the event instead of parking
/// it - acknowledged to skilj without ever successfully producing it, no
/// `POST /v1/parked-deliveries` call at all. `fail_acks` (not a produce
/// failure) is this test's own deterministic trigger - see this
/// function's own note on why that still exercises the real skip path
/// even though the event *is* re-produced to Kafka on each retry (an
/// existing, pre-issue-#21 property of retrying a combined produce+ack
/// step as one unit, not something this pass changes).
#[test]
fn an_outbound_event_is_skipped_after_exhausting_a_configured_retry_cap() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-out-skip");
        create_topic(bootstrap_servers, &topic).await;

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

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();

        let mapping = OutboundMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token,
            topic: topic.clone(),
            key_tag_key: Some("order".to_string()),
            partition: None,
        };
        let http = skilj_kafka::http_client();
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(10),
            1.0,
            Duration::from_millis(10),
            2,
        );
        let mut retry_state = None;

        // Cycle 1: produce succeeds, ack fails (1/2 of the fail budget) -
        // not yet exhausted, so this cycle stops here without skipping.
        let served = produce_once(
            &http,
            &skilj_base_url,
            &producer,
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

        // Cycle 2: backoff has elapsed - produce succeeds again, ack
        // fails again (2/2), which exhausts `max_attempts: 2`. The event
        // is skipped: acknowledged directly, without a 3rd produce.
        let served = produce_once(
            &http,
            &skilj_base_url,
            &producer,
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

/// Codeberg issue #25's investigation (docs/architecture.md §54) - real
/// partitioning proof against a real Kafka broker: two `OutboundMapping`s
/// for the same `EventType`, different `credential`s (simulating two
/// independent `EventReadToken`s, each seeing the identical stream - the
/// mock server's own `enqueue` keys by token), same `partition_count`,
/// different `partition_index`. Each gets a single `produce_once` call
/// over its own full six-event queue. The union of what actually lands
/// in Kafka across both must be exactly those six keys, no duplicates
/// (double-publish) and no gaps (lost delivery) - and every one of the
/// six sequences must be acknowledged on *both* tokens regardless of
/// which one actually produced it, proving a partition-skip still
/// advances that instance's own cursor rather than leaving it stuck.
#[test]
fn two_partitioned_mappings_together_produce_every_key_exactly_once() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-partitioned");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let order_ids = ["o-1", "o-2", "o-3", "o-4", "o-5", "o-6"];
        let events: Vec<Value> = order_ids
            .iter()
            .enumerate()
            .map(|(i, order_id)| {
                json!({
                    "sequence": i as i64,
                    "eventType": "OrderPlaced",
                    "payload": { "orderId": order_id },
                    "tags": [{ "key": "order", "value": order_id }],
                    "metadata": { "correlationId": null, "causationId": null },
                })
            })
            .collect();

        let token_0 = "read-token-partition-0".to_string();
        let token_1 = "read-token-partition-1".to_string();
        enqueue(&mock_state, &token_0, "OrderPlaced", events.clone());
        enqueue(&mock_state, &token_1, "OrderPlaced", events);

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

        let http = skilj_kafka::http_client();
        let retry_policy = skilj_retry::RetryPolicy::default();

        for (token, partition_index) in [(token_0.clone(), 0u32), (token_1.clone(), 1u32)] {
            let mapping = OutboundMapping {
                event_type: "OrderPlaced".to_string(),
                credential: token,
                topic: topic.clone(),
                key_tag_key: Some("order".to_string()),
                partition: Some((partition_index, 2)),
            };
            let mut retry_state = None;
            produce_once(
                &http,
                &skilj_base_url,
                &producer,
                &mapping,
                &retry_policy,
                &mut retry_state,
            )
            .await
            .unwrap();
        }

        let mut received_keys = std::collections::HashSet::new();
        for _ in 0..order_ids.len() {
            let msg = recv_within(&consumer, Duration::from_secs(15)).await;
            let key =
                String::from_utf8(msg.key().expect("message must have a key").to_vec()).unwrap();
            assert!(
                received_keys.insert(key.clone()),
                "key {key} was produced more than once - double-publish across partitions"
            );
        }
        assert_eq!(
            received_keys,
            order_ids.iter().map(|s| s.to_string()).collect(),
            "every key must be produced exactly once across both partitions"
        );

        // Every sequence acknowledged on *both* tokens - a partition-skip
        // still advances this instance's own cursor, it just never
        // produces to Kafka.
        // Codeberg issue #40: each instance acknowledges its whole page -
        // owned events and partition skips alike - with one call for the
        // last sequence, which moves its cursor past every one of them.
        let last = order_ids.len() as i64 - 1;
        assert_eq!(
            mock_state.acked.lock().unwrap().as_slice(),
            &[last, last],
            "each instance's cursor must pass every sequence, owned by its partition or not"
        );
    });
}

/// Codeberg issue #39: a partitioned mapping must never acknowledge an
/// event this instance doesn't own *past* an owned one it hasn't
/// produced yet. The read cursor is a single position, so that ack
/// would move it beyond the owned event - and if producing that event
/// then fails, the next `consume` starts after it and it is never
/// fetched again. Here the owned event's own payload is larger than the
/// producer's `message.max.bytes`, a deterministic produce failure, and
/// the not-owned event behind it must stay unacknowledged.
#[test]
fn a_failed_owned_event_is_never_acknowledged_past_by_a_partition_skip() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-partition-fail");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let partition = Some((0u32, 2u32));
        let candidates: Vec<String> = (0..64).map(|i| format!("o-{i}")).collect();
        let owned_key = candidates
            .iter()
            .find(|k| skilj_bridge::owns_partition(partition, Some(k)))
            .unwrap();
        let other_key = candidates
            .iter()
            .find(|k| !skilj_bridge::owns_partition(partition, Some(k)))
            .unwrap();
        let event = |sequence: i64, order_id: &str, payload: Value| {
            json!({
                "sequence": sequence,
                "eventType": "OrderPlaced",
                "payload": payload,
                "tags": [{ "key": "order", "value": order_id }],
                "metadata": { "correlationId": null, "causationId": null },
            })
        };
        let token = "read-token-partition-fail".to_string();
        enqueue(
            &mock_state,
            &token,
            "OrderPlaced",
            vec![
                event(1, owned_key, json!({ "padding": "x".repeat(10_000) })),
                event(2, other_key, json!({ "orderId": other_key })),
            ],
        );

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .set("message.max.bytes", "2000")
            .create()
            .unwrap();
        let mapping = OutboundMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token,
            topic,
            key_tag_key: Some("order".to_string()),
            partition,
        };
        let http = skilj_kafka::http_client();
        let retry_policy = skilj_retry::RetryPolicy::default();
        let mut retry_state = None;

        let served = produce_once(
            &http,
            &skilj_base_url,
            &producer,
            &mapping,
            &retry_policy,
            &mut retry_state,
        )
        .await
        .unwrap();

        assert_eq!(served, 0);
        assert!(
            retry_state.is_some(),
            "the failed produce must be retried, not skipped"
        );
        assert!(
            mock_state.acked.lock().unwrap().is_empty(),
            "nothing may be acknowledged past the owned event that failed to produce"
        );
    });
}

/// docs/architecture.md §97: Kafka offsets are cumulative - committing a
/// later message's offset commits every earlier one in its partition. So
/// when a message exhausts its retries and its park report fails too
/// (skilj still unreachable), the bridge must not move on: the next
/// message succeeding would commit past it, and it would be neither
/// processed nor parked - lost. Here the first message fails, its report
/// fails twice, and the second message would succeed.
#[test]
fn a_message_whose_park_report_fails_is_never_committed_past() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-in-report-fails");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        *mock_state.fail_external_requests.lock().unwrap() = 2;
        *mock_state.fail_parked_reports.lock().unwrap() = 2;
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        for order in ["o-first", "o-second"] {
            producer
                .send(
                    FutureRecord::to(&topic)
                        .payload(&format!(r#"{{"orderId":"{order}"}}"#))
                        .key("k"),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)
                .unwrap();
        }
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-report-fails")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();

        let mut mappings = HashMap::new();
        mappings.insert(
            topic.clone(),
            InboundMapping {
                credential: "external-token".to_string(),
                action: InboundAction::Record {
                    event_type: "OrderPlaced".to_string(),
                },
            },
        );
        let http = skilj_kafka::http_client();
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(10),
            1.0,
            Duration::from_millis(10),
            2,
        );
        tokio::spawn(async move {
            run_inbound(&consumer, &http, &skilj_base_url, &mappings, &retry_policy).await;
        });

        // Wait until the second message has been delivered.
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if !mock_state.external_requests.lock().unwrap().is_empty() {
                break;
            }
        }
        let delivered: Vec<Value> = mock_state.external_requests.lock().unwrap().clone();
        assert_eq!(delivered.len(), 1, "the second message was never delivered");
        assert_eq!(delivered[0]["payload"], json!({ "orderId": "o-second" }));

        // By then the first must have been parked - reported once the
        // report endpoint recovered, before the bridge moved on.
        let parked: Vec<Value> = mock_state.parked_deliveries.lock().unwrap().clone();
        assert_eq!(
            parked.len(),
            1,
            "the first message was skipped without being parked"
        );
        assert_eq!(
            parked[0]["request"]["payload"],
            json!({ "orderId": "o-first" })
        );
    });
}

/// docs/architecture.md §174 (Codeberg issue #60): partitions are
/// dispatched concurrently, one message in flight each. A message that
/// keeps failing holds up only the messages behind it in its own
/// partition: the other partition's message is delivered meanwhile, and
/// its own successor only once it has gone through.
#[test]
fn a_failing_message_holds_up_only_its_own_partition() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-in-partitions");
        create_topic_with_partitions(bootstrap_servers, &topic, 2).await;

        let mock_state = MockSkiljState::default();
        mock_state
            .fail_order_ids
            .lock()
            .unwrap()
            .insert("o-stuck".to_string());
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let produce = |order: &'static str, partition: i32| {
            let producer = producer.clone();
            let topic = topic.clone();
            async move {
                producer
                    .send(
                        FutureRecord::to(&topic)
                            .payload(&format!(r#"{{"orderId":"{order}"}}"#))
                            .key("k")
                            .partition(partition),
                        Duration::from_secs(10),
                    )
                    .await
                    .map_err(|(e, _)| e)
                    .unwrap();
            }
        };
        produce("o-stuck", 0).await;
        produce("o-behind", 0).await;
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-partitions")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();

        let mut mappings = HashMap::new();
        mappings.insert(
            topic.clone(),
            InboundMapping {
                credential: "external-token".to_string(),
                action: InboundAction::Record {
                    event_type: "OrderPlaced".to_string(),
                },
            },
        );
        let http = skilj_kafka::http_client();
        // Never parks within this test.
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(20),
            1.0,
            Duration::from_millis(20),
            100_000,
        );
        tokio::spawn(async move {
            run_inbound(&consumer, &http, &skilj_base_url, &mappings, &retry_policy).await;
        });

        // Only once the stuck message is failing does the other partition
        // get a message - so it can't have been received first.
        // See `an_inbound_message_parks_and_reports_after_exhausting_retries`
        // for the 20s budget.
        for _ in 0..200 {
            if *mock_state.refused_order_requests.lock().unwrap() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(*mock_state.refused_order_requests.lock().unwrap() > 0);
        produce("o-free", 1).await;

        let delivered = |state: &MockSkiljState| -> Vec<String> {
            state
                .external_requests
                .lock()
                .unwrap()
                .iter()
                .map(|r| r["payload"]["orderId"].as_str().unwrap().to_string())
                .collect()
        };
        for _ in 0..100 {
            if !delivered(&mock_state).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(delivered(&mock_state), ["o-free"]);
        // Give partition 0 time to run ahead if it wrongly would.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(delivered(&mock_state), ["o-free"]);

        mock_state.fail_order_ids.lock().unwrap().clear();
        for _ in 0..100 {
            if delivered(&mock_state).len() == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(delivered(&mock_state), ["o-free", "o-stuck", "o-behind"]);
        assert!(mock_state.parked_deliveries.lock().unwrap().is_empty());
    });
}

/// docs/architecture.md §174: a partition with more than
/// `INBOUND_PARTITION_BUFFER` messages waiting behind a failing one is
/// paused, and resumed once they drain. Every message still reaches skilj
/// exactly once, in offset order - pausing drops what librdkafka had
/// prefetched for the partition, and resuming must fetch again from the
/// first message not yet received, neither skipping nor repeating any.
#[test]
fn a_paused_partition_resumes_without_skipping_or_repeating_messages() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-in-paused");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        mock_state
            .fail_order_ids
            .lock()
            .unwrap()
            .insert("o-0".to_string());
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let count = 3 * skilj_kafka::INBOUND_PARTITION_BUFFER;
        let expected: Vec<String> = (0..count).map(|i| format!("o-{i}")).collect();
        for order in &expected {
            producer
                .send(
                    FutureRecord::to(&topic)
                        .payload(&format!(r#"{{"orderId":"{order}"}}"#))
                        .key("k"),
                    Duration::from_secs(10),
                )
                .await
                .map_err(|(e, _)| e)
                .unwrap();
        }
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-paused")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();

        let mut mappings = HashMap::new();
        mappings.insert(
            topic.clone(),
            InboundMapping {
                credential: "external-token".to_string(),
                action: InboundAction::Record {
                    event_type: "OrderPlaced".to_string(),
                },
            },
        );
        let http = skilj_kafka::http_client();
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_millis(20),
            1.0,
            Duration::from_millis(20),
            100_000,
        );
        tokio::spawn(async move {
            run_inbound(&consumer, &http, &skilj_base_url, &mappings, &retry_policy).await;
        });

        for _ in 0..200 {
            if *mock_state.refused_order_requests.lock().unwrap() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(*mock_state.refused_order_requests.lock().unwrap() > 0);
        // Long enough for the rest to be fetched and the partition paused.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(mock_state.external_requests.lock().unwrap().is_empty());

        mock_state.fail_order_ids.lock().unwrap().clear();
        let delivered = |state: &MockSkiljState| -> Vec<String> {
            state
                .external_requests
                .lock()
                .unwrap()
                .iter()
                .map(|r| r["payload"]["orderId"].as_str().unwrap().to_string())
                .collect()
        };
        for _ in 0..200 {
            if delivered(&mock_state).len() >= count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Anything repeated would have arrived by now as well.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(delivered(&mock_state), expected);
    });
}

/// `run_outbound_until`/`run_inbound_until` return once asked
/// (docs/architecture.md §129): the outbound loop produces and acks the
/// event it was served, then - idle, with a 60 s poll interval ahead of it
/// - stops at once; the inbound loop, waiting for a message, stops too.
#[test]
fn the_run_loops_stop_when_asked() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-stop");
        create_topic(bootstrap_servers, &topic).await;
        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;
        enqueue(
            &mock_state,
            "read-token",
            "OrderPlaced",
            [json!({
                "sequence": 9,
                "eventType": "OrderPlaced",
                "payload": { "orderId": "o-9" },
                "tags": [],
                "metadata": { "correlationId": null, "causationId": null },
            })],
        );
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let mappings = vec![OutboundMapping {
            event_type: "OrderPlaced".to_string(),
            credential: "read-token".to_string(),
            topic: topic.clone(),
            key_tag_key: None,
            partition: None,
        }];
        let (stop_outbound, stopped) = tokio::sync::oneshot::channel::<()>();
        let outbound = tokio::spawn({
            let skilj_base_url = skilj_base_url.clone();
            async move {
                run_outbound_until(
                    &skilj_base_url,
                    &producer,
                    &mappings,
                    Duration::from_secs(60),
                    &skilj_retry::RetryPolicy::default(),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await;
            }
        });
        for _ in 0..150 {
            if !mock_state.acked.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(mock_state.acked.lock().unwrap().as_slice(), &[9]);
        stop_outbound.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), outbound)
            .await
            .expect("run_outbound_until must return once asked")
            .unwrap();

        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-stop")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();
        let (stop_inbound, stopped) = tokio::sync::oneshot::channel::<()>();
        let inbound = tokio::spawn(async move {
            run_inbound_until(
                &consumer,
                &skilj_kafka::http_client(),
                &skilj_base_url,
                &HashMap::new(),
                &skilj_retry::RetryPolicy::default(),
                async {
                    let _ = stopped.await;
                },
            )
            .await;
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        stop_inbound.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), inbound)
            .await
            .expect("run_inbound_until must return once asked")
            .unwrap();
    });
}

/// docs/architecture.md §144: a message that isn't JSON is parked on its
/// first failure - no retry can make it JSON - and the parked request
/// keeps its raw content, not a `null`.
#[test]
fn a_non_json_message_is_parked_at_once_with_its_raw_content() {
    runtime().block_on(async {
        let Some(bootstrap_servers) = test_kafka().await else {
            return;
        };
        let topic = unique_topic("orders-in-malformed");
        create_topic(bootstrap_servers, &topic).await;

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("message.timeout.ms", "10000")
            .create()
            .unwrap();
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test-group-malformed")
            .set("bootstrap.servers", bootstrap_servers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[topic.as_str()]).unwrap();

        producer
            .send(
                FutureRecord::to(&topic).payload("not json {").key("k"),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .unwrap();

        let mut mappings = HashMap::new();
        mappings.insert(
            topic.clone(),
            InboundMapping {
                credential: "external-token".to_string(),
                action: InboundAction::Record {
                    event_type: "OrderPlaced".to_string(),
                },
            },
        );

        let http = skilj_kafka::http_client();
        // Retried, this would take 20 s and five attempts to park.
        let retry_policy = skilj_retry::RetryPolicy::bounded(
            Duration::from_secs(5),
            1.0,
            Duration::from_secs(5),
            5,
        );
        tokio::spawn(async move {
            run_inbound(&consumer, &http, &skilj_base_url, &mappings, &retry_policy).await;
        });

        let mut parked = None;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Some(p) = mock_state.parked_deliveries.lock().unwrap().first() {
                parked = Some(p.clone());
                break;
            }
        }
        let parked = parked.expect("run_inbound must have parked the malformed message");
        assert_eq!(parked["attemptCount"], json!(1));
        assert_eq!(parked["request"]["payload"], json!("not json {"));
        assert!(mock_state.external_requests.lock().unwrap().is_empty());
    });
}
