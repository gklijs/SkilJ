//! End-to-end proof of `skilj_amqp::produce_once`/`dispatch_inbound_message`
//! against a real, ephemeral AMQP 1.0 broker (Apache ActiveMQ Artemis,
//! `apache/artemis:latest-alpine` - no dedicated `testcontainers-modules`
//! feature exists for any AMQP broker, so a plain `GenericImage` is
//! used directly), neither mocked at the protocol level it matters for.
//! The skilj *side* is a small local mock server, the identical "wire
//! protocol only, no real skilj crate" shape
//! `skilj-kafka/tests/kafka_bridge.rs`/`skilj-temporal/tests/temporal_bridge.rs`
//! already use - skilj's own wire contracts are each already
//! exhaustively tested elsewhere; this crate's own job is proving it
//! calls them correctly with real AMQP 1.0 deliveries on the other end.
//!
//! One shared, ephemeral broker for the whole file (`OnceCell`, the
//! identical shape `skilj-kafka/tests/kafka_bridge.rs`'s own `TEST_KAFKA`
//! already settled on after a first draft's one-container-per-test
//! version genuinely starved this sandbox's Docker daemon under
//! `cargo test`'s default parallelism - not repeating that mistake
//! here). Artemis started with `ANONYMOUS_LOGIN=true` - no credentials
//! needed, this bridge's own authentication story is a deployment
//! concern for whoever configures a real broker URL, not something
//! these tests need to exercise.
//!
//! Needs a reachable Docker daemon - see CONTRIBUTING.md's own note on
//! the `DOCKER_HOST` quirk this can hit on WSL. Skips gracefully, the
//! same tolerance every other real-external-dependency test in this
//! workspace already has, if Docker isn't reachable at all.

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use fe2o3_amqp::types::messaging::{Data, Message, MessageId, Properties};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use serde_json::{json, Value};
use skilj_amqp::{
    dispatch_inbound_message, produce_once, InboundAction, InboundMapping, InboundMessageMeta,
    OutboundMapping,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build a tokio runtime for these tests")
    })
}

// --- provisioning: one shared, ephemeral Artemis broker for the whole
// file - see this file's own doc comment for why.

struct TestBroker {
    url: String,
    _container: ContainerAsync<GenericImage>,
}

static TEST_BROKER: tokio::sync::OnceCell<Option<TestBroker>> = tokio::sync::OnceCell::const_new();

async fn test_broker() -> Option<&'static str> {
    TEST_BROKER
        .get_or_init(provision_broker)
        .await
        .as_ref()
        .map(|b| b.url.as_str())
}

async fn provision_broker() -> Option<TestBroker> {
    let image = GenericImage::new("apache/artemis", "latest-alpine")
        .with_exposed_port(ContainerPort::Tcp(61616))
        .with_wait_for(WaitFor::message_on_stdout("AMQ221007"))
        .with_env_var("ANONYMOUS_LOGIN", "true");
    let container = match image.start().await {
        Ok(container) => container,
        Err(e) => {
            eprintln!(
                "skipping: starting the ephemeral Artemis container failed \
                 (no reachable Docker daemon, most likely - see CONTRIBUTING.md's \
                 own DOCKER_HOST note): {e}"
            );
            return None;
        }
    };
    let port = match container
        .get_host_port_ipv4(ContainerPort::Tcp(61616))
        .await
    {
        Ok(port) => port,
        Err(e) => {
            eprintln!("skipping: getting the Artemis container's own mapped port failed: {e}");
            return None;
        }
    };
    Some(TestBroker {
        url: format!("amqp://127.0.0.1:{port}"),
        _container: container,
    })
}

/// A real `Connection`/`Session` pair against the shared test broker -
/// each test opens its own, cheap enough not to need sharing the way
/// the broker container itself does. Returns *both* handles - dropping
/// `ConnectionHandle` closes the connection (it implements `Drop`
/// exactly for that), so a caller that only keeps the `SessionHandle`
/// around finds every operation on it failing with
/// `SessionStopped(ConnectionStopped(Closed))` as soon as the
/// now-dropped `Connection` tears the whole thing down - confirmed by
/// testing, not assumed: this is exactly the failure every test in this
/// file hit on a first draft that returned only the session.
async fn connect(
    url: &str,
    name: &str,
) -> (
    fe2o3_amqp::connection::ConnectionHandle<()>,
    fe2o3_amqp::session::SessionHandle<()>,
) {
    let mut connection = Connection::open(name, url)
        .await
        .expect("connecting to the ephemeral Artemis broker must succeed");
    let session = Session::begin(&mut connection)
        .await
        .expect("beginning a session must succeed");
    (connection, session)
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
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn unique_address(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// The outbound scenario: a skilj `OrderPlaced` event, tagged with a
/// real `order` DCB tag, gets sent to a real Artemis address with that
/// tag's own value as the message's own `group-id`, and its skilj
/// sequence number as `group-sequence` - proving `produce_once` actually
/// talks to a real AMQP 1.0 broker correctly. Received back with a real
/// `Receiver` to verify the payload and properties round-trip exactly,
/// and the mock skilj server's own `/consume/ack` was called only
/// *after* the send succeeded.
#[test]
fn an_order_placed_event_is_sent_with_its_own_tag_as_the_group_id() {
    runtime().block_on(async {
        let Some(url) = test_broker().await else {
            return;
        };
        let address = unique_address("orders");

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

        let (_send_conn, mut send_session) = connect(url, "sender-conn").await;
        let mut sender = Sender::attach(&mut send_session, "sender-link", address.as_str())
            .await
            .unwrap();
        let (_recv_conn, mut recv_session) = connect(url, "receiver-conn").await;
        let mut receiver = Receiver::attach(&mut recv_session, "receiver-link", address.as_str())
            .await
            .unwrap();

        let mapping = OutboundMapping {
            event_type: "OrderPlaced".to_string(),
            credential: token,
            address: address.clone(),
            key_tag_key: Some("order".to_string()),
        };
        let http = reqwest::Client::new();
        let served = produce_once(&http, &skilj_base_url, &mut sender, &mapping)
            .await
            .unwrap();
        assert_eq!(served, 1);

        let delivery = tokio::time::timeout(Duration::from_secs(15), receiver.recv::<Data>())
            .await
            .expect("must receive the sent message within 15s")
            .unwrap();
        receiver.accept(&delivery).await.unwrap();

        let properties = delivery.message().properties.as_ref().unwrap();
        assert_eq!(properties.group_id.as_deref(), Some("o-42"));
        assert_eq!(properties.group_sequence, Some(7));
        let payload: Value = serde_json::from_slice(delivery.body().0.as_ref()).unwrap();
        assert_eq!(payload, json!({ "orderId": "o-42" }));
        // Codeberg issue #18 - the event's own correlation_id round-trips
        // as AMQP 1.0's real standard correlation-id property, distinct
        // from group_id above (derived from the unrelated `order` DCB
        // tag). No causation_id application-property at all -
        // `causationId: null` on the consumed event means nothing is
        // sent, not an empty-string property.
        assert_eq!(
            properties.correlation_id,
            Some(MessageId::String("corr-42".to_string()))
        );
        assert!(delivery.message().application_properties.is_none());

        assert_eq!(
            mock_state.acked.lock().unwrap().as_slice(),
            &[7],
            "skilj must have been acked for sequence 7 after the real AMQP send succeeded"
        );
    });
}

/// The inbound `Record` scenario: a real message sent with real
/// `group-id`/`group-sequence` properties (as an upstream, non-skilj
/// sender would), received back with a real `Receiver`, and dispatched
/// through `dispatch_inbound_message` exactly as `run_inbound` would -
/// proving the `dedupe` partition key/sequence sent to skilj are
/// genuinely read from this message's own real AMQP properties, not
/// synthesised.
#[test]
fn an_inbound_record_message_carries_its_own_real_group_id_and_sequence_as_dedupe() {
    runtime().block_on(async {
        let Some(url) = test_broker().await else {
            return;
        };
        let address = unique_address("orders-in");

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let (_send_conn, mut send_session) = connect(url, "sender-conn").await;
        let mut sender = Sender::attach(&mut send_session, "sender-link", address.as_str())
            .await
            .unwrap();
        let (_recv_conn, mut recv_session) = connect(url, "receiver-conn").await;
        let mut receiver = Receiver::attach(&mut recv_session, "receiver-link", address.as_str())
            .await
            .unwrap();

        let message = Message::builder()
            .properties(
                Properties::builder()
                    .group_id(Some("partition-a".to_string()))
                    .group_sequence(Some(42u32))
                    .build(),
            )
            .data(br#"{"orderId":"o-1"}"#.to_vec())
            .build();
        sender
            .send(message)
            .await
            .unwrap()
            .accepted_or_else(|o| format!("{o:?}"))
            .unwrap();

        let delivery = tokio::time::timeout(Duration::from_secs(15), receiver.recv::<Data>())
            .await
            .expect("must receive the sent message within 15s")
            .unwrap();
        receiver.accept(&delivery).await.unwrap();

        let properties = delivery.message().properties.as_ref();
        let meta = InboundMessageMeta {
            group_id: properties.and_then(|p| p.group_id.clone()),
            group_sequence: properties.and_then(|p| p.group_sequence),
            message_id: properties.and_then(|p| p.message_id.clone()),
            correlation_id: None,
            causation_id: None,
        };

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
            &meta,
            delivery.body().0.as_ref(),
        )
        .await
        .unwrap();

        let requests = mock_state.external_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["payload"], json!({ "orderId": "o-1" }));
        assert_eq!(requests[0]["dedupe"]["partitionKey"], json!("partition-a"));
        assert_eq!(requests[0]["dedupe"]["sequence"], json!(42));
    });
}

/// Codeberg issue #18 - the inbound half of the same round trip the
/// outbound test above already proves: a real AMQP message carrying a
/// real `correlation-id` property and a `Skilj-Causation-Id`
/// application-property gets both read back (`InboundMessageMeta`) and
/// forwarded as `correlationId`/`causationId` on the `POST
/// /v1/events/external` body `dispatch_inbound_message` sends - not left
/// for skilj to generate a fresh one, which is what would happen if this
/// bridge silently dropped them.
#[test]
fn an_inbound_record_message_forwards_its_own_correlation_and_causation_properties() {
    runtime().block_on(async {
        let Some(url) = test_broker().await else {
            return;
        };
        let address = unique_address("orders-in-correlated");

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let (_send_conn, mut send_session) = connect(url, "sender-conn").await;
        let mut sender = Sender::attach(&mut send_session, "sender-link", address.as_str())
            .await
            .unwrap();
        let (_recv_conn, mut recv_session) = connect(url, "receiver-conn").await;
        let mut receiver = Receiver::attach(&mut recv_session, "receiver-link", address.as_str())
            .await
            .unwrap();

        let message = Message::builder()
            .properties(
                Properties::builder()
                    .correlation_id(MessageId::String("upstream-corr-1".to_string()))
                    .build(),
            )
            .application_properties(
                fe2o3_amqp_types::messaging::ApplicationProperties::builder()
                    .insert("Skilj-Causation-Id", "upstream-cause-1")
                    .build(),
            )
            .data(br#"{"orderId":"o-2"}"#.to_vec())
            .build();
        sender
            .send(message)
            .await
            .unwrap()
            .accepted_or_else(|o| format!("{o:?}"))
            .unwrap();

        let delivery = tokio::time::timeout(Duration::from_secs(15), receiver.recv::<Data>())
            .await
            .expect("must receive the sent message within 15s")
            .unwrap();
        receiver.accept(&delivery).await.unwrap();

        let properties = delivery.message().properties.as_ref();
        let causation_id = delivery
            .message()
            .application_properties
            .as_ref()
            .and_then(|props| props.get("Skilj-Causation-Id"))
            .and_then(|v| match v {
                fe2o3_amqp_types::primitives::SimpleValue::String(s) => Some(s.clone()),
                _ => None,
            });
        let meta = InboundMessageMeta {
            group_id: properties.and_then(|p| p.group_id.clone()),
            group_sequence: properties.and_then(|p| p.group_sequence),
            message_id: properties.and_then(|p| p.message_id.clone()),
            correlation_id: properties.and_then(|p| p.correlation_id.clone()),
            causation_id,
        };

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
            &meta,
            delivery.body().0.as_ref(),
        )
        .await
        .unwrap();

        let requests = mock_state.external_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["correlationId"], json!("upstream-corr-1"));
        assert_eq!(requests[0]["causationId"], json!("upstream-cause-1"));
    });
}

/// The inbound `Trigger` scenario: a real message sent with a real
/// `message-id` property, dispatched as a command trigger - proving
/// `Idempotency-Key` is derived from that message's own real
/// `message-id`, stringified via `skilj_amqp`'s own conversion, not a
/// synthesised value.
#[test]
fn an_inbound_trigger_message_derives_its_idempotency_key_from_its_own_real_message_id() {
    runtime().block_on(async {
        let Some(url) = test_broker().await else {
            return;
        };
        let address = unique_address("withdrawals-in");

        let mock_state = MockSkiljState::default();
        let skilj_base_url = serve_mock_skilj(mock_state.clone()).await;

        let (_send_conn, mut send_session) = connect(url, "sender-conn").await;
        let mut sender = Sender::attach(&mut send_session, "sender-link", address.as_str())
            .await
            .unwrap();
        let (_recv_conn, mut recv_session) = connect(url, "receiver-conn").await;
        let mut receiver = Receiver::attach(&mut recv_session, "receiver-link", address.as_str())
            .await
            .unwrap();

        let message = Message::builder()
            .properties(
                Properties::builder()
                    .message_id(MessageId::String("real-message-id-123".to_string()))
                    .build(),
            )
            .data(br#"{"amount":20}"#.to_vec())
            .build();
        sender
            .send(message)
            .await
            .unwrap()
            .accepted_or_else(|o| format!("{o:?}"))
            .unwrap();

        let delivery = tokio::time::timeout(Duration::from_secs(15), receiver.recv::<Data>())
            .await
            .expect("must receive the sent message within 15s")
            .unwrap();
        receiver.accept(&delivery).await.unwrap();

        let properties = delivery.message().properties.as_ref();
        let meta = InboundMessageMeta {
            group_id: properties.and_then(|p| p.group_id.clone()),
            group_sequence: properties.and_then(|p| p.group_sequence),
            message_id: properties.and_then(|p| p.message_id.clone()),
            correlation_id: None,
            causation_id: None,
        };

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
            &meta,
            delivery.body().0.as_ref(),
        )
        .await
        .unwrap();

        let requests = mock_state.trigger_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0["payload"], json!({ "amount": 20 }));
        assert_eq!(requests[0].1.as_deref(), Some("real-message-id-123"));
    });
}
