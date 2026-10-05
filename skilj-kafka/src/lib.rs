//! A bridge between skilj's own event stream and Kafka -
//! [docs/architecture.md §40](../../docs/architecture.md#skilj-kafka-bridge), `skilj-temporal`'s ([§34](../../docs/architecture.md#skilj-temporal-plan)) direct sibling.
//! Wire-protocol client only, on both sides: this crate speaks skilj's
//! REST surface and `rdkafka`'s own client - zero dependency on any
//! other skilj crate, the same "independently usable, wire protocol
//! only" posture `skilj-tui`/`skilj-temporal` already have.
//!
//! Two independent directions, each driven by its own mapping list -
//! unlike `skilj-temporal`, which only ever reacts to skilj's own
//! events, a message broker genuinely needs both:
//!
//! - **Outbound** (`OutboundMapping`/[`produce_once`]/[`run_outbound`]):
//!   a skilj `EventType` -> a Kafka topic, via `GET
//!   /v1/events/consume`/`POST /v1/events/consume/ack` - the identical
//!   delivery mechanism `skilj_temporal::poll_once`/`run` already
//!   establish for Temporal.
//! - **Inbound** (`InboundMapping`/[`dispatch_inbound_message`]/[`run_inbound`]):
//!   a Kafka topic -> skilj, either `POST /v1/events/external`
//!   ([`InboundAction::Record`]) or `POST /v1/commands/trigger`
//!   ([`InboundAction::Trigger`]) - a per-mapping choice, mirroring
//!   `skilj_temporal::MappingAction`'s own signal-vs-start split.
//!
//! # Correlation (outbound)
//!
//! Kafka's own analogue of `skilj-temporal`'s workflow id is the
//! *message key* - it drives partition assignment, so events sharing a
//! key land in the same partition and keep their relative order there.
//! Derived from one of the event's own DCB tags, the same "derived from
//! a tag, not a fresh concept" register `correlation_workflow_id`
//! already uses - see [`correlation_key`].
//!
//! # Redelivery safety (inbound)
//!
//! Both [`InboundAction`] variants derive their own redelivery-safety
//! key from the message's own stable identity - `"{topic}:{partition}"`
//! as the partition key, the message's own `offset` as the sequence -
//! exactly the shape [docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event)'s `dedupe` mechanism and
//! `submitCommand`'s own `Idempotency-Key` ([§21](../../docs/architecture.md#optional-idempotency-key-submission)) were each built to
//! take, not a new mechanism invented here. [`InboundAction::Record`]
//! sends `dedupe` on `POST /v1/events/external`; [`InboundAction::Trigger`]
//! sends `Idempotency-Key` on `POST /v1/commands/trigger`. The Kafka
//! offset itself is only committed after skilj confirms the call
//! succeeded ([`run_inbound`]), so a redelivery (a consumer crash before
//! its own commit lands) calls skilj again rather than silently
//! skipping the message - safe specifically *because* the skilj-side
//! mechanisms above make that redelivered call a no-op, not a
//! duplicate.
//!
//! # Dead-letter/parking (Codeberg issue #21)
//!
//! Both directions apply a [`skilj_retry::RetryPolicy`], but to different
//! ends - see docs/architecture.md's parked-deliveries section for the
//! full design, only summarised here:
//!
//! - **Inbound**: a message that keeps failing to dispatch is retried
//!   with backoff, *for that one message*, before anything behind it in
//!   its partition is dispatched ([`run_inbound`]) - not tracked via Kafka's own
//!   redelivery, which doesn't apply within one running session anyway
//!   (an uncommitted offset only replays after a restart/rebalance, not
//!   on the next `recv()` in the same session). Once the policy exhausts,
//!   the message is reported to skilj's own `POST /v1/parked-deliveries`
//!   (`report_parked_delivery`) and the offset is committed anyway -
//!   without that, a poison message would block this mapping's own
//!   durable commit point forever, redelivering an ever-growing backlog
//!   on every future restart.
//! - **Outbound**: an event that keeps failing to produce is retried with
//!   backoff across [`produce_once`] calls (state threaded through
//!   [`OutboundRetryState`]), same as inbound - but the default policy is
//!   [`skilj_retry::RetryPolicy::unbounded`], not bounded: a Kafka outage
//!   should self-heal once the broker is back, not give up. If a caller
//!   configures a bounded policy anyway, exhausting it skips that one
//!   event (acknowledges it to skilj without ever producing it) and moves
//!   on, logged loudly - no parked-delivery record, since there is
//!   nothing wrong with the *message*, only (temporarily) with reaching
//!   the broker.
//!
//! # Inbound concurrency
//!
//! [`run_inbound`] dispatches one message at a time *per partition*, in
//! offset order, and the partitions it is assigned concurrently
//! (docs/architecture.md §174). Kafka only orders messages within a
//! partition, and that is all the `dedupe` watermark and `decide()` rely
//! on, so this is as much concurrency as stays safe. A message that keeps
//! failing holds up its own partition only.
//!
//! Concurrency never weakens DCB consistency: a bounded context has one
//! sequence and one lock, and every `decide()` runs under it against all
//! committed events with its tags. What it changes is the *order* in
//! which messages from different partitions reach skilj, and with DCB
//! the order can change the outcome ("cancel order 1" decided before
//! "place order 1" is rejected - consistently, but differently). Kafka
//! never promised an order across partitions, and a sequential consumer
//! interleaves them however librdkafka hands them over, so this was
//! already true; concurrency makes it happen more often. So:
//!
//! **Messages whose relative order matters must share a Kafka key** -
//! typically the value of the DCB consistency tag they're about (the
//! order id), so they land in one partition. A command that spans two
//! entities (a transfer from A to B) sees A-keyed and B-keyed messages
//! in no guaranteed order whatever the key; DCB keeps the outcome
//! consistent, and arrival order decides which it is.
//!
//! One instance has at most as many requests in flight as it has
//! assigned partitions. To scale out further, run more instances in the
//! same consumer group - each partition goes to one of them, so up to one
//! instance per partition does useful work.
//!
//! # Producer configuration
//!
//! [`produce_once`] produces strictly one event at a time, in sequence
//! order, and acknowledges once per contiguous run of handled events -
//! never past one that hasn't been produced. The read cursor is a single
//! position that refuses to move backwards, so acknowledging a later
//! event before an earlier one is produced could pass it, and nothing
//! would ever fetch it again. With one record in flight per mapping,
//! per-key order in Kafka holds without any producer setting.
//!
//! What the caller's own `ClientConfig` still decides:
//!
//! - `enable.idempotence=true` stops the *producer's own* internal
//!   retries from writing a record twice. It does not deduplicate a
//!   record this crate sends again because its acknowledgement to skilj
//!   failed - delivery to Kafka is at-least-once either way.
//! - `compression.type` (`snappy`, `lz4` or `zstd`) is worth setting for
//!   JSON payloads; what it saves depends on the payloads.
//!
//! ```rust,no_run
//! use rdkafka::producer::FutureProducer;
//! use rdkafka::ClientConfig;
//!
//! let producer: FutureProducer = ClientConfig::new()
//!     .set("bootstrap.servers", "localhost:9092")
//!     .set("enable.idempotence", "true")
//!     .set("compression.type", "zstd")
//!     .create()
//!     .expect("producer creation failed");
//! ```

use chrono::{DateTime, Utc};
use futures_util::stream::{FuturesUnordered, StreamExt};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders, OwnedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, Offset, TopicPartitionList};
use std::collections::{HashMap, VecDeque};
use std::time::Duration;

// docs/architecture.md §165: the skilj side every bridge shares.
use skilj_bridge::parked_payload;
pub use skilj_bridge::{
    correlation_key, http_client, ConsumedEvent, ConsumedEventMetadata, InboundAction,
    InboundMapping, OutboundRetryState, Tag, HTTP_REQUEST_TIMEOUT,
};

/// Header names for the two Codeberg-issue-#18 ids this bridge carries
/// across the Kafka boundary, both directions. Kafka's own message
/// headers are fully custom (no native correlation/causation concept the
/// way AMQP has `correlation-id`), so both get a `Skilj-`-prefixed
/// header rather than reusing [`correlation_key`]'s own Kafka message
/// *key* - that's an unrelated, pre-existing concept (a DCB-tag-derived
/// partition-routing key), not a business-transaction id, and the two
/// must not be conflated.
const CORRELATION_ID_HEADER: &str = "Skilj-Correlation-Id";
const CAUSATION_ID_HEADER: &str = "Skilj-Causation-Id";

// --- outbound: skilj event -> Kafka ---

/// One skilj `EventType`'s own mapping to a Kafka topic - the outbound
/// half, direct analogue of `skilj_temporal::EventTypeMapping`.
pub struct OutboundMapping {
    /// The `EventType::NAME` this mapping applies to.
    pub event_type: String,
    /// An `EventReadToken` credential (`"{id}.{secret}"`) scoped to this
    /// event type - `EventReadToken` is always scoped to exactly one
    /// `EventType` (`docs/rest-event-reading.md`), so one token per
    /// mapped event type, not one for the whole bounded context.
    pub credential: String,
    pub topic: String,
    /// Which of this event type's own tag keys supplies the Kafka
    /// message key - `None` sends no key at all, and Kafka itself then
    /// distributes the message across partitions on its own (round-robin
    /// by default). See [`correlation_key`].
    pub key_tag_key: Option<String>,
    /// `Some((partition_index, partition_count))` splits this
    /// `EventType`'s own outbound work across `partition_count`
    /// independent bridge instances (Codeberg issue #25's investigation,
    /// docs/architecture.md §54) - each with its own dedicated
    /// `credential` (a distinct `EventReadToken`, so each gets its own
    /// independently-checkout-protected `read_cursors` row, §53 - no new
    /// coordination is needed beyond that). `None` (the default) means
    /// unpartitioned: this mapping alone handles every event, exactly as
    /// before this feature existed.
    ///
    /// Reuses [`correlation_key`]'s own output as the partitioning
    /// input: the same tag that drives Kafka's own native partition
    /// assignment also decides which bridge instance is responsible for
    /// an event, via the identical FNV-1a hash
    /// (`skilj_core::db::partition_for_key`) skilj-core's own §51/§52
    /// partitioning already established, vendored here rather than
    /// pulled in as a dependency (this crate is deliberately
    /// skilj-core-free - see the module doc comment). An event with no
    /// derivable key (no `key_tag_key` configured, or the tag absent)
    /// hashes on the empty string, which is a real, stable answer - just
    /// one that sends every keyless event to whichever single partition
    /// happens to own that hash, not spread across instances. A
    /// partitioned mapping that doesn't own a given event still
    /// acknowledges it (advancing this instance's own cursor past it)
    /// without producing it to Kafka - the identical "position always
    /// advances even when nothing happens for this particular item"
    /// treatment `catch_up_cross_context_route` already gives an
    /// unregistered target command type.
    pub partition: Option<(u32, u32)>,
}

#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("calling skilj's own REST surface: {0}")]
    Skilj(#[from] reqwest::Error),
    #[error("skilj returned {status}: {body}")]
    SkiljStatus {
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("producing to Kafka: {0}")]
    Produce(#[from] rdkafka::error::KafkaError),
    /// `OutboundMapping::event_type` doesn't match what the
    /// `EventReadToken` given as its own `credential` actually serves -
    /// almost certainly a copy-paste misconfiguration, the identical
    /// check `skilj_temporal::poll_once` already makes.
    #[error(
        "OutboundMapping declares event_type \"{declared}\", but its own credential is scoped \
         to \"{actual}\" - wrong token for this mapping"
    )]
    EventTypeMismatch { declared: String, actual: String },
    /// A Kafka message's own payload wasn't valid JSON - unrecoverable
    /// the same way `skilj_temporal`'s own `NoCorrelationTag` is: no
    /// amount of retrying produces valid JSON out of bytes that aren't.
    /// [`run_inbound`] parks it on the first failure, raw content kept,
    /// and commits past it - it doesn't retry (docs/architecture.md §144).
    #[error("Kafka message payload is not valid JSON: {0}")]
    MalformedPayload(#[from] serde_json::Error),
}

impl From<skilj_bridge::SkiljError> for BridgeError {
    fn from(error: skilj_bridge::SkiljError) -> Self {
        match error {
            skilj_bridge::SkiljError::Http(e) => BridgeError::Skilj(e),
            skilj_bridge::SkiljError::SkiljStatus { status, body } => {
                BridgeError::SkiljStatus { status, body }
            }
            skilj_bridge::SkiljError::EventTypeMismatch { declared, actual } => {
                BridgeError::EventTypeMismatch { declared, actual }
            }
        }
    }
}

impl BridgeError {
    /// Whether skilj refused this with one of
    /// [`skilj_retry::ANOTHER_INSTANCE_CODES`]: the instance that took the
    /// request can't process it, another one can - retried after
    /// [`skilj_retry::ANOTHER_INSTANCE_RETRY_DELAY`] without spending an
    /// attempt, and never parked (docs/architecture.md §161).
    fn another_instance_can_do_it(&self) -> bool {
        match self {
            BridgeError::SkiljStatus { body, .. } => skilj_bridge::another_instance_refusal(body),
            _ => false,
        }
    }
}

/// Kafka's own half of [`skilj_bridge::outbound_cycle`]: produces one
/// event to the mapping's topic, keyed by its [`correlation_key`].
struct KafkaSink<'a> {
    producer: &'a FutureProducer,
    mapping: &'a OutboundMapping,
}

impl skilj_bridge::OutboundSink for KafkaSink<'_> {
    type Error = BridgeError;

    fn broker_name(&self) -> &'static str {
        "Kafka"
    }

    async fn deliver(&mut self, event: &ConsumedEvent) -> Result<(), BridgeError> {
        let key = correlation_key(self.mapping.key_tag_key.as_deref(), &event.tags);
        let payload = event.payload.to_string();
        let mut record = FutureRecord::to(&self.mapping.topic).payload(&payload);
        if let Some(k) = key.as_deref() {
            record = record.key(k);
        }
        // Codeberg issue #18 - forwards the event's own correlation_id/
        // causation_id (always present on correlation_id, per the spec's
        // own CorrelationIdIsAlwaysRecorded invariant; causation_id absent
        // for a root event) as headers, distinct from `key` above.
        let mut headers = OwnedHeaders::new();
        if let Some(id) = &event.metadata.correlation_id {
            headers = headers.insert(Header {
                key: CORRELATION_ID_HEADER,
                value: Some(id),
            });
        }
        if let Some(id) = &event.metadata.causation_id {
            headers = headers.insert(Header {
                key: CAUSATION_ID_HEADER,
                value: Some(id),
            });
        }
        if headers.count() > 0 {
            record = record.headers(headers);
        }
        self.producer
            .send(record, Duration::from_secs(10))
            .await
            .map_err(|(e, _)| e)?;
        Ok(())
    }
}

/// One fetch-produce-ack cycle for a single [`OutboundMapping`] -
/// [`run_outbound`] is just this in a loop. Exposed separately so it can
/// be driven directly in tests without needing to interrupt a running
/// loop, the same shape `skilj_temporal::poll_once` already has. See
/// [`skilj_bridge::outbound_cycle`] for what a cycle does: events are
/// produced strictly in sequence order, one at a time, and acknowledged
/// once per contiguous run; it returns how many were produced and
/// acknowledged. An event is acknowledged only after Kafka accepted it,
/// so a crash in between produces it again next cycle - and that second
/// produce does land on Kafka: `enable.idempotence` only covers the
/// producer's own internal retries (see this crate's own "Producer
/// configuration" section), so Kafka consumers must tolerate duplicates.
pub async fn produce_once(
    http: &reqwest::Client,
    skilj_base_url: &str,
    producer: &FutureProducer,
    mapping: &OutboundMapping,
    retry_policy: &skilj_retry::RetryPolicy,
    retry_state: &mut Option<OutboundRetryState>,
) -> Result<usize, BridgeError> {
    let target = skilj_bridge::OutboundTarget {
        credential: &mapping.credential,
        event_type: &mapping.event_type,
        partition: mapping.partition,
        key_tag_key: mapping.key_tag_key.as_deref(),
    };
    let mut sink = KafkaSink { producer, mapping };
    skilj_bridge::outbound_cycle(
        http,
        skilj_base_url,
        &target,
        &mut sink,
        retry_policy,
        retry_state,
    )
    .await
}

/// Runs [`produce_once`] forever, one mapping at a time in the order
/// given, sleeping `poll_interval` between cycles that served nothing -
/// the identical shape `skilj_temporal::run` already has, including why
/// a real deployment runs one [`OutboundMapping`] per task
/// (`tokio::spawn`) rather than calling this with more than one mapping
/// serially. `retry_policy` applies to every mapping alike - see
/// [`produce_once`]'s own doc comment and this crate's own "Dead-letter/
/// parking" section for what it governs.
///
/// See this crate's own "Producer configuration" section for which
/// `producer` settings matter.
pub async fn run_outbound(
    skilj_base_url: &str,
    producer: &FutureProducer,
    mappings: &[OutboundMapping],
    poll_interval: Duration,
    retry_policy: &skilj_retry::RetryPolicy,
) -> ! {
    run_outbound_until(
        skilj_base_url,
        producer,
        mappings,
        poll_interval,
        retry_policy,
        std::future::pending(),
    )
    .await;
    unreachable!("run_outbound_until only returns once `stop` resolves, and `pending()` never does")
}

/// [`run_outbound`] until `stop` resolves (docs/architecture.md §129):
/// the cycle in progress - an event being produced and acknowledged -
/// completes, then this returns. Aborting the task instead can land
/// between producing and acknowledging, and the event is produced again
/// on the next start: a duplicate for the topic's consumers. `stop` is
/// raced only against the idle sleep between cycles and checked after
/// each cycle.
pub async fn run_outbound_until(
    skilj_base_url: &str,
    producer: &FutureProducer,
    mappings: &[OutboundMapping],
    poll_interval: Duration,
    retry_policy: &skilj_retry::RetryPolicy,
    stop: impl std::future::Future<Output = ()>,
) {
    let mut stop = std::pin::pin!(stop);
    let http = http_client();
    let mut retry_states: Vec<Option<OutboundRetryState>> = vec![None; mappings.len()];
    loop {
        let mut served_any = false;
        for (mapping, retry_state) in mappings.iter().zip(retry_states.iter_mut()) {
            match produce_once(
                &http,
                skilj_base_url,
                producer,
                mapping,
                retry_policy,
                retry_state,
            )
            .await
            {
                Ok(served) => served_any |= served > 0,
                Err(e) => {
                    tracing::error!(
                        event_type = %mapping.event_type,
                        "poll cycle failed, retrying after poll_interval: {e}"
                    );
                }
            }
        }
        let idle = if served_any {
            Duration::ZERO
        } else {
            poll_interval
        };
        tokio::select! {
            biased;
            () = &mut stop => return,
            () = tokio::time::sleep(idle) => {}
        }
    }
}

// --- inbound: Kafka -> skilj ---

/// Reads a header's value as UTF-8 text - `None` for a missing header, a
/// header present with no value (Kafka allows this), or one whose bytes
/// aren't valid UTF-8. [`run_inbound`]'s own extraction step for
/// `CORRELATION_ID_HEADER`/`CAUSATION_ID_HEADER`, kept as a free
/// function rather than inlined so [`dispatch_inbound_message`] itself
/// stays header-type-agnostic (see its own doc comment on why it takes
/// plain `Option<&str>` rather than a whole headers object). Public so
/// integration tests can assert on a real produced message's own headers
/// without reimplementing this lookup.
pub fn header_str<'a, H: Headers>(headers: Option<&'a H>, key: &str) -> Option<&'a str> {
    let headers = headers?;
    headers
        .iter()
        .find(|h| h.key == key)
        .and_then(|h| h.value)
        .and_then(|v| std::str::from_utf8(v).ok())
}

/// Dispatches one Kafka message to skilj - the one place [`InboundAction`]
/// is interpreted. Exposed separately from [`run_inbound`] so it can be
/// tested directly against raw `(topic, partition, offset, payload)`
/// values, without needing a real `rdkafka` message object at all - the
/// same "pure fields in, one HTTP call out" shape [`produce_once`]'s own
/// per-event body has. `correlation_id`/`causation_id` (Codeberg issue
/// #18) are plain `Option<&str>` for the identical reason - `run_inbound`
/// extracts them from the real message's own headers via [`header_str`]
/// before calling.
///
/// `"{topic}:{partition}"` is this message's own redelivery-safety
/// partition key, `offset` its own sequence - Kafka's guarantee of
/// strictly increasing, in-order delivery within one partition is
/// exactly what both [docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event)'s `dedupe` watermark and
/// `submitCommand`'s own idempotency key rely on, the same property
/// `skilj_temporal`'s own `"{run_id}:{activity_id}"` convention ([§34](../../docs/architecture.md#skilj-temporal-plan)
/// phase 1) already leans on one level further out.
/// The exact `ExternalEventRequest`/`CommandTriggerRequest` body
/// [`dispatch_inbound_message`] sends for `mapping`/`payload_json` -
/// factored out so [`report_parked_delivery`] can store the identical
/// body a `retryParkedDelivery` redrive later needs, without either
/// duplicating this shape or sending a live HTTP request just to build
/// it.
fn inbound_request_body(
    mapping: &InboundMapping,
    payload_json: &serde_json::Value,
    partition_key: &str,
    offset: i64,
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
) -> serde_json::Value {
    match &mapping.action {
        InboundAction::Record { .. } => serde_json::json!({
            "payload": payload_json,
            "sourceContent": format!("kafka:{partition_key}:{offset}"),
            "dedupe": { "partitionKey": partition_key, "sequence": offset },
            "correlationId": correlation_id,
            "causationId": causation_id,
        }),
        InboundAction::Trigger { .. } => serde_json::json!({
            "payload": payload_json,
            "correlationId": correlation_id,
            "causationId": causation_id,
        }),
    }
}

/// The `Idempotency-Key` a `Trigger` mapping's request carries for the
/// message at `partition_key`/`offset` - `None` for `Record`, which
/// dedupes via its body's own `dedupe` cursor instead. Shared by
/// [`dispatch_inbound_message`] (as the header) and
/// [`report_parked_delivery`] (so `retryParkedDelivery` redrives under
/// the same key, deduping against the original attempt if it committed
/// after all).
fn inbound_idempotency_key(
    mapping: &InboundMapping,
    partition_key: &str,
    offset: i64,
) -> Option<String> {
    match mapping.action {
        InboundAction::Record { .. } => None,
        InboundAction::Trigger { .. } => Some(format!("{partition_key}:{offset}")),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn dispatch_inbound_message(
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    topic: &str,
    partition: i32,
    offset: i64,
    payload: &[u8],
    correlation_id: Option<&str>,
    causation_id: Option<&str>,
) -> Result<(), BridgeError> {
    let payload_json: serde_json::Value = serde_json::from_slice(payload)?;
    let partition_key = format!("{topic}:{partition}");
    let body = inbound_request_body(
        mapping,
        &payload_json,
        &partition_key,
        offset,
        correlation_id,
        causation_id,
    );

    skilj_bridge::post_inbound(
        http,
        skilj_base_url,
        mapping,
        &body,
        inbound_idempotency_key(mapping, &partition_key, offset).as_deref(),
    )
    .await?;
    // A `Trigger` rejection is reported as a `200 { accepted: false, ... }`
    // per §5.4/§7.3, not an HTTP error - deliberately not inspected
    // here: the message itself was successfully delivered and decided
    // upon, which is all this bridge ever promises, so the offset still
    // commits (`run_inbound`) either way. A business rejection is not
    // this bridge's own concern to retry.
    Ok(())
}

/// Codeberg issue #21 - reports a message [`run_inbound`] gave up
/// retrying to skilj's own `POST /v1/parked-deliveries`, using the same
/// credential `mapping` already carries (that route resolves the
/// bounded context from the presented token itself, the identical
/// capability-based design this bridge's every other call already
/// relies on). `identifier` is `"{topic}:{partition}:{offset}"` -
/// `request` is [`inbound_request_body`]'s own output, the exact body
/// that kept failing, and `idempotency_key` the header it was sent with
/// ([`inbound_idempotency_key`]), both stored so a later
/// `retryParkedDelivery` redrives the identical request.
#[allow(clippy::too_many_arguments)]
async fn report_parked_delivery(
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    identifier: &str,
    request: &serde_json::Value,
    idempotency_key: Option<&str>,
    error: &str,
    attempt_count: u32,
    first_failed_at: DateTime<Utc>,
) -> Result<(), BridgeError> {
    Ok(skilj_bridge::report_parked_delivery(
        http,
        skilj_base_url,
        mapping,
        "kafka-inbound",
        identifier,
        request,
        idempotency_key,
        error,
        attempt_count,
        first_failed_at,
    )
    .await?)
}

/// How many received messages one partition may have waiting behind the
/// one in flight before [`run_inbound`] pauses fetching it
/// (docs/architecture.md §174). Fetching resumes once its queue is down
/// to half of this.
pub const INBOUND_PARTITION_BUFFER: usize = 64;

/// What [`handle_inbound_message`] ended with.
enum Handled {
    /// Dispatched, or parked and reported - commit its offset.
    Commit,
    /// Asked to stop before either happened - leave it uncommitted.
    Stopped,
}

/// One partition's state in [`run_inbound_until`]: whether a message of
/// it is in flight, the messages received behind that one, and the
/// offset the next new message must be at least.
struct PartitionQueue<'m> {
    busy: bool,
    paused: bool,
    queue: VecDeque<(OwnedMessage, &'m InboundMapping)>,
    next_offset: i64,
}

fn partition_list(topic: &str, partition: i32) -> TopicPartitionList {
    let mut list = TopicPartitionList::new();
    list.add_partition(topic, partition);
    list
}

/// Commits `msg`'s offset - the next offset to read, like
/// `commit_message` does.
fn commit(consumer: &StreamConsumer, msg: &OwnedMessage) {
    let mut list = TopicPartitionList::new();
    if let Err(e) = list.add_partition_offset(
        msg.topic(),
        msg.partition(),
        Offset::Offset(msg.offset() + 1),
    ) {
        tracing::error!("committing a Kafka offset failed: {e}");
        return;
    }
    if let Err(e) = consumer.commit(&list, CommitMode::Async) {
        tracing::error!("committing a Kafka offset failed: {e}");
    }
}

/// Resolves once `stop` has been set - [`run_inbound_until`]'s signal to
/// a message's retry backoff that the loop is stopping.
async fn stopping(mut stop: tokio::sync::watch::Receiver<bool>) {
    let _ = stop.wait_for(|stopping| *stopping).await;
}

/// Dispatches one message, retrying and finally parking it per
/// `retry_policy` - everything [`run_inbound`] does for a message before
/// its offset is committed. Returns `msg` so the caller can commit it
/// and move its partition on.
async fn handle_inbound_message(
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    retry_policy: &skilj_retry::RetryPolicy,
    msg: OwnedMessage,
    stop: tokio::sync::watch::Receiver<bool>,
) -> (OwnedMessage, Handled) {
    let handled =
        dispatch_with_retry(http, skilj_base_url, mapping, retry_policy, &msg, stop).await;
    (msg, handled)
}

async fn dispatch_with_retry(
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    retry_policy: &skilj_retry::RetryPolicy,
    msg: &OwnedMessage,
    stop: tokio::sync::watch::Receiver<bool>,
) -> Handled {
    let topic = msg.topic();
    let partition = msg.partition();
    let offset = msg.offset();
    // `run_inbound_until` only queues messages with a payload.
    let payload = msg.payload().unwrap_or_default();
    let headers = msg.headers();
    let correlation_id = header_str(headers, CORRELATION_ID_HEADER);
    let causation_id = header_str(headers, CAUSATION_ID_HEADER);

    let mut retry = skilj_retry::MessageRetry::default();
    loop {
        let e = match dispatch_inbound_message(
            http,
            skilj_base_url,
            mapping,
            topic,
            partition,
            offset,
            payload,
            correlation_id,
            causation_id,
        )
        .await
        {
            Ok(()) => return Handled::Commit,
            Err(e) => e,
        };
        let backoff = match retry.on_failure(
            retry_policy,
            Utc::now(),
            |at| (Utc::now() - at).to_std().unwrap_or_default(),
            // docs/architecture.md §161: no attempt spent, never parked.
            e.another_instance_can_do_it(),
            // Not JSON: no retry can change that (§144).
            matches!(e, BridgeError::MalformedPayload(_)),
        ) {
            skilj_retry::RetryDecision::Park {
                attempt,
                first_failed_at: failed_at,
            } => {
                tracing::error!(
                    topic,
                    partition,
                    offset,
                    attempt,
                    error = %e,
                    "dispatch failed repeatedly - parking and committing so \
                     this message doesn't block progress forever"
                );
                let payload_json = parked_payload(payload);
                let partition_key = format!("{topic}:{partition}");
                let body = inbound_request_body(
                    mapping,
                    &payload_json,
                    &partition_key,
                    offset,
                    correlation_id,
                    causation_id,
                );
                let identifier = format!("{partition_key}:{offset}");
                let idempotency_key = inbound_idempotency_key(mapping, &partition_key, offset);
                // docs/architecture.md §97: Kafka offsets are cumulative -
                // committing any later message in this partition would
                // commit past this one too. So until it is reported, this
                // partition doesn't move on: moving on would lose it,
                // neither processed nor parked.
                let mut report_attempt: u32 = 0;
                while let Err(report_err) = report_parked_delivery(
                    http,
                    skilj_base_url,
                    mapping,
                    &identifier,
                    &body,
                    idempotency_key.as_deref(),
                    &e.to_string(),
                    attempt,
                    failed_at,
                )
                .await
                {
                    report_attempt = report_attempt.saturating_add(1);
                    tracing::error!(
                        topic,
                        partition,
                        offset,
                        report_attempt,
                        "reporting this parked delivery failed - retrying; \
                         this partition waits until it succeeds: {report_err}"
                    );
                    tokio::select! {
                        biased;
                        () = stopping(stop.clone()) => return Handled::Stopped,
                        () = tokio::time::sleep(retry_policy.next_backoff(report_attempt)) => {}
                    }
                }
                return Handled::Commit;
            }
            skilj_retry::RetryDecision::Wait(backoff) => backoff,
        };
        tracing::warn!(
            topic,
            partition,
            offset,
            attempt = retry.attempt(),
            error = %e,
            "dispatch failed - retrying after backoff"
        );
        tokio::select! {
            biased;
            () = stopping(stop.clone()) => return Handled::Stopped,
            () = tokio::time::sleep(backoff) => {}
        }
    }
}

/// Runs forever: for every message this `consumer` receives (already
/// subscribed to whatever topics its own caller configured), looks up
/// the [`InboundMapping`] for that message's own topic and dispatches it
/// via [`dispatch_inbound_message`], committing the offset
/// (`CommitMode::Async` - the caller's own `ClientConfig` must set
/// `enable.auto.commit = false` for this to be the only thing that ever
/// advances it) once that call succeeds.
///
/// Messages of one partition are dispatched one at a time, in offset
/// order; different partitions are dispatched concurrently, one message
/// in flight each (docs/architecture.md §174, Codeberg issue #60). So
/// this instance has as many requests to skilj in flight as it has
/// assigned partitions with work. Messages received for a partition
/// while one of its messages is in flight wait in a queue; once
/// [`INBOUND_PARTITION_BUFFER`] are waiting, fetching that partition is
/// paused until half of them are done. See the crate docs' *Inbound
/// concurrency* section for what this means for the order messages reach
/// skilj in.
///
/// Codeberg issue #21: a dispatch failure is retried, *for this one
/// message*, with backoff up to `retry_policy` - not by relying on
/// Kafka's own redelivery, which doesn't apply within one running
/// session (an uncommitted offset only replays after a restart/
/// rebalance). Its partition waits meanwhile; the others carry on. Once
/// `retry_policy` exhausts, the message is reported to skilj via
/// `report_parked_delivery` and the offset is committed anyway - without
/// that, a poison message would block its partition's durable commit
/// point forever, redelivering an ever-growing backlog on every future
/// restart. If the *report* itself fails, it is retried (with
/// `retry_policy`'s backoff, logged each time) until it succeeds, and
/// nothing after this message in its partition is dispatched meanwhile:
/// offsets are cumulative, so committing any later message would commit
/// past this one and lose it (docs/architecture.md §97).
/// An unmapped topic or an empty payload is logged and skipped - not
/// silently swallowed, but also not something retrying could ever fix.
/// Neither is committed here: an unmapped topic's messages stay
/// uncommitted (they are redelivered once a mapping exists), while an
/// empty payload's offset is committed past by the next message in its
/// partition, as offsets are cumulative.
///
/// `http` should be bounded by a timeout - [`http_client`] is - or one
/// request stuck on a dead connection stalls its partition forever (§82).
pub async fn run_inbound(
    consumer: &StreamConsumer,
    http: &reqwest::Client,
    skilj_base_url: &str,
    mappings: &HashMap<String, InboundMapping>,
    retry_policy: &skilj_retry::RetryPolicy,
) -> ! {
    run_inbound_until(
        consumer,
        http,
        skilj_base_url,
        mappings,
        retry_policy,
        std::future::pending(),
    )
    .await;
    unreachable!("run_inbound_until only returns once `stop` resolves, and `pending()` never does")
}

/// [`run_inbound`] until `stop` resolves (docs/architecture.md §129).
/// Once it does, nothing new is received or started; the dispatches and
/// park reports in flight finish and are committed, while a message
/// waiting out a retry backoff is abandoned. Messages left uncommitted
/// come back on the next start, where skilj's own dedupe and
/// `Idempotency-Key` make the redelivery harmless.
pub async fn run_inbound_until(
    consumer: &StreamConsumer,
    http: &reqwest::Client,
    skilj_base_url: &str,
    mappings: &HashMap<String, InboundMapping>,
    retry_policy: &skilj_retry::RetryPolicy,
    stop: impl std::future::Future<Output = ()>,
) {
    let mut stop = std::pin::pin!(stop);
    let (stopping_tx, stopping_rx) = tokio::sync::watch::channel(false);
    let start = |msg: OwnedMessage, mapping| {
        handle_inbound_message(
            http,
            skilj_base_url,
            mapping,
            retry_policy,
            msg,
            stopping_rx.clone(),
        )
    };
    let mut partitions: HashMap<(String, i32), PartitionQueue<'_>> = HashMap::new();
    let mut in_flight = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            () = &mut stop => break,
            Some((msg, handled)) = in_flight.next() => {
                if let Handled::Commit = handled {
                    commit(consumer, &msg);
                }
                let key = (msg.topic().to_string(), msg.partition());
                let Some(state) = partitions.get_mut(&key) else {
                    continue;
                };
                match state.queue.pop_front() {
                    Some((next, mapping)) => in_flight.push(start(next, mapping)),
                    None => state.busy = false,
                }
                if state.paused && state.queue.len() <= INBOUND_PARTITION_BUFFER / 2 {
                    state.paused = false;
                    if let Err(e) = consumer.resume(&partition_list(&key.0, key.1)) {
                        // Most likely revoked meanwhile; a new assignment
                        // starts unpaused.
                        tracing::debug!(topic = key.0, partition = key.1, "resuming failed: {e}");
                    }
                }
            }
            received = consumer.recv() => {
                let msg = match received {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::error!("Kafka consumer error: {e}");
                        continue;
                    }
                };
                let topic = msg.topic();
                let Some(mapping) = mappings.get(topic) else {
                    tracing::warn!(
                        topic,
                        "message on an unmapped topic - skipping, not committing"
                    );
                    continue;
                };
                if msg.payload().is_none() {
                    tracing::warn!(
                        topic,
                        partition = msg.partition(),
                        offset = msg.offset(),
                        "message has no payload - skipping, not committing"
                    );
                    continue;
                }
                let state = partitions
                    .entry((topic.to_string(), msg.partition()))
                    .or_insert_with(|| PartitionQueue {
                        busy: false,
                        paused: false,
                        queue: VecDeque::new(),
                        next_offset: i64::MIN,
                    });
                // A rebalance that hands this partition back can fetch
                // again from the last committed offset - messages already
                // queued or handled here. Dispatching them again would be
                // harmless (skilj dedupes them) but out of order.
                if msg.offset() < state.next_offset {
                    tracing::debug!(
                        topic,
                        partition = msg.partition(),
                        offset = msg.offset(),
                        "message already received - skipping"
                    );
                    continue;
                }
                state.next_offset = msg.offset() + 1;
                let partition = msg.partition();
                let msg = msg.detach();
                if !state.busy {
                    state.busy = true;
                    in_flight.push(start(msg, mapping));
                    continue;
                }
                state.queue.push_back((msg, mapping));
                if state.queue.len() >= INBOUND_PARTITION_BUFFER {
                    state.paused = true;
                    if let Err(e) = consumer.pause(&partition_list(topic, partition)) {
                        tracing::warn!(topic, "pausing a partition failed: {e}");
                    }
                }
            }
        }
    }
    let _ = stopping_tx.send(true);
    while let Some((msg, handled)) = in_flight.next().await {
        if let Handled::Commit = handled {
            commit(consumer, &msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// docs/architecture.md §161: only skilj's "another instance can"
    /// codes are retried without spending an attempt.
    #[test]
    fn another_instance_refusals_are_recognised_by_their_code() {
        let status = |status: u16, body: &str| BridgeError::SkiljStatus {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            body: body.to_string(),
        };
        assert!(status(
            503,
            r#"{"code":"sync_projection_not_declared","message":"x"}"#
        )
        .another_instance_can_do_it());
        assert!(
            status(500, r#"{"code":"no_decider_registered","message":"x"}"#)
                .another_instance_can_do_it()
        );
        assert!(
            !status(503, r#"{"code":"database_error","message":"x"}"#).another_instance_can_do_it()
        );
        assert!(!status(502, "Bad Gateway").another_instance_can_do_it());
    }

    fn tag(key: &str, value: &str) -> Tag {
        Tag {
            key: key.to_string(),
            value: Some(value.to_string()),
        }
    }

    #[test]
    fn correlation_key_is_none_when_no_key_tag_key_is_configured() {
        let tags = vec![tag("order", "o-1")];
        assert_eq!(correlation_key(None, &tags), None);
    }

    #[test]
    fn correlation_key_finds_the_matching_tag() {
        let tags = vec![tag("company", "acme"), tag("order", "o-1")];
        assert_eq!(
            correlation_key(Some("order"), &tags),
            Some("o-1".to_string())
        );
    }

    #[test]
    fn correlation_key_is_none_when_the_tag_key_is_entirely_absent() {
        let tags = vec![tag("company", "acme")];
        assert_eq!(correlation_key(Some("order"), &tags), None);
    }

    #[test]
    fn correlation_key_is_none_when_the_tag_is_present_but_its_value_is_null() {
        let tags = vec![Tag {
            key: "order".to_string(),
            value: None,
        }];
        assert_eq!(correlation_key(Some("order"), &tags), None);
    }

    // --- Codeberg issue #25's investigation (docs/architecture.md §54) ---
}
