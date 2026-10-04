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
//!   with backoff, *for that one message*, before this consumer ever
//!   calls `recv()` again ([`run_inbound`]) - not tracked via Kafka's own
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
//! # Producer configuration
//!
//! [`produce_once`] produces and acknowledges strictly one event at a
//! time, in sequence order: the read cursor is a single position that
//! refuses to move backwards, so acknowledging any later event first
//! (or concurrently) could pass an earlier one that then fails, and
//! nothing would ever fetch it again. With one record in flight per
//! mapping, per-key order in Kafka holds without any producer setting.
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
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use std::collections::HashMap;
use std::time::Duration;

// docs/architecture.md §165: the skilj side every bridge shares.
use skilj_bridge::parked_payload;
pub use skilj_bridge::{
    correlation_key, http_client, ConsumedEvent, ConsumedEventMetadata, InboundAction,
    InboundMapping, Tag, HTTP_REQUEST_TIMEOUT,
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

/// Whether `mapping` (partitioned or not) is responsible for an event
/// whose own [`correlation_key`] output is `key` - `true` unconditionally
/// for an unpartitioned mapping (`partition: None`), matching
/// [`partition_for_key`] against this mapping's own `partition_index`
/// otherwise.
fn owns_partition_for(mapping: &OutboundMapping, key: Option<&str>) -> bool {
    skilj_bridge::owns_partition(mapping.partition, key)
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

/// Codeberg issue #21 - the backoff state one [`OutboundMapping`]'s own
/// blocked head-of-line event carries across [`produce_once`] calls,
/// threaded in by [`run_outbound`] (one instance per mapping - see its
/// own doc comment). Only the head can ever be blocked: [`produce_once`]
/// never attempts an event *behind* one still in backoff (see its own
/// doc comment), the identical invariant `skilj_core::db`'s own
/// `cross_context_route_cursors` retry columns rely on for the same
/// reason.
#[derive(Debug, Clone, Copy)]
pub struct OutboundRetryState {
    /// Which event this state belongs to - `produce_once` clears the
    /// state whenever a different sequence succeeds, so a stale state
    /// left over from an old, now-skipped event is never mistaken for
    /// the current head's.
    sequence: i64,
    attempt: u32,
    first_failed_at: DateTime<Utc>,
    next_attempt_at: DateTime<Utc>,
}

async fn ack_event(
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &OutboundMapping,
    sequence: i64,
) -> Result<(), BridgeError> {
    Ok(skilj_bridge::ack(http, skilj_base_url, &mapping.credential, sequence).await?)
}

/// Produces one event to Kafka, then acknowledges it to skilj - never
/// the other order, so a crash between the two redelivers the same event
/// next cycle rather than silently dropping it. That redelivered produce
/// does land on Kafka a second time: `enable.idempotence` only covers the
/// producer's own internal retries, not a fresh `send` of the same event
/// (see this crate's own "Producer configuration" section), so Kafka
/// consumers must tolerate duplicates.
async fn produce_and_ack_one(
    http: &reqwest::Client,
    skilj_base_url: &str,
    producer: &FutureProducer,
    mapping: &OutboundMapping,
    event: &ConsumedEvent,
) -> Result<(), BridgeError> {
    let key = correlation_key(mapping.key_tag_key.as_deref(), &event.tags);
    let payload = event.payload.to_string();
    let mut record = FutureRecord::to(&mapping.topic).payload(&payload);
    if let Some(k) = key.as_deref() {
        record = record.key(k);
    }
    // Codeberg issue #18 - forwards the event's own correlation_id/
    // causation_id (always present on correlation_id, per the spec's own
    // CorrelationIdIsAlwaysRecorded invariant; causation_id absent for a
    // root event) as headers, distinct from `key` above.
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
    producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(e, _)| e)?;
    ack_event(http, skilj_base_url, mapping, event.sequence).await
}

/// One fetch-produce-ack cycle for a single [`OutboundMapping`] -
/// [`run_outbound`] is just this in a loop. Exposed separately so it can
/// be driven directly in tests without needing to interrupt a running
/// loop, the same shape `skilj_temporal::poll_once` already has. Returns
/// how many events this cycle actually produced (0 when the mapping's
/// own read cursor is already caught up, or when its own head event is
/// still in backoff - see [`OutboundRetryState`]'s own doc comment). A
/// skipped event (Codeberg issue #21 - `retry_policy` exhausted) is
/// acknowledged but not counted here, so a caller checking "did this
/// cycle make real progress" isn't misled into thinking Kafka actually
/// received it.
///
/// A failure produces *or* acknowledging one event stops this cycle
/// right there - `retry_state` records it, and no event behind it is
/// even attempted this cycle (the identical "the head blocks everything
/// behind it" behaviour `catch_up_cross_context_route` has, and for the
/// same reason: skipping ahead would silently drop the blocked event
/// from ever being retried, since nothing would ever revisit it once a
/// later one's own ack passes it).
pub async fn produce_once(
    http: &reqwest::Client,
    skilj_base_url: &str,
    producer: &FutureProducer,
    mapping: &OutboundMapping,
    retry_policy: &skilj_retry::RetryPolicy,
    retry_state: &mut Option<OutboundRetryState>,
) -> Result<usize, BridgeError> {
    if let Some(state) = retry_state {
        if Utc::now() < state.next_attempt_at {
            return Ok(0);
        }
    }

    let consumed = skilj_bridge::consume(
        http,
        skilj_base_url,
        &mapping.credential,
        &mapping.event_type,
    )
    .await?;

    let mut served = 0;
    for event in &consumed.events {
        let key = correlation_key(mapping.key_tag_key.as_deref(), &event.tags);
        let owned = owns_partition_for(mapping, key.as_deref());
        let outcome = if owned {
            produce_and_ack_one(http, skilj_base_url, producer, mapping, event).await
        } else {
            // Codeberg issue #25's investigation (docs/architecture.md
            // §54) - not this instance's own partition: acknowledged
            // (so this instance's own cursor still advances past it)
            // without ever being produced to Kafka. Whichever instance
            // *does* own this event's own partition sees it too, via its
            // own dedicated credential/cursor - see `OutboundMapping::partition`'s
            // own doc comment.
            ack_event(http, skilj_base_url, mapping, event.sequence).await
        };
        match outcome {
            Ok(()) => {
                // A partition-skip is acknowledged but not counted here,
                // the identical "not misleading a caller checking did
                // this cycle make real progress" treatment this
                // function's own doc comment already gives a
                // retry-exhausted skip below.
                if owned {
                    served += 1;
                }
                if retry_state.is_some_and(|s| s.sequence == event.sequence) {
                    *retry_state = None;
                }
            }
            Err(e) => {
                let now = Utc::now();
                let (attempt, first_failed_at) = match retry_state {
                    Some(state) if state.sequence == event.sequence => {
                        state.attempt += 1;
                        (state.attempt, state.first_failed_at)
                    }
                    _ => {
                        *retry_state = Some(OutboundRetryState {
                            sequence: event.sequence,
                            attempt: 1,
                            first_failed_at: now,
                            next_attempt_at: now,
                        });
                        (1, now)
                    }
                };
                let elapsed = (now - first_failed_at).to_std().unwrap_or_default();
                if retry_policy.is_exhausted(attempt, elapsed) {
                    tracing::error!(
                        event_type = %mapping.event_type,
                        sequence = event.sequence,
                        attempt,
                        error = %e,
                        "giving up on this event after repeated failures - skipping it \
                         (acknowledging without ever producing it to Kafka) so the stream \
                         isn't blocked forever"
                    );
                    ack_event(http, skilj_base_url, mapping, event.sequence).await?;
                    *retry_state = None;
                    continue;
                }
                tracing::warn!(
                    event_type = %mapping.event_type,
                    sequence = event.sequence,
                    attempt,
                    error = %e,
                    "producing/acknowledging this event failed - will retry with backoff"
                );
                if let Some(state) = retry_state {
                    state.next_attempt_at = retry_policy.next_attempt_at(now, attempt);
                }
                return Ok(served);
            }
        }
    }
    Ok(served)
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

/// Runs forever: for every message this `consumer` receives (already
/// subscribed to whatever topics its own caller configured), looks up
/// the [`InboundMapping`] for that message's own topic and dispatches it
/// via [`dispatch_inbound_message`], committing the offset
/// (`CommitMode::Async`, via `commit_message` - the caller's own
/// `ClientConfig` must set `enable.auto.commit = false` for this to be
/// the only thing that ever advances it) once that call succeeds.
///
/// Codeberg issue #21: a dispatch failure is retried, *for this one
/// message*, with backoff up to `retry_policy` - not by relying on
/// Kafka's own redelivery, which doesn't apply within one running
/// session (an uncommitted offset only replays after a restart/
/// rebalance; the next plain `recv()` here would just move on to the
/// next message). Once `retry_policy` exhausts, the message is reported
/// to skilj via `report_parked_delivery` and the offset is committed
/// anyway - without that, a poison message would block this mapping's
/// own durable commit point forever, redelivering an ever-growing
/// backlog on every future restart. If the *report* itself fails, it is
/// retried (with `retry_policy`'s backoff, logged each time) until it
/// succeeds, and nothing after this message is consumed meanwhile:
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
/// request stuck on a dead connection stalls this loop forever (§82).
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
/// `stop` is raced against the wait for the next message and against the
/// backoff between a failing message's retries - never against a dispatch
/// or report in flight. A message it stops during is left uncommitted and
/// comes back on the next start, where skilj's own dedupe and
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
    loop {
        let received = tokio::select! {
            biased;
            () = &mut stop => return,
            received = consumer.recv() => received,
        };
        match received {
            Ok(msg) => {
                let topic = msg.topic();
                let Some(mapping) = mappings.get(topic) else {
                    tracing::warn!(
                        topic,
                        "message on an unmapped topic - skipping, not committing"
                    );
                    continue;
                };
                let Some(payload) = msg.payload() else {
                    tracing::warn!(
                        topic,
                        partition = msg.partition(),
                        offset = msg.offset(),
                        "message has no payload - skipping, not committing"
                    );
                    continue;
                };
                let headers = msg.headers();
                let correlation_id = header_str(headers, CORRELATION_ID_HEADER);
                let causation_id = header_str(headers, CAUSATION_ID_HEADER);
                let partition = msg.partition();
                let offset = msg.offset();

                let mut retry = skilj_retry::MessageRetry::default();
                loop {
                    match dispatch_inbound_message(
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
                        Ok(()) => {
                            if let Err(e) = consumer.commit_message(&msg, CommitMode::Async) {
                                tracing::error!("committing a Kafka offset failed: {e}");
                            }
                            break;
                        }
                        Err(e) => {
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
                                    let idempotency_key =
                                        inbound_idempotency_key(mapping, &partition_key, offset);
                                    // docs/architecture.md §97: Kafka offsets are
                                    // cumulative - committing any later message in
                                    // this partition would commit past this one too.
                                    // So until it is reported, this bridge doesn't
                                    // move on: moving on would lose it, neither
                                    // processed nor parked.
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
                                            () = &mut stop => return,
                                            () = tokio::time::sleep(
                                                retry_policy.next_backoff(report_attempt),
                                            ) => {}
                                        }
                                    }
                                    if let Err(commit_err) =
                                        consumer.commit_message(&msg, CommitMode::Async)
                                    {
                                        tracing::error!(
                                            "committing a Kafka offset failed: {commit_err}"
                                        );
                                    }
                                    break;
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
                                () = &mut stop => return,
                                () = tokio::time::sleep(backoff) => {}
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::error!("Kafka consumer error: {e}");
            }
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

    #[test]
    fn owns_partition_for_is_always_true_when_unpartitioned() {
        let mapping = OutboundMapping {
            event_type: "OrderPlaced".to_string(),
            credential: "irrelevant".to_string(),
            topic: "irrelevant".to_string(),
            key_tag_key: None,
            partition: None,
        };
        for key in [None, Some("o-1"), Some("o-2"), Some("")] {
            assert!(owns_partition_for(&mapping, key));
        }
    }

    /// Every key must be owned by exactly one partition index - not zero
    /// (a key silently dropped by every instance) and not more than one
    /// (a key double-published by two instances), for a real spread of
    /// keys, not just one.
    #[test]
    fn owns_partition_for_assigns_every_key_to_exactly_one_partition() {
        let partition_count = 4;
        let keys: Vec<Option<&str>> = vec![
            Some("o-1"),
            Some("o-2"),
            Some("o-3"),
            Some("o-4"),
            Some("o-5"),
            Some("o-6"),
            Some("o-7"),
            Some("o-8"),
            None,
        ];
        for key in keys {
            let owners: Vec<u32> = (0..partition_count)
                .filter(|&partition_index| {
                    owns_partition_for(
                        &OutboundMapping {
                            event_type: "OrderPlaced".to_string(),
                            credential: "irrelevant".to_string(),
                            topic: "irrelevant".to_string(),
                            key_tag_key: None,
                            partition: Some((partition_index, partition_count)),
                        },
                        key,
                    )
                })
                .collect();
            assert_eq!(
                owners.len(),
                1,
                "key {key:?} must be owned by exactly one of {partition_count} partitions, \
                 got {owners:?}"
            );
        }
    }
}
