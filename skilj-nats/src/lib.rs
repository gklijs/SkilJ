//! A bridge between skilj's own event stream and NATS JetStream -
//! [docs/architecture.md §42](../../docs/architecture.md#skilj-nats-bridge), a third sibling alongside `skilj-kafka`
//! ([§40](../../docs/architecture.md#skilj-kafka-bridge)) and `skilj-amqp` ([§41](../../docs/architecture.md#skilj-amqp-bridge)), each for a genuinely different
//! delivery model. Wire-protocol client only, on both sides: zero
//! dependency on any other skilj crate, the same "independently usable"
//! posture the other two bridges already have.
//!
//! # Why JetStream, not core NATS
//!
//! Core NATS pub/sub is fire-and-forget - a message published to a
//! subject with no active subscriber is simply gone, and there is no
//! concept of redelivery at all. That makes every mechanism this bridge
//! (and `skilj-kafka`/`skilj-amqp`) exists to use - `dedupe` ([§39](../../docs/architecture.md#external-message-dedup-create-external-event)),
//! `Idempotency-Key` ([§21](../../docs/architecture.md#optional-idempotency-key-submission)) - meaningless: there is nothing to guard
//! against redelivering if delivery was never guaranteed once. JetStream,
//! NATS's own persistence layer, is what actually gives an at-least-once
//! guarantee worth building a redelivery-safe bridge around.
//!
//! # A third, genuinely different delivery model again
//!
//! Unlike Kafka's partitioned log or AMQP 1.0's queue/topic address,
//! JetStream has no partition concept at all - one stream is one
//! ordered sequence, addressed by subject. Every message JetStream ever
//! delivers carries a real, broker-assigned `(stream, stream-sequence)`
//! pair (`fe2o3-amqp`'s own `group-id`/`group-sequence` are sender-
//! optional by contrast; JetStream's own equivalent is never optional -
//! confirmed against the real `Info` struct returned by every consumed
//! message, not assumed) - closer to Kafka's own guaranteed-metadata
//! story than AMQP's. The one genuinely optional piece is `Nats-Msg-Id`
//! (a header, set via [`async_nats::jetstream::context::Publish::message_id`]) -
//! used for `Idempotency-Key` on the inbound `Trigger` path exactly the
//! way AMQP's own `message-id` is, and by *this crate's own* outbound
//! half to get JetStream's own native, server-side idempotent-publish
//! deduplication for free (`PublishAck.duplicate`) - a real, favourable
//! difference from both Kafka (producer-side idempotence, unbounded for
//! the life of one producer session) and AMQP (no built-in publish-side
//! dedup at all).
//!
//! # Correlation (outbound)
//!
//! NATS has no Kafka-style "key routes to a partition" concept -
//! JetStream streams aren't partitioned, so there's no routing decision
//! a key could influence. What a DCB tag maps onto instead is a plain
//! header (`Skilj-Correlation-Key`, see [`OutboundMapping::correlation_tag_key`]) -
//! informational for whatever downstream consumer wants to filter or
//! group by it, not something this crate or NATS itself acts on.
//!
//! # Redelivery safety
//!
//! Outbound: [`produce_once`] sets `Nats-Msg-Id` to
//! `"{bounded_context}:{sequence}"` (the same
//! `"{bounded_context}:{event_type}:{sequence}"`-shaped convention
//! `skilj_temporal::signal_request_id` already uses, one field
//! narrower since a stream has no separate event-type dimension to
//! disambiguate) - a crash between a successful publish and skilj's own
//! ack redelivers the identical event next cycle, and JetStream's own
//! server-side dedup window recognises the repeated `Nats-Msg-Id` and
//! reports `duplicate: true` rather than storing a second message.
//! Inbound: [`InboundAction::Record`] uses the message's own
//! always-present `(stream, stream_sequence)` as [§39](../../docs/architecture.md#external-message-dedup-create-external-event)'s `dedupe` pair;
//! [`InboundAction::Trigger`] uses `Nats-Msg-Id` (when the upstream
//! sender populated one) as `Idempotency-Key` - omitted, never
//! fabricated, when absent, the same "omitting it is always fine"
//! register every other bridge in this workspace already has.
//!
//! # Dead-letter/parking (Codeberg issue #21)
//!
//! Both directions apply a [`skilj_retry::RetryPolicy`], but to different
//! ends - see docs/architecture.md's parked-deliveries section for the
//! full design, only summarised here (identical shape to
//! `skilj_kafka`/`skilj_amqp`'s own "Dead-letter/parking" sections):
//!
//! - **Inbound**: a message that keeps failing to dispatch is retried
//!   with backoff, *for that one message*, before this consumer ever
//!   pulls its next message ([`run_inbound`]) - not tracked via
//!   JetStream's own redelivery, which only replays after this
//!   consumer's own ack-wait timeout elapses, not on the very next pull
//!   in the same session. Once the policy exhausts, the message is
//!   reported to skilj's own `POST /v1/parked-deliveries`
//!   ([`report_parked_delivery`]) and acked anyway - without that, a
//!   poison message would keep being redelivered by JetStream forever,
//!   on every ack-wait timeout.
//! - **Outbound**: an event that keeps failing to publish is retried with
//!   backoff across [`produce_once`] calls (state threaded through
//!   [`OutboundRetryState`]), same as inbound - but the default policy is
//!   [`skilj_retry::RetryPolicy::unbounded`], not bounded: a JetStream
//!   outage should self-heal once the broker is back, not give up. If a
//!   caller configures a bounded policy anyway, exhausting it skips that
//!   one event (acknowledges it to skilj without ever publishing it) and
//!   moves on, logged loudly - no parked-delivery record, since there is
//!   nothing wrong with the *message*, only (temporarily) with reaching
//!   the broker.

use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::context::Context as Jetstream;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::Message as JetstreamMessage;
use chrono::{DateTime, Utc};

/// A pull consumer, already bound to its own stream and subject filter
/// at creation - see [`InboundMapping`]'s own doc comment for why
/// [`run_inbound`] doesn't need a lookup table keyed by address the way
/// `skilj_kafka::run_inbound` does.
pub type PullConsumer = Consumer<PullConfig>;
use futures_util::TryStreamExt;
use serde::Deserialize;

/// Header names for the two Codeberg-issue-#18 ids this bridge carries,
/// both directions. **Deliberately not `Skilj-Correlation-Key`** -
/// that's a pre-existing, unrelated header (a DCB-tag-derived value for
/// a downstream consumer's own filtering/grouping, see
/// [`OutboundMapping::correlation_tag_key`]/[`correlation_key`]), not a
/// business-transaction id - the two must not be conflated, hence `-Id`
/// rather than `-Key` here.
const CORRELATION_ID_HEADER: &str = "Skilj-Correlation-Id";
const CAUSATION_ID_HEADER: &str = "Skilj-Causation-Id";

// --- outbound: skilj event -> NATS JetStream ---

/// One skilj `EventType`'s own mapping to a NATS subject - the outbound
/// half, direct analogue of `skilj_kafka::OutboundMapping`/
/// `skilj_amqp::OutboundMapping`.
pub struct OutboundMapping {
    /// The `EventType::NAME` this mapping applies to.
    pub event_type: String,
    /// An `EventReadToken` credential (`"{id}.{secret}"`) scoped to this
    /// event type.
    pub credential: String,
    pub subject: String,
    /// Which of this event type's own tag keys becomes the message's
    /// own `Skilj-Correlation-Key` header - `None` sends no such header
    /// at all. See this module's own "Correlation" doc section for why
    /// this is a header, not a NATS-native routing concept the way a
    /// Kafka key or an AMQP `group-id` is.
    pub correlation_tag_key: Option<String>,
    /// `Some((partition_index, partition_count))` splits this
    /// `EventType`'s own outbound work across `partition_count`
    /// independent bridge instances (Codeberg issue #25's investigation,
    /// docs/architecture.md §54) - the identical mechanism
    /// `skilj_kafka::OutboundMapping::partition` establishes; see that
    /// field's own doc comment for the full design. Reuses
    /// `correlation_key`'s own output as the partitioning input here
    /// too: NATS has no native partition concept to align with (unlike
    /// Kafka's message key or AMQP's `group-id`), so this is purely
    /// skilj-side bookkeeping, with no broker-side echo. `None` (the
    /// default) means unpartitioned.
    pub partition: Option<(u32, u32)>,
}

/// `skilj_kafka::partition_for_key`'s own identical vendored copy - see
/// that function's own doc comment for why it's vendored rather than
/// shared via a dependency, and why not `DefaultHasher`.
fn partition_for_key(key: &str, partition_count: u32) -> u32 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash % u64::from(partition_count.max(1))) as u32
}

/// `skilj_kafka::owns_partition_for`'s own identical twin, for
/// `OutboundMapping::partition` here.
fn owns_partition_for(mapping: &OutboundMapping, key: Option<&str>) -> bool {
    match mapping.partition {
        None => true,
        Some((partition_index, partition_count)) => {
            partition_for_key(key.unwrap_or(""), partition_count) == partition_index
        }
    }
}

/// One event served by skilj's own `GET /v1/events/consume` - the
/// identical shape `skilj_kafka::ConsumedEvent`/`skilj_amqp::ConsumedEvent`
/// already use.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumedEvent {
    pub sequence: i64,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub tags: Vec<Tag>,
    pub metadata: ConsumedEventMetadata,
}

/// The subset of `MetadataDto`'s wire shape (`skilj-rest/src/routes/mod.rs`)
/// this bridge actually needs - the identical shape
/// `skilj_kafka::ConsumedEventMetadata`/`skilj_amqp::ConsumedEventMetadata`
/// already have.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumedEventMetadata {
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConsumeResponse {
    events: Vec<ConsumedEvent>,
    event_type_name: String,
}

/// The `Skilj-Correlation-Key` header value for a mapped event - `None`
/// when `correlation_tag_key` is `None`, or when the named tag is
/// absent entirely or present with a `null` value. Never an error, the
/// same "an uncorrelated message is still perfectly valid" register
/// `skilj_kafka::correlation_key`/`skilj_amqp::correlation_key` already
/// use.
pub fn correlation_key(correlation_tag_key: Option<&str>, tags: &[Tag]) -> Option<String> {
    let correlation_tag_key = correlation_tag_key?;
    tags.iter()
        .find(|t| t.key == correlation_tag_key)?
        .value
        .clone()
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
    #[error("publishing to JetStream: {0}")]
    Publish(#[from] async_nats::jetstream::context::PublishError),
    /// `OutboundMapping::event_type` doesn't match what the
    /// `EventReadToken` given as its own `credential` actually serves -
    /// the identical check `skilj_kafka::produce_once`/
    /// `skilj_amqp::produce_once` already make.
    #[error(
        "OutboundMapping declares event_type \"{declared}\", but its own credential is scoped \
         to \"{actual}\" - wrong token for this mapping"
    )]
    EventTypeMismatch { declared: String, actual: String },
    /// A JetStream message's own body wasn't valid JSON - unrecoverable
    /// the same way every other bridge's `MalformedPayload` is.
    /// [`run_inbound`] parks it on the first failure, raw content kept,
    /// and acks it - it doesn't retry (docs/architecture.md §144).
    #[error("JetStream message payload is not valid JSON: {0}")]
    MalformedPayload(#[from] serde_json::Error),
    /// Reading a consumed message's own `(stream, stream_sequence)` -
    /// only fails for a message that isn't genuinely a JetStream
    /// delivery at all (see `Message::info`'s own doc comment), which
    /// should be unreachable via [`run_inbound`]'s own pull-consumer
    /// source.
    #[error("reading a JetStream message's own stream/sequence info: {0}")]
    MessageInfo(#[source] async_nats::Error),
}

/// Codeberg issue #21 - the backoff state one [`OutboundMapping`]'s own
/// blocked head-of-line event carries across [`produce_once`] calls,
/// threaded in by [`run_outbound`] (one instance per mapping) - the
/// identical shape `skilj_kafka::OutboundRetryState`/
/// `skilj_amqp::OutboundRetryState` already have.
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
    let ack = http
        .post(format!("{skilj_base_url}/v1/events/consume/ack"))
        .bearer_auth(&mapping.credential)
        .json(&serde_json::json!({ "sequence": sequence }))
        .send()
        .await?;
    if !ack.status().is_success() {
        let status = ack.status();
        let body = ack.text().await.unwrap_or_default();
        return Err(BridgeError::SkiljStatus { status, body });
    }
    Ok(())
}

/// Publishes one event to JetStream, then acknowledges it to skilj -
/// never the other order, so a crash between the two redelivers the
/// same event next cycle rather than silently dropping it; the
/// `Nats-Msg-Id` this sets (see this module's own "Redelivery safety"
/// doc section) is what keeps that redelivered publish from landing
/// twice on the JetStream side, for free, via JetStream's own
/// server-side dedup window.
async fn publish_and_ack_one(
    http: &reqwest::Client,
    skilj_base_url: &str,
    jetstream: &Jetstream,
    bounded_context: &str,
    mapping: &OutboundMapping,
    event: &ConsumedEvent,
) -> Result<(), BridgeError> {
    let payload = event.payload.to_string().into_bytes();
    let message_id = format!("{bounded_context}:{}", event.sequence);
    let mut publish = PublishMessage::build()
        .payload(payload.into())
        .message_id(message_id);
    if let Some(key) = correlation_key(mapping.correlation_tag_key.as_deref(), &event.tags) {
        publish = publish.header("Skilj-Correlation-Key", key.as_str());
    }
    // Codeberg issue #18 - always present on correlation_id (the spec's
    // own CorrelationIdIsAlwaysRecorded invariant), absent for a root
    // event's causation_id. Distinct headers from `Skilj-Correlation-Key`
    // above - see this module's own doc comment on `CORRELATION_ID_HEADER`.
    if let Some(id) = &event.metadata.correlation_id {
        publish = publish.header(CORRELATION_ID_HEADER, id.as_str());
    }
    if let Some(id) = &event.metadata.causation_id {
        publish = publish.header(CAUSATION_ID_HEADER, id.as_str());
    }
    jetstream
        .send_publish(mapping.subject.clone(), publish)
        .await?
        .await?;
    ack_event(http, skilj_base_url, mapping, event.sequence).await
}

/// One fetch-publish-ack cycle for a single [`OutboundMapping`] -
/// [`run_outbound`] is just this in a loop. Exposed separately so it can
/// be driven directly in tests, the identical shape
/// `skilj_kafka::produce_once`/`skilj_amqp::produce_once` already have.
/// Returns how many events this cycle actually published (0 when the
/// mapping's own read cursor is already caught up, or when its own head
/// event is still in backoff - see [`OutboundRetryState`]'s own doc
/// comment). A skipped event (Codeberg issue #21 - `retry_policy`
/// exhausted) is acknowledged but not counted here, so a caller checking
/// "did this cycle make real progress" isn't misled into thinking
/// JetStream actually received it.
///
/// A failure publishing *or* acknowledging one event stops this cycle
/// right there - `retry_state` records it, and no event behind it is
/// even attempted this cycle (the identical "the head blocks everything
/// behind it" behaviour `skilj_kafka::produce_once`/
/// `catch_up_cross_context_route` already have, and for the same reason:
/// skipping ahead would silently drop the blocked event from ever being
/// retried, since nothing would ever revisit it once a later one's own
/// ack passes it).
pub async fn produce_once(
    http: &reqwest::Client,
    skilj_base_url: &str,
    jetstream: &Jetstream,
    bounded_context: &str,
    mapping: &OutboundMapping,
    retry_policy: &skilj_retry::RetryPolicy,
    retry_state: &mut Option<OutboundRetryState>,
) -> Result<usize, BridgeError> {
    if let Some(state) = retry_state {
        if Utc::now() < state.next_attempt_at {
            return Ok(0);
        }
    }

    let response = http
        .get(format!("{skilj_base_url}/v1/events/consume?mode=manual"))
        .bearer_auth(&mapping.credential)
        .send()
        .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(BridgeError::SkiljStatus { status, body });
    }
    let consumed: ConsumeResponse = response.json().await?;
    if consumed.event_type_name != mapping.event_type {
        return Err(BridgeError::EventTypeMismatch {
            declared: mapping.event_type.clone(),
            actual: consumed.event_type_name,
        });
    }

    let mut served = 0;
    for event in &consumed.events {
        let key = correlation_key(mapping.correlation_tag_key.as_deref(), &event.tags);
        let owned = owns_partition_for(mapping, key.as_deref());
        let outcome = if owned {
            publish_and_ack_one(
                http,
                skilj_base_url,
                jetstream,
                bounded_context,
                mapping,
                event,
            )
            .await
        } else {
            // Codeberg issue #25's investigation (docs/architecture.md
            // §54) - not this instance's own partition: acknowledged
            // (advancing this instance's own cursor) without ever being
            // published to JetStream - see `OutboundMapping::partition`'s
            // own doc comment.
            ack_event(http, skilj_base_url, mapping, event.sequence).await
        };
        match outcome {
            Ok(()) => {
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
                         (acknowledging without ever publishing it to JetStream) so the \
                         stream isn't blocked forever"
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
                    "publishing/acknowledging this event failed - will retry with backoff"
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
/// the identical shape `skilj_kafka::run_outbound`/`skilj_amqp::run_outbound`
/// already have. `retry_policy` applies to every mapping alike - see
/// [`produce_once`]'s own doc comment and this module's own "Dead-letter/
/// parking" section for what it governs.
pub async fn run_outbound(
    skilj_base_url: &str,
    jetstream: &Jetstream,
    bounded_context: &str,
    mappings: &[OutboundMapping],
    poll_interval: std::time::Duration,
    retry_policy: &skilj_retry::RetryPolicy,
) -> ! {
    run_outbound_until(
        skilj_base_url,
        jetstream,
        bounded_context,
        mappings,
        poll_interval,
        retry_policy,
        std::future::pending(),
    )
    .await;
    unreachable!("run_outbound_until only returns once `stop` resolves, and `pending()` never does")
}

/// [`run_outbound`] until `stop` resolves (docs/architecture.md §129):
/// the cycle in progress - an event being published and acknowledged -
/// completes, then this returns. Aborting the task instead can land
/// between publishing and acknowledging; `Nats-Msg-Id` dedupes a republish
/// only within the stream's duplicate window. `stop` is raced only against
/// the idle sleep between cycles and checked after each cycle.
pub async fn run_outbound_until(
    skilj_base_url: &str,
    jetstream: &Jetstream,
    bounded_context: &str,
    mappings: &[OutboundMapping],
    poll_interval: std::time::Duration,
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
                jetstream,
                bounded_context,
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
            std::time::Duration::ZERO
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

// --- inbound: NATS JetStream -> skilj ---

/// One NATS subject's own mapping to a skilj action - the inbound half,
/// the identical shape `skilj_kafka::InboundMapping`/
/// `skilj_amqp::InboundMapping` already have. Which *subject* this
/// applies to is implicit in whichever [`PullConsumer`] [`run_inbound`]
/// is given - a JetStream consumer is already bound to its own stream
/// and subject filter at creation, unlike an `rdkafka` consumer that
/// can subscribe to several topics at once, so there's no
/// per-address lookup table to keep here the way `skilj_kafka::run_inbound`
/// needs one.
pub struct InboundMapping {
    /// A credential scoped to whichever `action` needs it -
    /// `ExternalEventToken` for `Record`, `CommandToken` for `Trigger`.
    pub credential: String,
    pub action: InboundAction,
}

/// Which skilj call a mapped subject's own messages become - neither
/// variant's own `event_type`/`command_type` field is sent on the wire,
/// the identical "derived from the credential, not a body field"
/// reasoning every other bridge in this workspace already documents.
pub enum InboundAction {
    /// `POST /v1/events/external` - record the message verbatim as a
    /// fact. Always redelivery-safe via the `dedupe` mechanism
    /// ([docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event)) - JetStream's own `(stream,
    /// stream_sequence)` is broker-assigned on every message, never
    /// optional the way AMQP's `group-id`/`group-sequence` are.
    Record { event_type: String },
    /// `POST /v1/commands/trigger` - decide on the message via a real
    /// `decide()`. Redelivery-safe via `Idempotency-Key` ([§21](../../docs/architecture.md#optional-idempotency-key-submission), itself
    /// `client_id`-scoped since [§37](../../docs/architecture.md#idempotency-keys-client-id-scoping)) *only when* the message carries a
    /// `Nats-Msg-Id` - see [`InboundMessageMeta`]'s own doc comment.
    Trigger { command_type: String },
}

impl InboundAction {
    /// The `kind` `POST /v1/parked-deliveries` expects - see
    /// `skilj-rest::routes::ParkedDeliveryKindRequest`'s own identical
    /// two variants, and `skilj_kafka::InboundAction::parked_delivery_kind`/
    /// `skilj_amqp::InboundAction::parked_delivery_kind`.
    fn parked_delivery_kind(&self) -> &'static str {
        match self {
            InboundAction::Record { .. } => "external_event",
            InboundAction::Trigger { .. } => "command_trigger",
        }
    }
}

/// The subset of a consumed JetStream message's own metadata this
/// bridge actually uses for redelivery safety. Unlike `skilj_amqp::InboundMessageMeta`,
/// `stream`/`stream_sequence` are never optional here - every message a
/// [`PullConsumer`] ever delivers carries real values for both
/// (`Message::info`, confirmed against the real `Info` struct). Only
/// `message_id` (from the sender-optional `Nats-Msg-Id` header) mirrors
/// AMQP's own "may or may not be populated" story.
#[derive(Debug, Clone)]
pub struct InboundMessageMeta {
    pub stream: String,
    pub stream_sequence: u64,
    pub message_id: Option<String>,
    /// [`CORRELATION_ID_HEADER`]/[`CAUSATION_ID_HEADER`]'s own values
    /// (Codeberg issue #18) - both sender-optional, the same
    /// "may or may not be populated" story `message_id` above already
    /// has.
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl InboundMessageMeta {
    /// Reads a real consumed message's own metadata - the one place
    /// `Message::info()` is called, so [`dispatch_inbound_message`]
    /// itself can be tested against a plain struct without needing a
    /// real JetStream delivery. `pub` (unlike the equivalent internal
    /// step in `skilj_kafka`/`skilj_amqp`, which build their own
    /// `InboundMessageMeta`-shaped values inline in `run_inbound`)
    /// because a real JetStream `Message` needs this crate's own
    /// `Message::info()` call to read - a caller with a real delivery
    /// in hand (a test, or code driving `dispatch_inbound_message`
    /// outside `run_inbound`'s own loop) has no other way to build one.
    pub fn from_message(message: &JetstreamMessage) -> Result<Self, BridgeError> {
        let info = message.info().map_err(BridgeError::MessageInfo)?;
        let message_id = message
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Msg-Id"))
            .map(|v| v.to_string());
        let correlation_id = message
            .headers
            .as_ref()
            .and_then(|h| h.get(CORRELATION_ID_HEADER))
            .map(|v| v.to_string());
        let causation_id = message
            .headers
            .as_ref()
            .and_then(|h| h.get(CAUSATION_ID_HEADER))
            .map(|v| v.to_string());
        Ok(InboundMessageMeta {
            stream: info.stream.to_string(),
            stream_sequence: info.stream_sequence,
            message_id,
            correlation_id,
            causation_id,
        })
    }
}

/// The `payload` a parked delivery's stored request carries: the message
/// as JSON, or - when it isn't JSON at all - its raw content as a JSON
/// string, so an operator can see what arrived rather than a `null`
/// (docs/architecture.md §144). A retry of such a row sends that string,
/// which no event or command schema accepts, so it stays parked until
/// discarded.
fn parked_payload(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(payload).into_owned())
    })
}

/// Dispatches one JetStream message to skilj - the one place
/// [`InboundAction`] is interpreted, the identical shape
/// `skilj_kafka::dispatch_inbound_message`/`skilj_amqp::dispatch_inbound_message`
/// already have. Exposed separately from [`run_inbound`] so it can be
/// tested directly against a plain [`InboundMessageMeta`] and raw
/// payload bytes, without needing a real JetStream delivery.
///
/// `stream_sequence` is cast from JetStream's own `u64` to the `i64`
/// `dedupe.sequence` expects - checked, not wrapped: a stream whose own
/// sequence has (astronomically improbably) exceeded `i64::MAX` omits
/// `dedupe` rather than sending a negative, meaningless value, the same
/// "omit rather than wrap" register `skilj_amqp::produce_once` already
/// uses for AMQP 1.0's own narrower 32-bit `group-sequence`.
/// The exact `ExternalEventRequest`/`CommandTriggerRequest` body
/// [`dispatch_inbound_message`] sends for `mapping`/`payload_json`/`meta` -
/// factored out so [`report_parked_delivery`] can store the identical
/// body a `retryParkedDelivery` redrive later needs, without either
/// duplicating this shape or sending a live HTTP request just to build
/// it - the identical role `skilj_kafka::inbound_request_body`/
/// `skilj_amqp::inbound_request_body` already play.
fn inbound_request_body(
    mapping: &InboundMapping,
    meta: &InboundMessageMeta,
    payload_json: &serde_json::Value,
) -> serde_json::Value {
    match &mapping.action {
        InboundAction::Record { .. } => {
            let mut body = serde_json::json!({
                "payload": payload_json,
                "sourceContent": "nats-jetstream",
                "correlationId": meta.correlation_id,
                "causationId": meta.causation_id,
            });
            match i64::try_from(meta.stream_sequence) {
                Ok(sequence) => {
                    body["dedupe"] = serde_json::json!({
                        "partitionKey": meta.stream,
                        "sequence": sequence,
                    });
                }
                Err(_) => {
                    tracing::warn!(
                        stream = meta.stream,
                        stream_sequence = meta.stream_sequence,
                        "stream sequence exceeds i64::MAX - omitting dedupe rather than \
                         sending a wrapped, meaningless value"
                    );
                }
            }
            body
        }
        InboundAction::Trigger { .. } => serde_json::json!({
            "payload": payload_json,
            "correlationId": meta.correlation_id,
            "causationId": meta.causation_id,
        }),
    }
}

/// The `Idempotency-Key` a `Trigger` mapping's request carries: the
/// message's own Nats-Msg-Id, when the sender set one - `None` otherwise,
/// and always for `Record`, which dedupes via its body's `dedupe` cursor.
/// Shared by [`dispatch_inbound_message`] (as the header) and
/// [`report_parked_delivery`] (so `retryParkedDelivery` redrives under
/// the same key, deduping against the original attempt if it committed
/// after all).
fn inbound_idempotency_key(mapping: &InboundMapping, meta: &InboundMessageMeta) -> Option<String> {
    match mapping.action {
        InboundAction::Record { .. } => None,
        InboundAction::Trigger { .. } => meta.message_id.clone(),
    }
}

pub async fn dispatch_inbound_message(
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    meta: &InboundMessageMeta,
    payload: &[u8],
) -> Result<(), BridgeError> {
    let payload_json: serde_json::Value = serde_json::from_slice(payload)?;
    let body = inbound_request_body(mapping, meta, &payload_json);

    let response = match &mapping.action {
        InboundAction::Record { .. } => {
            http.post(format!("{skilj_base_url}/v1/events/external"))
                .bearer_auth(&mapping.credential)
                .json(&body)
                .send()
                .await?
        }
        InboundAction::Trigger { .. } => {
            let mut request = http
                .post(format!("{skilj_base_url}/v1/commands/trigger"))
                .bearer_auth(&mapping.credential)
                .json(&body);
            if let Some(key) = inbound_idempotency_key(mapping, meta) {
                request = request.header("Idempotency-Key", key);
            }
            request.send().await?
        }
    };
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(BridgeError::SkiljStatus { status, body });
    }
    // A `Trigger` rejection is reported as a `200 { accepted: false, ... }`
    // per §5.4/§7.3, not an HTTP error - not inspected here, the
    // identical reasoning every other bridge in this workspace already
    // documents: the message was successfully delivered and decided
    // upon, which is all this bridge ever promises.
    Ok(())
}

/// Codeberg issue #21 - reports a message [`run_inbound`] gave up
/// retrying to skilj's own `POST /v1/parked-deliveries`, using the same
/// credential `mapping` already carries (that route resolves the
/// bounded context from the presented token itself, the identical
/// capability-based design this bridge's every other call already
/// relies on) - the identical role `skilj_kafka::report_parked_delivery`/
/// `skilj_amqp::report_parked_delivery` already play. `identifier` is
/// `"{stream}:{stream_sequence}"` - always available (unlike
/// `Nats-Msg-Id`, sender-optional), the same pair [`inbound_request_body`]
/// already sends as `dedupe` for a `Record` action. `request` is
/// [`inbound_request_body`]'s own output, the exact body that kept
/// failing, stored verbatim so a later `retryParkedDelivery` redrives
/// the identical request.
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
    let response = http
        .post(format!("{skilj_base_url}/v1/parked-deliveries"))
        .bearer_auth(&mapping.credential)
        .json(&serde_json::json!({
            "source": "nats-inbound",
            "kind": mapping.action.parked_delivery_kind(),
            "identifier": identifier,
            "error": error,
            "attemptCount": attempt_count,
            "firstFailedAt": first_failed_at.to_rfc3339(),
            "request": request,
            "idempotencyKey": idempotency_key,
        }))
        .send()
        .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(BridgeError::SkiljStatus { status, body });
    }
    Ok(())
}

/// Runs forever: for every message this `consumer` pulls, dispatches it
/// via [`dispatch_inbound_message`], acking (`Message::ack`) only once
/// that call succeeds - the identical "commit/ack only after skilj
/// confirms" shape `skilj_kafka::run_inbound`/`skilj_amqp::run_inbound`
/// already have.
///
/// Codeberg issue #21: a dispatch failure is retried, *for this one
/// message*, with backoff up to `retry_policy` - not by relying on
/// JetStream's own redelivery, which only replays after this consumer's
/// own ack-wait timeout elapses, not on the very next pulled message in
/// the same session. Once `retry_policy` exhausts, the message is
/// reported to skilj via [`report_parked_delivery`] and acked anyway -
/// without that, a poison message would keep being redelivered by
/// JetStream forever, on every ack-wait timeout. If the *report* itself
/// fails, the message is deliberately left unacked (a real gap - better
/// a loud, visible redelivery loop than a silently unreported poison
/// message).
///
/// `http` should be bounded by a timeout - [`http_client`] is - or one
/// request stuck on a dead connection stalls this loop forever (§82).
pub async fn run_inbound(
    consumer: &PullConsumer,
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    retry_policy: &skilj_retry::RetryPolicy,
) -> ! {
    run_inbound_until(
        consumer,
        http,
        skilj_base_url,
        mapping,
        retry_policy,
        std::future::pending(),
    )
    .await;
    unreachable!("run_inbound_until only returns once `stop` resolves, and `pending()` never does")
}

/// [`run_inbound`] until `stop` resolves (docs/architecture.md §129).
/// `stop` is raced against the wait for the next message (and for the
/// subscription itself) and against the backoff between a failing
/// message's retries - never against a dispatch, report or ack in flight.
/// A message it stops during is left unacknowledged, and JetStream
/// redelivers it after `ack_wait`, where skilj's own dedupe and
/// `Idempotency-Key` make it harmless. Not negatively acknowledged: the
/// stopped loop's own pull request stays registered with the server until
/// it expires, and a NAK's immediate redelivery went into it - a dead
/// inbox - so it came back after `ack_wait` all the same.
pub async fn run_inbound_until(
    consumer: &PullConsumer,
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    retry_policy: &skilj_retry::RetryPolicy,
    stop: impl std::future::Future<Output = ()>,
) {
    let mut stop = std::pin::pin!(stop);
    // docs/architecture.md §100: how often a message being retried is
    // re-claimed - half the consumer's own `ack_wait`, so JetStream never
    // sees it go unacknowledged long enough to redeliver it underneath us.
    let claim_heartbeat =
        (consumer.cached_info().config.ack_wait / 2).max(std::time::Duration::from_millis(100));
    loop {
        let subscribed = tokio::select! {
            biased;
            () = &mut stop => return,
            subscribed = consumer.messages() => subscribed,
        };
        let mut messages = match subscribed {
            Ok(messages) => messages,
            Err(e) => {
                tracing::error!("subscribing to JetStream messages failed, retrying: {e}");
                tokio::select! {
                    biased;
                    () = &mut stop => return,
                    () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                }
                continue;
            }
        };
        loop {
            let next = tokio::select! {
                biased;
                () = &mut stop => return,
                next = messages.try_next() => next,
            };
            let Ok(Some(message)) = next else {
                break;
            };
            let meta = match InboundMessageMeta::from_message(&message) {
                Ok(meta) => meta,
                Err(e) => {
                    tracing::error!("reading a JetStream message's own metadata failed: {e}");
                    continue;
                }
            };

            let mut attempt: u32 = 0;
            let mut first_failed_at: Option<DateTime<Utc>> = None;
            loop {
                match dispatch_inbound_message(
                    http,
                    skilj_base_url,
                    mapping,
                    &meta,
                    &message.payload,
                )
                .await
                {
                    Ok(()) => {
                        if let Err(e) = message.ack().await {
                            tracing::error!("acking a JetStream message failed: {e}");
                        }
                        break;
                    }
                    Err(e) => {
                        attempt += 1;
                        let failed_at = *first_failed_at.get_or_insert_with(Utc::now);
                        let elapsed = (Utc::now() - failed_at).to_std().unwrap_or_default();
                        // Not JSON: no retry can change that (§144).
                        if retry_policy.is_exhausted(attempt, elapsed)
                            || matches!(e, BridgeError::MalformedPayload(_))
                        {
                            tracing::error!(
                                stream = meta.stream,
                                stream_sequence = meta.stream_sequence,
                                attempt,
                                error = %e,
                                "dispatch failed repeatedly - parking and acking so this \
                                 message doesn't get redelivered forever"
                            );
                            let payload_json = parked_payload(&message.payload);
                            let body = inbound_request_body(mapping, &meta, &payload_json);
                            let identifier = format!("{}:{}", meta.stream, meta.stream_sequence);
                            let idempotency_key = inbound_idempotency_key(mapping, &meta);
                            if let Err(report_err) = report_parked_delivery(
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
                                tracing::error!(
                                    stream = meta.stream,
                                    stream_sequence = meta.stream_sequence,
                                    "reporting this parked delivery failed - not acking, \
                                     will redeliver: {report_err}"
                                );
                                break;
                            }
                            if let Err(ack_err) = message.ack().await {
                                tracing::error!("acking a JetStream message failed: {ack_err}");
                            }
                            break;
                        }
                        let backoff = retry_policy.next_backoff(attempt);
                        tracing::warn!(
                            stream = meta.stream,
                            stream_sequence = meta.stream_sequence,
                            attempt,
                            error = %e,
                            "dispatch failed - retrying after backoff"
                        );
                        // Stopping here leaves the message unacknowledged:
                        // JetStream redelivers it after `ack_wait`, as after
                        // a crash (docs/architecture.md §129).
                        tokio::select! {
                            biased;
                            () = &mut stop => return,
                            () = wait_keeping_claim(&message, backoff, claim_heartbeat) => {}
                        }
                    }
                }
            }
        }
    }
}

/// Sleeps `total` while keeping `message` claimed: an in-progress ack
/// (`AckKind::Progress`) now and every `heartbeat` after, each of which
/// restarts the consumer's `ack_wait`. Without it, retrying one message for
/// longer than `ack_wait` had JetStream redeliver it meanwhile, and every
/// redelivered copy was dispatched again once the original finished
/// (docs/architecture.md §100). A failed progress ack is only logged: the
/// worst case is that redelivery, which skilj's own dedupe then absorbs.
async fn wait_keeping_claim(
    message: &JetstreamMessage,
    total: std::time::Duration,
    heartbeat: std::time::Duration,
) {
    let deadline = tokio::time::Instant::now() + total;
    loop {
        if let Err(e) = message
            .ack_with(async_nats::jetstream::AckKind::Progress)
            .await
        {
            tracing::warn!("sending an in-progress ack for a JetStream message failed: {e}");
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return;
        }
        tokio::time::sleep(heartbeat.min(deadline - now)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(key: &str, value: &str) -> Tag {
        Tag {
            key: key.to_string(),
            value: Some(value.to_string()),
        }
    }

    #[test]
    fn correlation_key_is_none_when_no_correlation_tag_key_is_configured() {
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

    // --- Codeberg issue #25's investigation (docs/architecture.md §54) ---

    #[test]
    fn owns_partition_for_is_always_true_when_unpartitioned() {
        let mapping = OutboundMapping {
            event_type: "OrderPlaced".to_string(),
            credential: "irrelevant".to_string(),
            subject: "irrelevant".to_string(),
            correlation_tag_key: None,
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
                            subject: "irrelevant".to_string(),
                            correlation_tag_key: None,
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

/// How long one HTTP request to skilj may take, end to end, before it
/// fails and the loop's own retry handling takes over. Without a bound, a
/// request stuck on a half-open connection (a network partition, a
/// stalled proxy) stalled the loop forever with nothing logged
/// (docs/architecture.md §82). Retrying after a timeout is safe: inbound
/// requests carry an idempotency key or dedupe cursor, and an outbound
/// consume's checkout lease covers one that was served but never answered.
pub const HTTP_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The `reqwest::Client` this crate's own loops use: bounded by
/// [`HTTP_REQUEST_TIMEOUT`] and a 10-second connect timeout. Pass it to
/// `run_inbound` too, unless the caller's own client is bounded already.
pub fn http_client() -> reqwest::Client {
    http_client_with_timeout(HTTP_REQUEST_TIMEOUT)
}

fn http_client_with_timeout(timeout: std::time::Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(std::time::Duration::from_secs(10).min(timeout))
        .build()
        .expect("a client with only timeouts configured always builds")
}

#[cfg(test)]
mod http_client_tests {
    /// A server that accepts the connection and never answers must fail
    /// the request within the timeout, not hang the loop (§82).
    #[tokio::test]
    async fn a_stalled_skilj_fails_the_request_instead_of_hanging() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                held.push(socket);
            }
        });
        let client = super::http_client_with_timeout(std::time::Duration::from_millis(200));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client
                .get(format!("http://{addr}/v1/events/consume"))
                .send(),
        )
        .await
        .expect("the request hung past its own timeout");
        assert!(result.unwrap_err().is_timeout());
    }
}
