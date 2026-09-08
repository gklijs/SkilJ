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

use async_nats::jetstream::consumer::pull::Config as PullConfig;
use async_nats::jetstream::consumer::Consumer;
use async_nats::jetstream::context::Context as Jetstream;
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::Message as JetstreamMessage;

/// A pull consumer, already bound to its own stream and subject filter
/// at creation - see [`InboundMapping`]'s own doc comment for why
/// [`run_inbound`] doesn't need a lookup table keyed by address the way
/// `skilj_kafka::run_inbound` does.
pub type PullConsumer = Consumer<PullConfig>;
use futures_util::TryStreamExt;
use serde::Deserialize;

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
    /// [`run_inbound`] still acks the message on this error, logging
    /// rather than redelivering it forever.
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

/// One fetch-publish-ack cycle for a single [`OutboundMapping`] -
/// [`run_outbound`] is just this in a loop. Exposed separately so it can
/// be driven directly in tests, the identical shape
/// `skilj_kafka::produce_once`/`skilj_amqp::produce_once` already have.
/// Returns how many events were served this cycle.
///
/// Publishes to JetStream *before* acknowledging to skilj - never the
/// other order - so a crash between the two redelivers the same event
/// next cycle rather than silently dropping it; the `Nats-Msg-Id` this
/// sets (see this module's own "Redelivery safety" doc section) is what
/// keeps that redelivered publish from landing twice on the JetStream
/// side, for free, via JetStream's own server-side dedup window.
pub async fn produce_once(
    http: &reqwest::Client,
    skilj_base_url: &str,
    jetstream: &Jetstream,
    bounded_context: &str,
    mapping: &OutboundMapping,
) -> Result<usize, BridgeError> {
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

    for event in &consumed.events {
        let payload = event.payload.to_string().into_bytes();
        let message_id = format!("{bounded_context}:{}", event.sequence);
        let mut publish = PublishMessage::build()
            .payload(payload.into())
            .message_id(message_id);
        if let Some(key) = correlation_key(mapping.correlation_tag_key.as_deref(), &event.tags) {
            publish = publish.header("Skilj-Correlation-Key", key.as_str());
        }
        jetstream
            .send_publish(mapping.subject.clone(), publish)
            .await?
            .await?;

        let ack = http
            .post(format!("{skilj_base_url}/v1/events/consume/ack"))
            .bearer_auth(&mapping.credential)
            .json(&serde_json::json!({ "sequence": event.sequence }))
            .send()
            .await?;
        if !ack.status().is_success() {
            let status = ack.status();
            let body = ack.text().await.unwrap_or_default();
            return Err(BridgeError::SkiljStatus { status, body });
        }
    }
    Ok(consumed.events.len())
}

/// Runs [`produce_once`] forever, one mapping at a time in the order
/// given, sleeping `poll_interval` between cycles that served nothing -
/// the identical shape `skilj_kafka::run_outbound`/`skilj_amqp::run_outbound`
/// already have.
pub async fn run_outbound(
    skilj_base_url: &str,
    jetstream: &Jetstream,
    bounded_context: &str,
    mappings: &[OutboundMapping],
    poll_interval: std::time::Duration,
) -> ! {
    let http = reqwest::Client::new();
    loop {
        let mut served_any = false;
        for mapping in mappings {
            match produce_once(&http, skilj_base_url, jetstream, bounded_context, mapping).await {
                Ok(served) => served_any |= served > 0,
                Err(e) => {
                    tracing::error!(
                        event_type = %mapping.event_type,
                        "poll cycle failed, retrying after poll_interval: {e}"
                    );
                }
            }
        }
        if !served_any {
            tokio::time::sleep(poll_interval).await;
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
        Ok(InboundMessageMeta {
            stream: info.stream.to_string(),
            stream_sequence: info.stream_sequence,
            message_id,
        })
    }
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
pub async fn dispatch_inbound_message(
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    meta: &InboundMessageMeta,
    payload: &[u8],
) -> Result<(), BridgeError> {
    let payload_json: serde_json::Value = serde_json::from_slice(payload)?;

    let response = match &mapping.action {
        InboundAction::Record { .. } => {
            let mut body = serde_json::json!({
                "payload": payload_json,
                "sourceContent": "nats-jetstream",
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
                .json(&serde_json::json!({ "payload": payload_json }));
            if let Some(id) = &meta.message_id {
                request = request.header("Idempotency-Key", id.as_str());
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

/// Runs forever: for every message this `consumer` pulls, calls
/// [`dispatch_inbound_message`], acking (`Message::ack`) only once that
/// call succeeds - the identical "commit/ack only after skilj confirms"
/// shape `skilj_kafka::run_inbound`/`skilj_amqp::run_inbound` already
/// have. A dispatch failure is logged, not acked - JetStream redelivers
/// the identical message once its own ack-wait timeout elapses, safe
/// because of [`dispatch_inbound_message`]'s own redelivery-safety keys.
pub async fn run_inbound(
    consumer: &PullConsumer,
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
) -> ! {
    loop {
        let mut messages = match consumer.messages().await {
            Ok(messages) => messages,
            Err(e) => {
                tracing::error!("subscribing to JetStream messages failed, retrying: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        while let Ok(Some(message)) = messages.try_next().await {
            let meta = match InboundMessageMeta::from_message(&message) {
                Ok(meta) => meta,
                Err(e) => {
                    tracing::error!("reading a JetStream message's own metadata failed: {e}");
                    continue;
                }
            };
            match dispatch_inbound_message(http, skilj_base_url, mapping, &meta, &message.payload)
                .await
            {
                Ok(()) => {
                    if let Err(e) = message.ack().await {
                        tracing::error!("acking a JetStream message failed: {e}");
                    }
                }
                Err(e) => {
                    tracing::error!(
                        stream = meta.stream,
                        stream_sequence = meta.stream_sequence,
                        "dispatch failed, not acking - will redeliver: {e}"
                    );
                }
            }
        }
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
}
