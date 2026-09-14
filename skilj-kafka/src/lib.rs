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

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

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
}

/// One event served by skilj's own `GET /v1/events/consume` - the subset
/// of `EventDto`'s wire shape (`skilj-rest/src/routes/mod.rs`) this
/// bridge actually needs, the identical shape `skilj_temporal::ConsumedEvent`
/// already uses.
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
/// this bridge actually needs - just the two Codeberg-issue-#18 ids,
/// forwarded onto the outbound Kafka message as headers by [`produce_once`].
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

/// The Kafka message key for a mapped event - `None` when
/// `key_tag_key` is `None`, or when the named tag is absent entirely or
/// present with a `null` value (the payload field it was mapped from was
/// itself absent at write time). Unlike `skilj_temporal::correlation_workflow_id`'s
/// own identically-shaped "nothing to correlate on" case, this is never
/// an error here - an unkeyed message is still a perfectly valid Kafka
/// message, Kafka itself just gets to place it.
pub fn correlation_key(key_tag_key: Option<&str>, tags: &[Tag]) -> Option<String> {
    let key_tag_key = key_tag_key?;
    tags.iter().find(|t| t.key == key_tag_key)?.value.clone()
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
    /// The caller ([`run_inbound`]) still commits the offset on this
    /// error, logging rather than redelivering it forever.
    #[error("Kafka message payload is not valid JSON: {0}")]
    MalformedPayload(#[from] serde_json::Error),
}

/// One fetch-produce-ack cycle for a single [`OutboundMapping`] -
/// [`run_outbound`] is just this in a loop. Exposed separately so it can
/// be driven directly in tests without needing to interrupt a running
/// loop, the same shape `skilj_temporal::poll_once` already has. Returns
/// how many events were served this cycle (0 when the mapping's own read
/// cursor is already caught up).
///
/// Produces to Kafka *before* acknowledging to skilj - never the other
/// order - so a crash between the two redelivers the same event next
/// cycle rather than silently dropping it; `rdkafka`'s own producer
/// idempotence (`enable.idempotence`, set on `producer`'s own
/// `ClientConfig` by the caller, not this function) is what keeps that
/// redelivered produce from landing twice on the Kafka side.
pub async fn produce_once(
    http: &reqwest::Client,
    skilj_base_url: &str,
    producer: &FutureProducer,
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
        let key = correlation_key(mapping.key_tag_key.as_deref(), &event.tags);
        let payload = event.payload.to_string();
        let mut record = FutureRecord::to(&mapping.topic).payload(&payload);
        if let Some(k) = key.as_deref() {
            record = record.key(k);
        }
        // Codeberg issue #18 - forwards the event's own correlation_id/
        // causation_id (always present on correlation_id, per the
        // spec's own CorrelationIdIsAlwaysRecorded invariant; causation_id
        // absent for a root event) as headers, distinct from `key` above.
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
/// the identical shape `skilj_temporal::run` already has, including why
/// a transient failure is logged and retried rather than ending the
/// loop, and why a real deployment runs one [`OutboundMapping`] per task
/// (`tokio::spawn`) rather than calling this with more than one mapping
/// serially.
pub async fn run_outbound(
    skilj_base_url: &str,
    producer: &FutureProducer,
    mappings: &[OutboundMapping],
    poll_interval: Duration,
) -> ! {
    let http = reqwest::Client::new();
    loop {
        let mut served_any = false;
        for mapping in mappings {
            match produce_once(&http, skilj_base_url, producer, mapping).await {
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

// --- inbound: Kafka -> skilj ---

/// One Kafka topic's own mapping to a skilj action - the inbound half,
/// with no equivalent in `skilj-temporal` (which only ever reacts to
/// skilj's own events; a message broker genuinely needs both
/// directions).
pub struct InboundMapping {
    /// A credential scoped to whichever `action` needs it -
    /// `ExternalEventToken` for `Record`, `CommandToken` for `Trigger`.
    pub credential: String,
    pub action: InboundAction,
}

/// Which skilj call a mapped Kafka topic's own messages become. Neither
/// variant's own `event_type`/`command_type` field is sent on the wire -
/// each token is already scoped to exactly one type, the identical
/// "derived from the credential, not a body field" shape `POST
/// /v1/commands/trigger`/`POST /v1/events/external` already have for
/// every other caller - they exist purely so a mapping list reads
/// clearly, not because either call needs them.
pub enum InboundAction {
    /// `POST /v1/events/external` - record the message verbatim as a
    /// fact. Redelivery-safe via the `dedupe` mechanism
    /// ([docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event)) - not `ExternalEventIngestion`'s own
    /// responsibility to invent, since that mechanism already exists for
    /// exactly this.
    Record { event_type: String },
    /// `POST /v1/commands/trigger` - decide on the message via a real
    /// `decide()`. Redelivery-safe via `Idempotency-Key` ([§21](../../docs/architecture.md#optional-idempotency-key-submission)), now
    /// itself `client_id`-scoped ([§37](../../docs/architecture.md#idempotency-keys-client-id-scoping)) so this mapping's own inbound
    /// traffic can never collide with an unrelated caller's.
    Trigger { command_type: String },
}

/// Reads a header's value as UTF-8 text - `None` for a missing header, a
/// header present with no value (Kafka allows this), or one whose bytes
/// aren't valid UTF-8. [`run_inbound`]'s own extraction step for
/// [`CORRELATION_ID_HEADER`]/[`CAUSATION_ID_HEADER`], kept as a free
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

    let response = match &mapping.action {
        InboundAction::Record { .. } => {
            http.post(format!("{skilj_base_url}/v1/events/external"))
                .bearer_auth(&mapping.credential)
                .json(&serde_json::json!({
                    "payload": payload_json,
                    "sourceContent": format!("kafka:{partition_key}:{offset}"),
                    "dedupe": { "partitionKey": partition_key, "sequence": offset },
                    "correlationId": correlation_id,
                    "causationId": causation_id,
                }))
                .send()
                .await?
        }
        InboundAction::Trigger { .. } => {
            http.post(format!("{skilj_base_url}/v1/commands/trigger"))
                .bearer_auth(&mapping.credential)
                .header("Idempotency-Key", format!("{partition_key}:{offset}"))
                .json(&serde_json::json!({
                    "payload": payload_json,
                    "correlationId": correlation_id,
                    "causationId": causation_id,
                }))
                .send()
                .await?
        }
    };
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(BridgeError::SkiljStatus { status, body });
    }
    // A `Trigger` rejection is reported as a `200 { accepted: false, ... }`
    // per §5.4/§7.3, not an HTTP error - deliberately not inspected
    // here: the message itself was successfully delivered and decided
    // upon, which is all this bridge ever promises, so the offset still
    // commits (`run_inbound`) either way. A business rejection is not
    // this bridge's own concern to retry.
    Ok(())
}

/// Runs forever: for every message this `consumer` receives (already
/// subscribed to whatever topics its own caller configured), looks up
/// the [`InboundMapping`] for that message's own topic and calls
/// [`dispatch_inbound_message`], committing the offset (`CommitMode::Async`,
/// via `commit_message` - the caller's own `ClientConfig` must set
/// `enable.auto.commit = false` for this to be the only thing that ever
/// advances it) only once that call succeeds. A dispatch failure is
/// logged, not committed - the identical message is redelivered on the
/// next `recv()` (or after this consumer's own restart), safe because
/// of [`dispatch_inbound_message`]'s own redelivery-safety keys, not
/// because this loop does anything clever. An unmapped topic or an
/// empty payload is logged and skipped, also uncommitted - not silently
/// swallowed, but also not something retrying could ever fix.
pub async fn run_inbound(
    consumer: &StreamConsumer,
    http: &reqwest::Client,
    skilj_base_url: &str,
    mappings: &HashMap<String, InboundMapping>,
) -> ! {
    loop {
        match consumer.recv().await {
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
                match dispatch_inbound_message(
                    http,
                    skilj_base_url,
                    mapping,
                    topic,
                    msg.partition(),
                    msg.offset(),
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
                    }
                    Err(e) => {
                        tracing::error!(
                            topic,
                            partition = msg.partition(),
                            offset = msg.offset(),
                            "dispatch failed, not committing - will redeliver: {e}"
                        );
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
}
