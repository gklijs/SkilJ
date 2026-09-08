//! A bridge between skilj's own event stream and any AMQP 1.0 broker
//! (Solace PubSub+, Azure Service Bus, ActiveMQ Artemis, and any other
//! AMQP 1.0-compliant broker) - [docs/architecture.md §41](../../docs/architecture.md#skilj-amqp-bridge),
//! `skilj-kafka`'s ([§40](../../docs/architecture.md#skilj-kafka-bridge)) sibling for a genuinely different delivery
//! model. Wire-protocol client only, on both sides: zero dependency on
//! any other skilj crate, the same "independently usable" posture
//! `skilj-tui`/`skilj-temporal`/`skilj-kafka` already have.
//!
//! # Why one AMQP 1.0 bridge, not one per vendor
//!
//! Solace/Azure Service Bus/Artemis each speak AMQP 1.0 as a
//! first-class standard protocol alongside their own proprietary ones -
//! one dependency (`fe2o3-amqp`, pure Rust, no C dependency) covers all
//! three, and any other compliant broker, rather than a weaker
//! vendor-specific crate per target. [docs/architecture.md §38](../../docs/architecture.md#message-broker-bridges-investigation)'s own
//! investigation found no healthy Solace-only Rust crate exists at
//! all - the unofficial `solace-rs` needs Solace's own proprietary C
//! SDK installed separately and is essentially unused.
//!
//! # A genuinely different delivery model than Kafka
//!
//! AMQP 1.0 has no partitions or broker-assigned offsets - a queue/
//! topic *address* instead, and messages that carry only whatever
//! metadata their own sender chose to set. The closest AMQP 1.0
//! analogue of Kafka's own broker-assigned `(topic, partition, offset)`
//! is the sender-populated `group-id`/`group-sequence` message
//! properties pair (AMQP 1.0 §3.2.4,
//! `fe2o3_amqp_types::messaging::Properties`), standard but *opt-in*:
//! nothing forces a sender to set them, unlike Kafka's own
//! broker-guaranteed offsets. [`produce_once`] (this crate's own
//! outbound half) always sets them; [`dispatch_inbound_message`] (the
//! inbound half) can only use them for redelivery-safety when whatever
//! upstream sender populated a given address did too - see
//! [`InboundMessageMeta`]'s own doc comment for exactly what happens
//! when it didn't (never an error - the same "omitting it changes
//! nothing, just isn't redelivery-safe" register skilj's own
//! `Idempotency-Key`/`dedupe` already have when omitted).
//!
//! # Correlation (outbound)
//!
//! `group-id` is derived from one of the event's own DCB tags, the
//! identical "derived from a tag, not a fresh concept" register
//! `skilj_kafka::correlation_key`/`skilj_temporal::correlation_workflow_id`
//! already use. `group-sequence` is the event's own skilj sequence
//! number - always real and available, since skilj (not the broker)
//! assigns it. AMQP 1.0's `group-sequence` is a 32-bit field
//! (`fe2o3_amqp_types::definitions::SequenceNo = u32`) - a real
//! protocol-level limit, not a library choice - so a bounded context
//! whose own sequence has exceeded `u32::MAX` omits it rather than
//! wrapping into a meaningless value; see [`produce_once`]'s own doc
//! comment.
//!
//! # Redelivery safety (inbound)
//!
//! [`InboundAction::Record`] uses `group-id`/`group-sequence` (when
//! both are present) as [docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event)'s `dedupe` pair - the
//! identical mechanism `skilj-kafka`'s own `"{topic}:{partition}"`/
//! offset pair feeds. [`InboundAction::Trigger`] uses `message-id`
//! (when present) as `Idempotency-Key` ([§21](../../docs/architecture.md#optional-idempotency-key-submission)) - a broader, more commonly
//! populated standard property (most senders set it, often to a UUID),
//! usable on its own without needing the ordering `group-sequence`
//! implies. The AMQP delivery itself is only accepted
//! (`Receiver::accept`, [`run_inbound`]) after skilj confirms the call
//! succeeded, so a redelivery (a consumer crash before its own accept
//! lands) calls skilj again rather than silently skipping the message.

use fe2o3_amqp::link::{RecvError, SendError};
use fe2o3_amqp::types::messaging::{Data, Message, MessageId, Properties};
use fe2o3_amqp::{Receiver as AmqpReceiver, Sender as AmqpSender};
use serde::Deserialize;
use std::collections::HashMap;

// --- outbound: skilj event -> AMQP ---

/// One skilj `EventType`'s own mapping to an AMQP address (a queue or
/// topic name, broker-specific in shape but always just a string to
/// AMQP itself) - the outbound half, direct analogue of
/// `skilj_kafka::OutboundMapping`/`skilj_temporal::EventTypeMapping`.
pub struct OutboundMapping {
    /// The `EventType::NAME` this mapping applies to.
    pub event_type: String,
    /// An `EventReadToken` credential (`"{id}.{secret}"`) scoped to this
    /// event type.
    pub credential: String,
    pub address: String,
    /// Which of this event type's own tag keys becomes the message's
    /// own `group-id` - `None` sends no `group-id` at all (a message
    /// with no group is still perfectly valid AMQP). See
    /// [`correlation_key`].
    pub key_tag_key: Option<String>,
}

/// One event served by skilj's own `GET /v1/events/consume` - the
/// identical shape `skilj_kafka::ConsumedEvent`/`skilj_temporal::ConsumedEvent`
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

/// The AMQP `group-id` for a mapped event - `None` when `key_tag_key`
/// is `None`, or when the named tag is absent entirely or present with
/// a `null` value. Never an error here, the same "an unkeyed message is
/// still perfectly valid" register `skilj_kafka::correlation_key`
/// already uses.
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
    #[error("sending an AMQP message: {0}")]
    Send(#[from] SendError),
    #[error("receiving an AMQP message: {0}")]
    Recv(#[from] RecvError),
    /// The broker settled the delivery as something other than
    /// `Accepted` (`Rejected`/`Released`/`Modified`) - this bridge
    /// treats any non-`Accepted` outcome as a send failure, the same
    /// "the produce itself didn't land" register a Kafka
    /// `MessageTimedOut` gets in `skilj-kafka`.
    #[error("AMQP broker did not accept the message: {0:?}")]
    NotAccepted(fe2o3_amqp_types::messaging::Outcome),
    /// `OutboundMapping::event_type` doesn't match what the
    /// `EventReadToken` given as its own `credential` actually serves -
    /// the identical check `skilj_kafka::produce_once`/
    /// `skilj_temporal::poll_once` already make.
    #[error(
        "OutboundMapping declares event_type \"{declared}\", but its own credential is scoped \
         to \"{actual}\" - wrong token for this mapping"
    )]
    EventTypeMismatch { declared: String, actual: String },
    /// An AMQP message's own body wasn't valid JSON - unrecoverable the
    /// same way `skilj_kafka::BridgeError::MalformedPayload` is: no
    /// amount of retrying produces valid JSON out of bytes that aren't.
    /// [`run_inbound`] still accepts the delivery on this error, logging
    /// rather than redelivering it forever.
    #[error("AMQP message body is not valid JSON: {0}")]
    MalformedPayload(#[from] serde_json::Error),
}

/// One fetch-produce-ack cycle for a single [`OutboundMapping`] -
/// [`run_outbound`] is just this in a loop. Exposed separately so it can
/// be driven directly in tests, the identical shape
/// `skilj_kafka::produce_once`/`skilj_temporal::poll_once` already have.
/// Returns how many events were served this cycle.
///
/// Sends to AMQP *before* acknowledging to skilj - never the other
/// order - so a crash between the two redelivers the same event next
/// cycle rather than silently dropping it. `group-sequence` is this
/// event's own skilj sequence number, cast to AMQP 1.0's own 32-bit
/// field - `None` (omitted, not wrapped) if it doesn't fit, logged as a
/// real, if distant, protocol-level limit rather than silently
/// producing a meaningless wrapped value a downstream consumer might
/// mistake for a genuine ordering.
pub async fn produce_once(
    http: &reqwest::Client,
    skilj_base_url: &str,
    sender: &mut AmqpSender,
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
        let group_id = correlation_key(mapping.key_tag_key.as_deref(), &event.tags);
        let group_sequence = match u32::try_from(event.sequence) {
            Ok(seq) => Some(seq),
            Err(_) => {
                tracing::warn!(
                    sequence = event.sequence,
                    "event sequence exceeds AMQP 1.0's own 32-bit group-sequence field - \
                     omitting it rather than wrapping into a meaningless value"
                );
                None
            }
        };
        let payload = event.payload.to_string().into_bytes();
        let message = Message::builder()
            .properties(
                Properties::builder()
                    .group_id(group_id)
                    .group_sequence(group_sequence)
                    .build(),
            )
            .data(payload)
            .build();
        sender
            .send(message)
            .await?
            .accepted_or_else(BridgeError::NotAccepted)?;

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
/// the identical shape `skilj_kafka::run_outbound`/`skilj_temporal::run`
/// already have, including why a transient failure is logged and
/// retried rather than ending the loop.
pub async fn run_outbound(
    skilj_base_url: &str,
    sender: &mut AmqpSender,
    mappings: &[OutboundMapping],
    poll_interval: std::time::Duration,
) -> ! {
    let http = reqwest::Client::new();
    loop {
        let mut served_any = false;
        for mapping in mappings {
            match produce_once(&http, skilj_base_url, sender, mapping).await {
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

// --- inbound: AMQP -> skilj ---

/// One AMQP address's own mapping to a skilj action - the inbound half,
/// no equivalent in `skilj-temporal`, the identical shape
/// `skilj_kafka::InboundMapping` already has.
pub struct InboundMapping {
    /// A credential scoped to whichever `action` needs it -
    /// `ExternalEventToken` for `Record`, `CommandToken` for `Trigger`.
    pub credential: String,
    pub action: InboundAction,
}

/// Which skilj call a mapped AMQP address's own messages become -
/// neither variant's own `event_type`/`command_type` field is sent on
/// the wire, the identical "derived from the credential, not a body
/// field" reasoning `skilj_kafka::InboundAction` already documents.
pub enum InboundAction {
    /// `POST /v1/events/external` - record the message verbatim as a
    /// fact. Redelivery-safe via the `dedupe` mechanism
    /// ([docs/architecture.md §39](../../docs/architecture.md#external-message-dedup-create-external-event)) *only when* the message carries both
    /// `group-id` and `group-sequence` - see [`InboundMessageMeta`]'s
    /// own doc comment for what happens when it doesn't.
    Record { event_type: String },
    /// `POST /v1/commands/trigger` - decide on the message via a real
    /// `decide()`. Redelivery-safe via `Idempotency-Key` ([§21](../../docs/architecture.md#optional-idempotency-key-submission), itself
    /// `client_id`-scoped since [§37](../../docs/architecture.md#idempotency-keys-client-id-scoping)) *only when* the message carries a
    /// `message-id` - see [`InboundMessageMeta`]'s own doc comment.
    Trigger { command_type: String },
}

/// The subset of an inbound AMQP message's own `Properties` this bridge
/// actually uses for redelivery safety - unlike `skilj-kafka`'s
/// `(topic, partition, offset)`, which the broker always supplies,
/// every field here is genuinely optional: AMQP 1.0 defines these as
/// standard properties, but nothing forces a sender to populate them.
///
/// **When a field is absent**: `dispatch_inbound_message` simply omits
/// the corresponding skilj mechanism rather than inventing a value or
/// refusing the message - a `Record` action with no `group_id`/
/// `group_sequence` pair calls `ExternalEventIngestion` without
/// `dedupe` (created every time, exactly as if this crate's own dedupe
/// support didn't exist - `ExternalEventIngestion`'s own
/// `OmittingTheDedupePairChangesNothing` guarantee, [§39](../../docs/architecture.md#external-message-dedup-create-external-event)); a `Trigger`
/// action with no `message_id` calls `CommandTrigger` without
/// `Idempotency-Key` (accepted, just not redelivery-safe - `submitCommand`'s
/// own long-standing "omitting it is always fine" behaviour, [§21](../../docs/architecture.md#optional-idempotency-key-submission)).
/// Never an error - a message lacking metadata this bridge would like
/// to have is still a message worth delivering.
#[derive(Debug, Default, Clone)]
pub struct InboundMessageMeta {
    pub group_id: Option<String>,
    pub group_sequence: Option<u32>,
    pub message_id: Option<MessageId>,
}

/// `MessageId` has no single canonical string form in the AMQP 1.0 spec
/// itself (it's a union of `ulong`/`uuid`/`binary`/`string`) - this
/// bridge's own `Idempotency-Key` derivation needs one stable string
/// regardless of which variant a given sender chose, so each variant is
/// rendered via its own natural textual form (`Display`/hex, not
/// re-parsed or round-tripped) - stable across a redelivery of the
/// identical message (the same `MessageId` value renders identically
/// every time), which is the only property `submitCommand`'s own
/// idempotency mechanism actually needs from it.
fn message_id_to_string(id: &MessageId) -> String {
    match id {
        MessageId::Ulong(v) => v.to_string(),
        MessageId::Uuid(v) => v.as_inner().iter().map(|b| format!("{b:02x}")).collect(),
        MessageId::Binary(v) => {
            let bytes: &[u8] = v.as_ref();
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }
        MessageId::String(v) => v.clone(),
    }
}

/// Dispatches one AMQP message to skilj - the one place [`InboundAction`]
/// is interpreted, the identical shape `skilj_kafka::dispatch_inbound_message`
/// already has. Exposed separately from [`run_inbound`] so it can be
/// tested directly against a plain [`InboundMessageMeta`] and raw
/// payload bytes, without needing a real `fe2o3_amqp` delivery object.
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
            let dedupe = match (&meta.group_id, meta.group_sequence) {
                (Some(partition_key), Some(sequence)) => Some(serde_json::json!({
                    "partitionKey": partition_key,
                    "sequence": sequence,
                })),
                _ => None,
            };
            let mut body = serde_json::json!({
                "payload": payload_json,
                "sourceContent": "amqp",
            });
            if let Some(dedupe) = dedupe {
                body["dedupe"] = dedupe;
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
                request = request.header("Idempotency-Key", message_id_to_string(id));
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
    // identical reasoning `skilj_kafka::dispatch_inbound_message`
    // already documents: the message was successfully delivered and
    // decided upon, which is all this bridge ever promises.
    Ok(())
}

/// Runs forever: for every message this `receiver` receives (already
/// attached to whatever address its own caller configured), looks up
/// the [`InboundMapping`] for that address and calls
/// [`dispatch_inbound_message`], accepting the delivery
/// (`Receiver::accept`) only once that call succeeds - the identical
/// "commit/ack only after skilj confirms" shape `skilj_kafka::run_inbound`
/// already has. A dispatch failure is logged, not accepted - the
/// identical message is redelivered on reconnect, safe because of
/// [`dispatch_inbound_message`]'s own redelivery-safety keys when
/// present, or simply re-decided/re-recorded harmlessly when absent (a
/// keyless `Trigger`/`Record` redelivery behaves exactly as any other
/// keyless submission already does).
///
/// One `receiver` per mapping's own address, the same "caller's own
/// process decides how many links/tasks to run" register
/// `skilj_kafka::run_inbound`'s own doc comment already establishes for
/// `OutboundMapping` - `mappings` here is keyed by address purely so a
/// single receiver whose own source address matches more than one
/// configured mapping (unusual, but not prevented by AMQP itself) has a
/// defined lookup, not because this function itself multiplexes several
/// receivers.
pub async fn run_inbound(
    receiver: &mut AmqpReceiver,
    http: &reqwest::Client,
    skilj_base_url: &str,
    address: &str,
    mappings: &HashMap<String, InboundMapping>,
) -> ! {
    loop {
        let delivery = match receiver.recv::<Data>().await {
            Ok(delivery) => delivery,
            Err(e) => {
                tracing::error!(address, "AMQP receive error: {e}");
                continue;
            }
        };
        let Some(mapping) = mappings.get(address) else {
            tracing::warn!(
                address,
                "message on an unmapped address - skipping, not accepting"
            );
            continue;
        };
        let properties = delivery.message().properties.as_ref();
        let meta = InboundMessageMeta {
            group_id: properties.and_then(|p| p.group_id.clone()),
            group_sequence: properties.and_then(|p| p.group_sequence),
            message_id: properties.and_then(|p| p.message_id.clone()),
        };
        let payload = delivery.body().0.as_ref();
        match dispatch_inbound_message(http, skilj_base_url, mapping, &meta, payload).await {
            Ok(()) => {
                if let Err(e) = receiver.accept(&delivery).await {
                    tracing::error!("accepting an AMQP delivery failed: {e}");
                }
            }
            Err(e) => {
                tracing::error!(
                    address,
                    "dispatch failed, not accepting - will redeliver: {e}"
                );
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
    fn message_id_to_string_is_stable_for_each_variant() {
        assert_eq!(message_id_to_string(&MessageId::Ulong(42)), "42");
        assert_eq!(
            message_id_to_string(&MessageId::String("abc".to_string())),
            "abc"
        );
        assert_eq!(
            message_id_to_string(&MessageId::Binary(vec![0xde, 0xad].into())),
            "dead"
        );
    }

    #[test]
    fn message_id_to_string_is_stable_across_a_simulated_redelivery() {
        let first = message_id_to_string(&MessageId::Ulong(7));
        let redelivered = message_id_to_string(&MessageId::Ulong(7));
        assert_eq!(first, redelivered);
    }
}
