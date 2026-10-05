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
//! [`InboundAction::Record`] sends `"{group-id}:{group-sequence}"` (when
//! both are present), or else `message-id`, as its `Idempotency-Key` - a
//! per-message key ([docs/architecture.md §175](../../docs/architecture.md#external-message-keys)), not [§39](../../docs/architecture.md#external-message-dedup-create-external-event)'s `dedupe`
//! watermark: a released or unsettled message comes back after
//! deliveries the link already holds, and with competing consumers in
//! any order, so a per-group watermark would drop it as already seen.
//! [`InboundAction::Trigger`] uses `message-id`
//! (when present) as `Idempotency-Key` ([§21](../../docs/architecture.md#optional-idempotency-key-submission)) - a broader, more commonly
//! populated standard property (most senders set it, often to a UUID),
//! usable on its own without needing the ordering `group-sequence`
//! implies. The AMQP delivery itself is only accepted
//! (`Receiver::accept`, [`run_inbound`]) after skilj confirms the call
//! succeeded, so a redelivery (a consumer crash before its own accept
//! lands) calls skilj again rather than silently skipping the message.
//!
//! # Dead-letter/parking (Codeberg issue #21)
//!
//! Both directions apply a [`skilj_retry::RetryPolicy`], the identical
//! shape `skilj-kafka` uses - see that crate's own "Dead-letter/parking"
//! doc section and docs/architecture.md's parked-deliveries section for
//! the full design, only summarised here:
//!
//! - **Inbound**: a message that keeps failing to dispatch is retried
//!   with backoff, *for that one message*, before this receiver ever
//!   calls `recv()` again ([`run_inbound`]) - a purely local, in-process
//!   retry loop, not tracked via any broker-reported delivery/redelivery
//!   count (AMQP 1.0 has no such standard property this bridge already
//!   reads - see [`InboundMessageMeta`]'s own doc comment on how sparse
//!   its own metadata already is). Once the policy exhausts, the message
//!   is reported to skilj's own `POST /v1/parked-deliveries`
//!   (`report_parked_delivery`) and the delivery is accepted anyway -
//!   without that, a poison message would block redelivery forever on
//!   whichever broker-side redelivery/ack-timeout mechanism applies.
//! - **Outbound**: an event that keeps failing to send is retried with
//!   backoff across [`produce_once`] calls (state threaded through
//!   [`OutboundRetryState`]) - but the default policy is
//!   [`skilj_retry::RetryPolicy::unbounded`], not bounded: a broker
//!   outage should self-heal once it's back, not give up. If a caller
//!   configures a bounded policy anyway, exhausting it skips that one
//!   event (acknowledges it to skilj without ever sending it) and moves
//!   on, logged loudly - no parked-delivery record, since there is
//!   nothing wrong with the *message*, only (temporarily) with reaching
//!   the broker.

use chrono::{DateTime, Utc};
use fe2o3_amqp::link::{RecvError, SendError};
use fe2o3_amqp::types::messaging::{ApplicationProperties, Data, Message, MessageId, Properties};
use fe2o3_amqp::types::primitives::SimpleValue;
use fe2o3_amqp::{Receiver as AmqpReceiver, Sender as AmqpSender};
use std::collections::HashMap;

// docs/architecture.md §165: the skilj side every bridge shares.
use skilj_bridge::parked_payload;
pub use skilj_bridge::{
    correlation_key, http_client, ConsumedEvent, ConsumedEventMetadata, InboundAction,
    InboundMapping, OutboundRetryState, Tag, HTTP_REQUEST_TIMEOUT,
};

/// The `application-properties` key this bridge uses for causation_id
/// (Codeberg issue #18) - AMQP 1.0's standard `Properties` has a real
/// `correlation-id` field (used directly, see [`produce_once`]/
/// [`InboundMessageMeta::correlation_id`]) but no causation equivalent,
/// so this rides in the message's own free-form application properties
/// instead, the same register `skilj-kafka`/`skilj-nats` use a custom
/// header for the identical reason.
const CAUSATION_ID_PROPERTY: &str = "Skilj-Causation-Id";

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
    /// `Some((partition_index, partition_count))` splits this
    /// `EventType`'s own outbound work across `partition_count`
    /// independent bridge instances (Codeberg issue #25's investigation,
    /// docs/architecture.md §54) - the identical mechanism
    /// `skilj_kafka::OutboundMapping::partition` establishes; see that
    /// field's own doc comment for the full design (each partition uses
    /// its own dedicated `credential`, no new coordination needed beyond
    /// §53's own per-token checkout). `None` (the default) means
    /// unpartitioned.
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
    /// [`run_inbound`] parks it on the first failure, raw content kept,
    /// and accepts it - it doesn't retry (docs/architecture.md §144).
    #[error("AMQP message body is not valid JSON: {0}")]
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

/// AMQP's own half of [`skilj_bridge::outbound_cycle`]: sends one event
/// to the mapping's address. `group-sequence` is the event's own skilj
/// sequence number, cast to AMQP 1.0's own 32-bit field - `None`
/// (omitted, not wrapped) if it doesn't fit, logged as a real, if
/// distant, protocol-level limit rather than silently producing a
/// meaningless wrapped value a downstream consumer might mistake for a
/// genuine ordering.
struct AmqpSink<'a> {
    sender: &'a mut AmqpSender,
    mapping: &'a OutboundMapping,
}

impl skilj_bridge::OutboundSink for AmqpSink<'_> {
    type Error = BridgeError;

    fn broker_name(&self) -> &'static str {
        "AMQP"
    }

    async fn deliver(&mut self, event: &ConsumedEvent) -> Result<(), BridgeError> {
        let group_id = correlation_key(self.mapping.key_tag_key.as_deref(), &event.tags);
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
        // Codeberg issue #18 - `correlation-id` is a real AMQP 1.0 standard
        // property (`Properties::correlation_id`), set directly;
        // `causation_id` has no standard equivalent, so it rides in
        // `application-properties` instead (see `CAUSATION_ID_PROPERTY`'s
        // own doc comment). correlation_id is always present by this point
        // (the spec's own CorrelationIdIsAlwaysRecorded invariant);
        // causation_id is absent for a root event.
        let mut properties_builder = Properties::builder()
            .group_id(group_id)
            .group_sequence(group_sequence);
        if let Some(id) = event.metadata.correlation_id.clone() {
            properties_builder = properties_builder.correlation_id(MessageId::String(id));
        }
        let application_properties = event.metadata.causation_id.as_deref().map(|id| {
            ApplicationProperties::builder()
                .insert(CAUSATION_ID_PROPERTY, id)
                .build()
        });
        let message = Message::builder()
            .properties(properties_builder.build())
            .application_properties(application_properties)
            .data(payload)
            .build();
        self.sender
            .send(message)
            .await?
            .accepted_or_else(BridgeError::NotAccepted)?;
        Ok(())
    }
}

/// One fetch-send-ack cycle for a single [`OutboundMapping`] -
/// [`run_outbound`] is just this in a loop. Exposed separately so it can
/// be driven directly in tests, the identical shape
/// `skilj_kafka::produce_once`/`skilj_temporal::poll_once` already have.
/// See [`skilj_bridge::outbound_cycle`] for what a cycle does: events are
/// sent strictly in sequence order, one at a time, and acknowledged once
/// per contiguous run; it returns how many were sent and acknowledged. An
/// event is acknowledged only after the broker accepted it, so a crash in
/// between sends it again next cycle.
pub async fn produce_once(
    http: &reqwest::Client,
    skilj_base_url: &str,
    sender: &mut AmqpSender,
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
    let mut sink = AmqpSink { sender, mapping };
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
/// the identical shape `skilj_kafka::run_outbound`/`skilj_temporal::run`
/// already have. `retry_policy` applies to every mapping alike - see
/// [`produce_once`]'s own doc comment and this crate's own "Dead-letter/
/// parking" section for what it governs.
pub async fn run_outbound(
    skilj_base_url: &str,
    sender: &mut AmqpSender,
    mappings: &[OutboundMapping],
    poll_interval: std::time::Duration,
    retry_policy: &skilj_retry::RetryPolicy,
) -> ! {
    run_outbound_until(
        skilj_base_url,
        sender,
        mappings,
        poll_interval,
        retry_policy,
        std::future::pending(),
    )
    .await;
    unreachable!("run_outbound_until only returns once `stop` resolves, and `pending()` never does")
}

/// [`run_outbound`] until `stop` resolves (docs/architecture.md §129):
/// the cycle in progress - an event being sent and acknowledged -
/// completes, then this returns. Aborting the task instead can land
/// between sending and acknowledging, and the event is sent again on the
/// next start: a duplicate for the address's consumers. `stop` is raced
/// only against the idle sleep between cycles and checked after each
/// cycle.
pub async fn run_outbound_until(
    skilj_base_url: &str,
    sender: &mut AmqpSender,
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
                sender,
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

// --- inbound: AMQP -> skilj ---

/// The subset of an inbound AMQP message's own `Properties` this bridge
/// actually uses for redelivery safety - unlike `skilj-kafka`'s
/// `(topic, partition, offset)`, which the broker always supplies,
/// every field here is genuinely optional: AMQP 1.0 defines these as
/// standard properties, but nothing forces a sender to populate them.
///
/// **When a field is absent**: `dispatch_inbound_message` simply omits
/// the corresponding skilj mechanism rather than inventing a value or
/// refusing the message - a `Record` action with neither a `group_id`/
/// `group_sequence` pair nor a `message_id` calls `ExternalEventIngestion`
/// without `Idempotency-Key` (created every time, exactly as if this
/// crate's own dedupe support didn't exist - `ExternalEventIngestion`'s own
/// `OmittingTheDedupePairChangesNothing` guarantee, [§175](../../docs/architecture.md#external-message-keys)); a `Trigger`
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
    /// AMQP 1.0's own standard `correlation-id` property (Codeberg issue
    /// #18) - rendered via `message_id_to_string`, same treatment
    /// `message_id` above already gets. Forwarded as `correlationId` on
    /// whichever skilj call `dispatch_inbound_message` makes; `None`
    /// leaves it absent on the wire, letting skilj generate one itself.
    pub correlation_id: Option<MessageId>,
    /// `CAUSATION_ID_PROPERTY`'s own value, read back from
    /// `application-properties` - no standard AMQP property for this.
    pub causation_id: Option<String>,
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

/// The exact `ExternalEventRequest`/`CommandTriggerRequest` body
/// [`dispatch_inbound_message`] sends for `mapping`/`meta`/`payload_json` -
/// factored out so [`report_parked_delivery`] can store the identical
/// body a `retryParkedDelivery` redrive later needs, the identical
/// `skilj_kafka::inbound_request_body` reasoning.
fn inbound_request_body(
    mapping: &InboundMapping,
    meta: &InboundMessageMeta,
    payload_json: &serde_json::Value,
) -> serde_json::Value {
    let correlation_id = meta.correlation_id.as_ref().map(message_id_to_string);
    match &mapping.action {
        // No `dedupe` cursor: deduplicated by `inbound_idempotency_key`
        // instead (docs/architecture.md §175).
        InboundAction::Record { .. } => serde_json::json!({
            "payload": payload_json,
            "sourceContent": "amqp",
            "correlationId": correlation_id,
            "causationId": meta.causation_id,
        }),
        InboundAction::Trigger { .. } => serde_json::json!({
            "payload": payload_json,
            "correlationId": correlation_id,
            "causationId": meta.causation_id,
        }),
    }
}

/// A stable-enough string identifying one inbound message for
/// `POST /v1/parked-deliveries`' own `identifier` field - purely
/// informational (an operator's own way of finding the original message
/// in the broker's own tooling), never used for redelivery-safety
/// itself (that's [`inbound_idempotency_key`], sent separately). Prefers `group-id`/`group-sequence`
/// (when both present), falls back to `message-id`, falls back to a
/// fixed placeholder when the sender populated neither - the identical
/// "never an error, just less identifiable" register every other use of
/// this message's own sparse metadata already has (see
/// [`InboundMessageMeta`]'s own doc comment).
fn message_identifier(meta: &InboundMessageMeta) -> String {
    match (&meta.group_id, meta.group_sequence) {
        (Some(group_id), Some(group_sequence)) => format!("{group_id}:{group_sequence}"),
        _ => meta
            .message_id
            .as_ref()
            .map(message_id_to_string)
            .unwrap_or_else(|| "unidentified-amqp-message".to_string()),
    }
}

/// Dispatches one AMQP message to skilj - the one place [`InboundAction`]
/// is interpreted, the identical shape `skilj_kafka::dispatch_inbound_message`
/// already has. Exposed separately from [`run_inbound`] so it can be
/// tested directly against a plain [`InboundMessageMeta`] and raw
/// payload bytes, without needing a real `fe2o3_amqp` delivery object.
/// The `Idempotency-Key` an inbound request carries. `Record`:
/// `"{group-id}:{group-sequence}"` when the sender set both - this
/// crate's own [`produce_once`] always does - or else `message-id`, or
/// `None`; a per-message key, since a watermark over the group would drop
/// a message redelivered after later ones (docs/architecture.md §175).
/// `Trigger`: the message's own AMQP `message-id`, when the sender set
/// one - `None` otherwise. Shared by [`dispatch_inbound_message`] (as the header) and
/// [`report_parked_delivery`] (so `retryParkedDelivery` redrives under
/// the same key, deduping against the original attempt if it committed
/// after all).
fn inbound_idempotency_key(mapping: &InboundMapping, meta: &InboundMessageMeta) -> Option<String> {
    match mapping.action {
        InboundAction::Record { .. } => match (&meta.group_id, meta.group_sequence) {
            (Some(group_id), Some(group_sequence)) => Some(format!("{group_id}:{group_sequence}")),
            _ => meta.message_id.as_ref().map(message_id_to_string),
        },
        InboundAction::Trigger { .. } => meta.message_id.as_ref().map(message_id_to_string),
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

    skilj_bridge::post_inbound(
        http,
        skilj_base_url,
        mapping,
        &body,
        inbound_idempotency_key(mapping, meta).as_deref(),
    )
    .await?;
    // A `Trigger` rejection is reported as a `200 { accepted: false, ... }`
    // per §5.4/§7.3, not an HTTP error - not inspected here, the
    // identical reasoning `skilj_kafka::dispatch_inbound_message`
    // already documents: the message was successfully delivered and
    // decided upon, which is all this bridge ever promises.
    Ok(())
}

/// Codeberg issue #21 - reports a message [`run_inbound`] gave up
/// retrying to skilj's own `POST /v1/parked-deliveries`, the identical
/// `skilj_kafka::report_parked_delivery` shape. `request` is
/// [`inbound_request_body`]'s own output, stored verbatim so a later
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
        "amqp-inbound",
        identifier,
        request,
        idempotency_key,
        error,
        attempt_count,
        first_failed_at,
    )
    .await?)
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
/// Codeberg issue #21: a dispatch failure is retried, *for this one
/// message*, with backoff up to `retry_policy` - a purely local,
/// in-process loop, since (unlike `skilj-kafka`'s broker-assigned
/// offsets) AMQP 1.0 gives this bridge no delivery/redelivery count to
/// track instead - see this crate's own "Dead-letter/parking" doc
/// section. Once `retry_policy` exhausts, the message is reported to
/// skilj via `report_parked_delivery` and the delivery is accepted
/// anyway - without that, a poison message would block redelivery
/// forever. If the *report* itself fails, the delivery is released
/// (AMQP's "not processed" outcome), so the broker redelivers it and the
/// next round of retries reports it again - a loud, visible redelivery
/// loop rather than a silently unreported poison message. Left unsettled
/// instead, it would stay with this link until the connection closed and
/// hold a unit of its credit (docs/architecture.md §98).
///
/// `http` should be bounded by a timeout - [`http_client`] is - or one
/// request stuck on a dead connection stalls this loop forever (§82).
pub async fn run_inbound(
    receiver: &mut AmqpReceiver,
    http: &reqwest::Client,
    skilj_base_url: &str,
    address: &str,
    mappings: &HashMap<String, InboundMapping>,
    retry_policy: &skilj_retry::RetryPolicy,
) -> ! {
    run_inbound_until(
        receiver,
        http,
        skilj_base_url,
        address,
        mappings,
        retry_policy,
        std::future::pending(),
    )
    .await;
    unreachable!("run_inbound_until only returns once `stop` resolves, and `pending()` never does")
}

/// [`run_inbound`] until `stop` resolves (docs/architecture.md §129).
/// `stop` is raced against the wait for the next delivery and against the
/// backoff between a failing delivery's retries - never against a
/// dispatch, report or settlement in flight. A delivery it stops during is
/// released first (not left unsettled, which would hold it with this link
/// until the connection closes - §98), so the broker redelivers it, and
/// skilj's own dedupe and `Idempotency-Key` make that harmless.
pub async fn run_inbound_until(
    receiver: &mut AmqpReceiver,
    http: &reqwest::Client,
    skilj_base_url: &str,
    address: &str,
    mappings: &HashMap<String, InboundMapping>,
    retry_policy: &skilj_retry::RetryPolicy,
    stop: impl std::future::Future<Output = ()>,
) {
    let mut stop = std::pin::pin!(stop);
    loop {
        let received = tokio::select! {
            biased;
            () = &mut stop => return,
            received = receiver.recv::<Data>() => received,
        };
        let delivery = match received {
            Ok(delivery) => delivery,
            // A body that isn't `Data` sections (an `amqp-value`, say)
            // can't be read as bytes to dispatch. Left unsettled it would
            // hold a unit of link credit forever - enough of them and the
            // receiver stalls, §98's failure - so it's rejected: the
            // broker's own dead-lettering takes it from there
            // (docs/architecture.md §144).
            Err(fe2o3_amqp::link::RecvError::MessageDecode(e)) => {
                tracing::error!(
                    address,
                    "AMQP message body is not data sections - rejecting it: {}",
                    e.source
                );
                if let Err(reject_err) = receiver.reject(e.info, None).await {
                    tracing::error!("rejecting an AMQP delivery failed: {reject_err}");
                }
                continue;
            }
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
        let causation_id = delivery
            .message()
            .application_properties
            .as_ref()
            .and_then(|props| props.get(CAUSATION_ID_PROPERTY))
            .and_then(|v| match v {
                SimpleValue::String(s) => Some(s.clone()),
                _ => None,
            });
        let meta = InboundMessageMeta {
            group_id: properties.and_then(|p| p.group_id.clone()),
            group_sequence: properties.and_then(|p| p.group_sequence),
            message_id: properties.and_then(|p| p.message_id.clone()),
            correlation_id: properties.and_then(|p| p.correlation_id.clone()),
            causation_id,
        };
        let payload = delivery.body().0.as_ref();

        let mut retry = skilj_retry::MessageRetry::default();
        loop {
            match dispatch_inbound_message(http, skilj_base_url, mapping, &meta, payload).await {
                Ok(()) => {
                    if let Err(e) = receiver.accept(&delivery).await {
                        tracing::error!("accepting an AMQP delivery failed: {e}");
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
                                address,
                                attempt,
                                error = %e,
                                "dispatch failed repeatedly - parking and accepting so this \
                                 message doesn't block redelivery forever"
                            );
                            let payload_json = parked_payload(payload);
                            let body = inbound_request_body(mapping, &meta, &payload_json);
                            let identifier = message_identifier(&meta);
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
                                // docs/architecture.md §98: released, not left
                                // unsettled - an unsettled delivery stays with
                                // this link (redelivered only once the connection
                                // closes) and holds a unit of its credit.
                                // Released, the broker redelivers it now, and the
                                // next round of retries reports it again.
                                tracing::error!(
                                    address,
                                    "reporting this parked delivery failed - releasing it \
                                 for redelivery: {report_err}"
                                );
                                if let Err(release_err) = receiver.release(&delivery).await {
                                    tracing::error!(
                                        "releasing an AMQP delivery failed: {release_err}"
                                    );
                                }
                                break;
                            }
                            if let Err(accept_err) = receiver.accept(&delivery).await {
                                tracing::error!("accepting an AMQP delivery failed: {accept_err}");
                            }
                            break;
                        }
                        skilj_retry::RetryDecision::Wait(backoff) => backoff,
                    };
                    tracing::warn!(
                        address,
                        attempt = retry.attempt(),
                        error = %e,
                        "dispatch failed - retrying after backoff"
                    );
                    let stopping = tokio::select! {
                        biased;
                        () = &mut stop => true,
                        () = tokio::time::sleep(backoff) => false,
                    };
                    if stopping {
                        if let Err(release_err) = receiver.release(&delivery).await {
                            tracing::error!("releasing an AMQP delivery failed: {release_err}");
                        }
                        return;
                    }
                }
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

    // --- Codeberg issue #25's investigation (docs/architecture.md §54) ---

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
