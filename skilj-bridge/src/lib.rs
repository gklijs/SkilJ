//! The skilj side of a message-broker bridge, shared by `skilj-kafka`,
//! `skilj-amqp` and `skilj-nats` (docs/architecture.md §165): consuming
//! and acknowledging a mapped event type's events (`GET
//! /v1/events/consume`, `POST /v1/events/consume/ack`), recording or
//! triggering from an inbound message (`POST /v1/events/external`, `POST
//! /v1/commands/trigger`), reporting a message that kept failing (`POST
//! /v1/parked-deliveries`), and which instance of a partitioned mapping
//! owns an event. Each bridge keeps only its broker-specific half and
//! re-exports the types its own public API names, so their APIs are
//! unchanged.
//!
//! Speaks skilj's wire protocol only - no dependency on `skilj-core` -
//! the same posture every bridge crate has.

use chrono::{DateTime, Utc};
use serde::Deserialize;

/// How long one HTTP request to skilj may take, end to end, before it
/// fails and the loop's own retry handling takes over. Without a bound, a
/// request stuck on a half-open connection (a network partition, a
/// stalled proxy) stalled the loop forever with nothing logged
/// (docs/architecture.md §82). Retrying after a timeout is safe: inbound
/// requests carry an idempotency key or dedupe cursor, and an outbound
/// consume's checkout lease covers one that was served but never answered.
pub const HTTP_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The `reqwest::Client` the bridges' loops use: bounded by
/// [`HTTP_REQUEST_TIMEOUT`] and a 10-second connect timeout. Pass it to
/// `run_inbound` too, unless the caller's own client is bounded already.
pub fn http_client() -> reqwest::Client {
    http_client_with_timeout(HTTP_REQUEST_TIMEOUT)
}

/// [`http_client`] with a request timeout of `timeout` instead.
pub fn http_client_with_timeout(timeout: std::time::Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(std::time::Duration::from_secs(10).min(timeout))
        .build()
        .expect("a client with only timeouts configured always builds")
}

/// One event served by skilj's own `GET /v1/events/consume` - the subset
/// of `EventDto`'s wire shape (`skilj-rest/src/routes/mod.rs`) the bridges
/// need.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumedEvent {
    pub sequence: i64,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub tags: Vec<Tag>,
    /// Defaults to no ids when a response leaves it out.
    #[serde(default)]
    pub metadata: ConsumedEventMetadata,
}

/// The subset of `EventDto.metadata`'s wire shape the bridges need - the
/// two Codeberg-issue-#18 ids, forwarded onto an outbound message.
#[derive(Debug, Default, Deserialize)]
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

/// `GET /v1/events/consume`'s response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumeResponse {
    pub events: Vec<ConsumedEvent>,
    /// The `EventReadToken`'s own scoped event type, echoed back on every
    /// response - [`consume`] checks it against the mapping's.
    pub event_type_name: String,
}

/// The value of the tag named `key_tag_key` among `tags` - an outbound
/// message's key, group id or correlation header. `None` when
/// `key_tag_key` is `None`, or when the named tag is absent entirely or
/// present with a `null` value (the payload field it was mapped from was
/// itself absent at write time). Never an error: an unkeyed message is
/// still a valid message, the broker just gets to place it.
pub fn correlation_key(key_tag_key: Option<&str>, tags: &[Tag]) -> Option<String> {
    let key_tag_key = key_tag_key?;
    tags.iter().find(|t| t.key == key_tag_key)?.value.clone()
}

/// A failed call to skilj. Each bridge converts it into its own
/// `BridgeError`'s `Skilj`/`SkiljStatus`/`EventTypeMismatch` variants.
#[derive(Debug, thiserror::Error)]
pub enum SkiljError {
    #[error("calling skilj's own REST surface: {0}")]
    Http(#[from] reqwest::Error),
    #[error("skilj returned {status}: {body}")]
    SkiljStatus {
        status: reqwest::StatusCode,
        body: String,
    },
    /// A mapping's declared event type doesn't match what the
    /// `EventReadToken` given as its credential actually serves - almost
    /// certainly a copy-paste misconfiguration.
    #[error(
        "OutboundMapping declares event_type \"{declared}\", but its own credential is scoped \
         to \"{actual}\" - wrong token for this mapping"
    )]
    EventTypeMismatch { declared: String, actual: String },
}

/// Whether a skilj error response `body` carries one of
/// [`skilj_retry::ANOTHER_INSTANCE_CODES`]: the instance that took the
/// request can't process it, another one can - retried after
/// [`skilj_retry::ANOTHER_INSTANCE_RETRY_DELAY`] without spending an
/// attempt, and never parked (docs/architecture.md §161).
pub fn another_instance_refusal(body: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|body| {
            body["code"]
                .as_str()
                .map(skilj_retry::another_instance_can_do_it)
        })
        .unwrap_or(false)
}

/// `Err(SkiljStatus)` for a non-success response, with its body.
async fn require_success(response: reqwest::Response) -> Result<reqwest::Response, SkiljError> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(SkiljError::SkiljStatus { status, body })
}

/// One manual-ack page of events for `credential` (`GET
/// /v1/events/consume?mode=manual`), checked to be of `event_type` - the
/// type the mapping declares.
pub async fn consume(
    http: &reqwest::Client,
    skilj_base_url: &str,
    credential: &str,
    event_type: &str,
) -> Result<ConsumeResponse, SkiljError> {
    let response = http
        .get(format!("{skilj_base_url}/v1/events/consume?mode=manual"))
        .bearer_auth(credential)
        .send()
        .await?;
    let consumed: ConsumeResponse = require_success(response).await?.json().await?;
    if consumed.event_type_name != event_type {
        return Err(SkiljError::EventTypeMismatch {
            declared: event_type.to_string(),
            actual: consumed.event_type_name,
        });
    }
    Ok(consumed)
}

/// Acknowledges `sequence` for `credential`'s read cursor (`POST
/// /v1/events/consume/ack`).
pub async fn ack(
    http: &reqwest::Client,
    skilj_base_url: &str,
    credential: &str,
    sequence: i64,
) -> Result<(), SkiljError> {
    let response = http
        .post(format!("{skilj_base_url}/v1/events/consume/ack"))
        .bearer_auth(credential)
        .json(&serde_json::json!({ "sequence": sequence }))
        .send()
        .await?;
    require_success(response).await?;
    Ok(())
}

/// One broker address's (topic, queue, subject) own mapping to a skilj
/// action - the inbound half.
pub struct InboundMapping {
    /// A credential scoped to whichever `action` needs it -
    /// `ExternalEventToken` for `Record`, `CommandToken` for `Trigger`.
    pub credential: String,
    pub action: InboundAction,
}

/// Which skilj call a mapped address's messages become. Neither variant's
/// own `event_type`/`command_type` field is sent on the wire - each token
/// is already scoped to exactly one type - they exist so a mapping list
/// reads clearly.
pub enum InboundAction {
    /// `POST /v1/events/external` - record the message verbatim as a
    /// fact. Redelivery-safe via the `dedupe` mechanism
    /// (docs/architecture.md §39) where the broker supplies an ordered
    /// sequence.
    Record { event_type: String },
    /// `POST /v1/commands/trigger` - decide on the message via a real
    /// `decide()`. Redelivery-safe via `Idempotency-Key` (§21),
    /// `client_id`-scoped (§37) so a mapping's inbound traffic can never
    /// collide with an unrelated caller's.
    Trigger { command_type: String },
}

impl InboundAction {
    /// The `kind` `POST /v1/parked-deliveries` expects - see
    /// `skilj-rest::routes::ParkedDeliveryKindRequest`'s own identical
    /// two variants.
    pub fn parked_delivery_kind(&self) -> &'static str {
        match self {
            InboundAction::Record { .. } => "external_event",
            InboundAction::Trigger { .. } => "command_trigger",
        }
    }
}

/// Sends an inbound message's request `body` for `mapping` - `POST
/// /v1/events/external` for `Record`, `POST /v1/commands/trigger` for
/// `Trigger`, either with `idempotency_key` as its `Idempotency-Key`
/// header (an external event's per-message key, docs/architecture.md
/// §175). A `Trigger` rejection is a `200 { accepted: false, ... }`, not
/// an error: the message was delivered and decided upon, which is all a
/// bridge promises.
pub async fn post_inbound(
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    body: &serde_json::Value,
    idempotency_key: Option<&str>,
) -> Result<(), SkiljError> {
    let request = match &mapping.action {
        InboundAction::Record { .. } => http.post(format!("{skilj_base_url}/v1/events/external")),
        InboundAction::Trigger { .. } => http.post(format!("{skilj_base_url}/v1/commands/trigger")),
    };
    let request = match idempotency_key {
        Some(key) => request.header("Idempotency-Key", key),
        None => request,
    };
    let response = request
        .bearer_auth(&mapping.credential)
        .json(body)
        .send()
        .await?;
    require_success(response).await?;
    Ok(())
}

/// Codeberg issue #21 - reports a message a bridge's `run_inbound` gave
/// up retrying to skilj's own `POST /v1/parked-deliveries`, with the
/// credential `mapping` already carries (that route resolves the bounded
/// context from the presented token). `source` names the bridge
/// (`"kafka-inbound"`, ...), `identifier` the message within it.
/// `request` is the exact body that kept failing and `idempotency_key`
/// the header it was sent with, both stored so a later
/// `retryParkedDelivery` redrives the identical request.
#[allow(clippy::too_many_arguments)]
pub async fn report_parked_delivery(
    http: &reqwest::Client,
    skilj_base_url: &str,
    mapping: &InboundMapping,
    source: &str,
    identifier: &str,
    request: &serde_json::Value,
    idempotency_key: Option<&str>,
    error: &str,
    attempt_count: u32,
    first_failed_at: DateTime<Utc>,
) -> Result<(), SkiljError> {
    let response = http
        .post(format!("{skilj_base_url}/v1/parked-deliveries"))
        .bearer_auth(&mapping.credential)
        .json(&serde_json::json!({
            "source": source,
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
    require_success(response).await?;
    Ok(())
}

/// The `payload` a parked delivery's stored request carries: the message
/// as JSON, or - when it isn't JSON at all - its raw content as a JSON
/// string, so an operator can see what arrived rather than a `null`
/// (docs/architecture.md §144). A retry of such a row sends that string,
/// which no event or command schema accepts, so it stays parked until
/// discarded.
pub fn parked_payload(payload: &[u8]) -> serde_json::Value {
    serde_json::from_slice(payload).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(payload).into_owned())
    })
}

/// `skilj_core::db::partition_for_key`'s own hand-written 64-bit FNV-1a,
/// vendored rather than pulled in via a `skilj-core` dependency. Not
/// `std::collections::hash_map::DefaultHasher`, which is explicitly not
/// guaranteed stable across Rust versions/std/build flags - unsuitable
/// for a scheme that must agree across every bridge instance in a fleet,
/// possibly running slightly different builds mid rolling-deploy.
pub fn partition_for_key(key: &str, partition_count: u32) -> u32 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    (hash % u64::from(partition_count.max(1))) as u32
}

/// Whether a mapping with `partition` (`(partition_index,
/// partition_count)`, or `None` when unpartitioned) is responsible for an
/// event whose [`correlation_key`] output is `key` - always for an
/// unpartitioned mapping, by [`partition_for_key`] otherwise.
pub fn owns_partition(partition: Option<(u32, u32)>, key: Option<&str>) -> bool {
    match partition {
        None => true,
        Some((partition_index, partition_count)) => {
            partition_for_key(key.unwrap_or(""), partition_count) == partition_index
        }
    }
}

/// Codeberg issue #21 - the backoff state one outbound mapping's own
/// blocked head-of-line event carries across [`outbound_cycle`] calls,
/// threaded in by each bridge's own `run_outbound` (one instance per
/// mapping). Only the head can ever be blocked: a cycle never attempts an
/// event *behind* one still in backoff, the identical invariant
/// `skilj_core::db`'s own `cross_context_route_cursors` retry columns rely
/// on for the same reason.
#[derive(Debug, Clone, Copy)]
pub struct OutboundRetryState {
    /// Which event this state belongs to - cleared once an
    /// acknowledgement covers it, so a stale state left over from an old,
    /// now-passed event is never mistaken for the current head's.
    sequence: i64,
    attempt: u32,
    first_failed_at: DateTime<Utc>,
    next_attempt_at: DateTime<Utc>,
}

/// The broker-specific half of one outbound mapping: hands one event to
/// the broker. Each bridge implements it over its own client (a Kafka
/// producer, an AMQP sender, a JetStream context); [`outbound_cycle`]
/// owns everything on the skilj side.
pub trait OutboundSink {
    /// The bridge's own error type, which a failed skilj call converts
    /// into.
    type Error: std::fmt::Display + From<SkiljError>;

    /// What a log line calls the broker ("Kafka", "AMQP", "JetStream").
    fn broker_name(&self) -> &'static str;

    /// Delivers `event`, resolving only once the broker has accepted it.
    fn deliver(
        &mut self,
        event: &ConsumedEvent,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send;
}

/// One outbound mapping, as [`outbound_cycle`] needs it.
pub struct OutboundTarget<'a> {
    /// The mapping's own `EventReadToken` credential.
    pub credential: &'a str,
    /// The event type the mapping declares - checked against what
    /// `consume` returns, and used in log lines.
    pub event_type: &'a str,
    /// `(partition_index, partition_count)` for a partitioned mapping.
    pub partition: Option<(u32, u32)>,
    /// The tag key whose value is an event's [`correlation_key`], which
    /// decides partition ownership.
    pub key_tag_key: Option<&'a str>,
}

/// Events delivered (or passed over) since the last acknowledgement,
/// not yet acknowledged themselves.
struct UnackedRun {
    /// The first of them - what the next `consume` returns first if the
    /// acknowledgement fails.
    head: i64,
    /// The last of them - what one acknowledgement covers.
    last: i64,
    /// How many were actually delivered, rather than passed over as
    /// another partition's or skipped.
    delivered: usize,
}

/// What [`record_failure`] decided.
enum FailureOutcome {
    /// The retry policy is exhausted - give up on this event.
    GiveUp,
    /// Retry later; the cycle stops here.
    Backoff,
}

/// One fetch-deliver-acknowledge cycle for one outbound mapping - each
/// bridge's own `produce_once` is this with its own [`OutboundSink`].
/// Returns how many events this cycle delivered *and* acknowledged (0
/// when the mapping's read cursor is caught up, or when its head event is
/// still in backoff). An event this instance doesn't own (§54) or gave up
/// on (Codeberg issue #21) is acknowledged but not counted, so a caller
/// checking "did this cycle make real progress" isn't misled.
///
/// Events are handled strictly in sequence order, one delivery at a time,
/// so per-key order holds at the broker. Acknowledgements are not sent
/// per event: the read cursor is a single position that never moves
/// backwards (§75), so acknowledging the last event of a contiguous run of
/// handled ones covers the whole run in one call (Codeberg issue #40). A
/// run is acknowledged before a failed event is dealt with and at the end
/// of the page - never past an event that hasn't been delivered, which
/// would move the cursor beyond it and lose it (§169).
///
/// A failure stops the cycle there and records backoff in
/// `retry_state`; nothing behind it is attempted until it succeeds,
/// because skipping ahead would acknowledge past it. A failed delivery is
/// charged to that event. A failed acknowledgement is charged to the
/// run's first event, the one the next `consume` returns first, and that
/// whole run is delivered again - at-least-once, as every bridge already
/// documents. When `retry_policy` is exhausted the event is acknowledged
/// without being delivered, logged loudly.
pub async fn outbound_cycle<S: OutboundSink>(
    http: &reqwest::Client,
    skilj_base_url: &str,
    target: &OutboundTarget<'_>,
    sink: &mut S,
    retry_policy: &skilj_retry::RetryPolicy,
    retry_state: &mut Option<OutboundRetryState>,
) -> Result<usize, S::Error> {
    if let Some(state) = retry_state {
        if Utc::now() < state.next_attempt_at {
            return Ok(0);
        }
    }

    let consumed = consume(http, skilj_base_url, target.credential, target.event_type).await?;

    let mut served = 0;
    let mut run: Option<UnackedRun> = None;
    for event in &consumed.events {
        let key = correlation_key(target.key_tag_key, &event.tags);
        let delivered = if owns_partition(target.partition, key.as_deref()) {
            match sink.deliver(event).await {
                Ok(()) => true,
                Err(e) => {
                    // Everything before this event is done - acknowledge
                    // it first, so a retry starts here and not earlier.
                    if !flush_run(
                        http,
                        skilj_base_url,
                        target,
                        sink.broker_name(),
                        run.take(),
                        retry_policy,
                        retry_state,
                        &mut served,
                    )
                    .await?
                    {
                        return Ok(served);
                    }
                    match record_failure(
                        target,
                        sink.broker_name(),
                        event.sequence,
                        &e,
                        retry_policy,
                        retry_state,
                    ) {
                        FailureOutcome::Backoff => return Ok(served),
                        FailureOutcome::GiveUp => false,
                    }
                }
            }
        } else {
            false
        };
        let run = run.get_or_insert(UnackedRun {
            head: event.sequence,
            last: event.sequence,
            delivered: 0,
        });
        run.last = event.sequence;
        if delivered {
            run.delivered += 1;
        }
    }
    flush_run(
        http,
        skilj_base_url,
        target,
        sink.broker_name(),
        run,
        retry_policy,
        retry_state,
        &mut served,
    )
    .await?;
    Ok(served)
}

/// Acknowledges `run`, if there is one. `Ok(true)` when the cycle may
/// carry on, `Ok(false)` when the acknowledgement failed and is now in
/// backoff. An exhausted retry policy gives up the way a failed delivery
/// does - by acknowledging anyway - so a failure of that last attempt is
/// returned as the cycle's error.
#[allow(clippy::too_many_arguments)]
async fn flush_run<E: std::fmt::Display + From<SkiljError>>(
    http: &reqwest::Client,
    skilj_base_url: &str,
    target: &OutboundTarget<'_>,
    broker_name: &str,
    run: Option<UnackedRun>,
    retry_policy: &skilj_retry::RetryPolicy,
    retry_state: &mut Option<OutboundRetryState>,
    served: &mut usize,
) -> Result<bool, E> {
    let Some(run) = run else {
        return Ok(true);
    };
    match ack(http, skilj_base_url, target.credential, run.last).await {
        Ok(()) => {
            *served += run.delivered;
            if retry_state.is_some_and(|s| s.sequence <= run.last) {
                *retry_state = None;
            }
            Ok(true)
        }
        Err(e) => {
            match record_failure(target, broker_name, run.head, &e, retry_policy, retry_state) {
                FailureOutcome::Backoff => Ok(false),
                FailureOutcome::GiveUp => {
                    ack(http, skilj_base_url, target.credential, run.last).await?;
                    Ok(true)
                }
            }
        }
    }
}

/// Charges one failure to `sequence`'s retry state and decides whether to
/// retry it later or give up on it now.
fn record_failure(
    target: &OutboundTarget<'_>,
    broker_name: &str,
    sequence: i64,
    error: &dyn std::fmt::Display,
    retry_policy: &skilj_retry::RetryPolicy,
    retry_state: &mut Option<OutboundRetryState>,
) -> FailureOutcome {
    let now = Utc::now();
    let state = match retry_state {
        Some(state) if state.sequence == sequence => {
            state.attempt += 1;
            state
        }
        _ => retry_state.insert(OutboundRetryState {
            sequence,
            attempt: 1,
            first_failed_at: now,
            next_attempt_at: now,
        }),
    };
    let attempt = state.attempt;
    let elapsed = (now - state.first_failed_at).to_std().unwrap_or_default();
    if retry_policy.is_exhausted(attempt, elapsed) {
        tracing::error!(
            event_type = %target.event_type,
            sequence,
            attempt,
            error = %error,
            "giving up on this event after repeated failures - acknowledging past it \
             (it may never have reached {broker_name}) so the stream isn't blocked forever"
        );
        *retry_state = None;
        return FailureOutcome::GiveUp;
    }
    tracing::warn!(
        event_type = %target.event_type,
        sequence,
        attempt,
        error = %error,
        "delivering to {broker_name} or acknowledging to skilj failed - will retry with backoff"
    );
    state.next_attempt_at = retry_policy.next_attempt_at(now, attempt);
    FailureOutcome::Backoff
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let client = http_client_with_timeout(std::time::Duration::from_millis(200));
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

    /// docs/architecture.md §161: only skilj's "another instance can"
    /// codes are retried without spending an attempt.
    #[test]
    fn another_instance_refusals_are_recognised_by_their_code() {
        assert!(another_instance_refusal(
            r#"{"code":"sync_projection_not_declared","message":"x"}"#
        ));
        assert!(another_instance_refusal(
            r#"{"code":"no_decider_registered","message":"x"}"#
        ));
        assert!(!another_instance_refusal(
            r#"{"code":"database_error","message":"x"}"#
        ));
        assert!(!another_instance_refusal("Bad Gateway"));
    }

    #[test]
    fn partition_for_key_is_deterministic_across_calls() {
        assert_eq!(partition_for_key("o-1", 4), partition_for_key("o-1", 4));
        assert_eq!(partition_for_key("", 4), partition_for_key("", 4));
    }

    #[test]
    fn owns_partition_is_always_true_when_unpartitioned() {
        for key in [None, Some("o-1"), Some("o-2"), Some("")] {
            assert!(owns_partition(None, key));
        }
    }

    /// Every key must be owned by exactly one partition index - not zero
    /// (a key silently dropped by every instance) and not more than one
    /// (a key double-published by two instances).
    #[test]
    fn owns_partition_assigns_every_key_to_exactly_one_partition() {
        let partition_count = 4;
        for i in 0..200 {
            let key = format!("order-{i}");
            let owners = (0..partition_count)
                .filter(|index| owns_partition(Some((*index, partition_count)), Some(&key)))
                .count();
            assert_eq!(owners, 1, "{key}");
        }
    }
}
