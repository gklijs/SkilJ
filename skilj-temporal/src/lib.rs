//! A bridge from skilj's own event stream to Temporal - phase 2/3 of the
//! plan in [docs/architecture.md §34](../../docs/architecture.md#skilj-temporal-plan). Wire-protocol client only, on both
//! sides: this crate speaks skilj's REST surface (`GET /v1/events/consume`
//! and `POST /v1/events/consume/ack`, `docs/rest-event-reading.md`) and
//! Temporal's `temporalio-client` gRPC client - zero dependency on any
//! other skilj crate, the same "independently usable, wire protocol
//! only" posture `skilj-tui` already has, from the outside. It also
//! deliberately depends on `temporalio-client` alone, not the full
//! `temporalio-sdk` worker/Activity-authoring crate - starting or
//! signaling a workflow execution from outside needs only the former;
//! the latter is a separate, much larger dependency a consuming
//! application's own Workflow/Activity code may or may not ever touch.
//!
//! # Correlation
//!
//! A skilj event maps to a Temporal action (`MappingAction::Signal` or
//! `MappingAction::Start`) via a configured [`EventTypeMapping`]. Which
//! *workflow execution* an action targets is derived automatically from
//! one of the event's own DCB tags - the fixed convention
//! `workflow_id = "{bounded_context}:{tag_key}:{tag_value}"`
//! ([`correlation_workflow_id`]), the same "derived from a tag, not a
//! fresh concept" register `Projection::OWNER_TAG_KEY` already
//! established. Reusing this identical id for both a `Start` and every
//! later `Signal` is what makes Temporal's own idempotent-start-by-
//! workflow-id semantics (`WorkflowIdConflictPolicy::UseExisting`) do
//! the correlation's own dedup work for free - this crate writes none of
//! its own.
//!
//! # Idempotency
//!
//! A redelivered skilj event (manual-ack mode - `docs/rest-event-reading.md`'s
//! own "redeliver on crash before ack, handler must be safe to run
//! twice" contract) must not double-signal or double-start. `Start`
//! already gets this for free from `WorkflowIdConflictPolicy::UseExisting`
//! above. `Signal` does not have an equivalent server-side dedup by
//! workflow id alone - Temporal's own answer is `WorkflowSignalOptions::request_id`,
//! and this crate derives one from the event's own `(bounded_context,
//! event_type, sequence)` - stable across a redelivery of the identical
//! event, unique across every other one, the same
//! `"{run_id}:{activity_id}"` reasoning `docs/temporal-integration.md`
//! already applies to Activity idempotency, one level further out.

use serde::Deserialize;
use std::time::Duration;
use temporalio_client::errors::{WorkflowInteractionError, WorkflowStartError};
use temporalio_client::{
    Client, UntypedSignal, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::data_converters::{PayloadConverter, RawValue};
use temporalio_common::UntypedWorkflow;

/// One skilj `EventType`'s own mapping to a Temporal action.
pub struct EventTypeMapping {
    /// The `EventType::NAME` this mapping applies to.
    pub event_type: String,
    /// An `EventReadToken` credential (`"{id}.{secret}"`) scoped to this
    /// event type - `EventReadToken` is always scoped to exactly one
    /// `EventType` (`docs/rest-event-reading.md`), so one token per
    /// mapped event type, not one for the whole bounded context.
    pub credential: String,
    /// Which of this event type's own tag keys supplies the correlation
    /// value - see [`correlation_workflow_id`].
    pub correlation_tag_key: String,
    pub action: MappingAction,
}

/// Which Temporal call a mapped event triggers.
pub enum MappingAction {
    /// `SignalWorkflowExecution` - the "await external event /
    /// human-in-the-loop" leg (Axon's `awaitEvent`, done by Temporal).
    Signal { signal_name: String },
    /// `StartWorkflowExecution` - a fresh workflow run (Axon's
    /// `@Workflow(startOnEvent = ...)`, done by Temporal).
    Start {
        workflow_type: String,
        task_queue: String,
    },
}

/// One event served by skilj's own `GET /v1/events/consume` - the subset
/// of `EventDto`'s wire shape (`skilj-rest/src/routes/mod.rs`) this
/// bridge actually needs: enough to derive the correlation id and
/// forward the payload, nothing this crate doesn't use.
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
    /// The `EventReadToken`'s own scoped event type, echoed back by
    /// skilj on every response (`docs/rest-event-reading.md`'s own
    /// wire contract) - checked in `poll_once` against
    /// `EventTypeMapping::event_type`, since a copy-paste mismatch
    /// between a mapping and the credential it was actually given would
    /// otherwise silently apply the wrong `MappingAction`/
    /// `correlation_tag_key` to whatever events the credential really
    /// serves, with no diagnostic at all.
    event_type_name: String,
}

/// The fixed correlation convention ([docs/architecture.md §34](../../docs/architecture.md#skilj-temporal-plan)):
/// `"{bounded_context}:{tag_key}:{tag_value}"`, derived from whichever
/// one of `event.tags` matches `tag_key`. `None` when the tag is
/// altogether absent, or present with a `null` value (`Tag.value` is
/// `None` when the mapped payload field was itself absent at write time,
/// see `value Tag` in the spec) - either way there is no value to
/// correlate on, and retrying can't produce one where the event itself
/// never carried it.
pub fn correlation_workflow_id(
    bounded_context: &str,
    tag_key: &str,
    tags: &[Tag],
) -> Option<String> {
    let value = tags.iter().find(|t| t.key == tag_key)?.value.as_deref()?;
    Some(format!("{bounded_context}:{tag_key}:{value}"))
}

/// The Activity-idempotency-style `request_id` this crate derives for
/// every `Signal` action - see this module's own doc comment.
fn signal_request_id(bounded_context: &str, event_type: &str, sequence: i64) -> String {
    format!("{bounded_context}:{event_type}:{sequence}")
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
    #[error("starting a Temporal workflow: {0}")]
    TemporalStart(#[from] WorkflowStartError),
    #[error("signaling a Temporal workflow: {0}")]
    TemporalSignal(#[from] WorkflowInteractionError),
    /// A mapped event's own correlation tag was absent - see
    /// [`correlation_workflow_id`]'s own doc comment on why this is
    /// unrecoverable rather than retried: acknowledged anyway (the
    /// caller's own `poll_once` does this), logged, never silently
    /// dropped without a trace.
    #[error(
        "event #{sequence} ({event_type}) has no \"{tag_key}\" tag to correlate a workflow id from"
    )]
    NoCorrelationTag {
        sequence: i64,
        event_type: String,
        tag_key: String,
    },
    /// `EventTypeMapping::event_type` doesn't match what the
    /// `EventReadToken` given as its own `credential` actually serves -
    /// almost certainly a copy-paste misconfiguration (a mapping's
    /// credential wired to the wrong event type's own token). Returned
    /// before any event is even looked at, since every event this
    /// credential could ever serve would be equally misrouted.
    #[error(
        "EventTypeMapping declares event_type \"{declared}\", but its own credential is scoped \
         to \"{actual}\" - wrong token for this mapping"
    )]
    EventTypeMismatch { declared: String, actual: String },
}

/// Dispatches one mapped event to Temporal - the one place `MappingAction`
/// is interpreted. `workflow_id` is already-derived
/// ([`correlation_workflow_id`]'s own output), not recomputed here.
async fn dispatch(
    client: &Client,
    workflow_id: &str,
    action: &MappingAction,
    event: &ConsumedEvent,
    bounded_context: &str,
) -> Result<(), BridgeError> {
    let converter = PayloadConverter::default();
    let raw = RawValue::from_value(&event.payload, &converter);
    match action {
        MappingAction::Signal { signal_name } => {
            let handle = client.get_workflow_handle::<UntypedWorkflow>(workflow_id.to_string());
            let request_id = signal_request_id(bounded_context, &event.event_type, event.sequence);
            handle
                .signal(
                    UntypedSignal::new(signal_name.clone()),
                    raw,
                    WorkflowSignalOptions::builder()
                        .request_id(request_id)
                        .build(),
                )
                .await?;
        }
        MappingAction::Start {
            workflow_type,
            task_queue,
        } => {
            let result = client
                .start_workflow(
                    UntypedWorkflow::new(workflow_type.clone()),
                    raw,
                    WorkflowStartOptions::new(task_queue.clone(), workflow_id.to_string())
                        // Two different questions, two different Temporal
                        // policies (easy to conflate - see this module's
                        // own doc comment): `id_conflict_policy` governs a
                        // workflow *currently running* under this id -
                        // `UseExisting` makes a redelivered Start that
                        // raced ahead of its own ack attach to that run
                        // instead of erroring. `id_reuse_policy` governs
                        // one that has already *closed* under this id -
                        // `RejectDuplicate` (rather than the default
                        // `AllowDuplicate`) is what stops a Start
                        // redelivered *after* the original run already
                        // finished from creating a second, independent
                        // execution: the whole reason this bridge's own
                        // correlation id is deterministic is that a given
                        // business entity gets exactly one workflow
                        // execution, ever, not one at a time. The
                        // `AlreadyStarted` this produces in that case is
                        // caught just below and treated as the success it
                        // actually is.
                        .id_conflict_policy(WorkflowIdConflictPolicy::UseExisting)
                        .id_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate)
                        .build(),
                )
                .await;
            match result {
                Ok(_) => {}
                Err(WorkflowStartError::AlreadyStarted { .. }) => {
                    // Not an error from this bridge's own point of view:
                    // a workflow already exists under this id (running,
                    // via `id_conflict_policy` above, or already closed,
                    // via `id_reuse_policy` above) - either way "ensure a
                    // workflow exists for this business entity" already
                    // holds, which is all a `Start` action ever promised.
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

/// One fetch-dispatch-ack cycle for a single [`EventTypeMapping`] -
/// `run` (below) is just this in a loop. Exposed separately so it can be
/// driven directly in tests without needing to interrupt a running
/// loop. Returns how many events were served this cycle (0 when the
/// mapping's own read cursor is already caught up) - not how many
/// dispatched successfully, since a `NoCorrelationTag` skip is still a
/// served, acknowledged event.
pub async fn poll_once(
    http: &reqwest::Client,
    skilj_base_url: &str,
    temporal: &Client,
    bounded_context: &str,
    mapping: &EventTypeMapping,
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
        match correlation_workflow_id(bounded_context, &mapping.correlation_tag_key, &event.tags) {
            Some(workflow_id) => {
                dispatch(
                    temporal,
                    &workflow_id,
                    &mapping.action,
                    event,
                    bounded_context,
                )
                .await?;
            }
            None => {
                let error = BridgeError::NoCorrelationTag {
                    sequence: event.sequence,
                    event_type: event.event_type.clone(),
                    tag_key: mapping.correlation_tag_key.clone(),
                };
                tracing::warn!(
                    "{error} - acknowledging it anyway, since retrying can never produce a \
                     value the event itself never carried"
                );
            }
        }
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

/// Runs [`poll_once`] forever, one mapping at a time in the order given,
/// sleeping `poll_interval` between cycles that served nothing. Never
/// returns: a transient failure (a network blip against skilj or
/// Temporal, either one briefly unreachable) is logged and retried after
/// `poll_interval`, not propagated - the one thing this function exists
/// to guarantee over calling [`poll_once`] directly is that one bad
/// cycle doesn't silently end the loop. A real deployment runs one
/// [`EventTypeMapping`] per task (`tokio::spawn`) rather than calling
/// this with more than one mapping serially - exposed this way (a plain
/// `&[EventTypeMapping]`, not pre-spawned) so the caller's own process
/// decides that, the same "caller's own responsibility" register the
/// rest of this crate already uses.
///
/// This is also what makes a `Signal` mapping racing ahead of its own
/// correlated `Start` mapping (both polled independently, backlog
/// catch-up or ordinary latency skew can deliver `PaymentConfirmed`
/// before `OrderPlaced` finishes being dispatched) self-healing rather
/// than a permanently stuck event: the failed cycle retries next
/// `poll_interval`, by which point the `Start` has ordinarily already
/// landed. `signal_with_start_workflow` (atomically starting-if-absent
/// and signaling in one call) was considered instead of relying on this
/// retry - and rejected: it would need fabricating the workflow's own
/// starting input from whichever event's payload lost the race, `Start`
/// or `Signal`, silently starting the workflow from the wrong shape
/// when the race actually happens, which is worse than a delayed but
/// correct signal.
pub async fn run(
    skilj_base_url: &str,
    temporal: &Client,
    bounded_context: &str,
    mappings: &[EventTypeMapping],
    poll_interval: Duration,
) -> ! {
    let http = reqwest::Client::new();
    loop {
        let mut served_any = false;
        for mapping in mappings {
            match poll_once(&http, skilj_base_url, temporal, bounded_context, mapping).await {
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
    fn derives_the_fixed_convention_from_the_matching_tag() {
        let tags = vec![tag("company", "acme"), tag("order", "o-1")];
        assert_eq!(
            correlation_workflow_id("orders", "order", &tags),
            Some("orders:order:o-1".to_string())
        );
    }

    #[test]
    fn returns_none_when_the_tag_key_is_entirely_absent() {
        let tags = vec![tag("company", "acme")];
        assert_eq!(correlation_workflow_id("orders", "order", &tags), None);
    }

    #[test]
    fn returns_none_when_the_tag_is_present_but_its_value_is_null() {
        let tags = vec![Tag {
            key: "order".to_string(),
            value: None,
        }];
        assert_eq!(correlation_workflow_id("orders", "order", &tags), None);
    }

    #[test]
    fn the_same_tag_key_in_a_different_bounded_context_derives_a_different_id() {
        let tags = vec![tag("order", "o-1")];
        let a = correlation_workflow_id("orders", "order", &tags).unwrap();
        let b = correlation_workflow_id("returns", "order", &tags).unwrap();
        assert_ne!(a, b, "bounded_context must be part of the derived id");
    }

    #[test]
    fn signal_request_id_is_stable_across_a_simulated_redelivery() {
        // The whole point: the same event redelivered (same sequence)
        // derives the identical request_id, so Temporal's own dedup
        // sees it as the same signal, not two.
        let first = signal_request_id("orders", "PaymentConfirmed", 42);
        let redelivered = signal_request_id("orders", "PaymentConfirmed", 42);
        assert_eq!(first, redelivered);
    }

    #[test]
    fn signal_request_id_differs_for_a_different_event() {
        let a = signal_request_id("orders", "PaymentConfirmed", 42);
        let b = signal_request_id("orders", "PaymentConfirmed", 43);
        assert_ne!(a, b);
    }
}
