---
name: skilj-temporal
description: >
  Pair a skilj deployment with Temporal for long-running, cross-system
  processes - an Activity submitting a skilj command idempotently, and
  the skilj-temporal crate bridging skilj's own event stream to
  starting/signaling a Temporal workflow execution. Triggers on "long
  running process", "saga", "process manager", "workflow orchestration",
  "Temporal integration", "start/signal a workflow from an event", or
  reading/extending the skilj-temporal crate. Do NOT use for: designing
  the domain's own events/commands (skilj-event-modeling), adding a new
  EventType/CommandType/Projection (skilj), or anything Temporal-side
  (writing the actual Workflow/Activity definitions) - this skill covers
  only the skilj-facing half of the integration.
---

# skilj-temporal

skilj doesn't orchestrate long-running, multi-step processes itself,
and doesn't try to - DCB (`skilj-event-modeling`'s own `dcb-tags.md`)
already replaces the saga/process-manager need for facts checkable in
one same-database transaction, but has nothing to say once a process
spans real I/O, real time, or another system entirely. See
[`docs/architecture.md` §34](../../../docs/architecture.md#skilj-temporal-plan) for the full investigation and decision:
pair with Temporal rather than build orchestration into skilj.

## Two independent pieces - use either alone or both together

**1. Submitting a skilj command from a Temporal Activity, idempotently.**
No skilj-temporal dependency needed at all - just skilj's own existing
`Idempotency-Key` header/argument on `submitCommand`/command
triggering, keyed by `"{run_id}:{activity_id}"` (Temporal's own
documented Activity-idempotency guidance). See
`docs/temporal-integration.md` for the full pattern and why Run ID,
not just Workflow ID, is part of the key. A key deduplicates for
`SkiljBuilder::idempotency_key_retention` (default 48 hours), so keep
the Activity's retry policy (its schedule-to-close timeout) inside that
window, or raise the retention - a retry after it lands twice. Keep
`no_decider_registered` and `sync_projection_not_declared` retryable:
they mean the skilj instance reached can't process the command but
another can (rolling deploy - docs/architecture.md §161;
`skilj_retry::another_instance_can_do_it`).

**2. Starting or signaling a Temporal workflow from a skilj event** -
the `skilj-temporal` crate. One `EventTypeMapping` per `EventType` you
want to react to:

```rust
use skilj_temporal::{run, EventTypeMapping, MappingAction};

let mappings = vec![
    EventTypeMapping {
        event_type: "OrderPlaced".to_string(),
        credential: order_placed_read_token, // an EventReadToken, one per mapped event type
        correlation_tag_key: "order".to_string(),
        action: MappingAction::Start {
            workflow_type: "OrderFulfillment".to_string(),
            task_queue: "orders".to_string(),
        },
    },
    EventTypeMapping {
        event_type: "PaymentConfirmed".to_string(),
        credential: payment_confirmed_read_token,
        correlation_tag_key: "order".to_string(), // same key -> same workflow execution
        action: MappingAction::Signal { signal_name: "paymentConfirmed".to_string() },
    },
];

run(skilj_base_url, &temporal_client, "orders", &mappings, poll_interval).await; // never returns
```

`correlation_tag_key` is which of the event's own DCB tags supplies the
workflow id - a fixed, non-configurable convention:
`workflow_id = "{bounded_context}:{tag_key}:{tag_value}"`. Two mappings
naming the same `tag_key` route events carrying the same tag value to
the *same* workflow execution - that's what lets `OrderPlaced` start it
and `PaymentConfirmed` signal that exact run later.

## Real gotchas worth knowing before you use this

- **One `EventReadToken` per mapped `EventType`, not one per bounded
  context.** A token is always scoped to exactly one event type
  (`docs/rest-event-reading.md`) - `poll_once` actively checks this
  (`EventTypeMismatch`) rather than silently misrouting events from a
  mismatched credential.
- **`run` never returns and never dies on a transient error** - a
  network blip against skilj or Temporal is logged and retried after
  `poll_interval`, not propagated. Run one `EventTypeMapping` per
  `tokio::spawn`ed task for real concurrency, not several passed to one
  `run` call expecting them to interleave.
- **An event racing ahead of its own correlated predecessor** (a
  `Signal` mapping's event arriving before its `Start` mapping's event
  has been dispatched - both polled independently) self-heals on the
  next `poll_interval` rather than erroring permanently, by design -
  see `skilj-temporal/src/lib.rs`'s own `run` doc comment for why
  Temporal's `signal_with_start_workflow` was considered and rejected
  as the "fix this at the root" alternative.
- **`run` retries a failing event forever, and events are handled in
  order** - so one that can never succeed (a `Signal` for a workflow
  that has already completed answers "not found" every time) holds up
  every event behind it on that mapping. `run_with_retry`/
  `run_until_with_retry` take a `skilj_retry::RetryPolicy`: once a
  bounded policy is exhausted the event is logged at `error`,
  acknowledged and skipped. Size the policy to outlast the
  `Signal`-before-`Start` race above, which fails the same way until the
  `Start` lands.
- **Workflow ids are per bounded-context *name*, not per incarnation.**
  Delete a bounded context and recreate it under the same name, and its
  entities reuse the old workflow ids: a `Start` is refused as a
  duplicate of the old, closed run (the bridge treats that as success,
  so no workflow runs) and signals reach that closed run. Recreate under
  a new name, or point the new one at its own Temporal namespace
  (`docs/architecture.md` §34/§149).
- Depends only on Temporal's thin `temporalio-client` crate, never the
  full `temporalio-sdk` worker/Activity-authoring one - writing the
  actual Workflow/Activity definitions Temporal executes is a separate
  concern this skill and this crate don't cover.
- **Call `run`, not `poll_once` in a loop of your own.** A manual-ack
  consume claims what it serves for the server's checkout lease (5
  minutes by default), and the same token's next consume is served
  nothing until then. `run` keeps a failed batch and retries it next
  cycle; `poll_once` drops it, so a loop around `poll_once` stalls for
  the whole lease after any failed dispatch (`docs/architecture.md`
  §99).
- No system `protoc` needed: `temporalio-client`'s vendored pure-Rust
  proto compiler is enabled (since 0.0.5 - see CONTRIBUTING.md).

See `skilj-temporal/tests/temporal_bridge.rs` for real, passing,
end-to-end examples against an ephemeral Temporal service, and
[`docs/architecture.md` §34](../../../docs/architecture.md#skilj-temporal-plan) for the complete design write-up (including
what was built vs. deliberately declined).
