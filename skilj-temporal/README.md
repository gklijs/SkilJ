# skilj-temporal

A small bridge from [skilj](https://codeberg.org/gklijs/SkilJ)'s event stream to
[Temporal](https://temporal.io): a skilj event **starts** a fresh workflow execution, or
**signals** one that is already running.

skilj is an event-sourcing platform built on Dynamic Consistency Boundaries (DCB). This crate
covers the "react to an event by driving a long-running process" half of pairing it with Temporal,
without a native saga/process-manager concept in skilj itself.

## How it works

- Reads skilj's REST event feed (`GET /v1/events/consume` and `POST /v1/events/consume/ack`, in
  manual-ack mode) and calls Temporal through `temporalio-client`.
- Wire protocol only, on both sides: no dependency on any other skilj crate, and none on
  Temporal's worker/Activity-authoring SDK (`temporalio-sdk`).
- **Correlation comes from a DCB tag.** Each mapped event type names a tag key, and the target
  workflow id is `"{bounded_context}:{tag_key}:{tag_value}"`, for example `orders:order:o-42`.
  `Start` and every later `Signal` for the same entity land on the same execution.
- **Redelivery-safe.** `Start` uses `WorkflowIdConflictPolicy::UseExisting`. `Signal` sends a
  `request_id` derived from the event's `(bounded_context, event_type, sequence)`, so a redelivered
  event never double-signals.

## Usage

```rust
use skilj_temporal::{run, EventTypeMapping, MappingAction};
use std::time::Duration;

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
        correlation_tag_key: "order".to_string(),
        action: MappingAction::Signal { signal_name: "paymentConfirmed".to_string() },
    },
];

// Polls forever; use `poll_once` for a single cycle in tests or a custom loop.
run("http://localhost:8080", &temporal_client, "orders", &mappings, Duration::from_secs(1)).await;
```

Mint each read token with `startFrom: LATEST` unless you deliberately want a backfill: by default a
token's first poll replays the event type's entire history, which would start one workflow per
historical event. See
[`docs/temporal-integration.md`](https://codeberg.org/gklijs/SkilJ/src/branch/main/docs/temporal-integration.md)
for the full walkthrough.

## Caveats

- `temporalio-client` is "Public Preview" upstream and its API is still evolving, so a bump of it
  can need a code change here.
- A `Signal` mapping can race ahead of its correlated `Start`. The failed cycle simply retries on
  the next poll interval.

## More

- [Design notes](https://codeberg.org/gklijs/SkilJ/src/branch/main/docs/architecture.md)
  (section 34) and the [Temporal pairing guide](https://codeberg.org/gklijs/SkilJ/src/branch/main/docs/temporal-integration.md)
- End-to-end test against an ephemeral Temporal service: `tests/temporal_bridge.rs`

Licensed under either of MIT or Apache-2.0, at your option.
