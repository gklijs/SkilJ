# Pairing skilj with Temporal

skilj doesn't orchestrate long-running, multi-step processes itself, and doesn't try to -
[`docs/architecture.md` §34](architecture.md) explains why, and lays out the fuller plan this page
is phase 1 of. This page covers what's built today: how a [Temporal](https://temporal.io) Activity
calls skilj's command-submission surface safely, using infrastructure skilj already has.

## The pattern

skilj's `submitCommand` (GraphQL) and `POST /v1/commands/trigger` (REST) both accept an optional
idempotency key (Codeberg issue #12) - the second request with the same key returns the *original*
outcome instead of re-deciding, and never double-applies the command. Temporal's own documented
guidance for making an Activity idempotent is to derive a key from its **Run ID + Activity ID** -
stable across every retry of that one invocation, unique across every other invocation, including
in a different workflow run. Put together: an Activity that calls skilj passes that same
composed string as skilj's idempotency key, and Temporal's own at-least-once Activity retry
becomes safe for free.

```
Idempotency-Key: {run_id}:{activity_id}
```

REST:

```http
POST /v1/commands/trigger
Authorization: Bearer <command-token>
Idempotency-Key: 019... :withdraw-1
Content-Type: application/json

{"payload": {"amount": 50}}
```

GraphQL, the same key as an argument to `submitCommand` (see `docs/architecture.md` §7/§21 for the
full mutation shape).

## Why Run ID, not just Workflow ID

A Workflow ID can outlive more than one Run - `continue-as-new`, a reset - and an Activity ID is
only unique *within* one run. Keying on Workflow ID alone would let two different runs that happen
to reuse the same Activity ID naming (ordinary - Activity IDs are typically positional within a
workflow definition, e.g. `"withdraw-1"` in every run) collide and dedupe against each other
incorrectly. Run ID avoids that.

This is a different concern from the *correlation* convention `docs/architecture.md` §34 plans for
phase 2/3 (which running workflow execution to signal or start) - that one is keyed on Workflow ID
deliberately, since Temporal guarantees at most one open run per Workflow ID at a time. Don't
conflate the two: this page's key answers "is this a retry of the same Activity attempt?"; §34's
future correlation convention answers "which workflow execution does this event belong to?".

## Getting a token

The Activity needs a `CommandToken` scoped to whichever command type it calls - mint one via the
`createCommandToken` GraphQL mutation (an `Admin`-level `RoleAccessMapping` credential does this;
see `docs/architecture.md` §7 for the full admin-operations surface) and give the resulting
`{id}.{secret}` credential to wherever the Activity's Worker process reads its configuration from.
One token per command type is enough for every Workflow that calls it - the idempotency key is
what distinguishes one invocation from the next, not the token.

## Worked example

`skilj/tests/temporal_activity_idempotency_example.rs` is a real, passing test against real
Postgres and the real REST router - not a mock. It plays out the exact scenario this page exists
for: a Temporal Worker executes an Activity that calls `POST /v1/commands/trigger`, then crashes
before Temporal's own server records that the Activity succeeded. From the Workflow's point of
view this is indistinguishable from the Activity never having run, so Temporal retries it with the
identical Run ID and Activity ID - and the test proves that retry is deduplicated, with exactly one
event ending up stored, not two.

It also proves the two ways a key could be *too* broad, and isn't: a different Activity ID within
the same run is not coalesced against the first, and the same Activity ID reused across two
different runs is not coalesced either - the concrete case that motivates including Run ID in the
key in the first place.

## Starting or signaling a workflow from a skilj event: `skilj-temporal`

The `skilj-temporal` crate (`docs/architecture.md` §34, phases 2-3) is a small bridge for the other
direction: a skilj event starting a fresh Temporal workflow execution, or signaling one already
running. It reads skilj's own `GET /v1/events/consume`/`POST /v1/events/consume/ack` (manual-ack
mode) and calls Temporal's client - no dependency on any other skilj crate, and no dependency on
Temporal's own worker/Activity-authoring SDK (`temporalio-sdk`), only its thin client
(`temporalio-client`).

Configure one `EventTypeMapping` per `EventType` you want to react to:

```rust
use skilj_temporal::{poll_once, EventTypeMapping, MappingAction};

let start_on_order_placed = EventTypeMapping {
    event_type: "OrderPlaced".to_string(),
    credential: order_placed_read_token,   // an EventReadToken - one per mapped event type
    correlation_tag_key: "order".to_string(),
    action: MappingAction::Start {
        workflow_type: "OrderFulfillment".to_string(),
        task_queue: "orders".to_string(),
    },
};

let signal_on_payment_confirmed = EventTypeMapping {
    event_type: "PaymentConfirmed".to_string(),
    credential: payment_confirmed_read_token,
    correlation_tag_key: "order".to_string(),
    action: MappingAction::Signal { signal_name: "paymentConfirmed".to_string() },
};
```

`correlation_tag_key` names which of the event's own DCB tags supplies the workflow id - both
mappings above name `"order"`, so `OrderPlaced` and `PaymentConfirmed` events carrying the same
`order` tag value route to the *same* workflow execution: `workflow_id =
"{bounded_context}:{tag_key}:{tag_value}"`, e.g. `"orders:order:o-42"`. This is a fixed convention,
not something you configure per mapping - reusing the identical id for the `Start` and every later
`Signal` is what makes Temporal's own `WorkflowIdConflictPolicy::UseExisting` do this crate's
correlation and redelivery-safety for free.

`skilj_temporal::run(...)` drives a list of mappings forever, polling and sleeping between empty
cycles; `poll_once(...)` runs a single fetch-dispatch-ack cycle and is what you'd call directly in a
test or a custom scheduling loop. See `skilj-temporal/src/lib.rs`'s own doc comment for the full
idempotency story (`Signal`'s own `request_id`, derived from the event's `(bounded_context,
event_type, sequence)` - the identical `"{run_id}:{activity_id}"` reasoning above, one level
further out) and `skilj-temporal/tests/temporal_bridge.rs` for a real, passing end-to-end proof
against an ephemeral Temporal service.

## What's still Temporal's job

Durable timers, retry policies, compensation logic, fan-out/fan-in, human-approval escalation -
none of that is skilj's concern, and none of it needs to be. skilj's own event store stays the
system of record for *what happened to the domain*; Temporal's own workflow history stays the
system of record for *what the process did to get there*. Two audit trails, each answering a
different question, correlated by convention rather than merged into one.
