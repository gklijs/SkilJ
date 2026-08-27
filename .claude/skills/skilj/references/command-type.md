# `CommandType`

The one place real domain logic lives - `decide()`. Everything else in
skilj (event storage, projections, the GraphQL/REST surfaces) is
generic; `decide()` is the plugged-in, bounded-context-specific part.

```rust
pub trait CommandType {
    type Payload: Serialize + DeserializeOwned + JsonSchema;
    type Event: BoundedContextEvent; // see "The Event associated type" below

    const NAME: &'static str;
    const BOUNDED_CONTEXT: &'static str = DEFAULT_BOUNDED_CONTEXT; // see registration.md

    fn tag_mappings() -> Vec<TagMapping> { Vec::new() }
    fn sensitive_fields() -> Vec<SensitiveField> { Vec::new() }
    fn rest_trigger_allowed() -> bool { false }
    fn required_role() -> Option<&'static str> { None }

    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision;
}
```

`tag_mappings`/`sensitive_fields` have the identical shape and rules as
`EventType`'s own (see `event-type.md`) - a command's payload is stored
too (`CommandQuery` reads it back), so the same encryption-at-rest and
DCB-tag mechanics apply.

## `decide()` - pure, synchronous, no I/O

```rust
fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision;
```

Deliberately no database access, no async, nothing but the two
arguments - `ProcessCommand`'s optimistic-then-locked retry may call
`decide()` more than once for a single real submission (once
optimistically, again under lock only if a genuine conflict was
detected), so it has to be safe to call twice with different
`matching_events` and produce a decision from those inputs alone.

Returns `CommandDecision`:

```rust
enum CommandDecision {
    Accepted { events: Vec<EventSpec> },
    Rejected { reason: String, kind: String },
}
```

`reason` is the human-readable explanation; `kind` is the short,
machine-readable label a caller branches on (`"insufficient_funds"`,
not `"Insufficient Funds"`) - always return both together, never one
without the other. A rejection is ordinary typed data, not thrown or
turned into a GraphQL/HTTP error - the caller sees `accepted: false` and
these two fields on the mutation's own result.

Real example - `skilj-demo/src/banking.rs`'s `WithdrawMoney`:

```rust
fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
    if payload.amount <= 0 {
        return CommandDecision::Rejected {
            reason: "withdrawal amount must be positive".into(),
            kind: "invalid_amount".into(),
        };
    }
    let balance = balance_of(matching_events); // folds matching_events by hand
    if payload.amount > balance {
        return CommandDecision::Rejected {
            reason: format!("account {} has balance {balance}, cannot withdraw {}",
                payload.account_id, payload.amount),
            kind: "insufficient_funds".into(),
        };
    }
    CommandDecision::Accepted {
        events: vec![EventSpec {
            event_type: "MoneyWithdrawn".into(),
            payload: serde_json::json!({ "account_id": payload.account_id, "amount": payload.amount }),
        }],
    }
}
```

`EventSpec.event_type` is the event type's `NAME` as a plain string
(not `MoneyWithdrawn::NAME` - there's no compile-time link between a
`CommandType` and the `EventType`s it can trigger, so a typo here is a
registration-time/runtime `UnregisteredEventType` error, not a compile
error - see `common-mistakes.md`). `EventSpec.payload` is a raw
`serde_json::Value`, not `Self::Event` - constructed by hand
(`serde_json::json!{...}`), not derived from the enum.

## `matching_events` - what `decide()` is actually deciding against

The tag-scoped set of prior events sharing at least one of this
command's own `tag_mappings`-derived tags - **not** every event in the
bounded context. `WithdrawMoney`'s own `tag_mappings` maps `"account"`
to `account_id`, so `matching_events` for a withdrawal against account
`"a1"` is exactly that account's own deposit/withdrawal history, never
another account's - structurally, not by a filter `decide()` has to
apply itself. This is skilj's Dynamic Consistency Boundary (DCB)
mechanism: two commands whose `tag_mappings` never overlap can never
conflict with each other, and their commits never contend for the same
retry either.

A command with no `tag_mappings` at all always sees `matching_events: []`
- there's no shared tag to scope by, so nothing before it is considered
"matching."

## The `Event` associated type

```rust
type Event: BoundedContextEvent;
```

One hand-written enum per bounded context, shared across every
`CommandType`/`Projection` in that context - not one per command type.
`matching_events: &[Self::Event]` is typed against it so a missed match
arm on a new event type is a compile error, not a silently-ignored one.
See `skilj-demo/src/banking.rs`'s own `BankingEvent` (covers both
`MoneyDeposited`/`MoneyWithdrawn`) and its `impl BoundedContextEvent`
(`try_from_event`, one match arm per registered `EventType::NAME`) -
when you add a new `EventType` to an existing bounded context, add its
variant and match arm to this same enum, not a new one.

## `required_role()` - an extra caller-facing gate

Don't override this method by hand - write `#[requires_role("name")]`
directly above the `impl CommandType for ...` block instead, so the
restriction reads as part of the command's own declaration
(`requires_role` is re-exported from `skilj` alongside `CommandType`
itself - `use skilj::{CommandType, requires_role, /* ... */};`):

```rust
#[requires_role("Teller")]
#[auto_register(BOUNDED_CONTEXT)]
impl CommandType for WithdrawMoney { /* ... */ }
```

On top of the ordinary write-level grant check - `Role.name` has no
uniqueness guarantee in the spec, so this is only as safe as the
deployment's own discipline keeping role names meaningful. GraphQL-only;
REST triggering uses `CommandToken`'s own separate per-token grant
instead.
