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

To see what `decide()` would say without committing anything, an
Admin-level caller can run the `dryRunCommand` GraphQL query: the
decision, the events it would append (rendered under the usual
sensitive/private-field rules), and the matching events it decided
from. A `CommandToken` holder gets only the outcome, from `POST
/v1/commands/dry-run`. Neither writes a command, an event or an
idempotency record.

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

### Narrowing it: `consistency_query()`

By default `matching_events` is *every* event carrying any of the
command's tags. When `decide()` only needs some of them, declare a
consistency query (DCB's query items; docs/architecture.md §198):

```rust
fn tag_mappings() -> Vec<TagMapping> { vec![company(), ticket()] }
fn consistency_query() -> Vec<QueryItemMapping> {
    vec![
        // The company's latest lifecycle event, whichever kind it is.
        QueryItemMapping::types(&["CompanySignedUp", "CompanyActivated", "CompanyExpired"])
            .tagged(vec![company()])
            .latest(),
        // Whether this ticket id was used.
        QueryItemMapping::types(&["TicketCreated"]).tagged(vec![ticket()]),
    ]
}
```

- An item matches events of its types (any type if it names none) that
  carry **all** of its tags; items are OR'd. `.latest()` keeps only the
  item's newest match - only right when `decide()` really only needs the
  last one (a status where the last event wins). `.last(n)` keeps the n
  newest, for a decision over the last few (at most 3 open tickets, the
  last 5 failed logins).
- The query is also the conflict check: an event matching no item no
  longer makes the command decide again. So every fact `decide()`
  depends on must be in the query, or a concurrent command can change it
  unnoticed.
- `build()` refuses an item's event type that isn't registered in the
  same bounded context, an item tag that isn't one of `tag_mappings()`,
  an item with neither types nor tags (other than a lone `nothing()`,
  below), `last(0)`, and a query together with `snapshot()`. Reach for a
  query before a `Snapshot`: it reads a few index entries instead of a
  growing history, and needs no catch-up. When the decision is a fold
  over a long history, put the `Snapshot` in the query (below).

### When `decide()` needs no history: `QueryItemMapping::nothing()`

Some commands decide from the payload alone - a deposit with no cap,
logging a contact, adding a note. Declare that, and the command reads no
events and nothing ever conflicts with it (docs/architecture.md §201):

```rust
fn tag_mappings() -> Vec<TagMapping> { vec![account()] } // still tagged
fn consistency_query() -> Vec<QueryItemMapping> {
    vec![QueryItemMapping::nothing()]
}
```

Its `tag_mappings()` still tag the command, so owner scoping works as
before - which is what dropping the tag mappings to get an empty
`matching_events` would lose. `nothing()` must be the only item and
can't take `.latest()`/`.last(n)`. In a `.skilj.toml`, it's
`reads_nothing = true` on the `[[command_type]]`. Only use it when
`decide()` truly checks nothing outside the payload: any rule over
earlier events (a balance, a uniqueness check, a status) needs those
events in the query.

### When `decide()` needs a fold over a long history: snapshot items

A balance, a count of open tickets - a value folded over every event of
an account or requester - grows with history. Put a `Snapshot` in the
query instead of the events, and implement `decide_with` to read its
state (docs/architecture.md §202):

```rust
fn tag_mappings() -> Vec<TagMapping> {
    vec![account("from"), account("to"), transfer()]
}
fn consistency_query() -> Vec<QueryItemMapping> {
    vec![
        // Balance's state for the account in payload.from, and for payload.to
        QueryItemMapping::snapshot::<Balance>("from"),
        QueryItemMapping::snapshot::<Balance>("to"),
        // plain items still work next to them
        QueryItemMapping::types(&["Transferred"]).tagged(vec![transfer()]).latest(),
    ]
}
fn decide_with(payload: &Self::Payload, decision: &Decision<'_, Self::Event>) -> CommandDecision {
    let Some(from) = decision.snapshot::<Balance>(&payload.from) else { /* misconfigured */ };
    if !decision.events().is_empty() { /* transfer id used */ }
    // ...
}
```

- `decision.snapshot::<S>(value)` is the stored state with every later
  event carrying that value already folded in, by the framework, with
  `S`'s own fold - don't fold anything yourself. `decision.events()` is
  what the other items selected.
- Every event carrying a snapshot item's value conflicts with the
  command, and is folded in before it decides again.
- A snapshot folds an event into every value of its key the event
  carries. When the fold depends on which one (a transfer debits `from`
  and credits `to`), implement `Snapshot::fold_for(state, value, event)`
  instead of `fold`.
- The item's field must map under the snapshot's `TAG_KEY` and be one of
  `tag_mappings()`; it takes no event types and no `latest`. `build()`
  checks that, and that the snapshot is registered.
- `decide_with` is optional: its default calls `decide()`, so a command
  without snapshot items keeps implementing `decide()` only.
- The older `snapshot()` + `decide_from_snapshot` pair still works for a
  single-tag command, but new code should use snapshot items.

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
