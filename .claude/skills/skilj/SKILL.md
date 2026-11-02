---
name: skilj
description: >
  Add a new EventType, CommandType, or Projection to an existing skilj
  bounded context - which trait to implement, how its JSON Schema is
  derived, the tag_mappings/sensitive_fields shape, registration via
  #[auto_register], and what a rejected command's error actually means.
  Triggers on "add an event type", "add a command type", "add a
  projection", "register a new event/command", "why did my command get
  rejected with <error>", "evolve/rename/version a payload field",
  "SchemaIncompatible", or reading/extending skilj-demo's banking.rs/
  courses.rs as a pattern. Do NOT use for: creating a brand-new bounded
  context, IdP/access-control/RoleAccessMapping setup, the GraphQL/REST
  wire contract itself, skilj-tui usage, or the cross-instance/inspector
  internals - those are skilj's own implementation, not the plugin API
  this skill covers. A uniqueness rule (an email address registered once)
  is the `skilj-uniqueness` skill.
---

# skilj

A skilj bounded context is built from three kinds of Rust types you
implement traits for - never a config file, never generated boilerplate
you then edit. This skill covers adding one of each to a bounded context
that already exists. See `references/registration.md` for what "already
exists" requires.

## Decision tree

- **Something happened, worth recording forever, immutable** → `EventType`.
  Past tense: `MoneyDeposited`, not `DepositMoney`. See
  [references/event-type.md](references/event-type.md).
- **Something a caller asks the system to do, which may be accepted or
  rejected** → `CommandType`. Imperative: `DepositMoney`. Owns the one
  piece of real logic in this whole system - `decide()`. See
  [references/command-type.md](references/command-type.md).
- **A read-optimised view folded from one or more event types** →
  `Projection`. Nouns: `AccountBalance`. See
  [references/projection.md](references/projection.md).

A command's own `decide()` never writes anything directly - it returns
which events to append (or a rejection); skilj appends them, and any
projection consuming that event type folds it in afterward.

**A value that must be unique** (an email address, a username) is a
command whose consistency query reads earlier claims of the value - and,
when the value is personal data, a blind tag (`BlindTag`,
`TagMapping::blind`) so it is only ever stored as a keyed hash. Use the
`skilj-uniqueness` skill for that; don't tag a sensitive field plainly or
hash it by hand.

There is a fourth plugin type, `Snapshot` - an optional accelerator for
`decide()` against a large per-entity history, its own trait with its
own registration and inspection endpoint. It isn't part of this decision
tree: a command uses one through a snapshot item in its consistency
query (see `references/command-type.md`, "snapshot items"), and
`docs/architecture.md` §202 has the design.

## The three traits, at a glance

All three live in `skilj_core::plugin` (re-exported from `skilj` - use
`use skilj::{EventType, CommandType, Projection, auto_register};`).
Every one of them:

- has an associated `type Payload`/`type State` that must derive
  `Serialize + DeserializeOwned + JsonSchema` (`schemars`'s
  `#[derive(JsonSchema)]`, alongside serde's own derives) - the JSON
  Schema stored at registration is *always* derived from this struct,
  never hand-written (see `references/event-type.md` for the shape
  rules this implies).
- has `const NAME: &'static str` - the wire name, and what
  `EventSpec`/`decide()`/GraphQL/REST all address it by.
- has `const BOUNDED_CONTEXT: &'static str`, defaulted to `"default"` -
  see `references/registration.md`, do not override this by hand if the
  file already declares its own `pub const BOUNDED_CONTEXT`.

```rust
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{auto_register, EventType, CommandType, Projection};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MoneyDepositedPayload {
    pub account_id: String,
    pub amount: i64,
}

pub struct MoneyDeposited;

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
}
```

Real, complete example (all three types, one bounded context):
`skilj-demo/src/banking.rs`. Read it before writing your first type -
every reference file below excerpts from it rather than inventing a
separate example, so it's worth having open already.

## Registration in one line

```rust
#[auto_register(BOUNDED_CONTEXT)]
impl EventType for MoneyDeposited { /* ... */ }
```

`BOUNDED_CONTEXT` here is the file's own `pub const BOUNDED_CONTEXT: &str = "banking";`
declared once at the top of the module - not a `skilj_core::plugin`
const you define per type. Full detail, including manual (non-macro)
registration and what happens with no bounded-context module at all:
[references/registration.md](references/registration.md).

## When an event was wrong

Events are immutable, so a wrong one stays in the journal and a later event says
it's no longer endorsed. Opt in with `.retractions()` on the bounded context
(`.retractions_owned_by(tag)` where event types declare an `owner_tag_key`), then
a decision emits `skilj::retraction::retract(position, ground, reason)`, or
`corrected(position, ground, reason, [replacement])` for both in one append. The
store copies the retracted event's tags onto the retraction, so every query that
found the original finds it too.

Use `type Event = Retractable<MyEvent>` to get positions and tags, and
`standing(events)` for what's still endorsed. Don't use `.latest()` on a query
folding this way: `claim_query` reads every match, as this needs. A position is
the caller's to choose, so check in `decide()` that it's one this command may
touch, as `skilj-demo/tests/expenses.rs` does.

What happens to what you've already built on the wrong event:

- **Snapshots:** nothing to do. The store keeps retracted events out of their
  state. `build()` refuses `CommandType::snapshot()` next to retractions.
- **Projections:** not recomputed. One that should reflect it lists
  `RETRACTED_EVENT_TYPE` in `consumed_event_types`, folds `Retractable` events,
  and keeps enough in its state to take a contribution out. A projection that
  doesn't consume it keeps the contribution, silently. The expense report total
  in `skilj-demo/tests/expenses.rs` is the pattern to copy.
- **Deadlines and routes:** not undone. A deadline still fires, so its target
  should read `standing` and refuse. A route's delivery stands, and a replay
  delivers a retracted event too. To cancel a deadline, emit a domain event next
  to the retraction: `CancelDeadline` can't get tags from a retraction.

Under investigation rather than wrong? `.doubts()` adds `doubt(position, reason)`
and `lift_doubt(position, reason)`; a retraction or correction ends a doubt without
a lift. It's only a marker: the event still counts, snapshots don't change, and a
decision that cares lists `DOUBT_EVENT_TYPES` in its query and reads `open_doubts`.
`build()` refuses `CommandType::snapshot()` next to it. Design: §236.

In a `.skilj.toml`: `retractions = true`, `doubts = true`, and `retractable = true`
on the command, which hands its `decide_*` `Retractable` events (see
`skilj-demo/tests/seats.rs`). `inspectEvent` answers `retractedBy` and `doubtedBy`
for tools; `queryEvents` and `GET /v1/events` take `upToSequence` to read as of a
sequence, and a projection type's `asOfSequence` says what an answer reflects (§237).

A dossier, legal basis or source goes in the correction command's payload, which
the retraction's `causation_id` leads to; `reason` is stored in the clear. An
occurrence time is a domain field, `occurred_at` by convention. A retraction
erases nothing: that's `forgetSubject`. Full design: docs/architecture.md §235.

## Testing your `decide()`/`project()`

Once a `CommandType`/`Projection` is written, test its own logic with
`skilj-test-fixture` - given/when/then against `decide()`/`project()`
directly, no database, no HTTP:

```rust
use skilj_test_fixture::command::GivenEvents;

GivenEvents::<WithdrawMoney>::new()
    .event(BankingEvent::MoneyDeposited(MoneyDepositedPayload { account_id: "a".into(), amount: 100 }))
    .when(WithdrawMoneyPayload { account_id: "a".into(), amount: 150 })
    .then_rejected("insufficient_funds");
```

`skilj_test_fixture::projection::GivenEvents` is the `Projection`
counterpart (`.then_state(expected)`). See `skilj-demo/tests/
banking_fixture.rs` for a complete worked example, and
docs/architecture.md §45 for what this deliberately does and doesn't
replace - it's a complement to a real Postgres-backed integration test
(`skilj-demo/tests/banking.rs`), not a substitute for one.

## If a registration or a submission gets rejected

Don't guess at the cause - [references/common-mistakes.md](references/common-mistakes.md)
lists every real error variant this can produce (`InvalidTagMapping`,
`InvalidSensitiveField`, `PayloadDoesNotMatchSchema`, and the rest),
each with what actually causes it and the fix, taken directly from
`skilj-core`'s own source rather than guessed.

## What this skill does not cover

Creating a bounded context itself (`AddBoundedContext`, superadmin/
access-control setup), the GraphQL/REST wire contracts, `skilj-tui`,
and anything under `skilj-core`/`skilj-graphql`/`skilj-rest`'s own
internals - those aren't the plugin API a consuming application touches.
`docs/architecture.md` is the real reference for all of that.
