---
name: skilj
description: >
  Add a new EventType, CommandType, or Projection to an existing skilj
  bounded context - which trait to implement, how its JSON Schema is
  derived, the tag_mappings/sensitive_fields shape, registration via
  #[auto_register], and what a rejected command's error actually means.
  Triggers on "add an event type", "add a command type", "add a
  projection", "register a new event/command", "why did my command get
  rejected with <error>", or reading/extending skilj-demo's banking.rs/
  courses.rs as a pattern. Do NOT use for: creating a brand-new bounded
  context, IdP/access-control/RoleAccessMapping setup, the GraphQL/REST
  wire contract itself, skilj-tui usage, or the cross-instance/inspector
  internals - those are skilj's own implementation, not the plugin API
  this skill covers.
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

There is a fourth plugin type, `Snapshot` - an optional accelerator for
`decide()` against a large per-entity history, its own trait with its
own registration and inspection endpoint - but it's a distinct,
narrower concept, not part of this decision tree, and out of scope for
this skill; see `docs/architecture.md`'s `Snapshot` section if that's
what you're looking for.

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
