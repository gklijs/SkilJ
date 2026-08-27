# `Projection`

A read-optimised view folded from one or more event types - the other
piece of real logic in a bounded context, alongside `decide()`.

```rust
pub trait Projection {
    type State: Serialize + DeserializeOwned + JsonSchema + Default;
    type Event: BoundedContextEvent; // same shared enum command-type.md describes

    const NAME: &'static str;
    const BOUNDED_CONTEXT: &'static str = DEFAULT_BOUNDED_CONTEXT; // see registration.md

    fn consumed_event_types() -> Vec<&'static str>; // no default - must be declared
    fn sync() -> bool { false }
    fn keys(_event: &Self::Event) -> Vec<String> { vec![String::new()] }

    fn project(state: &mut Self::State, event: &Self::Event, key: &str);
}
```

Real example - `skilj-demo/src/banking.rs`'s `AccountBalance`:

```rust
#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct AccountBalanceState {
    pub balance: i64,
}

pub struct AccountBalance;

#[auto_register(BOUNDED_CONTEXT)]
impl Projection for AccountBalance {
    type State = AccountBalanceState;
    type Event = BankingEvent;
    const NAME: &'static str = "AccountBalance";
    fn consumed_event_types() -> Vec<&'static str> {
        vec!["MoneyDeposited", "MoneyWithdrawn"]
    }
    fn sync() -> bool { true }
    fn keys(event: &Self::Event) -> Vec<String> {
        match event {
            BankingEvent::MoneyDeposited(p) => vec![p.account_id.clone()],
            BankingEvent::MoneyWithdrawn(p) => vec![p.account_id.clone()],
        }
    }
    fn project(state: &mut Self::State, event: &Self::Event, _key: &str) {
        match event {
            BankingEvent::MoneyDeposited(p) => state.balance += p.amount,
            BankingEvent::MoneyWithdrawn(p) => state.balance -= p.amount,
        }
    }
}
```

## `consumed_event_types()` - no default, must be declared

Unlike `tag_mappings()`/`sensitive_fields()`, there's no reasonable
"consumes nothing" default - and Rust can't infer this from `project()`'s
own body (which `Self::Event` variants it actually matches on isn't
something the type system exposes), so it has to be named explicitly.
Every `EventType::NAME` listed here must already be registered in the
same bounded context - registering a projection that names an
unregistered event type fails the same way a `CommandType` naming one
in `EventSpec` does (`UnregisteredEventType`, see `common-mistakes.md`).

## `project()` - pure, synchronous, folds exactly one event

```rust
fn project(state: &mut Self::State, event: &Self::Event, key: &str);
```

Same purity rule as `decide()` (§1.1: no I/O, no async) and for the
identical reason - it may run as part of history replay, not just live.
Mutates `state` in place for one event at a time; never rebuilds from
scratch itself (that's `RebuildProjection`'s own job, triggered
explicitly by an admin, not implied by a schema change - see
`docs/architecture.md` if you need that flow).

## `keys()` - single-instance vs. multi-instance projections

Defaults to one constant, unnamed key (`""`) - every projection that
never overrides `keys()` has exactly one shared instance, folding every
consumed event into that one `State`. Overriding it (as `AccountBalance`
does, keyed by `account_id`) creates one independent instance per
returned key - if an event's own `keys()` call returns more than one key
(a transfer event naming both a sender and a receiver, say), that one
event folds into *each* of those instances independently, via its own
separate call to `project()`. `key` is what tells those calls apart
inside `project()` when a single event touches more than one instance
(e.g. comparing `key` against the event's own sender/receiver fields to
decide whether this particular fold is a credit or a debit) - ignored
entirely by a projection using the default single-key shape, as
`AccountBalance` above does (it takes `_key`, unused, since each event
only ever names the one account it's about).

## `sync()` - inline vs. background updates

- `true` - updates in the *same transaction* as the event(s) it
  consumes. Reading it back immediately after triggering the command
  that produced it always reflects that command - no polling delay.
  `AccountBalance` is `sync: true` for exactly this reason: an operator
  who just deposited money expects the balance to already be right.
- `false` (default) - updates via a background consumer, eventually
  consistent. Cheaper for a projection nothing needs read-your-writes
  consistency for.

Re-registering a projection with a schema/logic change never replays or
disturbs what it's already serving, whichever `sync` setting - a change
that can't honestly be applied to already-folded content is staged,
waiting for an explicit `RebuildProjection` call, never triggered
automatically by a deploy.
