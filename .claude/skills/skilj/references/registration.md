# Registration

A type implementing `EventType`/`CommandType`/`Projection` does nothing
by itself - it has to be registered onto a `SkiljBuilder` before
`.build()` runs, or it's just an unused Rust type. Two ways to do that;
skilj-demo (and most real apps) use the first exclusively.

## `#[auto_register]` - the normal way

```rust
pub const BOUNDED_CONTEXT: &str = "banking";

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for MoneyDeposited { /* ... */ }
```

Applied directly above the `impl EventType for X`/`impl CommandType for
X`/`impl Projection for X` block. Expands to the impl block itself, plus
an `inventory::submit!` that registers `X` onto *any* `SkiljBuilder`
across the whole linked binary that later calls `.auto_register()` -
order of registration never matters, each type only ever inserts its
own `(bounded_context, NAME)` entry.

**The argument is what scopes it to a bounded context** -
`#[auto_register(BOUNDED_CONTEXT)]` injects
`const BOUNDED_CONTEXT: &'static str = BOUNDED_CONTEXT;` into the impl
block, exactly as if hand-written. The `BOUNDED_CONTEXT` inside the
parentheses is *not* a keyword - it's spliced verbatim as an expression,
and by convention names a `pub const BOUNDED_CONTEXT: &str = "...";`
declared once at the top of the module (`skilj-demo/src/banking.rs`'s
own pattern - every type in that file passes the identical module-level
const, so the whole file is scoped to `"banking"` without repeating a
string literal per type). **This is the one line that changes when
adding a type to an existing bounded-context module**: copy an existing
type's `#[auto_register(...)]` argument exactly, don't invent a new one.

Bare `#[auto_register]` (no argument) leaves `BOUNDED_CONTEXT`
unoverridden - it falls back to the trait's own default,
`DEFAULT_BOUNDED_CONTEXT` (`"default"`). Fine for a genuinely
single-bounded-context app with no module-level const at all. Writing
both the macro argument *and* a hand-written `const BOUNDED_CONTEXT` in
the same impl body is a compile error (a duplicate item) - use one or
the other, never both.

Then, once, wherever the binary builds its `SkiljBuilder` (`skilj-demo`'s
own `skilj_demo::register()` function, called from `server.rs`'s
`main()`):

```rust
let (skilj, report) = Skilj::builder(database_url)
    .auto_register()
    .reconciliation_role(admin_subject)
    .build()
    .await?;
```

`.auto_register()` folds every `inventory::submit!`'d type across the
whole binary onto the builder in one call - you never list types by
name here, adding a new `#[auto_register(...)]`'d type anywhere in the
crate is enough on its own.

## Manual registration - when you're not using the facade's discovery

```rust
let (skilj, report) = Skilj::builder(database_url)
    .bounded_context(bc_name)
    .event_type::<MoneyDeposited>()
    .command_type::<WithdrawMoney>()
    .projection::<AccountBalance>()
    .reconciliation_role(external_subject)
    .build()
    .await?;
```

`.bounded_context(name)` sets which context every following
`.event_type::<T>()`/`.command_type::<T>()`/`.projection::<T>()` call
registers under - it ignores each type's own `BOUNDED_CONTEXT` const
entirely (whether that came from `#[auto_register(...)]`'s argument or
was left at its default), so a type registered this way is scoped by
*where in the chain* it appears, not by anything declared on the type
itself. Call `.bounded_context(...)` again to switch contexts partway
through one chain. This is the shape most of `skilj/tests/*.rs` uses
(deliberately explicit, one builder call site listing exactly what a
test needs) - real applications almost always prefer `#[auto_register]`
instead, since it means a new type is live the moment it's written,
with nothing else to remember to update.

## What "an existing bounded context" requires

Registration alone (either way above) only makes a type *known to this
process*. `RegisterEventType`/`RegisterCommandType`/`RegisterProjection`
still gate on the bounded context existing and being `active`, and on
the caller (`reconciliation_role`, here - the identity the startup
reconciliation loop runs as) holding an admin-level grant on it. A
context created but never granted to anyone registers nothing on any
deploy, however often the process restarts - `report.skipped_no_access`
(returned alongside `skilj` from `.build()`) names anything that hit
this, so check it rather than assuming registration silently succeeded.
Creating the bounded context itself and granting that first access is
outside this skill's scope - see `docs/architecture.md` if you're doing
that, not just adding a type to a context that already works.
