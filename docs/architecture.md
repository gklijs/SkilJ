# SkilJ Architecture

This document captures the Rust-level implementation design for SkilJ:
how the library is put together in code. It is a companion to
[`specs/skilj.allium`](../specs/skilj.allium), not a replacement for it —
the Allium spec is the source of truth for *behaviour* (what SkilJ
guarantees, what every surface does, every rule's preconditions and
effects). This document is the source of truth for *shape*: crate
structure, trait signatures, which library each concern is built on, and
why. Where the two could be read as disagreeing, the Allium spec wins;
update this document to match it, not the other way round.

Status: living document, filled in incrementally as design decisions are
made in conversation. Sections marked **Open** have not been decided yet.

---

## 1. The plugin API: `decide()` and `project()`

These are the two black boxes the Allium spec names explicitly as
"plugged-in, bounded-context-specific" logic — nearly everything else in
the library (the reconciliation loop, the GraphQL/REST surfaces,
`read_projection`) exists to get the right data to these two functions
and store what they hand back. Their shape drives most of what follows.

### 1.1 Both are synchronous

Per the spec, `decide()` receives only `matching_events` and the
submitted payload — no direct database access, no I/O. `project()` folds
exactly one event into one projection's in-memory state. Neither needs
`async fn`. This is deliberate, not an oversight: `ProcessCommand`'s
optimistic-then-locked retry pattern (see the guidance note above that
rule) may call `decide()` more than once per submission, and keeping it a
pure, synchronous computation over data the caller already assembled
avoids async-trait ergonomics entirely on the hottest path in the
library.

### 1.2 JSON Schema is derived from the Rust type, not hand-written

The reconciliation-loop note in the spec says event/command/projection
schemas are "derived from the Rust type and projection definitions
themselves." SkilJ takes that literally: a payload is a plain Rust struct
carrying `#[derive(Serialize, Deserialize, JsonSchema)]`
([`schemars`](https://docs.rs/schemars)), and the JSON Schema text
`EventType.schema`/`CommandType.schema`/`Projection.schema` store is
generated from it. One definition, not two kept in sync by hand.

### 1.3 Trait-per-type, not closures, not a builder DSL

```rust
struct WithdrawMoney;

impl CommandType for WithdrawMoney {
    type Payload = WithdrawPayload; // #[derive(Serialize, Deserialize, JsonSchema)]
    const NAME: &'static str = "WithdrawMoney";

    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping::new("account", "account_id")]
    }
    fn sensitive_fields() -> Vec<SensitiveField> { vec![] }
    fn rest_trigger_allowed() -> bool { false }

    fn decide(payload: &Self::Payload, matching_events: &[BankingEvent]) -> CommandDecision {
        // ...
    }
}
```

One `impl` per `EventType`/`CommandType`/`Projection`, following the
standard idiomatic Rust plugin pattern (the same shape `diesel` models or
ECS systems use). Chosen over closures because a trait impl is
independently constructible and testable — call `WithdrawMoney::decide`
directly in a unit test with no framework wiring — and holds up if a
derive macro gets layered on top later without changing the underlying
shape.

**Decided: no macros for the plugin API itself, until asked.** Ship the
plain trait shape first, hand-written. A derive/attribute macro to cut
per-type boilerplate (`#[skilj::command_type]` or similar) is worth
revisiting once real usage shows what's actually tedious — designing it
now, before any type has been written by hand, risks locking in the wrong
ergonomics. §1.3.1/§1.3.2 below are both real proc-macros that predate any
revisit of this decision, and neither reopens it on their own —
`EventType`/`CommandType`/`Projection` impls stayed entirely hand-written
trait code through both, with no codegen step of their own. §1.3.3 is the
one exception: a real per-type codegen macro over these traits, added
later, once asked for directly (see that section for the full reasoning).

### 1.3.1 `#[requires_role(...)]`

A `CommandType` can declare an extra, caller-facing role-name gate on top
of the ordinary write-level `RoleAccessMapping` check — some commands
need to be restricted to a specific role beyond "anyone with write access
to this bounded context." The user asked for this to read as an
annotation on the command's own declaration, not a trait method its
author has to remember to override, so it's a real
`#[proc_macro_attribute]`, `skilj-macros::requires_role`, applied
directly above the `impl CommandType for ...` block:

```rust
#[requires_role("treasury_officer")]
impl CommandType for WithdrawMoney {
    type Payload = WithdrawPayload;
    type Event = BankingEvent;
    const NAME: &'static str = "WithdrawMoney";
    fn decide(payload: &Self::Payload, matching_events: &[BankingEvent]) -> CommandDecision {
        // ...
    }
}
```

It expands to nothing more than overriding `CommandType::required_role()`
(a new trait method, `None` by default — same register as
`rest_trigger_allowed()`) to return `Some("treasury_officer")`. Every
other plugin trait method stays exactly as hand-written as before; this
doesn't reopen the general "no macros" decision, generate any other
boilerplate, or touch `NAME`/`decide()`/schema derivation.

Two things worth being explicit about:

- **Not a spec-level concept, not persisted anywhere.** `required_role()`
  never reaches `specs/skilj.allium`, the `command_types` table, or
  `RegisterCommandType` — it's carried only in the in-memory registry
  `skilj`'s builder already keeps (`RegisteredCommandType`), read back out
  through a new `CommandDispatcher::required_role(bounded_context,
  command_type) -> Option<Option<&'static str>>` method (outer `None` =
  "not registered at all," mirroring `dispatch`'s own convention).
- **Checked by `skilj-graphql`'s mutation resolver only, once it exists
  (§8 item 5)** — before calling `CommandDispatcher::dispatch` at all, so
  an unauthorised caller never reaches `decide()`. REST triggering is
  untouched: `CommandToken` is already its own, separate per-token
  capability grant, and paying for this check there would be redundant.
- **A real caveat, not glossed over**: `Role.name` carries no uniqueness
  guarantee anywhere in `specs/skilj.allium` (see `entity Role`) — this
  check is only as trustworthy as a deployment's own discipline in
  keeping role names meaningful and non-colliding. `skilj-core` doesn't
  and can't enforce that; it's a caller-managed convention layered on
  top, the same register as choosing sensible `EventType`/`CommandType`
  names in the first place.

### 1.3.2 `gql_object!` — internal wire-type codegen, a different register entirely

A second `skilj-macros` proc-macro, `#[proc_macro] gql_object!`, added
later once `skilj-graphql/src/gql_types.rs` had grown to ~20 hand-written
`async_graphql::dynamic::Object` builders, each just a repetitive
`Object::new("X").field(scalar_field("name", type, |x| expr))...` chain
— genuinely declarative data dressed up as procedural code, not a case
§1.3's caution about designing prematurely applied to at all (the
tedium here was real and already visible, not speculative). A field
declares its own kind and is spliced into the matching
`scalar_field`/`object_field`/`list_field` helper call:

```rust
pub fn role_object() -> Object {
    gql_object!(Role => "Role" {
        scalar "id": TypeRef::named_nn(TypeRef::ID) => |r| Value::from(r.id.clone()),
        scalar "revokedAt": TypeRef::named(TypeRef::STRING) => |r| optional_timestamp(r.revoked_at),
        object "role": TypeRef::named_nn("Role") => |m| Some(m.role.clone()),
        list "tagMappings": TypeRef::named_nn_list_nn("TagMapping") => |et| et.tag_mappings.clone(),
    })
}
```

Worth being explicit about how this differs from §1.3.1, not just that
both are proc-macros:

- **A different crate, a different audience.** `requires_role` sits in
  `skilj-core`'s own plugin API — a library *consumer* writes it.
  `gql_object!` is used only inside `skilj-graphql`'s own
  `gql_types.rs`, generating the identical `Object`/`Field` values a
  hand-written call already built; no plugin author ever sees or writes
  it. §1.3's "no macros for the plugin API" is untouched by this either
  way.
- **Why not `async_graphql`'s own `#[derive(SimpleObject)]`?**
  `gql_types.rs`'s own module doc comment already answers this: this
  version of `async-graphql` has no bridge from that derive macro's
  static output into the `dynamic::Schema` this codebase assembles at
  runtime from plugin registrations (§5.1). `gql_object!` is a bespoke
  local replacement for that missing bridge, not a reimplementation of
  something upstream already offered.
- **Real syntax-tree work, not `macro_rules!`.** Each field's closure is
  written bare (`|r| ...`, no `: &Role`) - `gql_object!` parses it as a
  real `syn::ExprClosure` and splices the type ascription into its one
  parameter itself, since plain type inference can't resolve a bare
  closure passed to a generic `Fn(&T) -> _` parameter. That's real
  syntax-tree manipulation, past what `macro_rules!` token-matching can
  express - the same "needs actual inspection, not just substitution"
  reasoning `requires_role`'s own `impl CommandType for ...` check
  already relies on.

### 1.3.3 `#[auto_register]` + `BOUNDED_CONTEXT` — reopening §1.3's "no macros" call, deliberately

Unlike §1.3.1/§1.3.2, this one *does* reopen §1.3's original "no macros for
the plugin API itself" decision — at the user's own explicit request, not
a call this codebase made unilaterally. The user was told directly that
§1.3.1/§1.3.2 were both scoped to not reopen it, and chose to do so anyway
(the same register the 2026-08-19 pass that added `gql_object!` already
set a precedent for: revisit deliberately, when asked, not speculatively).

Two small, independent additions, both purely additive — nothing about
existing `SkiljBuilder` usage (`skilj-demo`'s own `banking`/`courses`
modules included) changes unless a type opts in:

**`BOUNDED_CONTEXT`, a new defaulted associated const** on `EventType`/
`CommandType`/`Projection` (`skilj-core::plugin`), alongside the existing
`NAME`:

```rust
const BOUNDED_CONTEXT: &'static str = DEFAULT_BOUNDED_CONTEXT; // "default"
```

A multi-context app overrides it the same way `skilj-demo`'s `banking.rs`/
`courses.rs` already declare a module-level `pub const BOUNDED_CONTEXT`,
pointing the associated const at that module const — in practice, always
via `#[auto_register(BOUNDED_CONTEXT)]`'s shorthand argument (below)
rather than a hand-written `const BOUNDED_CONTEXT = ...` line inside every
impl. This is consulted in exactly one place — `SkiljBuilder::
auto_register()` below — and nowhere else; manual `.bounded_context(name)
.event_type::<T>()` chaining ignores it entirely.

**`SkiljBuilder` itself now defaults `current_bounded_context` to
`DEFAULT_BOUNDED_CONTEXT`** rather than requiring a `.bounded_context(...)`
call before the first `.event_type::<T>()`/`.command_type::<T>()`/
`.projection::<T>()` (which used to panic otherwise). A single-bounded-
context app can now skip `.bounded_context(...)` entirely and every
manually-chained registration lands under `"default"`.

**`#[auto_register]`**, a third `skilj-macros` proc-macro, applied above
an `impl EventType for X`/`impl CommandType for X`/`impl Projection for X`
block:

```rust
#[auto_register]
impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
    // BOUNDED_CONTEXT left at its default ("default"), or overridden as above
}
```

expands to the impl block unchanged, plus one `inventory::submit!` (the
`inventory` crate — typed, `ctor`-based distributed plugin registration;
new workspace dependency, re-exported as `skilj::inventory` so a
`#[auto_register]`-using crate needs only its existing `skilj` dependency)
registering a small `fn(SkiljBuilder) -> SkiljBuilder` closure that does
exactly `b.bounded_context(X::BOUNDED_CONTEXT).event_type::<X>()`.
`SkiljBuilder::auto_register()` folds every closure `inventory` collected
across the whole linked binary into `self` — order doesn't matter, since
each closure only ever inserts its own `(bounded_context, NAME)` entry,
same as any other builder call. A minimal single-bounded-context app can
now be as little as:

**Shorthand argument for the common multi-context case**:
`#[auto_register(EXPR)]` injects `const BOUNDED_CONTEXT: &'static str =
EXPR;` into the impl block itself, so a bounded-context module never has
to spell out a full const declaration per type — only the one module-level
`pub const BOUNDED_CONTEXT` a file like `skilj-demo`'s `banking.rs`
already declares, referenced once per type via the argument alone:

```rust
pub const BOUNDED_CONTEXT: &str = "banking";

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
    // ...
}
```

`EXPR` is spliced verbatim into the injected const's initializer, so
anything valid there works, not just a bare path. Combining the argument
with a hand-written `const BOUNDED_CONTEXT` inside the same impl is a
macro-time compile error (a clear diagnostic instead of a confusing
"duplicate associated const" from the compiler after expansion) — pick
one or the other, never both.

```rust
let (skilj, _report) = Skilj::builder(database_url)
    .auto_register()
    .reconciliation_role(external_subject)
    .build()
    .await?;
```

with no `.bounded_context(...)`/`.event_type::<T>()`/`.command_type::<T>()`/
`.projection::<T>()` calls anywhere, as long as every plugin type in the
binary is `#[auto_register]`-tagged.

**Facade-only, unlike `requires_role`.** `requires_role` expands to a
trait-method override alone, so a `skilj-core`-only consumer (no `skilj`
facade) can use it directly. `#[auto_register]`'s whole point is
registering onto `skilj::SkiljBuilder`, so its expansion necessarily names
`skilj`'s own `EventTypeRegistrar`/`CommandTypeRegistrar`/
`ProjectionRegistrar` marker types — a `skilj-core`-only consumer can't use
this attribute, the same boundary that consumer already accepts by
hand-rolling its own `CommandDispatcher`/`ProjectionDispatcher`/
`EventDispatcher` (§1.7's own note on this).

Auto-registration and manual chaining are fully interoperable within one
`SkiljBuilder` — a bounded context can mix `#[auto_register]`-tagged types
with manually `.event_type::<T>()`-chained ones freely; `.auto_register()`
can be called before, after, or interleaved with manual calls.

Deliberately *not* changed by this pass: reconciliation itself (§1.5) is
untouched — a `#[auto_register]`-tagged type still needs the
reconciliation role to hold an active admin `RoleAccessMapping` on its own
`BOUNDED_CONTEXT` before it registers for real, exactly like manual
registration already requires; auto-registration only removes the
per-type `.event_type::<T>()` call, not the access-control gate around
what that call is allowed to do.

### 1.4 `matching_events` is a generated per-bounded-context enum

`decide()`'s `matching_events` can span multiple `EventType`s at once —
that is the entire point of the dynamic consistency boundary. Rather than
an untyped `&[(String, serde_json::Value)]`, SkilJ generates one enum per
bounded context with a variant per registered `EventType`:

```rust
enum BankingEvent {
    MoneyDeposited(MoneyDepositedPayload),
    MoneyWithdrawn(MoneyWithdrawnPayload),
    // ...
}
```

`decide()` pattern-matches exhaustively over this enum. The compiler
catches a missed case the moment a new `EventType` is registered into the
bounded context — a correctness property a dynamic `Value`-based
representation can't give for free. This is the same register as the
Allium spec's own `Event.origin` sum type: a mixed-shape collection
becomes an enum, not a tagged blob.

### 1.5 Startup registration is a builder

Matches the "compiled into the binary, reconciled on every startup"
framing already in the spec (see the note above `rule AddBoundedContext`
/ `RegisterEventType` on the reconciliation loop):

```rust
let skilj = Skilj::builder()
    .bounded_context("banking")
        .event_type::<MoneyDeposited>()
        .event_type::<MoneyWithdrawn>()
        .command_type::<WithdrawMoney>()
        .projection::<AccountBalance>()
    .reconciliation_role(external_subject: "service-account@example.com")
    .build()
    .await?;
```

**Reconciliation authenticates as a real, pre-existing Role — never a
system bypass.** `RegisterEventType`/`RegisterCommandType`/
`RegisterProjection` require a genuine `AdminAccess` grant, and the spec
already locks in that there is no self-service way around that (see the
note above `rule AddBoundedContext` on the bounded-context-access
sequencing gap: a freshly created bounded context is inert until an
explicit `GrantRoleAccessMapping`, deliberately, not an oversight). A
system-identity bypass for reconciliation would quietly reopen exactly
that door, so it isn't one: `.reconciliation_role(external_subject)`
names an existing Role by its `external_subject` — the same identifier
every other identity resolution in the spec keys on — and at startup
`skilj-core::bootstrap` looks that Role up directly (no JWT is involved;
this is an internal process, not an incoming request, so there's nothing
to verify a signature against). Reconciliation then calls
`RegisterEventType` etc. exactly as if that Role's own `RoleAccessMapping`
were being used by a human admin over GraphQL — because it is the same
rule, the same grant, just automated. A bounded context that Role has no
admin access to is skipped for that pass, not specially handled.

`.reconciliation_role(...)` is optional. Omitting it means reconciliation
is skipped entirely and cleanly — not an error — which matters for
`skilj-core`-only usage (tests, non-web bindings) that never wants it to
run.

**Reconciliation runs automatically inside `.build()`.** Matches the
spec's own framing directly (the loop "can run on every startup"), and is
the least surprising default. `.build()` returns
`(Skilj, ReconciliationReport)` (or an equivalent field on the `Skilj`
handle) so what got registered or skipped is observable, not a silent
side effect a caller has to go looking for.

**A genuine registration rejection fails `.build()` outright.** "Skipped
because the reconciliation Role has no admin access to this bounded
context yet" is an ordinary, expected state and never fails anything —
some contexts are legitimately not granted yet. A rejection for a real
reason instead — most importantly `schema_is_backwards_compatible`
failing, meaning the compiled binary's types have diverged incompatibly
from what's stored — is a bug, not a transient state, and the spec is
already zero-tolerance about it ("an incompatible change is rejected
outright... there is no accept-it-anyway path"). `.build()` returns
`Err` for that case rather than starting the process in a state where the
binary's own types don't match what got registered. This is a
consequence of the general principle, not a special case invented for
reconciliation — the same rejection reaches a human admin calling
`RegisterEventType` by hand over GraphQL exactly the same way.

**Done.** `roles`/`role_access_mappings` tables and their `skilj-core::db`
functions exist, and `.build()`'s reconciliation loop is implemented for
real in `skilj::SkiljBuilder` and verified end-to-end against a real
Postgres (§8 item 2, `skilj/tests/reconciliation.rs`). One clarification
against the paragraph above worth recording: `.build()` treats a
`.reconciliation_role(...)` naming no active Role at all as a genuine
`Err` (`access_control::Error::UnrecognisedSubject`), not a silent skip -
only *omitting* `.reconciliation_role(...)` entirely skips reconciliation
cleanly. A misconfigured `external_subject` is a real problem worth
surfacing, not indistinguishable from the "haven't set this up yet" case.

### 1.6 Raw `Event` → typed `Event`: the `BoundedContextEvent` trait

§1.4 called `BankingEvent` (or whichever per-bounded-context enum) SkilJ
"generates" — true at the design level, but §1.3 also decided **no
macros for now**, so nothing actually generates that enum's *source
code*. The app author hand-writes it, the same way they hand-write every
other `impl EventType`/`CommandType`/`Projection`. What was still
undecided is the other half: `decide()` receives `matching_events:
&[Self::Event]`, but every event this engine actually stores or loads is
the untyped `event_store::Event` (a JSON `payload: String` plus an
`EventType` reference) — something has to bridge from one to the other,
and nothing did.

Resolved: one small trait, implemented once per bounded context on that
context's own hand-written enum, matching the existing "hand-written,
one `impl` per type" register rather than inventing a second mechanism:

```rust
pub trait BoundedContextEvent: Sized {
    /// `None` when `event.event_type.name` doesn't match any variant
    /// this enum declares - never expected in practice (a caller only
    /// ever passes this bounded context's own events), so `None` is a
    /// defensive case, not a designed-for one. `Some(Err(..))` when the
    /// stored payload doesn't deserialize into the matched variant's
    /// payload type - narrower than it used to be (an externally- or
    /// directly-created event's own payload is now schema-checked before
    /// it's ever stored), but still reachable: a command-triggered or
    /// system-triggered event's payload came from `decide()`/
    /// `scheduled_payload`, which are never schema-checked at all.
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>>;
}
```

```rust
impl BoundedContextEvent for BankingEvent {
    fn try_from_event(event: &Event) -> Option<Result<Self, serde_json::Error>> {
        match event.event_type.name.as_str() {
            "MoneyDeposited" => Some(serde_json::from_str(&event.payload).map(Self::MoneyDeposited)),
            "MoneyWithdrawn" => Some(serde_json::from_str(&event.payload).map(Self::MoneyWithdrawn)),
            _ => None,
        }
    }
}
```

`CommandType::Event`/`Projection::Event` (currently a bare `type Event;`
in `skilj-core::plugin`) each grow a `: BoundedContextEvent` bound once
this is implemented — the trait itself is the contract §1.7's dispatch
bridge builds on, not an optional convenience.

### 1.7 The builder's internal registry, and the decide()-dispatch bridge

The other missing half: `SkiljBuilder::event_type::<T>()`/
`command_type::<T>()`/`projection::<T>()` are `todo!()` stubs today (they
don't even record `T`). Resolved shape, all living in the `skilj` facade
crate (not `skilj-core` — the type-erasure boundary the note above rule
`ProcessCommand` describes is exactly this registry, so it belongs on the
far side of it, in application-shaped code, per §3.1's crate split):

- **What gets captured per `.event_type::<T: EventType>()` call**: a
  plain data record — `T::NAME`, `T::Payload`'s JSON Schema (via
  `schemars::schema_for!`, serialised with `serde_json::to_string` — the
  same "derived from the Rust type" register §1.2 already committed to),
  `tag_mappings()`, `sensitive_fields()`, `external_creation_allowed()`,
  `direct_creation_allowed()`, `event_read_allowed()`. Enough to call
  `event_store::register_event_type` during reconciliation, nothing
  dispatched at runtime (an `EventType` is pure declaration - see its own
  doc comment). `system_triggered_allowed`/`system_triggered_schedule`
  (fields the full `EventType` entity has but the plugin trait doesn't
  expose) stay hard-defaulted to `false`/`None` here — the scheduler that
  would consult them doesn't exist yet either (same register as
  `EventOrigin::SystemTriggered` itself, still unmodelled), so there's no
  builder-exposed knob for a feature nothing yet reads.
- **What gets captured per `.command_type::<T: CommandType>()` call**:
  the same kind of record (`T::NAME`, schema, `tag_mappings()`,
  `sensitive_fields()`, `rest_trigger_allowed()`) for registration, *plus*
  a boxed, type-erased decider:

  ```rust
  type DeciderFn = Box<dyn Fn(&str, &[Event]) -> skilj_core::error::Result<CommandDecision> + Send + Sync>;

  fn decider<T: CommandType>() -> DeciderFn {
      Box::new(|payload_json, raw_events| {
          let payload: T::Payload = serde_json::from_str(payload_json)
              .map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?;
          let mut matching = Vec::with_capacity(raw_events.len());
          for event in raw_events {
              if let Some(converted) = T::Event::try_from_event(event) {
                  matching.push(converted.map_err(|e| EventStoreError::PayloadDecodeFailed(e.to_string()))?);
              }
          }
          Ok(T::decide(&payload, &matching))
      })
  }
  ```

  This is the whole dispatch bridge: it closes over `T` alone (no runtime
  state), so it's built once, at `.command_type::<T>()` call time, and
  stored keyed by `(bounded_context, T::NAME)`. `CommandTrigger`'s
  handler (still to be wired — see §8) becomes: resolve the token → look
  up the decider by `(bounded_context, command_type_name)` → call it with
  the raw payload string and this bounded context's matching events
  (`consistency_boundary_and_matching_events`, unchanged) → feed the
  resulting `CommandDecision` into `event_store::process_command` exactly
  as already implemented.
- **New library-level error**: `event_store::Error::PayloadDecodeFailed(String)`
  — the "stored data doesn't match what the compiled binary expects"
  case §1.6 flags as reachable. Same closed-set-of-defensive-checks
  register as `Error::UnregisteredEventType` right above it in that enum;
  renders through the existing `SkiljRejection` trait unchanged, no new
  wire-contract work needed on either GraphQL or REST.
- **`.projection::<T: Projection>()`** captures the equivalent
  registration record (schema from `T::State`, `sync()`) now, for
  consistency — a `project()` dispatch closure of the identical shape is
  a natural follow-up once something actually drives projections forward
  (still unmodelled per `crate::projections`' own doc comment), but isn't
  built this pass since nothing calls it yet.
- **Storage**: `Skilj` holds `HashMap<(String, String), RegisteredEventType>`/
  `HashMap<(String, String), RegisteredCommandType>`/
  `HashMap<(String, String), RegisteredProjection>`, each keyed by
  `(bounded_context, name)` — plain data plus (for command types) the one
  boxed closure above. No trait objects beyond that one `Fn`; everything
  else is concrete data collected eagerly at each builder call.
- **`resolve_event_type`** (the caller-supplied closure
  `event_store::process_command` already takes) is a lookup into this
  same `event_types` map, scoped to the command's own bounded context —
  the registry built here *is* that registry, not a second one.

**New persistence need, not yet built**: `process_command`'s
`consistency_boundary_and_matching_events` wants every `Event` in a
bounded context matching a set of tags, not one `event_type`'s events —
`db::list_events` today is scoped to `(bounded_context, event_type_name)`
(all that `EventFetch`'s routes needed). A `db::list_events_for_bounded_context`
(or equivalent) is a straightforward addition once `CommandTrigger`'s
implementation pass actually happens — no design question, just not
written yet. Tracked in §8.

---

## 2. Ecosystem choices

### 2.1 Web framework: `axum`

Decided by `async-graphql-axum` — an official, actively maintained
integration crate pairing directly with the `async-graphql::dynamic` +
`ArcSwap` dynamic-schema approach already chosen for SkilJ (see
[[skilj-language-choice]] in project memory). `axum` is also the
tower/tokio-ecosystem default, giving clean middleware composition for
the auth extraction both SkilJ surfaces need: JWT verification on the
GraphQL track, bearer-token parsing (`Authorization: Bearer <id>.<secret>`)
on the REST track.

### 2.2 Database layer: `sqlx`

SkilJ needs finer control than a typical ORM comfortably gives: raw
locking for `next_sequence` (the black box serialising sequence
assignment per bounded context), JSONB/tag-indexed queries for the
consistency-boundary and `EventQuery`/`CountEvents` lookups, and
compile-time-checked SQL without a full abstraction layer on top of it.
`sqlx` sits at the right level — async-native, strong Postgres support,
`sqlx::query!` compile-time checking as a safety net short of a full ORM.

Its built-in migration tooling (`sqlx::migrate!`) is how SkilJ owns its
own Postgres schema: SkilJ embeds its own migrations, and a consuming
application's own migration runner picks them up. Same "library owns its
own bootstrap, not the consuming application" pattern already locked in
for the superadmin-seeding and admin-bounded-context work in the Allium
spec.

### 2.2.1 Every `Integer` is `i64`/`BIGINT`, uniformly

Allium's `Integer` type is abstract — it carries no bit-width at the
spec level (the language uses it identically for a bounded
`retry_count`/`max_login_attempts` and for a monotonically-growing
`Event.sequence`), so the storage width is entirely this document's
decision, not the spec's. `sequence`-derived fields
(`Event.sequence`, `Command.consistency_boundary`,
`Projection.caught_up_to`, `ProjectionRebuild.caught_up_to`,
`Subscription.from_sequence`, `ReadCursor.sequence`, every
`after_sequence?`/`wait_for_sequence?` argument compared against them,
and `CountEvents`' result) are the field that actually motivates this:
`i32`/`INT` tops out at ~2.1 billion, reachable in weeks at even modest
sustained throughput on a single long-lived bounded context.

Rather than widening only those fields and leaving genuinely bounded
counters (`schema_version` on `EventType`/`CommandType`/`Projection`/
`ProjectionRebuild`, `Metadata.version`) at `i32`, **every `Integer`
maps to Rust `i64` and Postgres `BIGINT`, uniformly, no per-field
judgment call.** The storage cost of 4 extra bytes on a
schema-revision counter is nothing; the risk profile is what decides
it — a field picked too narrow needs a production column-type
migration and a wire-contract-breaking change to fix later, while a
field picked wide that never needed it costs nothing at all. This also
means nobody has to re-litigate "does this new field need to be wide"
for every future `Integer` the spec grows.

### 2.2.2 Schema-per-bounded-context storage

Every table falls into one of two tiers, split by whether it's shared
across bounded contexts or scoped to exactly one:

- **Global** (Postgres schema `public`, tracked by the static
  `sqlx::migrate!` set in `skilj-core/migrations/`): `roles`,
  `bounded_contexts`, `role_access_mappings`, `access_token_index`. Small,
  admin-managed, and either cross-context by nature (`Role` isn't scoped
  to one context; `bounded_contexts` is the registry of contexts itself)
  or a deliberate exception explained below (`access_token_index`).
- **Per-bounded-context** (Postgres schema `bc_<name>`, one per
  `BoundedContext`, provisioned and torn down dynamically in Rust code —
  `skilj_core::db::provision_bounded_context_schema`/
  `hard_delete_bounded_context` — never a `sqlx::migrate!` migration):
  `event_types`, `command_types`, `commands`, `projections`,
  `projection_rebuilds` and their consumed-event-type join tables,
  `access_tokens`, `read_cursors`, `events`, and a single-row `sequence`
  table backing `next_sequence`. Every row scoped to one context lives
  here, in that context's own tables, with its own indexes — one very
  active context's query and index-maintenance load never touches
  another's.

This buys two things at once: **query/index isolation** (a hot context's
`events` table and its indexes are physically separate from every other
context's, so growth in one doesn't degrade lookups in another the way
one shared table filtered by a `bounded_context` column would), and a
**genuinely atomic hard delete** — `DROP SCHEMA "bc_<name>" CASCADE`
removes every one of that context's tables, rows and indexes in a single
statement, instead of an error-prone sweep of `DELETE ... WHERE
bounded_context = $1` across a dozen tables. See `DeleteBoundedContext`
in `specs/skilj.allium` and `bootstrap::delete_bounded_context` (the pure
gate: superadmin caller, context already `archived`, never `admin`) for
the rule this backs.

The `bc_` prefix on every per-context schema name is what keeps a
context from ever colliding with a real Postgres schema (`public`,
`pg_catalog`, …) purely by being named the same thing — `pg_catalog`
itself, say. Combined with `AddBoundedContext`'s own new `requires:
valid_bounded_context_name(name)` (lowercase letters/digits/underscores,
starting with a letter, capped short enough to leave room for the
prefix under Postgres's 63-byte identifier limit), a context's `name` is
safe to interpolate directly into `CREATE SCHEMA`/`DROP SCHEMA` — the
double-quoting `schema_ident` still applies around it is defence in
depth, not the only thing standing between this and SQL injection.

Provisioning and deprovisioning are each one Postgres transaction:
`insert_bounded_context` creates the new schema and every per-context
table inside it, then inserts the `bounded_contexts` registry row, all
in one transaction — a failure partway through leaves neither an
orphaned schema nor a dangling registry row, since Postgres DDL
(`CREATE SCHEMA`/`CREATE TABLE`) is transactional and rolls back like
any other statement. `hard_delete_bounded_context` is the same shape in
reverse: `DROP SCHEMA ... CASCADE` then `DELETE FROM bounded_contexts`,
one transaction — whose own `ON DELETE CASCADE` FK cleans up that
context's `role_access_mappings` rows automatically, no separate cleanup
query needed.

One consequence needed its own small global table: `access_tokens` moving
into each context's own schema breaks `skilj-rest`'s bearer-credential
lookup, since `Authorization: Bearer <id>.<secret>` carries only the
token's `id` — no bounded context alongside it to say which schema to
search. `access_token_index` (`id TEXT PRIMARY KEY, bounded_context TEXT
NOT NULL REFERENCES bounded_contexts (name) ON DELETE CASCADE`) is the
fix: one cheap global row per token, resolved first to learn which
schema holds the full row — the same "small, cross-cutting table stays
global" reasoning `role_access_mappings` already gets above. `read_cursors`
needed no equivalent treatment: every caller reaching `get_read_cursor`
already holds a fully-resolved `EventReadToken` (bounded context already
known) by that point, since resolving the token itself is what already
went through `access_token_index`.

### 2.3 Property-based testing: `proptest`

Standard choice for Rust; matches the `allium:propagate` skill's own
default mapping for this language.

### 2.4 Configuration: a plain builder, not a config-loading crate

`Skilj::builder().jwks_endpoint(url).issuer(...).cache_warmup_count(1000)`
— SkilJ takes concrete values. How a consuming application sources them
(env vars, a config file, a secrets manager) is entirely its own concern,
never SkilJ's. Consistent with the "library, not a framework" positioning
the Allium spec keeps to throughout (see the scope header's exclusion of
IdP trust configuration values as "runtime/library configuration...
rather than modelled as domain state").

---

## 3. Crate and module structure

### 3.1 Four crates, not one

A workspace of four crates: `skilj-core` (the domain engine, zero
web-framework dependency), `skilj-graphql` and `skilj-rest` (the two
surfaces, each independently usable), and `skilj` (a thin facade for the
common case of wanting both). A fifth, `skilj-macros`, sits underneath
`skilj-core` as an implementation detail, not a fifth thing a consumer
chooses to depend on directly — it exists solely to provide
`#[requires_role(...)]` (§1.3.1), re-exported through
`skilj_core::plugin` so nothing outside `skilj-core`'s own `Cargo.toml`
ever names it.

Splitting `skilj-core` out on its own was decided because it pays for
itself twice: it keeps the domain engine's own test suite (in particular
everything `allium:propagate` generates in "spec mode" — entity, rule,
invariant and transition-graph tests) free of `axum`/`async-graphql`
compile time, and it means someone can build an entirely different
binding (gRPC, a CLI, a different web framework) against `skilj-core`
directly without carrying either surface crate.

Splitting `skilj-graphql` and `skilj-rest` further apart (rather than one
combined surface crate) was a deliberate choice to make each
independently usable/omittable — a deployment that only ever wants the
GraphQL track (say, an internal admin tool with no machine callers) can
depend on `skilj-core` + `skilj-graphql` alone and never pull in the REST
routing at all, and vice versa.

`skilj` itself is thin: it depends on all three and exists only to give
the common "I want both surfaces" case one crate to add and one
`Skilj::builder()` to call. A consumer who only wants one surface skips
it and depends on the relevant pair directly.

### 3.2 `skilj-core`: grouped by domain concern

```
skilj-core/src/
├── access_control/  -- Role, RoleAccessMapping, AccessToken (+4 variants);
│                       CreateRole, RevokeRole, GrantRoleAccessMapping,
│                       RevokeRoleAccessMapping, TokenRevocation rules;
│                       actor resolution (ReadAccess/WriteAccess/
│                       AdminAccess/SensitiveDataAccess/Superadmin) and
│                       the JWT-to-Role identity resolution entry point
├── event_store/     -- BoundedContext, EventType, CommandType,
│                       Event (+4 variants), Command, EncryptionKey;
│                       ProcessCommand, RegisterEventType,
│                       RegisterCommandType, CreateExternalEvent,
│                       CreateDirectEvent, FetchEvents, FetchCommands,
│                       QueryEvents/CountEvents/InspectEvent,
│                       ForgetSubject rules; protect_sensitive_fields,
│                       derive_tags, valid_filters, valid_tag_mappings,
│                       valid_sensitive_fields,
│                       schema_is_backwards_compatible, next_sequence,
│                       render_event, render_command; the in-memory
│                       per-bounded-context event cache; Subscription
│                       (+2 variants) and its rules live here too rather
│                       than as a fifth top-level module - delivering
│                       from the event stream is tightly coupled enough
│                       to event_store to not earn its own module, though
│                       this is a low-stakes call worth revisiting once
│                       real code shows whether it feels crowded
├── projections/     -- Projection, ProjectionRebuild; RegisterProjection,
│                       RebuildProjection, DiscardProjectionRebuild,
│                       QueryProjection rules; project(), read_projection(),
│                       await_projection_caught_up()
├── bootstrap/       -- BootstrapSecret, ContextCreator/SystemCreator;
│                       CreateSuperadmin, AddBoundedContext,
│                       ArchiveBoundedContext, ListBoundedContexts rules;
│                       the startup reconciliation loop (this is where
│                       the Open item in §1.5 lives now - the
│                       reconciliation-loop entry point and its
│                       interaction with the bounded-context-access
│                       sequencing gap both belong here)
├── shared/          -- Filter, Metadata, Tag, TagMapping, SensitiveField,
│                       CommandDecision, EncryptedPayload;
│                       generate_token_secret, secret_matches,
│                       generate_token_id - crypto primitives reused by
│                       both access_control's AccessToken and
│                       bootstrap's BootstrapSecret, which is why they
│                       live here rather than in either
├── plugin/          -- the public CommandType/EventType/Projection
│                       traits from §1 - the one module every consuming
│                       application's own code touches directly
├── db/              -- sqlx queries and migrations, persistence for
│                       every module above
└── error.rs
```

### 3.3 `skilj-graphql` and `skilj-rest`

```
skilj-graphql/src/
├── schema.rs    -- async-graphql::dynamic schema construction from
│                   EventType/CommandType/Projection.schema, held
│                   behind ArcSwap
├── resolvers/   -- one module per GraphQL surface: ProjectionQuery,
│                   CommandQuery, EventQuery, EventSubscription,
│                   CommandSubmission, TypeRegistration,
│                   AccessManagement, BoundedContextCreation,
│                   BoundedContextDirectory, SuperadminBootstrap,
│                   TokenRevocation, BoundedContextArchival,
│                   SubjectErasure
└── auth.rs      -- JWT extraction, delegates identity resolution to
                    skilj-core::access_control

skilj-rest/src/
├── routes/      -- ExternalEventIngestion, DirectEventCreation,
│                   EventFetch, CommandTrigger
└── auth.rs      -- bearer-token extraction
                    ("Authorization: Bearer <id>.<secret>"), delegates
                    to skilj-core::access_control
```

---

## 4. Error handling

### 4.1 Two tiers, because the spec already draws this line

`CommandDecision.rejection_kind` is explicitly "an opaque `String` rather
than an enum this library enumerates" (see `CommandDecision` in the
spec) — a bounded context's own `decide()` rejects for whatever domain
reason it wants, a set skilj cannot know ahead of time. Everything
skilj's *own* rules reject for instead (a revoked grant, the wrong
bounded context, an incompatible schema, an invalid filter, and so on)
*is* a closed set skilj defines itself. Two tiers, not one:

- **Library-level errors — enumerable, one `thiserror` enum per
  `skilj-core` module**, matching the domain-grouped structure in §3:
  `access_control::Error`, `event_store::Error`, `projections::Error`,
  `bootstrap::Error`. Each lives beside the code that raises it, the same
  cohesiveness reasoning that picked domain-grouped modules over
  spec-mirrored ones in the first place, carried one level down. A
  top-level `skilj_core::Error` aggregates all four via `#[from]`.
- **Business-level rejections — not enumerated, ever.**
  `Error::CommandRejected { reason: String, kind: String }` carries
  `decide()`'s own `rejection_reason`/`rejection_kind` straight through
  unchanged. This is the one variant skilj-core is structurally
  forbidden from narrowing into something more specific, by the spec's
  own design.

### 4.2 One shared trait unifies both tiers for rendering

Both tiers implement one small trait — a `code() -> &str` /
`message() -> String` pair, name still open — so the eventual GraphQL and
REST rendering layers can treat "a library rejection" and "a business
rejection" uniformly, without needing to know which tier produced a
given error. `code()` on a library-level variant is derived from the
variant itself (e.g. `"grant_revoked"`); on `CommandRejected` it's the
decider's own `kind` passed through untouched. This is the one piece of
"error handling conventions" decided now — deliberately, since it
determines how awkward or clean the wire-contract work is later, without
committing to any wire shape itself.

**Explicitly deferred to the GraphQL/REST wire contract pass** (§5,
unchanged scope): how `code()`/`message()` actually become a GraphQL
error's `extensions` object or a REST response's status code and JSON
body. `skilj-core` has no web dependency and doesn't produce either
shape itself — `skilj-graphql` and `skilj-rest` each own a thin
translation layer from `skilj_core::Error` (or a surface's more specific
error, where one exists) to their own wire format.

---

## 5. The GraphQL wire contract

The spec itself name-drops "the unified-graph naming scheme" in a couple
of guidance notes (`ProjectionQuery`, `CommandQuery`) without ever
defining it — a deliberate forward reference to exactly this pass, not
an oversight.

### 5.1 One unified schema, rebuilt at runtime

Since `async-graphql::dynamic` builds a `Schema` object from data rather
than from compile-time derive macros (the whole reason it was chosen —
see [[skilj-language-choice]]), `skilj-graphql` walks every registered
`EventType`/`CommandType`/`Projection`'s JSON Schema across every
bounded context whenever registration changes, and rebuilds the entire
schema from scratch behind the already-decided `ArcSwap<Schema>`. JSON
Schema → GraphQL type mapping is direct: scalar → scalar, optional →
nullable, a list of scalars → a GraphQL list, the one level of nested
object structure (`specs/skilj.allium`'s nested-payload resolution) → a
nested GraphQL object type. Field and argument names convert
snake_case → camelCase on the way out, matching what `async-graphql`'s
own derive macros already do by default — no new convention invented
there.

**Built for real, §8 item 6.5's own pass**: `ProjectionQuery` is the
first surface that actually needed this mapping (`skilj-graphql/src/
projection_types.rs`), and confirmed the mapping above against a real
`schemars::schema_for!` output (0.8): a nested struct field is
`{"$ref": "#/definitions/Name"}`, resolved against the schema's own
top-level `definitions` map (never a second, nested one — one level
only, matching the payload schema shape cap `specs/skilj.allium` states
above `entity CommandType`); an optional scalar's `type` may additionally
appear as `["T","null"]`, but `required` array absence is the actual
nullability signal used, not that array. A field shape outside this
contract (which the spec itself says is stated but not enforced upstream
— "undefined rather than defined-and-forbidden") renders as an opaque
JSON-encoded `String` rather than panicking schema generation for the
whole server over one misbehaving projection elsewhere. `SchemaRegistry`
still isn't real, though — see the correction to §5.2 below.

### 5.2 Namespacing: nested per bounded context — corrected

The original design here (kept below for the historical reasoning) was
never actually built. Every dynamic per-bounded-context surface shipped
(`EventQuery`/`CommandQuery`/`CommandSubmission`, Phase 3;
`ProjectionQuery`/`EventSubscription`, later) is a **flat top-level field
taking `boundedContext: String!` as a plain argument** — `queryEvents(boundedContext:
..., ...)`, `projection(boundedContext: ..., name: ..., ...)`,
`allEvents(boundedContext: ..., ...)` — not nested under a per-context
`BankingQueries`/`BankingSubscriptions` type, and `SchemaRegistry`
(the `ArcSwap` rebuild-on-registration-change mechanism this section
originally assumed) is still an unused stub: the schema is built once,
synchronously, at `Skilj::graphql_router()` time. This is the same kind
of "reality diverged from the original prose" correction §5.4 already
needed once, not a new decision reopened lightly — confirmed with the
user specifically for `ProjectionQuery`, which is the first surface
where the *type itself* (not just the field) needed to vary per
registration: `ProjectionQuery` exposes one field,
`projection(boundedContext, name, waitForSequence): ProjectionResult!`,
where `ProjectionResult` is a GraphQL **union** over every registered
projection's own generated type (selected via an inline fragment,
`... on AccountBalance { total }`) — reusing `TokenRevocation`'s already-
shipped `FieldValue::owned_any(v).with_type(name)` pattern exactly,
rather than inventing per-projection-named fields, which would have
reopened the nested-namespacing question this correction just closed.
Each generated type is named `{boundedContext}_{projectionName}` to stay
globally unique without nesting (two different bounded contexts can
register a projection with the same name) — the flat-prefix scheme the
original design below explicitly rejected for *fields*, reused here
because a GraphQL *type* name has no scoping mechanism nesting would
have given it anyway.

The original nested-namespacing design, never implemented:

```graphql
type Query {
  banking: BankingQueries
  inventory: InventoryQueries
  boundedContexts: [BoundedContext!]!   # BoundedContextDirectory, superadmin-only
}

type BankingQueries {
  accountBalance(id: ID!): AccountBalance          # ProjectionQuery
  events(eventTypes: [String!], tags: [TagInput!], after: String): EventConnection  # EventQuery
  commands(commandTypes: [String!], after: String, before: String): CommandConnection  # CommandQuery
}

type Mutation {
  banking: BankingMutations
  inventory: InventoryMutations
  createSuperadmin(bootstrap: ID!, bootstrapSecret: String!, name: String!, externalSubject: String!): Role
  # AccessManagement, BoundedContextCreation etc. are cross-context (Superadmin-facing),
  # so they stay at the root rather than nested under any one bounded context
}
```

It would have been chosen over flat prefixed names
(`Banking_AccountBalance`, `bankingAccountBalance(id)`) because
collisions between bounded contexts become structurally impossible
rather than avoided by convention, and it matches how large multi-domain
GraphQL APIs (Shopify, GitHub) namespace unrelated resource groups —
still true in principle, just not what got built. Superadmin-facing,
cross-context surfaces (`AccessManagement`, `BoundedContextCreation`,
`BoundedContextDirectory`, `SuperadminBootstrap`) do stay at the schema
root as planned — they aren't "for" any one bounded context, the same
reasoning the spec itself already gives for gating them by `Superadmin`
rather than a per-context `RoleAccessMapping`.

### 5.3 Pagination: Relay-style cursor connections

`edges { node, cursor }` / `pageInfo { hasNextPage, endCursor }` for
every list-returning query (`EventQuery`, `CommandQuery`,
`BoundedContextDirectory`). This was close to a foregone conclusion once
stated: the spec's own rule signatures are already cursor-shaped
(`FetchEvents`/`EventQuery`'s `after_sequence`, `FetchCommands`' `after`/
`before`), so the cursor value for event- and command-ordered
connections is the entity's own `sequence` (events) or `id` (commands),
not an invented offset. Offset/limit pagination was ruled out as the
GraphQL-ecosystem anti-pattern it generally is on top of that — unstable
under concurrent writes, since a new event pushes every later offset by
one.

### 5.4 Error shape — and a correction: business rejections aren't errors

Builds on §4's `code()`/`message()` trait, but only for one of the two
tiers. Revisiting this after designing the REST side surfaced a real gap:
`code()`/`message()` fit library-level errors cleanly (`extensions.code`
+ the error's own top-level `message`, standard `async-graphql`
error-extension usage), but a `CommandRejected` isn't a failure of the
*request* — `decide()` ran successfully and produced a legitimate
business answer of "no." Putting that through GraphQL's `errors` array
is a well-known anti-pattern; this also required a small correction to
`specs/skilj.allium` itself, since `value CommandDecision`'s own comment
had prematurely committed to exactly that shape ("surfaced verbatim as
the GraphQL error message... an error extension/code being the obvious
carrier") — now fixed to defer the wire shape like everything else in
that file, with this document as where it actually gets decided.

**Business rejections surface as ordinary typed data**, the same pattern
Shopify's `userErrors` and similar "typed payload" conventions use:

```graphql
type SubmitCommandPayload {
  accepted: Boolean!
  triggeredEventSequences: [Int!]   # present when accepted
  rejectionReason: String           # present when not accepted
  rejectionKind: String             # present when not accepted
}
```

Library-level errors (revoked grant, wrong bounded context, malformed
input) still go through GraphQL's real `errors` array via `code()`/
`message()`, unchanged. The REST design in §7 mirrors this exact split —
that's what keeps the two wire contracts consistent with each other
rather than accidentally answering the same question two different ways.

---

## 6. IdP trust configuration

```rust
let skilj = Skilj::builder()
    .identity_provider(IdpConfig {
        jwks_endpoint: "https://idp.example.com/.well-known/jwks.json".parse()?,
        issuer: "https://idp.example.com/".into(),
        signing_algorithm: SigningAlgorithm::Rs256,
        subject_claim: "sub".into(),   // the default; overridable per the
                                        // spec's identity-resolution note
    })
    // ...
```

Fills in the config pattern already decided in §2.4 — a plain struct of
concrete values, sourced however the consuming application likes. Most
of this is mechanical given that; the one real decision is **how JWKS
key rotation is handled**, since it has actual correctness stakes:
cache-forever means auth silently breaks the moment an IdP rotates its
signing key, until something restarts the process.

**Reactive refresh, not a background timer.** The fetched JWKS is
cached; when a JWT's `kid` (key ID) isn't found in the cached key set,
skilj refetches once before rejecting the token — no background refresh
task to manage the lifecycle of. Paired with a minimum time between
refetches (on the order of a few seconds) so a caller can't cheaply force
repeated JWKS fetches by presenting JWTs with garbage `kid` values.

**Crate: `jsonwebtoken`**, not a full OIDC-discovery crate like
`openidconnect`. Skilj only needs signature verification against a known
JWKS (`DecodingKey::from_jwk`) — it doesn't need discovery, token
exchange, or anything else OIDC bundles in.

---

## 7. The REST wire contract

### 7.1 Purpose: narrowly-scoped agents and automated callers, not general access

Worth stating plainly since it's the frame every choice below follows
from: REST exists for callers holding a specific, admin-issued
`AccessToken` scoped to exactly one registered type — AI agents, remote
workflows, adapters — never for general application access, which is
what the GraphQL/Role track is for. Every design choice below (flat
capability routes, no browsing, no cross-type reads) follows from that:
REST is deliberately narrow, not a second general-purpose API.

### 7.2 Routing: capability-based, not type-or-context-in-path

Since every `AccessToken` variant is already scoped to exactly one
registered type (`ExternalEventToken.event_type`, etc.), the URL doesn't
need to repeat that — the presented token alone determines what's being
read or written:

```
POST /v1/events/external        -- ExternalEventIngestion (ExternalEventToken)
POST /v1/events/direct          -- DirectEventCreation (DirectCreationToken)
GET  /v1/events                 -- EventFetch's FetchEvents (EventReadToken, client-tracked)
GET  /v1/events/consume         -- EventFetch's ConsumeEvents (EventReadToken, server-tracked)
POST /v1/events/consume/ack     -- EventFetch's AcknowledgeEvents (EventReadToken, manual_ack only)
POST /v1/commands/trigger       -- CommandTrigger (CommandToken)
```

Presenting the wrong token variant at a route is a 403, not a 404 — the
route exists, the credential just doesn't authorize that action.

### 7.3 Request/response bodies

```
POST /v1/events/external
  { "payload": {...}, "sourceContent": "...", "sourceContext": "..." }  # sourceContext optional
  -> 201 { "sequence": 42 }

POST /v1/events/direct
  { "payload": {...} }
  -> 201 { "sequence": 43 }

GET /v1/events?filter=field:op:value&filter=field2:op2:value2&after=41
  -> 200 { "events": [...], "nextCursor": "44" }   # cursor = sequence, same convention as §5.3

GET /v1/events/consume?mode=auto|manual&filter=field:op:value
  -> 200 { "events": [...] }
  # mode required on a token's first call, optional (and validated to match) after that -
  # see entity ReadCursor in the spec. No "after"/cursor param at all: the position lives
  # server-side, keyed by the token alone. filter= is the identical repeatable param
  # GET /v1/events uses - ConsumeEvents' own rule signature takes filters too.

POST /v1/events/consume/ack
  { "sequence": 44 }
  -> 200 {}
  # only valid when the token's cursor is in manual_ack mode; 409 otherwise

POST /v1/commands/trigger
  { "payload": {...} }
  -> 200 { "accepted": true, "triggeredEventSequences": [44, 45] }
  -> 200 { "accepted": false, "rejectionReason": "...", "rejectionKind": "insufficient_funds" }
```

`filter=field:op:value`, repeatable, was picked over a single
JSON-encoded query param — plain and readable in a URL, and
`FilterOperator`/field/value is a small enough shape that
colon-separation doesn't get ambiguous. `CommandTrigger`'s 200-with-
`accepted:false` mirrors §5.4's GraphQL fix exactly: a rejection is a
legitimate outcome of a successful request, not an HTTP-level error.

### 7.4 Three ways to read events, on purpose

`GET /v1/events`, `GET /v1/events/consume`, and `POST /v1/events/consume/ack`
exist because different callers have genuinely different needs, not
because one design subsumes the others:

| | Who tracks position | Delivery | When to use |
|---|---|---|---|
| `GET /v1/events` (client-tracked) | The caller, in its own storage | Whatever the caller implements | The caller already has somewhere durable to keep a cursor (a database row, a checkpoint file) and wants full control |
| `GET /v1/events/consume?mode=auto` (server-tracked, auto-advance) | skilj, per token | At-most-once — an event served is never served again, even if the caller crashes before processing it | A stateless worker, a shell script, a quick integration — simplest to use, occasional missed events on crash is acceptable |
| `GET /v1/events/consume?mode=manual` + `POST .../ack` (server-tracked, manual-ack) | skilj, per token, advanced only on explicit ack | At-least-once — nothing is lost, but a crash between fetch and ack means the same events are redelivered next time | Processing must not silently drop events, and the caller can handle duplicate delivery safely (idempotent processing) |

Two independent read positions under this model means two separate
`EventReadToken`s (already a lightweight, existing mechanism — an admin
mints tokens per reader), not a shared token with a caller-supplied
consumer name — a `ReadCursor` is 1:1 with a token in the spec, on
purpose. Mixing the client-tracked and server-tracked reads against the
*same* token is allowed but the two positions know nothing of each
other: `FetchEvents` never reads or moves the server-side cursor.

This same table (in plainer terms, aimed at people integrating the
library rather than building it) belongs in `README.md` — see the update
made alongside this document.

### 7.5 Error mapping

Library-level errors (the `code()`/`message()` tier from §4) map to
standard HTTP semantics: 401 (missing/malformed bearer credential), 403
(valid credential, wrong permission — revoked token, wrong token
variant for the route, an opt-in flag like `external_creation_allowed`
off), 400 (malformed body, invalid filter), 409 (a state conflict — a
bounded context archived, or an acknowledgement against an `auto_advance`
cursor). Body is `{ "code": ..., "message": ... }` from the same trait
GraphQL renders through, so a client library sees the identical shape
regardless of which track it's talking to. `CommandRejected` is 200, not
an error status, per §7.3/§5.4.

---

## 8. Open for a future pass

Every *design* question raised so far has a decision recorded above
(§1.6/§1.7 closed the last two: the `BoundedContextEvent` conversion
trait, and the builder's registry/decider shape). What's left is
implementation, in dependency order:

1. ~~**`Role`/`RoleAccessMapping` persistence**~~ — **done.** `roles`/
   `role_access_mappings` tables exist (`migrations/0001_init.sql`,
   `bounded_contexts.created_by_role_id` now a real FK into `roles`
   rather than the denormalised columns an earlier pass used before this
   table existed), with full CRUD in `skilj-core/src/db` and round-trip
   coverage in `skilj-core/tests/persistence.rs` (28 tests, passing
   against a real Postgres). Still blocks reconciliation (§1.5),
   `AccessManagement`'s actual rules, and JWT→`Role` identity resolution
   — those callers just don't exist yet (items 2/4/5 below).
2. ~~**The builder's registry, for real**~~ — **done.** `skilj`'s
   `SkiljBuilder::event_type::<T>()`/`command_type::<T>()`/`projection::<T>()`
   build and store the §1.7 records for real (`schemars`-derived schemas,
   the boxed decider closure for command types), and `.build()` runs
   reconciliation against a named `.reconciliation_role(...)` end to end:
   resolves the Role, checks admin access per bounded context (skipping,
   not erroring, when it's missing - §1.5), calls
   `register_event_type`/`register_command_type`/`register_projection`,
   and persists whatever each returns. `skilj-core::plugin::BoundedContextEvent`
   (§1.6) is implemented too, as `CommandType::Event`/`Projection::Event`'s
   new bound. Verified end-to-end against a real Postgres
   (`skilj/tests/reconciliation.rs`: registers a full event
   type/command type/projection trio, the admin-access skip path, the
   reconciliation-omitted no-op path, and idempotent re-registration).
   New along the way: `event_store::Error::PayloadDecodeFailed` (the
   decoder's own error case, per §1.7) and `skilj_core::Error::Migration`
   (`.build()`'s own `db::migrate` call needed a variant to propagate
   into).
3. ~~**`db::list_events_for_bounded_context`**~~ — **done**, alongside (2)
   (`register_projection`'s own `bounded_context_events` needed it).
4. ~~**`POST /v1/commands/trigger`**~~ — **done.** `skilj-rest`'s sixth
   and final route (§7.2), completing REST end to end. Resolved the
   crate-boundary question (2) flagged: a new `CommandDispatcher` trait
   lives in `skilj-core::plugin` (`dispatch(bounded_context, command_type,
   payload, matching_events) -> Option<Result<CommandDecision>>`) -
   `skilj-rest::router()` now takes an `Arc<dyn CommandDispatcher>` as a
   second parameter rather than reaching into `skilj` directly, keeping
   `skilj-rest` independently usable per §3.1 (a `skilj-core` +
   `skilj-rest`-only consumer implements the trait by hand instead of
   using `skilj`'s builder). `skilj`'s own `Skilj` now holds the
   `command_types` registry (`Arc`-wrapped) and implements the trait via
   a small private `Dispatcher` wrapper, so `Skilj::rest_router()` can
   hand out a cheap `Arc<dyn CommandDispatcher>` without `Skilj` itself
   needing to live behind an `Arc`.
   Two more real gaps closed along the way, both flagged and confirmed
   with the user before building: `Command` had no table at all (added,
   with a synthetic `id` - the one table in this schema that needs one
   for a real reason, since `Event.origin`'s `CommandTriggered` variant
   embeds a whole `Command` by value and needs to reference it), and
   `events.origin_kind = 'command_triggered'` was previously unhandled
   (`insert_event`/row-loading both `panic!`ed on it) - now a real
   `origin_command_id` FK column plus full read/write support.
   `CommandToken` also gained the `insert`/`get` pair the other three
   `AccessToken` variants already had.
   Verified end-to-end against a real Postgres via `skilj/tests/command_trigger.rs`
   - a genuine HTTP request through `Skilj::rest_router()`, through the
   dispatcher, into a real `decide()`, back out through
   `process_command`'s persistence, including an explicit check that a
   `command_triggered` origin reads back correctly (the highest-risk new
   code path this item added) - plus the rejection-is-200 case and a
   malformed-credential 401.
5. **`skilj-graphql`** — **Phase 1 done** (see the plan at
   `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`): real JWT/JWKS
   verification (`access_control::{IdpConfig, SigningAlgorithm, JwksCache,
   verify_and_extract_subject}`, `reqwest`-based, reactive-refresh per §6)
   and the full superadmin admin console over a real `async_graphql::dynamic`
   schema — `createSuperadmin`, `createRole`/`revokeRole`/
   `grantRoleAccessMapping`/`revokeRoleAccessMapping`, `addBoundedContext`/
   `archiveBoundedContext`/`deleteBoundedContext`, `boundedContexts`.
   `Skilj::graphql_router()` mounts it at `POST /graphql`, mirroring
   `rest_router()`. Verified end-to-end (`skilj/tests/graphql_admin_console.rs`):
   a real HTTP GraphQL request, through a real local JWKS server and real
   signed JWTs, through every mutation/query above in sequence, back out
   through Postgres persistence — bootstrap → grant → create/archive/
   delete a bounded context → directory listing confirms it's gone.
   **Phase 2 done too**: four of its five remaining static-typed,
   `AdminAccess`-gated surfaces — `TypeRegistration` (`registerEventType`/
   `registerCommandType`/`registerProjection`/`rebuildProjection`/
   `discardProjectionRebuild`, plus a `projections(boundedContext:)` query
   satisfying the surface's own `exposes` list), `EventTypeAdminOperations`
   (`createExternalEventToken`/`createDirectCreationToken`/
   `createEventReadToken`), `CommandTypeAdminOperations`
   (`createCommandToken`), `TokenRevocation` (`revokeToken`, returning
   the new `AccessToken` GraphQL union — `ExternalEventToken` |
   `DirectCreationToken` | `EventReadToken` | `CommandToken`, tagged via
   `FieldValue::with_type`). **`SubjectErasure` (`ForgetSubject`) excluded
   from this phase** — `EncryptionKey` had no persistence at all yet
   (`protect_sensitive_fields` was still `todo!()` for its only non-empty
   case — the one thing that would ever create a row to forget), so
   there was nothing this resolver could be exercised against yet; same
   "don't build ahead of what's wired" discipline REST already followed
   for the identical reason. **Built for real in its own later pass —
   see the `SubjectErasure` writeup further below.** Two small persistence gaps
   surfaced and filled along the way: `db::list_projections_for_bounded_context`
   and `db::revoke_access_token` (resolves the owning schema via
   `access_token_index` the same way `fetch_access_token_row` already
   does, then a status-only `UPDATE`). Verified end-to-end
   (`skilj/tests/graphql_type_registration.rs`): register an event type,
   a command type and a projection; a schema change on the projection
   stages a rebuild instead of updating in place; `rebuildProjection`/
   `discardProjectionRebuild` both round-trip; every token-minting
   mutation returns a real secret; `revokeToken` resolves the union
   correctly by id alone.
   **Phase 3 done too, scoped to three of its five surfaces** (see the
   plan for the full reasoning): `EventQuery` (`queryEvents`/
   `countEvents`/`inspectEvent`), `CommandQuery` (`fetchCommands`),
   `CommandSubmission` (`submitCommand`) — every pure function they
   needed already existed and was tested, the same shape Phase 1/2 had.
   **`ProjectionQuery`/`EventSubscription` deliberately excluded from
   Phase 3**, confirmed with the user: neither was a GraphQL-plumbing gap
   at the time. `ProjectionQuery` needed `project()` (`todo!()` then — no
   projection state existed anywhere to query, regardless of wire shape) —
   **built in its own later pass, once `project()` existed both sync and
   async (§8 item 6); see the Phase 4 writeup below.** `EventSubscription`
   needed a real-time event-delivery mechanism (none existed — no
   broadcast channel, no Postgres `LISTEN`/`NOTIFY`) — **built in its own
   later pass too, once that mechanism existed; see the Phase 6 writeup
   further below.** `submitCommand` is the highest-value addition: it's what finally
   gives `CommandDispatcher::required_role` (§1.3.1, built two sessions
   earlier with no caller) a real caller, checked before `dispatch` so
   an unauthorised caller never reaches `decide()`. `queryEvents` gets a
   genuine cursor (`event_store::query_events`'s return type changed
   from `Vec<String>` to `Vec<(i64, String)>` — sequence paired with the
   rendered payload, since a paging client needs the sequence back to
   supply as the next call's `afterSequence`); `fetchCommands` stays a
   flat `[String!]!` with plain `after`/`before` timestamp arguments, no
   Relay envelope — `Command` carries no exposed id, so a literal
   `edges{node,cursor}`/`pageInfo` shape (§5.3's stated default) doesn't
   naturally fit it, confirmed with the user rather than forced. Two more
   small persistence gaps filled: `db::get_event_by_sequence`
   (`InspectEvent`'s own lookup key) and
   `db::list_commands_for_bounded_context` (`FetchCommands`' full-snapshot
   parameter — no "list every command in a context" function existed).
   Verified end-to-end (`skilj/tests/graphql_business_surfaces.rs`):
   `submitCommand` accepted/rejected paths, a `#[requires_role(...)]`-gated
   command type rejecting the wrong caller and accepting the right one,
   `queryEvents` paging past a known sequence, `countEvents`,
   `inspectEvent` (checking a `COMMAND_TRIGGERED` origin), `fetchCommands`.
   One new workspace-wide decision made getting to Phase 1: **`axum`
   bumped 0.7 → 0.8** (async-graphql-axum 7.2.1 requires 0.8) — no route
   in this codebase uses path parameters, so the `:id` → `{id}` breaking
   change never applied; the only real fix needed was dropping
   `#[axum::async_trait]` (native `async fn` in traits, no macro).

   **Phase 4 done: `ProjectionQuery`.** Needed §5.1's own remaining
   piece — a real JSON-Schema→GraphQL-type mapping, built as
   `skilj-graphql/src/projection_types.rs` (see §5.1's own updated
   writeup and §5.2's correction for the field-shape/schema-timing
   decisions, both confirmed with the user). New `resolvers::
   projection_query` field (`projection(boundedContext, name,
   waitForSequence): ProjectionResult!`) is `ReadAccess`-gated via a new
   `require_read_mapping` helper (`resolvers/mod.rs`) — any active level,
   not `AdminAccess` like every prior dynamic surface, since
   `query_projection` (`skilj-core::projections`) itself imposes no level
   check. `await_projection_caught_up` is a real polling loop now
   (`wait_until_caught_up`, 20ms ticks against `Projection.caught_up_to`)
   bounded by a new process-start knob, `SkiljBuilder::
   projection_query_wait_timeout` (default 5s) — the spec's own guidance
   above `rule QueryProjection` explicitly asks for a configuration knob,
   not a per-query argument. `read_projection`'s real scope this pass:
   returns stored state verbatim, no per-field sensitive-value decrypt
   logic — at the time this phase shipped, `protect_sensitive_fields` was
   still `todo!()` for its non-empty case (the `SubjectErasure` pass
   below is what made that real), so `SensitiveFieldsStayProtected` was
   vacuously satisfied, not broken; the real two-grant decrypt-on-read
   test is still deferred even now that encrypted content genuinely
   exists — see the `SubjectErasure` writeup's own note on this.
   `schema::build`/`skilj_graphql::router`/`Skilj::graphql_router()`
   all became `async`, returning `Result` — a real, contained breaking
   change (three existing test call sites needed a one-line `.await`
   update), since listing every registered projection to generate their
   types is a genuine I/O failure mode the old synchronous `.expect(...)`
   never had to cover.

   Verified: `skilj-graphql`'s own crate-level unit tests
   (`projection_types::tests`, no DB) exercise the mapping directly
   against a real captured `schemars::schema_for!` output via a real
   GraphQL execution, not by reaching into `Object`/`Field` internals —
   this caught a genuine bug before it ever reached a real end-to-end
   test: a nested-object field's resolver was calling `.with_type(...)`
   the same way the top-level union dispatch does, which is only correct
   for an actual polymorphic (union/interface) field — a concrete nested
   `Object` field doesn't need or want it, and doing so anyway broke
   every nested-field query with an internal downcast error.
   `skilj/tests/projection_query.rs` (new, real Postgres + real JWKS
   server, one end-to-end test): a real sync `Projection` with a
   one-level nested field, triggered via `submitCommand`, queried with no
   `waitForSequence` and with one naming the triggering event's own
   sequence (both succeed), a `waitForSequence` past what the short
   configured timeout will ever reach (the distinguishable
   `projection_caught_up_timed_out` rejection), a Read-level-only grant
   succeeding (proving `ReadAccess`, not `AdminAccess`), a caller with no
   grant at all on the bounded context rejected
   (`GrantScopedToBoundedContext`'s "no mapping = same as revoked"), and
   an unknown projection name. **All tests pass, stable across repeated
   runs.**

   **Phase 5 done: `SubjectErasure`.** `ForgetSubject` (the pure rule
   itself) predates this pass and was already fully tested
   (`skilj-core/tests/subject_erasure.rs`) — what this pass actually
   built is everything around it: real `EncryptionKey` persistence,
   `protect_sensitive_fields`'s non-empty branch (genuine encryption, not
   `todo!()`), and the GraphQL mutation. The spec explicitly leaves "the
   encryption scheme itself (algorithm, key derivation, physical key
   storage)" to Rust-level discretion — the same register `AccessToken.secret`
   generation and JWT verification are already in — so the actual scheme
   is a real architecture decision, confirmed with the user before
   building: envelope encryption via `ring` (already in the dependency
   tree transitively via `reqwest`'s rustls-tls, promoted to a direct
   `skilj-core` dependency — no new crate). Each `EncryptionKey` gets a
   random AES-256-GCM `DataKey`, wrapped under a single
   `SkiljBuilder::encryption_master_key([u8;32])` (optional — only needed
   the moment a bounded context actually references a declared
   `sensitive_fields` entry; a real, actionable
   `encryption_master_key_not_configured` error otherwise, never a silent
   bypass), stored alongside the row it protects (new `skilj_core::encryption`
   module). Destroying a key (`db::destroy_encryption_key`) nulls the
   wrapped bytes in the same statement that flips `status`/`destroyed_at`
   — real crypto-shredding, irreversible even against someone who still
   has the master key, not a status flag alone.

   A second, more consequential discovery than the design itself: making
   `protect_sensitive_fields` real meant `render_event`/`render_command`
   (`EventQuery`/`CommandQuery`/`InspectEvent`'s own read-side black box)
   were no longer safe as `todo!()` — their own doc comment had explicitly
   staked that gap's safety on "no `Event` this engine produces ever has a
   non-empty `sensitive_fields` type with anything encrypted to decrypt in
   the first place," a claim this very pass makes false. Left alone, the
   next `queryEvents`/`fetchCommands`/`inspectEvent` call against a real
   sensitive field would have panicked in production. Fixed by dropping
   the `todo!()` branch entirely — both functions now simply return the
   stored payload verbatim (ciphertext included) regardless of
   `sensitive_fields`, correct per the spec's own contract for an
   unauthorised reader, since no caller gets real decryption yet either
   way; the real two-grant decrypt-for-an-authorised-caller test stays
   its own deferred follow-up, now finally buildable-and-testable since
   this pass is what created the first real ciphertext to decrypt.

   **Threading key resolution through the pure core**, without breaking
   §1.1's "`decide()` and everything downstream stays I/O-free" rule:
   reused the exact pattern sequence allocation already established
   rather than inventing a new one — `process_command`'s own
   `resolve_event_type`/`next_sequence` are plain closures the *caller*
   pre-resolves before ever invoking the pure function; `resolve_key`
   (new, threaded through `protect_sensitive_fields`/`process_command`/
   `create_external_event`/`create_direct_event`) works identically. New
   pure helper `event_store::sensitive_field_subjects` tells the caller
   *which* subjects a payload needs keys for (no DB, no encryption — a
   payload walk only); the caller resolves each via new
   `db::get_or_create_encryption_key` (a bounded-context-scoped
   `encryption_keys` table, partial-unique-indexed on `(subject_key,
   subject_value) WHERE status = 'active'` — the identical pattern
   `roles_unique_active_external_subject` already uses, since a destroyed
   key must stay around permanently rather than being revived), building
   one shared map `process_command` accumulates into across the command's
   own payload *and* every accepted event spec's — a subject a command
   and its own event both name resolves to the identical `EncryptionKey`
   this way, exactly as the spec requires. `EncryptionKey` is a real,
   independently-lived entity (its own lifecycle), so `Event`/`Command`
   reference it via new `event_encryption_keys`/`command_encryption_keys`
   join tables, not JSONB-embedded like `tag_mappings` — replacing two
   placeholder JSONB columns that never held real data (safe to change,
   nothing deployed).

   New `resolvers::subject_erasure` (`forgetSubject(boundedContext,
   subjectKey, subjectValue): EncryptionKey!`), `AdminAccess`-gated
   (`require_admin_mapping`) matching `ForgetSubject`'s own
   `access_mapping.level = admin` requirement — the surface has no
   `exposes` list at all, so `EncryptionKey` is only ever reachable as
   this one mutation's own return value.

   Verified: `skilj-core/tests/encryption.rs` (new, 10 tests, no DB) —
   `encrypt_leaf`/`wrap`/`unwrap` round-trip, a wrong master key failing
   to unwrap, ciphertext never repeating for the same plaintext (fresh
   nonce every call), `protect_sensitive_fields` actually encrypting a
   declared leaf while leaving the rest of the payload (including the
   read-only `subject_field` itself) untouched, and `render_event`
   returning a non-empty-`sensitive_fields` event's payload verbatim
   without panicking — the exact condition that used to crash.
   `skilj-core/tests/persistence.rs` +3 (`encryption_keys` round-trip:
   provision-then-reuse, `None` for an unknown subject, destroy nulling
   the wrapped columns and a later provision for the same subject being a
   genuinely new row, not the destroyed one back). `skilj/tests/subject_erasure.rs`
   (new, real Postgres + real JWKS, one end-to-end test): a real
   `EventType` with a real sensitive field, an event created via
   `POST /v1/events/direct`, its *stored* payload confirmed genuinely
   encrypted via a direct DB read (not just "the API accepted it"),
   `forgetSubject` destroying the key over GraphQL, a second
   `forgetSubject` on the same now-destroyed subject rejected (nothing
   active left to find), and a caller with no grant on the bounded
   context rejected before ever reaching the lookup. **All tests pass,
   stable across repeated runs.**

   **Phase 6 done: `EventSubscription`.** The last surface out of §8/§9 —
   see the plan at `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`.
   A real discovery, not assumed going in: `Subscription`/
   `AllEventsSubscription`/`EventTypeSubscription`, `create_all_events_subscription`/
   `create_event_type_subscription`, and `deliver_to_subscriptions` (the
   pure "who matches and what do they get" computation, already calling
   `render_event`) all already existed in `skilj-core/src/event_store/mod.rs`
   from an earlier test-propagation pass — what this pass built was purely
   the live-delivery infrastructure around them.

   **The mechanism, confirmed against the spec's own Excludes list, not
   assumed**: "Multi-instance / distributed deployment" is explicitly out
   of scope for this whole library, so a single-process, in-memory
   `tokio::sync::broadcast` channel (new `event_store::EventBroadcaster`)
   is the architecturally correct delivery mechanism, not a shortcut —
   Postgres `LISTEN`/`NOTIFY` or a broker would be solving a problem this
   spec doesn't have. One publish point, not four:
   `db::insert_event_and_update_sync_projections` gained a `broadcaster:
   &EventBroadcaster` parameter and calls `.publish(event)` right after
   `tx.commit().await?` succeeds — the same choke-point every
   event-creation call site (2 REST routes, `CommandTrigger`,
   `submitCommand`) already funnels through, so every one of them gets
   real-time delivery at once, REST-originated events included.
   `SkiljBuilder::event_broadcast_capacity` (default 1024) is the same
   "sensible default, opt-in override" register `async_projection_poll_interval`
   already lives in; `Skilj` holds one shared `EventBroadcaster`, threaded
   into `rest_router()`/`graphql_router()` exactly like the two
   dispatchers already are.

   **Ending the stream, never silently continuing** — two guarantees the
   spec is unusually explicit about, both resolved the same way: yield
   one final, distinguishable `Err`, then stop. `async_graphql::dynamic`'s
   own subscription execution loop already stops polling a stream the
   instant a yielded item is an `Err` (confirmed against the installed
   crate source), so no manual stream-termination bookkeeping was needed
   beyond returning after the yield.
   - `DeliveryIsAtMostOnce` ("no duplicates, no gaps... a transport-level
     disconnect is... the only signal" one was missed): a
     `tokio::sync::broadcast::Receiver` that falls behind returns
     `RecvError::Lagged` on its next `recv()` — treated as
     connection-equivalent, not silently skipped past.
   - `RevocationClosesTheConnection`: `access_mapping` is re-checked
     *live*, per delivered event, via a fresh `db::get_active_role_access_mapping`
     call — never the snapshot captured at subscribe time — so a
     mid-stream revocation stops delivery at the next matching event, not
     at the next reconnect.

   New `resolvers::event_subscription` — `allEvents(boundedContext,
   eventTypes, fromSequence)`/`eventsByType(boundedContext, eventType,
   filters, fromSequence)`, one field per the surface's own two
   `provides`, `ReadAccess`-gated (`require_read_mapping`, reused from
   `ProjectionQuery`). Built on `asynk-strim` (already transitively
   present via `async-graphql`, promoted to a direct `skilj-graphql`
   dependency) — the exact crate `async_graphql::dynamic`'s own
   subscription examples use for a `yielder.yield_ok(...)`/
   `yielder.yield_error(...)`-driven `Stream`. `filters` is real wire
   shape (`gql_types::filter_input()`/`filter_operator_enum()`, a new
   `parse_filters` helper), matching `CreateEventTypeSubscription`'s own
   signature faithfully rather than being silently dropped — but a
   non-empty list is rejected eagerly, the identical precedent REST's own
   `GET /v1/events` route already set for the same underlying reason:
   `matches_filters`/`valid_filters` are still `todo!()` for their
   non-empty case, an existing, separately-tracked gap this pass didn't
   newly touch.

   **A genuine mid-implementation API discovery**: the plan assumed
   `async_graphql_axum::GraphQLSubscription::new(schema).on_connection_init(...)`
   would work for injecting the caller's identity into a subscription.
   Reading the installed crate source showed `GraphQLSubscription` (the
   simple `tower::Service` wrapper) builds its own `GraphQLWebSocket`
   internally with no way to reach `on_connection_init` — only
   `GraphQLWebSocket` itself exposes that builder method. Fixed by
   writing `skilj_graphql::graphql_ws_handler` directly against
   `GraphQLProtocol`/`WebSocketUpgrade` extractors, then
   `GraphQLWebSocket::new(socket, schema, protocol).on_connection_init(...).serve()`
   — mounted on the same `/graphql` path as the existing `POST` handler
   via axum 0.8's own `post(handler).get(handler)` method-chaining idiom,
   so a GraphQL client's own protocol negotiation picks the right one
   with no new route needed. New `auth::resolve_role_from_connection_init`
   mirrors `auth::resolve_role` exactly, reading the bearer credential
   from the `connection_init` message's own JSON payload (the
   graphql-ws protocol's own place for it) instead of an HTTP header,
   sharing a factored-out `verify_jwt_to_role` core with the header case.

   Verified: `skilj-core/tests/event_broadcast.rs` (new, no DB, 4 tests)
   — `publish` reaching a `subscribe()`d receiver; zero receivers not
   erroring; two independent subscribers both receiving the same event
   (real fan-out); a lagging subscriber getting `RecvError::Lagged`.
   `skilj/tests/event_subscription.rs` (new, real Postgres + real JWKS +
   a real websocket client, since `tower::ServiceExt::oneshot` can't
   drive a long-lived streaming connection the way every other
   `skilj/tests/graphql_*.rs` test does — this one drives
   `tokio-tungstenite` against `Skilj::graphql_router()` mounted on a
   real `axum::serve` listener, alongside a cloned `axum::Router` for the
   ordinary `submitCommand` calls that trigger real events): connects,
   `connection_init`s with a real signed JWT, subscribes to `allEvents`,
   triggers a real event in a *different* bounded context via
   `submitCommand` and confirms nothing arrives, triggers a real matching
   event and confirms it arrives with the correct sequence/payload, then
   revokes the subscriber's own `RoleAccessMapping` mid-stream (a direct
   DB update) and confirms the next matching event closes the
   subscription with a `grant_not_active`-coded error followed by a
   `complete` message, not silently. **All tests pass, stable across two
   full workspace runs.** This closes out §8/§9 entirely — every item
   from the original backlog is done.
6. **`project()`'s own dispatch closure** — **done, both cases** (see the
   plan at `/home/gklijs/.claude/plans/serene-puzzling-pinwheel.md`):
   `project()` runs in one of two genuinely different ways (the note
   above the rules) — the inline, same-transaction `sync: true` case
   landed first; this pass added the async (`sync: false`, the default)
   case, plus `ProjectionRebuild` replay-to-completion and automatic
   promotion, confirmed with the user as both in scope for the same pass
   (leaving `RebuildProjection` unbuilt-in-practice — it flips a row to
   `building` and nothing had ever advanced one — would have been worse
   than not building it at all).

   **The design**: one shared background task, not one per bounded
   context — `SkiljBuilder::build()` spawns it via `tokio::spawn`, on a
   configurable timer (`SkiljBuilder::async_projection_poll_interval`,
   default 500ms; runs an immediate catch-up before its first sleep, so
   an event created right after `.build()` returns doesn't also pay for
   a full idle interval). One shared task rather than per-context because
   a bounded context's first async `Projection`/`ProjectionRebuild` can
   appear at any point after `.build()` returns (`registerProjection` is
   a live GraphQL mutation) — a per-context task model would need its own
   dynamic spawn-on-demand lifecycle to notice that; re-listing every
   bounded context each tick (`db::list_bounded_contexts`) sidesteps it
   entirely. Poll-only, confirmed with the user: no `LISTEN`/`NOTIFY`, no
   broadcast channel, no new event-delivery trait threaded through the
   four event-creation call sites — `ProjectionQuery`'s own
   `wait_for_sequence` exists specifically because async projections are
   *expected* to lag, not to be eliminated. No shutdown API — the spawned
   task is detached and runs for the process's lifetime, the same "don't
   build ahead of what's wired" call this crate makes repeatedly (nothing
   today needs to gracefully stop a running `Skilj`). A single bounded
   context's own failure during a tick is logged to stderr and skipped,
   not allowed to stop the task or block the rest of that tick.

   The real work is **`db::catch_up_bounded_context`** (called once per
   bounded context, per tick) — loads every `sync = false` `Projection`
   and every `building` `ProjectionRebuild` in the context, finds the
   lowest `caught_up_to` among them, loads events past that point via
   the new `db::list_events_for_bounded_context_from`, then folds each
   one into everything still behind it — the identical `SELECT ... FOR
   UPDATE` / fold-via-dispatcher / `UPDATE` transactional shape
   `insert_event_and_update_sync_projections` already established, one
   transaction per event, `caught_up_to` always advancing "either way"
   (the same rule the sync case follows). A `ProjectionRebuild` always
   replays asynchronously regardless of its own *target* `sync` value —
   the replay itself is background work either way; `sync: true` only
   takes effect after promotion. `db::promote_projection_rebuild` runs
   once a building rebuild's `caught_up_to` reaches the bounded context's
   latest committed sequence (`db::latest_sequence`, new): one
   transaction copies the rebuild's `schema`/`schema_version`/`sync`/
   `caught_up_to` onto the live `Projection` row, replaces
   `projection_consumed_event_types` wholesale, copies the rebuild's own
   `projection_rebuild_state` into `projection_state` (the live
   projection's old state is discarded, per the note above
   `RegisterProjection`), then deletes every rebuild-side row.

   **New persistence**: `projection_rebuild_state` (per bounded-context
   schema, mirroring `projection_state`) — a rebuild's own private fold,
   never sharing a row with the live projection's, since both can exist
   at once during a replay window. Unlike `projection_state`, it isn't
   seeded at registration time: a rebuild's `caught_up_to` resets to
   `None` on every `db::upsert_projection_rebuild` call (including a
   restage of an already-`building` row — `register_projection`'s own
   struct-update construction does this unconditionally), which
   invalidates whatever a previous attempt had accumulated; only the
   *current* dispatcher, not whichever caller happened to trigger the
   reset, knows the right starting value. So `ProjectionDispatcher`
   gained a second method, `default_state(bounded_context,
   projection_name) -> Option<String>` (`RegisteredProjection`'s own
   `default_state_json`, exposed per-lookup instead of only at
   registration) — `catch_up_bounded_context` deletes a `None`-`caught_up_to`
   rebuild's stale state row up front, then reseeds it from
   `default_state()` the moment it actually starts folding. A rebuild
   this process's dispatcher can't resolve at all (no compiled type
   registered here for that name — a purely GraphQL-staged rebuild, say)
   still advances: its state freezes at `"{}"`, a placeholder never
   actually deserialised by anything, since `dispatcher.project()`
   returns `None` for the same lookup key `default_state()` did — the
   same "position always advances, state only changes when the
   dispatcher can" treatment the sync path already gives an unconsumed
   event.

   Mirrors `CommandDispatcher`/`DeciderFn` (§1.7) exactly: a new
   `ProjectionDispatcher` trait (`skilj-core::plugin`) - `fn project(&self,
   bounded_context, projection_name, state_json, event) ->
   Option<Result<String>>` - implemented by a new `RegisteredProjection`
   closure built in `registered_projection::<T>()` (deserialise
   `state_json` into `T::State`, defaulting via `T::State::default()`
   at registration time rather than at fold time; convert `event` via
   `BoundedContextEvent::try_from_event`; call `T::project()` **only**
   when `event.event_type.name` is in `T::consumed_event_types()` -
   `try_from_event` succeeding isn't by itself permission, since a
   projection's generated `Event` enum can have variants beyond what it
   declared consuming). `Skilj::projection_dispatcher()` mirrors
   `command_dispatcher()`; both `skilj-rest`'s `AppState` and
   `skilj-graphql`'s `GraphqlState` carry the resulting `Arc<dyn
   ProjectionDispatcher>` alongside the existing command one.

   New persistence: a `projection_state` table per bounded-context schema
   (`projection_name`, `state`, `updated_at`), seeded at registration time
   so a fold's own row lock always finds something to lock rather than
   needing an insert-or-update branch; `db::get_projection_state`
   (for whoever eventually builds `ProjectionQuery`'s `read_projection`);
   and the real transactional entry point, **`db::
   insert_event_and_update_sync_projections`** — one `pool.begin()`:
   insert the event, then for every `sync = true` projection in the
   context, `SELECT ... FOR UPDATE` its `projection_state` row (the same
   row-lock-based serialisation `next_sequence` already relies on),
   fold via the dispatcher, `UPDATE` state, and `UPDATE caught_up_to`
   **unconditionally** (the note above the rules: "advances... either
   way", even for an event whose type this projection doesn't consume) —
   then commit. `insert_event` itself was refactored to take `impl
   sqlx::PgExecutor<'_>` instead of a bare `&Pool` (no call-site changes
   needed elsewhere), so the new function reuses it directly for the
   insert step rather than duplicating that SQL. All four event-creation
   call sites (`skilj-rest`'s two REST event routes, its `CommandTrigger`
   handler, `skilj-graphql`'s `submitCommand` resolver) switched to the
   new function - a no-op behaviour change for every bounded context with
   no `sync` projections (every existing test fixture).

   `ProjectionQuery` (the GraphQL surface) is **still not built**, even
   after this - `read_projection`'s wire-facing half still needs the
   JSON-Schema→GraphQL-type mechanism (§5.1, unbuilt); this item resolved
   its *other* prerequisite ("no projection state exists to query"), for
   both the sync and async cases now.

   Verified end-to-end, real Postgres: `skilj-core/tests/sync_projections.rs`
   exercises the transactional persistence function directly against a
   hand-rolled `ProjectionDispatcher` test double (a consumed event
   updates state and `caught_up_to`; an unconsumed one advances
   `caught_up_to` only; two sync projections both update off one write);
   `skilj/tests/sync_projections.rs` proves the real path end-to-end - a
   real `Projection` registered through the builder, triggered twice
   over a real `POST /v1/commands/trigger` request, the running total
   correctly accumulating across both. `skilj-core/tests/async_projections.rs`
   does the same for the async half — a cold-start catch-up folding
   existing history, a no-op second tick, and a `ProjectionRebuild`
   replaying from scratch (not resuming the live projection's own prior
   progress) and promoting automatically once caught up, with the live
   row's `schema`/`schema_version`/state all visibly replaced and the
   rebuild row gone; `skilj/tests/async_projections.rs` proves the
   spawned task itself is real — a command triggered over REST, then a
   short poll loop waiting on `db::get_projection_state` to reflect it,
   with no request-path code calling `project()` directly.

**Real decrypt-on-read: `render_event`/`render_command`, done - not a §8
item itself, but the natural follow-up once `SubjectErasure` made
`protect_sensitive_fields` real.** The spec is unusually explicit and
consistent about what "real" means here - identical text above
`DeliverToSubscriptions`/`QueryEvents`/`FetchCommands`/`QueryProjection`,
restated in four `@guarantee SensitiveFieldsStayProtected` blocks: a
sensitive field decrypts under either of two independent grants, checked
per field against that field's own `EncryptionKey.subject_value` -
`access_mapping.can_read_sensitive`, or `access_mapping.role.external_subject`
matching it (a caller reading their own data needs no separate grant) -
decided *inside* the black box, never as a post-hoc redaction. Confirmed
via `AskUserQuestion` (both recommended, before building): scoped to
`render_event`/`render_command` only this pass, `read_projection` staying
a separate future item (see the §9 bullet above for why); a missing
`encryption_master_key` for a caller who *is* otherwise granted is a hard,
actionable error, mirroring `resolve_encryption_keys`'s own write-side
precedent - never triggers for an unauthorised caller, who needs no key
at all in this design.

**The key insight that kept this small**: `protect_sensitive_fields`
never touches `subject_field` itself - it stays plaintext in the stored
payload forever. So `EncryptionKey.subject_value` for any sensitive field
is always recoverable directly from the payload's own `subject_field`
value, with no need to load `Event.encryption_keys`/`Command.encryption_keys`
(confirmed, separately, to still never be populated from the DB on any
load path - a real, pre-existing, currently-inert gap, left out of scope:
nothing reads it, and this approach doesn't need it either). New
`event_store::sensitive_field_is_granted` factors out the two-grant
boolean itself, shared by both the pre-resolution step (which subjects
are even worth fetching a key for) and `render_event`/`render_command`
(whether to actually substitute a decrypted value) - never two
independently-drifting copies of the same check. `render_event`/
`render_command`/`query_events`/`inspect_event`/`fetch_commands`/
`deliver_to_subscriptions` all gained a `resolve_data_key` closure
parameter, the identical "impure resolution, pure decision" split
`resolve_key` already has on the write side. A field granted but with no
active key (destroyed by `ForgetSubject`, or never provisioned) is left
untouched, no special-casing; a field granted but not actually valid
ciphertext (the spec's own acknowledged historical-row case) is left
untouched too, via the same `decrypt_leaf`-failed fallback - one
mechanism covers both. The decrypted leaf always renders as a JSON
string regardless of the field's original scalar type - an
already-shipped consequence of `protect_sensitive_fields` itself
discarding the original type at encrypt time, not a new limitation this
pass introduces.

New `db::get_active_data_key` (composes the already-private
`get_active_encryption_key_row` with the already-existing `unwrap_row` -
no new SQL) and `db::resolve_data_keys_for_reading` (the read-side twin of
`resolve_encryption_keys`, filtered through `sensitive_field_is_granted`
first so an unauthorised caller's query never needs a master key at all).
New `resolvers::resolve_read_data_keys` in `skilj-graphql` wraps it for
GraphQL's error shape; `event_query.rs`/`command_query.rs`/
`event_subscription.rs` each call it once per event/command being
rendered (scoped to the caller's own `eventTypes`/`commandTypes` argument
where a batch is involved, not the full unfiltered snapshot - may resolve
a few more keys than strictly needed when `tags`/`afterSequence` narrow
further, never fewer, no correctness impact).

Verified: `skilj-core/tests/encryption.rs` extended (11 new tests) -
`render_event`/`render_command` actually decrypting under each grant
independently, a destroyed/missing key still yielding ciphertext, a
historical-plaintext leaf left untouched with no panic, and
`sensitive_field_is_granted`'s own two branches directly.
`skilj-core/tests/persistence.rs` +3 real-Postgres tests for
`get_active_data_key`. New `skilj/tests/decrypt_on_read.rs` (real
Postgres + real JWKS, 2 end-to-end tests): the full two-grant flow over
`queryEvents` - neither grant sees ciphertext, `can_read_sensitive` sees
plaintext, the matching `external_subject` sees plaintext with no
`can_read_sensitive` at all, and - the crypto-shredding guarantee finally
verified through the real read path, not just a raw DB check -
`forgetSubject` destroying the key makes even the previously-granted
caller see ciphertext again; a second test confirms a whole `Skilj` built
with no `encryption_master_key` at all still answers queries fine when
nothing returned needs decrypting. **All tests pass on the first
real-Postgres run, stable across two full workspace runs.**

**Keyed / multi-row Projections - a real, user-driven correction to §8
item 6's own original design, not a bug fix.** Scoping `read_projection`'s
decrypt-on-read (above) surfaced research suggesting `projection_state`
was structurally one shared row per `(bounded_context, projection_name)`
everywhere - schema, `Projection` trait, and `specs/skilj.allium` itself.
The user corrected this directly, not via `AskUserQuestion` (two rounds
of questions were declined in favour of free-text clarification,
recorded here instead of in the usual "confirmed via AskUserQuestion"
form): projections are meant to support many independently-addressed
rows - one per customer for a purchase history, one per course for its
own participants - and this was simply never built, in the spec or the
implementation. Three concrete design points came directly from the
user, not inferred: **not** the existing tag mechanism (a dedicated,
Projection-specific key concept instead - "projections typically use one
of the fields as key," an annotation to cut boilerplate for the common
case); **one event can touch multiple rows** (the worked example: a
transfer event updating both the giver's and the receiver's own row from
a single fold); and storage stays a flexible JSON blob with metadata,
not a rigid per-key relational schema.

**The spec change**, delegated to `allium:tend` (one session-limit
retry mid-task - confirmed via `git diff specs/` that nothing partial
had landed before relaunching with the identical brief, the same
recovery pattern used once before this session): deliberately small and
mechanism-agnostic, matching `project()`'s own existing "black box, this
library owns *when*/*what a caller may see*, not *how it's computed*"
register. `rule QueryProjection`'s `when`/`ensures` and `surface
ProjectionQuery`'s `provides` all gained an optional `key` (defaulting to
`""` via `let instance_key = key ?? ""`, the identical null-coalescing
convention `from_sequence`/`wait_for_sequence` already use) - `key`
could **not** be threaded through the surface's own `context` binding
(`allium:tend`'s own pushback, correct): `context` binds an *entity
instance*, and `key` is a caller-supplied query-time argument matching
no entity, stored nowhere. `entity Projection` gained a short note that
its own instance data may be one value or many, each addressed by a
caller-supplied key, decided per event by the fold itself - no new
stored *field*. `RegisterProjection`/`entity ProjectionRebuild` needed
**no change at all**, confirmed explicitly rather than assumed: the
multiplicity lives in the instance data a projection produces, not in
its definition. `allium check`/`analyse`/`plan`'s obligation count is
byte-identical before and after (341) - expected, not a red flag:
`plan` derives obligations from `requires` clauses, rules, surfaces and
entity fields, none of which changed shape; only an existing rule/surface
gained an already-optional trigger parameter.

**The Rust API**: `Projection` gains `fn keys(event: &Self::Event) ->
Vec<String>` (default: one constant sentinel key, `""` - every existing
Projection keeps working completely unchanged, zero-boilerplate);
`project()` gains a `key: &str` parameter (lets one event fold
differently per row - credit vs. debit - by comparing `key` against the
event's own fields). `ProjectionDispatcher` mirrors both. `projection_state`/
`projection_rebuild_state` gain a `key TEXT NOT NULL` column, composite
primary key `(projection_name, key)` - and **pre-seeding at registration
is gone entirely**: every instance, keyed or not, is now created lazily
on first touch, via a Postgres get-or-create-with-lock idiom
(`INSERT ... ON CONFLICT (projection_name, key) DO UPDATE SET state =
projection_state.state RETURNING state` - a no-op write on the
already-exists path, existing purely to acquire the row lock there too,
the identical guarantee a plain `SELECT ... FOR UPDATE` gave the old
always-pre-seeded schema). `caught_up_to` is untouched - a property of
the whole projection's progress through the stream, not of any one row.
`promote_projection_rebuild`'s single-row copy generalizes to a bulk
`DELETE`-then-`INSERT ... SELECT` across every key, the identical shape
`projection_consumed_event_types` already uses. `resolvers::projection_query`
gained the `key` argument and a `default_state` fallback for a key
nothing has touched yet (a customer with no purchase history is a
legitimate, common case, not a "not found" error).

**A real, intentional behaviour change surfaced by this pass, not a
regression**: with pre-seeding gone, `get_projection_state` for a key
nothing has ever touched now genuinely returns `None`, not a
default-valued row - three existing tests
(`skilj-core/tests/sync_projections.rs`'s own unconsumed-event case,
`skilj/tests/sync_projections.rs`'s own "starts at its own default
state, seeded at registration time" assertion) encoded the old
always-pre-seeded behaviour and needed updating to match, not silently
left passing on a stale assumption.

Verified: `skilj-core/tests/sync_projections.rs`/`async_projections.rs`
extended with the mechanical `keys`/`project` ripple every registered
`ProjectionDispatcher` test double needed, plus a new
`a_transfer_event_updates_both_accounts_own_row_from_one_fold` test - one
event, two independently-updated rows, from a hand-rolled dispatcher
directly. New `skilj/tests/projection_query.rs::keyed_projection_end_to_end`
(real Postgres + real JWKS + real REST-then-GraphQL): three real
`ItemPurchased` events for two different customers, each customer's own
row queried independently and correct, a never-touched key answering
with the default (empty) state, and the implicit `""` instance (`key`
omitted) answering the same way, untouched by any customer-keyed event.
**All tests pass on the first real-Postgres run, stable across two full
workspace runs.**

**`read_projection`'s own decrypt-on-read - automatic, not declared.**
The very last item from §9. **First draft rejected by the user, for a
real reason**: the natural mirror of `EventType`/`CommandType` - a
`Projection.sensitive_fields` declaration the projection author fills in
- was rejected before any code was written: an author who forgets to
declare a field that actually holds sensitive data silently leaks it to
every caller regardless of grant, and by the time anyone notices it may
already have been read by callers who should never have seen it. That
also conflicted with an existing, deliberate guarantee already on
`ProjectionQuery` (`SensitiveFieldsStayProtected`): "sensitivity is
declared once, on the EventType, and inherited rather than re-declared
per projection" - a declared-`sensitive_fields` design would have needed
reversing that too.

**The design that shipped instead, driven directly by the user's own
correction**: automatic detection, using exactly what keyed projections
(above) already provide - a row's own `key` is already a real subject
*value*, in plaintext (a `SensitiveField`'s own `subject_field` is never
itself encrypted). At query time: resolve every active `EncryptionKey`
whose `subject_value` equals the query's own `key`, across *every*
`subject_key` namespace (`db::list_active_data_keys_for_subject_value`,
one indexed query - no declared namespace needed); if the caller is
granted for that subject (`sensitive_field_is_granted`, `render_event`'s
own two-grant test, reused unchanged) and at least one key was found,
recursively walk the stored state JSON - every string leaf, any depth,
into objects and arrays alike (`encryption::decrypt_ciphertext_leaves`)
- trying `decrypt_leaf` against each; a match substitutes the plaintext,
no match leaves the leaf completely untouched. `decrypt_leaf`'s own AEAD
tag makes a false-positive match astronomically unlikely, so trying
broadly is safe, not a heuristic guess. This can't be silently forgotten
by a projection author - there is nothing to declare, and no code change
needed on the write side at all: `project()` already receives the raw
event today (nothing decrypts before folding), so an author copying a
source event's own sensitive field straight into projection state,
unchanged, automatically produces real, protectable ciphertext.
**Believed at the time that no spec change was needed** - the call above
was that `SensitiveFieldsStayProtected`'s existing text never described a
mechanism, only an outcome, and this delivers that outcome for real,
exactly as already written. **That call turned out to be wrong, caught by
a later drift audit (see project memory `skilj-drift-audit-2026-08-18`,
finding #9)**: the prose above `rule QueryProjection` and `ProjectionQuery`'s
own copy of `SensitiveFieldsStayProtected` both did describe a mechanism -
decryption decided *per field*, each against that field's own
`EncryptionKey.subject_value`, mirroring `render_event`/`render_command`
literally - which is not what shipped here. What shipped decides once per
query, against the queried instance's own `key` as the subject, not per
field at all. Both spots were fixed for real in a follow-up `allium:tend`
pass once the discrepancy was found, rather than left as a stale claim -
see that finding's own write-up for the corrected wording. Crypto-shredding
falls out for free too: `ForgetSubject` destroying the `EncryptionKey`
means the query-time lookup simply stops finding it, so the same stored
ciphertext in projection state becomes permanently undecryptable there
too, the same moment it happens - no separate propagation step.

**Scope, stated honestly**: this protects exactly a sensitive field
about the *same* subject the row is already keyed by (a customer's own
email inside their purchase-history row) - not a row whose key is about
something else entirely while embedding a *different* subject's data
nested inside it (a course row's own list of per-participant grades,
say). That leaf's real subject_value isn't the row's key, so it's never
a decrypt candidate here - genuinely harder (would need discovering
candidate subject values from inside the state tree, not just the row's
own key), and explicitly deferred, documented as a known limitation
rather than a silent gap.

Delivered: `db::list_active_data_keys_for_subject_value` (empty result
needs no `master_key` at all; a **non-empty** one with `master_key: None`
is the identical hard, actionable `MasterKeyNotConfigured` error the
render_event pass already established - confirmed with the user
beforehand, not reopened); `encryption::decrypt_ciphertext_leaves` (the
recursive walker, pure, no grant logic inside it - the caller only
invokes it once already confirmed granted); `projections::read_projection`
(thin wrapper - empty `data_keys` returns state verbatim, not even
re-parsed); `resolvers::projection_query` wires the grant check and both
new functions in, `query_projection` itself completely unchanged (still
takes a pre-computed `read_projection_result: String`, the identical
"fully caller-supplied" shape it already had).

Verified: `skilj-core/tests/encryption.rs` +12 - `decrypt_ciphertext_leaves`
decrypting a top-level leaf, a leaf nested in an object, leaves nested
inside a list of objects, trying multiple candidate keys until one
matches, leaving a non-matching or non-string leaf completely untouched;
`read_projection`'s own two cases. `skilj-core/tests/persistence.rs` +4
real-Postgres tests for `list_active_data_keys_for_subject_value`
(finds every namespace for a subject; excludes a destroyed key; empty
for an unknown subject with no master key needed; the hard error when
one's genuinely missing). New `skilj/tests/projection_query.rs::keyed_projection_decrypt_on_read_end_to_end`
(real Postgres + real JWKS + real REST-then-GraphQL): a projection keyed
by customer id, folding a real sensitive `email` field verbatim into its
own state with no `Projection.sensitive_fields` declared anywhere;
neither grant sees ciphertext, `can_read_sensitive` and the matching
`external_subject` both see plaintext, and `forgetSubject` destroying the
key makes even the previously-granted caller see ciphertext again -
crypto-shredding verified through a projection this time, not just raw
events. **All tests pass on the first real-Postgres run, stable across
two full workspace runs.**

**`derive_tags` for real, plus the dotted-path gap it shared - closing a
real, pre-existing production-panic risk, not a new feature.** Unlike
every item above, this wasn't unbuilt scope waiting for its pass: any
bounded context that registered a real `tag_mappings` entry and then
created or triggered a matching event/command hit `derive_tags`'s own
`todo!()` on the very first one, over REST or GraphQL alike - the
mechanism behind Dynamic Consistency Boundaries had simply never been
exercised for real. Found by grepping for `todo!()` across the codebase
once §8/§9 and every follow-up it produced was genuinely closed, with
nothing else queued; presented to the user via `AskUserQuestion`
alongside two alternatives, picked as the recommended option.

A second, closely-related gap surfaced during research and was folded
into the same pass rather than left half-fixed: `resolve_field`/
`payload_field_value`/`payload_field_value_mut` - the shared primitives
`derive_tags` itself needs to walk a payload - were *also* `todo!()` for
a two-segment dotted path (e.g. `address.country`), the identical
deferred shape. Not separable scope creep: `valid_tag_mappings`/
`valid_sensitive_fields` (registration-time validation) already called
straight into `resolve_field`'s own `todo!()`, so **registering a
`TagMapping` or `SensitiveField` with a dotted `field` already panicked
today**, before this pass touched anything.

**Design.** `schema_definitions` is new, mirroring `schema_properties`
exactly - extracts a schema's own top-level `"definitions"` map.
`resolve_field` gained a `definitions` parameter; its dotted-path branch
resolves the outer segment in `properties`, reads that property's own
`"$ref"`, and resolves it against `definitions` - the identical pattern
`skilj-graphql::projection_types::build_field` already used for GraphQL
type generation, reused rather than reinvented.
`payload_field_value`/`payload_field_value_mut` needed no `definitions`
at all - real JSON *data* has no `$ref`s, so a dotted path there is
simply nested `get`/`get_mut` calls, falling through to `None` at either
missing segment exactly like the existing bare-name case already did (so
`protect_sensitive_fields` inherited dotted-path support for free, no
changes of its own needed).

`derive_tags` itself: per `TagMapping`, a non-empty JSON array pushes one
`Tag(key, value: e)` per *distinct* scalar element (`json_scalar_to_string`;
a non-scalar element is silently skipped, not turned into a spurious
"absent" tag); everything else - a present scalar, an explicit `null`, an
empty array, or the field missing entirely - falls through to
`json_scalar_to_string`'s own `None`-for-non-scalar behaviour, which
already collapses every one of those onto the single correct
`Tag(key, value: null)` "absent" state with no extra branching needed. A
small new `push_unique_tag` helper does the Set<Tag> dedup. No signature
change on `derive_tags`'s own public shape, so every existing call site
(`create_external_event`/`create_direct_event`/`process_command`, both
REST and GraphQL command-submission paths) picked up real behaviour
automatically, and every existing empty-`tag_mappings` test kept passing
unchanged. `matches_filters`/`valid_filters` (the `Filter`/
`FilterOperator` mechanism) is a genuinely separate feature and was
deliberately not touched this pass.

Verified: new `skilj-core/tests/tag_derivation.rs` (16 tests) - every
documented `derive_tags` case, bare-field and dotted-path alike (scalar
present, explicit `null`, absent, list of scalars including a duplicate
collapsing, empty list, absent list-typed field, a non-scalar list
element skipped), plus `valid_tag_mappings`/`valid_sensitive_fields`
accepting a real dotted-path field and `protect_sensitive_fields`
actually encrypting a dotted-path leaf - all previously panicking paths.
`skilj-core/tests/command_processing.rs` +2 (a real, non-empty
`tag_mappings` producing real `consistency_tags`; a full DCB scenario
using real `derive_tags` output end to end, not hand-built `Tag`/`Event`
fixtures like every other DCB test in that file).
`skilj-core/tests/event_creation_surfaces.rs` +2
(`create_external_event`/`create_direct_event` each deriving real tags).
One real end-to-end test extending `skilj/tests/command_trigger.rs`:
`WithdrawMoney` gained a real `tag_mappings() -> vec![TagMapping { key:
"amount", field: "amount" }]`, and a new test proves a real
`POST /v1/commands/trigger` request produces a stored `Command` with real
`consistency_tags` - the previously-panicking path closed for real over
the actual REST surface, not just at the pure-function layer. Stale doc
comments referencing the deferred status were swept and fixed in
`command_processing.rs`/`type_registration.rs`. `cargo fmt`/`clippy -D
warnings` clean; `allium check`/`plan` baseline unchanged (18
diagnostics / 0 findings / 341 obligations, confirmed independently -
implementation only, no file under `specs/` touched this pass). **All
tests pass on the first real-Postgres run, stable across two full
workspace runs.**

**`valid_filters`/`matches_filters` for real - the `Filter`/`FilterOperator`
mechanism, the one remaining `todo!()` this whole thread's `todo!()`-grep
pattern turned up.** `Error::InvalidFilter` and every call site
(`create_event_type_subscription`, `fetch_events`, `consume_events`) were
already fully wired and already gated on `valid_filters` - a caller
supplying any non-empty `filters` panicked the process. GraphQL's
`eventsByType` subscription already had real wire parsing
(`resolvers::parse_filters`) but eagerly rejected any non-empty result;
REST's `GET /v1/events` had a documented wire shape
(`filter=field:op:value`, §7.3) that was never implemented, and
`GET /v1/events/consume` had no `filter=` param at all despite
`consume_events`'s own signature (mirroring the spec's `rule
ConsumeEvents`) already taking one - closed alongside the rest, since
it's the identical mechanism.

**A second, closely-related gap folded in, the same register as
`derive_tags`'s dotted-path fix**: the payload schema shape note calls
for `valid_tag_mappings`/`valid_sensitive_fields`/`valid_filters` to each
reject a field that doesn't land on a scalar or list-of-scalar leaf
(e.g. a bare nested-object-typed field, not dotted one level in) - only
`valid_filters` actually needed this shape classification built for its
own operator matrix, but `valid_tag_mappings`/`valid_sensitive_fields`
were retrofitted onto the same `classify`/`resolve_field_kind`, closing
a real pre-existing gap (they only ever checked existence, never shape)
rather than leaving it half-fixed beside brand new code with the
identical check.

**Two correctness findings from actually generating real `schema_for!`
output** (a throwaway scratch crate against this workspace's pinned
`schemars 0.8`/`chrono`), not assumed: `Option<T>` renders `"type"` as
`["string", "null"]`, never a bare string - a naive read would have
rejected every optional field; and a unit enum renders via `"$ref"` even
for a *bare* top-level field (schemars' own uniform `$ref`/`definitions`
mechanism, reused for a scalar leaf, not just the one-level nested-object
case) - field-kind classification follows a bare `$ref` itself to
recover this, rather than assuming a resolved schema is always already a
leaf.

**A more serious correctness finding, also verified empirically, not
assumed**: `chrono::DateTime<Utc>`'s serde serialization is not
fixed-width - it trims trailing-zero fractional digits, including down
to no fractional part at all when exactly zero - which breaks plain
lexicographic string ordering (`"...:00Z"` sorts *after*
`"...:00.500Z"` despite being chronologically earlier, since `'Z'` >
`'.'`). Caught before shipping the original "lexicographic, no parsing
needed" design (which had already been proposed and provisionally
agreed via `AskUserQuestion`) by generating and comparing real values,
not by inspection. Fixed by having `matches_filters` genuinely parse
both sides via `chrono::DateTime::parse_from_rfc3339`/
`NaiveDate::parse_from_str`/`NaiveTime::parse_from_str` and compare the
typed values (`PartialOrd`) - `chrono` is already a real `skilj-core`
dependency, no new one needed. The *decision* to give `date-time`/
`date`/`partial-date-time` string fields ordering operators (on top of
the usual `equals`/`contains`/`is_like`) is unchanged; only the
comparison mechanism is.

**A well-known-GraphQL-scalars check, searched online** (the
`graphql-scalars` library - DateTime, UUID, BigInt, EmailAddress, JSON,
PhoneNumber, etc.) against what this codebase can actually produce found
no custom GraphQL scalar *types* exist anywhere today - every
`chrono::DateTime<Utc>` field across the whole GraphQL layer (48 call
sites in `gql_types.rs`) renders as plain `TypeRef::STRING`. Introducing
real scalar wire-types was confirmed out of scope for this pass via
`AskUserQuestion` - a genuinely separate, much larger change touching the
whole existing convention, not just filtering. In scope: `uuid::Uuid`
wasn't usable as a payload field at all - `schemars` gained the `uuid1`
feature (one line, `uuid` was already a workspace dependency) - UUID
needs no special matrix entry, the plain string bucket's `equals`
already covers exact matching.

Delivered: new `FieldKind`/`classify`/`resolve_field_kind`/
`filter_operator_is_valid` in `event_store/mod.rs` (the type-to-operator
matrix); `matches_filters` real, with a `like_matches` SQL-LIKE helper
(`%`/`_` wildcards, no `regex` dependency) and a `string_ordering` helper
for the three ordered formats. `skilj-graphql`: the eager
`filters_not_supported_error()` gate and its now-dead function removed.
`skilj-rest`: new `parse_filter_param`/`parse_filter_params`
(`field:op:value`, `splitn(3, ':')` so a value may itself contain `:`);
`ConsumeQuery` gained the same `filter` field `EventsQuery` already had;
`RestError::FiltersNotSupported` removed. **A genuine mid-implementation
discovery**: `axum::extract::Query` (built on `serde_urlencoded`) doesn't
support a repeated `filter=`/`filter=` query param deserializing into
`Vec<String>` at all - found by the real end-to-end REST test itself
failing, not assumed from documentation. Fixed with
`axum_extra::extract::Query` (built on `serde_html_form`, a drop-in
replacement), a new `axum-extra` dependency (`query` feature) - the
"write a real end-to-end test" discipline this whole thread has followed
caught a real gap in the already-documented wire contract that no
smaller test could have.

Verified: new `skilj-core/tests/event_filtering.rs` (29 tests) - the
full type-to-operator matrix, both `schema_for!`-probe shapes, the
bare-nested-object-vs-dotted-path distinction, and `matches_filters`
including the exact date-time ordering bug found (asserting the
*chronologically* correct result plain string `Ord` gets backwards) and
a malformed date falling through to `false` rather than panicking. The
three existing `catch_unwind`-based tests documenting the old `todo!()`
limitation rewritten into real `Err(...)`/`.code()` assertions. One new
`deliver_to_subscriptions` test proving a real filter actually narrows
delivery (the existing test only ever used empty filters). New
`skilj/tests/event_fetch_rest.rs` (real Postgres): `GET /v1/events`
narrowing for real over HTTP, a malformed `filter=` param and an
undeclared field both producing 400. New test in
`skilj/tests/event_subscription.rs` (real Postgres + real JWKS, extending
the existing harness rather than duplicating it): `eventsByType(filters:
...)` narrowing a live websocket push. **All tests pass on the first
real-Postgres run, stable across two full workspace runs.**

---

## 9. Next steps

Every item in §8's original backlog, and every follow-up it led to, is
now done: items 1–4 (persistence, the builder registry, REST fully
wired), item 5 (`skilj-graphql`) through Phase 6 (`EventSubscription`),
item 6 (`project()`, sync and async, `ProjectionRebuild` replay and
promotion), real decrypt-on-read for `render_event`/`render_command`,
keyed / multi-row Projections, `read_projection`'s own decrypt-on-read,
`derive_tags` for real (plus the dotted-path gap it shared with
`valid_tag_mappings`/`valid_sensitive_fields`/`protect_sensitive_fields`),
and now `valid_filters`/`matches_filters` for real too (the `Filter`/
`FilterOperator` mechanism, plus the shared scalar/list-of-scalar-leaf
retrofit it shared with `valid_tag_mappings`/`valid_sensitive_fields`) -
see each item's own writeup for the full breakdown. **§8/§9's backlog,
and every genuinely pre-existing `todo!()`/gap this multi-session thread
turned up along the way, is now closed out - a fresh grep for `todo!()`
across the whole workspace at this point finds nothing left.** The one
deliberately-out-of-scope limitation, named explicitly rather than
silently: a projection row whose own key isn't the same subject as a
sensitive value nested somewhere inside it (e.g. a course row's own list
of per-participant grades) - a genuinely different, harder problem, not
attempted. Real GraphQL scalar types (a `DateTime`/`UUID` `Scalar` in
the dynamic schema, replacing the current uniform `TypeRef::STRING`
rendering) were surfaced and deliberately deferred too - a genuinely
separate, much larger change than filtering needed, not a `todo!()`
anywhere today.

`/allium:propagate`, scoped to one representative surface at a time,
remains the right tool once code lands that a surface's obligations
haven't been checked against yet.

## 10. Dynamic Consistency Boundary (DCB) alignment

skilj's own consistency mechanism - `Tag`s on events, a command's
`consistency_tags` deriving a `consistency_boundary` and
`matching_events` set that `decide()` is evaluated against (§1) - is,
structurally, an implementation of the **Dynamic Consistency Boundary
(DCB)** pattern: a named, actively-discussed approach in the event-
sourcing community, introduced by Sara Pellegrini, with a dedicated
site, specification, and community at [dcb.events](https://dcb.events/).
Worth saying explicitly, for a reader who already knows DCB from
elsewhere - not a rename of anything here, and not a claim of wire/API
compatibility. The DCB [specification](https://dcb.events/specification/)
itself is deliberately language/implementation-agnostic
("implementations are not required to use the same terms or
function/field names — as long as they offer equivalent functionality"),
so what follows is a mapping, not a migration:

| DCB term (from the [spec](https://dcb.events/specification/)) | skilj's own term | Note |
|---|---|---|
| `Event { type, data, tags }` | `Event { event_type, payload, tags: Vec<Tag> }` | DCB's `tags` are opaque strings (conventionally `"key:value"`); skilj's `Tag { key, value: Option<String> }` (`skilj-core/src/shared/mod.rs`) is structured, not opaque - same purpose, different representation |
| `Query { items: [{ types?, tags? }] }` | `consistency_tags` (derived per command/event via `TagMapping`/`derive_tags`) | skilj never builds an explicit `Query` object; the equivalent selection is computed directly from the payload's own tag mappings |
| `read(query) -> SequencedEvents` | `consistency_boundary_and_matching_events(bounded_context_events, consistency_tags) -> (Option<i64>, Vec<Event>)` (`skilj-core/src/event_store/mod.rs`) | Same "read the relevant slice before deciding" ordering DCB requires; skilj computes the boundary (highest matching sequence) in the same pass |
| `append(events, condition?)`, `AppendCondition { failIfEventsMatch, after }` | the DCB-conflict recheck inside `db::submit_command` (re-run the read under `next_sequence`'s own row lock, redispatch `decide()` on change) | Mechanistically different: DCB's own model is one declarative condition passed to a single atomic append; skilj gets the identical guarantee via a Postgres row lock plus optimistic-read-then-locked-recheck-and-retry instead of a condition object |
| the "decision model" (an app-level concept the spec deliberately leaves out of scope) | `CommandType::decide()` | Named independently in skilj's own Allium spec, not borrowed from DCB - same role the DCB examples describe |

Two other Rust event stores - Tephra and Disintegrate - already appear
in DCB's own [implementation list](https://dcb.events/resources/libraries/)
- worth knowing about as neighbors, not a comparison to relitigate here.
Whether to list skilj there too is tracked separately in
`docs/open-source-todo.md`, gated on this repository actually being
public - a link to a private repo serves no one who'd read that list.

## 10. OpenTelemetry tracing, logging, and metrics

Added in a later pass: real distributed tracing across the HTTP surfaces,
the domain engine, and the two background tasks, exported as
OpenTelemetry via OTLP/HTTP. Not spec-driven (`specs/skilj.allium` has no
tracing entity), so this section - not an obligations table - is its
record.

**Init ownership: library crates never install a subscriber.**
`skilj-core`/`skilj-rest`/`skilj-graphql`/`skilj` only ever emit
`tracing` spans/events - none of them call `tracing_subscriber::registry().init()`
or anything like it. Standard Rust library practice (the same reason
`axum`/`sqlx`/`reqwest` themselves only emit `tracing`): a library that
installs a global subscriber takes that decision away from whatever
process embeds it. `skilj-demo/src/bin/server.rs`'s `init_telemetry` is
the reference example of what a real consuming application does instead
- always a console `fmt` layer (`RUST_LOG`, default `info`), and, only
when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, an `opentelemetry-otlp`
HTTP/protobuf exporter feeding `tracing-opentelemetry`'s layer too. This
also fixes the crate boundary: `skilj-core`, `skilj-rest`, and
`skilj-graphql` depend only on `tracing` plus the lightweight
`opentelemetry`/`opentelemetry-http`/`tracing-opentelemetry` *bridge*
crates (API-level - extracting/injecting W3C `traceparent` headers and
attaching that context to the current `tracing::Span`); the
`opentelemetry_sdk`/`opentelemetry-otlp`/`tracing-subscriber` stack that
actually builds an exporter is `skilj-demo`-only.

**OTLP/HTTP, not gRPC.** Reuses the `reqwest`-based HTTP stack already in
the workspace instead of adding `tonic`/`prost`/`hyper` as a second,
heavier RPC stack (§2.1's own reasoning for picking `axum`, applied
again here). One consequence accepted rather than fought:
`opentelemetry-otlp`'s `reqwest-client` feature pulls a newer `reqwest`
major version than the workspace's own pinned one, so `skilj-demo`'s
dependency tree carries both - cosmetic duplication, not a version
conflict, and confined to that one leaf crate.

**Instrumentation depth**, roughly outside-in:

- `skilj-rest`/`skilj-graphql` each mount one hand-rolled
  `axum::middleware::from_fn` (`trace_request` - identical logic
  duplicated in both, since no crate they already share would justify a
  new one) that builds one request-level span per call, extracts an
  incoming `traceparent`/`tracestate` pair via
  `opentelemetry::global::get_text_map_propagator`, and sets it as that
  span's parent *before* the span is ever entered. That ordering matters:
  `tracing-opentelemetry`'s `OpenTelemetrySpanExt::set_parent` only
  succeeds before a span's first `enter` (it returns
  `Err(AlreadyStarted)` afterwards, silently ignored here - a safe no-op
  when the consuming app never registers a real propagator, which is the
  common case). This is why `tower_http::trace::TraceLayer` isn't used
  for this: it creates and enters its own span internally, with no hook
  to set the parent first, so the two don't compose safely. A safe no-op
  either way when no propagator is registered.
- `skilj-graphql`'s schema builder (`schema::build`) registers
  `async_graphql::extensions::Tracing` - async-graphql's own built-in
  extension, wrapping every resolved field across all 17 resolver modules
  in a span automatically, so none of them need hand-written
  instrumentation. Deliberately the plain, `tracing`-backed `Tracing`,
  not `extensions::OpenTelemetry` (which talks to the OTel SDK directly)
  - keeps `skilj-graphql` on the same "emit via `tracing` only" boundary
  every other library crate here keeps.
- `skilj-core::db`'s ~77 functions each get `#[tracing::instrument(skip_all)]`
  - mechanical, one pattern throughout. `skip_all` rather than capturing
  arguments: several take a `&Pool`/`&mut PgConnection` (no useful
  `Debug`) or a raw JSON payload (which may hold a `sensitive_fields`
  value - the span attribute stream is exactly the kind of place that
  shouldn't leak one). Functions carrying a `bounded_context: &str` (or
  `projection_name`/`command_type`/`event_type`/`name`) parameter get
  that field explicitly instead, since it's cheap, structured, and
  genuinely useful for correlating a trace to a bounded context.
  `event_store::process_command` and `db::submit_command` - the domain
  engine's own real entry points (§1) - get explicit `bounded_context`/
  `command_type` fields the same way, since `command_type: &CommandType`
  isn't itself a `&str` the mechanical pass could pick up.
- `skilj`'s two background `tokio::spawn` loops (the async-projection
  poller and the scheduler) each wrap one tick's work in a
  `tracing::info_span!("async_projection_tick" | "scheduler_tick")` via
  `Instrument::instrument` - trace roots, since there's no HTTP request
  for either to inherit a parent from. Their pre-existing `eprintln!`
  error paths became `tracing::warn!` calls at the same time, for
  consistency; `SkiljBuilder::build`'s own one-time bootstrap-secret
  `eprintln!` (§1.5) stays as-is, deliberately - a secret an operator
  must see regardless of `RUST_LOG`/exporter configuration is exactly the
  kind of output that shouldn't route through a filterable log level.

**Logs, a second OTel signal, added in a later pass.** Traces above
answer "what happened during this request/tick"; logs answer "what did
the process say" - OpenTelemetry treats them as genuinely separate
signals with their own exporters, not one unified into the other.
Rather than adding new call sites anywhere, `skilj-demo`'s
`init_telemetry` bridges the `tracing::info!`/`warn!` events already in
the codebase (§ above) into OTel's Logs signal too, via
`opentelemetry-appender-tracing`'s `OpenTelemetryTracingBridge` -
another `tracing_subscriber::Layer`, stacked alongside the trace layer
and the console `fmt` layer, all three gated by the same top-level
`EnvFilter`. Each exported `LogRecord` automatically carries its
enclosing span's trace/span id, so a log line and the trace it happened
inside correlate in the collector without any extra plumbing. Its own
`SdkLoggerProvider` + OTLP/HTTP log exporter mirror the trace pipeline's
shape exactly (`opentelemetry-otlp`'s `logs` feature, alongside the
already-enabled `trace` one) - same `OTEL_EXPORTER_OTLP_ENDPOINT` gate,
same "only `skilj-demo` builds an exporter or installs a subscriber"
boundary library crates never cross (§ above). No test added
specifically for this: unlike the parent-propagation question §'s
`tracing_middleware.rs` test needed the real SDK to answer, "does this
bridge convert `tracing` events to `LogRecords`" is `opentelemetry-appender-tracing`'s
own tested behavior, not this codebase's.

**Metrics, the third OTel signal, added in a still-later pass.** Same
crate boundary as traces/logs: library crates record measurements
through `opentelemetry::global::meter(...)` (the API crate, already a
dependency everywhere `tracing`/context-propagation is used) - never the
SDK/exporter, which is `skilj-demo`-only, same as always. Five
instruments, each a module-level `LazyLock<Counter<_>>`/`LazyLock<Histogram<_>>`
next to its call site (no shared metrics module, matching how tracing
instrumentation was added directly at each call site too):
`http.server.request.duration` (the OTel HTTP semantic-convention name,
not `skilj.`-prefixed) in both `skilj-rest`/`skilj-graphql`'s
`trace_request`; `skilj.commands.processed` (by `bounded_context`/
`command_type`/`outcome`) at `db::submit_command`'s two return points;
`skilj.events.appended` (by `bounded_context`/`event_type`); and
`skilj.background_task.tick.duration`/`skilj.background_task.errors` (by
`task`, plus a `reason` slug for the latter) in `skilj`'s two background
loops.

`skilj.events.appended` deliberately does *not* live inside
`insert_event` itself, despite that being the one function every
event-creation path funnels through exactly once (tempting, since it
would mean a single call site instead of five) - `insert_event` runs
inside a transaction its own caller might still roll back afterwards
(sync-projection folding failing, an encryption-key join insert failing,
etc.), and counting there would over-count on any such rollback. Instead
it lives at each of the five `broadcaster.publish(event)` call sites
across `skilj-core::db` - the same "only after `tx.commit()` succeeds"
checkpoint those calls themselves already exist to enforce (see e.g.
`insert_event_and_update_sync_projections`'s own doc comment), via a
small shared `record_event_appended` helper so the five sites don't each
duplicate the `KeyValue` construction.

A real correctness trap, caught by reading `opentelemetry::global::meter()`'s
own doc comment rather than assumed: unlike the trace/log bridges (wired
to a concrete provider once, directly, inside `init_telemetry`),
`global::meter()` **snapshot-binds** to whichever `MeterProvider` is
globally registered at first call - "if the global `MeterProvider` is
changed after getting `Meter` instances from these calls, the `Meter`
instances returned will not reflect the change." Since every instrument
above is a `LazyLock`, it's only ever actually created the first time a
metric is recorded (the first command processed, the first HTTP
request) - safe only because `init_telemetry()`, which calls
`opentelemetry::global::set_meter_provider(...)`, is already `main`'s
very first statement, strictly before anything that could touch an
instrument runs. Every `LazyLock` instrument's own doc comment calls
this out; a future reordering of `main()` would silently turn every
metric into a permanent no-op.

`skilj-demo`'s metrics pipeline (`opentelemetry_otlp::MetricExporter` →
`PeriodicReader` → `SdkMeterProvider`) isn't a `tracing_subscriber` layer
at all, unlike the trace/log ones - metrics aren't `tracing` events, so
there's nothing to add to the `.with(...)` chain; recording happens
directly through each `Counter`/`Histogram` handle. Also nothing printed
to console when no collector is configured (unlike logs, there's no
sensible "print a histogram to stdout" fallback) - this pipeline is
simply inactive without one, the same as traces already are.

**A packaging bug this pass's own smoke test caught, not assumed fixed:**
`opentelemetry-otlp`'s default (non-`experimental_*`) batch processors/
periodic reader each run their own dedicated OS thread, not a Tokio
task - the very first real export attempt (triggered almost immediately,
by the SDK's own internal `otel_info!`/`otel_error!` log messages
reaching the log bridge) panicked with "there is no reactor running,
must be called from the context of a Tokio 1.x runtime". The cause:
`opentelemetry-otlp`'s `reqwest-client` feature (the *async* reqwest
client) needs a Tokio reactor to make its HTTP call, which that
dedicated background thread never has. `opentelemetry-otlp`'s own
default feature set already pairs `http-proto` with
`reqwest-blocking-client` instead - a deviation from that default worth
reverting outright, not a case for reaching into the SDK's separate (and
still `experimental_*`-feature-gated) async-runtime batch processor
variants. Fixed by switching the workspace's `opentelemetry-otlp`
dependency to `reqwest-blocking-client`; re-run confirmed a graceful
`BatchLogProcessor.ExportError` (no real collector at the smoke test's
endpoint) rather than a panic.

**Tests, no OTel collector needed.** Each crate boundary gets one
representative test using an in-process capturing `tracing_subscriber::Layer`
(duplicated per file rather than factored into shared test-support code,
matching this repository's existing "each test file owns its own
fixtures" convention - see e.g. `skilj/tests/auto_register.rs`'s own
provisioning harness): `skilj-core/tests/tracing_instrumentation.rs`
(`process_command`'s own fields), `skilj-rest/tests/tracing_middleware.rs`
(a full `opentelemetry_sdk` + `tracing-opentelemetry` stack backed by
`opentelemetry_sdk`'s own `testing` in-process exporter feature, proving
a `traceparent` header actually continues that trace rather than
starting a fresh root - the one test here that needed the real SDK, not
just the capturing layer, since "did the parent propagate" is an OTel-
level question), `skilj-graphql/tests/tracing_extension.rs` (a
standalone two-field schema proving `Tracing` traces an Object-returning
field and skips a scalar-returning one), and
`skilj/tests/tracing_background_tasks.rs` (a real `.build()` against a
real Postgres, short poll intervals, asserting both tick spans fire).

Metrics get the identical treatment, one file per crate boundary, using
`opentelemetry_sdk::metrics::InMemoryMetricExporter` (feature `testing`)
+ a real `SdkMeterProvider` + `force_flush()` instead of waiting out the
periodic export interval: `skilj-core/tests/metrics_instrumentation.rs`
(`insert_event_and_update_sync_projections` records `skilj.events.appended`
with the right `bounded_context`/`event_type` attributes),
`skilj-rest/tests/metrics_middleware.rs` (a request through `router()`
records `http.server.request.duration` with the right method/route
attributes - `skilj-graphql` skipped its own copy here, identical
`trace_request` code already covered), and
`skilj/tests/metrics_background_tasks.rs` (reuses
`tracing_background_tasks.rs`'s own harness, asserting both background
loops' `skilj.background_task.tick.duration` data points appear).

### 10.1 Four smaller follow-ups

Four further, smaller observability gaps, closed in the same later pass:

**Span/log error semantics.** Neither `skilj-rest` nor `skilj-graphql`'s
`trace_request` previously marked a request's own span as errored, or
logged anything at `error!` - a 500 and a 200 looked identical in a
collector except for a plain `status` attribute. Both now check
`status.is_server_error()` (5xx is always a genuine, unexpected
server-side failure here - a business rejection renders as 200, and
auth/validation failures as 4xx, per §5.4/§7.5) and, when true, record
`tracing-opentelemetry`'s own well-known `otel.status_description` field
(not `OpenTelemetrySpanExt::set_status` directly - see the note below)
and emit `tracing::error!`, which both prints regardless of a `warn`-or-
higher `RUST_LOG` filter and exports as an error-severity log record via
the same bridge every other event already goes through. Verified in
`skilj-rest/tests/tracing_middleware.rs` (extended, not a new file):
asserts the span named `"request"` - not the first `SpanData` off the
export channel, which turned out to be `db::access_token_kind`'s own
nested `#[tracing::instrument]` span, closing first - has `Status::Error`.

*A real dead end worth recording*: `OpenTelemetrySpanExt::set_status`
called directly appeared to silently do nothing in that same test, which
first looked like a genuine library limitation (`AlreadyStarted`-style,
matching the earlier `set_parent` finding). It wasn't - once the test
was fixed to read the right span, both the direct `set_status` call and
the field-based route should work equally well. The field-based one
(`otel.status_description`) is what's actually shipped, since it's the
mechanism `tracing-opentelemetry` itself documents for a status set well
after span creation, and it's the one this pass's test actually
exercises - not because the other one was proven broken.

**Graceful shutdown.** `skilj-demo`'s `axum::serve` previously ran with
no shutdown hook at all - a `SIGINT`/`SIGTERM` just killed the process,
silently dropping whatever was still sitting in each OTel batch
processor's buffer. `main` now passes `shutdown_signal()` (waits on
`Ctrl+C`, plus `SIGTERM` on Unix) to `.with_graceful_shutdown(...)`, and
calls a new `TelemetryProviders::shutdown()` - flushing/tearing down all
three providers, logging (not propagating) any failure - once
`axum::serve` returns. Verified for real, not just built: a real
Postgres (started by hand from the `postgresql_embedded`-cached binary,
same libxml2/`LD_LIBRARY_PATH` workaround as always), the actual compiled
`server` binary launched as a subprocess, a live request confirmed
against it, then `SIGTERM` - exits promptly both with no
`OTEL_EXPORTER_OTLP_ENDPOINT` set, and with one set to an unreachable
collector (confirms the shutdown path doesn't hang waiting on a flush
that can't succeed - each provider's own `shutdown()` call fails
gracefully, logged as a `WARN`, not a hang or a panic).

**JWKS fetch instrumented.** `access_control::JwksCache::refetch` - the
one `reqwest` call in `skilj-core` outside the OTel context-propagation
middleware itself - had no span of its own, so a slow or failing JWKS
fetch was invisible inside whatever span called `verify_and_extract_subject`.
Now `#[tracing::instrument(skip_all, fields(jwks_endpoint = %self.jwks_endpoint))]`,
the same mechanical treatment `skilj-core::db`'s own functions already
got.

**Trace id in error responses.** Both `skilj-rest::error::ErrorBody`
(REST) and `skilj-graphql::error::to_graphql_error` (the one conversion
every resolver's own rejection goes through) now include the current
span's OTel trace id - `trace_id` (REST, `#[serde(skip_serializing_if)]`)
/ `traceId` (GraphQL, a `code`-alongside extension) - so a caller
escalating a failure can quote back the exact trace a support engineer
would look up. `None`/omitted, not a string of zeroes, when no real
`tracing-opentelemetry` layer is installed (`TraceId::INVALID` filtered
out explicitly) - the common case for either crate used standalone, or
`skilj-demo` run without `OTEL_EXPORTER_OTLP_ENDPOINT` set. Same small
`current_trace_id()` helper duplicated in both crates, matching
`trace_request`'s own precedent for why.

## 11. `skilj-tui` - a Ratatui operator console

A sixth workspace member, added later: an interactive console for
browsing events, submitting commands, and inspecting projections against
a running `skilj` deployment - the one thing this project had no UI for
at all before this.

**Ratatui, not a web/native GUI.** Matches the project's single-language,
minimal-dependency ethos (the same reasoning that picked OTLP/HTTP over
gRPC to avoid a second RPC stack), and the actual use case - an
operator/debugging console, not a consumer-facing app - is exactly what
tools like `k9s`/`lazydocker` already prove this shape suits.

**GraphQL, not REST.** `skilj-rest`'s own doc comment already settles
this: REST is "for narrowly-scoped `AccessToken`-holding callers... never
for general application access, which is what `skilj-graphql` is for"
(§7.1). GraphQL also already has a working `EventSubscription` over
`graphql-transport-ws` - REST's own event access is poll-only.

**`skilj-tui` depends on no other `skilj-*` crate.** It only ever speaks
the wire protocol - the same "independently usable" spirit §3.1 gives
`skilj-graphql`/`skilj-rest`, from the client side this time. It never
talks to an IdP itself either: endpoint, bearer token, and bounded
context are all supplied up front (flag or env var -
`SKILJ_GRAPHQL_URL`/`SKILJ_TOKEN`/`SKILJ_BOUNDED_CONTEXT`), the same
credential presented however `curl -H 'authorization: Bearer <jwt>'`
already would be. Two real gaps this surfaced, both closed rather than
silently worked around:

- `skilj-graphql::auth::verify_jwt_to_role` always requires a JWT
  verified against a configured IdP - no bypass - and `skilj-demo`'s
  server never called `.identity_provider(...)`, so **GraphQL Role-based
  auth didn't work against `skilj-demo` at all** before this pass (only
  REST's `CommandToken` flow did). Fixed in `skilj-demo/src/bin/server.rs`:
  `serve_local_jwks`/`sign_jwt` spin up a tiny local JWKS endpoint
  signing with the same fixed, publicly-known test RSA keypair
  `skilj/tests/graphql_admin_console.rs` already uses (never a real
  secret - loopback-only), and the seeded admin `Role` now gets a real
  signed JWT printed alongside the REST command tokens already printed.
  Verified against a real Postgres, not just built: `skilj-demo/tests/graphql_auth.rs`
  proves the local JWKS server + signed JWT actually authenticates a real
  `queryEvents` GraphQL call end to end.
- `queryEvents`/`fetchCommands`/`submitCommand`/`projection` are all
  `AdminAccess`-gated, which the *same* Admin-level `RoleAccessMapping`
  `skilj-demo`'s server already creates covers - only `boundedContexts`
  (the cross-context directory) needs superadmin. So v1 skips that
  directory browse entirely: the operator names the bounded context they
  want (a flag), the same way they already have to know it to use a REST
  command token today - no superadmin credential needed anywhere.

**`projection(...): ProjectionResult!` is a GraphQL union** - one
concrete member type per registered projection, generated at runtime
from that projection's own JSON Schema (`skilj-graphql::projection_types::build`).
There is no generic `{ state: String }` shape to ask for, and this crate
has no prior knowledge of any bounded context's schema to hand-write a
selection set against. `src/projection_query.rs` resolves this with two
round trips, driven entirely by the wire protocol itself - never by
replicating the server's own internal type-naming scheme
(`graphql_type_name`'s `"{bc}_{projection}"` format is an implementation
detail, not part of the wire contract): first ask for just `__typename`
(the server always answers with which concrete union member a given
result actually is), then introspect *that* type's own fields
(`__type(name: ...)`) and build a selection set from them - recursing one
level into any nested `OBJECT` field, capped so a pathological or
self-referential shape can't recurse forever - then re-run the query for
real with that selection set. Verified against both a local mock server
(`tests/projection_query.rs`, including the one-level-of-nesting case)
and a real running `skilj-demo` server by hand (confirming the exact
hand-written query strings this module and `app.rs` use match the real
schema - `queryEvents`'s and `submitCommand`'s own field/argument names
included, and catching one real mismatch this way: `allEvents`'s
`QueriedEvent` has no `eventType` field, only `sequence`/`payload` - the
schema-builder source alone didn't make that obvious).

**Schema-driven command/event forms landed (Codeberg issue #8)**, once
§13's `eventTypes`/`commandTypes` queries gave this crate something to
build one from - the v1 gap this same section used to name is closed,
not just narrowed. The Commands tab's free-text type name became a real
picker over `commandTypes(boundedContext)`; picking one runs
`form::fields_from_schema` (new `src/form.rs`, pure/I/O-free - the
schema already came back with the type, no extra round trip) over that
type's own JSON Schema and replaces v1's raw-JSON payload entry with a
generated form. The Query Events tab's comma-separated free text became
the identical picker pattern over `eventTypes`, but a multi-select
checklist (`Space` toggles, `Enter` runs) rather than single-select,
since `queryEvents` takes several types at once.

Field classification (verified against real `schemars` 0.8 output via a
throwaway probe crate, never assumed): a bare or `Option`-wrapped
`"string"`/`"integer"`/`"number"` becomes a text/number input;
`"boolean"` becomes a real toggle (`Space`), not typed text; everything
else - `"object"`/`"array"`, or a bare `"$ref"` (schemars' identical
encoding for both a one-level-nested object *and* a unit enum) - falls
back to a raw-JSON input for that one field, deliberately not following
the `$ref` to tell the two apart (real extra work for a niche win; the
issue's own scope note allows this fallback "at least initially").

A real, pre-existing bug this surfaced rather than introduced: the
global "digits switch tabs" handling (`handle_key`) fired unconditionally,
so typing a digit into any free-text field - including v1's own raw-JSON
payload box - would jump tabs mid-keystroke. Latent and easy to miss
with v1's fields (nobody happened to type a payload starting with a
digit in testing), but the new `Widget::Number`/`Widget::Text` fields
make it immediately and severely visible (an `amount` field *is* digits).
Fixed by suppressing the global digit-switch specifically while editing
Commands' generated form (`Esc` first backs out to the picker, cheaply,
from the list already fetched - then digits switch tabs again); every
other tab's picker-only interaction (no free text at all, post-#8) stays
unaffected, and `ProjectionsTab`'s own free-text fields keep the
identical latent gap, out of scope for this pass.

Verified two ways: `src/form.rs`'s own unit tests (real `schema_for!`-
shaped fixture JSON, transcribed from the probe crate) for classification
and payload assembly, `tests/schema_driven_forms.rs` for the `App`-level
picker/form/checklist flow (including the digit-typing regression, driven
entirely through `App::handle` the same way `main.rs`'s loop would); and
a genuine interactive run in a real `tmux` pty (this sandbox has no TTY,
same `skilj-inspector` precedent as §14) against a real `skilj-demo`
server - picked `DepositMoney`, typed `a12`/`250` into its generated
fields (digits included, confirming the fix live), submitted, got a real
`"accepted": true`, then toggled `MoneyDeposited` in the Query Events
checklist and confirmed the just-created event came back.

**Structure**: `src/graphql.rs` (the client - `Client::request` for
queries/mutations, `spawn_subscription` for the `graphql-transport-ws`
protocol, adapted from the test-only client fixture already proven in
`skilj/tests/event_subscription.rs` into something this crate depends on
for real), `src/app.rs` (state + update logic - one `mpsc::channel<AppEvent>`
fed by the terminal-input reader thread, the Live Events subscription
task, and whichever ad hoc query/mutation task the user most recently
triggered), `src/ui.rs` (rendering, reads `App`, never mutates it),
`src/cli.rs` (the `clap`-derived config). Split into `src/lib.rs` +
`src/main.rs` (unusual for a `[[bin]]`-only crate) purely so
`tests/*.rs` can exercise `graphql`/`projection_query` directly - a
binary target alone has nothing integration tests can import.

**GraphQL responses are handled as raw `serde_json::Value`, indexed
dynamically** - not a codegen client (`graphql_client`/`cynic`), since
the schema is dynamic and grows per bounded context, so there's no fixed
schema file to codegen a typed client against. The same style every test
in the rest of this workspace already uses for GraphQL responses.

**Deliberately deferred past v1, named rather than silently skipped**:
the superadmin bounded-context directory browse and any admin-console
operations (role/access management, type registration) - a distinct
`AdminAccess`-vs-`Superadmin` concern from the "operate one bounded
context" core this v1 targets; an in-app IdP login flow (v1 only ever
takes a bearer token as config). Schema-driven command/event forms used
to be listed here too - closed by Codeberg issue #8, see above.

## 12. Cross-instance push completeness (Codeberg issue #2)

**A deliberate reversal, not a drift fix.** §10's own `EventBroadcaster`
writeup and the async-projection poller's writeup both cite the spec's
former blanket "Multi-instance / distributed deployment" exclusion as
the reason real-time delivery was single-process, in-memory only. That
exclusion has since been narrowed (`specs/skilj.allium`'s `Excludes`
list, and the two new guarantees `@guarantee DeliverySpansInstances`/
`@guarantee RegistrationReachesEveryInstance`) - confirmed explicitly
with the user before touching either the spec or the code, since it
reverses a decision made (and re-confirmed) twice before. What stays
excluded: partitioning/sharding, consensus, leader election,
cross-region replication, instance discovery/service registry. What's
now in scope: real-time push completeness and schema consistency for a
set of symmetric, stateless instances sharing one Postgres database -
nothing more.

**Investigated before designing, not assumed**: whether this needs
instances to discover each other, whether a read/write instance split is
needed, and whether the existing in-memory event cache and the "old
command" concern needed new handling.

- **No discovery needed.** Postgres `LISTEN`/`NOTIFY` is itself a pure
  connection-mediated broker - every instance opens its own listening
  connection and Postgres delivers to all of them. This is also already
  this codebase's own pattern for multi-instance safety elsewhere: the
  scheduler's `@guarantee ScheduleStateIsShared` and the admin
  `BoundedContext` seeding race fix both use `SELECT ... FOR UPDATE` row
  locking, not peer discovery.
- **No read/write split needed.** Every existing multi-instance
  mechanism (the two above, plus the DCB append-conflict recheck in
  `submit_command`) is already symmetric - any instance can write,
  coordinated purely through Postgres transactions/locks.
  `LISTEN`/`NOTIFY` is equally symmetric.
- **The event cache was already correct.** `skilj-core/src/event_cache.rs`
  was already built multi-instance-safe (its own doc comment, from an
  earlier drift-audit finding): every read compares its own highest
  known sequence against a fresh `db::latest_sequence` and fills the gap
  before answering. Nothing new needed here.
- **"Old commands" turned out not to be a real risk, but the GraphQL
  schema was.** Every real dispatch/submission call site
  (`db::submit_command` itself, plus the `command_submission`/
  `command_type_admin_operations`/`command_query` resolvers) already
  calls `db::get_command_type` fresh from Postgres on every request - no
  in-memory `CommandType` cache exists anywhere to go stale. The
  genuinely stale-prone in-memory cache was `skilj-graphql`'s
  `SchemaRegistry` (§5.1's own `ArcSwap<Schema>`), which had stayed a
  deliberate stub since Phase 6 (§9's writeup): "a projection registered
  after `graphql_router()` was called won't gain a `ProjectionResult`
  member until the process restarts." Multi-instance deployment was
  simply the trigger for finally building it - which also fixes the
  same-process version of that exact gap as a side effect.

**The mechanism**: one shared background task per `Skilj` instance
(`SkiljBuilder::build()` spawns it alongside the scheduler and
async-projection poller), holding one `sqlx::postgres::PgListener`
(`skilj_core::cross_instance::Listener`) subscribed to three channels:

1. `skilj_events` - `NOTIFY`'d from the same five `db::` call sites
   `record_event_appended`/`EventBroadcaster::publish` already share
   (`db::notify_event_appended`), right after commit. Payload is a
   pointer (`bounded_context`+`sequence`), not the full event - well
   under Postgres's 8000-byte cap, and every subscriber already
   re-fetches/renders the real event via the DB-backed path anyway. On
   receipt, the listener fetches the event with the plain, **uncached**
   `db::get_event_by_sequence` and republishes it into the local
   `EventBroadcaster`.
2. `skilj_revocations` - `NOTIFY`'d from `access_management`'s two
   `RevocationBroadcaster::publish` call sites (`db::notify_revocation`).
   On receipt, republishes the `RevokedMapping` into the local
   `RevocationBroadcaster`.
3. `skilj_registration_changed` - `NOTIFY`'d (`db::notify_registration_changed`,
   no payload - a bare "go check" signal) from the six `db::` functions
   that change what the schema needs to expose
   (`upsert_event_type`/`upsert_command_type`/`upsert_projection`/
   `insert_bounded_context`/`update_bounded_context_status`/
   `hard_delete_bounded_context`). On receipt, rebuilds and swaps the
   schema via `SchemaRegistry::rebuild`.

**A real bug this surfaced during verification, worth recording**: the
first version of the event-refetch path used
`db::get_event_by_sequence_cached` (routing through `EventCache`) rather
than the plain uncached read. That raced with the *original* write
path's own `event_cache.append(&event)` call for the identical
just-committed event - this instance's own self-`NOTIFY` can be received
and dispatched before that `.await` continuation resumes -
and `EventCache::try_event_by_sequence`'s own `freshen()` backfill has no
dedup against a concurrent direct `append()`, so the same event could
land in the cache twice. Caught by `skilj/tests/event_fetch_rest.rs`'s
own filter test going flaky (not deterministic - only sometimes
duplicated), traced to the exact race by re-running it against a
persistent (non-embedded) Postgres where the timing never lined up, then
reproducing and confirming the duplicate payload directly. Fixed by
switching the cross-instance refetch to the uncached read, which has no
reason to touch the cache at all - it is a one-shot read with no
repeated-read benefit to gain from caching in the first place.

**Not built, and explicitly not needed**: `SchemaRegistry` itself
already existed as a stub (`ArcSwap<Schema>`); this pass gave it real
`build`/`rebuild` methods and changed `skilj_graphql::router` and both
its handlers (`graphql_handler`/`graphql_ws_handler`) to read the live
schema per-request from an `Arc<SchemaRegistry>` rather than a schema
value baked into `axum` state once. `SkiljBuilder::build()` now builds
this registry itself (using the same `Dispatcher`/`ProjectionDispatcherImpl`
construction the reconciliation pass already needs *before* `Skilj`
itself exists to hand out `command_dispatcher()`), so `graphql_router()`
just shares it rather than building a schema per call.

**No `SkiljBuilder` opt-in toggle** - always-on, resolving the proposal's
own open question this way: nothing else in `SkiljBuilder` gates
correctness-affecting behavior behind an opt-in, and one extra idle
Postgres connection per instance is cheap.

**Verified end-to-end, not just unit-level**:
`skilj/tests/cross_instance.rs` runs two real `Skilj` instances, each
its own `axum::serve` listener with its own full set of background
tasks, sharing one Postgres database - and proves, in one flow, that (1)
a command submitted on instance A delivers to a live `allEvents`
subscription connected to instance B, (2) a revocation performed on
instance A closes a *quiet* subscription on instance B (one that never
sees another event - the only way to prove the push path, not the
per-delivery re-check, closed it), and (3) a `Projection` registered on
instance A becomes queryable on instance B's own GraphQL schema (checked
via `__type(name: "Query") { fields { name } }` introspection, polled
briefly since the rebuild is asynchronous) without instance B ever
restarting.

## 13. Self-describing GraphQL surface: `eventTypes`/`commandTypes` (Codeberg issue #6, "5a")

`TypeRegistration`'s `projections`/`scheduledEventTypes` queries already
let a caller discover what's registered without already knowing its
name; `eventTypes(boundedContext: String!)`/`commandTypes(boundedContext:
String!)` close the identical gap for event and command types
themselves - `skilj-tui`'s Commands/Query Events tabs (and issue #8's
schema-driven forms, once picked up) need a real type picker instead of
a name typed by hand.

Two new `db::` functions, `list_event_types_for_bounded_context`/
`list_command_types_for_bounded_context` (`skilj-core/src/db/mod.rs`) -
the same query `list_scheduled_event_types` already runs, minus its
`WHERE system_triggered_allowed = true` filter. Two new `AdminAccess`-
gated GraphQL fields on `Query`
(`skilj-graphql/src/resolvers/type_registration.rs`'s
`event_types_field`/`command_types_field`), copying
`scheduled_event_types_field`'s own shape exactly. `specs/skilj.allium`'s
`TypeRegistration` surface gained two new unfiltered `exposes:` loops
(`for event_type in bounded_context.event_types`/`for command_type in
bounded_context.command_types`) that deliberately stay separate from the
existing `where system_triggered_allowed = true`-filtered `scheduled_type`
loop rather than folding into it - the scheduled-only fields
(`system_triggered_schedule`/`missed_occurrence_policy`/`last_fired_at`/
`schedule_position`) would otherwise show as empty/null for every
non-scheduled type, contradicting that loop's own existing "types never
opted into scheduling are left out rather than listed with three empty
values" design. `@guarantee GrantScopedToBoundedContext` got one more
sentence confirming the same context-scoping already promised for
projections/scheduled types applies here too.

Verified end-to-end in `skilj/tests/graphql_type_registration.rs`
(`event_types_and_command_types_list_every_registered_type`): registers
one event type and one command type via the real mutations, queries both
new fields, and asserts the full registered set comes back - plus the
empty-bounded-context and unauthenticated-caller cases every other field
on this surface already covers.

## 14. `skilj-inspector` - a standalone read-only Postgres console (Codeberg issue #6, "5b")

Even with §13's fix, everything about a running `skilj` deployment still
requires `skilj-graphql` itself to be up - the exact moment an operator
most wants to look (the app is down, Postgres isn't) has no tool at all.
`skilj-inspector` is a second Ratatui console, built the opposite way
from `skilj-tui` on purpose: where that crate is deliberately a pure
GraphQL client with zero dependency on any other skilj crate (§11),
`skilj-inspector` depends on `skilj-core` directly, since raw Postgres
access *is* the whole point - there's no `Role`/`RoleAccessMapping`
layer to authenticate against when nothing is serving GraphQL. One
required arg, `--database-url`/`DATABASE_URL`, no IdP config, no token.

**Read-only by construction, not just convention** - stated as an
explicit doc comment on the crate root: every function in its `data`
module calls only existing `skilj_core::db` read functions
(`list_bounded_contexts`, the two new `list_*_types_for_bounded_context`
from §13, `list_projections_for_bounded_context`,
`list_recent_events_for_bounded_context`), reusing them rather than
re-deriving SQL in a second place, and the crate never calls
`db::migrate` or any write path. One real gap this surfaced:
`list_recent_events_for_bounded_context` (like `list_events_for_bounded_context`)
`.expect()`s the bounded context row already exists, assuming its caller
already checked - true for every existing caller, but not for a tool
whose whole premise is "look something up without already knowing it's
there." `data::load_bounded_context_data` now checks
`db::get_bounded_context` first and short-circuits to the same
all-empty shape the other three list functions already give an
unregistered context, rather than propagating that panic.

**Sensitive fields render as ciphertext, always** - confirmed with the
project owner as this crate's one real open design question before
building it (raw Postgres access bypasses GraphQL's per-field
entitlement check entirely, so *something* has to be decided here). The
chosen answer needed zero special-case code to implement correctly:
`encryption::encrypt_leaf` already substitutes ciphertext directly into
the JSON payload leaf at write time, so the stored `payload` column
already *is* ciphertext for every sensitive field before this crate's
read path ever runs - rendering a row exactly as stored is already
correct. This crate never accepts an `EncryptionMasterKey` and has no
decryption code path at all, not even behind a flag; anyone needing
plaintext goes through GraphQL, where the real entitlement check
(`can_read_sensitive`/subject-match) lives and stays the only path to
it.

**UI shape** mirrors `skilj-tui`'s own `app.rs`/`ui.rs` split (a `Tab`
enum, one `App` struct `handle_key` mutates, one `draw()` per tab) -
five tabs (Bounded Contexts, then within a selected one: Event Types,
Command Types, Projections, Events) rather than reinventing the
pattern. Simpler than `skilj-tui`'s own loop in one respect: no live
subscription means no background task or input channel is needed - a
plain `crossterm::event::poll` loop is enough, since every read is
already a key press away.

Verified two ways: `skilj-inspector/tests/data.rs` against real
(embedded) Postgres, seeding directly through `skilj_core` writes
(including a real encrypted sensitive field via
`db::create_and_insert_direct_event` - the same function the REST
direct-creation endpoint itself calls) and asserting the read layer's
output, especially that a sensitive field's plaintext never appears in
what comes back; and a genuine interactive run in a real `tmux` pty
(this sandboxed environment has no TTY, so a plain `cargo run` hangs on
`enable_raw_mode`) against a freshly seeded database - drilled into a
bounded context, cycled through all five tabs, confirmed the sensitive
field rendered as ciphertext on screen (not just in the test assertion),
and quit cleanly with `q`.
