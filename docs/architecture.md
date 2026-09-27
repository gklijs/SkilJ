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

## Table of contents

Numbered sections only - a pass's own internal subheadings (a "Method"/
"Verified"/"Recommendation" inside a single numbered section) aren't
listed separately here; see that section itself for its own structure.

- [1. The plugin API: `decide()` and `project()`](#plugin-api-decide-project)
  - [1.1 Both are synchronous](#11-both-are-synchronous)
  - [1.2 JSON Schema is derived from the Rust type, not hand-written](#12-json-schema-is-derived-from-the-rust-type-not-hand-written)
  - [1.3 Trait-per-type, not closures, not a builder DSL](#13-trait-per-type-not-closures-not-a-builder-dsl)
  - [1.3.1 `#[requires_role(...)]`](#131-requires_role)
  - [1.3.2 `gql_object!` — internal wire-type codegen, a different register entirely](#132-gql_object-internal-wire-type-codegen-a-different-register-entirely)
  - [1.3.3 `#[auto_register]` + `BOUNDED_CONTEXT` — reopening §1.3's "no macros" call, deliberately](#133-auto_register-bounded_context-reopening-13s-no-macros-call-deliberately)
  - [1.4 `matching_events` is a generated per-bounded-context enum](#14-matching_events-is-a-generated-per-bounded-context-enum)
  - [1.5 Startup registration is a builder](#15-startup-registration-is-a-builder)
  - [1.6 Raw `Event` → typed `Event`: the `BoundedContextEvent` trait](#16-raw-event-typed-event-the-boundedcontextevent-trait)
  - [1.7 The builder's internal registry, and the decide()-dispatch bridge](#17-the-builders-internal-registry-and-the-decide-dispatch-bridge)
- [2. Ecosystem choices](#ecosystem-choices)
  - [2.1 Web framework: `axum`](#21-web-framework-axum)
  - [2.2 Database layer: `sqlx`](#22-database-layer-sqlx)
  - [2.2.1 Every `Integer` is `i64`/`BIGINT`, uniformly](#221-every-integer-is-i64bigint-uniformly)
  - [2.2.2 Schema-per-bounded-context storage](#222-schema-per-bounded-context-storage)
  - [2.3 Property-based testing: `proptest`](#23-property-based-testing-proptest)
  - [2.4 Configuration: a plain builder, not a config-loading crate](#24-configuration-a-plain-builder-not-a-config-loading-crate)
  - [2.5 Payload serialization: JSON only, not pluggable, permanently](#25-payload-serialization-json-only-not-pluggable-permanently)
- [3. Crate and module structure](#crate-module-structure)
  - [3.1 Four crates, not one](#31-four-crates-not-one)
  - [3.2 `skilj-core`: grouped by domain concern](#32-skilj-core-grouped-by-domain-concern)
  - [3.3 `skilj-graphql` and `skilj-rest`](#33-skilj-graphql-and-skilj-rest)
- [4. Error handling](#error-handling)
  - [4.1 Two tiers, because the spec already draws this line](#41-two-tiers-because-the-spec-already-draws-this-line)
  - [4.2 One shared trait unifies both tiers for rendering](#42-one-shared-trait-unifies-both-tiers-for-rendering)
- [5. The GraphQL wire contract](#graphql-wire-contract)
  - [5.1 One unified schema, rebuilt at runtime](#51-one-unified-schema-rebuilt-at-runtime)
  - [5.2 Namespacing: nested per bounded context — corrected](#52-namespacing-nested-per-bounded-context-corrected)
  - [5.3 Pagination: Relay-style cursor connections](#53-pagination-relay-style-cursor-connections)
  - [5.4 Error shape — and a correction: business rejections aren't errors](#54-error-shape-and-a-correction-business-rejections-arent-errors)
- [6. IdP trust configuration](#idp-trust-configuration)
- [7. The REST wire contract](#rest-wire-contract)
  - [7.1 Purpose: narrowly-scoped agents and automated callers, not general access](#71-purpose-narrowly-scoped-agents-and-automated-callers-not-general-access)
  - [7.2 Routing: capability-based, not type-or-context-in-path](#72-routing-capability-based-not-type-or-context-in-path)
  - [7.3 Request/response bodies](#73-requestresponse-bodies)
  - [7.4 Three ways to read events, on purpose](#74-three-ways-to-read-events-on-purpose)
  - [7.5 Error mapping](#75-error-mapping)
- [8. Open for a future pass](#open-for-a-future-pass)
- [9. Next steps](#next-steps)
- [10. Dynamic Consistency Boundary (DCB) alignment](#dcb-alignment)
  - [10.1 The query algebra is narrower than the spec's, deliberately](#101-the-query-algebra-is-narrower-than-the-specs-deliberately)
- [10b. OpenTelemetry tracing, logging, and metrics](#otel-tracing-logging-metrics)
  - [10b.1 Four smaller follow-ups](#10b1-four-smaller-follow-ups)
- [11. `skilj-tui` - a Ratatui operator console](#skilj-tui-console)
- [12. Cross-instance push completeness (Codeberg issue #2)](#cross-instance-push-completeness)
- [13. Self-describing GraphQL surface: `eventTypes`/`commandTypes` (Codeberg issue #6, "5a")](#self-describing-graphql-surface)
- [14. `skilj-inspector` - a standalone read-only Postgres console (Codeberg issue #6, "5b")](#skilj-inspector)
- [15. `skilj-tui` debugging enhancements (Codeberg issue #7)](#skilj-tui-debugging-enhancements)
- [16. Declarative bounded-context format + codegen: a prototype, not a build (Codeberg issue #5)](#declarative-bounded-context-codegen-prototype)
- [17. Event/command codegen, for real: the narrower cut (Codeberg issue #5)](#event-command-codegen-real)
- [18. Ultra-review fixes: an access-control leak, a duplicate-delivery bug, and two nits](#ultra-review-fixes)
- [19. Optional snapshotting for `matching_events`: a discussion, not a build](#optional-snapshotting-matching-events)
- [20. Four new filter operators: geo, color, IP subnet, and generic `in`](#four-new-filter-operators)
- [21. Optional idempotency key for command submission (Codeberg issue #12)](#optional-idempotency-key-submission)
- [22. Background-polling and startup scaling (Codeberg issue #15)](#background-polling-startup-scaling)
- [23. Cross-tenant projection read fix: owner-tag scoping on `RoleAccessMapping`](#cross-tenant-projection-read-fix-owner-tag)
- [24. Cross-tenant read fix, part two: raw events (`FetchEvents`, `QueryEvents`/`CountEvents`/`InspectEvent`, `EventSubscription`)](#cross-tenant-read-fix-raw-events)
- [25. Cross-tenant read fix, part three: `CommandQuery`/`FetchCommands`](#cross-tenant-read-fix-command-query)
- [26. Closing the admin read-back gap on `owner_tag_key`](#admin-read-back-owner-tag-key)
- [27. Cross-tenant read fix, part five: `SnapshotInspection`](#cross-tenant-read-fix-snapshot-inspection)
- [28. Cross-tenant read fix, part six: `CreateBoundedContextFromTemplate`'s always-unscoped grant](#cross-tenant-read-fix-create-bounded-context-template)
- [29. Hardening: `list_role_access_mappings` no longer panics on a concurrent bounded-context deletion](#hardening-list-role-access-mappings)
- [30. Cross-tenant write fix: owner-tag scoping on `SubmitCommand`/`TriggerCommand`/`CreateExternalEvent`/`CreateDirectEvent`](#cross-tenant-write-fix-owner-tag-scoping)
- [31. Private fields: a third field-level protection, alongside `sensitive_fields` and the owner-tag `scope` series](#private-fields-third-protection)
- [32. Closing `ProjectionQuery`'s own team gate (Codeberg issue #17)](#projection-query-team-gate)
- [33. Payload upcasting (Codeberg issue #14): every option, and the one built](#payload-upcasting)
- [34. `skilj-temporal`: a plan, partially built (long-running/cross-system processes)](#skilj-temporal-plan)
- [35. Configurable connection pool sizing](#connection-pool-sizing)
- [36. `CrossContextRoute`: crossing bounded contexts without an external system](#cross-context-route)
- [37. `idempotency_keys` gets `client_id`-scoped: a real cross-tenant collision, live since 0.0.2](#idempotency-keys-client-id-scoping)
- [38. Message-broker bridges (Kafka/Solace/etc.): investigation, not yet built](#message-broker-bridges-investigation)
- [39. Built into skilj instead: external-message dedup on `CreateExternalEvent`](#external-message-dedup-create-external-event)
- [40. `skilj-kafka`: a bridge to Kafka, both directions](#skilj-kafka-bridge)
- [41. `skilj-amqp`: a bridge to any AMQP 1.0 broker (Solace/Azure Service Bus/Artemis)](#skilj-amqp-bridge)
- [42. `skilj-nats`: a bridge to NATS JetStream](#skilj-nats-bridge)
- [43. Stopping a new subscriber from replaying all of history](#new-subscriber-replay-fix)
- [44. Correlation/causation ids on commands and events (Codeberg issue #18)](#correlation-causation-ids)
- [45. A given/when/then test fixture for decide()/project() (Codeberg issue #19)](#given-when-then-test-fixture)
- [46. A native one-shot, per-entity deadline/timer (Codeberg issue #20)](#native-deadlines)
- [47. Dead-letter/parking for failed event/message handler delivery (Codeberg issue #21)](#parked-deliveries)

---

<a id="plugin-api-decide-project"></a>
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
  ([§8](#open-for-a-future-pass) item 5)** — before calling `CommandDispatcher::dispatch` at all, so
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
`CommandType`/`Projection`/`Snapshot` (`skilj-core::plugin`), alongside the
existing `NAME`:

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
an `impl EventType for X`/`impl CommandType for X`/`impl Projection for X`/
`impl Snapshot for X` block (the last added in the [§19](#optional-snapshotting-matching-events) pass that built
`Snapshot` for real):

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
`ProjectionRegistrar`/`SnapshotRegistrar` marker types — a `skilj-core`-only
consumer can't use this attribute, the same boundary that consumer already
accepts by hand-rolling its own `CommandDispatcher`/`ProjectionDispatcher`/
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
Postgres ([§8](#open-for-a-future-pass) item 2, `skilj/tests/reconciliation.rs`). One clarification
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
  handler (still to be wired — see [§8](#open-for-a-future-pass)) becomes: resolve the token → look
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
written yet. Tracked in [§8](#open-for-a-future-pass).

---

<a id="ecosystem-choices"></a>
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

### 2.5 Payload serialization: JSON only, not pluggable, permanently

Raised while comparing SkilJ against Axon Framework and other DCB
implementations, several of which (Axon's `Serializer` SPI, XStream/
Jackson choice included) let a consuming application swap the wire/
storage format for event and command payloads. SkilJ deliberately does
not offer this, and the user confirmed this is not a "not yet" - it's a
permanent, considered rejection, not tracked as an open item anywhere.

The reason isn't inertia - a pluggable serializer would cut against
several load-bearing decisions already made elsewhere in this document:
`#[derive(Serialize, Deserialize, JsonSchema)]` ([§1.2](#12-json-schema-is-derived-from-the-rust-type-not-hand-written))
derives the wire-visible JSON Schema *from* the same Rust type that
(de)serializes the payload - a second serializer would need its own
schema-derivation story, or the two would drift. `valid_tag_mappings`/
`valid_sensitive_fields`/`private_fields` ([§2.2](#22-database-layer-sqlx), [§31](#private-fields-third-protection))
all walk a `serde_json::Value` payload by field path (dot-separated JSON
Pointer-ish keys) to find what to redact or index - a different
serialization format has no equivalent structural walk for free.
Payload upcasting ([§33](#payload-upcasting)) is implemented as an
old-JSON-to-new-JSON transform chain; a second format multiplies that by
however many formats are live at once. And every message-broker bridge
([§40](#skilj-kafka-bridge)-[§42](#skilj-nats-bridge)) already assumes
the payload on the wire is JSON, with no format negotiation.

None of this is impossible to generalize - it's that doing so would
mean re-deriving JSON Schema generation, sensitive-field/owner-tag field
walking, upcasting, and every bridge against an abstract serializer
trait instead of against `serde_json::Value` directly, for a capability
this project has never had a concrete use case for. If that changes,
it's a new design pass from scratch, not a resumption of a deferred
item.

---

<a id="crate-module-structure"></a>
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
│                       traits from [§1](#plugin-api-decide-project) - the one module every consuming
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

<a id="error-handling"></a>
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
  `skilj-core` module**, matching the domain-grouped structure in [§3](#crate-module-structure):
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

**Explicitly deferred to the GraphQL/REST wire contract pass** ([§5](#graphql-wire-contract),
unchanged scope): how `code()`/`message()` actually become a GraphQL
error's `extensions` object or a REST response's status code and JSON
body. `skilj-core` has no web dependency and doesn't produce either
shape itself — `skilj-graphql` and `skilj-rest` each own a thin
translation layer from `skilj_core::Error` (or a surface's more specific
error, where one exists) to their own wire format.

---

<a id="graphql-wire-contract"></a>
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

**Built for real, [§8](#open-for-a-future-pass) item 6.5's own pass**: `ProjectionQuery` is the
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

Builds on [§4](#error-handling)'s `code()`/`message()` trait, but only for one of the two
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
`message()`, unchanged. The REST design in [§7](#rest-wire-contract) mirrors this exact split —
that's what keeps the two wire contracts consistent with each other
rather than accidentally answering the same question two different ways.

---

<a id="idp-trust-configuration"></a>
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
The refetch is single-flight: misses that arrive while one is in flight
wait for it rather than hitting the rate limit (see §65).

**Crate: `jsonwebtoken`**, not a full OIDC-discovery crate like
`openidconnect`. Skilj only needs signature verification against a known
JWKS (`DecodingKey::from_jwk`) — it doesn't need discovery, token
exchange, or anything else OIDC bundles in.

---

<a id="rest-wire-contract"></a>
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
  # At most max_events_per_read events (default 1000, §69), oldest first: page on with
  # after=nextCursor until a call returns no events.

GET /v1/events/consume?mode=auto|manual&filter=field:op:value
  -> 200 { "events": [...] }
  # Also at most max_events_per_read per call; the next call continues from the cursor.
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

Library-level errors (the `code()`/`message()` tier from [§4](#error-handling)) map to
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

<a id="open-for-a-future-pass"></a>
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
   verify_and_extract_subject}`, `reqwest`-based, reactive-refresh per [§6](#idp-trust-configuration))
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
   async ([§8](#open-for-a-future-pass) item 6); see the Phase 4 writeup below.** `EventSubscription`
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
   naturally fit it, confirmed with the user rather than forced.
   (Superseded in §70, again with the user: `fetchCommands` now returns
   `{ id, createdAt, payload }` objects and pages via `afterCommandId`,
   still without a Relay envelope.) Two more
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

   **Phase 6 done: `EventSubscription`.** The last surface out of [§8](#open-for-a-future-pass)/[§9](#next-steps) —
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
   full workspace runs.** This closes out [§8](#open-for-a-future-pass)/[§9](#next-steps) entirely — every item
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

**Real decrypt-on-read: `render_event`/`render_command`, done - not a [§8](#open-for-a-future-pass)
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
a separate future item (see the [§9](#next-steps) bullet above for why); a missing
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

**Keyed / multi-row Projections - a real, user-driven correction to [§8](#open-for-a-future-pass)
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
The very last item from [§9](#next-steps). **First draft rejected by the user, for a
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
once [§8](#open-for-a-future-pass)/[§9](#next-steps) and every follow-up it produced was genuinely closed, with
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

<a id="next-steps"></a>
## 9. Next steps

Every item in [§8](#open-for-a-future-pass)'s original backlog, and every follow-up it led to, is
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
see each item's own writeup for the full breakdown. **[§8](#open-for-a-future-pass)/[§9](#next-steps)'s backlog,
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

<a id="dcb-alignment"></a>
## 10. Dynamic Consistency Boundary (DCB) alignment

skilj's own consistency mechanism - `Tag`s on events, a command's
`consistency_tags` deriving a `consistency_boundary` and
`matching_events` set that `decide()` is evaluated against ([§1](#plugin-api-decide-project)) - is,
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
| `Query { items: [{ types?, tags? }] }` | `consistency_tags` (derived per command/event via `TagMapping`/`derive_tags`) | skilj never builds an explicit `Query` object; the equivalent selection is computed directly from the payload's own tag mappings - narrower than the spec's `Query`, see §10.1 |
| `read(query) -> SequencedEvents` | `consistency_boundary_and_matching_events(bounded_context_events, consistency_tags) -> (Option<i64>, Vec<Event>)` (`skilj-core/src/event_store/mod.rs`) | Same "read the relevant slice before deciding" ordering DCB requires; skilj computes the boundary (highest matching sequence) in the same pass |
| `append(events, condition?)`, `AppendCondition { failIfEventsMatch, after }` | the DCB-conflict recheck inside `db::submit_command` (re-run the read under `next_sequence`'s own row lock, redispatch `decide()` on change) | Mechanistically different: DCB's own model is one declarative condition passed to a single atomic append; skilj gets the identical guarantee via a Postgres row lock plus optimistic-read-then-locked-recheck-and-retry instead of a condition object |
| the "decision model" (an app-level concept the spec deliberately leaves out of scope) | `CommandType::decide()` | Named independently in skilj's own Allium spec, not borrowed from DCB - same role the DCB examples describe |

Two other Rust event stores - Tephra and Disintegrate - already appear
in DCB's own [implementation list](https://dcb.events/resources/libraries/)
- worth knowing about as neighbors, not a comparison to relitigate here.
Whether to list skilj there too is tracked separately in
`docs/open-source-todo.md`, gated on this repository actually being
public - a link to a private repo serves no one who'd read that list.

### 10.1 The query algebra is narrower than the spec's, deliberately

Raised as Codeberg issue #24. The DCB [specification](https://dcb.events/specification/)'s
own `Query` is a list of independently-scoped items, each `{types?,
tags?}`, OR'd together (with AND between one item's own types and tags).
That lets a single query express something like "conflict if there's a
`SeatReserved` event tagged `seat:12A` *or* a `SeatBlocked` event tagged
`zone:premium`" - two alternatives with different type/tag pairings, not
sharing a type set.

skilj's actual match is flatter, and narrower than even the flat framing
above suggests. `matching_events` isn't a per-command type set at all -
it's the *entire bounded context's* own registered `EventType` universe
([§1.4](#14-matching_events-is-a-generated-per-bounded-context-enum)),
the same enum shared by every `CommandType` in that context. Within that
one fixed universe, `consistency_boundary_and_matching_events`
(`skilj-core/src/event_store/mod.rs:3584`) filters by a single OR of
`consistency_tags` - an event matches if *any* of its tags is in the set,
regardless of its own type. There is no way to pair a type restriction
with only one of several OR'd tags: every candidate event, whatever its
type, is checked against the same flat tag set.

What this gets you for free: any number of tags OR'd together, checked
against every event type the bounded context has, with `decide()` itself
narrowing by payload afterward. `skilj-demo`'s `courses.rs` is the real
example - `EnrollStudentInCourse` names both the `student` and `course`
tags in one `tag_mappings()`, and `decide()` receives the union of that
student's history and that course's history in a single call, checking
both the course-capacity and per-student-course-limit invariants
atomically with no saga ([module doc comment](../skilj-demo/src/courses.rs)).
That's the flat OR-of-tags shape doing real work.

What it can't do: the independently-typed-and-tagged-alternative case
above, where different OR branches need different type/tag pairings, not
just different tags against the same shared universe. No bounded context
built so far - in `skilj-demo` or otherwise - has needed that; every
multi-invariant case so far (`courses.rs` included) has resolved fully
within the flat model, because the invariants involved were about "does
some event carry this tag", not "does an event of type X carry this tag
while a completely different type needs a different one". Per the same
reasoning as [§2.5](#25-payload-serialization-json-only-not-pluggable-permanently)'s
pluggable-serializer rejection: this is a deliberate scope narrowing, not
an oversight, and not tracked as an open item anywhere. If a concrete
bounded context ever needs the fuller per-item algebra, that's a new
design pass against `consistency_boundary_and_matching_events` and the
tag-indexed Postgres query it mirrors ([§19](#optional-snapshotting-matching-events)),
not a resumption of a deferred item.

<a id="otel-tracing-logging-metrics"></a>
## 10b. OpenTelemetry tracing, logging, and metrics

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
  engine's own real entry points ([§1](#plugin-api-decide-project)) - get explicit `bounded_context`/
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

### 10b.1 Four smaller follow-ups

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
same libxml2/`LD_LIBRARY_PATH` workaround as always - see
[`CONTRIBUTING.md`](../CONTRIBUTING.md)), the actual compiled
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

<a id="skilj-tui-console"></a>
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
[§13](#self-describing-graphql-surface)'s `eventTypes`/`commandTypes` queries gave this crate something to
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
same `skilj-inspector` precedent as [§14](#skilj-inspector)) against a real `skilj-demo`
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

<a id="cross-instance-push-completeness"></a>
## 12. Cross-instance push completeness (Codeberg issue #2)

**A deliberate reversal, not a drift fix.** [§8](#open-for-a-future-pass)'s own `EventBroadcaster`
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
  deliberate stub since Phase 6 ([§9](#next-steps)'s writeup): "a projection registered
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

<a id="self-describing-graphql-surface"></a>
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

<a id="skilj-inspector"></a>
## 14. `skilj-inspector` - a standalone read-only Postgres console (Codeberg issue #6, "5b")

Even with [§13](#self-describing-graphql-surface)'s fix, everything about a running `skilj` deployment still
requires `skilj-graphql` itself to be up - the exact moment an operator
most wants to look (the app is down, Postgres isn't) has no tool at all.
`skilj-inspector` is a second Ratatui console, built the opposite way
from `skilj-tui` on purpose: where that crate is deliberately a pure
GraphQL client with zero dependency on any other skilj crate ([§11](#skilj-tui-console)),
`skilj-inspector` depends on `skilj-core` directly, since raw Postgres
access *is* the whole point - there's no `Role`/`RoleAccessMapping`
layer to authenticate against when nothing is serving GraphQL. One
required arg, `--database-url`/`DATABASE_URL`, no IdP config, no token.

**Read-only by construction, not just convention** - stated as an
explicit doc comment on the crate root: every function in its `data`
module calls only existing `skilj_core::db` read functions
(`list_bounded_contexts`, the two new `list_*_types_for_bounded_context`
from [§13](#self-describing-graphql-surface), `list_projections_for_bounded_context`,
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

<a id="skilj-tui-debugging-enhancements"></a>
## 15. `skilj-tui` debugging enhancements (Codeberg issue #7)

Four independently shippable additions to [§11](#skilj-tui-console)'s console, picked up right
after [§13](#self-describing-graphql-surface) landed (one of them was blocked on it). **One correction to
the issue's own framing, found while implementing, not assumed**: it
describes all four as "no architectural change" - true for three of
them, but the DCB conflict visualizer needed a real, small three-crate
change (`skilj-core`/`skilj-graphql`/the spec), not just a `skilj-tui`
client change - see its own write-up below for why.

**DCB conflict visualizer.** `command_submission.rs`'s resolver already
computed `matching_events` (`event_store::consistency_boundary_and_matching_events`)
to make the dispatch decision, but discarded it -
`SubmitCommandOutcome::Rejected` carried only `{ reason, kind }`.
`submit_command` (`skilj-core/src/db/mod.rs`) gained one new parameter,
`matching_events: &[Event]` - both real call sites already computed this
value immediately before calling it, so this is threading an
already-computed value through, not new computation. Tracked internally
as `final_matching_events`, overwritten by the freshly-recomputed set
inside the function's own DCB-conflict redispatch branch, so a rejection
born from a conflict-triggered redispatch reports the *final* matching
set that actually governed it, never the caller's now-stale pre-lock
one - covered by extending the existing
`submit_command_redispatches_and_rejects_on_a_genuine_dcb_conflict` test
(`skilj-core/tests/submit_command.rs`) with exactly that assertion.
`skilj-graphql` gained a new `MatchingEvent` object
(`sequence`/`eventTypeName`/`payload` - deliberately narrower than
`Event` itself, enough to debug a conflict without inventing a bigger
wire type) and `SubmitCommandResult.matchingEvents: Option<Vec<Event>>` -
`Some` only on rejection, mirroring `rejectionReason`/`rejectionKind`'s
own existing "only populated on the relevant outcome" convention.
`skilj-rest`'s own response shape stays unchanged - out of scope per
§7.1, the new parameter just gets computed and dropped there.
`specs/skilj.allium`'s `CommandSubmission` surface gained a `@guidance`
paragraph (not a fabricated `exposes:` entry - `matching_events` is a
rule-local computed value, not a stored entity field, so `@guidance` is
the honest construct here) recording that a rejection now surfaces the
same `matching_events` `rule ProcessCommand` already defines, scoped to
rejection only, GraphQL-only. Verified against a real running
`skilj-demo` server, not just the unit-level assertion: deposited into
account "a1", then attempted an overdraft withdrawal - the real
rejection's `matchingEvents` showed the exact `MoneyDeposited` event
`decide()` actually saw.

**Time-travel projection viewer.** `projection_query::fetch` gained
`wait_for_sequence: Option<i64>`, threaded into both its own query
strings as `waitForSequence: $wait` - `ProjectionQuery`'s own field
already accepted this server-side, so this is wiring an existing
capability into the client, genuinely no server change.
`ProjectionsTab`/`ProjectionField` gained a third, Tab-cycled field,
parsed as `i64` at submit time (empty = no wait, an unparseable value
blocks submission with an inline error). **Naming honesty, not the
issue's own words verbatim**: the field's own UI label reads "at least
as fresh as sequence N", not "as of sequence N" -
`waitForSequence` is a freshness guarantee (don't answer before the
projection has caught up to at least this sequence), not a historical
snapshot; a projection that has already moved past it returns *current*
state, not state frozen at that instant.

**Search/filter on the Live Events feed.** A `/`-to-enter-filter-mode
convention (matching `less`/`vim`'s own well-known search key), not
always-on free text - Live Events was the one tab where every key fell
through unhandled before this, and always-on entry would have reopened
the exact digit-tab-switch conflict this session's own issue #8 pass
already found and fixed for Commands (a filter like "42" would jump to
Query Events mid-type). `/` sets `live_events_filter_active`; while
active, typed characters edit `live_events_filter` and `Enter`/`Esc`
both exit back to normal browsing (and normal digit-based tab
switching). Filtering itself is a pure view concern (`ui::draw_live_events`
substring-matches, case-insensitive, against each event's own compact
JSON rendering) - no server round trip, `App` keeps every event
regardless of what's currently filtered.

**Syntax-highlighted JSON rendering.** New `skilj-tui/src/json_style.rs` -
walks a `serde_json::Value` and produces styled `ratatui::text::Line`s
(distinct colours for object keys, string/number/bool/null values, and
punctuation) instead of the previous plain
`serde_json::to_string_pretty`/`.to_string()`, replacing every one of
`ui.rs`'s old `pretty`/`pretty_compact` call sites. Leaf tokens
(strings/numbers/bools/null) are rendered via `serde_json::to_string` on
that one value - reusing `serde_json`'s own already-correct escaping
rather than hand-rolling a string-escaper, exactly the kind of thing
that quietly mis-renders a payload containing a `"` or `\n`. Pure and
unit-tested (token → style mapping), the same discipline `form.rs`'s own
schema classifier already holds itself to.

**A real, pre-existing bug this pass's own tests caught, not shipped**:
the first draft suppressed digit-based tab-switching for the *entire*
Projections tab (needed once `waitForSequence` made every field there
inherently digit-capable) with no way back at all - every one of
Projections' three fields is always focused, unlike Commands' own
Picking/Form split, so there was no picker sub-state with no free text
to fall back to. A `slash_enters_filter_mode...` test's own sibling,
written to check the *equivalent* case for Projections, caught the trap
before it shipped. Fixed by giving `Esc` a real job on that tab: leave
it entirely, back to Live Events - the same "Esc backs out of whatever's
intercepting keys" role it already plays for Commands' form and Live
Events' filter, just with nowhere shallower to back out *to* on this
particular tab.

Verified: `skilj-core/tests/submit_command.rs`'s extended assertion,
`skilj-tui/src/json_style.rs`'s and `skilj-tui/src/form.rs`'s own unit
tests, `skilj-tui/tests/debugging_enhancements.rs`'s `App`-level tests
(filter mode, the extended digit fix, `waitForSequence` parsing),
`skilj-tui/tests/projection_query.rs`'s extended mock-server coverage of
the `wait` variable, `allium check` clean after the spec edit, and a
real interactive `tmux` pty run against a live `skilj-demo` server
(same no-TTY-in-this-sandbox precedent as [§14](#skilj-inspector)) - a genuine DCB conflict
triggered and its real matching event shown, the Live Events filter
shown hiding/showing events live, `waitForSequence: 0` shown returning
current state rather than hanging, and real ANSI colour codes confirmed
in the captured pane output (not just present in the test suite).

<a id="declarative-bounded-context-codegen-prototype"></a>
## 16. Declarative bounded-context format + codegen: a prototype, not a build (Codeberg issue #5)

Issue #5 proposes a small YAML/TOML format describing an event/command/
projection's *shape* (fields, DCB tags, sensitive fields), plus a
codegen step turning it into the Rust structs/trait impls/`#[auto_register]`
wiring - leaving only `decide()`/`project()` bodies hand-written. The
issue names itself "the biggest, riskiest proposal here" and explicitly
asks for prototyping against `skilj-demo`'s own two bounded contexts
before committing, rather than building the real thing on spec. This
section is that prototype's findings, not the feature - **no codegen
tool, no new crate, no `build.rs` integration exists after this pass**.
The translation from real Rust to the draft format happened entirely by
hand, so what follows is what that translation actually revealed, not
what a generator was assumed capable of.

**Two facts settled before prototyping, neither in the issue's own
text.** First: the issue asks readers to weigh this against its own
cheaper sibling proposal, a `cargo generate`-once scaffolding template -
that already shipped, as `templates/skilj-template/` (Codeberg issue
#10, closed before this pass). The issue's own "which one first"
question already has its first half answered; what was actually open
was just "is the expensive, continuously-regenerating option still
wanted." Second: this session's own #3 (`skilj` skill)/#4
(`skilj-event-modeling` skill) already lower the same friction this
issue targets, from cheaper angles (how to write it correctly, what
shape it should be) that don't add a second format to maintain forever.

### Method

Hand-translated `skilj-demo/src/banking.rs` and `skilj-demo/src/courses.rs`
into a draft YAML shape - the full translated files live in this
session's own scratchpad, not committed (throwaway working material, the
same treatment this session's other prototyping scaffolding got). One
event type, to show the shape:

```yaml
event_types:
  - name: MoneyDeposited
    fields:
      account_id: string
      amount: i64
    tags:
      account: account_id
```

For each file, every construct was classified as **eliminable** (the
generator could produce it entirely from the declarative shape) or
**stays** (real `decide()`/`project()` logic, or a domain constant/helper
function nothing about the format could express), counted from the real
file's own line ranges - blank lines and section comments are
apportioned by judgement, not mechanical counting, so treat the
percentages below as good approximations, not exact.

### Finding 1: the event enum + `BoundedContextEvent` impl is 100% mechanical

`BankingEvent`/`CoursesEvent` and their `try_from_event` match arms
(`courses.rs:119-142`, 24 lines including its own doc comment) are
entirely derivable from the event
type list alone - one variant, one match arm, per registered
`EventType`, with zero hand-judgement involved. This is real,
uncontested boilerplate, and it scales linearly with event-type count -
the strongest, least ambiguous case *for* codegen found in this pass.

### Finding 2: the more valuable the bounded context, the less codegen buys you

|  | Total lines | Eliminable (shape) | Stays (real logic/consts/docs) | Structural (blank/comments) |
|--|--|--|--|--|
| `banking.rs` | 228 | ~115 (~50%) | ~80 (~35%) | ~33 (~15%) |
| `courses.rs` | 437 | ~177 (~40%) | ~205 (~47%) | ~55 (~13%) |

`banking.rs` is close to half boilerplate. `courses.rs` - the bounded
context whose own module doc comment calls its two-invariant,
dual-tagged `EnrollStudentInCourse::decide()` (52 lines on its own,
`courses.rs:266-317`) "the whole point of this demo" - eliminates a
similar *absolute* line count (wrapper/struct/enum scaffolding scales
with type count regardless of complexity) but a smaller *proportion*,
because the interesting part of that file is exactly the part no format
could ever generate. The more a bounded context is worth building in
the first place, the less this feature helps with it, proportionally -
worth weighing against the issue's own implicit framing ("every event
type... is pure boilerplate").

### Finding 3: `Projection::keys()` needs a real escape hatch, not just a field name

`CourseRoster` keys all three of its consumed event types by `course_id`
uniformly - a bare `keyed_by: course_id` covers it. `StudentSchedule`'s
own `keys()` (`courses.rs:423-429`) does not: it keys `StudentEnrolled`/
`StudentUnenrolled` by `student_id` but must map `CourseOpened` (an
event type it's still required to list in `consumed_event_types`, for
`caught_up_to` accounting) to `vec![]` - that event type never carries a
student at all. A workable format needs `keyed_by` to accept either a
bare field name (the common case) or a per-event-type map (the
`StudentSchedule` case) - solvable, but real design work, and once
written out, the map form isn't meaningfully shorter than the 8-line
Rust `match` it replaces. This is the one place this pass recommends
deferring rather than solving now (see Recommendation).

### Finding 4: a real correctness win, not just less typing

Not anticipated going in. Today, `tag_mappings()`/`sensitive_fields()`
name a payload field as a **plain string** (`field: "account_id".into()`)
- nothing checks it against the actual struct's real field names at
compile time. A typo (`"acount_id"`) compiles cleanly and is only caught
at registration time, against a live database
(`InvalidTagMapping`/`InvalidSensitiveField` - see the `skilj` skill's
own `references/common-mistakes.md`). A generator deriving both the
payload struct *and* the tag reference from the same declarative source
can guarantee the field exists, and (since it already knows every
field's declared type from the same YAML) can check the "must resolve to
a scalar leaf" rule too - at generation/build time, not just at
registration time against a database. This is a genuine improvement
over what hand-written code has today, not merely a convenience.

### What this prototype didn't exercise

Neither `banking.rs` nor `courses.rs` uses `sensitive_fields`,
scheduling (`system_triggered_allowed`/`schedule`/`missed_occurrence_policy`),
or `#[requires_role]` - so this pass's own translation never stress-tested
those parts of the format the issue's scope still calls for. A real
build would need to design and prototype those separately, not assume
the pattern found here extends cleanly.

### Recommendation

Not a flat yes/no - the evidence supports a narrower first cut than the
issue's own full proposal, not the full bet. **Event/command type
generation** (structs, trait-impl wrapper methods, the event enum +
`BoundedContextEvent` impl) is the unambiguous, 100%-mechanical win
(Findings 1 and 4) and worth building for real. **Projection generation**
should wait - `keyed_by`'s real design complexity (Finding 3) isn't
resolved, and projections are a smaller share of most bounded contexts
than event/command types are, so the win-to-design-cost ratio is worse
there. Either way, this is a real new maintenance surface - a format
whose own compatibility story needs the same rigor
`SchemaEvolutionStaysCompatible` already gives hand-written schemas
(the issue's own explicit worry), and a codegen tool that has to track
`skilj-core`'s plugin API's own evolution in lockstep (this project's
own `#[auto_register]`/`auto_register` shorthand passes, 2026-08-25,
are real, recent examples of that API moving) - not a cost to wave away
against a boilerplate reduction that, per Finding 2, is smaller than
the issue's own framing suggests for exactly the bounded contexts most
worth building. The final call - build the narrower first cut, or close
#5 as adequately superseded by #10 (already shipped) plus #3/#4
(shipped this session) - is the project owner's, informed by these
numbers rather than the issue's own upfront guess.

<a id="event-command-codegen-real"></a>
## 17. Event/command codegen, for real: the narrower cut (Codeberg issue #5)

[§16](#declarative-bounded-context-codegen-prototype)'s recommendation was the narrower cut - event/command type
generation only, `Projection` generation deferred (its Finding 3,
`keyed_by`'s per-event-type map case, stays unresolved). The project
owner chose it. This section documents the actual build, not another
prototype: a real crate, wired into a real consumer, verified against
the real Postgres-backed test suite `banking.rs` already had.

**`skilj-codegen`** (new crate) is a plain library, not a proc-macro -
it runs from a consumer's own `build.rs`, at `cargo build` time, not at
`rustc`'s macro-expansion time, so generated code can never drift out of
sync with the `.skilj.toml` it came from (the issue's own preferred
default: no separate "did you remember to regenerate" step). Its entire
public surface is one function:

```rust
pub fn generate(toml_source: &str) -> Result<String, Error>
```

It parses `toml_source` into a small internal spec
(`BoundedContextSpec { bounded_context, event_types, command_types }`,
each `EventTypeSpec`/`CommandTypeSpec` carrying `fields: Vec<FieldSpec>`
- a `Vec`, not a map, specifically to preserve declared field order,
which a TOML map alone doesn't guarantee - `tags: BTreeMap<String,
String>`, and, for commands, `rest_trigger_allowed: bool`), builds a
real `proc_macro2::TokenStream` with `quote!` (the same
`syn`/`quote`/`proc-macro2` stack `skilj-macros` already proved out in
this codebase, reused here for build-time codegen instead of compile-time
macro expansion), and pretty-prints it with `prettyplease` (new
dependency) so the generated file is genuinely readable, not a minified
one-liner.

**Scoped to exactly what a real conversion needs, nothing wider.**
Matching this project's own repeated "don't build ahead of what's
wired" discipline, the format covers `fields`, `tags`, and
`rest_trigger_allowed` - not the plugin API's full trait surface.
`sensitive_fields`, the creation-origin flags
(`external_creation_allowed`/`direct_creation_allowed`/
`event_read_allowed`), scheduling, and `#[requires_role]` are real,
legitimate parts of that API but are **deliberately deferred**, named
here rather than silently missing: `banking.rs` never exercised any of
them (confirmed by direct reading during [§16](#declarative-bounded-context-codegen-prototype)'s own prototype pass), so
building generator support for them now would be speculative,
untested-by-anything-real surface area - the exact thing this project's
own convention avoids. A `FieldType` closed enum
(`string`/`i64`/`bool`) covers the scalar leaf shapes the plugin API's
schema rules already require, not a general type system.

**One deliberate deviation from [§16](#declarative-bounded-context-codegen-prototype)'s own draft format**: TOML, not
YAML. `serde_yaml` - the natural choice for the prototype's own
illustrative YAML sketch - was archived by its own maintainer in 2024,
not a dependency to newly adopt for real, ongoing code. `toml` is
actively maintained and expresses this exact shape (arrays of tables)
just as cleanly.

**What `generate()` emits**, per bounded context: `pub const
BOUNDED_CONTEXT: &str = "...";` at the top (so nothing about wiring
`#[auto_register(BOUNDED_CONTEXT)]` needs a separately hand-written
const either); one `#[derive(Debug, Clone, Serialize, Deserialize,
JsonSchema)] pub struct XPayload { ... }` per event/command type, fields
in declared order; one unit struct + `#[auto_register(BOUNDED_CONTEXT)]
impl EventType for X` per event type (`type Payload`, `const NAME`,
`tag_mappings()` - omitted entirely when a type declares no tags, rather
than emitted empty); one unit struct + `impl CommandType for X` per
command type (`type Payload`, `type Event`, `const NAME`,
`tag_mappings()`, `rest_trigger_allowed()` when set), with `decide()`
generated as a one-line delegation to a hand-written free function the
including module is expected to already provide - naming convention
`decide_<snake_case(NAME)>`, e.g. `decide_deposit_money`. A missing one
is a real, immediate compile error (an unresolved name) in the
including crate, not a silent gap; and the shared per-bounded-context
event enum (`BankingEvent`-shaped) plus its own `impl
BoundedContextEvent for ... { fn try_from_event(...) }`, one
variant/match-arm per event type - [§16](#declarative-bounded-context-codegen-prototype)'s own Finding 1, the cleanest,
least-arguable win, and Finding 4's correctness win falls out of the
same mechanism for free: the tag reference and the payload struct are
now derived from the same declarative source, so a typo'd field name is
a build-time error instead of a registration-time one against a live
database.

**Consumer wiring, proven against the real thing, not a synthetic
fixture.** `skilj-demo/src/banking.skilj.toml` holds the declarative
shape for `MoneyDeposited`/`MoneyWithdrawn`/`DepositMoney`/
`WithdrawMoney`. `skilj-demo/build.rs` reads it, calls
`skilj_codegen::generate`, and writes the result to
`$OUT_DIR/banking_generated.rs`. `skilj-demo/src/banking.rs` itself now
opens with `include!(concat!(env!("OUT_DIR"), "/banking_generated.rs"));`
and keeps only what stays genuinely hand-written: `decide_deposit_money`/
`decide_withdraw_money` (today's `decide()` bodies, lifted to free
functions) and `balance_of()` (the shared helper). `AccountBalance` (the
projection) and its own `keys()`/`project()` are untouched - projection
generation is out of scope for this pass, not broken by it.

`courses.rs` stays fully hand-written, deliberately, with no
`.skilj.toml` counterpart. Its own point -
`EnrollStudentInCourse`'s dual-invariant `decide()` - is real logic no
declarative format generates, and its two projections
(`CourseRoster`/`StudentSchedule`) are exactly the deferred
`keyed_by`-map case from [§16](#declarative-bounded-context-codegen-prototype)'s Finding 3. Converting it would prove
nothing this pass doesn't already prove via `banking.rs`.

**Verification.** `skilj-codegen` has its own test suite: 2 unit tests
for the PascalCase/snake_case helpers `decide_<name>` naming needs, and
7 integration tests asserting the generated, `prettyplease`-formatted
source contains the expected constructs (field order preserved, real
`tag_mappings()` values, a type with no tags gets no `tag_mappings()`
override at all, the event enum's own match arms, and a malformed TOML
file is a real `Error`, not a panic). The real regression proof is that
`skilj-demo`'s and `skilj`'s existing test suites - `banking.rs`'s own
three tests plus every other real-Postgres integration test in both
crates - were run completely **unchanged** against the newly-codegen'd
`banking.rs`, and all passed: the generated code is behaviourally
identical to the hand-written code it replaced, not merely
"compiles." `cargo build/clippy/test --workspace` is clean (the one
pre-existing `skilj-core` clippy warning at the time of this pass,
`db/mod.rs`'s `explicit_auto_deref`, belongs to separate, already-in-
flight work and is untouched by this one). `$OUT_DIR/banking_generated.rs`
was manually inspected after a real build and reads as genuinely clean,
idiomatic Rust - a maintainer debugging generated code would not be lost
in it.

<a id="ultra-review-fixes"></a>
## 18. Ultra-review fixes: an access-control leak, a duplicate-delivery bug, and two nits

A `/code-review ultra` cloud review of this session's own recent work
(`1bcfc70`..`HEAD` - roughly everything from `skilj-tui` through [§17](#event-command-codegen-real)'s
`skilj-codegen`, chosen to fit the tool's diff-size cap) surfaced four
findings, all fixed the same pass. Two were real, not nits.

**`matchingEvents` leaked event payloads past `Admin` gating.**
`CommandSubmission` ([§7](#rest-wire-contract)'s DCB conflict visualizer) faces `WriteAccess`
for submission itself, but a rejection's `matchingEvents` is full raw
event content - the same visibility `EventQuery`'s `queryEvents`/
`countEvents`/`inspectEvent` require `Admin` for. The resolver populated
it for any Write-level caller's rejection, meaning a Write-only caller
could construct a command whose derived tags scope any account/entity
of interest, force a rejection on purpose, and read that entity's whole
matching-event history back through this field - a read side channel
around the Admin-only query surfaces. Fixed in
`skilj-graphql/src/resolvers/command_submission.rs`: `matching_events`
is now `Some(...)` only when `access_mapping.level == Admin`, `None`
otherwise (a Write-level rejection still carries `rejection_reason`/
`rejection_kind` as before). `specs/skilj.allium`'s `CommandSubmission`
surface gained `@guarantee MatchingEventsRequiresAdminLevel` recording
this (via `allium:tend`, `allium check` clean). A new test,
`matching_events_is_only_returned_to_an_admin_level_caller` in
`skilj/tests/graphql_business_surfaces.rs`, submits the identical
rejecting command as both an Admin- and a Write-level caller against
real Postgres and asserts the field is present only for the former.

**Cross-instance `NOTIFY` double-delivered every locally-committed
event.** [§12](#cross-instance-push-completeness)'s write path both calls `EventBroadcaster::publish`/
`RevocationBroadcaster::publish` directly *and* `NOTIFY`s Postgres. But
Postgres delivers a `NOTIFY` to every listening backend, including ones
opened by the same process that sent it - so an instance's own
`cross_instance::Listener` received its own self-`NOTIFY`, refetched
the event, and republished it into the *same* local broadcaster the
direct `publish` call had already fed. Every `allEvents`/`eventsByType`
subscriber on the submitting instance saw every event twice; a
revocation double-fired the same way. Fixed with a per-broadcaster
random `instance_id` (`EventBroadcaster`/`RevocationBroadcaster` in
`skilj-core/src/event_store/mod.rs`/`access_control/mod.rs`, generated
once at construction via `shared::generate_token_id()`, exposed via
`instance_id()`): `db::notify_event_appended`/`db::notify_revocation`
now stamp their `NOTIFY` payload with the publishing broadcaster's own
id, `cross_instance::Message::EventAppended`/`Message::Revoked` carry it
as `origin_instance_id` (`#[serde(default)]` so an older instance's
payload during a rolling deploy still parses rather than being dropped
as malformed), and `skilj`'s dispatch loop skips republishing when
`origin_instance_id` matches its own broadcaster's `instance_id()` -
this instance's own write already delivered locally, once.
`Message::RegistrationChanged` deliberately gets no such treatment: it's
the *only* path that ever rebuilds the GraphQL schema, including for a
locally-originated registration change, so a same-instance echo of it
must always be acted on. Proven with a new single-instance test,
`same_instance_delivery_is_exactly_once_not_duplicated` in
`skilj/tests/cross_instance.rs` (every other test in that file is
deliberately two-instance - see its own module doc comment) - verified
to actually fail without the fix by temporarily short-circuiting the
dedup check and re-running it, then restoring the fix and confirming
green again.

**Two nits, `skilj-codegen`-scoped**: none of its spec structs carried
`#[serde(deny_unknown_fields)]`, so a deferred field name (`sensitive_fields`,
say) or a typo (`taggs`) in a `.skilj.toml` was silently ignored rather
than a build error - contradicting the crate's own "deliberately
deferred, not silently missing" doc comment on the input side. Fixed
with `#[serde(deny_unknown_fields)]` on all four spec structs in
`skilj-codegen/src/spec.rs`, plus two new regression tests in
`skilj-codegen/tests/generate.rs`. And `skilj-demo/src/banking.rs` still
declared a hand-written `BOUNDED_CONTEXT_NAME` const left over from
before [§17](#event-command-codegen-real)'s codegen refactor - dead, unreferenced anywhere, sitting two
lines above the real generated `BOUNDED_CONTEXT` every caller actually
uses. Deleted.

Verification for all four: `cargo build/clippy/test --workspace` clean
(the one pre-existing `skilj-core` `explicit_auto_deref` warning noted
in [§17](#event-command-codegen-real) is unrelated and untouched), `allium check` clean on the spec
change.

<a id="optional-snapshotting-matching-events"></a>
## 19. Optional snapshotting for `matching_events`: a discussion, not a build

User-initiated investigation, not tied to a Codeberg issue: for a
bounded context with a lot of history, a command's `matching_events`
(the tag-scoped union of prior events `decide()` folds) can be large and
slow to assemble, especially past what `EventCache` can serve. This
section records the investigation's findings and the design it
converged on. **No code changes accompany this section** - it's a
decision record, the same treatment [§16](#declarative-bounded-context-codegen-prototype) gave issue #5's prototype phase
before a build was chosen.

### What actually happens today (the real bottleneck)

`matching_events` for *every* command submission is assembled by
`list_events_for_bounded_context_cached(pool, cache, bc, -1)` →
`consistency_boundary_and_matching_events()`, which:

1. Fetches every event ever recorded for the *whole bounded context* -
   `SELECT * FROM {schema}.events ORDER BY sequence`, no `WHERE` clause
   at all (`db::list_events_for_bounded_context`).
2. Filters that entire set *in Rust, in memory* for tag matches
   (`consistency_boundary_and_matching_events`, `event_store/mod.rs`).

`EventCache` (default capacity 1000, per [§8](#open-for-a-future-pass)'s own drift-audit-closure
history) is a single bounded-context-wide recent-events window, not
scoped per tag/entity. Its `try_events_after(..., -1)` ("give me all
history", exactly what the read above asks for) can only be served from
cache if the window still covers back to the bounded context's very
first event - i.e. only until total events exceed `capacity`. Past that
point, *every* command submission, forever, is a guaranteed cache miss
that triggers the full unfiltered table scan, regardless of how
selective the command's own tags are. This is a harder wall than "some
old events aren't cached" - once a bounded context outgrows `capacity`,
nothing about `matching_events` is ever served from cache again.

Two distinct performance problems fall out of this, calling for
different fixes:

- **Problem 1**: fetching-and-filtering *the whole bounded context* to
  answer a *tag-scoped* question. Fixable without touching `decide()`
  at all.
- **Problem 2**: even correctly tag-scoped, *one entity's own* history
  can genuinely be large (an account with tens of thousands of
  transactions). This is what snapshotting targets, and no amount of
  indexing fixes it.

### Fix for Problem 1: a DB-side tag-scoped fetch (agreed, independent of snapshotting)

`events.tags` is already `JSONB`. A GIN index plus containment queries
(`tags @> '[{"key":"account","value":"x"}]'`, OR'd across a command's
derived tags) fixes Problem 1 for the common case with no `CommandType`
API change, no new storage, no new endpoint, and none of snapshotting's
correctness surface. Lower-risk than snapshotting and worth doing
regardless of whether snapshotting is ever built - snapshotting without
it would still pay an unnecessarily expensive "events since the
snapshot" fetch on every read.

### Why `Projection` reuse was considered and rejected

The initial design explored reusing `Projection` as the snapshot
mechanism - the shapes rhyme closely (`project(state, event, key)` vs. a
fold; `keys()` vs. tag-value scoping; `caught_up_to` vs. an as-of
marker; `ProjectionRebuild`'s restage vs. "the model changed, get all
the events again"). Rejected: `Projection`s are read-model
infrastructure - eventually-consistent by default, rebuildable and
restageable via `ProjectionRebuild`, exposed to Admin-level GraphQL
callers - and nothing in that trait's contract carries any obligation
about the timing or provable-correctness bar a DCB consistency check
needs. Letting `decide()`'s own inputs depend on that would mean an
operator rebuilding or restaging a projection for ordinary read-model
reasons could silently corrupt what `decide()` sees - a correctness
regression with no compiler or test surface to catch it. Snapshotting
needs its own, separate concept, even though the shape looks similar.

### The design: `Snapshot` as its own first-class concept

A new trait, not `Projection`:

```rust
trait Snapshot {
    type State: Serialize + DeserializeOwned + JsonSchema + Default;
    type Event: BoundedContextEvent;
    const NAME: &'static str;
    const BOUNDED_CONTEXT: &'static str = DEFAULT_BOUNDED_CONTEXT;
    /// Single tag key only, deliberately - see "multi-tag commands" below.
    const TAG_KEY: &'static str;
    /// Bumped by hand whenever fold()'s logic or State's shape changes -
    /// see "model changed" below. Not inferred: Rust can't detect a
    /// fold's own semantic change, only a decider declaring one can.
    const VERSION: u32;
    fn fold(state: &mut Self::State, event: &Self::Event);
}
```

A `CommandType` opts in via something like `fn snapshot() -> Option<&'static str>`
naming the `Snapshot::NAME` to consult. **Scoped per `(tag_key)`, not
per `CommandType`**: in `banking.rs`, both `DepositMoney` and
`WithdrawMoney` tag on `account` and both need `balance_of()` - scoping
per-`CommandType` would give them two independently-computed snapshot
streams for the same entity, free to drift apart. One snapshot
definition per tag key, shared by every command that tags on it.

**The `decide()` signature question, stated plainly**: an I/O-only
optimization (fetch a cached `Vec<Event>` faster) still makes `decide()`
walk every old event's business logic on every call - it doesn't save
the compute Problem 2 is actually about. Getting that saving requires
`decide()` itself to take the folded state instead of replaying through
it, which is a real, explicit, opt-in extension to `CommandType`, not
something hidden under the existing signature:

```rust
fn decide_from_snapshot(
    payload: &Self::Payload,
    snapshot: &SnapshotState,      // Default::default() if none exists yet
    events_since_snapshot: &[Self::Event],
) -> CommandDecision
```

The framework only calls this for a `CommandType` that opts in; every
other `CommandType` keeps working exactly as today, unchanged - the
"optional" the original ask was for.

**Storage** - its own table, not `projections`:

```sql
CREATE TABLE {schema}.snapshots (
    snapshot_name TEXT NOT NULL,
    snapshot_version BIGINT NOT NULL,
    tag_key TEXT NOT NULL,
    tag_value TEXT NOT NULL,
    as_of_sequence BIGINT NOT NULL,
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (snapshot_name, tag_key, tag_value)
);
```

**"Model changed"** - no shared `ProjectionRebuild` machinery; its own,
simpler rule instead: a stored row whose `snapshot_version` doesn't
match the currently-registered `Snapshot::VERSION` is treated as if it
doesn't exist - fall back to a full (or tag-indexed) replay for that one
tag value. The same "coverage miss, not a wrong answer" philosophy
`EventCache`'s own module doc comment already states, applied to a
different subsystem rather than shared with it. Self-healing: the next
write past that point can lay down a fresh row at the new version.

**Write cadence** - a `SNAPSHOT_EVERY: u32` cadence (store a state after
every N events, not every one), written by its own small background
task that polls each stale snapshot row and folds forward - deliberately
not the `Projection` dispatcher's task, even though the shape rhymes,
for the separation reason above. Async, never in the hot commit path:
correctness only needs "never trust a snapshot ahead of what's
committed," and events after `as_of_sequence` are always still fetched
fresh, under `next_sequence`'s lock, exactly as `matching_events` already
is today regardless of snapshot freshness.

**Multi-tag commands** (`courses.rs`'s `EnrollStudentInCourse`, which
unions one student's *and* one course's history in a single `decide()`
call - the DCB case this project's own courses example exists to prove
out): out of scope for a first cut, deliberately. `TAG_KEY` is singular
by design, so a command unioning two tags gets no snapshot benefit until
a later pass composes multiple single-tag snapshots (feasible in
principle - dedup the union by `sequence` - but real, unbuilt design
work). Matches this project's own repeated "narrower cut" discipline
([§16](#declarative-bounded-context-codegen-prototype)/[§17](#event-command-codegen-real)'s own precedent).

**Inspection endpoint** - new and separate, Admin-gated the same way
`EventQuery` is (a snapshot's `state` is derived business data, the same
sensitivity class as raw event content), not folded into
`ProjectionQuery`. A real new GraphQL/REST surface, a new spec entity,
plausibly a `skilj-inspector` read-only addition too, per that crate's
own "second console for when the wire protocol isn't the point" niche.

### Verified non-issues (so they aren't re-litigated later)

- **Encryption/subject-erasure**: `decide()` already only ever sees
  whatever is in the `payload` column as stored, and sensitive-field
  encryption happens *after* `decide()` returns
  (`submit_command`'s `resolve_encryption_keys` step runs post-decision).
  `decide()` - and so any fold derived from the identical data - is
  already ciphertext-blind for sensitive fields. A snapshot built this
  way can't newly leak plaintext or newly evade crypto-shredding; it's
  no more exposed than the `events` table already is.
- **DCB freshness under the lock**: correctness only requires that
  events *after* a snapshot's `as_of_sequence` are always fetched fresh,
  under `next_sequence`'s lock, exactly as `matching_events` already is
  today. The snapshot only ever replaces the prefix, never the
  freshness-critical tail.

### Upsides

- Removes the Problem 1 wall for high-volume bounded contexts (though
  the tag-indexed fetch alone already covers this for single-tag
  commands - see above).
- Addresses genuinely large per-entity histories (Problem 2) that no
  amount of indexing fixes.
- Kept structurally separate from `Projection`, per the correction
  above - no shared code path a read-model change could accidentally
  destabilise.
- Genuinely optional per `CommandType` - a decider that never opts in is
  completely unaffected.

### Downsides / risks

- **Multi-tag commands benefit least** - exactly the DCB cases that
  differentiate this project from classic event sourcing get partial
  coverage at best, full-fetch fallback for any un-snapshotted tag,
  until a later composition pass.
- **New correctness-critical surface**: a decider's `fold()` must be
  provably equivalent to replaying the real events for its tag scope. A
  bug here is silent logic corruption in `decide()` - wrong business
  decisions - not a rejected write. This needs the same rigor the
  crypto/erasure work already got, not a convenience-feature bar.
- **Write amplification**: a snapshot write on some cadence, per hot tag
  value - a new background task, new failure modes to reason about
  (falling behind, a version bump landing mid-catch-up), not a detail.
- **Multi-instance consistency**: unlike `EventCache` (in-memory,
  self-healing via `freshen()`), a snapshot is Postgres-backed and every
  instance must agree on "the latest trustworthy snapshot" - the same
  bar cross-instance push ([§12](#cross-instance-push-completeness)) and the scheduler's own
  `ScheduleStateIsShared` already had to clear.
- **Real, non-optional cost even for non-adopters**: a schema migration,
  a new registration surface, a new endpoint, and spec work land on the
  whole project regardless of adoption.
- **Anticipatory, not yet observed**: neither `banking` nor `courses`
  currently has volume proving either fix is needed today - this is
  designed ahead of demonstrated pain, a deliberate exception to this
  project's usual "don't build ahead of what's wired" convention,
  justified only because the user's own production experience is what's
  motivating it, not speculation from inside this codebase.

### Where this stands

**Both are built, for real.** Problem 1 (the tag-indexed fetch) first;
`Snapshot` (Problem 2) afterward, once the user asked for "a solution
for when there are a lot of events needed to determine the
consistency" - see "Snapshot, built for real" below for that pass.

`db::list_events_for_bounded_context_matching_tags` (`skilj-core/src/db/mod.rs`)
does exactly what's designed above - a `tags @> $N::jsonb` containment
clause per wanted tag, `OR`'d, optionally `AND sequence > $N` - backed
by a new `events_by_tags` GIN index added to `provision_bounded_context_schema`
(applies to every bounded context created from here on; an already-
provisioned one needs the index backfilled by hand, exactly the caveat
this section originally called out). `EventCache::try_events_matching_tags`
and the cached wrapper `list_events_for_bounded_context_matching_tags_cached`
give it the same cache-first/Postgres-fallback shape every other cached
read here already has.

Four real call sites were switched over: `submitCommand` (GraphQL),
`post_commands_trigger` (REST) - both now fetch tag-scoped
`matching_events` directly instead of the whole bounded context - and
`queryEvents`/`countEvents`, when a caller supplies a `tags` filter
(user confirmed this scope explicitly, not just the originally-
motivating DCB path). A gap found only during implementation planning,
not in the original write-up: `db::submit_command`'s own DCB-conflict
redispatch check was *still* doing an unfiltered range scan
(`list_events_for_bounded_context_from`) to decide whether a concurrent
write actually conflicted - left alone, that would have just moved
Problem 1's cost from the common path into the conflict-recheck path
instead of removing it. Fixed the same way, internal to that function,
no signature change: the redispatch delta is now fetched via the same
tag-indexed query, `AND`-bounded by `original_highest` (itself now the
tag-scoped high-water mark, not the whole bounded context's). One
accepted behavioural side effect worth recording: `submit_command`'s own
`locked_highest > original_highest` branch now runs more often in a
busy, multi-entity bounded context - the tag-scoped high-water mark
moves more slowly than the bounded context's own overall sequence
counter - but each run is a small indexed query instead of a full scan,
so this is still a net win, not a regression.

Verified: a new `skilj-core/tests/tag_indexed_events.rs` (6 tests)
proves the query directly - correct tag matching, real union-not-
intersection semantics (mirroring `courses.rs`'s own dual-tag
`EnrollStudentInCourse`), an event matching two wanted tags returned
exactly once, `after_sequence` bounding, `sequence` ordering, and the
empty-`tags` short-circuit never touching Postgres at all. The GIN
index's actual use was confirmed by hand, not assumed: `EXPLAIN` against
8,000 rows still chose a sequential scan (correct, expected Postgres
behaviour for a table that small - not a bug), so the check was
re-run against 100,000 rows, which produced a real `Bitmap Index Scan
on events_by_tags` - proof the index does what it's for, not just that
it exists. Every existing real-Postgres test across `skilj-core`/
`skilj`/`skilj-demo`/`skilj-rest`/`skilj-inspector` passed unchanged,
`courses.rs`'s dual-tag tests included - the real regression proof, per
this session's own established discipline. `cargo build/clippy/test
--workspace` clean throughout (the one pre-existing, unrelated
`skilj-core` `explicit_auto_deref` warning noted in [§17](#event-command-codegen-real)/[§18](#ultra-review-fixes) untouched).

### `Snapshot`, built for real

`Snapshot`'s own design (above) held up against real implementation,
with the refinements already recorded at the top of this pass's own
plan and carried through faithfully: raw-JSON `decide_from_snapshot`
(not a typed state - Rust has no stable defaulted associated types),
no `Snapshot::keys()` (tag-value extraction is generic off `event.tags`),
no `SNAPSHOT_EVERY` cadence knob (folds every relevant event per catch-up
tick instead, mirroring `catch_up_bounded_context`'s own proven shape),
a new `snapshot_progress` table (one row per `(bounded_context,
snapshot_name)`, since `MAX(as_of_sequence)` over `snapshots` rows can't
safely stand in for how far the walk has progressed), and no GraphQL
registration mutation (confirmed by tracing the real registration
pipeline: `RegisterCommandType`/etc. are *already* a fully separate,
optional layer from the in-process Rust dispatcher - a Snapshot stays
compile-time-Rust-only, `#[auto_register]`/`SkiljBuilder::snapshot::<T>()`).

**The trait** (`skilj-core/src/plugin/mod.rs`): `Snapshot { type State,
type Event, NAME, BOUNDED_CONTEXT, TAG_KEY, VERSION, fn fold }` -
structurally parallel to `Projection` but a genuinely separate trait,
per the rejection already recorded above. `CommandType` gained two
defaulted methods, fully backward-compatible: `snapshot() -> Option<&'static str>`
(`None` by default) and `decide_from_snapshot(payload, snapshot_state_json:
&str, events_since_snapshot)` (a safe, non-panicking `Rejected` by
default, reachable only if a `CommandType` overrides `snapshot()`
without also overriding this).

**Wiring** mirrors `EventType`/`CommandType`/`Projection` exactly, one
new parallel track through the whole pipeline: `#[auto_register]` gained
a `"Snapshot"` arm (`skilj-macros`); `SkiljBuilder` gained `.snapshot::<T>()`,
a `SnapshotRegistrar`, and a `snapshots: HashMap<(String, String),
RegisteredSnapshot>` registry; a new `SnapshotDispatcher` trait
(`tag_key`/`version`/`fold`/`default_state`, plus `snapshot_names` - the
one method with no sibling on the other three dispatchers, needed
because there's deliberately no metadata table to enumerate instead);
`CommandDispatcher` gained `snapshot_name`/`dispatch_from_snapshot`,
the same outer/inner `Option` convention `required_role` already uses.

**Storage**: `{schema}.snapshots` (one row per `(snapshot_name, tag_key,
tag_value)`, `snapshot_version`/`as_of_sequence`/`state`/`updated_at`)
and `{schema}.snapshot_progress`, both added to
`provision_bounded_context_schema`. `db::get_snapshot_state`/
`resolve_snapshot_context` (the shared "does this command take the
snapshot path, and if so what does it read" logic, used by both
`skilj-graphql`'s `submitCommand` resolver and `skilj-rest`'s
`post_commands_trigger` route, so it lives once) and
`db::catch_up_snapshots` (the background task's own per-tick function,
mirroring `catch_up_bounded_context`'s shape closely but deliberately
not sharing code with it). A real bug caught by the test suite, not
inspection: the first version of both the insert/update and the read
queries didn't cast between the `String` sqlx binds/decodes and the
`JSONB` column (`state = $N` needs `$N::jsonb`; reading it back needs
`state::text`) - Postgres/sqlx don't do this coercion automatically
outside a `VALUES (...)` list. Two real failing tests caught this
immediately; fixed with explicit casts on both sides.

**`submit_command`'s own integration**: a new `SnapshotContext { state_json,
as_of_sequence }` parameter (`Option`, `None` for every existing
non-snapshot caller). The redispatch branch (a DCB conflict landed
between the optimistic read and the lock) now calls
`dispatch_from_snapshot` again, with the same `state_json`, when the
initial decision was snapshot-accelerated - calling the ordinary
`dispatch` there instead would silently drop everything the snapshot
had already folded, since the redispatch delta alone doesn't carry it.
One documented, narrow, audit-only limitation: `Command.consistency_boundary`
can under-report as `None` for a snapshot-accelerated command whose own
`events_since_snapshot` is empty, since that field's own computation has
no way to know about a snapshot's `as_of_sequence` - never affects the
decision itself, only that one audit field; left as a known gap rather
than widening `process_command`'s own signature too.

**The background task** (`skilj/src/lib.rs`): a new shared task, same
shape as the async-projection catch-up loop (`snapshot_poll_interval`,
default 500ms; `BACKGROUND_TASK_TICK_DURATION`/`BACKGROUND_TASK_ERRORS`
tagged `"snapshot"`; a trace root span per tick).

**The inspection endpoint**: `inspectSnapshot(boundedContext, snapshotName,
tagValue): InspectedSnapshot` (nullable - cold is a real, valid `null`,
not an error), Admin-gated via the same `require_admin_mapping` helper
`inspectEvent` uses, not folded into the `ReadAccess`-gated
`ProjectionQuery`. Deliberately shows the *raw stored row*, including a
`snapshot_version` that no longer matches the currently-registered one
- unlike `decide_from_snapshot`'s own path, which treats that as absent;
an operator inspecting a snapshot wants to see what's really there, only
the decision path needs to distrust it. An unrecognised `snapshotName`
is a real `not_found` error, distinguishable from a real, cold one.

**The real adopter**, `skilj-demo/src/banking.rs`: `AccountBalanceSnapshot`
(`TAG_KEY = "account"`) and `WithdrawMoneyFast`, both hand-written -
deliberately *not* part of `banking.skilj.toml`'s codegen'd shape, since
extending `skilj-codegen` itself for `snapshot()`/`decide_from_snapshot`
was out of scope for this pass. Shares `apply_money_event` (the same
per-event step `balance_of` already used) with the codegen'd `WithdrawMoney`'s
own `decide_withdraw_money`, so the two can never silently drift apart.

**Verification, in order of how convincing it is**:
- `skilj-core/tests/snapshot_context.rs` (5 tests): `resolve_snapshot_context`'s
  fallback rules (tag mismatch, multi-tag), cold-start default, a real
  `catch_up_snapshots` fold read back correctly, and the version-mismatch
  "treated as absent" rule, proven by constructing two dispatcher
  instances at different versions over the same stored row.
- `skilj-demo/tests/snapshot.rs` (2 tests): real HTTP through
  `WithdrawMoneyFast`, cold and after a real, forced `catch_up_snapshots`
  tick - and the decisive one: directly UPDATE-ing the stored row's own
  `state` to a deliberately wrong balance (leaving `snapshot_version`
  untouched, so it's still trusted) and showing the next withdrawal's
  own accept/reject decision changes to match the *tampered* number.
  This is what actually distinguishes "`decide_from_snapshot` genuinely
  reads the stored row" from "a full-replay fallback happens to reach
  the same correct answer" - a plain positive-path test can't tell the
  two apart, since both would reach the identical correct decision
  against real, untampered data.
- `skilj/tests/graphql_business_surfaces.rs`: a new, separate,
  minimal fixture (`ThingHappened`/`ThingTotalSnapshot`/`DoThingFast` -
  not `WithdrawMoney`, which this file's own fixture never tags at all)
  proving `inspectSnapshot` itself: cold is `null`, a real row after a
  real catch-up tick reads back correctly, an unregistered name is a
  real error, and a Write-level caller is rejected before ever reaching
  the snapshot table.
- Full existing regression suite (every crate) passes unchanged.
  `cargo build/clippy/test --workspace` clean (the one pre-existing,
  unrelated `skilj-core` warning noted throughout this file untouched).
  `allium check`/`allium analyse` clean on the spec change - no new
  findings, `SnapshotInspection` correctly recognised among the other
  surfaces.

**Spec** (`specs/skilj.allium`, via `allium:tend`): no new `entity` -
a `Snapshot`'s own definition is never created by any rule in this
system (no registration surface, deliberately), so there is nothing for
a `context` clause to range over the way `Event`/`Projection` are
ranged over elsewhere. Instead: a prose note alongside `rule ProcessCommand`'s
own `decide()` black box, describing the optional accelerated
computation as strictly behaviour-preserving (`decide()` always
observes the identical `matching_events` either way - this is a cost
optimisation, not a new observable branch), plus a new surface,
`SnapshotInspection` (`AdminAccess`-faced, no context clause -
`access_mapping.bounded_context` alone scopes every answer, the same
way `QueryEvents`/`CountEvents` already scope without one), with
`GrantScopedToBoundedContext`/`UnknownNameIsAnError`/`SensitiveFieldsStayProtected`
guarantees and a guidance note tying it back to the `ProcessCommand`
note and explaining the deliberate `Projection` separation.

<a id="four-new-filter-operators"></a>
## 20. Four new filter operators: geo, color, IP subnet, and generic `in`

User-initiated: geo/color/IP-address filtering, brainstormed further
into "should this be a general user-extensible type system instead?"
Investigated first (2 Explore agents plus direct code verification):
**no extensibility seam exists anywhere in the scalar/filter machinery**
today - `classify`/`filter_operator_is_valid`/`matches_one_filter`
(`event_store/mod.rs`), `scalar_kind_and_name`/`build_field`
(`skilj-graphql::projection_types`), and `form.rs::classify`
(`skilj-tui`) are three independent, un-shared closed `match` statements
over JSON-Schema `"type"`/`"format"` strings. This exact territory ("real
GraphQL scalar types") was already investigated and deliberately
deferred in [§8](#open-for-a-future-pass)/[§9](#next-steps) as "a genuinely separate, much larger change." Decided
via `AskUserQuestion` (twice, as the scope grew): ship four concrete
operators as contained additions to the existing `match` statements, no
registry, `FilterOperator` stays a closed enum. The general
user-extensible idea stays not built.

### What was built

| Type | JSON Schema shape | New `FilterOperator` | `Filter.value` encoding |
|---|---|---|---|
| Geo point | string, `format: "geo-point"` | `Near` | `"lat,lng,radius_meters"` |
| Color | string, `format: "color"` | `SimilarColor` | `"#RRGGBB,max_distance"` |
| IP address | string, `format: "ip"` (v4 or v6) | `InSubnet` | CIDR, e.g. `"192.168.1.0/24"` |
| Any scalar | no new format | `In` | comma-separated candidates, e.g. `"a,b,c"` |

All four follow the exact convention `date-time`/`date`/`partial-date-time`
already established for `GreaterThan`/`LessThan`: a plain JSON-Schema
string leaf with a `format` hint, schema-gated validity
(`filter_operator_is_valid`) but format-blind runtime matching
(`matches_one_filter` - the operator itself disambiguates what's being
compared, same as `GreaterThan` already tries three date/time parsers
without knowing which format the schema declared). No `Filter`/
`FilterInput` wire-shape change - compound values (a geo radius, a color
threshold) ride in the existing single `value: String`, exactly like a
date-range comparison already does. `In`'s comma delimiter is unescaped,
a known, accepted limitation matching `IsLike`'s `%`/`_` wildcards
already being unescaped too.

New helpers in `skilj-core::event_store` (each: parse both sides,
`false` on any parse failure, never panic - same register as
`string_ordering`): `geo_distance_within` (haversine, pure `f64`, no new
dependency), `color_similarity_within` (Euclidean RGB distance,
deliberately not a perceptual/CIE ΔE metric - the doc comment on
`FilterOperator::SimilarColor` says so, to not overclaim), `ip_in_subnet`
(delegates CIDR containment to the new `ipnet` crate rather than
hand-rolled bitwise subnet math - IPv6 in particular is easy to get
subtly wrong by hand), `matches_any_of` (reuses the existing
`json_scalar_to_string` helper the `Array`/`Contains` arm already had,
so `In` is exactly "equals one of").

### A real gap fixed along the way, not just a nice-to-have

Investigating `In`'s natural companion use case (filtering on an enum
value) found that enum filtering already mostly works today - a
`schemars`-derived unit enum already resolves to a filterable string
scalar via `event_store::classify`'s bare-`$ref`-following. But
`skilj-graphql::projection_types` (the one place payload-derived fields
become typed GraphQL fields, for `ProjectionQuery`) had **no equivalent
bare-`$ref`-to-scalar fallback** - `build_field`'s `depth == 0` branch
only ever tried `object_from_schema_value` (needs `"properties"`, so it
returns `None` for an enum definition) and then fell all the way through
to the *opaque-JSON* fallback, which double-JSON-encodes a string value
(`raw.to_string()` on a `Value::String` produces `"\"Shipped\""`, quotes
included) - not the clean `ScalarKind::String` rendering a first read of
the code might suggest. Fixed for real, not just documented as a gap: a
new `enum_values_from_schema` check recognises a `{"type": "string",
"enum": [...]}` definition and registers a real `async_graphql::dynamic::Enum`
(namespaced `{parent_type_name}_{field_name}`, same collision-avoidance
`nested_type_name` already uses for one-level nested objects) instead.
Verified against `async-graphql`'s own vendored `dynamic::Enum` test
(`src/dynamic/enum.rs`) that the correct runtime representation is
`Value::from(Name::new(s))`, not a plain `String` `Value` - getting this
wrong would have been a runtime schema-mismatch error, not a compile
error. A stored value no longer among the enum's registered items (e.g.
old data after a schema change removed a variant) now surfaces as a real
GraphQL field-level error rather than either panicking or silently
degrading - same "reject gracefully, not silently" register as the rest
of this module, just realised as a typed error instead of an
opaque-JSON fallback here.

### Not touched, per the locked-in scope decision

- No registry/trait - every change is a new arm in an already-duplicated
  `match`, not a new mechanism.
- `skilj-codegen`'s `FieldType` (`String`/`I64`/`Bool`) - unchanged, per
  its own documented "not a general type system" stance; the four new
  types are only usable via hand-written `#[derive(JsonSchema)]` structs,
  same as `date-time` already is.
- `skilj-tui::form.rs` - no change needed. All four types are
  string-encoded, so they already get the existing `Text` widget by
  default.
- Structured "address" types, raised in the same brainstorm - out of
  scope. A postal address is already filterable today as a one-level
  nested object with per-field dotted-path filters (e.g.
  `address.city`), no new code needed for that case; genuine geo-aware
  "near this address" just means the payload also carries a `geo-point`
  field and reuses `Near` directly.

### Verified

`skilj-core/tests/event_filtering.rs`: 12 new pure-function tests (no
DB) - `valid_filters` gating for each new format (accepted only when
gated, rejected on an ungated field or the wrong operator) and `In`
across every scalar kind; `matches_filters` real match/no-match/malformed
-input cases for all four operators, using genuinely far-apart
points/colors/subnets so a broken distance/containment check would fail
the test, not pass by coincidence. `skilj-graphql/src/projection_types.rs`:
3 new tests using a real `schemars::schema_for!` capture (not
hand-written, same discipline as this module's existing tests) - the
enum renders as a real string through actual GraphQL execution, the
schema really registers a distinct `Enum` type (not `String`/opaque
JSON), and a stale stored value really does surface as a GraphQL error.
`skilj/tests/event_fetch_rest.rs` and `skilj/tests/event_subscription.rs`:
one real end-to-end test each (`In`, over REST and over a live GraphQL
subscription respectively) proving the new wire-parsing arms
(`parse_filter_param`, `gql_types::filter_operator_enum`/
`resolvers::parse_filters`) actually reach `valid_filters`/
`matches_filters` through the real HTTP/WebSocket stack - not repeated
per-operator, since `Near`/`SimilarColor`/`InSubnet` share the identical
parsing plumbing, just gated to a different `format`. Full existing
regression suite passes unchanged; `cargo build/clippy/test --workspace`
clean; `allium check` clean on the spec change.

<a id="optional-idempotency-key-submission"></a>
## 21. Optional idempotency key for command submission (Codeberg issue #12)

Command submission had no idempotency mechanism - `Command.id` is
always server-generated (`generate_token_id()`), never derived from or
checked against the caller's own request. A client retrying a command
after a network timeout had no way to avoid double-applying it.

Investigation (tracing the real `submit_command` pipeline) found two
things that reshaped the naive version of this feature, both resolved
with the user via `AskUserQuestion` before building:

1. **Per-bounded-context schemas are provisioned once, never migrated.**
   `provision_bounded_context_schema` runs exactly once, at
   `BoundedContext` creation; there was no `ALTER TABLE` anywhere in
   this codebase before this pass. A naive new column/table would only
   have existed for bounded contexts created *after* this shipped -
   every already-provisioned one (real, now that skilj is public)
   wouldn't have gotten it. **Resolved**: a small, targeted, idempotent
   schema-patch (`CREATE TABLE IF NOT EXISTS`), run against every
   bounded context on every `build()`, piggybacking on the existing
   unconditional per-bounded-context startup loop in
   `SkiljBuilder::build()` (previously just warming `EventCache`) - not
   a general migration framework, a deliberately narrower fix.
2. **Rejected outcomes are never persisted at all** - `submit_command`
   returns immediately on rejection, no `Command` row written. Since a
   rejection has zero side effects, re-deciding a duplicate rejected
   submission is harmless. **Resolved**: dedup applies to `Accepted`
   outcomes only - the case with real side effects (events). A
   duplicate that was rejected is simply recomputed fresh, correctly,
   every time.

Also resolved: REST wire shape is an `Idempotency-Key` header (the
Stripe-style convention, keeps it out of the domain payload); scoping
is `(bounded_context, command_type)` only, not also per-caller; no
retention/TTL (matches this codebase's existing "nothing is ever
deleted except via `ForgetSubject`/bounded-context deletion"
precedent).

**Realising "generate a random key when absent" as actually asked
for**: rather than literally generating and storing a random key (and
its insert) on every keyless submission - which would add a DB write
to every single command submission for zero possible benefit, since a
fresh random key can never collide with anything - a keyless
submission simply skips the idempotency mechanism entirely: no lookup,
no insert, zero overhead, byte-identical to the pre-existing
behaviour. Observably equivalent to "always succeeds as new," which
was the actual intent.

### Storage - one new table, minimal by construction

```sql
CREATE TABLE IF NOT EXISTS {schema}.idempotency_keys (
    command_type_name TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    triggered_event_sequences BIGINT[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (command_type_name, idempotency_key)
)
```

Deliberately stores only `triggered_event_sequences` - neither
response shape (`SubmitCommandResult`/`CommandTriggerResponse`) needs
the full `Command`/`Event` objects back on a dedup hit, only the
sequence numbers, so a hit needs no join back to `commands`/`events`
at all. `db::ensure_idempotency_keys_table` (`impl PgExecutor`, the
same "works on `&Pool` autocommit or inside a caller's own open
`Transaction`" treatment `update_role`/`next_sequence` already have)
owns this DDL, called from both `provision_bounded_context_schema`
(brand-new bounded contexts) and `SkiljBuilder::build()`'s startup
loop (patches every already-provisioned one, every startup, forever -
`CREATE TABLE IF NOT EXISTS` makes repeated calls free).

### `submit_command` integration

New `idempotency_key: Option<&str>` parameter. The existing `SELECT
next_value FROM {schema}.sequence FOR UPDATE` already serializes every
submission to one bounded context across all instances (the
codebase's own existing concurrency precedent - Postgres NOTIFY/LISTEN
is at-most-once and not usable for correctness) - so the idempotency
check needs no lock of its own, riding entirely on the lock already
held:

1. Right after the sequence lock is acquired: if `idempotency_key` is
   `Some`, look it up in `idempotency_keys` (plain `SELECT`, inside the
   same transaction, already race-free under the held lock). A hit
   short-circuits immediately to a new `SubmitCommandOutcome::Deduplicated
   { triggered_event_sequences }`, skipping DCB-conflict redispatch,
   event insertion, and the `Command` row entirely - a cached prior
   answer, not a new decision. `tx` is simply dropped (implicit
   rollback) on a hit, the same as every other early return in this
   function.
2. A miss proceeds exactly as before. After computing a real `Accepted`
   outcome and before `tx.commit()`: if `idempotency_key` was `Some`,
   `INSERT INTO {schema}.idempotency_keys (...)` in the same
   transaction - plain insert, no `ON CONFLICT`, since the lock already
   ruled out a concurrent duplicate reaching here.

No change to the optimistic pre-lock prelude in either surface - on a
true retry, `decide()` runs once more, wastefully but harmlessly (pure
function, no side effects), before the DB-level check prevents any
actual persistence. Same accepted-optimism shape the existing
DCB-conflict redispatch path already has.

### Both surfaces

`skilj-graphql`'s `submitCommand` gained an optional `idempotencyKey:
String` argument; `skilj-rest`'s `POST /v1/commands/trigger` gained an
optional `Idempotency-Key` header. Both map `Deduplicated` the same
way as `Accepted` in their response (`accepted: true`,
`triggeredEventSequences` from the stored value, no live
`matchingEvents` - a dedup hit has nothing redispatched to show), plus
a new `deduplicated: bool` field (default `false`, `true` only on a
dedup hit) on both `SubmitCommandResult`/`CommandTriggerResponse` -
cheap, real observability for a caller that wants to know whether its
retry actually got deduped.

### Spec

`specs/skilj.allium`, via `allium:tend`: this is a genuine new
observable guarantee, not a pure optimisation (unlike `Snapshot`'s
"identical answer either way" framing) - a duplicate accepted
submission returns the *original* decision, which can differ from what
a fresh `decide()` would now produce if state changed in between.

### Verified

`skilj-core/tests/submit_command.rs`: a real short-circuit test
(mirroring a genuine retry - the caller's own optimistic `dispatch()`
runs a second time too, same key both times - the second
`submit_command` call must return the *first* call's own outcome
verbatim and insert nothing new), a companion regression test proving
the no-key case still double-processes exactly as before this feature
existed, and a migration-gap test that drops a freshly-provisioned
bounded context's own `idempotency_keys` table (simulating "provisioned
before this feature existed"), re-runs `ensure_idempotency_keys_table`,
and proves idempotency actually works normally afterward, not just that
the table exists again. The short-circuit test was verified decisive,
not just passing by construction: temporarily disabling the insert side
made it fail for real (`Accepted` instead of `Deduplicated` on the
second call), then the fix was restored.

`skilj/tests/command_trigger.rs` (REST) and
`skilj/tests/graphql_business_surfaces.rs` (GraphQL): real end-to-end -
submit twice with the same `Idempotency-Key`/`idempotencyKey`, assert
identical `triggeredEventSequences` and `deduplicated: true` on the
second response, and confirm via a follow-up event read that only one
set of events actually exists. Full existing regression suite passes
unchanged - proving the "no key ⇒ zero behaviour change" property for
real. `cargo build/clippy/test --workspace` clean; `allium check` clean
on the spec change.

<a id="background-polling-startup-scaling"></a>
## 22. Background-polling and startup scaling (Codeberg issue #15)

`SkiljBuilder::build()`'s startup warm-up loop and all three
per-bounded-context background pollers (async-projection catch-up,
snapshot catch-up, scheduler tick) redid real, uncached work for *every*
bounded context on *every* tick, sequentially, with no early-exit before
real cost was paid. Direct code reading (not just the investigation
that raised this issue) found it went deeper than first estimated:
`catch_up_bounded_context`'s per-projection rebuild check called
`get_projection_rebuild`, which itself issued 3 queries per call
(`get_projection`, the rebuild row, `rebuild_consumed_event_types`) -
and `rebuild_consumed_event_types` had its **own** inner N+1
(`get_event_type` per consumed event type name). A bounded context with
`P` registered projections paid roughly `2P` queries every 500ms tick in
the common "nothing building" case, not just `P`, before its own
early-exit even fired.

Raised while investigating a "route commands to specific instances"
idea for multi-tenancy (Codeberg issue #13) - that idea was not
adopted: every skilj instance is fully interchangeable for any bounded
context today, a deliberate, twice-confirmed design decision ([§12](#cross-instance-push-completeness)), and
building instance routing would have reversed it for no correctness
benefit. The real, underlying performance concern was legitimate, just
aimed at the wrong fix - this section is that fix, entirely orthogonal
to instance routing/affinity, which stays untouched.

### Fix 1: `db::list_bounded_contexts`'s N+1

Used to call `get_role` once per row with a `created_by_role_id`.
`db::list_roles` already fetches every `Role` in one query - reused
instead: one full-table fetch plus an in-memory `HashMap` lookup
replaces what was one query per row.

### Fix 2: `SkiljBuilder::build()`'s startup loop, and Fix 5: all three background pollers

All four used to be a plain sequential `for bc in list_bounded_contexts(...)
{ <per-bc work>.await }` - no concurrency, so cost scaled linearly with
bounded-context count. Each bounded context's own work only ever
touches its own Postgres schema, so nothing is shared mutable state
across iterations - safe to run concurrently. Converted to
`futures_util::stream::iter(...).buffer_unordered(BACKGROUND_TASK_CONCURRENCY)`
(startup, needs real error propagation, `try_collect`) /
`.for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, ...)` (the three
pollers, which already handle their own per-bc errors inline - no
propagation needed). `BACKGROUND_TASK_CONCURRENCY` is a fixed constant
(16), not a new `SkiljBuilder` tunable - the lower-risk incremental fix
the issue's own open questions asked about; easy to expose as a builder
option later if a real need for tuning it ever comes up.
`scheduler_tick`'s own per-bc body (real side-effecting logic - firing
system events, tracking missed occurrences) was extracted verbatim into
`scheduler_tick_for_bounded_context` first, a pure mechanical
extraction with no logic change, so the concurrent fan-out had a
function to call per bounded context without touching the delicate
inner per-event-type/per-occurrence walk at all - that inner loop stays
exactly as sequential as it always was within one bounded context's own
turn.

`futures-util` was already a workspace dependency but only a
`skilj`-crate *dev*-dependency (used in its own tests for WebSocket
handling) - promoted to a real dependency now that `src/lib.rs` uses it
too.

**Verified empirically, not just reasoned about from the code shape**:
a new test times the identical warm-up work (`EventCache::warm` +
`ensure_idempotency_keys_table`) both sequentially and concurrently
against 40 real, seeded bounded contexts in the same test run - a real
A/B comparison, not a before/after across separate commits. Measured
result on a local, low-latency embedded Postgres: sequential 63.5ms,
concurrent (×16) 28.7ms - roughly 2.2x, with the gap expected to be
larger over a real network deployment where the per-round-trip latency
this concurrency overlaps is higher.

### Fix 3: `db::catch_up_bounded_context`'s rebuild-check loop

Replaced the per-projection `get_projection_rebuild` loop with one new
batched function, `list_building_projection_rebuilds_for_bounded_context`:
one query for every `building` row in the bc's `projection_rebuilds`
table, one for every row in `projection_rebuild_consumed_event_types`
(grouped by `projection_name` in memory), one for every `EventType` in
the bc (`list_event_types_for_bounded_context`, already a single
query), and zero further queries for the live `Projection` each rebuild
belongs to - `all_projections` is data `catch_up_bounded_context`
already had in hand, handed in rather than re-fetched via `get_project`
per row the way `get_projection_rebuild` does for its own single-row
callers (`RebuildProjection`/`DiscardProjectionRebuild`'s resolvers -
untouched, a single lookup is the right shape there). Net: the whole
rebuild-check phase drops from up to `~2P` queries to a small constant
number (3-4) regardless of `P` or how many rebuilds are in flight.

**A real, targeted regression test, not just reliance on existing
coverage**: two real, differently-named projections registered, only
one with a building rebuild - proves the batched query correctly
attributes each `projection_rebuilds` row (and its own
`consumed_event_types`) to the *right* projection, not just that "some"
rebuild gets found. A single-projection fixture (all the pre-existing
tests) can't expose an identity mix-up between two rows the way this
can - the other, non-building projection's schema/schema_version are
asserted completely untouched.

### Fix 4: `db::catch_up_snapshots`'s progress-check loop

Same shape, smaller: the per-snapshot-name `get_snapshot_progress` loop
replaced with one new `list_snapshot_progress_for_bounded_context(pool,
bc) -> HashMap<String, i64>` - one query, no `WHERE`, looked up per
registered snapshot name in memory afterward (defaulting to `-1` for a
name with no row yet, matching the old per-call default exactly).
`get_snapshot_progress` had no other call site, so it was replaced
outright rather than kept alongside the new batched function.

### Not in scope this pass (flagged, not forgotten)

Pagination of `list_bounded_contexts` itself, or a UNION-style
cross-schema query to replace the scheduler's own remaining O(N) (one
query per active bc, no multiplier - already the least urgent of the
four loops before this pass, still true after it). Real open questions
from the issue itself; revisit only if these fixes aren't enough at the
target scale. No general per-bounded-context migration/dirty-tracking
framework was built - these are targeted query-shape fixes, not new
infrastructure. Nothing about instance routing/affinity - confirmed out
of scope by the investigation that raised this issue; untouched here.

**Verified**: full existing regression suite (every crate) passes
unchanged - including the real end-to-end async-projection/snapshot/
scheduler tests, proving the batched queries and concurrent fan-out
preserve behaviour exactly, not just "look right." `cargo
build/clippy/test --workspace` clean.

<a id="cross-tenant-projection-read-fix-owner-tag"></a>
## 23. Cross-tenant projection read fix: owner-tag scoping on `RoleAccessMapping`

A security review found that `ProjectionQuery`'s only access check
(`require_read_mapping`, `skilj-graphql/src/resolvers/mod.rs`) was "does
the caller hold any active `RoleAccessMapping` on this bounded context" -
never whether the specific instance queried belonged to that caller. In
a bounded context shared by several tenants (skilj-helpdesk's own
motivating case: every company's tickets live in one `helpdesk` bounded
context, `company_id` a payload field rather than a tenancy boundary),
any authenticated caller with read access could query any instance by
key - `TicketSummary`/`CompanyTicketList` for a company they had nothing
to do with, and equally a staff-only, non-sensitive projection like
`TicketInternalNotes`. The existing per-field sensitive-data mechanism
(`can_read_sensitive`/subject-match) didn't cover this: it's a
field-level crypto-shredding gate, not an instance-level read ACL, and
its self-match grant only fires when a projection's key *is* a person's
own identity - never for a ticket-id-keyed projection, and it says
nothing at all about a non-sensitive projection's own visibility.

skilj's authorization model had exactly two scopes: bounded-context-level
(`RoleAccessMapping`) and per-field-sensitive-value (`EncryptionKey`) -
no primitive for "this Role may only see instances whose owner matches a
value it's scoped to" within one shared bounded context. Three
directions were on the table (a scoping value on `RoleAccessMapping`
checked against a declared owner tag on the projection; a pluggable
per-query authorization callback the embedding app registers; extending
the subject/`EncryptionKey` mechanism to also gate plain visibility) -
the first was chosen: it fits the existing DCB-tag-based design most
closely, and doesn't conflate encryption with access control the way the
third would.

**Design**: two new pieces, deliberately asymmetric in how public they
are.

`Projection::OWNER_TAG_KEY: Option<&'static str>` (`skilj-core/src/plugin/mod.rs`,
default `None`) names which tag key - one a consumed event type's own
`tag_mappings` already produces - is this projection's "owner" dimension.
Mirrors `Snapshot::TAG_KEY`'s own treatment exactly: a compile-time Rust
constant resolved in-process via a new `ProjectionDispatcher::owner_tag_key`
method (same `Option<Option<&'static str>>` shape
`CommandDispatcher::snapshot_name` already has - outer `None` for "pair
not registered"), with **no spec entity field and no registration
surface** - implementation detail an admin has no need to see, not
something `RegisterProjection` reconciles.

`RoleAccessMapping.scope: Option<String>`, by contrast, **is** spec'd
and DB-persisted (`role_access_mappings.scope`, migration
`0004_add_role_access_mapping_scope.sql`), because `GrantRoleAccessMapping`
is a real, GraphQL-exposed mutation an admin calls. `None` (every
mapping's default, including every one granted before this column
existed) means unrestricted within the bounded context - identical to
this mapping's own behaviour before `scope` existed. `Some(v)` restricts
the grant: for a projection that declares `OWNER_TAG_KEY`, it may only
read instances whose own derived owner equals `v`. A projection that
declares no owner dimension at all is unaffected by `scope` regardless
of its value - `banking.rs`/`courses.rs` and every other existing
projection needed zero changes.

Each instance's own owner is derived automatically, not caller-supplied:
when an event folds into a projection that declares `OWNER_TAG_KEY`, and
that event's own `tags` carries a tag with that key and a non-null
value, that value becomes (or refreshes) the instance's stored `owner`
column (`projection_state.owner`/`projection_rebuild_state.owner`, both
nullable). An event lacking the tag leaves an already-established owner
untouched - one wrongly-modelled event in an instance's history can't
erase existing scoping. `apply_projection_fold_update`
(`skilj-core/src/db/mod.rs`) is the one place this is computed, shared by
all four "fold one event, persist the new state" call sites
(`insert_event_and_update_sync_projections_in_tx`, both of
`catch_up_bounded_context`'s live and rebuild-building loops, and
`fold_history_into_new_sync_projection`) that were near-identical copies
of the same `UPDATE` before this pass - consolidated along the way, not
left duplicated a fifth time. `promote_projection_rebuild`'s own
`INSERT ... SELECT` from `projection_rebuild_state` into `projection_state`
carries `owner` across too, so a promoted rebuild keeps whatever
ownership it derived while building.

Enforcement lives in `projections::query_projection`
(`skilj-core/src/projections/mod.rs`), the spec's own `owner_scope_satisfied`
black box: a new `Error::GrantScopeMismatch` rejection, fail-closed. When
the projection declares an owner dimension and the grant's `scope` is
`Some`, the query is rejected unless the instance's own derived owner is
`Some` *and* equal to it - an instance whose ownership can't be
affirmatively proven (including a key nothing has touched yet, which
previously answered from `ProjectionDispatcher::default_state` with no
further check) is treated the same as a proven mismatch, not as "no
conflict." `skilj-graphql/src/resolvers/projection_query.rs`'s resolver
wires it: a new `db::get_projection_state_and_owner` (kept separate from
the existing `get_projection_state`, which has many callers - tests
included - uninterested in `owner`) fetches the stored owner alongside
state, `ProjectionDispatcher::owner_tag_key` says whether the projection
declares a dimension at all, both feed into `query_projection`. The
GraphQL-facing `grantRoleAccessMapping` mutation gained an optional
`scope: String` argument (`access_management.rs`), and `RoleAccessMapping`'s
own GraphQL type exposes it (`gql_types.rs`, reusing the existing
`optional_string` helper).

`specs/skilj.allium` changes (via `allium:tend`, `allium check` clean,
`allium plan` moved 397 → 399 obligations with exactly the two expected
additions - `entity-optional.RoleAccessMapping.scope`,
`rule-failure.QueryProjection.4` - nothing else): `entity RoleAccessMapping`
gained `scope: String?`; `rule GrantRoleAccessMapping` gained an optional
`scope` parameter; `rule QueryProjection` gained
`requires: owner_scope_satisfied(projection, instance_key, access_mapping)`,
placed before the `wait_for_sequence` check (resolve the instance, check
ownership, *then* block on catch-up - not the other way round); `surface
ProjectionQuery` gained `@guarantee GrantScopedToOwnerWhenDeclared`
alongside the existing `GrantScopedToBoundedContext`. One gap flagged by
the spec pass, deliberately left alone: `rule CreateBoundedContextFromTemplate`
is the *second* place a `RoleAccessMapping` comes into being and takes no
`scope` argument, so a templated tenant's grant is always created
unrestricted - and, since `UniqueActiveGrantPerRoleAndContext` forbids a
second active grant for the same `(role, bounded_context)` pair with no
amend-in-place rule, cannot be scoped for the life of that mapping short
of revoking and re-granting it. Out of scope for this pass; revisit if
owner-scoped templated tenants become a real need.

**Explicitly out of scope, flagged for a follow-up**:
`FetchEvents`/`QueryEvents`/`EventSubscription` read/deliver raw events
by tag filter under the same "any active bounded-context mapping sees
everything" gate, with no per-role tag scoping either - a same-shaped
gap, but a separately-designed fix (a subscription can span several
event types, each with its own independent `tag_mappings`, so there's no
single natural "the owner tag" the way one `Projection` declares one).

**Verified**: a new real-Postgres regression suite,
`skilj-core/tests/projection_owner_scoping.rs` - a `TicketOpened` event
type tagged `company`, feeding a `TicketSummary` projection that
declares `OWNER_TAG_KEY = Some("company")`, exercised through the real
fold path (not `apply_projection_fold_update` called directly): confirms
two different companies' tickets get correctly distinct derived owners,
an untagged follow-up event (`TicketCommented`) leaves an established
owner untouched, and - the concrete vulnerability this pass closes - a
`scope`-restricted grant can read its own company's ticket but is
rejected reading the other company's, a never-touched key is rejected
the same fail-closed way, and an unscoped (staff) grant remains
unrestricted. `skilj-core/tests/projection_query.rs` gained six new pure
unit tests covering `query_projection`'s new parameters directly (the
new `rule-failure.QueryProjection.4` obligation, plus every accept path).
Every one of the ~40 existing `RoleAccessMapping`/`grant_role_access_mapping`
call sites across the workspace (test fixtures and the three real
admin-bootstrap sites in `skilj-demo`/`templates/skilj-template`/
`bounded_context_templating.rs`) updated to carry `scope: None` -
`cargo build/clippy/test --workspace` and `cargo fmt --check` all clean,
full existing suite green with no behavioural change to any
already-unscoped mapping or projection.

<a id="cross-tenant-read-fix-raw-events"></a>
## 24. Cross-tenant read fix, part two: raw events (`FetchEvents`, `QueryEvents`/`CountEvents`/`InspectEvent`, `EventSubscription`)

[§23](#cross-tenant-projection-read-fix-owner-tag) closed the gap for `ProjectionQuery`; this pass closes the same gap
for skilj's other read surfaces, all of which shared the identical
shallow check (`require_read_mapping`/a bearer `EventReadToken`: "any
active grant/token", never "does this specific *event* belong to the
caller"). `EventSubscription` is `ReadAccess`-faced, exactly parallel to
`ProjectionQuery`; `QueryEvents`/`CountEvents`/`InspectEvent` are
`Admin`-faced but still leak across tenants sharing one bounded
context - a company's own admin, scoped to their own company, could read
every other company's raw events too; `FetchEvents`/`ConsumeEvents` are
the REST track's own pull-based reads, authenticated by a bearer
`EventReadToken` with no `Role` behind it at all.

**Design**: the same `scope`-vs-derived-owner mechanism as [§23](#cross-tenant-projection-read-fix-owner-tag), but
reshaped around two differences from the projection case.

First, raw events are already fully described by `EventType.tag_mappings`,
a real, registered, spec'd field - unlike a `Projection`'s opaque
instance state, there's no need for a parallel Rust-only declaration.
`EventType.owner_tag_key: Option<String>` (`skilj-core/src/event_store/mod.rs`)
names which of a type's own `tag_mappings` keys is its owner dimension,
and **is** spec'd and registered (unlike `Projection::OWNER_TAG_KEY`,
which deliberately mirrors `Snapshot::TAG_KEY`'s Rust-only treatment):
`RegisterEventType` gained a `valid_owner_tag_key(tag_mappings,
owner_tag_key)` requires clause, mirroring `valid_tag_mappings`'s own
shape - null, or a key present in `tag_mappings`. Unlike `tag_mappings`
itself, `owner_tag_key` is *not* additive-only on re-registration: it's
a pointer into the mappings, re-validated fresh every time, free to
change or clear. `plugin::EventType::owner_tag_key() -> Option<&'static str>`
(default `None`) is the Rust-facing declaration; `RegisteredEventType`
(`skilj/src/lib.rs`) and both reconciliation paths that call
`register_event_type` (the `#[auto_register]` startup loop, and
`bounded_context_templating.rs`'s template-application path) carry it
through unchanged from the source type.

Second, an instance's own owner is derived per-event, not per-instance:
`event_store::event_owner_scope_satisfied(event: &Event, scope: Option<&str>) -> bool`
holds when `scope` is `None`, the event's own type declares no
`owner_tag_key`, or `event.tags` carries a tag with that key and a
matching value - fails closed otherwise (no such tag, or a null-valued
one, is treated as a proven mismatch, the identical stance
`projections::query_projection`'s own `owner_scope_satisfied` takes).
Deliberately takes a raw `scope: Option<&str>` rather than a whole
`RoleAccessMapping`, so it serves both tracks identically: GraphQL
callers pass `access_mapping.scope`, the REST track passes the new
`EventReadToken.scope: Option<String>` (`skilj-core/src/access_control/mod.rs`,
set at minting time by `create_event_read_token`'s new optional `scope`
parameter - independent of the minting admin's own `access_mapping.scope`,
so an unscoped staff admin can mint a company-scoped token for a
company's own external integration).

Multi-record surfaces filter rather than reject: `query_events`/
`count_events`/`deliver_to_subscriptions`/`fetch_events`/`consume_events`
(`skilj-core/src/event_store/mod.rs`) each gained one more `.filter()` in
their existing chains - a non-owned event is silently excluded, the call
still succeeds, the same "redact, don't reject the whole query" register
sensitive-field decryption already uses. `inspect_event`, the one
single-record surface here (mirrors `query_projection`'s own shape),
rejects outright with the relocated `access_control::Error::GrantScopeMismatch` -
moved up from `projections::Error` (where [§23](#cross-tenant-projection-read-fix-owner-tag) first put it) to sit
alongside `GrantBoundedContextMismatch`, since it's now shared by both
`projections::query_projection` and every function in this pass, the
same cross-cutting-error convention `GrantBoundedContextMismatch` itself
already follows.

No GraphQL/REST resolver wiring changed for `queryEvents`/`countEvents`/
`inspectEvent`/`fetchEvents`/`consumeEvents`/event delivery - every call
site already passed its whole `RoleAccessMapping`/`EventReadToken`/
`Subscription` through unchanged, so `scope` rides along for free.
`registerEventType`'s mutation gained an optional `ownerTagKey: String`
argument (`type_registration.rs`, mirroring `grantRoleAccessMapping`'s
own `scope` argument from [§23](#cross-tenant-projection-read-fix-owner-tag)) and `createEventReadToken`'s gained
`scope: String` - pulled out of the shared `create_type_token_field!`
macro into its own hand-written resolver, the same reason
`db::get_event_type_access_token!` already has an identical exception
for `get_event_read_token` (only `EventReadToken` carries the extra
field; the macro's fixed shape has no way to vary it per invocation).

**Deliberately not addressed this pass, flagged for a follow-up**:
`CommandQuery`/`FetchCommands` have no owner scoping at all, and
`CommandType` already carries `tag_mappings` the identical way
`EventType` does - a scoped caller filtered out of an event by
`QueryEvents` can currently still read the `Command` that produced it
unfiltered, via `FetchCommands`. Also unexposed: `EventType.owner_tag_key`
has no GraphQL read-back (`TypeRegistration`'s `exposes:` doesn't list
it) and `EventReadToken.scope`/`RoleAccessMapping.scope` aren't the only
place `token_object!`'s shared macro would need a similar per-variant
exception to expose it on the wire. Neither is a security gap in what
this pass covers - both are read-back/completeness gaps only.

**Verified**: a new pure-function suite,
`skilj-core/tests/event_owner_scoping.rs` (16 tests, no Postgres needed -
every function this pass touches is already pure) - `event_owner_scope_satisfied`
in isolation across every hold/fail-closed case, then `query_events`/
`count_events` filtering, `inspect_event` rejecting, `fetch_events`/
`consume_events` filtering by `token.scope`, and `deliver_to_subscriptions`
skipping a scoped subscription for another company's event while
delivering to an unscoped one regardless. Three new `type_registration.rs`
tests cover `valid_owner_tag_key`'s reject/accept/re-registration-clears
paths. `EventType.owner_tag_key`/`EventReadToken.scope`'s own DB
round-trip is covered by `persistence.rs`'s existing whole-struct
`assert_eq!` tests, which now include both fields by construction.
`cargo build/clippy/test --workspace` and `cargo fmt --check` clean;
`allium check`/`plan`/`analyse` independently re-run against the spec
diff (not taken on the `allium:tend` report alone) - clean, 399 → 402
obligations with exactly the three expected additions, the same 4
pre-existing `analyse` findings unchanged.

<a id="cross-tenant-read-fix-command-query"></a>
## 25. Cross-tenant read fix, part three: `CommandQuery`/`FetchCommands`

The third and final pass. [§23](#cross-tenant-projection-read-fix-owner-tag) covered `ProjectionQuery`, [§24](#cross-tenant-read-fix-raw-events) covered raw
events (`QueryEvents`/`CountEvents`/`InspectEvent`, `FetchEvents`/
`ConsumeEvents`, `EventSubscription`); this one covers `CommandQuery` -
`FetchCommands`, the only read surface for `Command`. Narrower than [§24](#cross-tenant-read-fix-raw-events)
in one real way: the REST track's `CommandToken` only *triggers* one
command type and reads nothing back (`surface CommandQuery`'s own
guidance: "the REST track has no equivalent and is not meant to grow
one"), so there is no token-scope side to build here - one entity field,
one rule, one surface guarantee, done.

**Design**: exactly [§24](#cross-tenant-read-fix-raw-events)'s mechanism, adapted to `Command`/`CommandType`
in place of `Event`/`EventType`. `CommandType.owner_tag_key: Option<String>`
(`skilj-core/src/event_store/mod.rs`) - spec'd and registered exactly
like `EventType.owner_tag_key`, validated by the *same*
`valid_owner_tag_key(tag_mappings, owner_tag_key)` function reused
unchanged (it only asks a question about a `Set<TagMapping>` and a
string, generic over which entity the mappings came from - the same
"takes an EventType and a CommandType interchangeably" register
`derive_tags` already established). `command_owner_scope_satisfied(command: &Command, scope: Option<&str>) -> bool`
is `event_owner_scope_satisfied`'s twin, reading `Command.consistency_tags`
where that one reads `Event.tags`. `consistency_tags` is the right field
precisely because it's `derive_tags(command_type, payload)`
unconditionally on every stored command - a plain, never-null `Vec<Tag>` -
regardless of whether that particular command actually used a
consistency boundary; only `consistency_boundary: Option<i64>` goes
missing for that case. `fetch_commands` gained one more `.filter()` in
its existing chain, covering both the type/time-window narrowing and the
`triggered_event` reverse lookup uniformly (naming an event whose
command belongs to another owner yields nothing, not an error). No
single-command reject case here, unlike `inspect_event` on the event
side - `FetchCommands` is a browse over many, always a filter.

`RegisterCommandType` gained the identical treatment `RegisterEventType`
got: a new optional `owner_tag_key` parameter,
`valid_owner_tag_key`-checked, *not* additive-only on re-registration
(free to change or clear, unlike the `tag_mappings` keys it points into).
`RegisteredCommandType` (`skilj/src/lib.rs`) and both reconciliation
paths (`#[auto_register]`'s startup loop, and `bounded_context_templating.rs`'s
template-application path) carry it through from `plugin::CommandType::owner_tag_key()`
unchanged, mirroring the event-type wiring exactly.
`registerCommandType`'s GraphQL mutation gained the matching optional
`ownerTagKey: String` argument. No resolver wiring changed for
`fetchCommands` itself - it already passed the whole `RoleAccessMapping`
through.

**Same read-back gap as [§23](#cross-tenant-projection-read-fix-owner-tag)/[§24](#cross-tenant-read-fix-raw-events), spanning all three now**: neither
`EventType.owner_tag_key` nor `CommandType.owner_tag_key` is exposed by
`TypeRegistration`'s own query side - an admin can set or clear the
declaration but can't read back which key is currently in force. Left
alone again, consistently, rather than fixed on one side only; worth a
follow-up if it becomes a real operational need.

**Verified**: a new pure-function suite,
`skilj-core/tests/command_owner_scoping.rs` (12 tests, no Postgres -
`fetch_commands`/`command_owner_scope_satisfied` are pure like every
function this pass touches) - the same isolation coverage
`event_owner_scoping.rs` has for its own predicate, `fetch_commands`
filtering by scope (including the concrete cross-tenant scenario: a
company's own admin no longer sees another company's commands) and the
`triggered_event` reverse lookup filtered the same way, plus three
`RegisterCommandType` tests (`valid_owner_tag_key` reject/accept/
re-registration-clears). `CommandType.owner_tag_key`'s own DB round-trip
is covered by `persistence.rs`'s existing whole-struct `assert_eq!`
tests. `cargo build/clippy/test --workspace` and `cargo fmt --check`
clean; `allium check`/`plan`/`analyse` independently re-run - clean,
402 → 404 obligations with exactly the two expected additions, the same
4 pre-existing `analyse` findings unchanged.

This closes the cross-tenant read gap across all three of skilj's read
surfaces - projections, raw events, and commands - with one consistent
mechanism (`scope` vs. a per-record derived owner, fail-closed on an
unproven owner) applied three times, each adapted to what that surface's
own record shape already provided rather than forcing a single
implementation onto all three.

<a id="admin-read-back-owner-tag-key"></a>
## 26. Closing the admin read-back gap on `owner_tag_key`

[§24](#cross-tenant-read-fix-raw-events)/[§25](#cross-tenant-read-fix-command-query) each flagged the same small completeness gap and left it alone:
`surface TypeRegistration`'s own `exposes:` clause never listed
`event_type.owner_tag_key`/`command_type.owner_tag_key` alongside the
other registered fields it already exposes (`tagMappings`,
`sensitiveFields`, etc.) - an admin could set or clear the declaration
via `registerEventType`/`registerCommandType` but had no way to read
back which key was currently in force. Closed now, on both sides at
once (per [§25](#cross-tenant-read-fix-command-query)'s own note: fixing one side only would have been worse
than neither).

Purely additive, no new mechanism: `exposes:` gained
`event_type.owner_tag_key`/`command_type.owner_tag_key`
(specs/skilj.allium), and `gql_types.rs`'s `event_type_object()`/
`command_type_object()` each gained an `ownerTagKey` scalar field,
reusing the existing `optional_string` helper `RoleAccessMapping.scope`'s
own field already established. No resolver/mutation changes needed - the
value was already being set and stored by the three prior passes, this
just makes it queryable. `allium plan`'s own obligation count is
unchanged (404 → 404): an `exposes:` field addition extends an existing
`surface-exposure` obligation's own scope rather than creating a new one.

**Verified**: `skilj/tests/graphql_type_registration.rs`'s existing
`full_type_registration_lifecycle_end_to_end` test extended - registers
an `EventType` with `ownerTagKey: "account"`, asserts it on both the
mutation's own response and a separate `eventTypes` query afterward (so
the value is proven to round-trip through Postgres, not just echoed back
by the resolver); `commandTypes` gained the same field asserted `null`
for a type that never set one, covering the unset path. `cargo
build/clippy/test --workspace` and `cargo fmt --check` clean; `allium
check`/`plan`/`analyse` independently re-verified - clean, obligation
count unchanged, same 4 pre-existing `analyse` findings.

<a id="cross-tenant-read-fix-snapshot-inspection"></a>
## 27. Cross-tenant read fix, part five: `SnapshotInspection`

A gap the first four passes didn't cover, found by re-checking every
`AdminAccess`-facing single-record read surface for the same shape after
[§26](#admin-read-back-owner-tag-key) closed the read-back gap: `inspectSnapshot`'s resolver
(`skilj-graphql/src/resolvers/snapshot_query.rs`) only ever checked
`require_admin_mapping` - bounded-context-level - then fetched a stored
row by a caller-supplied `tagValue` directly, with no check that the
value belonged to the caller. In skilj-helpdesk's shape, any admin-level
grant could inspect any other company's snapshot by tag value alone.

**Design**: `Snapshot::OWNER_TAG_KEY: Option<&'static str>`
(`skilj-core/src/plugin/mod.rs`) - deliberately Rust-only, the identical
treatment `Snapshot::TAG_KEY` itself already gets (no spec field, no
registration surface: `Snapshot` is compiled, deployed configuration
throughout, unlike `EventType`/`CommandType`'s registered
`owner_tag_key` from [§24](#cross-tenant-read-fix-raw-events)/[§25](#cross-tenant-read-fix-command-query)). Genuinely a separate dimension from
`TAG_KEY`: a snapshot keyed by `"account"` can still need owner-scoping
by `"company"` - `catch_up_snapshots` (`skilj-core/src/db/mod.rs`)
derives each stored row's own `owner` at fold time from whichever tag on
the folding event matches `OWNER_TAG_KEY`, independent of the tag it
already reads for `TAG_KEY`/`tag_value` itself, using the identical
"leave an established owner untouched when a later event lacks the tag"
rule `apply_projection_fold_update` set in [§23](#cross-tenant-projection-read-fix-owner-tag).

Unlike the other four surfaces, `inspectSnapshot` had no pure-function
authorization layer in `skilj-core` at all to extend - its whole check
lived inline in the resolver. Rather than force a new named predicate
into `event_store`/`projections` for a single call site, the comparison
itself is a tiny reusable function,
`access_control::scope_matches_owner(owner, scope) -> bool` - the same
fail-closed contract as `query_projection`'s own inline check, factored
out only because this surface had nowhere else to put it. `db::get_snapshot_state_and_owner`
is a new sibling to `get_snapshot_state`, not a replacement: that
function's other caller, `submit_command`'s snapshot-accelerated
`decide_from_snapshot` path, is a write-path internal accelerator, not a
caller read, and stays untouched - the identical "`ProcessCommand`'s
`matching_events` is deliberately untouched" principle [§24](#cross-tenant-read-fix-raw-events) already
applied to the analogous case on the command side.

Single-record surface, so this rejects rather than filters, the same
shape `inspectEvent`/`QueryProjection` already have - but with one
real wrinkle worth naming: a `scope`-restricted caller querying a
never-touched `tagValue` must **not** get the ordinary "cold, not a
failure" `null` `inspectSnapshot` already answers an unscoped caller
with, because that would let a scoped caller distinguish "nothing
recorded yet" from "something recorded but not mine" - an oracle this
fix exists to close, not preserve by accident. So the resolver checks
`owner_tag_key.is_some() && access_mapping.scope.is_some()` once, up
front, and fails closed on *both* "no row at all" and "a row whose owner
doesn't match" alike whenever that holds - only an unscoped caller, or a
snapshot declaring no owner dimension, still gets the original
null-when-cold behaviour.

**Verified**: `skilj-core/tests/snapshot_owner_scoping.rs` (8 tests, no
new provisioning harness - `TAG_KEY`-vs-`OWNER_TAG_KEY` fold-time
derivation via `catch_up_snapshots`, including a tag key genuinely
different from `TAG_KEY` itself and the "later untagged event doesn't
clear an established owner" case) plus `access_control::scope_matches_owner`
unit-tested in isolation. `skilj/tests/graphql_business_surfaces.rs`
gained a new real end-to-end test and its own self-contained fixture
(`TicketOpened`/`TicketTotalSnapshot`/`OpenTicket`, distinct from the
file's existing `ThingHappened`/`ThingTotalSnapshot` - not modified) -
the concrete cross-tenant scenario over real HTTP: a scoped admin reads
its own company's snapshot, is rejected reading another company's, is
rejected the identical way for a never-touched `tagValue`, and an
unscoped admin remains unrestricted. `cargo build/clippy/test --workspace`
and `cargo fmt --check` clean; `allium check`/`plan`/`analyse`
independently re-verified - clean, obligation count unchanged (this
guarantee is prose, not a `requires` clause, since `OWNER_TAG_KEY` is
Rust-only - enforcement lives entirely in the test suite above, the same
as every guarantee over `Projection`'s own Rust-only declaration
already does), same 4 pre-existing `analyse` findings.

This closes the fifth and, as far as a deliberate re-check of every
`AdminAccess`-facing single-record read surface found, final instance of
the cross-tenant read gap.

<a id="cross-tenant-read-fix-create-bounded-context-template"></a>
## 28. Cross-tenant read fix, part six: `CreateBoundedContextFromTemplate`'s always-unscoped grant

A gap found by re-checking, symmetrically to [§27](#cross-tenant-read-fix-snapshot-inspection)'s sweep, every place a
`RoleAccessMapping` comes into being rather than every place one is read:
`GrantRoleAccessMapping` ([§23](#cross-tenant-projection-read-fix-owner-tag)) lets an admin set `scope` on a grant it
creates, but `CreateBoundedContextFromTemplate`'s own grant to the
tenant's first role - the only *other* rule that creates one - had no
such parameter and always passed `None`. A caller stamping a tenant from
a template had no way to hand that tenant's own first role a scoped
grant at creation time, and no way to fix that afterwards either:
`UniqueActiveAccessPerRoleAndContext` blocks a follow-up
`GrantRoleAccessMapping` call for the same `(role, bounded_context)` pair
while the original grant is still active, so rescoping meant revoking
and re-granting - workable, but not what "the tenant's first grant is
scoped from the moment the tenant exists" should require.

**Design**: identical mechanism to every prior pass - `scope` stays the
unvalidated, opaque, caller-chosen string `RoleAccessMapping.scope`
already is, just accepted here too. `specs/skilj.allium`:
`CreateBoundedContextFromTemplate`'s `when:` gains `scope?`, threaded
onto the `RoleAccessMapping.created(...)` its `ensures:` produces; a
prose note (mirroring `GrantRoleAccessMapping`'s own) explains why it
has to be settable here rather than left to a follow-up grant, for the
`UniqueActiveAccessPerRoleAndContext` reason above; `@guarantee
AccessGrantedWithCreation` gains a clause naming the read scope the
caller chose explicitly, alongside the level and sensitivity it already
named. `skilj-graphql/src/resolvers/bounded_context_templating.rs`:
`finish_creating_tenant()` gains a `scope: Option<String>` parameter,
passed through to its `grant_role_access_mapping()` call instead of a
hardcoded `None`; `create_bounded_context_from_template_field()` parses
an optional `scope: String` GraphQL argument the same way
`grantRoleAccessMapping` already does and threads it through. No
`skilj-core` change at all - `grant_role_access_mapping()` already took
a `scope` parameter from [§23](#cross-tenant-projection-read-fix-owner-tag); this pass only stops one of its two
callers from silently discarding it.

**Verified**: `skilj/tests/bounded_context_templating.rs` gained
`create_bounded_context_from_template_carries_scope_onto_the_initial_grant`,
calling `createBoundedContextFromTemplate` twice against one live
`Skilj` instance - once with `scope: "company-a"`, once with
`scope: null` - asserting the returned grant's `scope` each time.
`cargo build/clippy/test --workspace` and `cargo fmt --check` clean;
`allium check`/`plan`/`analyse` independently re-verified against the
diff - obligation count unchanged (404, same as the last committed
spec: no new `requires` clause, since `scope` stays unvalidated the same
way it already is on `GrantRoleAccessMapping`), same 11
warnings/8 infos/0 findings on `check`, same 4 pre-existing findings on
`analyse`.

<a id="hardening-list-role-access-mappings"></a>
## 29. Hardening: `list_role_access_mappings` no longer panics on a concurrent bounded-context deletion

Found while chasing an intermittent panic that surfaced during [§28](#cross-tenant-read-fix-create-bounded-context-template)'s own
test-suite verification, in a test [§28](#cross-tenant-read-fix-create-bounded-context-template) didn't touch:
`RoleAccessMappingRow::into_domain` (`skilj-core/src/db/mod.rs`) read a
`role_access_mappings` row, then made a *separate* follow-up query to
load the `bounded_contexts` row it names, and `.expect()`-panicked if
that came back empty. `hard_delete_bounded_context`'s `DROP SCHEMA` +
`DELETE` + `ON DELETE CASCADE` guarantees no *committed* state ever has
a `role_access_mappings` row outliving its `bounded_contexts` row - but
those are two unsynchronized queries against the pool, not one snapshot,
so a `hard_delete_bounded_context` committing in the gap between them is
exactly this: a row legitimately read a moment ago, legitimately gone by
the next query. `list_role_access_mappings` in particular reads
*every* mapping in the system regardless of which bounded context a
caller asked about (`load_bounded_context_with_mappings`, its own
resolver-facing wrapper, filters by name only after every row has
already been resolved) - so any admin operation that lists mappings
could be made to panic by an unrelated concurrent tenant deletion
anywhere else in the system, not just its own. This is a real
robustness gap independent of tests: nothing about it requires two
things racing in the same test file, only two things racing in the same
process, and skilj is a library other applications embed and run
concurrent requests against.

**Fix**: `into_domain` returns `Ok(None)` instead of panicking when
either the role or the bounded context it names has vanished by the
time of its own lookup, treating that row as if it hadn't been in the
snapshot to begin with rather than as a corrupt one. Its three callers
(`get_active_role_access_mapping`, `list_role_access_mappings`,
`list_active_role_access_mappings_for_role`) already had the right
shape to absorb this: the first already returns `Option`, so a `None`
row folds in as "no active mapping" with no new branch; the other two
already build a `Vec` in a loop, so they skip a `None` instead of
pushing it.

**Verified**: this was caught and fixed by re-running
`skilj/tests/bounded_context_templating.rs` - the file whose test count
this session's own additions had just grown from 4 to 5 - directly:
50 consecutive default-threaded (parallel) runs clean after the fix,
against an observed ~10-20% failure rate on the same command before it
(both the pre-existing `resync_bounded_context_from_template_pulls_in_a_later_schema_change`
and this pass's own new test were each observed panicking with the
identical signature across different runs, confirming the race was
cross-test, not specific to either). `cargo build/clippy/test
--workspace` and `cargo fmt --check` clean, full workspace suite (92
test binaries/suites) green with no `FAILED`/panicked entries anywhere,
not just the one file.

<a id="cross-tenant-write-fix-owner-tag-scoping"></a>
## 30. Cross-tenant write fix: owner-tag scoping on `SubmitCommand`/`TriggerCommand`/`CreateExternalEvent`/`CreateDirectEvent`

The read-side series ([§23](#cross-tenant-projection-read-fix-owner-tag)-[§29](#hardening-list-role-access-mappings)) closed every instance of "any grant on the
bounded context reads any record in it" it found - but never touched the
mirror-image gap on the *write* side: a `scope`-restricted grant or token
could no longer read another owner's records after those passes, yet
could still blindly submit a command or create an event that mutated
one, as long as it knew or could guess the record's own identifying
tags. In skilj-helpdesk terms, a company-scoped `WriteAccess` role could
no longer read company B's tickets, but could still open or modify one
by command - a caller-visible IDOR on write, arguably worse than the
original read one since it requires no read access at all.

**Design**: the identical mechanism as every prior pass, applied to the
four surfaces that create a record rather than read one -
`SubmitCommand` (GraphQL, `RoleAccessMapping`), `TriggerCommand` (REST,
`CommandToken`), `SubmitExternalEvent`/`SubmitDirectEvent` (REST,
`ExternalEventToken`/`DirectCreationToken`). A new shared predicate,
`event_store::tag_owner_scope_satisfied(tags, owner_tag_key, scope) ->
bool` (`skilj-core/src/event_store/mod.rs`) - the same three-way
fail-closed logic `event_owner_scope_satisfied`/`command_owner_scope_satisfied`
already have (both now delegate to it), but taking the derived `tags`
and the type's own `owner_tag_key` as plain values rather than reading
them off an already-materialized `Event`/`Command`, since none of these
four call sites have one yet - the record does not exist until after
the check passes. Named `tag_owner_scope_satisfied` in the spec (not
`owner_scope_satisfied`, already taken by `QueryProjection`'s
unrelated, differently-shaped `owner_scope_satisfied(projection, key,
access_mapping)` predicate - a real naming collision the `allium:tend`
agent building this pass's spec diff caught and avoided).

`authorise_command_submission`/`authorise_command_trigger` each gained a
`let consistency_tags = derive_tags(...)` (previously computed only by
the *caller*, after authorisation returned) plus the new check, and
`CommandAuthorised` gained a Rust-only `consistency_tags: Vec<Tag>`
field so the value is computed once and reused, not recomputed by every
caller as before - a small efficiency side-effect of the fix, not a
separate change. `create_external_event`/`create_direct_event` gained
the identical check just before constructing the `Event`, reusing a new
`let tags = derive_tags(...)` binding their own `ensures`-equivalent
construction already needed. Single-record surfaces throughout, so every
one of the four rejects outright on a mismatch (`Error::GrantScopeMismatch`,
reused unchanged) rather than filtering - there is exactly one record
being authorised at each of these call sites, never a range.

`CommandToken`/`ExternalEventToken`/`DirectCreationToken` each gained a
`scope: Option<String>` field, minted the same way
`EventReadToken.scope` already is: an independent, unvalidated,
caller-chosen string, set by whichever admin mints the token and
unrelated to that admin's own `access_mapping.scope` (an unscoped staff
admin may mint a scoped token - see `create_event_read_token`'s own doc
comment, now shared by all four minting functions rather than
`EventReadToken`'s alone). The shared `access_tokens.scope` Postgres
column (added in [§24](#cross-tenant-read-fix-raw-events) for `EventReadToken` alone) already existed for
every token kind; this pass just started writing and reading it for the
other three (`insert_command_token`'s own hand-written `INSERT` needed a
literal new `scope` column added - the three `insert_access_token_row`-backed
kinds only needed their callers to stop passing `None` unconditionally).

**A real bug found and fixed along the way, independent of the owner-tag
mechanism itself**: `skilj-rest`'s `status_for` HTTP-status table
(`skilj-rest/src/error.rs`) had no entry for `Error::GrantScopeMismatch`
at all, so it fell through to the wildcard `_ => StatusCode::INTERNAL_SERVER_ERROR`
- a 500 instead of the 403 every other authorisation rejection in that
table gets. This was latent, not yet caller-visible, because the
variant was previously only ever *filtered* on the REST track
(`fetch_events`/`consume_events`), never actually raised as an error -
this pass's `authorise_command_trigger`/`create_external_event`/
`create_direct_event` are the first REST-reachable sources that reject
outright with it. Fixed by adding the missing match arm; decisively
verified by reverting the fix and confirming
`command_trigger_rejects_a_command_whose_owner_does_not_match_the_tokens_scope`
actually fails with 500 before restoring it.

**A second gap found while writing this pass's own GraphQL test**:
`EventReadToken.scope` ([§24](#cross-tenant-read-fix-raw-events)) was never actually exposed on the GraphQL
wire at all - `createEventReadToken`'s own mutation accepted and
persisted the argument, but the returned `EventReadToken` object had no
`scope` field to read it back, the identical class of admin read-back
gap [§26](#admin-read-back-owner-tag-key) closed for `owner_tag_key`. Fixed in the one place all four
token GraphQL objects are built (`gql_types.rs`'s shared `token_object!`
macro) rather than four separate patches, closing it for
`EventReadToken` retroactively and for the three new token kinds at
once.

**Mechanical**: `create_type_token_field!` (`skilj-graphql/src/resolvers/mod.rs`),
the macro backing three of the four `create*Token` GraphQL mutations,
gained `scope` argument parsing and threading, uniform across all four
now that every `create_*_token` function takes the identical parameter
- `createEventReadToken` (`event_type_admin_operations.rs`), hand-written
since [§24](#cross-tenant-read-fix-raw-events) specifically because it alone needed this argument, folded
back into the macro now that the asymmetry that justified the exception
is gone. `db::get_event_type_access_token!` (`skilj-core/src/db/mod.rs`)
gained the same third invocation for the identical reason, on the read
side.

**Verified**: `skilj-core/tests/command_processing.rs`/`event_creation_surfaces.rs`
gained pure unit tests for all four rules' new obligations (mismatch
rejects, a matching owner still succeeds, an untagged payload fails
closed the same as a real mismatch, an unscoped grant/token stays fully
unrestricted) - `command_processing.rs`'s own obligation count moved
27→29, `event_creation_surfaces.rs`'s 22→24, both exactly the +2 each
`allium plan`'s own new `rule-failure.*.6` obligations predict.
`skilj-core/tests/persistence.rs` gained a new `round_trips_a_command_token`
test (no prior round-trip test existed for `CommandToken` at all) and
upgraded `round_trips_an_external_event_token_and_its_kind` to a real
non-null scope value, proving the shared column round-trips for every
kind now, not just `event_read`. Two new real end-to-end tests over
actual HTTP (`skilj/tests/command_trigger.rs`,
`skilj/tests/payload_validation.rs`) mint a second, differently-scoped
token against a live `Skilj` instance and prove the rejection is real
over the wire, with the right status code - not just at the
pure-function layer. `skilj/tests/graphql_type_registration.rs`'s
existing lifecycle test now passes a real `scope` to
`createExternalEventToken` and asserts it reads back unchanged.
`cargo build/clippy/test --workspace` and `cargo fmt --check` clean (92
test binaries/suites, 3 consecutive full-workspace runs with zero
failures); `allium check`/`plan`/`analyse` independently re-verified
against the diff myself, not taken on the `allium:tend` agent's own
report alone - `check` byte-identical to baseline (11 warnings/8
infos/0 findings), `plan` obligation count 404→408 (exactly the four
new `requires` clauses, nothing else), `analyse` the same 4 pre-existing
findings, no new ones.

This closes the write-side mirror of the entire cross-tenant read-fix
series - between this pass and [§23](#cross-tenant-projection-read-fix-owner-tag)-[§29](#hardening-list-role-access-mappings), every surface that either reads
or creates a record now respects the same owner-tag scoping, read and
write alike.

<a id="private-fields-third-protection"></a>
## 31. Private fields: a third field-level protection, alongside `sensitive_fields` and the owner-tag `scope` series

A genuinely new mechanism, not a bugfix - user-initiated, prompted by a
Codeberg issue proposing a whole-*projection* staff-only gate
(`Projection::PRIVATE`/`can_read_private`, a single bounded-context-wide
boolean) that turned out, on inspection, to be one instance of a broader
and more useful primitive: a field visible by default only to a
*specific* default reader determined per record, with that reader able
to *share* it further. `sensitive_fields` (crypto-shredding-shaped PII
protection, subject named by the payload, real `EncryptionKey`s, GDPR
erasure) and the owner-tag `scope` series ([§23](#cross-tenant-projection-read-fix-owner-tag)-30, cross-tenant/company
scoping) both already exist; neither fits "my own note, visible to me
alone until I choose otherwise."

**Three kinds**, each a different default reader, decidable from the
record and the reading caller's own identity alone - no projection, no
other record, no clock:

- **`own`** - visible by default only to whoever created the record
  (`Event.metadata.client_id`/`Command.metadata.client_id`, the Role's
  own internal id for anything GraphQL-originated). Shareable: the
  creator may grant another Role read access, to one specific record or
  to every record of its own, present and future, and revoke that again.
- **`team`** - visible to any Role whose own `name` equals a fixed
  string the field declares. Nothing is granted and nothing needs to be
  - membership is the grant, instantly and automatically, and it stops
    the moment the Role is no longer named that. The originally-proposed
  "staff-only projection" is one instance of this (`team: "staff"`),
  not a mechanism of its own - the issue's own proposal is absorbed here
  rather than built separately.
- **`addressed`** - visible by default to the one other party the
  payload itself names, read from a second payload field and matched
  against the reader's own `Role.external_subject` - the identical
  match a sensitive field's subject already gets, minus the encryption:
  "who this is for," not "whose PII this is."

A fourth kind, `draft` - visible only until some external status
changes - is a real, deliberately deferred future kind, not one
considered and rejected: it needs a live `Projection` lookup at read
time, a materially different evaluability shape from the three built
here, with its own open questions about staleness. Nothing here forecloses
it; nothing here is shaped around it either.

**Design decision, made explicit early and load-bearing throughout**: no
encryption, anywhere. `sensitive_fields` earns its `EncryptionKey`/
ciphertext-at-rest weight from GDPR erasure - none of these three kinds
need to survive a legal erasure request or protect against a raw
database compromise, only express an ordinary application-level
visibility rule. Reaching for real crypto to do that would mean
provisioning master-key infrastructure to solve a problem that has
nothing to do with encryption - the same reasoning the original issue's
own proposal already gave for not reusing `sensitive_fields`, generalised
to all three kinds rather than the one it was written about. A private
field is stored in plaintext exactly as written; protection is a
read-time redaction to `null`, the identical place and shape an
unentitled caller already finds a *sensitive* field's leaf left as
stored ciphertext - never the enclosing object, never the whole record
withheld.

**Entities and fields** (`specs/skilj.allium`, `skilj-core/src/shared/mod.rs`):
`value PrivateField { field, kind: PrivateFieldKind, team: String?,
addressee_field: String? }` and `enum PrivateFieldKind { own | team |
addressed }`; `EventType`/`CommandType` each gain `private_fields:
Set<PrivateField>` alongside `sensitive_fields`, opt-in per field.
`RegisterEventType`/`RegisterCommandType` gained a `private_fields`
parameter, a new `valid_private_fields` validation black box (schema-leaf
checks plus the kind-specific field-presence rule -
`team`/`addressee_field` set exactly when `kind` calls for them), and two
new overlap `requires` clauses - a private field may not also be a
`tag_mappings` key or a `sensitive_fields` entry, the identical leak
`SensitiveFieldTagOverlap` already guards against (a tag or ciphertext
leaf carries no per-field redaction, so an overlapping field's plaintext
would leak through regardless of what `private_fields` says).

**Sharing - the entity the mechanism actually needs, and only one**:
`entity PrivateFieldGrant { bounded_context, grantor: Role, grantee:
Role, event: Event?, command: Command?, status, created_at, revoked_at }`
- at most one of `event`/`command` ever set (both null is a *blanket*
grant: every record of any type the grantor is itself the default
reader of, present and future). Only `own`/`addressed` ever produce one;
`team` has nothing to share, a Role either carries the name or it does
not. Self-service throughout - the defining trait separating this from
every other grant in the codebase: no admin, no superadmin, anywhere.
Every other grant here hands out a capability the recipient never had
(a superadmin for `RoleAccessMapping`, an admin for an `AccessToken`); a
private-field grant hands out nothing the grantor did not already hold
itself, so requiring an admin would only turn "share my own note with a
colleague" into an administrative ticket. `grant_private_field_access_for_event`/
`_for_command` (two minting rules, the same `CreateExternalEvent`/
`CreateDirectEvent` paired-rule shape, not one polymorphic rule) check
the grantor is genuinely the named record's own default reader via a new
shared predicate, `is_default_private_reader(record, role)` - generic
over `Event`/`Command` via a small `PrivateFieldRecord` trait
(`skilj-core/src/event_store/mod.rs`), one predicate reused at grant
time and render time alike, never two implementations of the same
question. A blanket grant cannot be checked against records that do not
exist yet, so it is trusted when made and *re-derived per record at
every read* instead - what stops it being a wider trust boundary than
the per-record form: a reader coming through a blanket grant sees a
field only where the grantor would itself have been that exact record's
default reader. `revoke_private_field_access` - only the grantor may end
it, not the grantee, not an admin. `list_private_field_grants` - a
caller's own outgoing grants need no particular level; naming a
*different* grantor needs `access_mapping.level = admin`, the
compliance/oversight half. Revoked grants are listed too, not filtered
out - "what have I shared, and what have I stopped sharing" needs both
answers.

Rust/DB elaboration beyond the spec's own entity shape, both
deliberate: `PrivateFieldGrant.id` is a synthetic id the spec doesn't
name (needed because, unlike `RoleAccessMapping`, there is no "at most
one active" uniqueness constraint here - a grantor may re-grant after
revoking); `event_sequence: Option<i64>`/`command_id: Option<String>`
stand in for the spec's own `event: Event?`/`command: Command?` - every
real use of the record a per-record grant names is an identity
comparison alone, never any of that record's own fields, so carrying
the identity is exactly as capable as embedding the whole value and
needs no extra load to reconstruct one at read time.

**Render-time integration**: `render_event`/`render_command` each gained
a `grants: &[PrivateFieldGrant]` parameter and a third redaction pass
after the existing sensitive-field one - per `PrivateField` entry,
`Team`-kind checks `access_mapping.role.name` directly; `Own`/`Addressed`
check `is_default_private_reader` first, then any active grant naming
this exact record or (re-derived) a matching blanket one. Every call
site that already threaded `access_mapping ` through now also loads and
threads a `Vec<PrivateFieldGrant>` snapshot - `query_events`/
`inspect_event`/`fetch_commands`/`deliver_to_subscriptions`'s own
resolvers, each a small addition since `render_event`/`render_command`
already filter the passed-in list down to `grantee = access_mapping.role`
internally, so one shared snapshot serves every caller (and, for
subscriptions, every differently-entitled subscriber) in one request
without a per-caller query.

**The REST track's own gap, found and closed in the same pass**: a
private field is stored in plaintext, so `FetchEvents`/`ConsumeEvents`
- which are "safe by construction" for *sensitive* fields only because
those are ciphertext at rest - would otherwise hand a private field's
real value to any `EventReadToken` holder outright. New black box
`redact_private_fields(record)`, applied unconditionally in both rules:
no `access_mapping`/`Role` argument at all, deliberately, since there is
none to condition it on - an `EventReadToken` is tied to no `Role`, so
all three kinds fail closed *structurally*, not by omission (nobody's
`client_id`, no `Role.name`, no `external_subject`, and no
`PrivateFieldGrant` can name a bearer token as a grantee). Declaring
`private_fields` on a REST-readable event type is therefore safe rather
than merely permitted. Applied at the delivery site only, never to the
served set itself - `ConsumeEvents`' own cursor still advances over the
*unredacted* set, so what a caller is allowed to see never influences
where the cursor sits.

**A real, pre-existing admin read-back gap, found and closed for all
four token kinds at once**: while wiring `PrivateFieldGrant`'s own
GraphQL object, `EventReadToken.scope` ([§24](#cross-tenant-read-fix-raw-events)) turned out never to have
been exposed on the wire at all - `createEventReadToken`'s own mutation
accepted and persisted the argument, but the returned object had no
`scope` field to read it back, the identical class of gap [§26](#admin-read-back-owner-tag-key) already
closed for `owner_tag_key`. Fixed once, in the one place all four token
GraphQL objects are built (`gql_types.rs`'s shared `token_object!`
macro), closing it for `EventReadToken` retroactively and for
`ExternalEventToken`/`DirectCreationToken`/`CommandToken` at once -
these three needed the field added regardless, for their own new
`scope` ([§30](#cross-tenant-write-fix-owner-tag-scoping)).

**Mechanical**: `EventType.private_fields`/`CommandType.private_fields`
persisted as `JSONB`, the identical `Json<Vec<T>>` treatment
`sensitive_fields`/`tag_mappings` already get; a new
`private_field_grants` table per bounded context (`id`,
`grantor_role_id`/`grantee_role_id` - cross-schema `REFERENCES
public.roles (id)`, freely allowed in Postgres - `event_sequence`,
`command_id` referencing `commands`' own internal `BIGSERIAL`, `status`,
`created_at`, `revoked_at`), provisioned for a new bounded context and
patched into an existing one at every `build()`, the same
`ensure_*_table`/`ensure_*_columns` idempotent-migration pattern every
prior pass's own schema addition already uses. `PrivateFieldGrantRow::
into_domain` returns `Ok(None)` rather than panicking when a referenced
`Role` is gone by the time it's looked up - proactively applying [§29](#hardening-list-role-access-mappings)'s
own hardening pattern to this new row type rather than waiting to
rediscover the same race. `registerEventType`/`registerCommandType`
gained a required `privateFields: [PrivateFieldInput!]!` GraphQL
argument (mirroring `sensitiveFields`'s own shape); the shared
`create_type_token_field!` macro backing all four `create*Token`
mutations already needed no change for this pass, but `EventType`/
`CommandType`'s own construction sites across the whole workspace -
tests, `skilj-demo`, the plugin trait's own default methods - needed the
usual mechanical sweep, the same shape [§23](#cross-tenant-projection-read-fix-owner-tag)'s own ~40-call-site pass
already established.

**Verified**: a new `skilj-core/tests/private_field_visibility.rs` (47
pure tests, no Postgres) covers every kind's default-reader logic in
isolation, `render_event`/`render_command`'s full redaction pass
(per-record grant, blanket grant re-derivation, a blanket grant never
reaching past its own grantor's standing, revocation), `redact_private_fields`'s
unconditional REST-track behaviour, and the grant/revoke/list functions'
own authorisation rejections (`NotDefaultPrivateReader`, `NotGrantor`,
`PrivateFieldGrantNotActive`, `PrivateFieldRecordWrongBoundedContext`,
the admin-vs-self listing split). A new real end-to-end test in
`skilj/tests/graphql_business_surfaces.rs`, over actual HTTP and
Postgres, with two independently-authenticated Roles (real signed
JWTs): a creator submits a command producing an `own`-kind private
field, a colleague reads it back redacted to `null`, the creator grants
access, the colleague now reads it in full, the creator revokes it, the
colleague is redacted again, and `listPrivateFieldGrants` reads both
the creator's own view and (as the colleague, admin-level, naming the
creator explicitly) the same grant's history back correctly. `cargo
build/clippy/test --workspace` and `cargo fmt --check` clean (93 test
binaries/suites, full run green); `allium check`/`plan`/`analyse`
independently re-verified against the diff, not taken on the
`allium:tend` agent's own two reports alone - `check` 13
warnings/8 infos/0 findings (2 new warnings, both the same pre-existing
checker name-resolution artefact [§16](#declarative-bounded-context-codegen-prototype)'s own `AccessToken`/
`RoleAccessMapping` findings already have, confirmed by testing the fix
that clears them and choosing not to take it - see the spec's own
`entity PrivateFieldGrant` comment), `plan` obligation count 448 (from
408 before this pass), `analyse` 5 findings (4 pre-existing plus the
identical one new artefact, no others).

**A real bug found and fixed mid-pass, independent of the mechanism
itself**: two hand-rolled Python sweep scripts used to thread the new
`private_fields`/`&[PrivateFieldGrant]` arguments through several dozen
existing call sites corrupted a handful of them - an off-by-one byte
offset in one script silently reordered adjacent arguments in test
calls carrying scheduling booleans, and a second script's blind
trailing-comma insertion produced outright invalid syntax in one more.
Both were caught before landing: the first by an independent audit
script diffing every touched call's own argument list against `git
show HEAD` (not by trusting either sweep script's own success), the
second by the compiler itself. All affected files were reverted and
re-swept with a corrected, insertion-only (never whole-span-replacing)
script, then re-audited clean.

<a id="projection-query-team-gate"></a>
## 32. Closing `ProjectionQuery`'s own team gate (Codeberg issue #17)

A real, pre-existing gap left by [§31](#private-fields-third-protection) rather than a new proposal: that
pass's `team`-kind private field redacts one field of one raw
event/command to any Role not carrying the required name, protecting
`queryEvents`/`fetchCommands` - surfaces no `Write`-level Role can reach
anyway, since both need read-level access at minimum. `ProjectionQuery`,
the one surface a `Write`-level Role *can* reach, was never touched by
that mechanism at all: nothing stopped a Role of any name from reading
any projection instance its `RoleAccessMapping` otherwise covered. The
original Codeberg issue that prompted [§31](#private-fields-third-protection) - a whole-*projection*
staff-only gate - was absorbed into the `team` private-field kind on the
understanding that it covered every reader-facing surface; issue #17 is
the finding that it didn't.

**The fix, in kind rather than in shape**: a whole-*projection* gate,
not a private field of any one record - a stored projection instance has
no single record each field individually traces back to (it is folded
from possibly many events by `project()`), so nothing short of gating
the whole instance generalises the way a private field does. New
`Projection::TEAM_ONLY: Option<&'static str>` (`skilj-core/src/plugin/mod.rs`),
identical treatment to `OWNER_TAG_KEY` right above it: Rust-only, no
field on the spec's own `entity Projection`, no registration argument,
nothing an admin can read back - compiled and deployed configuration
only. `ProjectionDispatcher` gained a matching `team_only` method
(`skilj`'s own `ProjectionDispatcherImpl`, the same `Option<Option<_>>`
shape `owner_tag_key` already returns), and `projections::query_projection`
gained a `team_only: Option<&str>` parameter and one new check: when
`Some`, reject unless `access_mapping.role.name` equals it exactly. New
`access_control::Error::NotOnRequiredTeam`.

**Independent of, not a refinement of, the existing owner-scope check**:
a projection may declare both `OWNER_TAG_KEY` and `TEAM_ONLY` (company-
scoped *and* staff-only), and a query must satisfy both - the two checks
run one after the other in `query_projection`, each failing on its own
terms (`GrantScopeMismatch` vs. `NotOnRequiredTeam`), neither able to
compensate for the other. No superadmin bypass, deliberately: unlike
`scope`, which a superadmin grant simply never carries, team membership
is decided by `Role.name` equality alone, an orthogonal axis - the same
"vacuously true unless a projection names one" framing `owner_scope_satisfied`
already gets, not a privilege escalation lever.

**Spec**: `rule QueryProjection` gained `requires:
team_only_satisfied(projection, access_mapping)` alongside the existing
`owner_scope_satisfied` clause, and `surface ProjectionQuery` gained
`@guarantee TeamGatedWhenDeclared`, stated in the identical register
`GrantScopedToOwnerWhenDeclared` already uses - to a Role named anything
else the projection is invisible, not merely unreadable, the same
framing a cross-bounded-context query already gets.

**Wiring**: `skilj-graphql`'s `projection_query` resolver resolves
`team_only` from the dispatcher alongside its existing `owner_tag_key`
lookup and threads it through unchanged. `NotOnRequiredTeam` needed no
`skilj-rest::error::status_for` arm - `ProjectionQuery` is GraphQL-only
(confirmed in `status_for`'s own `CoreError::Projections(_)` comment),
so `to_graphql_error`'s generic `code()`/`message()` rendering is the
only path this error ever takes.

**Verified**: two new tests in `skilj-core/tests/projection_owner_scoping.rs`,
against real Postgres - a `StaffTicketSummary` projection declaring both
`TEAM_ONLY = Some("staff")` and `OWNER_TAG_KEY = Some("company")`
confirms a non-staff Role is rejected regardless of scope, and that the
owner-scope and team checks fail independently of one another (a staff
Role scoped to the wrong company still gets `GrantScopeMismatch`; a
correctly-scoped non-staff Role still gets `NotOnRequiredTeam`). Every
other `ProjectionDispatcher` test double across the workspace (`skilj-core`'s
`sync_projections.rs`/`async_projections.rs`/`submit_command.rs`/
`metrics_instrumentation.rs`, `skilj-rest`'s `metrics_middleware.rs`/
`tracing_middleware.rs`, `skilj-inspector`'s `data.rs`) picked up the new
trait method; `skilj-core/tests/projection_query.rs`'s dozen pre-existing
`query_projection` call sites picked up the new parameter. `cargo
build/clippy/test --workspace` and `cargo fmt --check` clean, real
Postgres throughout (not skipped - see `CONTRIBUTING.md`'s note on the
embedded-Postgres/libxml2 workaround this environment needed); `allium check` 13
warnings/8 infos/0 findings, `analyse` 5 findings, both identical to
[§31](#private-fields-third-protection)'s own baseline (no new drift); `plan` obligation count 449 (from 448
before this pass, the one new `requires` clause).

**Addendum, found immediately after committing the above**: the
`projection` field's own `TEAM_ONLY` check has a sibling on the same
GraphQL surface - `projectionSchema`, which returns a projection's
declared name/schema/schemaVersion without touching any stored instance
at all. It was never wired to `team_only` in the pass above, so a Role
off the required team could still learn a `TEAM_ONLY` projection's shape
through `projectionSchema` even though `projection` correctly refused
its data - a real gap in `TeamGatedWhenDeclared`'s own "invisible, not
merely unreadable" promise, on the very surface [§32](#projection-query-team-gate) exists to close, not
a new proposal. Fixed the same way: `schema_field()`'s resolver now
resolves `team_only` from the dispatcher and rejects with
`NotOnRequiredTeam` before calling `get_projection`, identical to
`field()`'s own check (and reordered ahead of `get_projection` there too,
so a rejected caller no longer pays for the DB round trip, `waitForSequence`
poll, or a sensitive-field decrypt first).

Also consolidated the equality test itself: `PrivateFieldKind::Team`'s
own check ([§31](#private-fields-third-protection)) and this one were the identical `role.name == required`
comparison, arrived at independently in two different passes with no
shared definition. New `access_control::role_matches_required_team(role,
required)` - `true` when `required` is `None`, `role.name == name`
otherwise - is now the one place either call site tests it, and
`query_projection`'s three/four related parameters
(`declares_owner`/`instance_owner`/`team_only`) moved into one
`ProjectionAccessScope` struct so they can't be silently transposed at a
call site the way same-typed positional arguments could. `ProjectionDispatcher::
team_only` also gained a default (`None`) matching `owner_tag_key`'s own
already-defaulted sibling shape, since every test-double implementation
across the workspace overrode it with the identical `None` body anyway.

**Verified**: new `skilj/tests/projection_query.rs` end-to-end test
(`team_only_projection_gates_both_projection_and_projection_schema_end_to_end`)
drives both `projection` and `projectionSchema` over real HTTP against a
`TEAM_ONLY`-declaring projection, confirming a same-team Role reads both
fields and an off-team Role is rejected on both with `not_on_required_team`
- the second assertion is the one that would have failed before this
addendum. `cargo build/clippy -D warnings/test --workspace` and `cargo
fmt --check` clean, real Postgres throughout; `allium check`/`plan`
unchanged (spec untouched - `team_only_satisfied` already covers
`ProjectionQuery` surface-agnostically, so no new `requires` clause was
needed to close a gap that was purely in the Rust wiring).

<a id="payload-upcasting"></a>
## 33. Payload upcasting (Codeberg issue #14): every option, and the one built

Issue #14 names a real trade-off and deliberately doesn't resolve it:
`schema_is_backwards_compatible` (§ note above `entity CommandType` in
specs/skilj.allium) enforces additive-only evolution - a field can be
added or loosened, never removed, tightened or retyped - and forbids
genuine reshapes (rename a field, change its type, split or merge
fields, restructure a nested shape) as a revision of an existing type.
Most event-sourcing frameworks answer this with upcasting: a versioned
transform chain applied at read time. The issue's own "why this isn't a
clear yes" is real too - upcaster chains accumulate indefinitely in
practice and are rarely retired. This section is the investigation the
issue asked for, closing with "stay additive-only, but make the existing
escape hatch easier to use" rather than building first-class upcasting.

**The scenario that motivates it**: `MoneyDeposited { amount: f64 }`
where `amount` was dollars, and eighteen months later you want
`amount_cents: i64` - same concept, different name, different type.
Not addable-alongside in any painless way, and the textbook case
additive-only categorically can't do.

**Grounding fact, already settled by the spec** (specs/skilj.allium,
the note above `entity CommandType`'s payload-schema-shape section):
because a type's current schema is always a superset of every schema it
has ever had, `Metadata.version` - the schema_version a payload was
actually written under - is stamped "for audit and provenance... not a
key a reader resolves to something before it can interpret the
payload." Nothing in the spec reads it back to validate against, and
nothing needs to, for any payload that only ever grew additively. It
does, however, sit right there on every stored `Event` a hand-written
`BoundedContextEvent::try_from_event` already receives in full - which
is what makes option 5 below possible with zero framework changes.

**Options with the current implementation, no framework changes
needed:**

1. **A new event type name** (`MoneyDeposited2`, say). A first-time
   registration, so no backward-compatibility check applies to it at
   all - it can have any shape. `decide()`/`project()` match arms handle
   both variants forever, each interpreting its own shape. This is the
   escape hatch `schema_is_backwards_compatible`'s own spec commentary
   implies, and the one this issue's own reporter proposed instead of
   building anything.
2. **An additive shadow field, normalized on read.** Add
   `amount_cents: Option<i64>` to the *same* type (legal - a new
   optional field), have new commands populate it, and have
   `decide()`/`project()` derive it from `amount` when `None`. No new
   type, but every reader carries the derivation branch forever - a
   real cost, arguably worse long-term than option 1 since it's silent
   per-read logic rather than a visible type distinction in the wire
   schema.
3. **Custom `Deserialize`, entirely inside serde.**
   `schema_is_backwards_compatible` only ever compares two JSON Schema
   *documents* `schemars` derives from `Payload`'s field names/types; it
   never looks at how `Payload` actually deserializes. `#[serde(alias =
   "amount")]`, `#[serde(default)]`, or a hand-written
   `deserialize_with` can absorb a rename or an old/new shape switch
   inside one Rust struct, with zero schema registration change - as
   long as what schemars derives from the struct still only grows.
   Covers renames and defaulting cleanly; doesn't cover a genuine type
   change (string -> int), since that changes the derived schema.
4. **Reshape inside `project()`, not the event.** `Projection::State` is
   a completely separate Rust type from `Payload` - `project()` is
   already a free-form fold function with no obligation to mirror the
   event's own shape. A projection can normalize old and new event
   shapes into one canonical `State` today; this was always the one
   surface meant to look however it wants to.
5. **Branch on `event.metadata.version` inside a hand-written
   `try_from_event`.** The strongest of the five: `try_from_event`
   already receives the *whole* `Event`, `metadata.version` included, so
   `match event.metadata.version { 1 => old_shape_transform(&event.payload), _ => serde_json::from_str(&event.payload) }`
   works today, no registration or storage change needed. This is the
   one option this section turns into real, tested sugar - see below.

**Heavier tiers, evaluated and declined:**

- **Sugar around option 5** (a declared chain instead of hand-written
  `match` boilerplate) - genuinely worth building, since it costs
  nothing framework-side and removes real per-type repetition. Built;
  see below.
- **Real first-class upcasting** - relax
  `schema_is_backwards_compatible` to accept a breaking change only
  alongside a required `migrate(old_payload) -> new_payload` function,
  applied lazily at read time. This is the feature the issue actually
  names, and it reintroduces exactly the "upcasters accumulate forever,
  rarely retired" cost the issue itself warns about, for no scenario
  options 1-5 don't already cover. It would also break the spec's own
  clean argument that no historical schema text needs to be kept - a
  registered migration function needs to know what it's migrating
  *from*, which means keeping old schema/struct definitions around
  indefinitely, the exact accumulation the issue is skeptical of.
  Declined: no code, no spec change.

**What got built**: `plugin::upcast_payload<T>(payload: &str,
written_at_version: i64, chain: &[UpcastStep])` and `UpcastStep {
to_version: i64, transform: fn(serde_json::Value) -> serde_json::Value
}` (`skilj-core/src/plugin/mod.rs`, next to `BoundedContextEvent` since
it exists to be called from inside `try_from_event`). Deserializes
`payload` to a `serde_json::Value` first, walks `chain` in the order
given applying every step whose `to_version` is strictly greater than
`written_at_version`, then deserializes the result into `T`. A payload
written at version 1 against a `[to 2, to 3]` chain runs both steps; one
already at version 3 runs neither. Steps are trusted to be given in
ascending `to_version` order - not sorted, the same "caller's own
responsibility" register `consumed_event_types()` already carries for
its own list.

Deliberately a plain generic function plus a plain struct, not a macro:
the boilerplate this removes is a `match` arm's worth of branching, not
a whole trait impl the way `#[auto_register]`/`gql_object!` justify
their own existence (docs/architecture.md §1.3.3/§1.3.1) - a function
call already reads as tersely as a macro invocation would here, so
there was nothing a macro would have bought.

**Verified**: `skilj-core/tests/payload_upcasting.rs`, 9 pure unit
tests, no Postgres involved (the same "pure logic, no DB fixture
needed" register `type_registration.rs`'s neighbouring
`schema_is_backwards_compatible` tests already use, just in their own
file since this isn't a `RegisterEventType`/`RegisterCommandType`
obligation) - a single-step chain applied/skipped/skipped-entirely by
version, an empty chain reducing to a plain deserialize, malformed JSON
and a still-unsatisfied target shape both surfacing as ordinary
`serde_json::Error`s, and a two-step chain confirming each version runs
exactly its own remaining suffix of steps, in order. `cargo
build/clippy -D warnings/test --workspace` and `cargo fmt --check`
clean. No spec change - `upcast_payload` is pure application-facing
Rust with no registration, storage, or wire surface of its own to
obligate.

<a id="skilj-temporal-plan"></a>
## 34. `skilj-temporal`: a plan, partially built (long-running/cross-system processes)

Investigated whether skilj should grow something like AxonIQ Framework's
new "Workflows" feature - a durable-execution engine for long-running,
multi-step business processes that survive crashes and resume exactly
where they left off (steps recorded as events; imperative top-down code
in place of the classic scattered-event-handler Saga). The motivating
gap is real: `decide()`'s "synchronous, no I/O" rule (§1.1) means DCB
(the thing that already replaces a saga/process manager for
same-transaction, same-database invariants - `skilj-demo/src/courses.rs`'s
own worked example) has nothing to say about a process that spans real
I/O, real time, or another system entirely - reserve inventory, call an
external payment gateway, wait up to 24h for a webhook, ship or
compensate.

**Decided against building a first-class `Process`/`Saga` trait inside
skilj-core.** It would tie two things together that shouldn't be tied:
skilj's job is being a correct, auditable event store with an atomic
effect boundary; a process's job is tracking multi-step state, retries,
and durable waits. Temporal already does the second job well, and
skilj already has the one piece a Temporal integration actually needs -
idempotent command submission (Codeberg issue #12, [§21](#optional-idempotency-key-submission)). The plan below
is a thin bridge between two systems that each keep their own history,
correlated by convention, not a shared abstraction.

**The one already-perfect fit**: Temporal's own documented idempotency
guidance is to derive an Activity's idempotency key from `Workflow Run
ID + Activity ID` - stable across retries, unique per invocation. Run
ID, deliberately, not Workflow ID alone: a Workflow ID can outlive more
than one Run (continue-as-new, a reset), and an Activity ID is only
unique *within* one run, so Workflow ID alone would let two different
runs that reuse the same Activity ID naming collide. That derivation is
a direct, drop-in match for skilj's existing `Idempotency-Key` header on
`submitCommand`. A Temporal Activity that calls skilj passes
`"{run_id}:{activity_id}"` as the key, and Temporal's own at-least-once
Activity retry becomes safe for free, using infrastructure skilj
already shipped. Zero new skilj-core code needed for this half -
documentation and a worked example only. (Phase 2/3's own
`workflow_id`-based correlation convention below is a related but
distinct concern - *which running execution to signal or start*, not
*which retry this is* - and Workflow ID is the right key for that one,
since Temporal guarantees at most one open run per Workflow ID at a
time.)

**Plan, in phases:**

1. **Command-side (docs + example, no new crate) - built.**
   `docs/temporal-integration.md` documents the
   `"{run_id}:{activity_id}"` -> `Idempotency-Key` pattern;
   `skilj/tests/temporal_activity_idempotency_example.rs` is the
   runnable proof - a simulated Temporal Activity retry (identical Run
   ID and Activity ID) is deduplicated with no double-apply, a different
   Activity ID in the same run is not coalesced, and the same Activity
   ID in two different runs is not coalesced either (the reason Run ID,
   not just Workflow ID, is part of the key). Real Postgres, no Temporal
   dependency - the test composes the header exactly as an Activity
   implementation in any language would, over the real REST surface.

2. **`skilj-temporal`, a new workspace crate - built**, for the two
   directions that do need reusable glue - reading skilj's own event
   stream and calling Temporal's client API:
   - **Event-to-signal**: a configured `EventTypeMapping` names which
     `EventType`s become a Temporal `SignalWorkflowExecution` call
     (signal name + which one of the event's own `tags` supplies the
     correlation value) - the "await external event / human-in-the-loop"
     leg Axon's `awaitEvent` covers, done by Temporal instead.
   - **Event-to-start**: the same mapping, naming which `EventType`s
     become a `StartWorkflowExecution` call instead (workflow type +
     task queue) - skilj's `OrderPlaced` triggering a fresh
     `OrderFulfillment` workflow, the way Axon's
     `@Workflow(startOnEvent = ...)` does.
   - **Correlation convention, fixed, not caller-configured**:
     `workflow_id = "{bounded_context}:{tag_key}:{tag_value}"` for
     whichever one `tag_key` a mapping entry names - the same
     "automatically derived from a DCB tag, not a fresh concept"
     register `owner_tag_key` already established ([§23](#cross-tenant-projection-read-fix-owner-tag) and others).
     Reusing this same ID for both the start and every later signal is
     what makes Temporal's own idempotent-start-by-workflow-ID semantics
     apply for free - no dedup work of skilj's own to write.
   - **Delivery mechanism**: `GET /v1/events/consume?mode=manual` +
     `POST /v1/events/consume/ack` (`skilj-rest/src/routes/mod.rs`),
     not a GraphQL `EventSubscription` - a polling loop needs no
     persistent connection management, and manual-ack's own "redeliver
     on crash before ack, handler must be safe to run twice" contract
     (`docs/rest-event-reading.md`) composes cleanly with
     signal/start's own idempotent-by-workflow-ID delivery: don't ack
     until the Temporal call succeeds, and a redelivered event just
     repeats an already-idempotent call.
   - **Dependency**: Temporal's own `temporalio-client`/`temporalio-common`
     crates (not the full `temporalio-sdk` worker/Activity-authoring
     crate) - a thin wrapper over Temporal's gRPC service for exactly
     `start_workflow`/`signal_workflow`, the only two calls this bridge
     makes, using each crate's own `Untyped*` marker types
     (`UntypedWorkflow`/`UntypedSignal`/`RawValue`) rather than the
     generated, statically-typed workflow definitions those crates
     otherwise favour - this bridge's whole point is dispatching to
     workflow/signal names only known at runtime, from a config, not
     compiled in. Both crates are "Public Preview" per Temporal's own
     docs as of this investigation (2026-09) - re-check maturity before
     depending on it for real, and expect to track breaking changes;
     this was the one real risk named in the plan, and turned out real
     in one concrete way: building against this crate needs a local
     `protoc` binary at compile time (`prost-wkt-types`'s own build
     script) - see CONTRIBUTING.md's new note.
   - **Signal idempotency, a detail the plan didn't originally name**:
     `Start`'s redelivery-safety comes for free from
     `WorkflowIdConflictPolicy::UseExisting`, but `Signal` has no
     equivalent dedup-by-workflow-id - Temporal's own answer is
     `WorkflowSignalOptions::request_id`, which this bridge derives from
     the event's own `(bounded_context, event_type, sequence)`: stable
     across a redelivery of the identical event, unique across every
     other one - the identical `"{run_id}:{activity_id}"` reasoning
     phase 1's own idempotency key already uses, one level further out.
     An unmapped/absent correlation tag is acknowledged anyway rather
     than retried forever (logged, not silently dropped) - retrying
     can never produce a tag value the event itself never carried.

3. **What deliberately stays Temporal's job, unbuilt here**: durable
   timers/sleep, retry policies, compensation logic, fan-out/fan-in,
   human-approval escalation - all in the Temporal workflow definition
   itself, in whichever language a team writes those in (Temporal's Go/
   Java/TypeScript/Python/.NET SDKs are stable; Rust's is not, so
   workflow/Activity *authoring* in Rust is a choice to make separately
   from building `skilj-temporal`, which only ever needs the client).
   skilj's own event store and Temporal's own workflow history stay two
   separate, independently-queryable audit trails, correlated by the ID
   convention above rather than merged into one - a deliberate design
   point, not a gap.

**Phase 1 verified**: `skilj/tests/temporal_activity_idempotency_example.rs`,
3 tests, real Postgres, over the actual `POST /v1/commands/trigger`
REST surface (no in-process shortcut) - a redelivered Activity Task
(same Run ID and Activity ID) is deduplicated with identical
`triggeredEventSequences` and exactly one stored event; a different
Activity ID within the same run is not coalesced (two events); the same
Activity ID across two different runs is not coalesced either (two
events) - the case that motivates keying on Run ID rather than Workflow
ID alone. `cargo build/clippy -D warnings/test --workspace` and `cargo
fmt --check` clean. No spec change - this is client-side usage of
`submitCommand`'s already-spec'd idempotency behaviour, not a new
surface.

**Phases 2-3 verified**: the new `skilj-temporal` crate
(`skilj-temporal/src/lib.rs`) - `poll_once`/`run`, `EventTypeMapping`/
`MappingAction`, `correlation_workflow_id` - has 6 pure unit tests (no
network, the correlation convention's own edge cases: matching tag,
absent tag, `null`-valued tag, bounded-context-qualifies the id,
`signal_request_id` stable across a simulated redelivery and distinct
per event) plus 3 real end-to-end tests
(`skilj-temporal/tests/temporal_bridge.rs`) against a *real*, ephemeral
Temporal service (`temporalio_sdk_core::ephemeral_server::TestServerConfig`,
dev-dependency only - never linked into the shipped crate) and a small
local mock of skilj's own REST surface (the same "wire protocol only,
no real skilj crate" treatment `skilj-tui`'s own
`tests/subscription.rs` already gives its GraphQL half, since skilj's
own consume/ack contract is already exhaustively tested elsewhere):
an `OrderPlaced` event really starts a Temporal workflow execution
(confirmed via a real `describe()` call, not just "no error"), a later
`PaymentConfirmed` event really signals that same running execution
rather than starting a second one, both events are acknowledged by
their own token/sequence only after a successful dispatch, a second
bounded context reusing the identical tag value derives a different,
independent workflow id, and an event with no matching correlation tag
is acknowledged (not redelivered forever) while never reaching Temporal
at all. Downloads a small (~25MB) test-server
binary on first run, cached outside `/tmp` (see this pass's own
CONTRIBUTING.md note); skips gracefully, matching this workspace's
existing embedded-Postgres tolerance, if that download can't reach the
network. `cargo build/clippy -D warnings/test --workspace` and `cargo
fmt --check` clean (with `PROTOC` set - see CONTRIBUTING.md). No spec
change - `skilj-temporal` has no registration/storage/wire surface of
skilj's own to obligate; it is only ever a caller of surfaces that
already exist.

**A `/code-review high` pass on this diff found 4 real gaps**, verified
by reading the pinned `temporalio-client`/`temporalio-common` 0.8.0
source directly rather than trusting the crate's own doc comments at
face value - all fixed, 2 with new regression tests:

- **`Start`'s redelivery-after-completion gap**: `id_conflict_policy`
  alone (`WorkflowIdConflictPolicy::UseExisting`) only governs a
  workflow *currently running* under this id - it says nothing about
  one that already *closed* under it, which defaults to
  `WorkflowIdReusePolicy::AllowDuplicate` (Temporal's own proto
  comment: "allow starting a workflow execution using the same
  workflow id"). A `Start` redelivered after the original run had
  already finished would silently create a *second*, independent
  execution - directly contradicting this module's own "gets this for
  free" doc comment. Fixed: `id_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate)`
  alongside the existing conflict policy, with `dispatch`'s `Start` arm
  now catching the resulting `WorkflowStartError::AlreadyStarted` and
  treating it as the success it actually is - "a workflow exists for
  this business entity" already held either way. New test:
  `a_redelivered_start_while_the_workflow_is_still_running_does_not_error`
  (the closed-and-redelivered half of this fix is verified by reading
  Temporal's own enum documentation rather than a live test, since
  driving a workflow to actually close would need a real Worker,
  pulling in the worker/Activity-authoring SDK this crate deliberately
  keeps out of even its own tests).
- **`run` died forever on one transient error**: a single `BridgeError`
  from `poll_once` (a momentary network blip against skilj or Temporal)
  propagated straight out of the polling loop via `?`, ending that
  mapping's own processing permanently with no supervisor to notice.
  Fixed: `run` now logs and continues, backing off `poll_interval`
  before retrying - it never returns at all now (`-> !`), a stronger
  guarantee than "returns `Result`" ever was.
- **A `Signal` racing ahead of its own correlated `Start`** (both
  mappings polled independently; backlog catch-up or ordinary latency
  skew can deliver `PaymentConfirmed` before `OrderPlaced` finishes
  dispatching) is exactly the failure mode the `run` fix above turns
  from "permanently stuck" into "self-healing on the next poll interval" -
  documented explicitly in `run`'s own doc comment, including why
  Temporal's `signal_with_start_workflow` (considered as the "fix the
  race at its root" alternative) was *not* adopted: it would need
  fabricating the workflow's own starting input from whichever event's
  payload lost the race, silently starting the workflow from the wrong
  shape exactly when the race actually happens - worse than a
  self-healing delay.
- **`EventTypeMapping::event_type` was dead configuration**: declared
  by every caller, documented as load-bearing, never actually checked
  against anything - a mapping wired to the wrong credential (copy-paste
  between two mappings) would silently apply the wrong `MappingAction`/
  `correlation_tag_key` to whatever events that credential really
  serves. Fixed: `poll_once` now checks `ConsumeResponse::event_type_name`
  (skilj's own echoed-back token scope, already on the wire, previously
  just not read) against `mapping.event_type`, rejecting outright with
  a new `BridgeError::EventTypeMismatch` before looking at a single
  event. New test: `a_mapping_whose_event_type_does_not_match_its_credential_is_rejected`.

Re-verified after all four fixes: `cargo build/clippy -D warnings/test
--workspace` and `cargo fmt --check` clean, 11 `skilj-temporal` tests
(up from 9), real Postgres and the real ephemeral Temporal service
throughout, `allium check`/`plan` still unchanged (449 obligations).

**Addendum (0.0.5): the `protoc` requirement above is gone.** A user
tip pointed at [sdk-rust#1589](https://github.com/temporalio/sdk-rust/issues/1589),
closed via [#1590](https://github.com/temporalio/sdk-rust/pull/1590):
`temporalio-client`/`temporalio-common` 1.0.0 (up from 0.8.0) added a
`vendored-protox` feature, forwarding down to `temporalio-protos`'s own
`vendored-protox` - the pure-Rust `protox` compiler instead of shelling
out to a system `protoc`. Enabled in `skilj-temporal/Cargo.toml`;
confirmed by testing, not assumed - `cargo build/clippy -D
warnings/test -p skilj-temporal` (including the real-ephemeral-Temporal
integration tests) passes clean with no `protoc` on `PATH` and no
`PROTOC` set. The 1.0.0 bump needed one real source change:
`WorkflowIdConflictPolicy`/`WorkflowIdReusePolicy` moved from a
`temporalio_common::protos::...` re-export to being owned directly by
`temporalio_client` - a two-line import fix, no behavior change. Still
"Public Preview" caution applies going forward (see RELEASING.md) - this
was a welcome upstream fix landing four days before this release, not a
signal the API has stopped evolving.

**Relationship to native deadlines ([§46](#native-deadlines), Codeberg issue #20)**: `skilj-temporal` stays the
right answer for a genuine multi-step process - retries, compensation,
state that outlives any one command, real waits on an external system.
[§46](#native-deadlines)'s `ScheduleDeadline`/`CancelDeadline` is the lightweight native option for
the much more common case this pairing was always overkill for: "fire
one command if nothing else happens by a given time." The two live side
by side, not one superseding the other - the same "no multi-step state,
no compensation, one hop" scoping decision [§36](#cross-context-route)'s `CrossContextRoute` already
made for its own, narrower slice of what would otherwise need Temporal.

<a id="connection-pool-sizing"></a>
## 35. Configurable connection pool sizing

Found while reviewing what's worth doing before a release: `db::connect`
was `sqlx::PgPool::connect(database_url)` with no configuration
whatsoever - `sqlx`'s own bare default (a 10-connection cap, no
configured `acquire_timeout`/`idle_timeout`), and no way for a caller to
change it at all. Inconsistent with the rest of `SkiljBuilder`, where
every other tunable (`async_projection_poll_interval`,
`snapshot_poll_interval`, `scheduler_poll_interval`,
`event_broadcast_capacity`, `event_cache_warm_up_count`) already has a
sensible default plus an escape hatch - arguably the most load-bearing
one for real production throughput was the one exception. The
background async-projection/snapshot/scheduler pollers `.build()` already
spawns compete with every foreground GraphQL/REST request for whatever
the pool provides, so a fixed cap of 10 is a real ceiling on real load,
not a hypothetical one.

**Fix**: `db::connect_with(database_url, PgPoolOptions)` alongside the
unchanged `db::connect` (now just `connect_with(url, PgPoolOptions::new())`
- `sqlx`'s own bare default, byte-identical behaviour for every existing
caller); `PgPoolOptions` re-exported from `skilj_core::db` the same way
`Pool` already is, so `skilj` itself needs no direct `sqlx` dependency to
accept one. `SkiljBuilder::pool_options(PgPoolOptions)`, unset by
default, threading through to `connect_with` in `.build()` when set.

**Verified**: `skilj-core/tests/pool_options.rs`, two real-Postgres
tests - `connect_with` actually carries the caller's own options,
confirmed by reading them straight back off the live pool
(`Pool::options().get_max_connections()`), not just trusting
construction didn't error; and `connect` itself is unchanged, confirmed
against `PgPoolOptions::new()`'s own default. `cargo
build/clippy -D warnings/test --workspace` and `cargo fmt --check`
clean. No spec change - pool sizing is deployment configuration, the
same "process-start knob, not a registered value" register every other
`SkiljBuilder` tunable already lives in.

<a id="cross-context-route"></a>
## 36. `CrossContextRoute`: crossing bounded contexts without an external system

Prompted by "is there something we could do to make it easier to have
messages cross bounded contexts, without needing Temporal?" - [§34](#skilj-temporal-plan)'s
`skilj-temporal` pairing is the right answer for a *process*: multiple
steps, retries, compensation, state that outlives any one command. Most
cross-context needs in practice are much smaller than that - "when
`OrderPlaced` happens over here, submit `ReserveStock` over there" - and
paying for an external workflow engine, a worker process, and its own
operational surface for a single hop is a real tax with nothing to show
for it. DCB's own tags are structurally scoped to *one* bounded context
(§0/spec - two bounded contexts sharing a tag key still never interact),
so nothing already in skilj closes this gap on its own.

**Design**: `skilj_core::plugin::CrossContextRoute` - a new plugin trait,
alongside `EventType`/`CommandType`/`Projection`/`Snapshot`:

```rust
pub trait CrossContextRoute {
    type Source: EventType;
    type Target: CommandType;
    const NAME: &'static str;
    fn route(source_payload: &<Self::Source as EventType>::Payload)
        -> Option<<Self::Target as CommandType>::Payload>;
}
```

Deliberately **not** a Saga/process manager - one hop, `Source` commits
in its own bounded context, `route()` decides `None` (skip) or
`Some(payload)`, and that payload goes through `Target`'s own real
`decide()`/`submit_command` path in *its* bounded context, exactly like
any other caller of `Target`. No multi-step state, no compensation, no
retry policy of its own - `route()` is a pure, synchronous function of
one event's payload, so the entire "what happens next" question is
answered the instant `Source` commits, not carried forward as state a
process has to keep re-evaluating. Reaching for a Saga/Process trait here
was considered and rejected on the same grounds [§34](#skilj-temporal-plan) already settled for
Temporal: state/retry complexity belongs somewhere it can be owned
properly (an external orchestrator, when a real multi-step process is
actually needed), not folded into skilj's own plugin surface as a
half-measure. A route that needs more than one hop is a process - use
[§34](#skilj-temporal-plan)'s `skilj-temporal` pairing instead, which already leverages the same
idempotent-command-submission mechanism this feature also relies on.

`Source`/`Target` are typed against each other's own bounded context via
their existing `const BOUNDED_CONTEXT: &'static str` (§1.3.3's
`auto_register` default), *not* the builder's "current bounded context" chain
`.event_type::<T>()`/`.command_type::<T>()` use - a route by definition
spans two bounded contexts, so there's no single "current" one to infer
it from. `SkiljBuilder::cross_context_route::<R>()` registers it (keyed
by `R::NAME`, last registration for a given key wins, same convention
every other `HashMap`-backed registry here already has); `.cross_context_route_poll_interval(Duration)`
tunes the poll rate (default 500ms, same shape as `async_projection_poll_interval`/
`snapshot_poll_interval`).

**Delivery mechanism**: a durable cursor per route
(`{Source::BOUNDED_CONTEXT}.cross_context_route_cursors`, one row per
`route_name`, mirroring `idempotency_keys`'s own per-bounded-context
table), driven by one new shared background task `SkiljBuilder::build()`
spawns - same shape as the async-projection/snapshot tasks ([§8](#open-for-a-future-pass) item 6,
[§19](#optional-snapshotting-matching-events)): one task for every registered route, not one per route, ticking on
`cross_context_route_poll_interval`, `stream::iter(...).for_each_concurrent`
over the fixed route list (read once at spawn time, since routes have no
runtime registration surface - the same "compiled in, fixed at startup"
model `EventType`/`CommandType`/`Projection`/`Snapshot` already have).
Each tick: read the cursor, fetch `Source` events after it, and for each
one call `route()` through the type-erased `CrossContextRouteDispatcher`;
`None` advances the cursor with nothing submitted, `Some(payload)` submits
`Target` via `skilj_core::db::decide_and_submit_command` (below) with an
idempotency key of `"{NAME}:{source_event_sequence}"` before advancing
the cursor - so a redelivered/retried tick is exactly as safe as any
other idempotency-keyed submission already is, and a real error from the
submission itself leaves the cursor where it was, retried next tick
rather than silently skipped.

**`decide_and_submit_command`**: the route's own submission needed the
identical "optimistic `decide()`, then locked `submit_command`" sequence
`skilj-rest`'s `post_commands_trigger` and `skilj-graphql`'s
`submitCommand` resolver each already ran inline - `derive_tags`,
resolve an optional snapshot context, fetch matching events, dispatch,
then `submit_command`. Rather than a third copy, that whole sequence is
now `skilj_core::db::decide_and_submit_command`, and both existing
callers were refactored onto it (behaviour-preserving - verified against
each one's own existing test suite, unchanged pass/fail and unchanged
error codes/messages). `submitCommand`'s own `required_role` check
(§1.3.1) stays outside the shared helper, checked before calling it - REST
triggering never needs it, and the route caller (`client_id:
"cross-context-route"`) bypasses both REST/GraphQL authorisation
entirely, submitting directly the same way the background pollers already
reach `Target`'s `decide()` without going through a token.

**No spec entity** - like `Snapshot` ([§19](#optional-snapshotting-matching-events)) and the owner-tag scoping
series, `CrossContextRoute` has no registration surface a spec `contract`
would describe (no GraphQL mutation registers one, no REST endpoint lists
them); it's a Rust-only construct layered on top of DCB, not a change to
DCB's own model. The spec's existing "two bounded contexts sharing a tag
key never interact" statement stays true - a route reacts to an event
*after* it commits, it doesn't let `Target`'s `decide()` see `Source`'s
tags or history.

**Verified**: `skilj/tests/cross_context_route.rs`, a real end-to-end
test against Postgres with two real bounded contexts ("shipping",
"inventory") wired into one `Skilj` instance - a directly-created
`OrderShipped` event in "shipping" is picked up by the route's own poll
task and turned into a real `ReserveStock` command submission in
"inventory", observed through a real (`sync: true`) projection there.
A second case in the same test proves the `route() -> None` skip path:
a backorder occurrence produces no command (projection state unchanged
after several poll intervals' worth of margin), and a third, ordinary
occurrence afterward still gets processed - proof a skip never stalls the
cursor. `cargo build/clippy -D warnings/test --workspace` and `cargo fmt
--check` clean.

**Security-review follow-up: the shared `idempotency_keys` namespace has
no caller column.** `idempotency_keys`'s primary key is
`(command_type_name, idempotency_key)` - no `client_id`/caller column at
all, because every prior caller (Codeberg issue #12's design, and the
`skilj-temporal` bridge in [§34](#skilj-temporal-plan)) only ever collided with its *own* past
submissions, so nothing needed to scope the namespace by who wrote a
key. `CrossContextRoute`'s background task is the first caller to place
an *unauthenticated internal* key into that same shared table
(`"{route.name}:{sequence}"`, unpredictable only in the sense that an
outside caller wouldn't know a given route's name or the sequence
number its next occurrence would land on - not a secret). An ordinary
Write-level caller who *did* know or guess both could pre-plant that
exact key via `submitCommand`'s `idempotencyKey`/the REST trigger's
`Idempotency-Key` header ahead of time; when the route's own poll task
later tried to submit under the same key, `submit_command`'s existing
dedup logic would treat it as an already-seen request and silently
return `Deduplicated` - the route's cursor would still advance (an
error would correctly leave it retried, but a dedup hit isn't an error),
so the real cross-context delivery would be dropped with nothing in the
logs pointing at why.

Fixed by reserving a namespace rather than trying to make the key
unguessable: `catch_up_cross_context_route` now derives its key as
`"{RESERVED_IDEMPOTENCY_KEY_PREFIX}{route.name}:{sequence}"`
(`skilj_core::event_store::RESERVED_IDEMPOTENCY_KEY_PREFIX`, currently
`"skilj-cross-context-route:"` - not a guarantee against a legitimate
caller who happens to pick a key starting with that exact literal, who
gets rejected the same as an attacker would; low enough odds in
practice to accept as this mechanism's tradeoff), and a new
`reject_reserved_idempotency_key` is called at both wire boundaries -
`skilj-rest`'s `post_commands_trigger` and `skilj-graphql`'s
`submitCommand` resolver, immediately after each reads its own
caller-supplied idempotency key - rejecting any caller-supplied key that
starts with the reserved prefix outright (`Error::ReservedIdempotencyKeyPrefix`,
400 over REST, a normal GraphQL error over GraphQL) before it ever
reaches the shared `idempotency_keys` lookup. Checked at the call site
rather than folded into `authorise_command_trigger`/
`authorise_command_submission` themselves - purely to match the same
"wire-boundary concern" register `submitCommand`'s own `required_role`
gate already uses (above), not out of necessity: the route's own
internal caller reaches `decide_and_submit_command` directly and never
calls either of those two functions, so centralising the check there
would have been just as safe. `catch_up_cross_context_route`
additionally logs a warning (not an error - the cursor still correctly
advances) if it ever *does* see its own key deduplicated - expected,
not anomalous, after an ordinary crash/restart between a prior
submission committing and its cursor advance (the two are separate,
non-transactional writes); unexpected for any other reason, since
nothing else can write into the reserved namespace. Verified by a real
Postgres-backed test on each wire boundary
(`command_trigger_rejects_a_reserved_idempotency_key_prefix`,
`submit_command_rejects_a_reserved_idempotency_key_prefix_over_graphql`)
asserting both the specific error code and that nothing was written to
the event store at all.

**Follow-up, closed for real in [§37](#idempotency-keys-client-id-scoping)**: the fix above patched the two
existing callers, not the shared `idempotency_keys` table's own
structural gap - investigating that gap further surfaced a second, more
serious bug already live since 0.0.2, not merely a future risk. See [§37](#idempotency-keys-client-id-scoping).

<a id="idempotency-keys-client-id-scoping"></a>
## 37. `idempotency_keys` gets `client_id`-scoped: a real cross-tenant collision, live since 0.0.2

Prompted by asking "investigate the primary key with no caller/client_id
- what is the potential real problem?" of [§36](#cross-context-route)'s own "known limitation"
note above, rather than accepting that note's framing (a future-caller
risk) at face value.

**The original decision, and why it held up at the time.** [§21](#optional-idempotency-key-submission) (issue
#12) explicitly decided `idempotency_keys` scoping would be
`(bounded_context, command_type)` only, "not also per-caller" - resolved
with the user directly, not an oversight. That held up fine under its
own assumption: a well-randomized caller-chosen key (a UUID) practically
never collides with another caller's by accident, so *which* caller
wrote a row was never something the lookup needed to know.

**What broke the assumption.** Owner-tag multi-tenancy ([§23](#cross-tenant-projection-read-fix-owner-tag)/[§25](#cross-tenant-read-fix-command-query)/[§30](#cross-tenant-write-fix-owner-tag-scoping)),
built *after* issue #12, means many distinct tenants can legitimately
submit the *same* `CommandType` in one bounded context - each with their
own `CommandToken`/`RoleAccessMapping`, disambiguated only by that
grant's own `scope` (`authorise_command_trigger`/
`authorise_command_submission`, skilj-core/src/event_store/mod.rs),
invisible to `idempotency_keys`. A business-derived idempotency key (an
order id, an invoice number - a common, even recommended, convention)
from one tenant can plausibly coincide with an unrelated tenant's own,
with no attacker, guessing, or malice needed at all. Verified this was
actually reachable, not merely plausible, by reading the real
authorization code rather than assuming: `CommandToken.scope`/
`RoleAccessMapping.scope` are the *only* thing distinguishing two
tenants hitting the same `CommandType`, and `client_id` in
`CommandAuthorised` (`token.id` for REST, `access_mapping.role.id` for
GraphQL) is a real, already-existing, per-tenant-grant identifier -
already threaded through `submit_command`/`decide_and_submit_command`
for the resulting event's own metadata, just never used to scope the
idempotency lookup.

**The actual bug.** Two tenants, same `CommandType`, same key string:
the second tenant's real submission silently short-circuits to
`Deduplicated`, returning the *first* tenant's own `triggered_event_sequences`.
Two harms, not one: the second tenant's real command never runs at all
(`decide()` never executes, nothing persists) while the API reports
`accepted: true` - a silent write-loss/correctness bug, not merely a
leak - and the second tenant also receives sequence numbers belonging to
an unrelated tenant's own event stream, a minor cross-tenant
disclosure (existence/ordering, not payload). This has been live since
**0.0.2** (2026-08-30, when issue #12 shipped) - `CrossContextRoute`'s
own predictable-key variant of the same root cause ([§36](#cross-context-route)) is narrower and
newer, not the original or the more consequential form of this bug.

**The fix**: `idempotency_keys`' primary key becomes `(command_type_name,
client_id, idempotency_key)`. `client_id` was already available at
every call site for other reasons - this is purely a matter of finally
scoping the lookup by it. A useful side effect: this structurally closes
`CrossContextRoute`'s own [§36](#cross-context-route) issue too, on a firmer footing than the
reserved-prefix convention that fix shipped with - no external caller's
`client_id` is ever caller-suppliable (always derived server-side from
an authenticated token/role), so no external submission can ever land
under `"cross-context-route"`'s own partition regardless of what
`idempotency_key` string it uses, prefix or not.
`RESERVED_IDEMPOTENCY_KEY_PREFIX`/`reject_reserved_idempotency_key` stay
in place as harmless defense-in-depth, no longer load-bearing.

**Migrating an already-provisioned bounded context** (real ones exist,
back to 0.0.2) needed more than this codebase's established `ALTER
TABLE ... ADD COLUMN IF NOT EXISTS` idiom
(`ensure_projection_state_owner_columns`/`ensure_event_scoping_columns`)
covers - the whole point is `client_id` must be *part of the primary
key*, and Postgres has no `ADD CONSTRAINT IF NOT EXISTS`/`ALTER PRIMARY
KEY` form to lean on for that half. `db::migrate_idempotency_keys_client_id_scoping`
(called from `SkiljBuilder::build()`'s own per-bounded-context startup
loop, right after `ensure_idempotency_keys_table`) wraps `ADD COLUMN ...
DEFAULT ''` + `DROP CONSTRAINT` + `ADD PRIMARY KEY` in one transaction,
guarded by a check against `information_schema.key_column_usage` (skip
if `client_id` is already part of the primary key) and a
`pg_advisory_xact_lock` keyed by the bounded context's own schema name
for the transaction's whole duration - a real fleet runs more than one
instance, and the existing per-process warm-up concurrency (Codeberg
issue #15) only serializes work *within* one process. Verified directly
that the lock isn't there because skipping it would be unsafe: a real
test racing several concurrent callers against the same pre-migration
table with the lock removed never errored, because `DROP CONSTRAINT IF
EXISTS` ahead of `ADD PRIMARY KEY` means there's never an *existing*
primary key for a second `ADD PRIMARY KEY` to collide with, and
Postgres's own whole-transaction-duration `ACCESS EXCLUSIVE` table lock
(held from each instance's first `ALTER TABLE` onward) already
serializes every concurrent instance's 3-statement sequence on its own.
The lock's real, more modest purpose: without it, every loser of that
natural serialization still repeats the whole `DROP CONSTRAINT`/`ADD
PRIMARY KEY` dance once its turn comes (each a genuine no-op end state,
but a real catalog write nonetheless); with it, a loser's own
`already_migrated` recheck sees the winner's now-committed change first
and skips straight to a plain read, so only the first instance through
ever does real DDL work. `_xact` (not plain `pg_advisory_lock`) both
releases automatically at commit/rollback and guarantees the lock and
the statements it protects share one connection, rather than each
landing on a different one from the pool.

**The legacy-row tradeoff - the user's own explicit call.** A
pre-migration row never recorded who submitted it - not recoverable by
any cleverer migration, the information was simply never written.
Backfilled to `client_id = ''` (this codebase's own "nothing is ever
deleted" precedent, [§21](#optional-idempotency-key-submission)) rather than deleted, but two genuinely
different failure modes were on the table for what `lookup_idempotency_key`
does with that backfilled row afterward, and there's no third option
that avoids both:

- Keep it matchable as a fallback for whoever retries that same string
  first: preserves dedup power for a genuine old retry (avoiding a
  double-execution), but leaves a frozen, never-growing set of
  already-used key strings still able to collide across unrelated
  *future* callers who happen to reuse one of those specific strings -
  proven directly in an early draft of this fix's own test suite, then
  corrected once the implication was surfaced.
- Retire it permanently, matched by no one ever again: fully closes the
  collision class this fix exists for, with no residual, shrinking or
  not - at the cost that a genuine retry of a request submitted just
  before this migration runs won't be recognised as a duplicate,
  possibly double-executing a real command.

Asked directly rather than decided unilaterally (the first draft of
this fix chose the fallback and only surfaced the tradeoff after a test
written to prove it "worked" instead proved the residual collision was
real): the user chose to retire pre-migration rows outright, prioritising
fully closing the cross-tenant collision class over preserving legacy
retry-safety at its edges. The row itself stays in the table, permanently
orphaned rather than deleted - `lookup_idempotency_key` is a plain,
non-fallback `client_id = $2` match, nothing more.

**Verified**: `skilj-core/tests/submit_command.rs` -
`submit_command_with_the_same_idempotency_key_from_two_different_clients_does_not_collide`
(two tenants, one shared key string, both get real, independent
`Accepted` outcomes; each tenant's own retry still correctly dedups
against their own answer, never the other's),
`migrate_idempotency_keys_client_id_scoping_retires_pre_migration_rows`
(rolls a table back to its literal pre-fix, column-for-column 0.0.2
shape with a real legacy row, migrates it, and proves the legacy row
is never matched again - even by whoever originally wrote it - while
staying physically present in the table, and that an unrelated fresh
key works normally), and
`migrate_idempotency_keys_client_id_scoping_is_safe_under_concurrent_callers`
(genuinely races several `tokio::spawn`'d concurrent callers - a real
race, this test's runtime is the standard multi-threaded one, not
merely interleaved awaits on one thread - against the same
pre-migration table; this is also the test that caught the `pg_advisory_xact_lock`
doc comment's own overclaim above, by initially passing with the lock
removed - a claim worth re-checking by testing, not trusting the
reasoning that motivated writing it in the first place). The full
existing `skilj-core` test suite (every test binary, real embedded
Postgres) passes unchanged, including `CrossContextRoute`'s own
end-to-end test and the REST/GraphQL idempotency-key wire tests from
[§36](#cross-context-route)'s own fix - `cargo build/clippy -D warnings/test` and `cargo fmt
--check` clean throughout.

<a id="message-broker-bridges-investigation"></a>
## 38. Message-broker bridges (Kafka/Solace/etc.): investigation, not yet built

Prompted by "investigate other modules that would make skilj easier to
use and integrate - with Kafka, to send out events and retrieve
external events through it, or things like Solace, look for
things/crates used a lot with Rust." Investigation only - nothing built
yet, findings below to ground a decision on scope before any code.

**Nothing new needed on skilj's own side - the hook points already
exist**, the same "broker-agnostic, already built" realisation [§34](#skilj-temporal-plan)
started from for Temporal:

- **Outbound** (a skilj event reaching an external system): `GET
  /v1/events/consume?mode=manual` + `POST /v1/events/consume/ack`
  (`skilj-rest/src/routes/mod.rs`) - server-tracked polling, no
  persistent connection management, "redeliver on crash before ack"
  composing cleanly with whatever at-least-once delivery a broker
  client already gives a producer.
- **Inbound** (an external system's own message becoming a skilj
  event): `POST /v1/events/external` (`ExternalEventIngestion`, opt-in
  per `EventType.external_creation_allowed`) for "this fact already
  happened externally, record it" - the direct analogue of `CreateExternalEvent`
  in the spec. `POST /v1/commands/trigger` is the other inbound door,
  for "this message should be *decided upon*" rather than recorded
  verbatim - which one a given mapping wants is a modelling choice per
  message type, not something the bridge itself needs an opinion on.
- **Correlation/idempotency conventions already established, directly
  reusable**: [§34](#skilj-temporal-plan)'s `workflow_id = "{bounded_context}:{tag_key}:{tag_value}"`
  pattern (deriving a stable external identity from a DCB tag) maps
  onto a Kafka message key, an AMQP routing key, or a NATS subject the
  same way it maps onto a Temporal workflow ID. [§34](#skilj-temporal-plan)'s `Signal`
  idempotency fix - deriving a request id from `(bounded_context,
  event_type, sequence)` - is the same shape a Kafka producer's own
  idempotent-produce config or a consumer-side dedup key would want.
  Inbound, the exact `"{run_id}:{activity_id}"` -> `Idempotency-Key`
  realisation ([§21](#optional-idempotency-key-submission)/[§34](#skilj-temporal-plan) phase 1) generalises to `"{topic}:{partition}:{offset}"`
  for Kafka, or whatever a given broker's own stable per-message
  identity is - the pattern, not the Temporal specifics, is what's
  reusable.

**Rust crate ecosystem, checked directly (crates.io download counts and
last-release dates, 2026-09), not assumed**:

| System | Crate | Total downloads | 90-day downloads | Last release |
|---|---|---|---|---|
| Kafka | `rdkafka` | 35.7M | 6.46M | Jan 2026 |
| NATS | `async-nats` | 46.5M | 5.55M | Jul 2026 |
| AWS SQS | `aws-sdk-sqs` | 17.2M | 5.82M | Sep 2026 |
| RabbitMQ (AMQP 0-9-1) | `lapin` | 12.2M | 2.08M | May 2026 |
| MQTT | `rumqttc` | 8.0M | 1.91M | Nov 2025 |
| Google Cloud Pub/Sub | `google-cloud-pubsub` | 6.8M | 1.66M | Aug 2026 |
| AMQP 1.0 (Solace/Azure Service Bus/Artemis) | `fe2o3-amqp` | 2.36M | 0.83M | Aug 2026 |
| Apache Pulsar | `pulsar` | 2.35M | 0.42M | Aug 2026 |
| Solace-specific | `solace-rs` | 35K | 142 | May 2025 (stale) |

**Solace, specifically**: no healthy Solace-specific Rust crate exists
- `solace-rs` is an unofficial FFI wrapper needing Solace's own
  proprietary C SDK installed separately, essentially unused (142
  downloads in 90 days) and over a year stale. The actually practical
  path is protocol-level, not vendor-crate-level: Solace PubSub+
  natively speaks AMQP 1.0 (and MQTT) as first-class protocols
  alongside its own proprietary one, so `fe2o3-amqp` (pure Rust, no C
  dependency, healthy and active) talks to a Solace broker the same way
  it talks to Azure Service Bus or ActiveMQ Artemis - one dependency
  covering three "enterprise" brokers via a shared open standard,
  rather than a dedicated, much weaker Solace-only crate.

**Recommendation, not yet decided with the user**: mirror [§34](#skilj-temporal-plan)'s own
shape - a small, thin bridge crate per *protocol family* (not
skilj-core changes, no shared cross-broker abstraction forced over
systems with genuinely different delivery semantics - partitioned logs
vs. exchange/routing-key vs. subject-based pub/sub - the same reasoning
that ruled out a shared `Process`/`Saga` trait in [§34](#skilj-temporal-plan)). Candidate first
build, ranked by the table above and by matching the user's own named
example: `skilj-kafka` on `rdkafka` - highest combined maturity and
category fit for "the thing most people mean by event streaming
integration." `skilj-amqp` on `fe2o3-amqp` would be the second,
covering Solace/Azure Service Bus/Artemis/RabbitMQ-via-AMQP-1.0-plugin
in one crate rather than one-per-vendor. NATS (`async-nats`, actually
*more* total downloads than `rdkafka`, and a materially simpler
deployment story - no Zookeeper/KRaft, a single static binary) is worth
naming as a real contender for "first build" too, not just an
also-ran, if simplicity/ops-overhead matters more than Kafka's own
partitioned-log/replay semantics for a given adopter.

**Open questions for the user, not resolved here**: which
broker/protocol to build first (Kafka, per the user's own example, or
NATS, per the raw download numbers and simpler ops story); whether
"send out"/"retrieve" should be one crate per broker (both directions)
or split; and whether inbound messages should default to
`ExternalEventIngestion`, `CommandTrigger`, or be a per-mapping choice
the way [§34](#skilj-temporal-plan)'s own `EventTypeMapping` names signal-vs-start per
`EventType`.

<a id="external-message-dedup-create-external-event"></a>
## 39. Built into skilj instead: external-message dedup on `CreateExternalEvent`

[§38](#message-broker-bridges-investigation)'s own investigation flagged that `ExternalEventIngestion` has no
dedup mechanism at all - a real problem for any at-least-once broker
bridge (Kafka included) recording facts via it, since a redelivery after
a crash-before-commit would create a second event for the same message.
The plan at the time was to leave this to each bridge crate's own
responsibility, the same way a bridge author handles anything else
specific to their own broker. Told directly this was the wrong call:
"most external messages have some unique identifier we could store in a
table... these things are great to have baked in, and not leave to the
responsibility of users." Built into `CreateExternalEvent` itself
instead - no bridge crate needed this to exist first.

**The insight that makes it cheap**: most message-streaming systems
(Kafka, Kinesis, Pulsar, Azure Event Hubs) share one shape - messages
are numbered *within a partition*, strictly increasing, and a consumer
only ever needs to know the highest number it has already handled for a
given partition to recognise every redelivery below it. That's a single
integer per partition, not a row per message - `idempotency_keys`'
own shape would have worked (one row per message key, kept forever) but
wastes space and a write per message for information a watermark
already implies for free.

**Spec first, delegated to `allium:tend`, independently re-verified -
not implemented off a private design.** `SubmitExternalEvent` gains an
optional `dedupe_partition_key?`/`dedupe_sequence?` pair (both-or-neither,
a real `requires` guard), a `highest_dedupe_sequence(adapter,
dedupe_partition_key)` black box in the same register as
`next_sequence`/`recorded_acceptance`, and a conditional `ensures` -
`ExternalTriggered` is created only when the sequence is above the
recorded watermark, mirroring `RegisterProjection`'s own precedent for
conditional entity creation rather than `ProcessCommand`'s `requires`-
based short-circuit (a `requires` failure on an *external stimulus*
trigger is a caller error, per the language reference - a redelivery is
not one). Independently re-verified after the agent's own report, not
trusted at face value: `allium check`/`allium analyse` byte-identical to
baseline (0 findings both, same 21/5 diagnostics), `allium plan` still
449 obligations with the expected shift (`+rule-failure.CreateExternalEvent.7`
for the new guard, `-rule-entity-creation.CreateExternalEvent.1` since
the planner doesn't emit that obligation for a conditionally-created
entity - already true of `RegisterProjection`/`RegisterEventType`/
`RegisterCommandType` in this same spec, not something this change
uniquely introduces).

**Storage**: `external_message_cursors`, one row per `(adapter_id,
partition_key)` pair, storing only `last_sequence`. A brand-new table,
so no migration dance the way [§37](#idempotency-keys-client-id-scoping)'s `idempotency_keys` retrofit needed -
`ensure_external_message_cursors_table`'s own `CREATE TABLE IF NOT
EXISTS` is the whole story, wired into `provision_bounded_context_schema`
and the `SkiljBuilder::build()` startup loop exactly like every other
table in this file. Scoped by `adapter_id`, not `event_type_name` - an
`ExternalEventToken` is already issued for exactly one `EventType`, so
scoping by adapter is at least as narrow, and the alternative would let
two unrelated adapters for the same event type collide on a partition
key string they each chose independently (the identical cross-tenant
collision class [§37](#idempotency-keys-client-id-scoping) closed for `idempotency_keys`, verified not
reachable here for real: `two_different_adapters_sharing_a_partition_key_string_do_not_collide`,
`skilj-core/tests/external_event_dedup.rs`).

**Mechanism**, entirely in `db::create_and_insert_external_event` -
`event_store::create_external_event`'s own pure signature is untouched,
exactly mirroring where `submit_command`'s own idempotency check lives
relative to `event_store::process_command`. Checked right after
`next_sequence`'s own row lock is acquired (the same "earliest race-free
point" `lookup_idempotency_key` already uses): a watermark hit rolls the
transaction back (nothing else was written) and returns
`CreateExternalEventOutcome::Redelivered`, without ever calling the pure
`create_external_event` function at all; a miss proceeds exactly as
before, plus one more write - `advance_dedupe_watermark`, an upsert
(`ON CONFLICT ... DO UPDATE`, since a given partition is written many
times over its life, unlike `insert_idempotency_key`'s plain
once-per-key insert) - before the same commit.

**Both-or-neither, satisfied structurally, not by a runtime check.**
`DedupeCursor<'a> { partition_key: &'a str, sequence: i64 }` has no
`Option` fields - there is no way to construct one with only one of the
two present. The REST wire mirrors this: `ExternalEventRequest.dedupe:
Option<DedupeRequest>`, and `DedupeRequest`'s own two fields are
non-optional, so a JSON body naming exactly one of them fails ordinary
deserialization (a 400) before the handler is ever reached. The spec's
own `requires` guard for this case is satisfied by construction here,
not duplicated as application logic.

**Deliberately not `SubmitCommandOutcome`'s shape reused.** A duplicate
idempotency-key hit returns the *original* `triggered_event_sequences` -
`CreateExternalEventOutcome::Redelivered` carries nothing, because a
watermark remembers only the highest sequence seen, not which event any
particular past message produced; inventing an answer would mean
storing per-message state again, the exact cost this design exists to
avoid. `skilj-rest`'s `POST /v1/events/external` response reflects
this: `{ sequence: Option<i64>, redelivered: bool }`, `sequence: null`
on a redelivery - and still a 201, not an error, since
`ARedeliveryProducesNoEventAndNoOutcome` is explicit that a replaying
adapter is doing exactly what an at-least-once source is supposed to
do.

**A cost stated plainly, not discovered later**: the watermark cannot
distinguish "redelivery" from "a message that genuinely arrived out of
order within its own partition" - both look identical (a sequence at or
below the highest already seen), and the latter is silently dropped
just the same. This is the trade an adapter makes by supplying the pair
at all; one whose source cannot promise in-partition ordering supplies
neither value and is never deduplicated. `allium:tend` documented this
in the rule itself as a stated cost of the design rather than raising it
as an open question, since it's inherent to the compactness asked for,
not something a different implementation choice would avoid for free.

**Verified**: `skilj-core/tests/external_event_dedup.rs` (5 tests
against `db::create_and_insert_external_event` directly - omitting the
pair changes nothing; a new higher sequence creates and advances the
watermark; a redelivered *or* stale-lower sequence creates nothing; two
adapters sharing a partition key string don't collide; two partitions
from the same adapter have independent watermarks) and
`skilj/tests/external_event_dedup.rs` (2 tests over the real REST wire,
mirroring `command_trigger.rs`'s own "layer in isolation, then the real
wire" split - the JSON `dedupe` shape and the `sequence`/`redelivered`
response fields, end to end through `Skilj::rest_router()`). The central
claim in both files - a redelivery creates nothing - was confirmed
non-vacuous the same way as every other fix this session: the check
disabled, the test rerun and shown to fail with the exact expected
assertion, then restored and reconfirmed green. Full existing
`skilj-core` test suite (every binary, real embedded Postgres) passes
unchanged. `cargo build/clippy -D warnings/fmt --check` clean
throughout.

<a id="skilj-kafka-bridge"></a>
## 40. `skilj-kafka`: a bridge to Kafka, both directions

[§38](#message-broker-bridges-investigation)'s own investigation ranked `rdkafka` as the most-downloaded, most
actively-maintained crate for the category the user's own example named
(Kafka); [§39](#external-message-dedup-create-external-event) closed the one real gap that would have made an inbound
bridge unsafe. This section builds the crate itself -
`skilj-temporal`'s ([§34](#skilj-temporal-plan)) direct sibling, same posture (wire-protocol
client only, zero dependency on any other skilj crate), one real
difference in shape: `skilj-temporal` only ever reacts to skilj's own
events (one direction), where a message broker genuinely needs both.

**Outbound** (`OutboundMapping`/`produce_once`/`run_outbound`): a skilj
`EventType` -> a Kafka topic, via the identical `GET
/v1/events/consume`/`POST /v1/events/consume/ack` delivery mechanism
`skilj_temporal::poll_once`/`run` already establish - produces to Kafka
*before* acknowledging to skilj, never the reverse order, so a crash
between the two redelivers the same event next cycle rather than
silently dropping it. Kafka's own analogue of a Temporal workflow id is
the *message key* (drives partition assignment) - derived from one of
the event's own DCB tags (`correlation_key`), the identical "derived
from a tag, not a fresh concept" register `correlation_workflow_id`
already uses, just without that function's own "no correlation tag is
an error" rule: an unkeyed Kafka message is still perfectly valid,
Kafka itself just gets to place it.

**Inbound** (`InboundMapping`/`dispatch_inbound_message`/`run_inbound`):
no equivalent in `skilj-temporal`. A Kafka topic maps to one of two
skilj actions, a per-mapping choice mirroring `MappingAction`'s own
signal-vs-start split - both now genuinely safe under Kafka's
at-least-once redelivery, which is precisely why [§39](#external-message-dedup-create-external-event) had to exist
before this could be built responsibly:

- `InboundAction::Record` - `POST /v1/events/external`, redelivery-safe
  via [§39](#external-message-dedup-create-external-event)'s own `dedupe` mechanism.
- `InboundAction::Trigger` - `POST /v1/commands/trigger`,
  redelivery-safe via `Idempotency-Key` ([§21](#optional-idempotency-key-submission)), itself `client_id`-scoped
  since [§37](#idempotency-keys-client-id-scoping) so this mapping's own traffic can never collide with an
  unrelated caller's.

Both derive their own redelivery-safety key identically:
`"{topic}:{partition}"` as the partition key, the message's own
`offset` as the sequence - Kafka's guarantee of strictly increasing,
in-order delivery within one partition is exactly the property both
mechanisms need, the same one `skilj_temporal`'s own
`"{run_id}:{activity_id}"` convention ([§34](#skilj-temporal-plan) phase 1) leans on one level
further out. The Kafka offset itself is committed (`run_inbound`) only
after skilj confirms the call succeeded, so a redelivery calls skilj
again rather than skipping the message - safe *because* the skilj-side
mechanisms make that redelivered call a no-op, not because this loop is
itself clever about it. A `Trigger` business rejection (`200 {
accepted: false, ... }`) is not inspected or retried - the message was
successfully delivered and decided upon, which is all this bridge ever
promises, so the offset still commits.

**Dependency and a real build-time snag, worked around, not routed
around**: `rdkafka` with the `cmake-build` feature (vendors and
compiles `librdkafka` from source - no system package needed). Hit
exactly the kind of environment quirk this project's own CONTRIBUTING.md
already tracks a few of: this librdkafka version `#include`s
`curl/curl.h` unconditionally in `rdkafka_conf.c`, even with
`WITH_CURL=0` passed - a real upstream quirk, confirmed by reading the
actual C source, not a Cargo feature misconfiguration. With no root
available in this environment: `apt-get download
libcurl4-openssl-dev` (no root needed) + `dpkg-deb -x` to extract just
the headers, `CPATH` pointed at them. See CONTRIBUTING.md's own new
note for the full recipe.

**Verified against a real, ephemeral Kafka broker, not mocked at the
protocol level that matters** - `testcontainers-modules`' `kafka`
feature (KRaft mode, no ZooKeeper), dev-dependency only. Getting Docker
itself reachable in this sandbox needed its own real investigation, not
an assumption: the `docker` CLI on `PATH` here is a wrapper script
hardcoding a stale WSL Docker Desktop `DOCKER_HOST` that no longer
resolves - `unset DOCKER_HOST` falls back to the real, working socket
at `/var/run/docker.sock`, confirmed with `docker run hello-world`
before trusting it for anything real. `skilj-kafka/tests/kafka_bridge.rs`
mirrors `skilj-temporal/tests/temporal_bridge.rs`'s own "mock skilj +
real external system" shape exactly - skilj's own wire contracts are
each already exhaustively tested elsewhere, so this crate's own job is
proving it calls them correctly with real Kafka messages on the other
end. Three tests: an outbound event round-trips through a real topic
with its own tag as the key, and both inbound actions carry a real
message's own real partition/offset as their dedupe/idempotency key -
not synthesised values, obtained by actually producing and consuming a
real message and reading its own fields back.

**A real concurrency finding along the way, fixed properly rather than
documented as a limitation**: an early draft started one Kafka
container per test (three total) rather than sharing one across the
file - unlike every embedded-Postgres-backed test suite in this
workspace, which already shares one `OnceCell`-provisioned instance per
file. Running three containers concurrently (`cargo test`'s own default
parallelism) genuinely starved this sandbox's Docker daemon
(`OperationTimedOut` on topic creation, reproduced across repeated
runs, not a one-off). Refactored to the established one-shared-instance
shape instead of just recommending `--test-threads=1` - each test still
gets its own uniquely-named topic, so nothing is lost by sharing the
broker, and the fix is also just faster (~7s for the suite, versus
20-60s per run before, with the multi-container version's own worst
case timing out entirely under load).

<a id="skilj-amqp-bridge"></a>
## 41. `skilj-amqp`: a bridge to any AMQP 1.0 broker (Solace/Azure Service Bus/Artemis)

[§38](#message-broker-bridges-investigation)'s own investigation named AMQP 1.0 (`fe2o3-amqp`, pure Rust) as the
practical path to Solace specifically, since no healthy Solace-only
Rust crate exists - the unofficial `solace-rs` needs Solace's own
proprietary C SDK installed separately and is essentially unused (142
downloads in 90 days, stale since May 2025). Solace PubSub+ natively
speaks AMQP 1.0 as a first-class protocol alongside its own
proprietary one, and so do Azure Service Bus and ActiveMQ Artemis - one
dependency covers all three via the shared open standard, the user's
own second choice of protocol to build after `skilj-kafka` ([§40](#skilj-kafka-bridge)).

**A genuinely different delivery model, not just a different
library.** Confirmed directly against `fe2o3-amqp`'s own real source
(`Properties`/`Delivery`/`Sender`/`Receiver` APIs), not assumed from
the concept: AMQP 1.0 has no partitions or broker-assigned offsets - a
queue/topic *address* instead, and messages that carry only whatever
metadata their own sender chose to set. The closest analogue of
Kafka's own broker-assigned `(topic, partition, offset)` is the AMQP
1.0 standard's own `group-id`/`group-sequence` message properties pair
(§3.2.4) - real and standard, but *opt-in*: nothing forces a sender to
populate them, unlike Kafka's own broker-guaranteed offsets. This
crate's own outbound half always sets them (skilj, not the broker,
assigns the sequence, so it always can); the inbound half can only use
them for [§39](#external-message-dedup-create-external-event)'s own `dedupe` mechanism when whatever upstream sender
populated a given address did too. `message-id` (also standard, far
more commonly populated in practice - often a UUID) is the fallback for
`Idempotency-Key` ([§21](#optional-idempotency-key-submission)), usable on its own without needing
`group-sequence`'s own ordering guarantee.

**A real protocol-level limit, documented rather than silently
handled**: AMQP 1.0's `group-sequence` is a 32-bit field
(`fe2o3_amqp_types::definitions::SequenceNo = u32`), unlike Kafka's own
64-bit offsets - confirmed against the actual type alias, not assumed.
A bounded context whose own sequence exceeds `u32::MAX` has
`produce_once` omit `group-sequence` rather than silently wrap it into
a value a downstream consumer could mistake for a genuine, smaller
ordering.

**`InboundMessageMeta`, not `(topic, partition, offset)`**: since every
field a message might carry is genuinely optional here (unlike Kafka's
own always-present triple), `dispatch_inbound_message` takes a plain
struct of `Option`s rather than deriving guaranteed values itself. A
`Record` action with no `group-id`/`group-sequence` pair, or a
`Trigger` action with no `message-id`, simply omits the corresponding
skilj mechanism (`dedupe`/`Idempotency-Key`) - never an error, the same
"omitting it is always fine, just not redelivery-safe" register both
mechanisms already have on skilj's own side.

**Dependency**: `fe2o3-amqp` with the `rustls` feature - the identical
TLS backend choice this workspace's own `reqwest`/`jsonwebtoken`
dependencies already make, needed for any real deployment (`amqps://`)
against Solace/Azure Service Bus.

**Verified against a real, ephemeral AMQP 1.0 broker** - Apache
ActiveMQ Artemis (`apache/artemis:latest-alpine`, `ANONYMOUS_LOGIN=true`
for the test's own simplicity), via a plain `testcontainers::GenericImage`
- no dedicated `testcontainers-modules` feature exists for any AMQP
broker, confirmed by checking that crate's own full feature list rather
than assuming one would exist the way it did for Kafka.
`skilj-amqp/tests/amqp_bridge.rs` mirrors `skilj-kafka/tests/kafka_bridge.rs`'s
own shape exactly, including its already-learned lesson: one shared
broker container for the whole file (`OnceCell`) from the start, not
one per test. Three tests: an outbound event round-trips with its own
DCB tag as `group-id` and its own skilj sequence as `group-sequence`;
both inbound actions read a real message's own real AMQP properties
(sent exactly as an upstream, non-skilj sender would) as their
dedupe/idempotency key.

**A second real bug caught by testing, immediately after the
concurrency lesson already learned from `skilj-kafka`**: every test
failed identically on a first run - `SessionStopped(ConnectionStopped(Closed))`
- because the test's own `connect()` helper returned only a
`SessionHandle`, dropping the `ConnectionHandle` (which implements
`Drop` specifically to close the connection) as soon as the helper
returned. Fixed by returning both handles and keeping both alive for
the test's own duration - a real Rust ownership bug in the test
harness, not the library, caught by running the tests for real rather
than assuming a compiling test proves anything.

<a id="skilj-nats-bridge"></a>
## 42. `skilj-nats`: a bridge to NATS JetStream

[§38](#message-broker-bridges-investigation)'s third named candidate (`async-nats` actually out-downloads
`rdkafka`, pure Rust, a much simpler ops story than either Kafka or an
enterprise AMQP broker - no ZooKeeper/KRaft, no broker cluster to run).
`skilj-kafka`'s ([§40](#skilj-kafka-bridge))/`skilj-amqp`'s ([§41](#skilj-amqp-bridge)) third sibling - a third
delivery model again, confirmed against `async-nats`'s own real source
(pulled locally, mirroring how both prior bridges were researched) and
a real example already inside the `testcontainers-modules` crate's own
test suite for the exact JetStream flow this bridge needed.

**Core NATS pub/sub is the wrong layer, on purpose not used at all** -
fire-and-forget, no redelivery concept whatsoever, which makes every
mechanism this whole family of bridges exists to use (`dedupe`, [§39](#external-message-dedup-create-external-event);
`Idempotency-Key`, [§21](#optional-idempotency-key-submission)) meaningless: there is nothing to guard against
redelivering if delivery was never guaranteed once. JetStream, NATS's
own persistence layer, is what actually gives an at-least-once
guarantee worth building around - `skilj-nats` speaks JetStream only.

**A third delivery model, not a copy of either prior one**: JetStream
has no partition concept at all - one stream is one ordered sequence,
addressed by subject. Every message a `PullConsumer` ever delivers
carries a real, broker-assigned `(stream, stream_sequence)` pair
(`Message::info()`, confirmed against the real `Info` struct - never
optional, closer to Kafka's own guaranteed-metadata story than AMQP's
sender-optional `group-id`/`group-sequence`). The one genuinely
optional piece is `Nats-Msg-Id` (a header) - used for `Idempotency-Key`
on the inbound `Trigger` path the same way AMQP's `message-id` already
is, and - a real, favourable difference from both prior bridges - by
this crate's own *outbound* half too, to get JetStream's own native,
server-side idempotent-publish deduplication for free
(`PublishAck.duplicate`): neither Kafka's producer-side idempotence
(unbounded for one producer session) nor AMQP (no built-in publish-side
dedup at all) offers this.

**Correlation, honestly not a routing mechanism here**: NATS has no
Kafka-style "key routes to a partition" concept - JetStream streams
aren't partitioned, so a DCB tag maps onto a plain
`Skilj-Correlation-Key` header instead, informational for whatever
downstream consumer wants to filter or group by it, not something this
crate or NATS itself acts on. Documented as a real, narrower thing than
Kafka's own key or AMQP's `group-id`, not oversold as equivalent.

**Redelivery safety**: outbound sets `Nats-Msg-Id` to
`"{bounded_context}:{sequence}"` (the same shape `skilj_temporal::signal_request_id`
already uses, one field narrower - a stream has no separate event-type
dimension to disambiguate); inbound `Record` always has `dedupe`
available (JetStream's own guaranteed `(stream, stream_sequence)`),
`Trigger` uses `Nats-Msg-Id` only when an upstream sender populated one -
the identical "omit rather than fabricate" register both prior bridges
already have, `InboundMessageMeta::from_message` made `pub` so a caller
holding a real delivery (a test, or code outside `run_inbound`'s own
loop) can build one directly.

**`run_inbound` takes one `InboundMapping`, not a lookup table** - a
genuine, structural simplification over `skilj_kafka::run_inbound`'s
own `HashMap<String, InboundMapping>`: a JetStream `PullConsumer` is
already bound to its own stream and subject filter at creation, unlike
an `rdkafka` consumer that can subscribe to several topics on one
connection, so there's no per-address dispatch this crate needs to do
itself.

**Verified against a real, ephemeral NATS server with JetStream
enabled** (`testcontainers_modules::nats`, which has a dedicated
feature unlike AMQP) - `skilj-nats/tests/nats_bridge.rs` mirrors both
prior bridges' own "mock skilj, real external system" shape, applying
the shared-container lesson from the very first draft this time (no
repeat of `skilj-kafka`'s own first-draft mistake). Three tests: an
outbound event publishes with its own DCB tag as
`Skilj-Correlation-Key` and `"{bounded_context}:{sequence}"` as
`Nats-Msg-Id`; both inbound actions read a real message's own real
JetStream metadata (sent exactly as an upstream, non-skilj sender
would) as their dedupe/idempotency key.

**One real bug caught immediately by actually running the test, not
just compiling it**: `InboundMessageMeta::from_message` was originally
private, written only for `run_inbound`'s own internal use - the test
file's own need to call it directly from outside the crate surfaced
that it needed to be `pub`, exactly the same "a real caller with a
delivery in hand needs this too" reasoning that shaped the rest of this
crate's own public API.

**Verified**: 3 unit tests (correlation-key derivation) + 3 real
end-to-end tests against a real ephemeral NATS+JetStream server, all
passing reliably across repeated runs, default and single-threaded
parallelism, and fast (under a second per run once the image is
cached) thanks to the shared-container pattern applied from the start.
Full workspace build/clippy -D warnings/fmt --check/allium check clean.

<a id="new-subscriber-replay-fix"></a>
## 43. Stopping a new subscriber from replaying all of history

Prompted by a design question, not a filed issue: "you add a new
`UserRegistered -> send a welcome email` event handler - how do you stop
it emailing every user who has ever registered?" Answered honestly first
(no existing mechanism prevents it) and then closed for real, on both of
skilj's own "a new reader starts consuming a stream" mechanisms:
`EventReadToken`'s server-tracked `ReadCursor` (rule `ConsumeEvents`) and
`CrossContextRoute`'s own durable cursor ([§36](#cross-context-route)). Both, until this
pass, unconditionally seeded a brand-new cursor at `position = -1` - the
very beginning of the stream - with no way to ask for anything else.
Wiring either mechanism to a real "send an email" side effect the day it
ships would have replayed every historical occurrence through it once.

**Spec** (`specs/skilj.allium`, delegated to `allium:tend`, independently
re-verified): a new `enum EventReadStartPosition { beginning | latest }`,
a new `EventReadToken.start_from` field, a new optional `start_from?`
parameter on `rule CreateEventReadToken` (`?? beginning` default - every
token minted before this argument existed keeps behaving exactly as it
always has), and `rule ConsumeEvents`'s own `position` binding rewritten
around a new `latest_position` binding: `highest_sequence` over this
token's own event type, scoped by `token.scope` exactly as a served
event already is (an event outside a token's scope was never visible to
it, so it can't count as "already seen" either) but deliberately blind
to the filters *this one call* happens to supply, since the seed is
decided once, at the token's first call, and must not depend on which
filter that particular call passed. A `latest` token's first
`ConsumeEvents` call therefore serves nothing at all - its cursor is
provisioned already past everything committed by then - and every call
after that is completely ordinary, indistinguishable from a `beginning`
token's.

**`CrossContextRoute`** has no spec entity of its own ([§36](#cross-context-route)'s "no spec
entity" note), so its side gets a separate, Rust-only
`plugin::CrossContextRouteStartFrom { Beginning, Latest }` rather than
reusing the spec-backed enum - deliberately two types, not one shared
between a spec-derived module and a plugin-only construct that owes it
nothing. `CrossContextRoute::START_FROM` is a defaulted associated
const (`= Beginning`), the same "part of the trait, not a builder
argument" register `NAME`/`Source`/`Target` already have, carried
through `CrossContextRouteInfo::start_from` into
`db::catch_up_cross_context_route`. `db::get_cross_context_route_cursor`
changed its return type from `i64` (a `-1` sentinel collapsing "never
ticked" and "ticked, seeded at -1" into one value) to `Option<i64>`,
because this fix needs to tell those two apart: `None` (no row at all)
on a `Latest` route's very first tick loads `Source`'s full history
once - the identical `list_events_cached(..., -1)` call the ordinary
path already makes - purely to find its highest sequence, seeds the
cursor there (or leaves it at `-1`, unchanged, if `Source` has no
occurrences yet), and returns *without dispatching a single one of
them*. Every tick after that reads a real cursor row and behaves exactly
as a `Beginning` route always has. A `Beginning` route's own first tick
is untouched - the `None` branch only special-cases `Latest`, so no
existing route's behaviour on upgrade changes at all.

**Migration**: `access_tokens.start_from TEXT NOT NULL DEFAULT
'beginning'`, `ensure_event_read_token_start_from_column` - the same
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, called unconditionally on
every `build()`, every prior schema-evolution pass in this codebase
already uses (`ensure_event_scoping_columns` et al.). `CrossContextRoute`
needed no migration at all - `cross_context_route_cursors` already had
no `start_from` column to add, since the seeding decision lives on the
Rust-only `CrossContextRouteInfo`, never persisted.

**GraphQL**: `createEventReadToken` gained a `startFrom:
EventReadStartPosition` argument and `EventReadToken` a `startFrom`
output field. This is the one place the fix cost real structural churn:
`createEventReadToken` had, one pass ago, been folded into
`create_type_token_field!` - the macro shared with
`createExternalEventToken`/`createDirectCreationToken`/`createCommandToken` -
on the strength of all four `create_*_token` functions taking an
identical parameter list. `start_from` broke that premise for
`createEventReadToken` alone, so it went back to being hand-written (see
`event_type_admin_operations::create_event_read_token_field`'s own doc
comment for the full history) - the second time this exact field has
swapped between "shared macro" and "hand-written exception" as the
underlying token shapes diverged and reconverged.

**A one-time cost accepted deliberately, not overlooked**: both
mechanisms' seeding step loads full history once to compute a highest
sequence, then discards all of it - exactly the same read the ordinary
`Beginning` path already pays for on a first call/tick, just without
serving what it loads. Unlike `db::latest_sequence`'s own cheap-query
optimisation (`catch_up_bounded_context`'s first check every poll tick,
so a quiet context costs one small aggregate query rather than a full
reload - see that function's own doc comment), which exists specifically
because that check repeats on every idle poll tick forever, this seeding
step runs exactly once, ever, per token/route
- a fundamentally different cost profile that doesn't justify the extra
machinery a dedicated `SELECT MAX(sequence) WHERE event_type_name = $1`
query would add.

**Verified**: `skilj-core/tests/token_lifecycle.rs`
(`create_event_read_token_honours_an_explicit_start_from` plus the
existing success test asserting the `?? beginning` default),
`skilj-core/tests/event_fetch_surface.rs`/`persistence.rs` (updated
fixtures/round-trip), `skilj/tests/event_fetch_rest.rs`
(`a_latest_token_never_serves_history_that_predates_its_own_minting` -
real REST `POST /v1/events/direct` then `GET /v1/events/consume`, two
historical deposits before minting, first call serves zero, a
post-minting deposit is served alone), `skilj/tests/
graphql_type_registration.rs` (`createEventReadToken` over real GraphQL,
both the default and an explicit `LATEST`), `skilj/tests/
cross_context_route.rs`
(`a_latest_route_never_dispatches_history_that_predates_its_own_registration` -
two real `Skilj::builder()` calls against the same two bounded contexts,
a historical `OrderShipped` posted between them, the `Latest`-registered
route's projection never reflects it, a later occurrence still does).
`cargo build/clippy -D warnings/test --workspace` and `cargo fmt --check`
clean; `allium check` unchanged from baseline bar one checker-limitation
warning on the new enum (an enum referenced only from a `variant` field,
not an `entity` one, per allium 3.5.3 - confirmed on a minimal scratch
spec, not a real defect); `allium analyse` byte-identical to baseline.

**Extension, same session: offset- and time-based starting positions.**
A follow-up question - "is it possible to subscribe from a specific
offset or time?" - closed the same gap `beginning`/`latest` leave open:
neither can replay *some* history from a caller-chosen point, only none
of it or all of it. `EventReadStartPosition` gained `at_sequence`/
`at_time`, `EventReadToken` two new nullable fields
(`start_at_sequence: Integer?`/`start_at_time: Timestamp?`), and
`CreateEventReadToken` two new optional parameters plus three `requires`
guards (spec delegated to `allium:tend` again, independently
re-verified): `at_sequence` demands `start_at_sequence` and forbids
`start_at_time`, `at_time` demands the reverse, and `beginning`/`latest`
forbid both - naming one without its value, or with the other one's, is
refused outright (`Error::StartAtSequenceMismatch`/`StartAtTimeMismatch`/
`StartAtValueNotAllowed`) rather than silently resolved to some default.
`rule ConsumeEvents` gained a sibling `at_time_position` binding next to
`latest_position` (identical `history`/scope treatment, plus `e.metadata.
created_at <= token.start_at_time`); `at_sequence`'s own position is the
caller's value directly, unvalidated, the same opaque-value treatment
`scope` already gets. An empty `at_time_position` (nothing committed at
or before the cutoff) resolves to `-1` - exactly `beginning`'s own
behaviour, not a special case.

The real distinguishing value over `latest`, proved by both new pure and
end-to-end tests: `at_sequence`/`at_time` can replay history from a
chosen *mid-history* point - including occurrences that already existed
before the token/route was ever minted/registered - which `latest`
structurally cannot (it only ever starts at "whatever's already there
right now") and `beginning` over-serves (everything). `CrossContextRoute`'s
own `CrossContextRouteStartFrom` grew matching `AtSequence(i64)`/
`AtTime(i64)` variants - `i64` (Unix seconds), not `chrono::DateTime<Utc>`,
because `START_FROM` is a trait associated const and `DateTime` has no
`const fn` constructor to build one from at the implementing type's own
definition site; converted to a real `DateTime<Utc>` only where compared
against event timestamps. `db::catch_up_cross_context_route`'s seeding
branch (previously `Latest`-only) now covers all three non-`Beginning`
variants uniformly: `AtSequence(n)` seeds the cursor at `n` directly (no
event load needed, though one happens anyway for one shared code path
rather than a fourth special case - a one-time cost, same reasoning as
above); `AtTime`'s own seed mirrors `at_time_position`, minus the scope
filter `CrossContextRoute` has no concept of.

**GraphQL**: `createEventReadToken` gained `startAtSequence: Int`/
`startAtTime: String` arguments and matching output fields on
`EventReadToken` - `Int`/`String` (an RFC3339 timestamp), the same shape
`queryEvents`'s own `afterSequence`/`fetchCommands`'s own `after`/
`before` already use, no custom scalar existing in this schema for
either. `parse_rfc3339` moved from `command_query.rs` (its only prior
caller) to `resolvers/mod.rs` once this became its second - the same
"promoted once a second call site needs it" move `parse_missed_occurrence_policy`/
`create_type_token_field!` already went through in earlier passes.

**Verified**: `skilj-core/tests/event_fetch_surface.rs`
(`consume_events_at_sequence_starts_strictly_after_the_given_sequence`,
`consume_events_at_time_starts_strictly_after_events_at_or_before_that_cutoff`,
`consume_events_at_time_with_no_events_before_the_cutoff_serves_everything`),
`skilj-core/tests/token_lifecycle.rs` (six new tests: both success
shapes, all three `requires` guards' own failure shapes),
`skilj/tests/event_fetch_rest.rs`
(`an_at_sequence_token_replays_history_after_a_chosen_cutoff_but_not_before_it`/
`an_at_time_token_replays_history_after_a_chosen_cutoff_but_not_before_it` -
real REST, real wall-clock gaps around the `at_time` cutoff to rule out
same-second flakiness, both proving mid-history replay: a deposit at or
before the cutoff excluded, one already historical but after it
replayed, a genuinely new one afterward served normally),
`skilj/tests/graphql_type_registration.rs` (`AT_SEQUENCE`/`AT_TIME` over
real GraphQL, plus the mismatch-rejection wire path), `skilj/tests/
cross_context_route.rs`
(`at_sequence_and_at_time_routes_replay_from_their_own_chosen_cutoffs` -
two routes against one shared source, independently cursored, each
proving its own seeding branch for real). One real bug caught by
actually running this last test, not just compiling it: the two new
target command/projection types were first written reusing the earlier
`Latest` scenario's own shared `InventoryEvent`/`BoundedContextEvent`
impl (which matches the literal event-type name `"StockReserved"`) -
harmless there because that scenario's own event type kept that exact
literal name in its own separate bounded context, but silently wrong
here, where `StockReservedAtSequence`/`StockReservedAtTime` need their
own distinct names to coexist in one bounded context and therefore their
own matching `BoundedContextEvent` impls; every event still got created
correctly, but silently never folded into either projection, since
`try_from_event` never matched either name - a projection-state lookup
returning `None` where a real fold had already committed, not a panic
or a compile error, so it only surfaced once the test's own assertions
ran for real, not from `cargo build` alone. `cargo build/clippy -D
warnings/test --workspace` and `cargo fmt --check` clean; `allium check`
unchanged from baseline (0 errors, same warning/info set as §43's own
first pass); `allium analyse` byte-identical to baseline.

---

<a id="correlation-causation-ids"></a>
## 44. Correlation/causation ids on commands and events (Codeberg issue #18)

Raised comparing skilj against Axon Framework and other event-sourcing
frameworks for gaps: nothing recorded "which command caused this event"
or "which business transaction this belongs to" as a durable, queryable
field. `Event.origin`'s `CommandTriggered` variant already links an
event back to the `Command` that produced it, but that's a single hop -
nothing threaded an id across a longer chain (a submitted command → the
events it triggers → a `CrossContextRoute`-triggered command in another
bounded context → more events). Deliberately distinct from the OTel
trace id already threaded on REST/GraphQL error responses (§10b) - a
trace id is ephemeral and exporter-dependent; these are durable, written
into the row, and queryable from the store itself years later.

### Spec (`specs/skilj.allium`)

`value Metadata` gained `correlation_id: String?`/`causation_id: String?`,
with a note distinguishing both from OTel tracing explicitly. A new
shared black box, `valid_correlation_id(id) -> Boolean` (`None`/empty
vacuously valid, otherwise ≤ 200 characters, no charset restriction -
deliberately looser than `valid_bounded_context_name`, since this is
arbitrary caller/bridge-supplied trace data, never embedded as an
identifier anywhere), gates both fields wherever they're set.
`CommandSubmission`/`CommandTrigger`/`ExternalEventIngestion`/
`DirectEventCreation` all gained the pair on their own `provides:` facts
(the last two needed it too - a message-broker bridge attaches an
upstream trace id exactly there), each with its own `@guarantee`.
`ProcessCommand`'s `ensures:` stamps `Command.created(...)` with
`correlation_id ?? generate_id()` (every stored command ends up with
one) and `causation_id` passed through as-is (`None` for a plain
submission); every `CommandTriggered.created(...)` in the same rule
inherits the command's own `correlation_id` and gets
`causation_id: command.id`. `CreateExternalEvent`/`CreateDirectEvent`
generate when absent identically; `CreateSystemEvent` always generates
(a scheduler tick has no caller to inherit from). A new invariant,
`CorrelationIdIsAlwaysRecorded`, checks `correlation_id != null` across
every stored `Command`/`Event` - deliberately *not* extended to
`causation_id`, whose whole point is that `null` is a true, common
statement ("this is a root"). `allium check`/`allium analyse` both
unchanged from baseline after the full spec pass.

### Core types and engine (`skilj-core`)

`shared::Metadata` gained the two `Option<String>` fields (honest at the
type level regardless of the "always generated" guarantee, since a row
written before this existed genuinely has neither).
`event_store::valid_correlation_id`/`CORRELATION_ID_MAX_LEN` (200) and a
new `Error::CorrelationIdTooLong` variant implement the spec's black
box. A new `event_causation_id(event: &Event) -> String` (
`"{bounded_context}:{sequence}"`) is the stable string a causing `Event`
is named by, since `Event` deliberately has no synthetic id of its own
the way `Command.id` does. `process_command`, `create_external_event`,
`create_direct_event`, `authorise_command_submission`/
`authorise_command_trigger` (whose `CommandAuthorised` fan-in struct now
carries both fields through to `process_command`), `submit_command`, and
`decide_and_submit_command` all gained `correlation_id: Option<&str>`/
`causation_id: Option<&str>` parameters, threaded end to end.

**`CrossContextRoute`'s own attachment point** (`db::catch_up_cross_context_route`,
§36): the routed command's `correlation_id` is forward-carried from the
source event's own (always-present) one, and its `causation_id` is
`Some(event_causation_id(source_event))` - replacing the previous
`client_id: "cross-context-route"` with no upstream link at all with a
real, traceable answer to "what caused this."

### Persistence

`events`/`commands` each gained `metadata_correlation_id`/
`metadata_causation_id TEXT` columns (both in the fresh-provision
`CREATE TABLE` and via a new `ensure_correlation_causation_columns`
patch for an already-provisioned bounded context, the same
`ensure_event_scoping_columns`-style idiom every prior additive column
in this schema uses - no advisory-lock/PK-swap machinery needed, this is
purely additive and nullable). A partial index on each
(`WHERE metadata_correlation_id IS NOT NULL`) backs the new query
surface below.

### Wire contracts

GraphQL: `submitCommand` gained optional `correlationId`/`causationId`
arguments and echoes the resolved `correlationId` back on
`SubmitCommandPayload` (`None` for a rejection or a deduplicated
outcome - the latter has no fresh `Command` to read one off of).
`EventMeta` gained `correlationId`/`causationId` fields, falling out
"for free" wherever `Metadata` is already surfaced. The deliberately
narrow `MatchingEvent`/raw `EventSubscription` tuple stay unchanged, per
their own already-documented "narrow on purpose" reasoning.

REST: `ExternalEventRequest`/`DirectEventRequest`/`CommandTriggerRequest`
each gained optional `correlationId`/`causationId` body fields;
`CommandTriggerResponse` echoes `correlationId` back identically to
GraphQL's `SubmitCommandResult`; `MetadataDto`/`EventDto` expose both on
every existing read path.

### Query-by-correlation-id

Checked first: the existing generic `Filter`/`filters=` mechanism only
ever matches against `event.payload` (`matches_filters` parses it as
JSON) - it has no access to `Metadata` at all, so it couldn't be reused
as-is. Instead, `query_events`/`count_events`/`fetch_commands`/
`fetch_events` each gained a `correlation_id: Option<&str>` parameter,
the same "typed filter parameter" shape `fetch_commands` already used
for `metadata.created_at` - wired through GraphQL's `queryEvents`/
`countEvents`/`fetchCommands` and REST's `GET /v1/events?correlationId=`.

### Message-broker bridges

All three (`skilj-kafka`/`skilj-amqp`/`skilj-nats`) gained symmetric
inbound/outbound plumbing. Causation has no standard protocol concept in
any of them, so all three use a custom `Skilj-Causation-Id`
header/application-property. Correlation prefers each protocol's own
native concept where one exists:

- **`skilj-amqp`**: AMQP 1.0's standard `correlation-id` message
  property (never read or set before this pass) is used directly;
  causation rides in `application-properties` (no standard field for
  it).
- **`skilj-kafka`**: no native correlation concept in the protocol
  (headers are fully custom either way) - `Skilj-Correlation-Id`/
  `Skilj-Causation-Id` headers, symmetric with AMQP's naming. Not to be
  confused with the pre-existing `correlation_key` (an unrelated
  DCB-tag-derived Kafka *message key* for partition routing).
- **`skilj-nats`**: a real naming collision to resolve - `Skilj-Correlation-Key`
  already existed (a DCB-tag-derived header for consumer-side
  filtering/grouping, unrelated to a business-transaction id). Resolved
  by using `Skilj-Correlation-Id` (`-Id`, not `-Key`) for the new
  concept, with cross-referencing doc comments at both header constants
  so the two are never conflated again.

All three: an inbound message with no correlation header simply omits
the field on the `ExternalEventRequest`/`CommandTriggerRequest` the
bridge posts - skilj-core's own generate-if-absent behaviour takes over
from there, no bridge-side generation logic needed.

### Tests and verification

A new `skilj-core/tests/correlation_causation.rs` (12 tests, pure/no-DB)
covers generation-when-absent, verbatim preservation, inheritance across
every `CommandTriggered` event, the length-cap rejection on both fields,
and `event_causation_id`'s composed string. `skilj/tests/
cross_context_route.rs`'s own end-to-end test gained real-Postgres
assertions that a routed command's `correlation_id`/`causation_id`
actually match the source event's, not just that the pure functions
agree. `skilj/tests/graphql_business_surfaces.rs` and `skilj/tests/
event_fetch_rest.rs` each gained a real end-to-end round trip
(submit/create with an explicit id, read it back via `queryEvents`/
`fetchCommands`/`GET /v1/events?correlationId=`). All three bridge
crates' own real-broker integration tests gained one new case each
(outbound header propagation via the existing round-trip test, plus a
new dedicated inbound-forwarding test).

`cargo build/clippy -D warnings --workspace --all-targets` and `cargo
fmt --check` clean. `cargo test -p skilj-core` (real embedded Postgres,
~710 tests across every test binary) fully green. Running `-p skilj`'s
own real-Postgres integration suite under this sandbox's default test
parallelism intermittently hits `Database(PoolTimedOut)` inside
`Skilj::builder().build()` - confirmed, by `git stash`-ing this entire
change and rerunning the identical failing test against the unmodified
baseline, to be a **pre-existing environmental resource-contention
artifact** (many concurrent embedded-Postgres instances exhausting this
sandbox's connection/FD budget under `cargo test`'s default
parallelism, compounded by leftover orphaned `postgres` processes
accumulating across a long session), not a regression this pass
introduced - every test that hit it passes cleanly in isolation
(`--test-threads=1` or run alone), including the newly-added ones.

---

<a id="given-when-then-test-fixture"></a>
## 45. A given/when/then test fixture for decide()/project() (Codeberg issue #19)

Raised comparing skilj against Axon Framework (`AggregateTestFixture`),
Occurrent's Decider testing DSL, and Disintegrate's `Decision` testing
support - every one of these ships an in-memory harness for a plugin
author's own business logic. skilj had none: `skilj-demo`'s own tests
exercised `decide()`/`project()` only indirectly, through a real
Postgres-backed `Skilj` instance and, for commands, a full HTTP/GraphQL
round trip - the right level for testing skilj's own engine
(persistence, DCB conflict handling, access control), but heavy and slow
for a plugin author who just wants to know whether their own
`decide()`/`project()` produces the right answer.

### Scope: deliberately narrower than it first sounds

The issue's own proposal sketch mentions reusing
`consistency_boundary_and_matching_events` to build the `matching_events`
slice from raw, tagged events. Looked at closely, that machinery belongs
to a different layer than this issue is actually about: `CommandType::
decide()`/`Projection::project()` already take `&[Self::Event]` - the
bounded context's own generated, *already-filtered, already-typed* event
enum, never a raw `Event` with tags. Deriving a command's consistency
tags from its payload and filtering a bounded context's full history
down to that slice is real work, but it's `skilj`'s own engine's job
(`db::process_command_transaction`'s real caller of
`consistency_boundary_and_matching_events`, §19's own redispatch path),
already covered end to end by `skilj-core/tests/submit_command.rs` and
`skilj-demo`'s real-Postgres integration suite. Rebuilding that here,
from a plugin author's own test file, would just be a second, slower
copy of coverage that already exists - not what a plugin author testing
their *own* logic in isolation needs.

So `skilj-test-fixture`'s `GivenEvents` takes the "given" events exactly
as `decide()`/`project()` would see them - `T::Event` values, given
directly - and calls straight through to `T::decide()`/`T::project()`.
No raw `Event`, no tags, no DCB boundary computation, no database, no
HTTP. The events a test gives *are* the matching events, by construction
- an honest, narrower promise than "replicates skilj's own filtering",
stated as such in the crate's own doc comment rather than glossed over.

### A new crate, not a feature-gated module in `skilj-core`

The issue floated both. A separate crate (`skilj-test-fixture`, the same
"depends on `skilj-core` directly" posture `skilj-inspector` already has,
§14) won out: it publishes to crates.io on its own cadence alongside the
other 12 (RELEASING.md), and a consumer adds it as an ordinary dev
dependency with no Cargo feature to remember to enable - simpler for the
actual audience (a plugin author's own `[dev-dependencies]`) than
threading a `test-fixture` feature through `skilj-core` and hoping every
downstream `Cargo.toml` opts in consistently.

### API shape

Two builders, one per plugin trait, both `.event(T::Event)`/`.events(...)`
then a terminal call:

- `command::GivenEvents<T: CommandType>` - `.when(payload: T::Payload)`
  returns a `CommandOutcome` wrapping `T::decide()`'s own
  `CommandDecision`. `.then_accepted(expected: Vec<EventSpec>)` asserts
  `Accepted` and that every event's `event_type`/`payload` matches, in
  order; `.then_rejected(expected_kind: &str)` asserts `Rejected` and
  checks `kind` only - `reason` is human-facing prose, not something a
  test should pin to exact wording. `.then(FnOnce(&CommandDecision))` is
  an escape hatch for anything else.
- `projection::GivenEvents<T: Projection>` - folds every given event into
  `T::State::default()` via `T::project()`, under the single default key
  (`Projection::keys`'s own default `""`) - a projection whose `keys()`
  fans one event across several instances isn't covered by this pass,
  same "real, unbuilt, out of scope" register as the tag-derivation gap
  above. `.then_state(expected: T::State)` asserts the folded state
  matches; `.then(FnOnce(&T::State))` is the same escape hatch.

Both assertion paths compare via `serde_json::Value` (`serde_json::
to_value` on both sides) rather than requiring `PartialEq`/`Debug` on
`T::Payload`/`T::State` beyond what `CommandType`/`Projection` already
demand - `AccountBalanceState` in `skilj-demo` needed no changes at all
to work with `then_state`. A mismatch panics with both sides
pretty-printed, so a failing test reads like a diff.

### Tests and adopter proof

`skilj-test-fixture`'s own `tests/fixture.rs` (9 tests) proves the
builder/assertion logic itself against a tiny hand-rolled bounded
context, including `#[should_panic]` cases for every mismatch path.
`skilj-demo/tests/banking_fixture.rs` (5 tests) is the real adopter: the
same `DepositMoney`/`WithdrawMoney`/`AccountBalance` scenarios `tests/
banking.rs`'s real-Postgres suite already covers, run purely in-process
here - no `Skilj::builder().build()`, no embedded Postgres, no HTTP.

`cargo build/clippy -D warnings --workspace --all-targets` and `cargo
fmt --check --all` clean.

### Left for later

The issue's own closing note - pairing this with the `skilj` Claude Code
skill (Codeberg #3) as the natural next thing it points a plugin author
at once they've added a `CommandType`/`Projection` - is a real, wanted
follow-up (the skill package now mentions `skilj-test-fixture` as a
pointer), not fully built out into a worked example inside the skill
itself yet.

<a id="native-deadlines"></a>
## 46. A native one-shot, per-entity deadline/timer (Codeberg issue #20)

Raised comparing skilj against Axon Framework's `DeadlineManager` -
schedule a one-off timer tied to a *specific* entity/tag ("cancel this
order if not paid within 30 minutes"), cancellable if the awaited thing
happens first. The existing scheduler (`rule CreateSystemEvent`/`rule
SkipMissedOccurrences`, [§22](#background-polling-and-startup-scaling)) is cron-based and global: it fires the
same system event on a recurring schedule for a whole `EventType`, not a
one-shot timer scoped to one entity's tags. Today, "cancel this if
nothing happens within N minutes" either had to be hand-built on top of
that recurring scheduler, or meant reaching for `skilj-temporal` ([§34](#skilj-temporal-plan)) -
a much heavier dependency for what's usually a single deferred command.

**The trigger-model question, resolved with the user before building**:
Axon's `DeadlineManager` is called imperatively from inside a command
handler. skilj's `decide()` is a pure function whose only output is
`CommandDecision` (`Accepted { events }`/`Rejected`) - no side-effect
channel exists there or should be added. Two shapes were on the table:
an event-reactive Rust-only plugin trait (mirroring [§36](#cross-context-route)'s
`CrossContextRoute`: `decide()` stays pure, a background poller does the
actual write, no new wire surface) versus a wire-exposed `scheduleDeadline`/
`cancelDeadline` GraphQL mutation/REST endpoint any authenticated caller
invokes directly (closer to the issue's own literal `ScheduleDeadline {
fire_at, tags, ... }` notation, but a materially bigger lift: a new spec
entity, its own access-control/owner-tag design, a new way for a caller
to flood the `deadlines` table). The user picked the event-reactive trait
- no new spec entity, reuses every piece of already-proven `CrossContextRoute`
machinery, and keeps "what happens next" answerable purely from committed
history, the same register [§36](#cross-context-route) already settled for cross-context
routing.

**Cancellation, also resolved with the user**: by tag, not by id. The
cancelling event (`OrderPaid`, say) only ever carries its own payload and
tags - never an opaque id a separate `ScheduleDeadline` reactor generated
deep inside its own row insert - so cancel-by-tag is what actually lets
"whichever happens first" work without threading an id back out through
some side channel the domain model has no natural place for.

### Design

Two new plugin traits alongside `CrossContextRoute` in
`skilj_core::plugin`:

```rust
pub struct DeadlineSpec<P> {
    pub fire_at: DateTime<Utc>,
    pub tags: Vec<Tag>,   // scopes this deadline for a later cancel-by-tag lookup
    pub payload: P,       // Target command payload, submitted when it fires
}

pub trait ScheduleDeadline {
    type Source: EventType;
    type Target: CommandType;
    const NAME: &'static str;
    const START_FROM: DeadlinePollStartFrom = DeadlinePollStartFrom::Beginning;
    fn schedule(source_payload: &<Self::Source as EventType>::Payload)
        -> Option<DeadlineSpec<<Self::Target as CommandType>::Payload>>;
}

pub trait CancelDeadline {
    type Source: EventType;
    type Deadline: ScheduleDeadline;   // which schedule's own pending rows this targets
    const NAME: &'static str;
    const START_FROM: DeadlinePollStartFrom = DeadlinePollStartFrom::Beginning;
    fn cancel_tags(source_payload: &<Self::Source as EventType>::Payload) -> Option<Vec<Tag>>;
}
```

`DeadlinePollStartFrom` is a small new enum, identical in shape to
`CrossContextRouteStartFrom` (`Beginning`/`Latest`/`AtSequence(i64)`/
`AtTime(i64)`) but its own type rather than a reuse - the same reasoning
`CrossContextRoute` itself already gives for not reusing
`EventReadStartPosition`: no spec entity to hang a shared type off, and a
name that says what it's actually for rather than one whose own doc
comment is written entirely in terms of routes. `CancelDeadline::Deadline`
is what stops two unrelated schedules that happen to reuse a tag key
(e.g. two different features both tagging `order:123`) from
cross-cancelling each other's rows - cancellation is always scoped to one
named schedule's own pending rows, never "every pending row with this
tag."

Type-erased dispatchers (`ScheduleDeadlineDispatcher`/`CancelDeadlineDispatcher`,
each with an `*Info` struct carrying the registration's static shape)
mirror `CrossContextRouteDispatcher`/`CrossContextRouteInfo` exactly -
`skilj/src/lib.rs`'s `ScheduleDeadlineDispatcherImpl`/`CancelDeadlineDispatcherImpl`
are thin registry wrappers, the same shape `CrossContextRouteDispatcherImpl`
already is.

**Storage** - one new per-bounded-context table, `deadlines`, provisioned
via `db::ensure_deadlines_table` (the `CREATE TABLE IF NOT EXISTS` idiom
`ensure_idempotency_keys_table`/`ensure_cross_context_route_cursors_table`
already use - called from both `provision_bounded_context_schema`, a
brand-new bounded context, and the startup warm-up loop in
`skilj/src/lib.rs`, an already-provisioned one). `id` is **deterministic**
- `"{schedule_name}:{source_event_sequence}"` - inserted with `ON
CONFLICT (id) DO NOTHING`, the same "redelivery of the same occurrence is
a safe no-op" property `CrossContextRoute`'s idempotency key already
gives its own command submissions, applied here to the row insert
itself. A second, shared `deadline_cursors` table (one row per registered
`ScheduleDeadline::NAME`/`CancelDeadline::NAME`) holds the durable
per-reactor read position - kept separate from `cross_context_route_cursors`
rather than piggybacked on it, since these are a genuinely different
reactor family even though the cursor shape (`{owner} -> last_dispatched_sequence`)
is identical. `tags` is GIN-indexed the same way [§19](#optional-snapshotting-matching-events) Problem 1 already
indexes the `events` table's own `tags` column, backing `catch_up_cancel_deadline`'s
tag-containment lookup.

**Three background pollers**, spawned from `SkiljBuilder::build()` the
same `tokio::spawn` + `for_each_concurrent(BACKGROUND_TASK_CONCURRENCY, ...)`
shape every other background task here already uses, all three sharing
one `deadline_poll_interval` builder knob (default 500ms, matching every
sibling default):

1. `db::catch_up_schedule_deadline` - per registered `ScheduleDeadline`,
   walks `Source` events after its own cursor; `schedule()` returning
   `Some(spec)` inserts a `pending` row, `None` just advances the cursor
   - identical control flow to `catch_up_cross_context_route`.
2. `db::catch_up_cancel_deadline` - per registered `CancelDeadline`,
   walks its own `Source` events after its own cursor; `cancel_tags()`
   returning `Some(tags)` runs a tag-containment `UPDATE ... SET status =
   'cancelled'` scoped to `Self::Deadline`'s own `schedule_name` -
   cancelling zero, one, or several matching pending rows is all a
   legitimate outcome, the same register `route() -> None` already is.
3. `db::fire_due_deadlines` - **not** tied to any one registered type,
   unlike the two above: it scans every bounded context's own `deadlines`
   table directly (`WHERE status = 'pending' AND fire_at <= now()`), so a
   row fires regardless of whether the `ScheduleDeadline` that created it
   is still registered in this process - the same "every instance does
   the same redundant, idempotent work" register [§22](#background-polling-and-startup-scaling) already
   established. Deliberately no `FOR UPDATE SKIP LOCKED` row-claiming
   here either: two instances racing to fire the same row both submit
   under the identical idempotency key (below), so the second is a
   harmless `Deduplicated`, and both marking the row `fired` afterward is
   a harmless no-op the second time (`WHERE status = 'pending'` guards
   the update). A due row whose `target_command_type` isn't registered at
   all is marked `fired` without ever submitting - logged, not retried
   forever, the identical stance `catch_up_cross_context_route` already
   takes for its own "target `CommandType` isn't registered" case. A
   `Target` command that *is* submitted but gets rejected by its own
   `decide()` is marked `fired` too - a legitimate business outcome
   (deciding whether a deadline is still relevant happens at fire time,
   against current state, exactly the reasoning `CrossContextRoute::Target`
   already gives for firing a command rather than a raw event), not a
   reason to retry.

Firing goes through the existing `db::decide_and_submit_command` ([§36](#cross-context-route)),
idempotency-keyed with a new sibling reserved prefix,
`RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX` (`"skilj-deadline:"`,
`event_store::reject_reserved_idempotency_key` extended to check both
prefixes) - the exact §36/§37 pre-plant-vulnerability fix, applied
proactively here rather than found after the fact. Not load-bearing
either, for the identical reason `RESERVED_IDEMPOTENCY_KEY_PREFIX`'s own
doc comment gives: this call's own `client_id` (`"deadline"`) is
server-derived, never caller-suppliable, so `idempotency_keys`'
`client_id`-scoping ([§37](#idempotency-keys-client-id-scoping)) already puts every key it writes in a partition
no external caller's own submission ever lands in. Kept anyway as the
same harmless defense-in-depth register every other internal caller here
now gets.

**No spec entity** - like `Snapshot`/`CrossContextRoute`, this is a
Rust-only construct layered on top of DCB, not a change to DCB's own
model or the Allium spec.

### Verified

`skilj/tests/deadlines.rs`, a real end-to-end test against Postgres: one
bounded context, three orders sharing one `ScheduleOrderCancelDeadline`
(`OrderPlaced -> CancelOrder`) and a second, independent
`ScheduleReminderDeadline` (`OrderPlaced -> SendReminder`) tagged
identically by order id. Order A is paid before its own deadline - its
`CancelOrder` deadline is cancelled by `CancelOrderDeadlineOnPaid` and
never fires, while its *reminder* deadline (a different schedule sharing
the same tag) still fires, proving `CancelDeadline::Deadline`'s own
scoping. Order B is never paid - its `CancelOrder` deadline fires
normally. Order C has `schedule_cancel_deadline: false` - `ScheduleOrderCancelDeadline::schedule`
itself decides the occurrence doesn't apply, so no `CancelOrder` deadline
is ever scheduled at all, while its own reminder still fires unaffected.
Final projection state proves all three combinations at once: reminders
fired for A/B/C, cancellations only for B. `cargo build/clippy -D
warnings/test --workspace` and `cargo fmt --check` clean.

**Not covered by a dedicated test**: a real crash/redelivery simulation
proving `ON CONFLICT (id) DO NOTHING` makes a redelivered schedule
catch-up tick a safe no-op. That property is structural, not incidental
- the identical mechanism (a deterministic id/idempotency key,
`Deduplicated` handled as a warning, not an error) `CrossContextRoute`
already relies on and which [§36](#cross-context-route)/[§37](#idempotency-keys-client-id-scoping) reasoned about at length - and
`cross_context_route.rs`'s own three tests don't carry a dedicated
crash-simulation test for it either, for the same reason this pass
doesn't: the black-box, full-`Skilj` harness every test file here uses
has no fault-injection hook to force a real redelivery, only ever
exercising the ordinary, non-redelivered path.

<a id="parked-deliveries"></a>
## 47. Dead-letter/parking for failed event/message handler delivery (Codeberg issue #21)

The gap the issue named: a persistently-failing delivery had no
operator-visible surface anywhere in this codebase. `CrossContextRoute`'s
own catch-up loop ([§36](#cross-context-route)) retried a failing target
command forever, on every single poll tick, blocking that route's cursor
- and therefore every occurrence behind it - indefinitely. Each of
`skilj-kafka`/`skilj-amqp`/`skilj-nats` ([§40](#skilj-kafka-bridge)/[§41](#skilj-amqp-bridge)/[§42](#skilj-nats-bridge))
had the mirror-image problem on their own outbound side (an unthrottled
sleep-and-retry-the-whole-cycle loop) and, on inbound, a real correctness
gap: a poison message never advanced that mapping's own durable commit/
ack point, so a bridge restart replayed an ever-growing backlog forever.

### `skilj-retry`: one shared backoff/attempt-cap policy

A new, dependency-free crate - `RetryPolicy { initial_backoff,
multiplier, max_backoff, max_attempts: Option<u32>, max_elapsed:
Option<Duration> }`, pure data and pure functions only (`next_backoff`,
`is_exhausted`), no clock read, no `sleep`. `RetryPolicy::bounded(...)`
gives up after `max_attempts`; `RetryPolicy::unbounded(...)` never does
by attempt count alone, `.with_max_elapsed(...)` layering an optional
elapsed-time cap on top of either. Every caller (`skilj-core`'s
`CrossContextRoute` catch-up loop, and each bridge's inbound *and*
outbound loop) already has its own notion of "now" and its own way of
sleeping/rescheduling, so this crate only ever answers "how long before
the next attempt" and "have I tried enough (or for long enough) to give
up" - the one piece of real code-sharing this pass needed, replacing
four independently hand-rolled poll-sleep-retry shapes with one.

### The inbound/outbound asymmetry, by design

Confirmed with the user before building, not assumed: the two directions
warrant genuinely different default behaviour, not the same mechanism
applied twice.

- **Inbound** (a route's own target command, or a bridge's own `POST
  /v1/events/external`/`POST /v1/commands/trigger` call) defaults to
  `RetryPolicy::bounded` - a real submission that keeps failing (a
  schema mismatch, a rejected precondition, a downstream bug) won't ever
  start succeeding just by waiting longer, so this direction gives up and
  **parks** the occurrence: a durable, operator-visible record (see
  below), the delivery marked done from the route/bridge's own point of
  view so it stops blocking everything behind it.
- **Outbound** (a bridge publishing an event *to* a broker) defaults to
  `RetryPolicy::unbounded` - a publish failure is almost always the
  target broker being unreachable, which should self-heal once it's back
  rather than being given up on. If a caller configures a bounded policy
  anyway, exhausting it **skips** that one event (acknowledged to skilj
  without ever having been produced) and moves on, logged loudly - no
  parked-delivery record, since there is nothing wrong with the message
  itself, only (temporarily) with reaching the broker. Accepted,
  deliberate data loss on the caller's own explicit opt-in, not a
  default.

### `parked_deliveries` - one row per persistently-failed delivery

A new per-bounded-context table (`skilj_core::db::ParkedDelivery`/
`ensure_parked_deliveries_table`, the identical `CREATE TABLE IF NOT
EXISTS` "brand-new table needs no migration dance" register
`ensure_external_message_cursors_table` ([§39](#external-message-dedup-create-external-event)) already established):
`id, source, kind, identifier, access_token_id, target_bounded_context,
target_command_type, request_json, error, attempt_count,
first_failed_at, last_failed_at`. `kind` is `CrossContextRoute`/
`ExternalEvent`/`CommandTrigger` - which other columns are populated and
how a later retry redrives the row both follow from it:

- `CrossContextRoute`: `request_json` is the `Target` command's own
  already-translated JSON payload; `target_bounded_context`/
  `target_command_type` name where to resubmit it; `access_token_id` is
  `None`.
- `ExternalEvent`/`CommandTrigger`: `request_json` is the bridge's own
  original `ExternalEventRequest`/`CommandTriggerRequest` body,
  verbatim; `access_token_id` names which `ExternalEventToken`/
  `CommandToken` to re-resolve and redrive through (never the token's
  own secret, which is never stored here); `target_bounded_context`/
  `target_command_type` are `None`.

**Lives in the target's own schema, not the source's** - a real bug this
pass's own new end-to-end test caught (see "Verified" below): the first
`CrossContextRoute` integration wrote a parked row into the *source*
bounded context's schema (the same schema `cross_context_route_cursors`
already lives in, "cursor lives with whoever's reading" -
[§36](#cross-context-route)'s own register), not the target's. Fixed
before merge - an operator managing the *target* bounded context is who
actually wants to see "deliveries into my bounded context that failed",
matching where `ExternalEvent`/`CommandTrigger` kinds already land
naturally (the resolved token's own bounded context, from the REST
ingestion endpoint below).

### `CrossContextRoute`: retry-then-park replaces retry-forever

`catch_up_cross_context_route` ([§36](#cross-context-route)) used to
propagate a target-submission `Err` straight out via a bare `?`, stopping
that tick without advancing the cursor - correct as a crash-safe "the
next tick retries the identical occurrence", but unthrottled and
unbounded. Now takes a `retry_policy: &skilj_retry::RetryPolicy`
(`SkiljBuilder::cross_context_route_retry_policy`, defaulting to
`RetryPolicy::default()` - 1s initial backoff, x2, capped at 5 minutes,
5 attempts) and three new `cross_context_route_cursors` columns
(`retry_attempt_count`/`retry_first_failed_at`/`retry_next_attempt_at`) -
a single row's worth of state suffices, the same invariant
`last_dispatched_sequence` itself already relies on: only the cursor's
own next occurrence can ever be the one currently failing, since the
cursor never advances past it. A tick reads this state once, up front -
if still within backoff, the whole tick is skipped (no dispatch, no
re-fetch cost) rather than re-attempting the identical failing
submission on every single poll. Once exhausted, the occurrence is
recorded as a `ParkedDelivery` (`source: "cross-context-route:{name}"`)
and the cursor finally advances past it, un-blocking the route for
everything behind it. An already-provisioned bounded context gets the
three new columns patched in by `ensure_cross_context_route_retry_columns`
(the `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` idiom
`ensure_projection_state_owner_columns` already established) -
`ensure_cross_context_route_cursors_table`'s own `CREATE TABLE IF NOT
EXISTS` already gives a brand-new one the full shape directly.

### REST ingestion: `POST /v1/parked-deliveries`

A bridge reports a message it gave up retrying using the *same*
`ExternalEventToken`/`CommandToken` credential it already presents for
the delivery itself (`kind` in the body says which) - the identical
capability-based design ("the credential says what's being written and
where, not a caller-supplied argument") every other `skilj-rest` route
already follows ([§7.2](#rest-wire-contract)). The bounded context a delivery is
parked under, and which `access_token_id` a later `retryParkedDelivery`
re-resolves, both come from that same token - no bounded-context
argument on the wire at all.

### GraphQL admin surface

`parkedDeliveries(boundedContext:)`/`retryParkedDelivery`/
`discardParkedDelivery`, `AdminAccess`-gated (`require_admin_mapping`),
following `rebuildProjection`/`discardProjectionRebuild`'s own list+
retry+discard shape exactly ([§8](#open-for-a-future-pass) item 5's own
Phase 2 precedent). `retryParkedDelivery` redrives a delivery's own
stored `request_json` straight through the same `skilj_core::db`/
`skilj_core::event_store` functions the original REST route (or
`catch_up_cross_context_route`) would have called - `db::
decide_and_submit_command` for `CrossContextRoute`/`CommandTrigger`
kinds (the latter re-running `event_store::authorise_command_trigger`
first, so a schema/status change since parking is still caught), `db::
create_and_insert_external_event` for `ExternalEvent` - bypassing the
REST/token-secret layer entirely (this mutation is already admin-gated,
so re-presenting the original bridge's own secret would add nothing).
Any outcome that isn't a real `Err` - accepted, rejected, redelivered,
deduplicated - means the delivery is no longer stuck, so the row is
deleted; only a genuine `Err` (the identical failure class that parked
it) leaves it parked, with `attempt_count`/`error`/`last_failed_at`
updated to reflect the fresh attempt. A `CrossContextRoute` redrive
deliberately doesn't reconstruct the original source event's own
`correlation_id`/`causation_id` (not stored on the parked row) - the
retried command gets a fresh, server-generated `correlation_id` instead,
the same "omitted means generated server-side" register Codeberg issue
#18 ([§44](#correlation-causation-ids)) already established for every
other caller with none to hand.

### The three bridges

Each of `skilj-kafka`/`skilj-amqp`/`skilj-nats` ([§40](#skilj-kafka-bridge)/[§41](#skilj-amqp-bridge)/[§42](#skilj-nats-bridge))
gets the identical shape:

- **Inbound**: a dispatch failure is retried, *for that one message*,
  entirely locally (a `sleep`-backoff loop around the same dispatch
  call) before the consumer ever reads its next message - not tracked
  via any broker-reported redelivery/attempt count. None of the three
  brokers gives this bridge a redelivery count worth trusting anyway:
  Kafka has no such concept at all reachable from here; AMQP 1.0's own
  `delivery-count` header isn't one this crate already reads; NATS/
  JetStream's own redelivery only replays after this consumer's own
  ack-wait timeout, not on the very next pulled message in the same
  session. A purely local, in-process counter sidesteps needing any of
  that - and sidesteps the "does it survive a bridge restart" question
  entirely, since every fresh delivery (first time or post-restart
  redelivery alike) gets its own full retry budget. Once exhausted, the
  message is reported to `POST /v1/parked-deliveries` and then
  committed/accepted/acked anyway - without that last step, a poison
  message would block that mapping's own durable commit point forever,
  each future restart redelivering a growing backlog. If the *report*
  itself fails, the message is deliberately left uncommitted (a real
  gap - better a loud, visible redelivery loop than a silently
  unreported poison message).
- **Outbound**: a produce/ack failure is retried across `produce_once`
  calls via a new `OutboundRetryState` (one instance per mapping,
  threaded by `run_outbound`) - the identical "only the head can be
  blocked" invariant the route's own cursor retry state relies on, so a
  plain `Option<OutboundRetryState>` suffices. A failure producing *or*
  acknowledging one event stops that cycle right there; no event behind
  it is even attempted, since skipping ahead would silently drop the
  blocked event from ever being retried (nothing would ever revisit it
  once a later one's own ack passes it). Default `RetryPolicy::unbounded`
  per the asymmetry above; exhausting a caller-configured bounded one
  skips the event (direct ack, no produce) and moves on.

### `skilj-tui`: a fifth tab

`skilj-tui` ([§11](#skilj-tui-console)) gets its first real admin
list+action view - `Tab::ParkedDeliveries`, digit `5`: a list (`r` to
refresh) with the selected row's own full JSON shown below it (including
`request_json` - what a retry actually redrives, too wide to fit the
list row itself), `Enter` to retry the selected row,
`d` to discard it. Shaped like `QueryEventsTab` (a flat list, no picker/
form sub-stage) rather than `CommandsTab`'s two-stage enum - there's no
second view to switch into here, only actions on the one list. Both
mutations return the identical `{ id }` shape, so `ParkedDeliveriesTab::
action_pending` (the `id` of whichever call is currently in flight) is
what tells `App::handle`'s own result arm which row to remove on
success, not the response itself.

### No spec entity

Like `Snapshot`/`CrossContextRoute`/the deadline mechanism ([§46](#native-deadlines)),
`ParkedDelivery` is a Rust-only construct layered on top of DCB, not a
change to DCB's own model or the Allium spec.

### Verified

- `skilj-retry`'s own unit tests: geometric backoff growth and capping,
  attempt-count exhaustion, elapsed-time exhaustion, and that either cap
  alone is enough (both independent).
- `skilj-core/tests/cross_context_route_parking.rs`, a real end-to-end
  test against Postgres, deliberately at the `skilj-core` layer (a
  hand-rolled `CommandDispatcher`/`CrossContextRouteDispatcher` pair, not
  the full `skilj` builder facade `skilj/tests/cross_context_route.rs`
  exercises) - the legitimate typed `CrossContextRoute` API gives no way
  to make a *real* registration fail deterministically, `route()`'s own
  return type being tied to `Target::Payload` at compile time. Proves,
  with a target dispatcher that fails a controlled number of times: the
  occurrence parks (not blocks) once `retry_policy` exhausts; the
  backoff genuinely throttles (a tick within the backoff window makes no
  dispatch call at all); the cursor actually unblocks (a second
  occurrence posted afterward is picked up and parked independently,
  proof the route kept running rather than staying stuck on the first);
  a manual failed retry bumps `attempt_count`/`error` rather than
  deleting the row; a manual successful retry (the exact redrive
  `retryParkedDelivery`'s own resolver makes) both creates the real
  command and deletes the row; and a discard without ever retrying
  removes the row with no command created. This test is what caught the
  wrong-schema bug described above - it failed on the very first run,
  for exactly that reason.
- `skilj/tests/parked_deliveries_graphql.rs`, a real end-to-end test
  through `Skilj::graphql_router()` with real JWT/JWKS auth (mirroring
  `skilj/tests/graphql_type_registration.rs`'s own harness) - narrower
  and complementary to the `skilj-core` test above: given a parked
  delivery already exists (seeded directly via `db::insert_parked_delivery`,
  not reproduced through a real failure), proves `parkedDeliveries`
  renders every field correctly, `retryParkedDelivery` redrives the
  stored `ExternalEvent`-kind request for real (a genuine event lands)
  and deletes the row, and a second row discarded without ever being
  retried is removed with no event created.
- Each bridge's own `tests/*_bridge.rs` gained two new tests -
  `an_inbound_message_parks_and_reports_after_exhausting_retries` and
  `an_outbound_event_is_skipped_after_exhausting_a_configured_retry_cap` -
  against the identical real-broker-plus-mock-skilj harness those files
  already use, the mock's own `GET /v1/events/consume` changed from
  drain- to peek-based (and `POST /v1/events/consume/ack` to remove only
  the acked entry) to correctly simulate real manual-ack semantics across
  the multiple retry cycles these new tests need. Run against real
  brokers once Docker became reachable in this sandbox (KRaft-mode Kafka,
  Artemis for AMQP 1.0, ephemeral NATS+JetStream) - all 18 tests across
  the three crates pass. Two real bugs this run caught, neither in the
  library code itself:
  - All three new inbound-parking tests initially timed out (a 5s
    polling budget) - real consumer-group/session/broker setup latency
    on a first subscription needs the identical ~15-20s tolerance
    `recv_within`'s own established pattern elsewhere in these files
    already budgets for; tightened to 20s across all three.
  - `skilj-amqp`'s own inbound-parking test attached its receiver *after*
    sending the test message - unlike JetStream's durable log (NATS) or
    a queue a message survives regardless of consumer presence, this
    broker/address combination has no durable subscription for a message
    published before any receiver exists to be held for, so it was
    simply never delivered. Fixed by attaching before sending, the same
    ordering every other inbound test in that file already uses.
- `skilj-tui/tests/schema_driven_forms.rs` gained
  `parked_deliveries_lists_navigates_and_acts_on_the_selected_row`,
  driven purely through `App::handle` the same synthetic-result shape
  every other test in that file already uses (no real server needed) -
  proves entering the tab fetches, Up/Down navigates, discard/retry both
  set `action_pending` correctly, a successful action removes the row
  and updates the status line, and a failed one leaves the row in place.
- `cargo build/clippy -D warnings/test --workspace` and `cargo fmt
  --check` clean across every crate this pass touched
  (`skilj-retry`/`skilj-core`/`skilj-rest`/`skilj-graphql`/`skilj`/
  `skilj-kafka`/`skilj-amqp`/`skilj-nats`/`skilj-tui`).

## 48. Investigation: atomic transactions spanning multiple command types (Codeberg issue #22)

Issue #22 asked whether `skilj` needs a way to atomically process
multiple independently-registered `CommandType`s in one transaction,
citing DCB's headline claim (via AxonIQ's Axon Framework 5 framing)
that "a single transaction can span decision models that would
traditionally be separate aggregates". `CommandDispatcher::dispatch()`
does take exactly one command type per call, so the premise is
accurate as a description of the code - the question was whether that's
a real gap.

### What "spans decision models that would traditionally be separate
aggregates" actually means

Checked against the primary sources (dcb.events, AxonIQ's own Framework
5 writeup), not just the issue's paraphrase: the DCB consistency
boundary is defined **per command**, by that command's own query/tag
selection - not by atomically batching multiple independently-submitted
commands together. dcb.events' own canonical motivating example is a
student enrolling in a course, needing both a `Course` capacity
invariant and a `Student` enrollment-limit invariant - "classic
one-stream-per-aggregate" territory that would otherwise need a saga.
Axon 5 doesn't offer cross-command atomic batching either; its
"decision model" is likewise assembled per command handler invocation.

### skilj already has this - it's the flagship demo, not a gap

`skilj-demo/src/courses.rs`'s `EnrollStudentInCourse` **is** that exact
canonical example, already built and tested:

- `tag_mappings()` names both `student` and `course` tags at once
  (`student_and_course_tags()`), so `decide()` receives the *union* of
  both entities' histories as a single `matching_events` slice and
  checks both invariants (course capacity, per-student course limit) in
  one synchronous call - no second round trip, no separate aggregate
  transaction.
- `CommandDecision::Accepted { events: Vec<EventSpec> }`
  (`skilj-core/src/shared/mod.rs`) already allows one `decide()` call to
  emit multiple events of different types atomically.
- `db::submit_command`'s row-locked optimistic-then-locked commit (the
  DCB-conflict-retry machinery this doc already covers) makes this
  atomic under real concurrency, not just logically consistent in a
  single-threaded read - `skilj-demo/tests/courses.rs` races two
  enrollments for a course's last seat to prove it.

The module's own doc comment on `courses.rs` already names the result:
"no saga, no reservation, no compensation" - precisely what issue #22
asked whether skilj could do.

### What's genuinely not supported, and why that's fine

Dynamically gluing two already-registered, independent `CommandType`s
together into one call without writing new code isn't supported. But
DCB itself doesn't provide that either - the idiomatic answer to a new
cross-entity use case is a new `CommandType` whose `tag_mappings()`
names every tag the new invariant needs (exactly what
`EnrollStudentInCourse` is), not command-composition plumbing bolted
onto the dispatcher.

### Recommendation (issue #22's own third listed outcome)

Document the single-command-per-transaction model as deliberate: it
already delivers DCB's actual capability - atomic decisions spanning
what would traditionally be separate aggregates - at the tag/decision-
model level, which is the level DCB itself operates at. No feature
gap identified; issue closed as already-solved, pointing at
`EnrollStudentInCourse` as the existing proof.

## 49. A GraphQL subscription for Projections: `projectionUpdates` (Codeberg issue #23)

`EventSubscription` ([§9](#next-steps)'s "last surface out of the
backlog") already gave clients a live push of raw events, but a client
wanting "push me the current balance whenever it changes" had to
subscribe to raw events and re-implement the projection's own `project()`
fold client-side. This adds `projectionUpdates` - `ProjectionQuery`'s own
subscription counterpart, pushing the already-computed `Projection`
state itself.

### The refactor this needed first

`ProjectionQuery.projection`'s resolver (`skilj-graphql/src/resolvers/projection_query.rs`)
already had the entire read path this needed: `team_only` gate →
`get_projection` → `wait_until_caught_up` → `get_projection_state_and_owner`
→ decrypt-on-read → `projections::query_projection`. Rather than a second,
hand-rolled copy for the subscription to drift from, that path was
pulled out into `fetch_projection_result` (same file, now `pub(crate)`,
along with `wait_until_caught_up`) - `field()` becomes a thin wrapper
around it, and `projectionUpdates`'s initial snapshot and every one of
its per-event refetches call the identical function. Any future fix to
that read path (an owner-scope bug, a decrypt gap) now fixes both
surfaces by construction, the same reasoning `event_subscription.rs`'s
own re-fetch-fresh-`access_mapping` pattern already established for
`EventSubscription`.

### Shape: mirrors `EventSubscription`, two deliberate divergences

New module `skilj-graphql/src/resolvers/projection_subscription.rs`,
one field: `projectionUpdates(boundedContext: String!, name: String!,
key: String): ProjectionResult!` (`key` defaults to `""`, identical
convention to `projection`; no `waitForSequence` - a subscription starts
from current state and lives indefinitely, nothing to wait for up
front). Registered into the schema's `Subscription` root under the same
`projection_types.is_some()` guard `projection_query::field()` already
uses (`skilj-graphql/src/schema.rs`) - nothing to register when no
projection exists anywhere to reference.

The resolver shape mirrors `event_subscription.rs`'s two fields exactly:
`require_read_mapping` → `team_only` gate (fail fast, before any DB work
or subscribing - a caller who was never going to be authorized never
even reaches `state.event_broadcaster.subscribe()`) → confirm the
projection exists → subscribe *before* the initial snapshot read (the
identical drift-audit-finding-#7 ordering `event_subscription.rs`
already documents: an event landing in the gap must be either already
reflected in the snapshot or delivered live, never neither) → yield the
initial state → loop on `tokio::select!` between the event receiver and
`state.revocation_broadcaster`, reusing `revocation_closes_connection`
(now `pub(crate)`) unchanged for the push-revocation path.

Two places this genuinely, deliberately differs from `EventSubscription`:

- **What triggers a push.** Not every event - only one whose
  `ProjectionDispatcher::keys(bounded_context, name, event)` names this
  subscription's own `key`. Every other event is a cheap in-memory
  `continue`, no DB round trip at all - the same "position always
  advances, state only changes when consumed" distinction `keys()`'s own
  doc comment already draws, now load-bearing for cost rather than just
  correctness.
- **What gets pushed, and `RecvError::Lagged` handling.** Never the
  triggering event itself - always a fresh `fetch_projection_result`
  refetch, passed the triggering event's own `sequence` as
  `wait_for_sequence`. That one argument is what makes this correct for
  an *async* (`sync() == false`) projection too, with no sync/async
  branch anywhere in the subscription's own code: `wait_until_caught_up`
  (inside the shared function) resolves on its very first poll for a
  sync projection (already current by commit time) and waits out a real
  `catch_up_bounded_context` poll tick for an async one. Because of that,
  `RecvError::Lagged` doesn't need to be fatal here the way it is for
  `EventSubscription`: that surface promises every individual event
  (`DeliveryIsAtMostOnce`), so falling behind is real, unrecoverable data
  loss. `projectionUpdates` promises "current state whenever it changes",
  not "every event that changed it" - a lagged receiver loses nothing a
  plain refetch can't recover, so it self-heals (refetch this key's
  current state with `wait_for_sequence: None`, keep the connection
  open) instead of closing.

### Tests

`skilj/tests/projection_subscription.rs`, the same real-Postgres/real-
JWKS-server/real-websocket harness `skilj/tests/event_subscription.rs`
already established (duplicated rather than extracted into shared
test-support, this project's own established call):

- Initial push on subscribe, a push per matching event afterward
  (cumulative, not one-shot), and revocation mid-stream closing the
  connection distinguishably (`grant_not_active`) - one consolidated
  end-to-end test, the same consolidation style
  `event_subscription_end_to_end` already uses.
- Key-filtering: a keyed `CustomerPurchaseHistory` projection proves an
  event for a *different* key never pushes anything, while one for the
  subscribed key does.
- `TEAM_ONLY` (Codeberg issue #17's gate, reached through this new
  surface): a Role missing the required team name is rejected as the
  subscription's own first and only message, before any event could
  possibly be involved - proving the check runs before
  `event_broadcaster.subscribe()`, not just before a push. A Role
  carrying the team name subscribes normally.
- An async (`sync() == false`) projection: proves the push genuinely
  waits for `catch_up_bounded_context`'s own poll tick rather than
  racing it - a naive immediate refetch would risk pushing the still-
  default state with no second event ever arriving to correct it, since
  `keys()` already matched on the first one.

Owner-tag cross-tenant scoping and sensitive-field decrypt-on-read are
deliberately not re-tested here - both run through the identical
`fetch_projection_result` `projection_query.rs`'s own suite already
covers end to end; duplicating them through a second transport would
test the shared function twice, not the new wiring.

### Verified

`cargo build/clippy -D warnings/test --workspace` and `cargo fmt --check`
clean; the new `projection_subscription.rs` suite (4 tests) and the
pre-existing `projection_query.rs`/`event_subscription.rs` suites (5 + 4
tests) all pass unchanged against real embedded Postgres, confirming the
`fetch_projection_result` refactor didn't alter `projection`'s own
behaviour.

## 50. Investigation: segmented/parallel event processing for horizontal scale-out (Codeberg issue #25)

Speculative, not a commitment - the issue asked whether skilj should
adopt Axon Framework's Tracking Event Processor model (parallel
segments claimed via leased tokens), replacing the current "every
instance redundantly redoes the same idempotent work" design, and named
three possible outcomes: a targeted narrow fix, a fuller segment-based
redesign, or documentation that the current model already suits skilj's
scale. It also asked, as an open question, whether that "redundant work
is idempotent" premise actually holds.

### The premise didn't hold - a real bug, not a hypothetical

Checking that premise directly (not just re-reading the existing design
notes) found it's true for the scheduler and deadline-firing paths -
both genuinely idempotent by construction (`fire_due_deadlines`'s own
doc comment: "two instances racing to fire the same row both submit
under the identical idempotency key, so the second is a harmless
`Deduplicated`") - but **false** for async `Projection` catch-up.
`Snapshot` catch-up (`catch_up_snapshots`) already re-reads its own
per-row `as_of_sequence` under the row lock and skips a event already
folded into that specific row; `Projection` catch-up
(`catch_up_bounded_context`, `insert_event_and_update_sync_projections_in_tx`,
`fold_history_into_new_sync_projection`) had no equivalent per-row
check - only a projection-level `caught_up_to` snapshotted once at
function entry and never rechecked mid-walk.

Proven with a real `tokio::join!` race (not reasoned about from the code
shape): two concurrent `catch_up_bounded_context` calls against the same
bounded context, simulating two instances' own background pollers both
firing at once, folded one `MoneyDeposited(20)` event into
`AccountBalance` twice - state landing on `40` instead of `20` in most
runs. A second, independently-reachable path to the identical bug: two
instances starting up at once and both reconciling the same brand-new
sync projection registration would both see `existing: None` (a plain
read-then-decide in `skilj/src/lib.rs`'s reconciliation loop, no claim
mechanism) and both call `fold_history_into_new_sync_projection`
concurrently.

### The fix: `projection_state`/`projection_rebuild_state` get their own `as_of_sequence`

Mirrors `snapshots.as_of_sequence` exactly, rather than introducing a
new mechanism (an advisory lock serializing an entire bc's catch-up
tick was considered and rejected - coarser than necessary, and would
have worked against, not for, any future parallelism): a new
`as_of_sequence BIGINT NOT NULL DEFAULT -1` column on both tables,
returned by `get_or_create_projection_state_for_update`/
`..._rebuild_state_for_update` alongside `state`, checked by every fold
call site immediately after (`if as_of_sequence >= event.sequence {
continue; }`) - *before* `dispatcher.project()` ever runs, so a losing
instance's own read of the winner's already-updated state is never
folded a second time. `apply_projection_fold_update` advances it to
`event.sequence` in the same `UPDATE` that persists the new `state`.
`promote_projection_rebuild`'s existing `INSERT ... SELECT` from
`projection_rebuild_state` into `projection_state` carries the column
across too - left out, every promoted row's `as_of_sequence` would reset
to `-1` while `state` already reflected a full replay, reintroducing the
exact bug on the very next tick.

`insert_event_and_update_sync_projections_in_tx`'s own sync-projection
fold is provably unreachable concurrently already (it shares the one
transaction that holds the bounded context's `sequence` row lock for its
entire duration, per `insert_event_via_the_locked_path`'s own doc
comment) - the check is added there too anyway, for structural
consistency across all four fold call sites rather than leaving it as
the one unexplained exception.

An already-provisioned bounded context gets the column patched in by
`ensure_projection_state_as_of_sequence_columns`, following
`ensure_projection_state_owner_columns`'s own established pattern (a
plain `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, called unconditionally
on every `build()` - see that function's own doc comment for why a
blanket `-1` backfill is safe even against a row with real pre-existing
`state`: the coarser, untouched `projections.caught_up_to` already
decides which events a tick ever re-fetches, so a freshly-`-1` row is
only ever compared against events it genuinely never folded).

### Recommendation (issue #25's own first listed outcome)

A targeted narrow fix, not a segment-based redesign: no evidence
throughput has actually hit a real limit (the issue's own first
question), and the redundant-work model's real cost is bounded per tick
already (§22). Building Axon-style leased-segment claiming now would
add real coordination machinery (a token store, claim/renew/release,
segment-count configuration) to solve a problem that isn't yet
demonstrated, and would have been built on top of a design that was
*actively incorrect* for one of its own components - fixing the
correctness gap first was the prerequisite either way. Revisit
segmentation if a real throughput ceiling shows up in practice, not
speculatively.

### Verified

A real concurrent-race regression test for each of the two reachable
paths (`two_concurrent_instances_never_double_fold_the_same_event`,
`two_concurrent_first_time_registrations_never_double_fold_history`,
both in `skilj-core/tests/async_projections.rs`) - both reproduced the
double-fold reliably before the fix (10/10 runs clean after; the
pre-fix version failed in the majority of runs, confirming it wasn't a
one-off timing fluke) and pass deterministically after it.
`cargo build/test --workspace` clean, including the full pre-existing
`async_projections.rs`/`sync_projections.rs`/`projection_registration.rs`
suites unchanged.

## 51. Segmented/parallel event processing, built (Codeberg issue #25, part two)

§50 above recommended a narrow fix over segmentation - no evidence of a
real throughput ceiling, and the redundant-work model's real cost was
already bounded per tick. The user asked for the fuller feature anyway,
despite that. This section is that feature: real horizontal parallelism
for async `Projection` catch-up, built on top of §50's `as_of_sequence`
fix rather than instead of it.

### The key thing that de-risks the whole design

`as_of_sequence` (§50) already makes every fold safe under *arbitrary*
concurrent races, regardless of which partition or instance performs
it. Everything below - the advisory-lock claiming in particular - is a
pure work-avoidance optimization, not a correctness mechanism. Even a
hash disagreement between instances (a theoretical `hashtext` collision,
or a rolling deploy briefly running two different `PARTITION_COUNT`
values for the same projection) could only ever cause wasted duplicate
work, never a double-fold. That guarantee is what makes it safe to build
this without any lease/heartbeat/claim-table machinery at all.

### Two confirmed design forks

- **Partition assignment reuses `Projection::keys()`** rather than
  introducing a separate "partition key" concept. A projection keyed by
  a tag value is already partitioned by that tag; one keyed by an id is
  already partitioned by that id - `keys()` already is each projection's
  own notion of "which instance does this event touch," partitioning
  just hashes that same string into one of `PARTITION_COUNT` buckets.
  Zero new trait surface beyond the one new constant itself.
- **Claiming is a non-blocking, per-tick `pg_try_advisory_xact_lock`
  race**, not a lease/claim table with heartbeat/expiry (the fuller,
  Axon-style alternative). Each tick, each instance tries the lock for
  each partition with pending work; whichever instance wins folds that
  partition's own pending keys inside the one transaction holding the
  lock, and commits (releasing it). A losing instance just skips that
  partition this tick. Self-healing for free - a dead instance simply
  stops winning locks, no reclaim/rebalance logic needed anywhere.

### New table: `projection_partition_progress`

One row per `(projection_name, partition_index)`, `caught_up_to BIGINT
NOT NULL DEFAULT -1` - `ensure_projection_partition_progress_table`
(`skilj-core/src/db/mod.rs`), following the established `CREATE TABLE IF
NOT EXISTS`/`impl PgExecutor` idiom every sibling `ensure_*_table`
function here already uses, called from both
`provision_bounded_context_schema` and the startup loop in
`skilj/src/lib.rs`.

No columns were added to `projections`/`projection_state`/
`projection_rebuild_state`. `projections.caught_up_to` - the single
external source of truth `Projection.caughtUpTo` (GraphQL) and
`wait_until_caught_up`/`fetch_projection_result` (`skilj-graphql`,
backing both the `projection` query and the `projectionUpdates`
subscription) depend on - stays exactly what it always meant: every key
of this projection reflects every event up to this sequence. It's now
computed, for a partitioned projection, as a monotonic-only rollup
(`MIN(caught_up_to) WHERE partition_index < PARTITION_COUNT`, guarded
by `WHERE caught_up_to IS NULL OR caught_up_to < $1` so it can never
regress) rather than being advanced directly - GraphQL/the subscription
needed zero changes.

### `catch_up_bounded_context`: branch, don't rewrite

Async projections are split by their own `dispatcher.partition_count(...)`
(defaulted to `None` ≡ unpartitioned) into `unpartitioned_async_projections`
(`<= 1`, the default, and every projection that predates this feature)
and `partitioned_async_projections` (`> 1`). Everything for the
unpartitioned set - the per-event transaction loop, `min_caught_up`, the
early-return guard - is **byte-identical** to before this pass, just
renamed; every pre-existing test in `async_projections.rs`/
`sync_projections.rs` passes unchanged, proving it. Order stays kept by
default simply because the new code path is never entered.

Partitioned projections are handled afterward by a new
`catch_up_partitioned_projection`, one per projection, sequentially (not
`for_each_concurrent` - see below).

### `catch_up_partitioned_projection`

Per partitioned projection, per tick: seed missing `0..PARTITION_COUNT`
progress rows (`ON CONFLICT DO NOTHING`); load current progress; fetch a
**capped** batch of new events (`MAX_EVENTS_PER_PARTITION_TICK = 1000`,
via the new `list_events_for_bounded_context_from_limited` - a separate
function from `list_events_for_bounded_context_from` so that function's
other caller, `catch_up_snapshots`, stays untouched); skip any partition
already caught up through this batch's tail; for each remaining
partition, open one transaction, `pg_try_advisory_xact_lock` (keyed by
`partition_lock_key`, `hashtext(...)::bigint`, the identical primitive
`migrate_idempotency_keys_client_id_scoping` already established), and
on success fold every key in this batch that hashes into that partition
through the **unmodified** `get_or_create_projection_state_for_update` →
`as_of_sequence` guard → `dispatcher.project()` →
`apply_projection_fold_update` sequence the unpartitioned loop already
uses, then upsert this partition's own progress and commit. Regardless
of which partitions this instance won, it rolls up the projection-level
`caught_up_to` (above) every tick - harmless, idempotent, whichever
instance happens to do it.

**One transaction per `(partition, tick)`, not per-event** - a
deliberate, evaluated tradeoff, not an oversight. A non-blocking lock
re-acquired per event would force stopping at the first lost race
anyway, since a partition's own progress can only advance contiguously
in sequence order - far more lock round trips for comparatively little
benefit. The real cost traded away, relative to the unpartitioned loop's
own per-event transaction granularity: a poison event anywhere in a
partition's own batch this tick rolls back that whole partition's tick,
not just the offending event. `MAX_EVENTS_PER_PARTITION_TICK` bounds the
blast radius; the next tick's fresh, non-blocking race is self-healing
regardless of which instance (if any) previously held that partition.

**Intra-tick partition processing stays sequential in v1** (not further
fanned out within one instance) - the real scale-out value comes from
separate *instances* racing for partitions, not from one instance's own
intra-tick concurrency, and stacking more concurrency here on top of
this function's own already-concurrent per-bounded-context caller
(`BACKGROUND_TASK_CONCURRENCY` in `skilj/src/lib.rs`) against a pool
with a modest default connection cap (§35) was judged a poor trade for
what little it would buy.

**A finding worth contrasting, not fixing here**: the existing
unpartitioned loop's own `caught_up_to` update
(`UPDATE {schema}.projections SET caught_up_to = $1 WHERE name = $2`)
has no monotonic guard - in principle, two concurrent redundant
`catch_up_bounded_context` calls could commit their own ascending
per-event transactions in a different real order, letting a slower
instance's earlier-event commit land after a faster instance's
later-event commit and regress it. A pre-existing, latent, self-
correcting risk (the next tick just re-advances past it) in code this
pass leaves untouched, since it's out of scope for a partitioning
feature - but the new partitioned rollup deliberately does add the
monotonic guard, which is why it's worth naming the asymmetry here.

### Key → partition hash

Hand-written 64-bit FNV-1a (`partition_for_key`), not
`std::collections::hash_map::DefaultHasher` (explicitly not guaranteed
stable across Rust versions/std/build flags - unsuitable for a scheme
that must agree across every instance in a fleet, possibly running
slightly different builds mid rolling-deploy) and not a new crate
dependency (not judged worth it for a few lines of well-known, public-
domain algorithm). Computed in Rust, not pushed into Postgres via
`hashtext()`, since partition membership must be known before deciding
which partitions are even worth a lock attempt that tick - `hashtext()`
is still used, unrelated, for the advisory lock key itself.

### Trait/dispatcher surface

`Projection::PARTITION_COUNT: u32 = 1` (`skilj-core/src/plugin/mod.rs`,
next to `OWNER_TAG_KEY`/`TEAM_ONLY`) - the identical "deliberately
Rust-only, no spec entity field, no registration/admin-visible surface"
treatment those two already get, and for the same underlying reason:
it's a pure work-distribution detail, invisible to what `project()`
actually folds, so `register_projection`'s rebuild-trigger logic
(`schema_changed`/`consumed_change_has_history`/`becoming_sync`) never
needs to know about it - changing `PARTITION_COUNT` between deploys
needs no rebuild.

`ProjectionDispatcher::partition_count(...)`, defaulted to `None`
(≡ unpartitioned) rather than required, the same treatment `team_only`
already gets - every hand-rolled test-double `ProjectionDispatcher`
impl across the test suite kept compiling unchanged. Wiring the real
implementation (`skilj/src/lib.rs`) turned out to need no macro
changes: one new field on `RegisteredProjection`, set from
`T::PARTITION_COUNT` in `registered_projection::<T>()`, and one new
accessor on `ProjectionDispatcherImpl`, mirroring `owner_tag_key`/
`team_only` exactly.

### Deliberate v1 scope cuts

- The `building_rebuilds` loop (same function) and
  `fold_history_into_new_sync_projection` stay single-instance,
  unpartitioned, regardless of a live projection's own
  `PARTITION_COUNT`. Rebuilds are one-time, bounded events (only
  `schema_changed`/`consumed_change_has_history`/`becoming_sync` ever
  stage one), not the sustained-throughput concern this issue is
  actually about.
- **Known accepted limitation**: after a rebuild promotes
  (`promote_projection_rebuild`), a partitioned live projection's
  `projection_partition_progress` rows can be stale relative to the
  freshly-jumped `projections.caught_up_to` the rebuild just set. The
  next tick(s) re-scan that gap - capped and self-healing over a few
  ticks by `MAX_EVENTS_PER_PARTITION_TICK` - purely as wasted work,
  never a correctness risk, since `as_of_sequence` still skips every
  already-folded key/event pair encountered along the way. Not worth
  promotion-side bookkeeping to close for a rare, self-correcting cost.

### Verified

New tests in `skilj-core/tests/async_projections.rs` -
`partitioned_projection_concurrent_instances_fold_every_key_exactly_once`
(a real `tokio::join!` race across three concurrent instances and a
four-partition, three-key projection), `partitioned_projection_caught_up_to_never_regresses_and_reaches_latest`,
`partitioned_projection_uneven_key_distribution_converges` (mostly-idle
partitions still advance), and
`catching_up_a_bounded_context_with_both_partitioned_and_unpartitioned_projections_keeps_both_correct`
(the two loops don't cross-contaminate) - all pass deterministically
across 10 repeated runs. The full pre-existing `async_projections.rs`
suite (12 tests total now) and `sync_projections.rs`/
`projection_query.rs`/`skilj-graphql`'s own suites pass unchanged,
confirming the unpartitioned path is genuinely untouched and GraphQL's
`caughtUpTo`/`wait_for_sequence` contract needed no changes. `cargo
build/clippy --workspace --all-targets -- -D warnings` and `cargo fmt
--check` clean.

## 52. Scoping parallelism beyond Projection catch-up, and building the Snapshot half

After §51, the question came up: does the same design apply anywhere
else events get processed? Three candidates were investigated directly
against the code: `Snapshot` catch-up, `CrossContextRoute` catch-up, and
the Kafka/AMQP/NATS outbound bridge crates. They needed genuinely
different answers.

### `CrossContextRoute`: no fit, no action

`CrossContextRoute::route()` takes one event payload and returns one
optional command payload - no key, no instance concept, no fan-out.
`cross_context_route_cursors` has exactly one row per `route_name`, a
single linear stream by construction. Every real registered example in
the codebase (only in `skilj/tests/cross_context_route.rs` - none in
skilj-demo) is a single coarse mapping like `OrderShipped → ReserveStock`,
matching §36's own explicit scoping. Unlike `Projection` before §50's
fix, this mechanism is already correctness-safe under redundant
concurrent execution today - not via a row-level guard, but because
target-side command submission uses a deterministic idempotency key
(`"{route_name}:{event.sequence}"}`); a losing concurrent instance's
submission comes back `Deduplicated`, handled and logged, not an error.
There is no independent, divisible unit of work here the way a
`Projection`'s many keys or a `Snapshot`'s many tag values are - a
route's own cost is O(events since cursor), not O(events × keys).
**No action taken.** If a specific route's own redundant-work cost ever
becomes a real, demonstrated concern, the applicable fix would be a
single non-blocking lock per *route* (not per partition within one) to
cut wasted redundant scans - a work-avoidance optimization, not a
throughput increase, since one route's own processing stays inherently
serial regardless. Not built speculatively, for the same reason §50
gave for not building segmentation before a real need was shown.

### `Snapshot`: built, mirroring §51 almost exactly

`Snapshot` has no `Projection::keys()`-equivalent, but doesn't need one:
it's already strictly one row per `(snapshot_name, tag_key, tag_value)`,
with exactly one tag value derived per event (`catch_up_snapshots`).
That derived tag value plays the identical structural role a
`Projection`'s per-event key string does. `snapshots.as_of_sequence`
already existed per row, already checked before folding - the same
guard §50 had to *add* for `Projection`, `Snapshot` already had it.

**Before building anything on top of that guard**, it was proven
directly rather than assumed: no test anywhere in the repo had run
`catch_up_snapshots` under a real concurrent race before this pass
(`grep -rln "tokio::join!" skilj-core/tests/` matched only
`async_projections.rs`). A new
`two_concurrent_instances_never_double_fold_the_same_snapshot_row`
(`skilj-core/tests/snapshot_context.rs`) - two simultaneous
`catch_up_snapshots` calls against one account's single deposit event -
passed clean across 10 repeated runs. Unlike `Projection`'s own
investigation, this one found no bug: `Snapshot`'s guard was already
correct.

**The build itself**, mechanically identical to §51:
- `Snapshot::PARTITION_COUNT: u32 = 1` (`skilj-core/src/plugin/mod.rs`,
  next to `Snapshot::OWNER_TAG_KEY`) - the same Rust-only, no-spec-
  surface treatment, and for the identical reason: a pure work-
  distribution detail, invisible to what `fold()` actually folds.
- `SnapshotDispatcher::partition_count(&self, bc, name) -> Option<u32>`,
  defaulted to `None` (≡ unpartitioned) - every existing hand-rolled
  `SnapshotDispatcher` test double (and the real
  `SnapshotDispatcherImpl` in `skilj/src/lib.rs`, which needed one new
  field on `RegisteredSnapshot` plus one new accessor, mirroring
  `ProjectionDispatcherImpl`'s own `partition_count` wiring) kept
  compiling unchanged.
- New `snapshot_partition_progress` table (`snapshot_name,
  partition_index, caught_up_to, updated_at`) - **no `REFERENCES`
  clause**, unlike `projection_partition_progress`: `snapshots`/
  `snapshot_progress` have no backing registration table either, so
  there's nothing to foreign-key against and no table-creation-ordering
  constraint to get wrong (§51's own placement bug - caught and fixed
  during that pass - doesn't recur here for that reason).
  `ensure_snapshot_partition_progress_table` follows the established
  `CREATE TABLE IF NOT EXISTS` idiom, called from both
  `provision_bounded_context_schema` and the `skilj/src/lib.rs` startup
  loop.
- `catch_up_snapshots` splits `snapshot_names` into
  `unpartitioned_snapshot_names`/`partitioned_snapshot_names` by
  `dispatcher.partition_count(...)`, identical branch shape to
  `catch_up_bounded_context`. The unpartitioned path is byte-identical
  to before this pass - every pre-existing test in
  `snapshot_context.rs`/`snapshot_owner_scoping.rs`/
  `skilj-demo/tests/snapshot.rs` passes unchanged, proving it. A new
  `catch_up_partitioned_snapshot` per partitioned name reuses
  `partition_for_key` (the identical FNV-1a §51 established),
  `list_events_for_bounded_context_from_limited`/
  `MAX_EVENTS_PER_PARTITION_TICK` (both already generic, untouched), and
  the identical non-blocking `pg_try_advisory_xact_lock`-per-`(name,
  partition)`-per-tick race, folding through the **unmodified**
  `get_or_create_snapshot_state_for_update` → `as_of_sequence` guard →
  `dispatcher.fold()` → the existing owner/no-owner `UPDATE snapshots`
  branches. `snapshot_progress.caught_up_to` (the existing rollup) gets
  the same monotonic-only `MIN()` update `projections.caught_up_to`
  does in §51.
- Four new tests in `snapshot_context.rs`, the `Snapshot` twins of §51's
  four:
  `partitioned_snapshot_concurrent_instances_fold_every_tag_value_exactly_once`
  (a real 3-instance race, four partitions, three accounts),
  `partitioned_snapshot_as_of_sequence_reaches_latest_across_ticks`,
  `partitioned_snapshot_uneven_tag_distribution_converges` (mostly-idle
  partitions still advance) - via a small `PartitionedTestSnapshotDispatcher`
  that delegates everything to the existing `TestSnapshotDispatcher`
  except `partition_count`, so none of the file's existing construction
  sites needed touching. All pass deterministically across 10 repeated
  runs.

### Kafka/AMQP/NATS outbound bridges: scoped, not yet built

A different design (§53 covers this once built): the bridge crates are
pure REST clients (`produce_once` polls `GET /v1/events/consume` over
HTTP; none of the three has a `sqlx`/Postgres dependency at all), so
`Projection`/`Snapshot`'s advisory-lock racing can't apply there
directly - there is no database connection in that crate to hold a lock
on. The investigation also found a real, previously-unverified gap
worth closing independent of any parallelism work: `read_cursors`
(the server-side cursor `mode=manual` polling advances) has no claiming
mechanism at all today - two concurrent instances of the same
`OutboundMapping` would both fetch the same unacked batch and both
double-publish. See docs/architecture.md's next section for the fix and
the partitioning design built on top of it.

### Verified

`cargo build/clippy --workspace --all-targets -- -D warnings` and
`cargo fmt --check` clean. `two_concurrent_instances_never_double_fold_the_same_snapshot_row`
and the three partitioned-snapshot tests pass deterministically across
10 repeated runs each. Full `snapshot_context.rs` (9 tests),
`snapshot_owner_scoping.rs` (8 tests), and `skilj-demo/tests/snapshot.rs`
(2 tests) all pass.

## 53. Closing the Kafka/AMQP/NATS bridges' double-publish gap: `ReadCursor.checked_out_at`

§52 found a real, previously-unverified gap in the bridge crates
(`skilj-kafka`/`skilj-amqp`/`skilj-nats`): they're pure REST clients
(`produce_once` polls `GET /v1/events/consume` over HTTP; none of the
three has a `sqlx`/Postgres dependency), and `read_cursors`' `manual_ack`
mode had no claiming mechanism at all - two concurrent callers presenting
the same `EventReadToken` would both be served the identical unacknowledged
batch, both act on it externally (publish to a broker), and only
afterward both acknowledge. This section closes that gap, spec-first.

### Spec change

`ReadCursor`/`ConsumeEvents`/`AcknowledgeEvents` are Allium spec
constructs (`specs/skilj.allium`), not just an implementation detail -
this project treats the spec as the source of truth for behaviour, so
the fix went through `allium:tend` before any Rust was written, verified
clean with `allium check`/`allium analyse` (zero new diagnostics or
findings beyond what already existed).

- New `Config` section (the spec had none before this):
  `read_cursor_checkout_lease: Duration = 5.minutes`.
- `ReadCursor` gains `checked_out_at: Timestamp?` - `manual_ack` only,
  always `null` for `auto_advance` (which has no equivalent gap: serving
  already advances its own cursor in the same moment).
- `rule ConsumeEvents`: a new `is_leased` binding (`manual_ack`, not
  `is_new`, `checked_out_at` set and not yet past the lease) forces
  `served = []` when true; a new `ensures` branch claims the cursor
  (`checked_out_at = now`) whenever it serves a `manual_ack` batch not
  already claimed by a live lease.
- `rule AcknowledgeEvents`: unconditionally clears `checked_out_at`.
- `DeliveryFollowsAckMode` and the "Server-side read cursors" note both
  updated to state precisely that this narrows the existing at-least-once
  guarantee rather than weakening it - a served-but-unacknowledged batch
  is still always eventually redelivered, just never to two callers
  racing each other at once.

### The mechanism, and a bug caught by testing it for real

`event_store::consume_events` gained a `checkout_lease: chrono::Duration`
parameter and the `is_leased` logic verbatim from the spec;
`CursorUpdate` gained a `Claimed { checked_out_at }` variant alongside
the existing `Created`/`Advanced`/`Unchanged`. `ReadCursor.checked_out_at`
threads through `skilj-core`'s persistence layer
(`get_read_cursor`/`upsert_read_cursor`/`apply_cursor_update`/
`record_acknowledgement`), and `SkiljBuilder::read_cursor_checkout_lease`
(default 5 minutes, matching the spec) threads it down to
`skilj-rest`'s `GET /v1/events/consume` handler.

**The first implementation was wrong, and a real concurrent test caught
it, not review.** The obvious persistence-layer design - wrap the read
and the write in one transaction, holding a `SELECT ... FOR UPDATE` row
lock across both - works for an *existing* cursor but protects nothing
for a token's *very first* call: there's no row yet to lock, so two
concurrent first-time callers both see `existing_cursor = None` and both
get served. A new end-to-end test,
`two_concurrent_manual_consumes_never_both_serve_the_same_batch`
(`skilj/tests/event_fetch_rest.rs`, real HTTP through `Skilj::rest_router()`,
`tokio::join!` on two simultaneous requests for a brand-new token),
failed reliably (`a=1 b=1` - both served) with the row-lock design,
exposing exactly this gap.

**The fix**: `pg_advisory_xact_lock`, the identical primitive
`migrate_idempotency_keys_client_id_scoping` already establishes, keyed
by `(bounded_context, token_id)` - acquired first thing inside the same
transaction (`db::lock_read_cursor_for_consume`), blocking (not
`pg_try_advisory_xact_lock`: two callers racing the same token should
serialize for the few milliseconds a request takes, not have one
immediately give up), released automatically on commit. Unlike a row
lock, an advisory lock doesn't care whether the row exists yet, so it
protects the first-call case identically to every later one.
`get_read_cursor`/`apply_cursor_update` both became generic over `impl
sqlx::PgExecutor` (the same generalisation `insert_event` already has)
so the REST handler can call them inside the same locked transaction -
critical, not cosmetic: a lock taken on one pooled connection while the
protected read/write happen on a different one would be meaningless, the
identical failure mode that migration guard's own doc comment already
warns about. After the fix, the same end-to-end test passed clean across
15 repeated runs.

### Verified

New tests at every layer: `event_fetch_surface.rs` (5 new pure-function
tests - live claim serves nothing, stale claim reclaims, `manual_ack`'s
first call claims where `auto_advance` never does, plus updated
assertions on the two pre-existing tests whose own `Unchanged` outcome
this pass narrowed to `Claimed`), `persistence.rs` (2 new/extended DB
round-trip tests - `Claimed` persists without moving `sequence`,
`AcknowledgeEvents` clears a live claim), and the real end-to-end
`event_fetch_rest.rs` test described above. `cargo build/clippy
--workspace --all-targets -- -D warnings` and `cargo fmt --check` clean.
Full `skilj-core`/`skilj-rest` suites pass unchanged elsewhere, confirming
`auto_advance` and every other cursor path stayed untouched.

## 54. Bridge partitioning: skilj-kafka/skilj-amqp/skilj-nats, on top of §53

The design from §52's scoping: per-partition `EventReadToken`s, not
advisory-lock racing (these bridges are already one dedicated task per
mapping by design, unlike `Projection`/`Snapshot` catch-up's
interchangeable-fleet model) - each partition gets its own dedicated
`credential`, so each partition's own `read_cursors` row is already
independently checkout-protected by §53, with no new coordination needed
in `skilj-core` at all.

### The change, identical across all three crates

- `OutboundMapping` gains `partition: Option<(u32, u32)>` -
  `(partition_index, partition_count)`. `None` (the default) is
  unpartitioned, byte-identical to before this pass.
- A vendored copy of the same hand-written 64-bit FNV-1a
  `partition_for_key` §51 established (not `DefaultHasher` - see that
  section's own reasoning), plus `owns_partition_for(mapping, key)`.
  Vendored, not pulled in via a `skilj-core` dependency: all three
  bridge crates are deliberately skilj-core-free, pure REST/broker
  clients (confirmed by §52's own investigation - none has a `sqlx`
  dependency).
- Reuses each crate's own existing [`correlation_key`] output (already
  computed for Kafka's native partition assignment / AMQP's `group-id`)
  as the partitioning input - the same tag that decides broker-side
  placement now also decides which bridge instance is responsible for
  an event. NATS has no native partition concept to align with (its
  `correlation_tag_key` is header-only, per §38's own finding) - there,
  partitioning is purely skilj-side bookkeeping, no broker-side echo.
- `produce_once`'s own per-event loop: an event not owned by this
  mapping's own partition is acknowledged (advancing this instance's own
  cursor past it) but never produced/sent/published - the identical
  "position always advances even when nothing happens for this
  particular item" treatment `catch_up_cross_context_route` already
  gives an unregistered target command type. Not counted toward
  `served`, the same treatment a retry-exhausted skip already gets.

### Verified against real brokers, not just compiled

Each crate gained a real end-to-end test
(`two_partitioned_mappings_together_{produce,send,publish}_every_key_exactly_once`):
two `OutboundMapping`s, different dedicated credentials, same
`partition_count`, different `partition_index`, each given a single
`produce_once` call over an identical six-key queue - asserting the
union of what actually lands on the real broker (Kafka/Artemis/JetStream,
via each crate's own existing `testcontainers`-backed harness) is
exactly those six keys with no duplicates and no gaps, and that every
sequence was acknowledged on both tokens regardless of which one
produced it. All three passed clean against real ephemeral brokers
(Docker reachable partway through this work - initially unavailable in
this session, confirmed reachable and used for real verification once
it was), repeated multiple times each with no flakes. Each crate also
gained fast, Docker-free unit tests for `owns_partition_for`/
`partition_for_key` directly (unpartitioned is always owned; every key
is owned by exactly one of N partitions, never zero, never more than
one). Full pre-existing suites in all three crates (7 tests each,
including the pre-existing single-mapping outbound/inbound/retry/parking
tests) pass unchanged. `cargo build/clippy --workspace --all-targets --
-D warnings` and `cargo fmt --check` clean across the whole workspace.

### What this completes

Codeberg issue #25, in full: §51 (Projection partitioning), §52
(scoping - Snapshot partitioning built, CrossContextRoute found not to
fit, the bridge gap found), §53 (the `read_cursors` checkout mechanism
that gap needed), and this section (partitioning built on top of it).

## 55. Closing the `CancelDeadline` fire-vs-cancel race

A segmented review of everything since v0.0.6 (this project's own
`/code-review ultra` normally does this in the cloud, but that costs
real money the user didn't want to spend here - so this pass ran the
review as ordinary in-session forks against three commit-range segments
instead, then fixed what they found) turned up a real bug in §46's own
`fire_due_deadlines`: it selected due-and-`pending` rows, called
`decide_and_submit_command` (the real side effect), and only *afterward*
marked the row `'fired'` via `mark_deadline_resolved`'s own
`WHERE status = 'pending'` guard. Meanwhile `catch_up_cancel_deadline`
runs on its own independent poll loop and can flip that same row to
`'cancelled'` at any time via its own `WHERE status = 'pending'` update.
If the cancel landed between the `SELECT` and the submission, the row
ended up recorded `'cancelled'` while the target command had already
run - exactly the outcome `CancelDeadline` exists to prevent, e.g. an
order's cancel-if-unpaid deadline firing anyway right as the payment
lands.

The existing "deliberately no `FOR UPDATE SKIP LOCKED`" reasoning in
`fire_due_deadlines`'s own doc comment only ever covered the *fire-vs-fire*
race (two instances racing the same row both submit under the identical
idempotency key, so the second is a harmless `Deduplicated`) - it never
addressed fire-vs-*cancel*, and nothing else in the original issue #20
pass closed that gap.

**The fix**: each due row is now atomically claimed (`'pending'` ->
`'firing'`) immediately before submitting, via an `UPDATE ... WHERE
status = 'pending' RETURNING id` - only a row this instance actually
claimed gets submitted. `catch_up_cancel_deadline`'s own cancel `UPDATE`
already guards on `status = 'pending'`, so it naturally loses the race
once a row is `'firing'`, with no change needed there. A due row whose
own target `CommandType` isn't registered still resolves directly from
`'pending'` (no claim) - no side effect happens on that path, so losing
a race with a concurrent cancel there is just ordinary "cancelled before
it fired," not a bug.

**The new `'firing'` state needed its own crash-recovery story**: a
process that crashes between claiming and resolving a row would
otherwise leave it stuck `'firing'` forever, invisible to the `deadlines_due`
partial index (which only covers `'pending'`). Closed with a new nullable
`firing_at` timestamp column (patched into an already-provisioned
bounded context the same `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` way
`read_cursors.checked_out_at` was in §53) and a 5-minute staleness
window: `fire_due_deadlines`'s own `SELECT`/claim now also match a
`'firing'` row whose `firing_at` is older than that, atomically
reclaiming it. Five minutes is long enough that a live instance's own
`decide_and_submit_command` (a handful of DB round trips) never comes
close, short enough that a genuinely crashed claim retries on a
human-visible timescale. The retry is safe even if the original attempt
actually committed its command before crashing -
`RESERVED_DEADLINE_IDEMPOTENCY_KEY_PREFIX` makes the resubmission a
harmless `Deduplicated`, the identical defense-in-depth the original
fire-vs-fire race already relied on, now covering a second scenario.
`mark_deadline_resolved`'s own guard widened from `status = 'pending'`
to `status IN ('pending', 'firing')` to match, since it's now called
from both states depending on the path.

**Proof, not just the fix**: a new regression test
(`skilj/tests/deadlines.rs`) inserts a real due-and-`pending` row and
races the real `db::fire_due_deadlines` against a hand-issued
cancel-shaped `UPDATE` via `tokio::join!` - not sequenced with a sleep.
The assertion doesn't try to land inside a narrow timing window; it
checks an invariant that must hold under *either* ordering the database
lets win (`status == "cancelled"` implies the target command never ran,
`status == "fired"` implies it did) - deterministic regardless of actual
scheduling. Verified failing against the pre-fix code first (it failed
with exactly the predicted symptom: `status` ends `"cancelled"` while
the command still ran), then verified passing again with the fix
restored - this project's own standing discipline of not trusting a new
test until it's been shown to actually catch the bug it claims to.

## 56. Closing the parked-delivery duplication race in `catch_up_cross_context_route`

The same review pass (§55) found a second real bug, in §47's own
retry/parking mechanism this time: `get_cross_context_route_retry_state`
is a bare `SELECT` with no row lock, and `record_cross_context_route_retry_failure`/
`update_cross_context_route_cursor` write back an absolute
`retry_attempt_count` rather than an atomic increment. Two instances
polling the same route concurrently could both read the same
almost-exhausted retry state, both independently fail the identical
submission, both compute the policy as exhausted, and both call
`insert_parked_delivery` for the identical occurrence - two rows for one
real failure, an operator-facing phantom duplicate. The underlying
target-command submission itself was never at risk of duplicating (the
reserved idempotency-key prefix already covers that), only the parking
bookkeeping around it.

This is the same shape of bug as the async `Projection` catch-up
double-fold §51 fixed in this same commit range, and the `NOTIFY`
double-delivery and `CrossContextRoute` idempotency-key bugs from
earlier sessions before it - cross-instance races this project has
repeatedly needed a dedicated lock or guard for, not something a green
test suite catches on its own.

**The fix, two layers**:

1. **The must-have**: `catch_up_cross_context_route` now serializes its
   *entire* tick per `route_name` with a blocking `pg_advisory_xact_lock`
   - the identical primitive §53's `lock_read_cursor_for_consume` already
   uses for its own claim race, just held on a dedicated lock-only
   transaction here rather than the transaction the protected work
   itself runs in. Threading one shared transaction through the whole
   tick wasn't practical - it spans many independent pooled calls, and
   `decide_and_submit_command` alone opens and commits its own nested
   transaction - so a separate transaction that does nothing but hold
   the lock for the tick's duration serializes callers just as
   effectively, since what needs protecting is "only one instance runs
   this route's tick at a time," not any single row. `pg_advisory_xact_lock`
   releases automatically on commit or rollback, so there's no separate
   unlock call to forget on any exit path, panics included.
2. **Belt-and-suspenders**: a `UNIQUE` index on `parked_deliveries(source,
   kind, identifier)` (since widened to include the access token - see
   §64) - the tuple identifying one real failed occurrence,
   whichever `ParkedDeliveryKind` it is - so `insert_parked_delivery`'s
   own `INSERT ... ON CONFLICT (source, kind, identifier) DO UPDATE`
   turns any remaining duplicate-insert path, from this bug or a future
   one, into a harmless upsert (refreshing the failure details onto the
   existing row) instead of a second row. `RETURNING` so the id/fields
   the function returns always describe the row that actually exists
   afterward - the freshly generated id on a real insert, or the
   winning row's own id on a conflict.

**The unique index needed a real migration, not a bare `CREATE UNIQUE
INDEX`**: a bounded context that already hit this race can have genuine
duplicate rows sitting in `parked_deliveries` right now, and creating
the index directly against those would fail outright at startup - a
silent duplication bug turning into a hard crash. `migrate_parked_deliveries_dedup_and_unique_index`
follows `migrate_idempotency_keys_client_id_scoping`'s own shape from
§37: a dedicated transaction, a `pg_advisory_xact_lock` on the schema
name, an idempotency check against `pg_indexes` so an already-migrated
schema is a cheap no-op, then the real work - deleting every duplicate
but the newest (by `(last_failed_at, id)`, `id` only breaking an exact
tie) before creating the index. A duplicate row this deletes was never
independently actionable - both were always the identical occurrence
under `retryParkedDelivery`'s own terms - so discarding all but one
loses no real operator-facing information.

**Proof, not just the fix**: a new test
(`skilj-core/tests/cross_context_route_parking.rs`,
`concurrent_catch_up_ticks_never_park_the_same_occurrence_twice`) runs
two real `db::catch_up_cross_context_route` ticks concurrently via
`tokio::join!` against a target that always fails, paired with a
`RetryPolicy` that exhausts after a single attempt - the narrowest
possible window for this race. Verified failing against the pre-fix
code first (both the advisory lock and the unique index/upsert
temporarily reverted, reproducing exactly two parked rows for the one
occurrence), then verified passing again with the fix restored.

## 57. Shortening `submit_command`'s own lock hold (Codeberg issue #32)

A load test of `skilj-helpdesk` against this library (its own
`docs/load-test-report-2026-09-17.md`) found that total write throughput
for a single bounded context is capped at roughly 10-16 commands/second
- in total, across every tenant/entity sharing that bounded context, not
per-entity - independent of DB pool size or a consuming app's own
concurrency limiting. The reported symptom: a single `SELECT next_value
FROM sequence FOR UPDATE` observed taking up to 60s under concurrent
load, with the pool nowhere near exhausted. The root cause is exactly
what `submit_command`'s own doc comment already describes and §22
confirmed is deliberate: one bounded context's whole commit path -
dispatch/redispatch on conflict, `insert_command`, every triggered
event's insert - runs inside a single transaction holding that one
`sequence` row's lock, so no two commands against the same bounded
context can ever commit concurrently. That serialization is what keeps
`SequenceIsGaplessPerBoundedContext`/`DynamicConsistencyBoundaryHonoured`
true, and this pass does not touch it - what it does instead is shrink
how long each commit holds that lock, since every extra round trip made
while the lock is held directly caps how many commands/second the whole
bounded context can ever process, regardless of pool size.

**Three independent round-trip reductions, all inside `submit_command`
(`skilj-core/src/db/mod.rs`):**

1. **Batched sequence allocation.** A command whose decision triggers
   several events used to call `next_sequence` once per event, each its
   own round trip while the lock was held. `next_sequence_batch` claims
   the whole range in one `UPDATE {schema}.sequence SET next_value =
   next_value + $1 RETURNING next_value` instead - the returned
   high-water mark plus `count` derives the same contiguous, ascending
   range `event_specs` order already expected, just in one round trip
   instead of `count` of them. `next_sequence` itself (the single-value
   form) is untouched - every other call site, all single-event, still
   uses it.
2. **One `sync_projections` fetch per commit, not one per event.**
   `insert_event_and_update_sync_projections_in_tx` used to re-run
   `list_projections_for_bounded_context`'s own metadata read (itself
   several round trips: a bounded-context lookup, the `projections` row
   scan, one `consumed_event_types` query per row) on every single call
   - so a command triggering N events paid for it N times. The function
   now takes `sync_projections: &[Projection]` as a parameter instead of
   a `pool` handle, and every real call site (`submit_command`,
   `create_and_insert_external_event`, `create_and_insert_direct_event`,
   the scheduled-occurrence path, `insert_event_and_update_sync_projections`'s
   own pooled wrapper) fetches it exactly once via the new
   `sync_projections_for_bounded_context` helper and passes the same
   slice through every event one commit inserts.
   **This one is not free to reorder, though**: the read is still a
   plain `pool` read, not `tx`, deliberately (the same "small,
   admin-managed list, not worth locking" register the metadata read
   always was) - but it is only race-free *after* the caller's own
   bounded-context `sequence` row lock is already held, because
   `promote_projection_rebuild` (the one place a projection's own `sync`
   flag can flip mid-flight, drift audit finding #6) takes that
   identical lock before it can promote. Every call site therefore
   fetches `sync_projections` no earlier than right after its own
   `next_sequence`/`SELECT ... FOR UPDATE` on that row - fetching it
   before the lock, which would have shortened the lock's hold span
   further, would silently reopen that exact race. This is why this fix
   only removes *redundant* reads (N down to 1 per commit), not the one
   remaining read's own position relative to the lock.
3. **A pre-lock warm-up for the common, no-DCB-conflict path.** Every
   event type an accepted decision needs, and every encryption key its
   sensitive fields need, used to be resolved only after the lock was
   already held - one query per not-yet-cached event type, plus
   `resolve_encryption_keys`'s own `get_or_create_encryption_key` round
   trips. `submit_command` now runs that same resolution once, before
   `pool.begin()` is even called, against `initial_decision`'s own
   event_specs (the ones a redispatch-free commit will actually use) -
   mirroring `create_and_insert_external_event`/`create_and_insert_direct_event`'s
   own pre-existing "resolve encryption keys before opening the
   transaction" shape, extended here to event-type lookups too. Unlike
   the `sync_projections` fetch above, this warm-up is safe to run
   before the lock: `event_types_by_name`'s `Entry::Vacant` guard and
   `resolve_encryption_keys`'s own `resolved.contains_key` guard were
   already there for a different reason (letting several event specs
   share one resolution) and happen to make both maps tolerant of being
   pre-populated, partially stale (a DCB-conflict redispatch can still
   add new specs the warm-up never saw), or simply empty (an
   `initial_decision` that started out `Rejected` skips the block
   entirely) - the post-lock code is completely unchanged and still
   governs correctness; the warm-up only removes round trips for the
   large majority of commands that never hit a conflict at all.

**What this does not change**: the bounded-context lock itself, its
scope, or the invariants it protects - `submit_command`'s own doc
comment, and §22's confirmation that DCB's multi-entity atomicity is
already solved by tag-scoped `matching_events` rather than a
per-entity lock, both still apply exactly as before. The issue's own
write-up lists other options not attempted here - deferring
sync-projection folding to after commit, or batching several *different*
commands' sequence allocation into one lock acquisition - as real but
separately-scoped follow-ups, not something this pass rejected; picking
between them needs its own investigation, not a rider on a round-trip
cleanup. Per-tenant bounded contexts (the issue's own item 4, sidestepping
the ceiling entirely for a multi-tenant app at the cost of harder
cross-tenant querying) also stays a modelling recommendation for a
consuming app to weigh, not something this library can decide on an
app's behalf.

**Verified**: `cargo build/clippy/test --workspace` and `cargo fmt
--check` clean, real Postgres throughout (not skipped - the
embedded-Postgres/libxml2 workaround `CONTRIBUTING.md` describes). A new
test, `skilj-core/tests/submit_command.rs`'s
`submit_command_batches_sequence_allocation_for_a_multi_event_command`,
reuses the existing `TriggerTwoEvents` decider from the poison-pill
rollback test just above it, but with no `Poisonable` projection
registered (so nothing poisons the second event) - proving the two
resulting events land at sequences 0 and 1, in `event_specs` order, with
`next_sequence` continuing at 2 afterward: exactly the range one batched
allocation derives, with no gap and no overlap. The pre-existing
`submit_command_rolls_back_the_command_and_every_event_together_when_a_later_event_fails_to_insert`
test (same decider, `Poisonable` registered) already exercised the batch
call's own rollback path unchanged - both provisionally allocated
sequences roll back together on the poison event's failure, same as
before this pass.

## 58. Group-commit batching for `submit_command` (Codeberg issue #32, round two)

§57's round-trip reductions helped, but a follow-up load test against
`skilj-helpdesk` still found the same ceiling, because shortening one
command's own critical section does nothing about the fundamental shape
of the problem: every command commit to one bounded context is *fully
serialized* through that one `sequence` row's lock (required for
`SequenceIsGaplessPerBoundedContext`/`DynamicConsistencyBoundaryHonoured`,
still not something this pass touches), so under real concurrent load
the throughput ceiling is `1 / (lock hold time)`, full stop, no matter
how short each individual hold is. §57's own write-up already flagged
this as a real, separately-scoped follow-up rather than something it
rejected - this is that follow-up.

**The lever: make one lock acquisition serve more than one command.**
`skilj-core/src/command_batcher.rs`'s `CommandBatcher` implements a
"group commit" - standard database terminology for the same pattern
write-ahead-log flushes and MySQL's binlog use. Instead of every
concurrent caller independently opening its own transaction and queueing
for the lock, callers that arrive close together in time are coalesced
into one shared batch, processed by whichever one of them becomes that
batch's *leader* inside a single transaction (`db::submit_command_batch`),
committed once. N queued callers that used to mean N separate lock
acquisitions now mean one.

**Self-tuning, no batch-window or batch-size knob to guess.** A caller
becomes the leader for a new batch only when it finds its bounded
context's own queue empty at the moment it pushes onto it - a check made
under the same `Mutex` acquisition as the push itself, so leadership for
a given batch is assigned unambiguously, exactly once. The leader does
*not* immediately grab whatever's in the queue: it first does the slow
part (`pool.begin()` plus the actual `SELECT ... FOR UPDATE` wait, which
is exactly the time other commands are still arriving and queueing
behind it under real contention) and only *then* drains the queue,
taking everyone who joined in the meantime (capped at `MAX_BATCH_SIZE =
256`, so a pathological pile-up still bounds one batch's own lock hold
time and memory rather than growing unboundedly - nothing is lost, the
remainder just becomes a later batch). Under low load, a lone caller
becomes its own leader, finds nobody else queued once its lock is
acquired, and pays essentially the identical latency a standalone
`submit_command` call always had - a batch of one. Under heavy load -
exactly the case the load test found collapsing - the leader's own wait
for the lock is naturally longer, so more callers accumulate in that
same window, producing a *larger* batch precisely when amortising the
lock acquisition matters most.

**Making `submit_command`'s own machinery reentrant per command, not
just once, was the real implementation work.** Its post-lock half is now
`submit_one_command_in_tx`, callable against a transaction (or, in the
batched path, a `SAVEPOINT`-backed nested transaction - `tx.begin()` on
an already-open `sqlx::Transaction`) that some earlier command in the
same batch already used, rather than always a fresh one. `db::submit_command_batch`
opens one transaction, takes the bounded-context lock exactly once
(`lock_bounded_context_sequence`), fetches `sync_projections` once for
the whole batch (identical reasoning to §57 item 2), then runs every
command in order, each inside its own `SAVEPOINT`: a real per-command
failure (an unregistered event type, a database error - never an
ordinary `Rejected` decision, which isn't an `Err` at all) rolls back
only that command's own work (`ROLLBACK TO SAVEPOINT`, awaited
explicitly rather than left to `Drop`, since every command in the loop
shares one underlying connection and the rollback must be known-complete
before the next `tx.begin()` reuses it) - not its batch-mates', and not
the shared lock. `submit_command` itself is now just this same function
called for a batch of exactly one.

**Same-batch DCB conflicts need a second detection path, not just
`locked_highest`.** The pre-existing conflict check compares
`locked_highest` (the bounded context's own latest sequence, read under
the lock) against `original_highest` (the caller's optimistic read) -
but two commands *in the same batch* can each have computed an identical
`original_highest` before either knew the other existed, and neither
one's own insert is visible to a plain `pool` query yet (it's sitting
uncommitted on `tx`'s own connection). `submit_one_command_in_tx` takes
a new `extra_committed_events` parameter for exactly this: every accepted
command's own events, accumulated in memory by `submit_command_batch` as
it works through the batch and handed to each subsequent command,
tag-filtered the same way the `pool`-sourced delta already is. Proven by
`submit_command_batch_detects_a_dcb_conflict_between_two_commands_in_the_same_batch`
(`skilj-core/tests/submit_command.rs`) - without this, two same-batch
commands shipping the same order would both be accepted, silently
double-shipping it.

**The pre-lock warm-up (§57 item 3) moves earlier still.** Rather than
whichever caller becomes leader running it once, serially, for the whole
batch, `warm_up_event_types_and_encryption_keys` is pulled out of
`submit_command` into its own function and run by *every* caller,
fully in parallel, before any of them join the shared queue at all
(`CommandBatcher::submit`). Still governed by the identical
`Entry::Vacant`/`resolved.contains_key` guards that make it tolerant of
being partial, stale, or simply unused (a redispatch inside
`submit_one_command_in_tx` can still add specs the warm-up never saw) -
this is purely about moving already-safe-to-parallelize work earlier,
not a new correctness mechanism.

**Wiring**: `Skilj` now owns one process-wide `CommandBatcher` (`skilj/src/lib.rs`),
constructed once in `.build()` and cloned into `RestState`/`GraphqlState`
alongside `event_broadcaster`/`event_cache` - the identical "one shared
instance, not a redundant second one" treatment those already get, for
the same reason: every concurrently-submitting caller across both wire
surfaces needs to reach the *same* batcher. `skilj-rest`'s
`post_commands_trigger` and `skilj-graphql`'s `submitCommand` resolver
and parked-delivery redrive - real, externally-triggered, potentially
bursty submission volume, exactly what this exists to coalesce - now
call `CommandBatcher::decide_and_submit` instead of
`db::decide_and_submit_command` directly. The lower-volume, periodic
internal callers (the cross-context event router's own tick,
`fire_due_deadlines`) deliberately still call `db::decide_and_submit_command`
directly: batching buys the least where a caller is already ticking on
its own schedule rather than arriving as a burst of concurrent external
requests, and routing them through the batcher would only add a
queueing indirection with no real coalescing to show for it.

**A new error variant for followers**: `Error::BatchFailed(String)` -
what a non-leader caller gets back if the leader's own lock acquisition
or final commit failed (every command in that batch shares this, since
nothing in it is trustworthy once the shared transaction is in doubt) or
if the leader's task ended without ever sending a reply at all (a bug,
not a real database error, but a follower still needs *some* `Result`).
Carries the underlying failure's own rendered message rather than the
original typed error, since `crate::error::Error` isn't `Clone` (it
wraps `sqlx::Error`, which isn't either) - the leader's own return value
keeps the original. `skilj-rest`'s `status_for` maps it to the identical
`INTERNAL_SERVER_ERROR` `Error::Database` already gets; it isn't this
caller's fault and doesn't map to anything more specific it could itself
recover from.

**Verified**: `cargo build/clippy/test --workspace` and `cargo fmt
--check` clean, real Postgres throughout. Two new tests in
`skilj-core/tests/command_batcher.rs` exercise `CommandBatcher` itself,
end to end, under real concurrency:
`concurrent_submits_for_the_same_order_accept_exactly_one_and_reject_the_rest`
and `concurrent_submits_for_different_orders_all_accept_with_gapless_distinct_sequences`.
Two new tests in `skilj-core/tests/submit_command.rs` exercise
`db::submit_command_batch` directly - the same-batch DCB-conflict
detection above, and
`submit_command_batch_isolates_a_failing_commands_savepoint_from_its_batch_mates`
(the poison-pill mechanism §22's own rollback test uses, proving a
second command's real failure rolls back only its own savepoint and its
own provisional sequence allocation, leaving the first command's event
and `next_sequence`'s own continuation both intact).

**What this does not change**: the bounded-context lock itself, its
scope, or the invariants it protects, exactly as §57 already noted -
every command in a batch still commits inside the one transaction that
holds that lock, `DynamicConsistencyBoundaryHonoured` is enforced
identically whether a command is alone in its own batch or one of 256,
and idempotency-key deduplication (§37) is checked per command, inside
its own savepoint, unchanged.

## 59. Fixing §58's own permanent-hang regression, and closing the cross-instance routing investigation alongside it (Codeberg issues #36, #35)

A real load test against `skilj-helpdesk` (Codeberg issue #36) found
that §58's `CommandBatcher` didn't just fail to raise the throughput
ceiling under heavy load - it could wedge a whole bounded context
*permanently*: one connection stuck `idle in transaction` holding the
`sequence` row's lock forever, every other writer queueing behind it
with no self-recovery short of a server restart. Investigated alongside
Codeberg issue #35 (cross-instance command routing), filed in the same
window and sharing the identical lock/leader machinery - closing both
together turned out to matter, not just be convenient: §35's own
recommended future design (see below) builds on exactly the seam this
fix introduces.

**A real, confirmed bug: `run_as_leader` drained the queue *before*
acquiring the lock, not after.** `command_batcher`'s own module doc
comment always described the self-tuning design as "the leader does not
immediately grab whatever's in the queue - it first does the slow part
(`pool.begin()` plus the actual `SELECT ... FOR UPDATE` wait) ... and
only then drains the queue." The code never did that: `run_as_leader`
called `drain_up_to` first and `db::submit_command_batch` (lock included)
second. In practice this meant a caller almost never found anyone else
already queued - the whole point of the self-tuning design, growing
batches under contention precisely when amortising the lock matters
most, was quietly defeated from day one. Worse, under real concurrent
load this produces something closer to the *pre-batching* shape (many
separate leaders, each opening their own transaction and queueing for
the same row lock) while paying the batcher's own extra bookkeeping on
top - plausibly part of why the load test found a *worse* failure mode,
not just the same ceiling.

Fixed by splitting `db::submit_command_batch` into two halves:
`db::begin_command_batch_leader_tx` (opens `tx`, optionally sets the
idle-in-transaction backstop below, takes `lock_bounded_context_sequence`,
fetches `sync_projections`) and `db::commit_command_batch` (runs the
per-command `SAVEPOINT` loop and the final `tx.commit()`, unchanged from
before). `run_as_leader` now awaits the lock-acquiring half *first*,
drains the queue *unconditionally* right after - success or failure
alike, so a follower that joined during the wait is never left stranded
in a queue nobody will drain again, since a *new* leader is only ever
assigned when the queue is found empty - and only then calls the
processing half with whatever the drain collected. `submit_command_batch`
itself survives as a thin wrapper (`begin` immediately followed by
`commit`, no queue involved) purely so this crate's own hand-built-batch
tests (`skilj-core/tests/submit_command.rs`) keep working unchanged.

**Defense-in-depth backstop, regardless of whether this was the load
test's *only* cause**: `begin_command_batch_leader_tx` takes an optional
`idle_in_transaction_session_timeout` and, when set, issues `SET LOCAL
idle_in_transaction_session_timeout` on the leader's own transaction
right after opening it - scoped to that one transaction only (`SET
LOCAL` unwinds at commit/rollback/timeout, never leaking onto whatever
the pooled connection serves next). If a batch leader's connection ever
does stall indefinitely for *any* reason - this bug, a stuck decider, a
hung downstream await, something not yet found - Postgres itself kills
the idle session and releases the row lock, so a later batch can proceed
instead of every writer to that bounded context queueing forever behind
a wedge nothing else ever clears. `CommandBatcher::with_idle_in_transaction_timeout`
(default 30 seconds - generous for any real batch, short enough that an
operator would never need to notice and restart) and
`SkiljBuilder::command_batch_idle_in_transaction_timeout` wire it through
the same "sensible default, escape hatch alongside it" register as
`read_cursor_checkout_lease`/`pool_options`. Proven end to end, not just
read from the code: `a_batch_leader_stuck_past_the_idle_in_transaction_timeout_is_killed_and_releases_the_lock`
(`skilj-core/tests/command_batcher.rs`) fakes a leader that never comes
back via `begin_command_batch_leader_tx` directly, sleeps well past the
configured timeout, then proves a *separate* real submission to the same
bounded context still completes - verified to actually fail (timing out
at 5s) with the timeout disabled, before being restored, so the test is
known to exercise the real mechanism and not pass vacuously.

**Codeberg issue #35, closed alongside this rather than separately**:
investigated whether multiple `skilj` instances routing commands with
overlapping DCB tags to different instances is a correctness risk.
Conclusion: no bug. The `sequence` row's `FOR UPDATE` lock is a standard
Postgres row lock - it serializes every transaction wanting it
regardless of which process or instance opened it, and `submit_one_command_in_tx`'s
"optimistic dispatch, recheck under lock, redispatch on conflict" shape
was already built for exactly this (a caller's own stale optimistic read
is a stale read whether the thing that beat it there was on the same
instance or a different one - not new, not specific to §58's batching).
What multiple instances *do* lose is §58's own batching benefit: its
queue is in-process (`Arc<RwLock<HashMap<...>>>` inside one `Skilj`), so
a load balancer spreading a hot bounded context's traffic across N
instances means each instance's own batcher only ever sees ~1/N of the
concurrent volume - smaller batches, more separate lock acquisitions in
aggregate, and adding instances to handle more load can make contention
on that one row *worse*, not better. Recommended future direction (not
built here - a genuinely new subsystem, deserving its own design pass):
a raw-command table plus a DB-elected single worker per bounded context,
generalising `CommandBatcher` itself across instances the same way this
already works within one - "whichever instance holds a session-scoped
advisory lock on a dedicated pinned connection is the leader" in place of
"whichever caller finds the queue empty," auto-released (and so
auto-failed-over) the instant that connection drops, reusing every
coordination mechanism `cross_instance.rs` already proves out (Postgres
as the only coordination substrate, `NOTIFY`/`LISTEN` for at-most-once
wake-up, no instance discovery ever) rather than inventing peer-to-peer
RPC. Opt-in per bounded context, defaulting to today's direct
in-process path.

**Why closing both together mattered**: this session's own
`begin_command_batch_leader_tx`/`commit_command_batch` split is exactly
the seam that future cross-instance design needs - "acquire this bounded
context's exclusive right to process the next batch" is now a single,
swappable step, separate from "process whatever batch you're handed."
A cross-instance leader-election implementation can replace the body of
the first half (a plain `pool.begin()` + row lock) with the
advisory-lock-based election #35 describes, without touching the second
half at all. Splitting them apart for issue #36's own reasons happened
to do most of the structural work issue #35's future implementation
would have needed anyway.

**Verified**: `cargo build/clippy/test --workspace` and `cargo fmt
--check` clean, real Postgres throughout (`skilj-core/tests/command_batcher.rs`'s
existing two concurrency tests, unchanged, still pass; one new test
added, described above). A workspace-wide `cargo test --workspace` run
also surfaced pre-existing, unrelated `PoolTimedOut`/schema-race
flakiness in a few `skilj/tests/*` files under this environment's own
resource pressure from many embedded-Postgres-backed test binaries
running at once - reproduced identically against unmodified `main`
(`0451fd0`) via `git stash`, so confirmed pre-existing, not a regression
this pass introduced; every test file this pass actually touches passes
cleanly and repeatably in isolation.

## 60. Shrinking a batch's own per-command round trips further (Codeberg issue #32, round three)

A follow-up load test against `skilj-helpdesk` (post-§59) confirmed the
wedge was fixed and found roughly double the pre-batching throughput
ceiling, but also confirmed §58's own module doc comment was right that
the remaining ceiling is architectural: `commit_command_batch`'s
per-command loop still paid several round trips *per command*, all
inside the one lock hold the whole batch shares, that had nothing to do
with each command's own real work. Two independent fixes, on a separate
branch (`perf/batch-round-trip-reduction`), reviewed and tested before
merging - not shipped from the load test's own read alone.

**Fix one: stop re-fetching a `BoundedContext` row every batch already
has in hand.** `submit_one_command_in_tx`'s own DCB-conflict delta check
(`list_events_for_bounded_context_matching_tags`) re-fetched the
bounded context row - plus, transitively, its `created_by_role_id`'s own
`Role` row - via a fresh `pool` round trip every time `locked_highest >
original_highest`, which is true for nearly every command once a batch
has grown past one or two. Every command in one `CommandBatcher` batch
shares the identical bounded context (that's what the queue is keyed
on), and `command_type.bounded_context` is already that exact row -
loaded once when the command type itself was resolved, already trusted
elsewhere in the same function for `.name` without re-fetching. Added
`list_events_for_bounded_context_matching_tags_with_bc`, taking `&BoundedContext`
instead of a name to fetch; the public, name-taking function now just
fetches once and delegates. Zero behaviour change, zero new fetches -
purely deleting up to `MAX_BATCH_SIZE` (256) redundant round trips per
batch.

**Fix two: pool sequence-number allocation across a whole batch instead
of once per command.** `next_sequence_batch` was called once per command
inside its own `SAVEPOINT` - safe (a command's own rollback un-reserves
exactly what it asked for) but still one `UPDATE {schema}.sequence ...
RETURNING` round trip per command. A naive "allocate the whole batch's
total up front" fix is actually unsafe: a command that fails *after* its
numbers are allocated (an unregistered event type, a poison-pill
projection) would otherwise leave those specific numbers permanently
unused - a real gap `SequenceIsGaplessPerBoundedContext` forbids, since
nothing recycles them to a later command.

Fixed properly by first splitting the old, monolithic `submit_one_command_in_tx`
into `decide_command_in_tx` (idempotency lookup, DCB-conflict redispatch,
event-type warm-up - everything before a sequence number is needed,
returning either a terminal `Rejected`/`Deduplicated` outcome or an
`AcceptedDecision`) and `finish_accepted_command_in_tx` (everything from
encryption resolution through the actual inserts, given sequence numbers
already in hand). `submit_one_command_in_tx` itself survives as a thin
`decide` → plain `next_sequence_batch` → `finish` composition, unchanged
in observable behaviour, purely so `submit_command`'s own single-command
path (no batching, no pool, nothing to amortise across) keeps working
exactly as before.

`commit_command_batch`'s own loop instead calls `decide_command_in_tx`
directly against the *leader's `tx`* - wrapped in its own tiny
`SAVEPOINT` purely so a genuine DB error from its one `tx`-scoped read
(`lookup_idempotency_key`) can't abort `tx` and take every command still
queued behind it down too, the isolation the old code got for free from
running inside a per-command `SAVEPOINT` already - then, for an accepted
decision, draws from a new `SequencePool` (also against `tx`, not a
`SAVEPOINT`) before opening the write-phase `SAVEPOINT`
(`finish_accepted_command_in_tx`) as before. Drawing against `tx`
itself, deliberately outside any `SAVEPOINT`, is what makes pooling safe:
a reservation survives a later command's real failure instead of being
rolled back with it. `SequencePool` reserves in chunks sized to "how many
commands are still left in this batch" (so a run of small, single-event
commands can share one round trip), remembers each draw, and requires
the caller to call exactly one of `confirm_last_draw` (the command
committed - numbers truly spent) or `return_last_draw` (the command
failed - numbers go back onto the *front* of the queue, in order, for
whichever command draws next) once that command's outcome is known.
Only genuinely unused numbers - left in the pool when the batch's own
loop ends - are corrected back out of `{schema}.sequence` in one shot
(`shrink_back`), right before the shared `tx.commit()`. Because the pool
is only ever driven by one strictly sequential loop, never concurrently,
the persisted `next_value` this leaves behind is provably identical to
what one `next_sequence_batch` call per command, each inside its own
`SAVEPOINT`, would have left - only the round-trip count differs. One
accepted, documented behaviour change: a failure specifically in the
pool's own refill `UPDATE` (not any per-command work) now fails the
*whole* batch, the same `BatchFailed`-for-everyone treatment a final
`tx.commit()` failure already gets, rather than being isolated to one
command - a deliberate trade, since that operation deliberately runs
outside any `SAVEPOINT` and a real failure there almost certainly means
`tx`'s own connection is already broken for every command still to come.

**Verified with new tests, not just re-running old ones**: `skilj-core/tests/submit_command.rs`
gained `submit_command_batch_recycles_a_failed_commands_sequence_numbers_for_a_later_command_in_the_same_batch`
(a three-command batch - accept, fail, accept - proving the third
command's event lands on the sequence number the failing second command
drew and gave back, not a fresh one past it, and that `next_sequence`
afterward reflects only the two real events) and
`submit_command_batch_deduplicates_a_repeated_idempotency_key_shared_by_two_commands_in_the_same_batch`
(proving `decide_command_in_tx`'s idempotency lookup still sees an
earlier same-batch command's own uncommitted-but-`SAVEPOINT`-released
insert, unchanged by the decide/finish split). Every existing test in
`skilj-core/tests/submit_command.rs` and `skilj-core/tests/command_batcher.rs`
- including the two that already hand-prove sequence gaplessness and
same-batch failure isolation - passes unchanged against real (embedded)
Postgres, alongside `cargo check/clippy --workspace --all-targets -D
warnings` and `cargo fmt --check` clean. Landed on its own branch
(`perf/batch-round-trip-reduction`); no fresh end-to-end load-test
number captured for this pass specifically - the round-trip counting
above is derived from reading the code path, not measured throughput,
so treat the ceiling improvement as directionally expected rather than
quantified until re-run against `skilj-helpdesk`.

## 61. Closing §60's own measurement gap, a real load-test surprise about `skilj-helpdesk`'s own tenancy shape, and one more round-trip cut (Codeberg issue #32, round four)

§60 shipped without a fresh load-test number. This pass re-ran the exact
`docs/load-test-report-2026-09-18c.md` methodology against §60's own
commit (`5fa2e67`) and reproduced its numbers closely (throughput within
a few percent at every step), confirming the setup is faithful - then
went further than any prior pass by adding real instrumentation instead
of reasoning from throughput deltas alone.

**A `skilj.command_batch.size` histogram (plus a matching `tracing::info!`
line for zero-infra grepping) settles what three prior load-test reports
only speculated about.** Batch size *does* scale with load exactly as
`command_batcher`'s own doc comment predicts - mean batch size climbed
1.25 (8 workers) → 1.98 (20) → 6.30 (40) → 15.0 (80), batch-of-one share
dropping from 82% to 9% - but aggregate throughput stayed flat at ~27/s
across the 20/40/80-worker steps regardless. That rules out "the batcher
isn't forming big enough batches" as an explanation for the ceiling, with
real measurement rather than inference: the remaining cost is
per-command work inside the batch, not batch formation itself, exactly
as §58/§59/§60's own analyses already suspected but never confirmed.

**A bigger finding, unrelated to `skilj-core` itself: `skilj-helpdesk`'s
own single shared "helpdesk" bounded context - the thing every one of
these four load tests has been measuring the throughput ceiling *of* -
is an explicitly documented placeholder, not the target architecture.**
`specs/skilj-helpdesk.allium` calls for one tenant bounded context per
company via `skilj/CreateBoundedContextFromTemplate`; `helpdesk.rs`'s own
module doc comment says multi-tenant provisioning is deferred, keeping
Company and Ticket side by side in one context "to prove the real
`decide()` logic, not the tenancy mechanism around it." Once that lands,
three (or three thousand) companies' traffic stops sharing one lock
entirely. This doesn't change anything in `skilj-core` - `CreateBoundedContextFromTemplate`
already exists as a library guarantee - but it means `skilj-helpdesk`'s
own ~27-28/s ceiling is "one bounded context under this workload," not a
`skilj-core` ceiling, and the highest-leverage next step for that
project's real throughput is wiring up the deferred tenant provisioning,
not further round-trip work here.

**Two `skilj-core` changes landed anyway, real but narrower than hoped:**

- **Batching the `command_encryption_keys`/`event_encryption_keys` join
  inserts** (one `INSERT ... SELECT $1, unnest($2::bigint[])` instead of
  one round trip per key) **and parallelizing `resolve_encryption_keys`**
  in `finish_accepted_command_in_tx` (gathering every distinct subject
  across a command's own payload and all its accepted events' payloads
  up front - pure, no I/O - deduping, then resolving the survivors
  concurrently via `futures_util::future::try_join_all` instead of one
  payload at a time). Correct and tested (`skilj-core`'s full suite,
  including `tests/encryption.rs`, passes unchanged), but a **null result
  for `skilj-helpdesk` specifically**: that bounded context declares zero
  `sensitive_fields` anywhere, so `encryption_key_ids` and the resolved-subject
  list are always empty for its workload - re-running steps 3 and 5
  against this build showed no real throughput change, exactly as
  expected once checked. Real value for any deployment that *does* use
  `sensitive_fields` (`skilj-demo`, or a future `skilj-helpdesk` PII
  feature), just not this one. `futures-util` moved from `skilj-core`'s
  `dev-dependencies` to real `[dependencies]` for this (previously only
  a test dependency for the Codeberg issue #15 warm-up-concurrency A/B
  test).

- **Moving the same-batch idempotency check off `tx` entirely, removing
  `decide_command_in_tx`'s own `SAVEPOINT`.** `decide_command_in_tx` used
  `tx` for exactly one thing - `lookup_idempotency_key` - and that own
  doc comment already says the bounded-context lock makes a plain `pool`
  read just as race-free as a `tx`-scoped one for anything genuinely
  already committed (the identical reasoning the DCB delta query a few
  lines below it already relies on, via `pool`, for the exact same lock).
  The only case a `pool` read can't see is a same-batch, not-yet-committed
  duplicate - covered by a new in-memory `batch_idempotency_keys: HashMap<(command_type_name,
  client_id, idempotency_key), triggered_event_sequences>` in
  `commit_command_batch`'s own loop, populated right after each accepted
  command's persist succeeds, checked first, `pool` as the fallback - the
  exact same two-source shape `extra_committed_events` already
  established for the DCB case. With no `tx`-scoped statement left inside
  it, `decide_command_in_tx` needed no `SAVEPOINT` of its own at all -
  §60's `decide_tx` wrapper is gone outright (not merged with the
  write-phase `nested` `SAVEPOINT`; those two were investigated for
  merging first and found genuinely incompatible - `SequencePool`'s own
  draw deliberately sits *outside* any `SAVEPOINT`, between decide and
  finish, specifically so a rolled-back command's sequence numbers can be
  recycled rather than silently gapped, so folding decide and finish into
  one `SAVEPOINT` would pull the draw inside one and break
  `SequenceIsGaplessPerBoundedContext`). One fewer `SAVEPOINT`/`RELEASE`
  round-trip pair per command, unconditionally - every command, not just
  ones carrying an idempotency key. Verified by the tests that already
  hand-prove this exact scenario -
  `submit_command_batch_deduplicates_a_repeated_idempotency_key_shared_by_two_commands_in_the_same_batch`
  and `submit_command_with_the_same_idempotency_key_from_two_different_clients_does_not_collide`
  both pass unchanged, alongside the rest of `skilj-core`'s 736-test
  suite, `cargo clippy --all-targets -D warnings`, and `cargo fmt --check`.

**A fuller "pipeline `decide(i+1)` concurrently with `persist(i)`" design
was investigated and deliberately not built.** Even with the idempotency
check off `tx`, real concurrency between two commands' own `tx`-bound
work (the sequence draw, the actual inserts) is impossible within one
connection - not a tuning problem but a hard constraint: Rust's borrow
checker won't allow two live `&mut tx` borrows at once, and one Postgres
connection can't have two statements in flight regardless. The only
genuine overlap available is CPU-only `dispatcher.decide()` work (and a
`pool`-only delta query, when a real conflict is suspected) against an
*adjacent* command's `tx`-bound writes - real, but for the common
no-conflict case `decide_command_in_tx` now does close to zero I/O
already, leaving little to actually overlap. Building a full tag-based
dependency scheduler for that uncertain a return, in the same code area
that already produced two real regressions (§59, Codeberg issues #36/#35),
wasn't judged worth it this round. Landed on `perf/batch-round-trip-reduction`,
same branch as §60.

## 62. Finding and fixing the real ceiling: a redispatch check's own row-materialization cost (Codeberg issue #32, round four, continued)

§61's SAVEPOINT removal, measured directly against `skilj-helpdesk`,
moved throughput by an amount indistinguishable from this environment's
own run-to-run noise (§61 itself said so plainly). Digging into *why*
required going further than any prior pass in this whole issue: real
per-phase wall-clock instrumentation inside `commit_command_batch`
itself (`decide_us`/`sequence_us`/`persist_us`/`commit_us`, one
`tracing::info!` line per batch), not just round-trip counting from
reading the code.

**What it found**: at real concurrent load, `decide` - not `persist` -
was 77-86% of the batch loop's own wall-clock time, averaging
15-18ms/command. Tracing one level further in, 86% of *that* was one
specific call: the DCB-conflict redispatch check
(`list_events_for_bounded_context_matching_tags_with_bc`), which fires
for essentially every command once a batch is large enough that its
shared `locked_highest` has outrun some individual command's own stale
pre-lock snapshot - true of nearly every command under real load,
regardless of whether a genuine conflict exists (59% of these queries
returned zero rows). And *within* that query, splitting pool-acquire
from execution from row-materialization found the query itself was
fast (under 2ms combined) - the real cost was building the `Event`
objects for whatever rows it *did* return: `get_event_type` (its own
redundant `get_bounded_context` re-fetch, the identical class of bug
§60's "fix one" already closed at a different call site) and, far more
expensive, `event_origin_from_row`'s `command_triggered` case, which
fetches the *entire* originating `Command` via `get_command_by_id` -
itself several round trips deep (`get_command_type` →
`get_bounded_context` → `get_role`) - **per row, uncached, every single
time**. Measured precisely: 98% of the row-processing loop's own cost
was this one chain.

**Two wrong turns before the real fix, both caught by measuring rather
than assuming**:

1. Threading a caller's already-warmed `event_types_by_name` map into
   the delta query's own row loop, reasoning it duplicated work
   `decide_command_in_tx` had already done. Built, tested, shipped to a
   load test - **zero measurable effect**. The map is warmed for the
   command's own *future* events, not the *historical* events a delta
   query re-checks - almost never the same types, so the cache was
   real but almost never hit for this call site. A useful, correct,
   harmless change in its own right (kept), just not the fix.
2. Parallelizing the row loop's per-row fetches via `try_join_all` -
   the same technique §61/`finish_accepted_command_in_tx` already used
   successfully for `resolve_encryption_keys`. Measured *worse*:
   accepted throughput at step 5 dropped to 22.5/s (from a ~27/s
   baseline) even though `decide_us` itself fell 3x (18ms → 6.4ms/command).
   The batch loop's own lock-wait time exploded to ~1.7s mean (up from
   under 100ms) - bursting many simultaneous connection requests out of
   an already-contended pool starved the *next* batch's own lock
   acquisition, a real regression this session caught and reverted
   before it ever left the branch, not a hypothetical risk. The lesson,
   confirmed rather than assumed: this workload was never short on
   *parallelism*, it was short on *total round-trip count* against a
   size-capped connection pool - concurrency without reduction just
   moves the queueing, it doesn't remove it.

**The fix that actually worked: batch, don't parallelize.** Two new
functions, `get_commands_by_ids_with_bc` and
`get_event_types_by_names_with_bc`, each one `WHERE id/name = ANY($1)`
round trip for however many distinct commands/event-types a delta
result needs - collapsing what used to be up to one
`get_command_by_id`-chain per row into one query for the whole batch's
own row set (plus one more for the distinct `command_type`s those
commands reference, itself taking `bc` directly rather than re-fetching
it - same `_with_bc` pattern as `get_event_type_with_bc`/§60's original
fix). `event_origin_from_row` itself is untouched, and unused for this
call site now - its logic is inlined here against the pre-fetched maps,
since building an `EventOrigin` from data already in hand is pure/sync,
no `.await` needed per row at all once the two batched fetches are done.

**Measured, not assumed**: full `skilj-core` suite (736 tests, including
`submit_command_redispatches_and_rejects_on_a_genuine_dcb_conflict`
and the other DCB/redispatch tests that exercise this exact path with
real `command_triggered` events) passes unchanged, `clippy --all-targets
-D warnings` and `fmt --check` clean. Against `skilj-helpdesk`, real
before/after load-test numbers, same methodology as every prior report:

| step | workers | before (§61 baseline) | after |
|---|---|---|---|
| 3 | 20 | ~27/s | **45.10/s** (+67%) |
| 5 | 80 | ~27/s | **57.75/s** (+115%) |

Phase timing on the fixed build confirms the mechanism, not just the
headline number: `decide_us` fell from ~18ms to ~5.7ms/command, mean
lock-wait time at step 5 dropped to ~482ms (nowhere near the failed
concurrent attempt's ~1.7s), and mean batch size *grew* to ~34.5 (up
from ~15-16) - the self-tuning batcher given more room to work now that
each batch's own processing is faster, a virtuous cycle rather than a
coincidence.

**What's still open**: `skilj-helpdesk`'s own deferred per-company
tenant provisioning (§61's other finding) remains the bigger, separate
lever for that project's real-world ceiling - this fix raises the
throughput of *one* bounded context under contention, which is exactly
what today's single-shared-context deployment shape needs, but doesn't
change the fact that a properly multi-tenant deployment would mostly
sidestep this lock's contention entirely. Landed on
`perf/batch-round-trip-reduction`, same branch as §60/§61.


## 63. Code-review fixes for the group-commit batcher (post-0.0.7 review)

A `/code-review high v0.0.7..HEAD` pass over the batching work (§58-§62) found six things; two were real permanent-hang bugs of the same class as issue #36.

1. **Over-`MAX_BATCH_SIZE` remainder was stranded (real).** Leadership is only assigned to a push that finds the queue empty. `drain_up_to` left the remainder queued, so nobody was ever elected for it. `run_as_leader` now loops (lock, drain, process) until a drain empties the queue; `drain_up_to` reports whether a remainder exists, atomically with the drain. Test: 300 queued commands behind an externally held lock all get answered.
2. **Leader election was not cancellation-safe (real).** The leader runs inside the caller's request future, which the server can drop (client disconnect). A `LeaderGuard` stays armed until a drain empties the queue; if dropped first it fails everything still queued with a retryable `BatchFailed` and empties the queue, so the next push elects a fresh leader. This required switching the queue to a `std::sync::Mutex` (the guard's `Drop` is synchronous; the lock is never held across an await). Followers are failed rather than promoted - promoting one would mean moving its already-queued command out from under its own waiting future, for a rare case. Test: leader aborted mid-lock-wait, follower answered, later submit succeeds.
3. **Pool exhaustion (real, latent).** Each leader pins a connection while its decide/persist reads need more. A semaphore in `CommandBatcher` (sized lazily to half the pool's `max_connections`, minimum 1) caps concurrent leaders across bounded contexts, so non-leader work can always get a connection.
4. **Idle timeout across a big batch (mostly a misreading).** Postgres's idle clock restarts after every statement, so it bounds the gap between two statements, not the batch. Documented on `begin_command_batch_leader_tx`; no code change. Lock wait is an active statement and is deliberately not covered.
5. **Follower errors lost their type (partly fixed).** `Error::BatchFailed` is now `{ code, message }`, copied from the original error, so `extensions.code` matches what the leader sees. The HTTP status stays 500 and the typed variant is still lost (`Error` is not `Clone`).
6. **Savepoint statement failure aborts the whole batch (correct as is).** If `tx.begin()`/`commit()`/`rollback()` on a savepoint fails, the shared connection is unusable and the earlier commands' writes exist only in that transaction, so failing the batch is the only consistent outcome. Per-command failures are still isolated by the savepoint. Comment added.

**Follow-up in the same pass**: `max_batch_size` and the leader-permit cap are now configurable (`CommandBatcher::with_max_batch_size`/`with_max_concurrent_leaders`, `SkiljBuilder::command_batch_max_size`/`command_batch_max_concurrent_leaders`); per-batch timing logs dropped from `info` to `debug`; new `docs/performance.md`; two more tests (small configured batch size drains fully; random client-disconnect churn never wedges the queue). The `skilj-helpdesk` load test was not re-run against this change - it is a separate ~16-minute multi-step harness.

**The `PoolTimedOut` test flake, root-caused.** `skilj/tests/command_trigger.rs` and `event_fetch_rest.rs` failed intermittently (a "pool starving under sandbox variance" hypothesis in earlier load-test reports). Real cause: every test leaks a live `Skilj` whose background tasks keep its default 10-connection pool open, so ~10 tests sharing one embedded Postgres exhaust its 100-connection limit and the last `build()` fails after the 30s acquire timeout - reproducible even with `--test-threads=1`, and it predates the batching work. Fixed test-side by giving those files' `Skilj::builder` a 4-connection pool; both files now pass in ~1s / ~3s. No library change.

## 64. Hardening `POST /v1/parked-deliveries` (§47 follow-up)

A review of §47's bridge-facing parking route found four things, all fixed:

1. **Cross-credential overwrite (security).** §56's unique index keyed one occurrence on `(source, kind, identifier)`. Bridge identifiers are guessable (`orders:0:7`), so a second `ExternalEventToken`/`CommandToken` in the same bounded context reporting that identifier hit the `ON CONFLICT ... DO UPDATE` and swapped its own `request_json` onto the first token's row. `retryParkedDelivery` redrives under the row's `access_token_id` - the *first* token - so the second caller got its payload submitted with someone else's authority. The key is now `(source, kind, identifier, COALESCE(access_token_id, ''))`. `COALESCE` rather than `NULLS NOT DISTINCT` (Postgres 15+ only) so `CrossContextRoute`'s always-`NULL` rows still dedupe against each other; a real token id is never empty.
2. **Migration.** `migrate_parked_deliveries_dedup_and_unique_index` now handles three starting states: no index (dedupe on the new key, create it), legacy index only (the legacy key is strictly stricter, so the same dedupe pass finds nothing; create the new index, drop the legacy one), and new index only (no-op). Test: `legacy_parked_delivery_index_migrates_to_the_token_scoped_one`.
3. **Runtime-added bounded contexts had no index at all.** The index was only ever created by `build()`'s startup migration loop, so a context added via `addBoundedContext` failed every `insert_parked_delivery` (Postgres refuses an `ON CONFLICT` target with no matching unique index) until restart. `provision_bounded_context_schema` now creates it. The two test files that used to call the migration by hand to paper over this no longer do, so they now prove the fix.
4. **Unchecked input, then a panic.** The route stored `request` as arbitrary JSON; `redrive_parked_delivery` then `.expect()`ed it decoded as the kind's redrive DTO. The route now validates `request` against the same DTO its kind's original route takes (`ExternalEventRequest`/`CommandTriggerRequest`), and the redrive returns the new `event_store::Error::InvalidParkedDeliveryRequest` (400 / `invalid_parked_delivery_request`) for a row stored before the check existed. Also, unlike every other REST route, nothing downstream of this one re-checks token status (`insert_parked_delivery` is a plain write, not a core rule), so a revoked token could keep writing rows; it is now refused with `TokenNotActive`.

Tests: `parked_delivery_reports_are_shape_checked_token_scoped_and_need_an_active_token` and `retrying_a_delivery_with_a_malformed_stored_request_errors_gracefully` in `skilj/tests/parked_deliveries_graphql.rs`.

## 65. Two concurrency bugs: JWKS cold-cache 401s and double-redriven parked deliveries

Found while writing a concurrent-retry test for §64's follow-up: the test's two parallel GraphQL requests failed *authentication* before they ever reached the code under test.

**1. `JwksCache::key_for` turned away concurrent misses.** On a cache miss it stamped `last_refetch` and then fetched. Any other miss arriving while that fetch was in flight saw a refetch "less than `min_refetch_interval` ago" and returned `None`, which is `unknown_signing_key`. Every cold start, and every IdP signing-key rotation (a new `kid`), therefore 401'd all requests that arrived while the JWKS fetch was in progress. The rate limit was meant to stop garbage `kid`s from forcing a refetch per request, not to reject valid tokens arriving during a legitimate one. The fix is single-flight: `last_refetch` is now a `tokio::sync::Mutex` held across the fetch, and the key cache is re-checked after acquiring it, so waiters get the leader's result. The rate limit still applies once no fetch is in flight. Test: `concurrent_cold_cache_lookups_all_succeed_off_one_refetch` (8 concurrent verifies against a JWKS endpoint that takes 200ms to respond: all succeed, exactly one fetch). It failed deterministically before the fix.

**2. `retryParkedDelivery` redrove a row once per concurrent caller.** The resolver read the row, redrove it, then deleted it, with nothing in between stopping a second caller doing the same. Reproduced deterministically: two concurrent retries of one `ExternalEvent`-kind row produced two events. Fixed in two layers, like §56:

- *Serialization.* `db::try_lock_parked_delivery_for_retry` takes a `pg_try_advisory_xact_lock` on `(bounded_context, id)` in a lock-only transaction held until the resolver returns, and the row is only read after that. Non-blocking rather than `pg_advisory_xact_lock`, because a blocked waiter would pin a pooled connection while the holder still needs connections for its own redrive (a burst of retries on a small pool could starve it), and for an operator action "already being retried" (`ParkedDelivery_retry_in_progress`) is the more useful answer anyway.
- *Idempotency on command redrives.* Previously both command kinds redrove with no idempotency key, so a redrive whose command committed but whose row delete failed would land again on the next retry. `db::parked_delivery_redrive_identity` now supplies the `(client_id, idempotency_key)`. For `CrossContextRoute` that is the route's own client id and the *exact* key its original attempt used (`cross_context_route_idempotency_key`, now shared with `catch_up_cross_context_route` so they can't drift), which also dedupes an original attempt that committed but reported an error to the route. For `CommandTrigger` it is the token's client id plus a new reserved per-row key prefix, `skilj-parked-delivery:`. `ExternalEvent` has no idempotency key; when the bridge's request carried no `dedupe` cursor, the redrive supplies one (`db::parked_delivery_redrive_dedupe_partition_key`: partition `skilj-parked-delivery:{id}` at sequence 1, under the row's own token), so a second redrive of the same row is `Redelivered`. This was closed in a follow-up; the first version of this section recorded it as an open gap. Test: `a_retry_after_a_failed_row_delete_does_not_land_the_event_twice`, which uses a `BEFORE DELETE` trigger to make the row delete fail after the redrive commits (2 events before the follow-up, 1 after). A second follow-up closed the remaining hole for `CommandTrigger`. All three bridges send `POST /v1/commands/trigger` with an `Idempotency-Key` *header* (Kafka: `{topic}:{partition}:{offset}`; AMQP/NATS: the message id), and the parked `request` is only the *body*. So the redrive never knew that key, and a bridge's original attempt that committed without the bridge hearing back (a lost response) was redriven into a second command. `POST /v1/parked-deliveries` now takes an optional top-level `idempotencyKey` (`command_trigger` only, reserved prefixes refused), stored in the row's `request` as `idempotencyKey`; the redrive runs under `(token client_id, that key)`, exactly the original submission's identity, and falls back to the per-row key only without one. Each bridge derives the header and the report field from one `inbound_idempotency_key` function, so they can't disagree. Test: `retrying_a_parked_trigger_whose_original_attempt_committed_is_deduplicated` (real REST trigger with a key commits, the same key is parked, the retry is deduplicated; 2 commands with the redrive ignoring the stored key, 1 with it) and `skilj-kafka`'s `a_parked_trigger_message_reports_the_idempotency_key_it_was_sent_with`. What remains uncovered: an `ExternalEvent` original attempt that committed without a `dedupe` cursor. Nothing identifies it, and a bridge that wants redelivery safety sends a cursor anyway (all three do when the broker provides a position).

Visible change: a redriven `CrossContextRoute` command's `client_id` is now `cross-context-route`, not `parked-delivery-retry`. It is the route's command either way, and the key only dedupes under the route's own client id.

Tests: `concurrent_retries_of_one_parked_delivery_redrive_it_once` (`skilj/tests/parked_deliveries_graphql.rs`, failed with 2 events before the fix, stable over 5 runs after), and the redrive in `a_persistently_failing_target_parks_instead_of_blocking_forever` now goes through `parked_delivery_redrive_identity` and asserts that a second redrive is `Deduplicated`.

## 66. Test harness: the embedded Postgres (and broker containers) never stopped

`/tmp` - a 7.7G RAM-backed tmpfs on the WSL dev machine - hit 100% during a session of three full `cargo test --workspace` runs, taking the Kafka broker tests down with admin-operation timeouts and making even shell output fail with `ENOSPC`. The cause: 46 test files each started their own `postgresql_embedded` server and kept it in a `static` `OnceCell`. Rust never drops statics, so `PostgreSQL`'s `Drop` (stop the server, delete the temp data dir) never ran. Every run left about one live Postgres per test binary plus its data dir behind: 128 orphaned servers (1,100+ processes) and 297 `/tmp/.tmp*` dirs by the time it was noticed. It had been happening since the first embedded-Postgres test. This is very likely what was behind this project's repeated mid-session WSL crashes: memory pressure from tmpfs plus hundreds of idle postmasters, not anything in skilj itself. The bridge crates' Kafka/Artemis/NATS testcontainers had the identical leak for the identical reason (a `ContainerAsync` in a `static`).

**Fix: a new, unpublished `skilj-test-support` crate.** `database_url(name)` returns `DATABASE_URL` when set; otherwise it returns a database on the process's one embedded server. `embedded_database_url(name)` always uses the embedded server, for `admin_context_bootstrap.rs`'s race test, which needs a database nothing has touched even when `DATABASE_URL` is set. On first start it spawns a watchdog `sh` in its own process group. The watchdog polls `kill -0` on the test process and, once that process is gone, runs `pg_ctl stop -m immediate` and deletes the data dir and the password file's own temp dir. That dir is only deleted when it really is a `.tmp*` dir under the temp dir: `postgresql_embedded` falls back to the current directory if it can't make one, and deleting that would delete the checkout. `remove_container_on_exit(id)` does the same for a container (`docker rm -f -v`). A watchdog process rather than an exit hook, because the exits that caused the pile-up (Ctrl-C, killed binaries, VM crashes) run no exit hooks. Its own process group so a terminal's Ctrl-C doesn't kill it along with the tests.

Each test file keeps its own connect/migrate logic (they differ: pool sizes, whether a `Pool` or a URL is kept, `connect_and_migrate` vs inline); only the ~20-line start-a-server block and the `_embedded` field went. 42 files converted mechanically by script, 4 by hand.

**Verified**: a SIGINT and a SIGKILL to a process holding an embedded database both leave no server and no temp dir within a few seconds, and a full `cargo test --workspace --no-fail-fast` leaves 0 Postgres processes, 0 `/tmp/.tmp*` dirs and 0 containers afterwards (before the fix it left ~40 servers, several GB of `/tmp` and 3 containers per run). 937 tests pass, no skipped Postgres tests.

`cargo publish` strips a dev-dependency with a `path` and no `version` (checked with `cargo package -p skilj-core`: the packaged manifest has no `skilj-test-support`), so none of the published crates gain a dependency on the unpublished one. The cost: tests in a downloaded `.crate` tarball no longer build on their own, which nothing here relies on.

## 67. CI's silent-skip guard never fired

`.woodpecker.yml` greps the test log for Postgres `skipping: ...` notes and fails the build if any appear (commits `67960be`/`d3281ab`), so a CI run can't go green without touching the database. It ran `cargo test --workspace` without `--nocapture`. The note is printed from inside a test that then returns early and *passes*, and libtest discards a passing test's captured output, so the note never reached the log. Reproduced by pointing `DATABASE_URL` at a dead port: all 59 `persistence.rs` tests skipped and "passed", `cargo test` exited 0, and the CI grep matched nothing. The earlier verification used a hand-inserted log line, which can't show a capture problem.

Fix: CI runs `cargo test --workspace -- --nocapture`, and the grep drops its `^` anchor, since parallel tests can interleave a note mid-line after another test's `test ... ` prefix. Checked both ways: the dead-port reproduction now matches, and a healthy full run (119 KB of output) matches nothing. The same capture effect had hidden the Docker-down broker skips locally too. They show up under `--nocapture` (`skilj-kafka`/`-amqp`/`-nats` tests "passing" in 0.01s is the other tell).

## 68. Colliding or invalid projection type names in `ProjectionQuery`

Each registered projection gets a GraphQL type named `{bounded_context}_{projection}` (`projection_types::graphql_type_name`), with nested shapes and enums named `{parent}_{field}`. Two problems, both silent:

- **Collisions.** Bounded context names may contain `_`, so `a_b` + `c` and `a` + `b_c` both produce `a_b_c`; a nested `{parent}_{field}` can also equal another projection's top-level name. `async_graphql`'s dynamic `SchemaBuilder::register` keeps only the *last* type per name, with no error (checked directly), and the `projection` resolver tags every result with `graphql_type_name(bc, name)`. So both projections were served, one of them rendered through the other's type: wrong or missing fields, no error. No access-control impact, since each query still checks its own bounded context first.
- **Invalid names.** Projection names and state field keys aren't validated as GraphQL names. `finish()` accepts `order-summary` or a serde-renamed `total-amount` field without complaint, but the published schema is then not valid GraphQL, which breaks clients that parse it (codegen, GraphiQL, fragments). Separately, `total_amount` and `totalAmount` both become field `totalAmount` and overwrite each other the same silent way.

Fix, in line with how this module already treats an unparseable projection schema (exclude it and log why, rather than fail the whole schema): `build` claims each projection's generated names all-or-nothing (`admit_type_names`). A projection whose names are invalid or already taken is excluded with a `tracing::warn!`. A state field without a valid, unique GraphQL name is left out (before any nested type gets named after it). The admitted `(bounded_context, projection)` pairs go into the schema as `AdmittedProjections` data, and `projection` / `projectionUpdates` refuse anything outside it with `projection_not_in_schema`. That set is keyed by the pair, not the type name, because a colliding pair shares the name. This module's two `eprintln!` warnings became `tracing::warn!`, matching the crate's "emit via `tracing` only" rule stated in `schema.rs`.

Tests: `colliding_projection_type_names_serve_one_and_refuse_the_other` (`skilj/tests/projection_query.rs`; both served before, exactly one after, and the shared type carries only the served projection's field) plus unit tests for the name grammar, all-or-nothing admission and field skipping.


## 69. Bounded event reads (`config.max_events_per_read`)

`GET /v1/events`, `GET /v1/events/consume` and GraphQL `queryEvents` each served *every* matching event after their cursor, and loaded the whole remaining history into memory first to find them. A new consumer starting from the beginning of a long history, or one `GET /v1/events` without `after`, made the server load and serialize all of it in one response - enough to exhaust memory on either end.

**Spec.** `specs/skilj.allium` leaves paging to the wire contract, but *what* a rule serves is spec behaviour, so it now says so: a new `config.max_events_per_read` (default 1000) and a black-box `first_by_sequence(events, n)`. `FetchEvents`, `ConsumeEvents` and `QueryEvents` serve `first_by_sequence(<what they served before>, config.max_events_per_read)`. That keeps every rule's continuation exact: `ConsumeEvents`' auto-advance already moved the cursor to `highest_sequence(served)`, and a manual-ack caller acks the last event it handled, so a prefix is never skipped. The cap counts *served* events, not scanned candidates. A cap on candidates would leave a filtered consumer whose next N candidates all miss its filter with an empty page and an unmoved cursor forever. `CountEvents` stays unbounded - it is an aggregate, not a page. `allium check`/`analyse` unchanged (0 findings; 22 diagnostics, 5 findings as before).

**Implementation.**
- Pure functions keep their signatures (`fetch_events`, `consume_events`, `query_events` apply `DEFAULT_MAX_EVENTS_PER_READ`); `*_page` variants take the cap explicitly. `consume_events`' new-cursor seed moved into `initial_consume_position(token, history)`, a maximum, so it can be computed chunk by chunk; `consume_events_page` takes the resolved seed. `query_events_select` separates choosing a page from rendering it, since `skilj-graphql` resolves decryption keys asynchronously for just the served events.
- `db::for_each_event_chunk` walks a bounded context's history (optionally one event type) in chunks, cache-first per chunk with a `LIMIT`ed Postgres fallback (`list_events_from_limited`, `list_events_for_bounded_context_from_limited`). `db::collect_event_page` builds one page on it, calling the rule's own pure function per chunk, so the page is exactly the first N it would have served over the whole history. The chunk size is the page size, so memory stays at roughly one chunk plus one page, tuned by the one setting.
- `SkiljBuilder::max_events_per_read` (clamped to at least 1) reaches the REST state and `GraphqlState`. `skilj_rest::router` gained the parameter.
- `skilj-tui`'s query view shows how many events it has and loads the next page with `n`; it used to show one unbounded result. The Kafka/AMQP/NATS/Temporal bridges already poll consume in a loop and need no change. `skilj-inspector` reads Postgres directly and is unaffected.
- Not bounded: a tag-filtered `queryEvents` still loads its tag-index matches whole before paging them - already narrowed by the index, and the page it returns is capped.

**Breaking** for a `GET /v1/events` or `queryEvents` client that assumed one call returns everything: it must page (`after`=`nextCursor` / `afterSequence` = the last `sequence`) until a call returns nothing.

Tests: `reads_serve_bounded_pages_and_continue_where_they_stopped` (`skilj/tests/event_fetch_rest.rs`: page size 3 and a two-event cache window, so chunks come from both Postgres and the cache; `GET /v1/events` pages 7 events as 3+3+1 via `nextCursor`, a filtered page `[1,3,5]` fills across chunks, consume pages 3+3+1+0, and a `Latest` consumer minted after seven events seeds past all of them. Making the chunk walk stop after one chunk turns the filtered page into `[1,3]`) and `query_events_serves_bounded_pages_over_graphql` (`skilj/tests/graphql_business_surfaces.rs`, the bounded-context-wide path).


## 70. Bounded, pageable `fetchCommands`

The command-side counterpart to §69, and the worse case of the two. `fetchCommands` loaded the bounded context's entire command history (`list_commands_for_bounded_context`, with no `ORDER BY`, so in whatever order Postgres returned rows), then made a database round trip per matching command to resolve decryption keys, then rendered all of them. It returned bare payload strings with no id or timestamp, so a client couldn't have paged even if it wanted to. §8's earlier decision to keep it a flat `[String!]!` was made when `Command` had no exposed id; that no longer holds.

**Spec.** `FetchCommands` (and the `CommandQuery` surface) gains an optional `after_command`, with the same bounded-context requirement `triggered_event` already has, and serves `first_by_recording(<candidates recorded after after_command>, config.max_events_per_read)`. The two new black boxes are `recorded_after(a, b)` and `first_by_recording(commands, n)`. `Command` has no sequence, but this library records commands in an order, and that is what pages follow. They don't follow `created_at`: two commands can share a timestamp, and a page boundary between them would skip or repeat one. The cap is the same `config.max_events_per_read` - despite its name it covers commands too, noted in the spec, rather than renaming a setting just added. `allium check`/`analyse` unchanged.

**Wire (breaking).** `fetchCommands(..., afterCommandId: String)` returns `[QueriedCommand!]!` `{ id, createdAt, payload }` instead of `[String!]!`. `id` is the command's own `Command.id`, and passing it back as `afterCommandId` gets the next page; there is no separate opaque cursor. An unknown `afterCommandId` is `Command_not_found` rather than a silent restart from the beginning. Within the workspace only tests called it.

**Implementation.** `db::command_recording_position` maps a command id to its `commands.id` key (a `BIGSERIAL`, so it only grows with insertion). `db::collect_command_page` walks commands after that position `ORDER BY id LIMIT <page size>`, a chunk at a time, calling the pure `event_store::fetch_commands_select` per chunk - the rule's filters and cap, now separate from rendering, the same split `query_events_select` got. Recording order isn't part of the domain `Command`, so the loader applies `recorded_after` and the ordering, and the pure function checks `after_command`'s bounded context (new `AfterCommandNotInBoundedContext`). Decryption keys are resolved for the served page only. `fetch_commands` keeps its signature (rendered strings, default cap) for existing callers.

Test: `fetch_commands_serves_bounded_pages_over_graphql` (page size 3: seven commands come back 3+3+1 in recording order via `afterCommandId`, each with `createdAt`; an unknown id is `Command_not_found`). The two existing `fetchCommands` assertions in the same file moved to the object shape.

## 71. No request or background path loads a whole history any more

A sweep for the remaining callers of the full-history loaders after §69/§70 found five, none of them a paged read:

- **`countEvents`** loaded the whole bounded context to count it. The spec keeps `CountEvents` an unbounded aggregate, so only the loading changed: the rule's pure `count_events` runs per chunk (`db::for_each_event_chunk`) and the results are summed; a count is additive over disjoint chunks. The tag-filtered path still uses the tag index, as `queryEvents` does.
- **Both event subscriptions** (`allEvents`/`eventsByType`) loaded the full history from Postgres - uncached, on every subscribe and every reconnect - only to take its maximum sequence for the default starting point. Now `from_sequence ?? MAX(sequence)` via the existing `db::latest_sequence`, still after the broadcaster subscription so drift-audit finding #7's ordering holds.
- **`register_projection`'s history** (the `registerProjection` mutation, *every projection's reconciliation on every startup*, and template resync) was loaded whole for one question: does any event of a consumed type exist yet (does a new sync projection need a history fold)? `db::witness_events_of_types` answers it with at most one event per consumed type (`LIMIT 1`), which the pure function treats identically.
- **Catch-up ticks** for `CrossContextRoute`, `ScheduleDeadline` and `CancelDeadline` loaded every source event after their cursor per tick. A new one starting from the beginning of a long history would load all of it. Each tick now loads at most `MAX_EVENTS_PER_CATCH_UP_TICK` (1000). The cursor persists per processed event, so the next tick continues exactly where this one stopped. A new cursor's `Latest`/`AtTime` seed is one `MAX(sequence) ... WHERE event_type_name = $1 [AND metadata_created_at <= $2]` (`highest_sequence_of_type`), exact here because these seeds have no owner scope, unlike consume's.
- **`fold_history_into_new_sync_projection`** reads history in chunks of the same size; each event still commits in its own transaction.
- **Follow-up: async projection/rebuild and snapshot catch-up** (`catch_up_bounded_context`, `catch_up_snapshots`) loaded every event after the slowest projection's/snapshot's position, so a new async projection or a rebuild over a long history loaded all of it in one tick. Both now use the same `MAX_EVENTS_PER_CATCH_UP_TICK` cap - `caught_up_to` and snapshot progress persist per event, and a rebuild is only promoted once a re-read shows it at `latest`, so it simply promotes on the tick that finishes. The partitioned paths were already capped (`MAX_EVENTS_PER_PARTITION_TICK`). Test: `a_cold_start_catch_up_over_a_long_history_spans_ticks` (1003 events: tick one folds 1000, tick two the rest, final state identical).

Tests: `a_catch_up_tick_is_bounded_and_the_next_one_continues` (1003 source events against a 1000-event cache window: tick one routes exactly 1000 via the `LIMIT`ed Postgres path, tick two the last 3 from the cache, tick three nothing), and `query_events_serves_bounded_pages_over_graphql` now also checks `countEvents` = 7 across three 3-event chunks. The existing route/deadline start-position, subscription, templating, reconciliation and sync-projection suites all pass unchanged.

## 72. Per-request bounds on `/graphql`

§69-§71 bounded what one *field* reads, but a GraphQL request could still route around them, and `/graphql` had no limits at all:

- **Body size, before authentication.** `async_graphql_axum::GraphQLRequest` reads the body as a raw stream - axum's default 2 MB body limit only applies to extractors that ask for it - and it runs before `graphql_handler`'s auth check. Its multipart path used `MultipartOptions::default()`, with no file-size or file-count limit, spilling parts to temp files (and this schema has no `Upload` type at all). Reproduced: an unauthenticated 3 MiB request was read whole and executed (HTTP 200).
- **Alias amplification.** `{ a1: queryEvents(...) a2: queryEvents(...) ... }` gives each alias its own `max_events_per_read` page, so one request multiplied the cap by the number of aliases; `countEvents` aliases each scan the whole history.
- **Depth and size.** No `limit_depth`/`limit_complexity`.

Fix, all configurable via `SkiljBuilder::graphql_limits(GraphqlLimits { .. })` (re-exported from `skilj`):
- `max_request_body_bytes` (default 2 MiB): a `POST /graphql` middleware reads at most that many bytes with `axum::body::to_bytes(body, max)` and answers `413` beyond it. Reading with a cap, rather than trusting `Content-Length`, also covers a missing or false header and chunked bodies. The extractor buffered the whole body anyway, so this adds no copy that wasn't there. The websocket upgrade (`GET`) is untouched.
- `max_depth` (24) and `max_complexity` (2000): `async_graphql`'s own validation limits. The dynamic schema API can't weight individual fields - every field counts 1 - which is why the next item exists.
- `max_expensive_fields` (10): an `async_graphql` extension on `parse_query` counts selections of `queryEvents`/`countEvents`/`fetchCommands`/`projection` per operation - aliases, inline fragments and each fragment spread included - and refuses the request with `query_too_expensive` beyond it. Only one operation executes, so the most expensive one decides. It runs before validation, which is what would reject a fragment cycle, so the walk doesn't follow one.

The defaults fit the standard full introspection query (graphql-js `getIntrospectionQuery`: depth about 14) that GraphiQL and codegen tools send; the test runs it.

Tests: `graphql_requests_are_bounded_in_size_depth_and_expensive_fields` (unauthenticated 3 MiB body gets 413, was 200 before; 20 `countEvents` aliases gets `query_too_expensive`; a 30-level-deep query is refused; full introspection passes), plus unit tests for alias/fragment counting, the most-expensive-operation rule, and a fragment cycle. Adding the tenth test to `skilj/tests/graphql_business_surfaces.rs` tipped it into §63's `PoolTimedOut` leak (every test leaks a `Skilj` with a 10-connection pool), so that file's setup now uses the same 4-connection pool its siblings do.

## 73. The websocket endpoint shares the body cap

§72's body cap only covered `POST /graphql`. On the websocket (`GET /graphql` upgrade, subscriptions) every message is a GraphQL document too, and the connection accepts messages before - or without - a credential: a `connection_init` with no bearer resolves to no caller, which is allowed, since a subscription may need none. The upgrade used axum's defaults, 64 MiB per message and 16 MiB per frame. Reproduced: an unauthenticated connection sending a 3 MiB `subscribe` message had it parsed and executed (`next` with data). `graphql_ws_handler` now sets `max_message_size`/`max_frame_size` to `GraphqlLimits::max_request_body_bytes`, so one setting caps both transports; beyond it the connection is closed. Depth, complexity and the expensive-field budget already applied to subscription documents (same schema). Test: `an_oversized_websocket_message_closes_the_connection`.

How many subscriptions one authenticated connection may hold at once was left unbounded here; §74 closes that.

## 74. Subscriptions per websocket connection

Each subscription holds its own event-broadcast receiver and filters every committed event, so one connection opening thousands of them multiplied per-event work with no bound. No wrapping of `async_graphql`'s protocol handling was needed: `connection_init` inserts a per-connection `ConnectionSubscriptions` counter into the connection's data, and each subscription resolver (`allEvents`, `eventsByType`, `projectionUpdates`) claims a slot after its authorization checks - so a refused unauthenticated attempt never takes one - refusing with `too_many_subscriptions` once `GraphqlLimits::max_subscriptions_per_connection` (default 100) are running. The claim is a `SubscriptionSlot` guard owned by the subscription's stream, released on drop, so it goes however the stream ends: a client `complete`, the stream finishing, or the connection closing. Outside a websocket connection (a schema executed directly) there's no counter and no bound.

Wire note: `async_graphql` reports this resolver error as a `next` message carrying `errors`, followed by `complete` - not a protocol-level `error` message - the same as any other subscription resolver error.

Test: `a_connection_holds_at_most_max_subscriptions_at_once` (limit 2: two run, a third is refused, completing one lets a fourth start). With the guard's release removed, the fourth is refused and the test fails.

## 75. Acknowledgements serialized with consume

`POST /v1/events/consume/ack` read the token's `ReadCursor`, checked `rule AcknowledgeEvents`' `sequence >= cursor.sequence` in Rust, and wrote the new position - three separate autocommit steps, outside the per-token advisory lock `GET /v1/events/consume` takes (§53). Two races followed:

- **Two acks** could both pass against the same old cursor, and the lower one could land last, moving the cursor *backwards* - violating the rule's own requirement and redelivering the events in between. Reproduced deterministically: a `BEFORE UPDATE` trigger stalls the ack of 10 inside its write (Postgres takes the row lock before firing a `BEFORE` row trigger, so it holds the row). The ack of 5 meanwhile reads the still-uncommitted cursor (-1), passes, queues on the row, and then overwrites 10 with 5 - answered `200`. A first version of the test stalled the *lower* ack instead and passed without the fix, for exactly that row-lock reason, before being turned around.
- **An ack interleaving with a consume** could have the consume's claim (`checked_out_at`) written after the ack had cleared it, leaving a lease nobody holds and the next consumer served nothing until it expired.

Fix: the ack route runs in one transaction under `lock_read_cursor_for_consume`, with the cursor read inside it, exactly as consume does; `db::record_acknowledgement` now takes any executor. Test: `concurrent_acknowledgements_never_move_the_cursor_backwards` (the ack of 5 is now refused with `409 AcknowledgementRegresses` and the cursor stays at 10).

Manual-ack clients now acknowledge once per page (§69), which makes this path busier than it was.
