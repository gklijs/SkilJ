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

**Decided: no macros for now.** Ship the plain trait shape first,
hand-written. A derive/attribute macro to cut per-type boilerplate
(`#[skilj::command_type]` or similar) is worth revisiting once real usage
shows what's actually tedious — designing it now, before any type has
been written by hand, risks locking in the wrong ergonomics.

### 1.3.1 The one exception: `#[requires_role(...)]`

A `CommandType` can declare an extra, caller-facing role-name gate on top
of the ordinary write-level `RoleAccessMapping` check — some commands
need to be restricted to a specific role beyond "anyone with write access
to this bounded context." The user asked for this to read as an
annotation on the command's own declaration, not a trait method its
author has to remember to override, so — deliberately, as the one named
exception to §1.3's "no macros for now" — it's a real `#[proc_macro_attribute]`,
`skilj-macros::requires_role`, applied directly above the `impl
CommandType for ...` block:

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
    /// payload type - reachable today, since nothing yet validates a
    /// payload against `EventType.schema` at write time.
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
**No spec change was needed** - `SensitiveFieldsStayProtected`'s existing
text never described a mechanism, only an outcome, and this delivers
that outcome for real, exactly as already written. Crypto-shredding
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
