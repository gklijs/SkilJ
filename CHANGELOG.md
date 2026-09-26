# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(pre-1.0: breaking changes may land in minor/patch versions until 1.0).

## [Unreleased]

### Changed

- `skilj-amqp`: `fe2o3-amqp`/`fe2o3-amqp-types` 0.17 -> 0.18 (upstream now
  ships both; verified against the real Artemis broker tests).
- `skilj-core`: `jsonschema` 0.56 -> 0.58 (checked the two intervening
  releases' changelogs against this crate's own narrow usage -
  `validator_for`/`is_valid` only, `default-features = false` - none of
  the breaking changes apply; picks up upstream fixes for a `oneOf`
  canonicalization bug and two numeric-keyword panics).

### Fixed

- Reading a bounded context that a concurrent `DeleteBoundedContext` had
  just removed panicked (seven `.expect()` sites in `skilj-core::db`)
  instead of returning an error; it now returns `RowNotFound` like any
  other database error.
- Follow-up sweep of the same panic shape one level deeper: reading an
  `EventType`/`CommandType`/`Command` row that a concurrent
  `DeleteBoundedContext` had just taken out from under an in-flight
  `list_events`/`list_events_from`/access-token/command-conversion call
  also panicked instead of erroring (11 more sites, via new
  `require_event_type`/`require_command_type`/`require_command`
  helpers). Two lookalike sites were checked and deliberately left as
  panics: their own batch query already fails loudly on a dropped schema
  before ever reaching the lookup, so a miss there is a genuine data
  inconsistency, not this race.
- Same shape again, in `skilj-graphql`'s `retryParkedDelivery`: a
  `CrossContextRoute`-kind `ParkedDelivery`'s own `target_bounded_context`
  can be hard-deleted independently of (and long after) the context the
  delivery is parked and retried in, and the `ExternalEvent`/
  `CommandTrigger` token lookups have the same gap for their own,
  narrower window. All three used to panic on a stale reference; retrying
  a stranded delivery now fails gracefully and leaves the row parked,
  like any other failed redrive. New `skilj_core::error::Error::
  row_not_found()` gives `skilj-graphql` the same error shape `db`'s own
  `require_*` helpers use, without a new `sqlx` dependency just for it.
- Same shape a third time, in four `skilj-graphql` mutations/queries that
  each write or read a `BoundedContext` and then immediately reload it
  with a nested `.expect("...it must still be there")`:
  `archiveBoundedContext`, `deleteBoundedContext`,
  `resyncBoundedContextFromTemplate`, and - the widest window of the
  four, an admin "list everything" query looping over every listed
  context doing real per-item work - `boundedContexts`. All four can
  have a concurrent `deleteBoundedContext` land in the gap; all four now
  return an ordinary `BoundedContext_not_found` GraphQL error instead of
  panicking. A fifth, identical-looking site in `addBoundedContext` was
  checked and left alone: `deleteBoundedContext` requires `Archived`,
  and a context this call just inserted is `Active`, so that one
  genuinely cannot race - documented inline instead of "fixed" into a
  false positive.
- Same shape a fourth time, in `skilj-rest`'s `resolve_token` - the
  highest-traffic version found yet, since it runs on *every*
  authenticated REST request, not just an occasional admin action.
  `access_token_kind` and the matching `TokenLookup::get` each
  independently re-resolve a bearer token's own bounded context; a
  concurrent `DeleteBoundedContext` landing between the two used to
  panic with "a real bug" instead of the ordinary `401
  UnrecognisedCredential` a caller whose token has genuinely vanished
  should see either way.

### Security

- Lockfile bump of `rustls` 0.23.44 -> 0.23.45 for RUSTSEC-2026-0285
  (TLS 1.3 handshake messages accepted across encryption level
  boundaries). Reached only through optional/dev TLS paths (OTLP export
  over `reqwest`, `fe2o3-amqp`, testcontainers' `bollard`); no source
  change needed.

## [0.0.8] - 2026-09-19

### Added

- Group-commit command batching: concurrent commands to the same bounded
  context now share one lock acquisition and one commit, raising measured
  single-bounded-context throughput from ~27/s to ~45-60/s in the
  `skilj-helpdesk` load test. Includes several rounds of round-trip cuts
  inside the held lock (Codeberg issues #32, #35, #36). See
  [docs/architecture.md §58-§62](docs/architecture.md) and the new
  [docs/performance.md](docs/performance.md).
- New builder options `command_batch_max_size` (default 256) and
  `command_batch_max_concurrent_leaders` (default half the pool), plus
  `CommandBatcher::with_max_batch_size` / `with_max_concurrent_leaders` and
  `command_batcher::DEFAULT_MAX_BATCH_SIZE`.
- `command_batch_idle_in_transaction_timeout` builder option: a backstop
  that lets Postgres kill a stuck batch leader and release the
  bounded-context lock.

### Changed

- **Breaking (pre-1.0):** `Error::BatchFailed` is now
  `BatchFailed { code, message }` instead of `BatchFailed(String)`. Its
  `code()` reports the original error's code rather than a fixed
  `database_error`.
- Per-batch timing log lines are now `debug` instead of `info`.

### Fixed

- A batch leader whose request future was dropped (client disconnect,
  timeout) could leave later commands to that bounded context waiting
  forever; stranded followers now get a retryable `BatchFailed` and the
  next request becomes leader.
- More than the max batch size queued commands could leave the remainder
  unprocessed forever; the leader now keeps draining until the queue is
  empty.
- Batch leaders are capped so they cannot exhaust the connection pool
  waiting on each other.

## [0.0.7] - 2026-09-17

### Added

- `Metadata` on commands and events gains `correlation_id`/`causation_id`:
  a durable, queryable thread from a submitted command through every
  event and cross-context command it triggers, distinct from the
  ephemeral OTel trace id already carried on REST/GraphQL error
  responses. Every stored `Command` ends up with a `correlation_id`
  (generated when the caller omits one); `causation_id` is `null` for a
  root command and set to the causing event's own stable name otherwise.
  See [docs/architecture.md §44](docs/architecture.md#correlation-causation-ids)
  (Codeberg issue #18).
- New `skilj-test-fixture` crate: a given/when/then harness for
  exercising a plugin's own `decide()`/`project()` directly, in memory -
  no Postgres, no license required for a full HTTP round trip just to
  check whether a decision function returns the right event. See
  [docs/architecture.md §45](docs/architecture.md#given-when-then-test-fixture)
  (Codeberg issue #19).
- Native one-shot, per-entity deadlines: `plugin::ScheduleDeadline`/
  `CancelDeadline` schedule a deferred command tied to a specific
  entity's own tags - "cancel this order if it has no time to die within
  30 minutes without payment" - and cancel it if the awaited event
  happens first, without reaching for the much heavier `skilj-temporal`
  for a single deferred command. See
  [docs/architecture.md §46](docs/architecture.md#native-deadlines)
  (Codeberg issue #20).
- Dead-letter/parking for failed event and message handler delivery:
  `CrossContextRoute`'s catch-up loop and the `skilj-kafka`/`skilj-amqp`/
  `skilj-nats` bridges now share one backoff/attempt-cap policy (new
  `skilj-retry` crate) instead of retrying a poison delivery forever on
  every poll tick, blocking every occurrence behind it. Past the attempt
  cap, the occurrence is parked to an operator-visible surface instead
  of looping - a failing delivery finally gets to live and let die,
  rather than take the whole route down with it. See
  [docs/architecture.md §47](docs/architecture.md#parked-deliveries)
  (Codeberg issue #21).
- New GraphQL subscription `projectionUpdates`: the subscription
  counterpart to `ProjectionQuery`, pushing the already-computed
  `Projection` state itself so a client wanting "push me the current
  balance whenever it changes" no longer has to subscribe to raw events
  and reimplement `project()`'s own fold client-side. See
  [docs/architecture.md §49](docs/architecture.md#49-a-graphql-subscription-for-projections-projectionupdates-codeberg-issue-23)
  (Codeberg issue #23).
- Segmented/parallel event processing: async `Projection` and `Snapshot`
  catch-up can now be spread across a fleet of instances, each claiming
  and processing its own segment via `pg_advisory_xact_lock`, for
  workloads where one instance redundantly redoing the same idempotent
  work isn't enough - the world, as it turns out, is not always enough.
  The `skilj-kafka`/`skilj-amqp`/`skilj-nats` bridges gain the same
  per-partition split, on top of a newly-closed double-publish gap
  (`ReadCursor.checked_out_at`) where two concurrent instances of the
  same mapping could otherwise both be served, and both act on, the
  identical unacknowledged batch. See
  [docs/architecture.md §50](docs/architecture.md#50-investigation-segmentedparallel-event-processing-for-horizontal-scale-out-codeberg-issue-25)
  through
  [§54](docs/architecture.md#54-bridge-partitioning-skilj-kafkaskilj-amqpskilj-nats-on-top-of-53)
  (Codeberg issue #25).

### Fixed

- A real cross-instance double-fold bug in async `Projection` catch-up,
  found while checking issue #25's "is the redundant work actually
  idempotent?" premise directly instead of trusting the existing design
  notes: two instances racing the same fold could each apply an
  overlapping batch of events, double-counting some of them. See
  [docs/architecture.md §50](docs/architecture.md#50-investigation-segmentedparallel-event-processing-for-horizontal-scale-out-codeberg-issue-25).
- A fire-vs-cancel race in `CancelDeadline` (§46): `fire_due_deadlines`
  could still exercise a deadline's license to fire its command after a
  concurrent `catch_up_cancel_deadline` tick had already revoked it,
  because the row was only marked `'fired'` after the side effect ran.
  See [docs/architecture.md §55](docs/architecture.md#55-closing-the-canceldeadline-fire-vs-cancel-race).
- A duplicate parked-delivery race in `catch_up_cross_context_route`
  (§47): two instances polling the same route concurrently could both
  read the same near-exhausted retry state and both park a row for the
  identical failure - not quite diamonds are forever, but two rows for
  one real failure was one too many. Retry-state reads and writes are
  now lock-guarded and atomic. See
  [docs/architecture.md §56](docs/architecture.md#56-closing-the-parked-delivery-duplication-race-in-catch_up_cross_context_route).

## [0.0.6] - 2026-09-13

### Added

- `EventReadToken` and `CrossContextRoute` no longer unconditionally start
  at the very beginning of history. `createEventReadToken` gains an
  optional `startFrom` (`BEGINNING` default, unchanged; `LATEST` skips
  straight past current history so only events from then on are delivered),
  plus `startAtSequence`/`startAtTime` for replaying from a chosen
  mid-history point - exactly one of the four may be given. The Rust-only
  `plugin::CrossContextRouteStartFrom` trait const gives a route the same
  four choices (`Beginning`/`Latest`/`AtSequence`/`AtTime`), seeded on its
  first-ever catch-up tick. Wiring a new side effect (e.g.
  `UserRegistered` -> send a welcome email, or a new `EventTypeMapping`
  onto a Temporal workflow) against an event type with existing history no
  longer replays every historical occurrence through it by default. See
  [docs/architecture.md §43](docs/architecture.md#43-stopping-a-new-subscriber-from-replaying-all-of-history).

### Changed

- `docs/architecture.md`'s top-level sections now carry stable,
  number-independent anchors instead of GitHub's auto-generated
  number-dependent slugs, so a `§NN` citation elsewhere in the repo no
  longer breaks the next time a section is added, removed, or
  renumbered. Every existing `§N`/`§N.M` citation - in `docs/architecture.md`
  itself, other `.md` files, and Rust `///`/`//!` doc comments - now links
  against the new anchors. New `scripts/check-section-refs.sh`, wired
  into CI, scans every tracked file for `§N` citations and fails the
  build if a top-level number no longer resolves to a current heading.
  No behavior change.

## [0.0.5] - 2026-09-08

### Added

- Optional `dedupe: { partitionKey, sequence }` on `POST /v1/events/external`
  (`ExternalEventIngestion`): lets an adapter for an at-least-once
  message source (Kafka, Kinesis, Pulsar, and similar partitioned-log
  systems) submit a message's own partition and sequence number so a
  redelivery after a crash-before-commit creates no second event,
  instead of the caller needing to build this themselves. A compact
  per-`(adapter, partition)` watermark, not a row per message - sound
  because these sources already guarantee strictly increasing delivery
  order within one partition. Response gains `redelivered: bool`;
  `sequence` is `null` on a redelivery. Omitting the pair changes
  nothing. See [docs/architecture.md §39](docs/architecture.md#external-message-dedup-create-external-event).
- New `skilj-kafka` crate: a bridge between skilj's own event stream and
  Kafka, both directions. Outbound, a mapped `EventType` is produced to
  a Kafka topic (message key derived from a DCB tag) via the same
  `GET /v1/events/consume`/`POST /v1/events/consume/ack` mechanism
  `skilj-temporal` already uses. Inbound, a Kafka topic maps to either
  `ExternalEventIngestion` (redelivery-safe via the `dedupe` mechanism
  above) or `CommandTrigger` (redelivery-safe via `Idempotency-Key`),
  a per-mapping choice - both derive their own key from the message's
  own `"{topic}:{partition}"`/offset. See [docs/architecture.md §40](docs/architecture.md#skilj-kafka-bridge).
- New `skilj-amqp` crate: `skilj-kafka`'s own sibling for any AMQP 1.0
  broker (Solace PubSub+, Azure Service Bus, ActiveMQ Artemis) via
  `fe2o3-amqp` (pure Rust). Outbound, a mapped `EventType` is sent to an
  AMQP address with a DCB tag as `group-id` and the event's own sequence
  as `group-sequence`. Inbound, an address maps to `ExternalEventIngestion`
  or `CommandTrigger`; both use the message's own `group-id`/
  `group-sequence` or `message-id` properties for redelivery safety when
  the sender populated them (optional in AMQP 1.0, unlike Kafka's own
  broker-guaranteed offsets - omitted gracefully when absent, never an
  error). See [docs/architecture.md §41](docs/architecture.md#skilj-amqp-bridge).
- New `skilj-nats` crate: a third bridge sibling, for NATS JetStream via
  `async-nats`. Outbound, a mapped `EventType` is published to a subject
  with a DCB tag as a `Skilj-Correlation-Key` header and
  `"{bounded_context}:{sequence}"` as `Nats-Msg-Id` (getting JetStream's
  own native, server-side idempotent-publish deduplication for free).
  Inbound, a subject maps to `ExternalEventIngestion` (always
  redelivery-safe - JetStream's own `(stream, stream_sequence)` is
  broker-assigned on every message, never optional) or `CommandTrigger`
  (via `Nats-Msg-Id` when an upstream sender populated one). See
  [docs/architecture.md §42](docs/architecture.md#skilj-nats-bridge).

## [0.0.4] - 2026-09-04

### Security

- `ProjectionQuery` had no whole-projection access gate: a projection
  could declare a `team`-kind private field on the events/commands it
  was built from, but nothing stopped a Role of any name from reading
  the projection itself. New `Projection.TEAM_ONLY` (deployed
  configuration only, not a spec field) rejects a query unless the
  caller's Role carries the required name, composing with the existing
  owner-scope check rather than replacing it (Codeberg issue #17).
- The `projectionSchema` GraphQL field was missed by the fix above: a
  Role off the required team could still read a `TEAM_ONLY` projection's
  declared name/schema/schemaVersion through it even though `projection`
  itself correctly refused the same projection's data. Now gated
  identically to `projection`.
- `CrossContextRoute`'s own background task (added below) derived its
  idempotency key as `"{route.name}:{sequence}"` into the same shared
  `idempotency_keys` table `submitCommand`/command triggering write
  into - which has no caller/client_id column at all. A Write-level
  caller who knew or guessed a route's name and an upcoming sequence
  number could pre-plant that exact key, causing the route's real
  delivery to be silently deduplicated away with no error. Caught and
  fixed before this release: a reserved `skilj-cross-context-route:`
  key prefix for the route's own internal use, rejecting any
  caller-supplied idempotency key that uses it at both the REST trigger
  and `submitCommand` GraphQL wire boundaries. See docs/architecture.md
  [§36](docs/architecture.md#cross-context-route).
- Investigating the fix above surfaced a second, more serious,
  **already-shipped** issue in `idempotency_keys` (live since 0.0.2, not
  new to this release): its key was `(command_type_name,
  idempotency_key)` only, not scoped per caller at all. In a bounded
  context with multiple tenants (owner-tag scoping,
  `RoleAccessMapping`/`CommandToken` `scope`) submitting the same
  `CommandType`, two unrelated tenants choosing the same idempotency-key
  string - plausible with business-derived keys (an order id, an
  invoice number), no attacker needed - would silently collide: the
  second tenant's real command never ran, and they received sequence
  numbers belonging to the first tenant's own event stream. Fixed by
  scoping `idempotency_keys`' key to `(command_type_name, client_id,
  idempotency_key)`, migrated onto every already-provisioned bounded
  context (self-hosted deployments on 0.0.2/0.0.3 should upgrade).
  **Breaking, deliberately, for one edge case**: a genuine retry of a
  request submitted just before this migration runs will no longer be
  recognised as a duplicate (pre-migration rows are retired, not
  reused) - accepted in exchange for fully closing the collision above
  rather than leaving any part of it open. See [docs/architecture.md §37](docs/architecture.md#idempotency-keys-client-id-scoping).

### Added

- `plugin::CrossContextRoute`: a single-hop, stateless reaction - when a
  registered `EventType` commits in its own bounded context, submit a
  `CommandType`'s own payload into a *different* bounded context, going
  through that command's real `decide()`/`submit_command` path like any
  other caller. The answer to "cross bounded contexts without an
  external system like Temporal" for the common single-hop case;
  deliberately not a Saga/process manager - a route that needs more than
  one hop should reach for the `skilj-temporal` pairing instead. Driven
  by a new shared background task (`SkiljBuilder::cross_context_route::<R>()`,
  `.cross_context_route_poll_interval(Duration)`) with its own durable,
  per-route cursor and idempotency-keyed submissions (the Security
  entry above covers a namespace-collision issue in this mechanism,
  found and closed before release), so a redelivered/retried catch-up
  tick is exactly as safe as any other idempotency-keyed submission
  already is. See [docs/architecture.md §36](docs/architecture.md#cross-context-route).
- `skilj_core::db::decide_and_submit_command`: the "optimistic `decide()`,
  then locked `submit_command`" sequence `skilj-rest`'s command-trigger
  route and `skilj-graphql`'s `submitCommand` resolver each ran inline is
  now one shared, real-Postgres-tested function, used by both of them
  and by `CrossContextRoute`'s own background task.
- `plugin::upcast_payload`/`UpcastStep`: sugar for a hand-written
  `BoundedContextEvent::try_from_event` that needs to interpret a
  payload differently depending on which schema_version it was written
  under (`Metadata.version`) - a declared chain of pure JSON transforms
  instead of a repeated `match` per event type. Doesn't relax
  `schema_is_backwards_compatible`'s additive-only contract or touch
  registration in any way; operates entirely on the raw JSON before
  final deserialization (Codeberg issue #14).
- `docs/temporal-integration.md`: documents pairing skilj with Temporal
  via its existing `Idempotency-Key` on `submitCommand`/command
  triggering, keyed by `{run_id}:{activity_id}` per Temporal's own
  documented Activity-idempotency guidance - no new skilj code, phase 1
  of the plan in [`docs/architecture.md` §34](docs/architecture.md#skilj-temporal-plan).
- New `skilj-temporal` crate (phases 2-3 of the same plan): a bridge
  from skilj's own event stream (`GET /v1/events/consume`/`POST
  /v1/events/consume/ack`) to Temporal's client API, starting or
  signaling a workflow execution correlated by a fixed
  `"{bounded_context}:{tag_key}:{tag_value}"` convention over a
  configured DCB tag. Speaks only Temporal's thin `temporalio-client`
  gRPC client, never the full `temporalio-sdk` worker/Activity-authoring
  crate - zero dependency on any other skilj crate, independently
  usable like `skilj-tui`.
- `SkiljBuilder::pool_options`/`db::connect_with`: configurable Postgres
  connection pool sizing/timeouts (`sqlx::postgres::PgPoolOptions` -
  `max_connections`, `min_connections`, `acquire_timeout`, ...),
  previously hardcoded to `sqlx`'s own bare default (10 connections, no
  timeouts) with no way to tune it at all. `db::connect`'s own default
  behaviour is unchanged for every existing caller.

### Changed

- Every direct dependency brought to its latest major version:
  `thiserror` 1→2, `base64` 0.22→0.23, `toml` 0.8→1, `prettyplease`
  0.2→0.3, `syn` 2→3, `tokio-tungstenite` 0.29→0.30, `reqwest` 0.12→0.13,
  `axum-extra` 0.10→0.12, `jsonwebtoken` 9→11, `schemars` 0.8→1, `sqlx`
  0.8→0.9. Most were drop-in; `jsonwebtoken` 11 now requires explicitly
  selecting a crypto backend (`aws_lc_rs`, already used elsewhere in the
  dependency graph); `schemars` 1's JSON Schema output moved `$ref`
  targets from `#/definitions/` to `#/$defs/` (handled for both the new
  and old shape, since a schema stored under 0.8 keeps its old shape
  until next re-registered); `sqlx` 0.9 requires proof that dynamic SQL
  strings aren't injectable - every schema-qualified query (a per-tenant
  Postgres schema name interpolated via `format!`) now wrapped in
  `AssertSqlSafe`, independently audited against the pre-change source
  to confirm no other content moved. No behavior change from any of
  this - verified via the full existing test suite, unchanged.

## [0.0.3] - 2026-09-03

### Security

- Cross-tenant read access: any grant with bounded-context-level access
  (`RoleAccessMapping`/`EventReadToken`) could read another tenant's
  projection instances, raw events, commands, or snapshots, regardless
  of who they actually belonged to. `RoleAccessMapping.scope`/
  `EventReadToken.scope` now gate every read against a per-record
  derived owner (a declared `owner_tag_key`), fail-closed whenever
  ownership can't be affirmatively proven. Applied consistently across
  `ProjectionQuery`, `QueryEvents`/`CountEvents`/`InspectEvent`,
  `FetchEvents`/`ConsumeEvents`, `EventSubscription`, `CommandQuery`/
  `FetchCommands`, `InspectSnapshot`, and the initial grant
  `createBoundedContextFromTemplate` makes for a new tenant.
- Cross-tenant write access: the identical gap existed on the write
  side - even after the read-side fix above, a scope-restricted grant
  or token could still blindly submit a command or create an event for
  another tenant's records. Closed across `submitCommand`, REST command
  triggering, and REST event creation (`ExternalEventToken`/
  `DirectCreationToken`), using the same owner-tag mechanism.
- A REST scope-mismatch rejection returned an HTTP 500 instead of 403 -
  latent until the write-side fix above made it reachable at all; fixed
  alongside it.

### Added

- Private fields: a new field-level visibility mechanism alongside
  `sensitive_fields` - a field visible by default only to its own
  creator (`own`), to any Role in a named team (`team`), or to the
  party its own payload addresses (`addressed`), with self-service
  sharing (`grantPrivateFieldAccessForEvent`/`ForCommand`,
  `revokePrivateFieldAccess`, `listPrivateFieldGrants`) and no
  encryption involved anywhere - a plain read-time redaction, not a
  cryptographic one, since none of the three need to survive an
  erasure request the way `sensitive_fields` does.
- `EventType`/`CommandType.owner_tag_key` and `RoleAccessMapping.scope`/
  `EventReadToken.scope`/`ExternalEventToken.scope`/
  `DirectCreationToken.scope`/`CommandToken.scope`, all exposed on the
  wire (`registerEventType`/`registerCommandType`'s own `ownerTagKey`
  argument, and each token's own `scope` field on the object returned
  by minting it).
- `createBoundedContextFromTemplate` accepts an optional `scope` for the
  tenant's own initial grant.

### Fixed

- A panic (`role_access_mappings row references a bounded_contexts row
  that no longer exists`) when a bounded context was hard-deleted while
  an unrelated concurrent read was in flight - a stale row is now
  treated as gone rather than corrupt.

## [0.0.2] - 2026-08-30

### Added

- Templated bounded-context tenants (Codeberg issue #13): a new
  `createBoundedContextFromTemplate`/`resyncBoundedContextFromTemplate`
  GraphQL surface lets an operator stamp out a new tenant `BoundedContext`
  from an existing one's current `EventType`/`CommandType`/`Projection`
  registrations in a single call - no Rust redeploy per tenant. See
  `specs/skilj.allium`'s `surface BoundedContextTemplating` and
  `docs/architecture.md`.
- An optional idempotency key on command submission (Codeberg issue
  #12), backwards compatible - a key is generated at random whenever
  the caller omits one, so existing callers see no behavioural change.
- Four new filterable scalar `FilterOperator`s: geo, color, IP, and enum.
- `SkiljBuilder::build()`'s startup warm-up and background pollers now
  fan out concurrently across bounded contexts instead of one at a time
  (Codeberg issue #15) - startup latency no longer scales linearly with
  bounded-context count.

### Fixed

- A same-instance dispatch-resolution race, an unbounded orphaned-tenant
  window on a bad role ID, a panic on a legitimate concurrent-delete
  race, and (the most consequential) deleting a template silently and
  permanently breaking command dispatch for every one of its tenants -
  all found by an `ultrareview` pass and fixed with real, decisively
  verified regression tests.
- Six places where documentation had drifted from the `Snapshot`
  concept's own introduction.

### Changed

- Three repeated GraphQL/DB boilerplate shapes (token-creation
  mutations, list-query resolvers, access-token getters) consolidated
  into shared macros - pure refactor, no behavioural change.

## [0.0.1] - 2026-08-28

First published release of the `skilj` workspace: `skilj-macros`,
`skilj-core`, `skilj-graphql`, `skilj-rest`, `skilj`, `skilj-codegen`,
`skilj-tui`, and `skilj-inspector`.

An event-sourced, DDD-style library for building Postgres-backed
applications around a Dynamic Consistency Boundary (DCB), with GraphQL
and REST surfaces, a declarative codegen path for bounded contexts, and
two Ratatui-based operator consoles. See
[`docs/architecture.md`](docs/architecture.md) and
[`specs/skilj.allium`](specs/skilj.allium) for the full design and
behavioural specification.

[Unreleased]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.8...HEAD
[0.0.8]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.7...v0.0.8
[0.0.7]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.6...v0.0.7
[0.0.6]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.5...v0.0.6
[0.0.5]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.4...v0.0.5
[0.0.4]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.3...v0.0.4
[0.0.3]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.2...v0.0.3
[0.0.2]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.1...v0.0.2
[0.0.1]: https://codeberg.org/gklijs/SklilJ/releases/tag/v0.0.1
