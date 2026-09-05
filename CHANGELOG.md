# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(pre-1.0: breaking changes may land in minor/patch versions until 1.0).

## [Unreleased]

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
  §36.
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
  rather than leaving any part of it open. See docs/architecture.md §37.

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
  already is. See docs/architecture.md §36.
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
  of the plan in `docs/architecture.md` §34.
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

[Unreleased]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.4...HEAD
[0.0.4]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.3...v0.0.4
[0.0.3]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.2...v0.0.3
[0.0.2]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.1...v0.0.2
[0.0.1]: https://codeberg.org/gklijs/SklilJ/releases/tag/v0.0.1
