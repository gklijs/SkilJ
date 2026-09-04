# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(pre-1.0: breaking changes may land in minor/patch versions until 1.0).

## [Unreleased]

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

### Added

- `plugin::upcast_payload`/`UpcastStep`: sugar for a hand-written
  `BoundedContextEvent::try_from_event` that needs to interpret a
  payload differently depending on which schema_version it was written
  under (`Metadata.version`) - a declared chain of pure JSON transforms
  instead of a repeated `match` per event type. Doesn't relax
  `schema_is_backwards_compatible`'s additive-only contract or touch
  registration in any way; operates entirely on the raw JSON before
  final deserialization (Codeberg issue #14).

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

[Unreleased]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.3...HEAD
[0.0.3]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.2...v0.0.3
[0.0.2]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.1...v0.0.2
[0.0.1]: https://codeberg.org/gklijs/SklilJ/releases/tag/v0.0.1
