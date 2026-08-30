# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(pre-1.0: breaking changes may land in minor/patch versions until 1.0).

## [Unreleased]

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

[Unreleased]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.2...HEAD
[0.0.2]: https://codeberg.org/gklijs/SklilJ/compare/v0.0.1...v0.0.2
[0.0.1]: https://codeberg.org/gklijs/SklilJ/releases/tag/v0.0.1
