# SklilJ

`skilj` is a Rust library for building event-sourced, DDD-style applications backed by
Postgres, using a Dynamic Consistency Boundary (DCB) instead of classic per-aggregate event
sourcing - a command's consistency check spans exactly the tags it needs, not one fixed
aggregate boundary. It exposes a GraphQL surface and a REST surface, and is built to guide an
AI coding agent toward a correct, well-structured solution: the domain model is spec-first,
data is encrypted/decrypted consistently, every surface is access-controlled, and adding a new
event/command/projection follows one repeatable shape.

See [`specs/skilj.allium`](specs/skilj.allium) for the full behavioural specification, and
[`docs/architecture.md`](docs/architecture.md) for how it's built in Rust.

## Crates

| Crate | What it is |
|---|---|
| [`skilj`](skilj) | The main facade - a thin wrapper over `skilj-core`/`skilj-graphql`/`skilj-rest`. Start here. |
| [`skilj-core`](skilj-core) | The domain engine: entities, rules, the plugin API, and persistence. Zero web-framework dependency. |
| [`skilj-graphql`](skilj-graphql) | The GraphQL surface - a runtime-rebuilt dynamic schema and resolvers, independently usable. |
| [`skilj-rest`](skilj-rest) | The REST surface - narrowly-scoped, `AccessToken`-authenticated routes for agents and automated callers. |
| [`skilj-macros`](skilj-macros) | Two narrowly-scoped proc-macros used internally (re-exported through `skilj-core`, not usually added directly). |
| [`skilj-codegen`](skilj-codegen) | Optional `build.rs` codegen: turns a declarative `.skilj.toml` bounded-context file into real Rust `EventType`/`CommandType` impls. |
| [`skilj-tui`](skilj-tui) | `cargo install skilj-tui` - a Ratatui operator console, a pure GraphQL client for any `skilj` deployment. |
| [`skilj-inspector`](skilj-inspector) | `cargo install skilj-inspector` - a read-only Ratatui console that talks directly to Postgres, for when `skilj-graphql` isn't running. |

`skilj-demo` (in this repo, not published) is a full worked example - two bounded contexts
(banking, courses) showing what the DCB buys over classic per-aggregate event sourcing.

## Getting started

```toml
[dependencies]
skilj = "0.0"
```

See [`skilj-demo`](skilj-demo) for a complete, runnable example, and
[`.claude/skills/skilj/`](.claude/skills/skilj) for a Claude Code skill that walks an AI agent
through adding a new event/command/projection to an existing bounded context.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this project, as defined in the Apache-2.0 license, shall be dual-licensed as above, without
any additional terms or conditions.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for how to build, test, and submit changes,
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) for community expectations,
[`SECURITY.md`](SECURITY.md) for how to report a vulnerability, and
[`CHANGELOG.md`](CHANGELOG.md) for release notes.

## Reading events over REST

If you're a machine caller holding an `EventReadToken` (an AI agent, a remote workflow, an
adapter), there are three ways to read a token's event stream, each trading off differently. Pick
based on how your own process fails, not just on which is "best":

| | Who remembers where you are | If your process crashes mid-read | Best for |
|---|---|---|---|
| **Client-tracked** (`GET /v1/events?after=...`) | You do | You resume exactly where you left off — you control the position | You already persist a checkpoint somewhere (a database row, a file) and want full control |
| **Server-tracked, auto-advance** (`GET /v1/events/consume?mode=auto`) | SkilJ does, per token | You lose whatever you were served but hadn't finished handling — SkilJ won't send it again | Quick integrations, stateless workers, scripts — no checkpoint to manage at all, occasional missed events on crash is fine |
| **Server-tracked, manual-ack** (`GET /v1/events/consume?mode=manual` + `POST /v1/events/consume/ack`) | SkilJ does, per token, but only once you confirm | You get the same events again next time — nothing is lost | Processing that must never silently drop an event, as long as your handler is safe to run twice on the same event (idempotent) |

A few things that trip people up:

- **One token = one read position.** If you want two independent places in the stream (say, two
  worker instances), mint two `EventReadToken`s rather than trying to share one — there's no
  separate "consumer name" to pass.
- **Auto-advance and manual-ack are a one-time choice per token.** Whichever mode a token's first
  `consume` call uses is the mode it keeps for that token's lifetime. Want to switch? Use a new
  token.
- **Manual-ack can redeliver duplicates, on purpose.** If you fetch a batch and crash before
  acknowledging it, the next fetch serves the same batch again. This library doesn't de-duplicate
  for you — your handler needs to be safe to run twice on the same event (e.g. keyed by the
  event's own `sequence`).
- **Mixing modes on one token is allowed but not coordinated.** `GET /v1/events` (client-tracked)
  never reads or moves a token's server-side cursor, so using both against the same token gives
  you two positions that know nothing about each other.

See §7 of [`docs/architecture.md`](docs/architecture.md) for the full wire contract (routes,
request/response shapes, error codes), and `entity ReadCursor` in
[`specs/skilj.allium`](specs/skilj.allium) for the underlying behavioural guarantee.
