# SklilJ

Rust project to guide AI to good solution, making sure the data is stored in a proper way, authenticated, and it's easy to add new features.

See [`specs/skilj.allium`](specs/skilj.allium) for the full behavioural specification, and
[`docs/architecture.md`](docs/architecture.md) for how it's built in Rust.

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
