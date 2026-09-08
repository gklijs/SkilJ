# Reading events over REST

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
- **A brand-new token replays your entire history by default.** Its first `consume` call starts
  at the very beginning of the stream — fine for a backfill, a footgun for a side effect that
  must never re-fire for old events (the classic case: wiring a new `UserRegistered` handler
  that sends a welcome email, and watching it email every user who has ever registered). Have
  whoever mints the token pass `startFrom: LATEST` to the `createEventReadToken` GraphQL
  mutation instead of leaving it at its `BEGINNING` default — that token's first call then skips
  straight past everything already committed and only ever sees what happens from there on.
  This is a one-time choice made at minting, like the ack mode above: it can't be changed on an
  existing token, only chosen for a new one.
- **Manual-ack can redeliver duplicates, on purpose.** If you fetch a batch and crash before
  acknowledging it, the next fetch serves the same batch again. This library doesn't de-duplicate
  for you — your handler needs to be safe to run twice on the same event (e.g. keyed by the
  event's own `sequence`).
- **Mixing modes on one token is allowed but not coordinated.** `GET /v1/events` (client-tracked)
  never reads or moves a token's server-side cursor, so using both against the same token gives
  you two positions that know nothing about each other.

See [§7](architecture.md#rest-wire-contract) of [`architecture.md`](architecture.md) for the full wire contract (routes,
request/response shapes, error codes), `entity ReadCursor` in
[`../specs/skilj.allium`](../specs/skilj.allium) for the underlying behavioural guarantee, and
[§43](architecture.md#new-subscriber-replay-fix) for the `startFrom`/`EventReadStartPosition` design.
