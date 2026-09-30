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

- **Every read returns one page, not everything.** Each `GET /v1/events` and each `consume` call
  returns at most `max_events_per_read` events (default 1000, set on the server's
  `SkiljBuilder`), oldest first. A short page just means you're caught up for now; a full page
  means there may be more. Keep asking: for `GET /v1/events`, pass the response's `nextCursor`
  as the next call's `after` until a call returns no events; for `consume`, simply call again (it
  continues from its own cursor). With manual-ack, acknowledge the last event you handled before
  asking for the next page - the next page starts after the acknowledged one. A replay of a long
  history (below) arrives this way too, a page per call.
- **`nextCursor` marks how far the server looked, not just the last event you got.** On a short
  page it can be past the last event served - past events your `filter` or the token's scope
  left out - so keep polling from it and a narrow filter doesn't make the server re-read the
  same excluded events every time. A `consume` cursor moves the same way (for manual-ack, only
  on a call that serves nothing). Use the same `filter` for the whole walk: a cursor reached under
  one filter has passed over events a different filter would have matched.
- **An `after` past the latest committed sequence is refused** with `400 invalid_request`. It
  can't be a `nextCursor` this route returned - most often it's a checkpoint kept from before the
  bounded context was deleted and recreated, whose sequences start over. Reset the checkpoint
  (omit `after` to read from the start). Before this was refused, such a cursor came back
  unchanged as `nextCursor` and every event up to it was silently skipped.
- **Filters are bounded.** A read takes at most 32 `filter` parameters, each value at most 4096
  characters, and an `is_like` pattern at most 1024. Past that the call is refused with `400`
  `invalid_filter` rather than answered - every filter runs against every event the read walks
  ([§121](architecture.md), [§122](architecture.md)).
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
- **Want to replay from a specific point instead of "nothing" or "everything"?** `startFrom:
  AT_SEQUENCE` with `startAtSequence: <n>` starts the token's first call strictly after sequence
  `n` — useful when you know exactly where you want to pick up, even mid-history (`n` can be
  older than the token's own minting time; unlike `LATEST`, this can replay some history on
  purpose). `startFrom: AT_TIME` with `startAtTime: "<RFC3339 timestamp>"` does the same by time
  instead of sequence number — it looks only at what's already committed the moment the token's
  first call actually happens, not a schedule: a future timestamp just behaves like `LATEST` did
  at that moment. Naming one of these without its own value (or with the other one's) is
  refused, not silently defaulted.
- **Manual-ack can redeliver duplicates, on purpose.** If you fetch a batch and crash before
  acknowledging it, a later fetch serves the same batch again. This library doesn't de-duplicate
  for you — your handler needs to be safe to run twice on the same event (e.g. keyed by the
  event's own `sequence`).
- **A served, unacknowledged batch is claimed for a while.** A manual-ack fetch claims what it
  serves for `read_cursor_checkout_lease` (default 5 minutes, set on the server's `SkiljBuilder`),
  so that two workers sharing a token never both get the same events. A fetch that serves nothing
  claims nothing, so polling an idle stream never delays the next event. Until you acknowledge or
  the claim lapses, the *same token's* next fetch returns nothing. So if handling an event fails
  and you want to retry, keep the batch you were served and retry it in memory - don't fetch
  again expecting it back, or you'll wait out the whole lease (see
  [§99](architecture.md) for a bridge that did exactly that). A crashed worker's batch comes back
  once the claim lapses.
- **Mixing modes on one token is allowed but not coordinated.** `GET /v1/events` (client-tracked)
  never reads or moves a token's server-side cursor, so using both against the same token gives
  you two positions that know nothing about each other.

See [§7](architecture.md#rest-wire-contract) of [`architecture.md`](architecture.md) for the full wire contract (routes,
request/response shapes, error codes), `entity ReadCursor` in
[`../specs/skilj.allium`](../specs/skilj.allium) for the underlying behavioural guarantee, and
[§43](architecture.md#new-subscriber-replay-fix) for the `startFrom`/`EventReadStartPosition` design.
