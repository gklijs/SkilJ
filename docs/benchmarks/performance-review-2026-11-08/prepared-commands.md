# Owned command preparation: issue #172, first stage

`CommandBatcher::decide_and_submit` and `decide_and_submit_many` now move
resolver-owned history, selected history, consistency tags and snapshot states
into the queue. Batch items also move their command type, payload and caller
metadata. Previously queue preparation cloned these values after resolution.

This is the independently landable ownership stage described in #172. Selection
still produces a separate `Vec<Event>` and conflict redispatch still uses the
existing event representation. There is no shared event-view contract or
validation-only evidence contract in this change. In particular, #174 must not
interpret `PreparedCommand` as permission to skip locked cryptographic checks.

## API and compatibility

The public addition is `command_batcher::PreparedCommand`, with private fields,
created by `CommandBatcher::prepare_command` and consumed by
`CommandBatcher::submit_prepared`. It is not `Clone`: a caller cannot accidentally
copy its histories or submit the same preparation twice. Dropping a preparation
writes no command or event; preparation can provision encryption keys during
warm-up, as the existing submission path does.

Call `prepare_command` with an already authorised `CommandBatchItem` and the
chosen decision time. Pass the result by value to `submit_prepared`, with the
same database and collaborators. It derives its context from the prepared value.
The low-level split API returns errors that require a new read; callers can use
`db::back_to_the_read`, rebuild the item and prepare it again. The higher-level
`decide_and_submit` retains its existing one-retry behavior. All existing public
signatures and dispatcher implementations remain source compatible. The
borrowed `submit` entry point still copies slices because it does not own them;
callers seeking to avoid that cost should use the owned or higher-level APIs.

Opaque ownership does not bind a preparation to a particular database or set of
collaborators at the type level. Using the same collaborators is a documented
precondition, as it already is for the batcher's borrowed submission API.
No REST or GraphQL representation changes.

## Correctness and replay

The queue receives the same histories, high-water mark, snapshot positions,
initial decision and decision time as before. Moves do not alter their contents.
Temporary plaintext from `decision_history` is dropped before the resolver
returns; the prepared value holds the original persisted representation.
No plaintext is added to caches or rejection history.

All locked paths remain intact: current registration checks, subject-key
validation and locks, delta selection, same-batch visibility, idempotency and
conflict redispatch. The batch writer still borrows the queued `BatchedCommand`.
It clones decisions where needed for the first set-write attempt, leaving the
original replayable for per-command fallback if that write fails. Consuming the
outer preparation at enqueue does not consume these decisions during that first
attempt. Decision time remains fixed between optimistic and locked dispatch.

The delayed-preparation regression test prepares three optimistic acceptances
before any submission: the first commits, the second deduplicates, and the third
redispatches and rejects with the committed event in its history. It also submits
an initially rejected preparation with nonempty history, checks caller metadata
and decision time, and verifies preparation itself writes no events. Existing
batch tests exercise ordered same-batch visibility, disconnections, spillover,
conflicts and failed-write isolation.

## Evidence and remaining measurements

The checked-in [allocation probe](history-copy.rs) measured the two old queue
history copies at 7,958,000 requested bytes in 20,002 allocations for 1,000
synthetic directly-created events (1 KiB payloads, 2 KiB schemas). Moving those
vectors requested zero bytes in zero allocations. These are isolated copying
costs, not durable throughput, resident memory or context-lock measurements.
The queue conversion now performs those moves in production; it still allocates
for warm-up, channels, queue storage and subsequent locked processing.

Remaining work in #172 needs separate measurements and API decisions: selected
history indices or shared records, typed prepared input, command-origin and
large-schema retention, 0/100/10,000-event workloads, conflicts and rejection,
and end-to-end durable throughput and lock duration. Reference counting is not
adopted here without measuring its allocation and retention costs. The public
event-view redesign must be coordinated with #173, and any validation evidence
with #174. Generic incremental folding remains optional.

## Validation of this stage

On 2026-11-08 the six core suites `command_batcher`, `submit_command`,
`command_owner_scoping`, `command_query`, `encryption` and `snapshot_context`
passed (110 tests, no skips) against the existing local Postgres 17 container.
The embedded fallback cannot run in this environment because `libxml2.so.2` is
missing, so the recorded verification used `DATABASE_URL` with sandbox approval.
Core clippy with all targets and warnings denied, formatting, and core rustdoc
with warnings denied passed. The isolated allocation probe was rerun against
the changed library and reproduced the checked-in counts exactly.
The pinned CGA check passed against the unchanged baseline (6,017 potential
panic occurrences and 186 skipped generic targets); all 17 checker regression
tests passed outside the sandbox. This retains the analyzer's documented
partial coverage and does not establish panic freedom.
