# Performance review for the next breaking release

Reviewed revision: `c1a509025aa27676a02c381b1031fee0019ff638`.

There are worthwhile opportunities beyond the batching, snapshots and indexed
tag reads already described in [performance.md](performance.md). I would spend
the API break on an owned prepared-command object and a smaller event read
model. Neither requires weakening consistency or authorization. Start with the
bounded cache read, which is a smaller change with directly observable waste.

This is a source review plus an isolated allocation experiment, not a fresh
end-to-end throughput comparison. The numbers below are allocations requested,
not peak RSS, retained memory, or commands per second. No production API or
storage behavior was changed by this review.

## Priorities

| Order | Opportunity | Where it helps | API impact | Evidence |
| --- | --- | --- | --- | --- |
| 1 | Apply cache type/size bounds before copying events | Cached pagination, polling and counts | Can be internal | Existing code copies the remaining window before truncation; isolated probe shows 10x fewer requested bytes for a 100-of-1000 read |
| 2 | Move an owned prepared command into the queue; share history and declarations | History-heavy commands and queued concurrency | Change lower-level submit/history types | The two history copies in queue preparation alone allocate about 8 MB in the probe |
| 3 | Push event selection/counting into SQL and hydrate only returned rows | Large histories, selective admin queries | Internal initially; explicit page/cursor object is useful | Current untagged counts hydrate every event in chunks |
| 4 | Separate locked sensitive-history validation from plaintext materialization | Commands over protected history, especially unchanged decisions | Internal initially; prepared validation evidence may affect dispatcher contract | No-conflict locked path decrypts history and discards the result |
| 5 | Prepare typed command input once; consider incremental decision state | Hot keys and repeated decisions | Command dispatcher/decider API | Typed payload/history deserialization repeats on redispatch |
| 6 | Replace full data-key-cache flushes with bounded eviction and coalesced misses | Working sets near/above 10,000 keys, remote key providers | Usually internal | A full cache clears all live entries on the next insertion |
| 7 | Batch provider unwrap/wrap for key migration | Tenant-key assignment and legacy migration | Extend `KeyProvider` | Cross-key rewrap currently awaits each row's unwrap/wrap serially |

## 1. Bound cache reads before copying

In [`db/events.rs`](../skilj-core/src/db/events.rs),
`for_each_event_chunk` calls `EventCache::try_events_after`, then filters the
returned `Vec<Event>` by type and takes `chunk_size`. The cache has already
cloned all events after the cursor. `list_events_cached_limited` has the same
shape. A request for 100 events from a 1000-event window can copy 1000 events,
including their schemas and command origins, to keep 100. Successive chunks
can copy the remaining suffix again.

Add a cache read accepting an event-type predicate and a limit; filter and
take while holding the read guard, before `window.serve`. Return explicit
coverage/end information so a short page isn't confused with an incomplete
window. Apply `up_to` before hydration where possible too. The existing
freshness/registration/epoch checks must still precede serving data.

The allocation probe models the copying/truncation portion, excluding database
freshening and registration replacement. It observes 3,979,000 requested bytes
for clone-then-take versus 397,900 for take-then-clone. This supports the work
reduction, not a claim of 10x faster HTTP requests.

Validate cache hit/miss parity with type selection, exact-size final pages,
empty results, recreation, policy changes and concurrent append/erasure.
Measure allocation bytes and p95 for page sizes 1/100 and window sizes
1000/10,000, including rare event types.

## 2. Own preparation and share immutable records

[`resolve_command_submission`](../skilj-core/src/db/commands.rs) obtains
`bounded_context_events`, then `select_matching_events` deep-clones selected
records into another vector. [`CommandBatcher::prepare`](../skilj-core/src/command_batcher.rs)
copies both vectors again with `to_vec`. A conflict builds an extended history
and selects into another owned vector under the sequence lock.

`Event` owns its `BoundedContext`, `EventType`, JSON payload, tags, encryption
references and origin; a command-triggered origin owns a whole `Command` and
its type. Cloning one event therefore duplicates much more than its payload.

Proposed breaking contract: a private-field `PreparedCommand` consumed by
submission, containing owned input, decision context, read watermark and
history selection. Pass the resolver result into queue preparation by value.
Represent selection with indices/references into one owned history, rather
than a second deep copy. Share immutable payloads/type declarations/command
origins using `Arc`, and expose a borrowed `EventView` to dispatchers instead
of requiring contiguous owned `[Event]`. Move-only preparation can land before
the larger event-view change; keeping a borrowed compatibility entry point is
also possible.

In the synthetic 1000-event case, the two queue copies request 7,958,000 bytes
in 20,002 allocations; moving them requests none. Copying 1000 existing
`Arc<Event>` handles requests 8000 bytes in one allocation. Creating those
handles has a cost, excluded from that measurement; share at initial loading,
rather than wrapping deep clones on every call.

Use shared *ciphertext* records and separate ephemeral decrypted views.
Current registration checks must still select current privacy/owner policies;
an `Arc` to an old declaration is not permission to keep serving that policy.
Retain ordered same-batch visibility, bounded queues and rejection history.
The set-write fallback currently replays the batch, so consuming its decisions
inside the first attempt requires an explicit replay strategy. Merely moving
the resolver's vectors into the queue does not require that redesign.

Validate allocation scaling with 0/100/10,000 history events, large schemas,
multi-event command origins, rejection, conflicts, sensitive history and set
write fallback. Compare durable throughput and lock time, not only clone cost.

The [ownership stage of #172](benchmarks/performance-review-2026-11-08/prepared-commands.md)
now moves resolver histories and snapshot states into an opaque prepared value,
retaining the borrowed submission API. Selection sharing and typed input reuse
remain separate work; the original probe results are not throughput results.

## 3. Select/count in the database; separate event and inspection models

[`count_events_field`](../skilj-graphql/src/resolvers/event_query.rs) walks
chunks, hydrates full events and sums the pure `count_events` predicate. It
does not need payloads or originating commands. `events_from_rows` already
batches related-row loads, so this is not an unaddressed per-event SQL query;
the remaining waste is loading and constructing records at all for a count.
Untagged queries can also scan many unrelated event types/owners/correlation
IDs before assembling a small page.

Add a shared authorized query plan for REST and GraphQL. Push event types,
tag union, correlation ID, cursor and owner scope into SQL; count with an
aggregate and hydrate only selected page rows. The owner predicate needs the
current type's `owner_tag_key`: ownerless types remain visible, and scoped
types require the matching owner tag. Preserve null-tag behavior, empty type
lists, tag union semantics and validation errors. Avoid duplicate counts from
tag joins by using `EXISTS` or distinct event identities.

The useful API break is a small event envelope carrying a command reference,
with command details loaded explicitly for inspection. Decisions, counts and
many stream consumers don't need a full originating-command object. This
reduces both DB work and all downstream copies, but consumers currently
matching `EventOrigin::CommandTriggered { command }` would need migration.

A page result with `events`, `scanned_through`, `up_to` and `has_more` can also
make scan progress explicit for sparse queries. A fixed high-water mark would
give multi-statement counts a defined event-prefix boundary; that is a semantic
choice to document, since the present chunked count can observe later commits.
Authorization declarations still need their own consistent treatment.

Differential-test SQL against the existing pure predicates and conformance
transcript. Benchmark million-row histories with wide tags, owner scopes and
rare types/correlation IDs. Inspect plans before adding indexes: extra indexes
also increase append/WAL cost.

## 4. Avoid decrypting history solely to validate an unchanged decision

Investigation result (#174, 2026-11-09): retain full locked cryptographic
validation; skip JSON conversion for nonsensitive events in mixed histories.
[Measurements, race coverage and evidence-contract requirements](benchmarks/sensitive-history/README.md)
record why validation-only handling is deferred. The proposal below is the
original review, before that investigation.

[`decide_command_in_tx`](../skilj-core/src/db/commands.rs) calls
`decision_history` even when its delta is empty, then discards the returned
history. This is important validation: keys could have been erased since the
optimistic decision. However, `decision_history` also copies the entire slice,
parses JSON, decrypts leaves and serializes the result. If any event has
sensitive fields, it copies and parses nonsensitive events in that slice too.

First separate a validation operation from plaintext materialization, and skip
parsing unchanged nonsensitive events in the latter. An owned preparation can
carry the relevant event/key identities. Under the lock, validate and lock the
same active key rows, and verify that the prepared evidence is still valid.
Only materialize plaintext for a decision that actually runs again.

This needs a correctness design before implementation: key destruction,
rewrap/retirement, missing keys, malformed ciphertext and changed registrations
must retain their current refusal behavior. A TTL-only key cache or skipping
the under-lock check is insufficient. Remote provider work under this lock
can be particularly costly, but the review did not measure that cost.

Test races with subject erasure, retirement and concurrent append. Measure
no-conflict and conflict cases separately, with warm/cold keys and a slow
provider; record critical-section wall time as well as command latency.

## 5. Prepare typed input once; make hot-state folding explicit

The registered decider closures in [`skilj/src/lib.rs`](../skilj/src/lib.rs)
deserialize the command payload and convert every selected raw event into the
bounded context's typed event enum on each invocation. Conflict redispatch
repeats this work inside the context lock.

A type-erased prepared invocation owned by the registered command handler
could retain typed input across optimistic and locked decisions. A broader
optional fold/state interface could process new events incrementally, then
decide from the state. This is more invasive than reducing raw-record copies;
existing snapshots and `QueryItemMapping::nothing()` already solve important
versions of this problem without an API break.

Do not assume every decision can incrementally fold: `latest(n)` can remove an
old selected event, and retractions, doubts and snapshot changes affect state.
Retain a full-recompute fallback and the established decision time/deadline
context. Keep plaintext typed state local to the invocation. Benchmark against
the existing hot-snapshot path before adding another plugin abstraction.

## 6. Avoid the data-key-cache eviction cliff

[`encryption/service.rs`](../skilj-core/src/encryption/service.rs)'s `Cache::insert`
purges expired entries when full, then clears *all* remaining live entries if
still at capacity. With 10,000 live entries, one new key can evict the full
working set. Concurrent misses can also unwrap the same stored key separately.

Use bounded LRU/clock eviction and single-flight unwraps keyed by the stored
wrap digest. Keep reading the DB row before consulting the cache, preserve
TTL, and fence in-flight loads across `clear_cache`/retirement so old requests
cannot repopulate a cache that was just invalidated. This can be internal.
Measure provider calls and p99 around the capacity boundary, including repeated
subjects, provider failures and retirement during an in-flight unwrap.

## 7. Batch remote key migration operations

[`rewrap_data_keys`](../skilj-core/src/db/master_key.rs) batches a target key's
own rewraps, but moves from another key/legacy format by awaiting unwrap and
target wrap for each row. Add provider batch unwrap/wrap operations with
default scalar implementations, grouping by provider/key and using bounded
batch sizes. With a remote provider, the mechanism reduces network waits from
roughly one or two per row to a handful per batch; actual gain is unmeasured.

Preserve per-item unreadable results, outage/permission errors, conditional row
updates, subject erasure wins and the completion audit event. This improves
maintenance and assignment latency, rather than ordinary warm command reads.

## Scope and validation

The [security fixes](security-audit-2026-11-08.md) remain constraints throughout:
bounded GraphQL preflight, bounded signing-key trust, current owner/privacy
policies and protected provenance. Caching authentication indefinitely or
omitting registration/key checks would undo those fixes.

Removing the context lock is a different protocol project. The existing
[DCB investigation](disjoint-consistency-boundaries.md) and
[Postgres experiments](dcb158-postgres-experiment.md) already describe its
correctness obligations and prototype limits. Changing the Rust API alone
does not remove ordered durable append, synchronous projection, idempotency
and multi-instance validation requirements. I would implement priorities 1–3
before reopening that work.

The allocation probe uses real `Event`/`select_matching_events` code, a counting
system allocator and synthetic directly-created events: 1000 events, roughly
1 KiB JSON payloads and 2 KiB schemas, one tag, no encryption or command origin.
Its [source](benchmarks/performance-review-2026-11-08/history-copy.rs) and
[output](benchmarks/performance-review-2026-11-08/history-copy.log) are checked in.
It is deliberately single-threaded and measures individual copying operations,
not an end-to-end submission or a proposed implementation's equivalence.

Reproduce from the repository root (Rust 1.99.1 used for this review):

```sh
cargo build -p skilj-core --offline
python3 - <<'PY'
from pathlib import Path
import subprocess
lib = max(Path('target/debug/deps').glob('libskilj_core-*.rlib'),
          key=lambda p: p.stat().st_mtime)
subprocess.run(['rustc', '--edition=2021', '-O',
    'docs/benchmarks/performance-review-2026-11-08/history-copy.rs',
    '--extern', f'skilj_core={lib}', '-L', 'dependency=target/debug/deps',
    '-o', '/tmp/skilj-history-copy'], check=True)
subprocess.run(['/tmp/skilj-history-copy'], check=True)
PY
```

Validation completed: core offline build and allocation probe. No fresh
Postgres/load comparison or full workspace test run was performed. Each
proposed optimization needs the targeted checks above before adoption; changes
to the core would additionally need the repository's normal tests, clippy and
CGA checks. The measured clone reductions should not be presented as overall
throughput gains.
