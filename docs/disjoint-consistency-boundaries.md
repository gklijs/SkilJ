# Investigation: concurrency for disjoint consistency boundaries (#158)

**Follow-up, 2026-11-02:** Docker resolved the database startup blocker. See
[the PostgreSQL/UUID/batch-order experiment](dcb158-postgres-experiment.md)
for measured results, a working commit-order streaming prototype, and the
revised recommendation. The report below records the initial investigation.

Investigated 2026-11-01 against `2cacdb2b6e48ab7476925ab25a3e998d298159b2`.
[Issue and acceptance criteria](https://github.com/gklijs/SkilJ/issues/158).
This change contains investigation material and benchmark fixtures, not a new
production append protocol.

## Recommendation and evidence status

**Defer pending a concrete workload and durable measurements.** Retain the
current append protocol. The first candidate to measure is a shorter global
validation/append phase, not a table per tag or a replacement sequence.

Disjoint commands can prepare concurrently today. More preparation could in
principle run concurrently, but the existing transactional sequence counter
still serializes the append-to-commit suffix. Query locks alone do not remove
that suffix. A 10x gain is not established: there are no fresh throughput or
latency measurements from this investigation. In particular, the old numbers in
[performance.md](performance.md) are historical evidence, not measurements of a
new protocol.

The benchmark compiled, but its PostgreSQL could not start: the cached 18.6.0
binary requires `libxml2.so.2`; this host has `libxml2.so.16`. `DATABASE_URL` was
unset. The ordinary harness printed a skip and returned success. The new
`SKILJ_BENCH_REQUIRE_DURABLE=1` guard correctly failed that same run instead.
No system installation or permission request was made. This leaves exit
criterion 4 (durable comparison, including multi-instance contention) **open**.
It also leaves actual database interleavings unverified here. The finite model
below is explicitly not a substitute.

## Current critical section

Read these functions together; the lock is not merely in `next_sequence`.
This also respects [#45's explicit exclusions](https://github.com/gklijs/SkilJ/issues/45):
same-batch events cannot be hidden and allocation cannot escape the transaction.

| Stage | Source | Protected work |
|---|---|---|
| Optimistic preparation | `command_batcher.rs::prepare`-related paths, `db/commands.rs::resolve_command_submission` | Derive tags/query, obtain history/snapshot state, run initial decision, warm event types/keys before queuing. |
| Enter transaction | `db/commands.rs::begin_command_batch_leader_tx` | Acquire the bounded context's single `sequence` row `FOR UPDATE`, capture committed high-water mark, read sync projection registrations. |
| Drain queue | `command_batcher.rs::run_as_leader` | Drain after acquisition, up to batch cap. Independent instances have separate queues but contend on the same database row. |
| Validate/plan | `db/commands.rs::write_command_batch_as_set`, `decide_command_in_tx` | Recheck idempotency, blind-index generation, query delta since the covered read; redispatch if matching events appeared. Resolve/protect records and assign tentative numbers from the locked high-water mark. |
| Preserve batch order | same | Visit commands in input order; feed earlier planned events through `extra_committed_events` and claimed idempotency keys, even though SQL writes have not happened. |
| Write | `db/commands.rs::write_planned_commands` | Savepoint, transactional counter update, set inserts of commands/events/key links/idempotency rows, synchronous projection folds. |
| Recover individual failures | `commit_command_batch_per_command` | Roll back the set savepoint and replay per command with savepoints. Counter changes roll back with records. |
| Commit | `commit_command_batch` | Commit before releasing transaction locks or answering acceptance. |
| Publish | `broadcast_appended_batch` | Publish/cache/notify after commit. These costs affect response latency but are outside the row-lock critical section. |

Source files are under `skilj-core/src/`.
The single-command `db::submit_command` also takes the sequence lock before its
recheck and persistence. It is not safe to optimize only `CommandBatcher`.
An empty decision still needs consistency/idempotency treatment even if no
event number is allocated.

The timing labels have limits: leader `lock_wait_us` includes connection and
transaction setup and sync-registration reads, not just PostgreSQL lock wait.
The four phase sums omit queue drain and some bookkeeping and therefore are not
an exact hold-time measurement. The set path assigns numbers in memory during
planning, so a near-zero `sequence_us` does not mean ordering is free.
`redecides` counts decision redispatches, not SQL transaction retries.

## Append and coordination inventory

| Entry point / participant | Sequence and consistency requirement |
|---|---|
| `submit_command`, `decide_and_submit_command`, `submit_command_batch`, batch leader | Validate effective query, preserve idempotency and ordered same-batch visibility, append atomically. REST/GraphQL and trigger paths ultimately use these DB operations. |
| `create_and_insert_external_event`, `create_and_insert_external_events`, `insert_external_event_in_tx` | Adapter/dedupe checks plus sequence allocation and event/projection writes in the same transaction. External events can invalidate command reads without themselves reading a DCB query. |
| `create_and_insert_direct_event` | Sequence plus direct event and sync projections atomically; also a potential invalidating writer. |
| `event_types.rs::fire_system_event` | Locks scheduled type, claims sequence transactionally, appends and advances schedule atomically. Invalidates matching command reads too. |
| `access.rs::record_access_event`, `baseline_access_events`; `master_key.rs` audit insertion | Administrative events claim the admin bounded context's sequence in the encompassing transaction. They must remain covered by any universal writer protocol. |
| `events.rs::insert_event`, `next_sequence`, `insert_event_and_update_sync_projections` | Public low-level building blocks. `next_sequence(&pool, ...)` still permits autocommit; `insert_event` does not itself claim/check a sequence. The higher-level invariant is a caller obligation, not a database constraint enforced for arbitrary SQL. |
| Blind-tag rotation (`schema.rs`), projection promotion/rebuild coordination (`projection_records.rs`, `projection_consumers.rs`) | Also use the sequence row to fence changes. A replacement protocol must preserve these barriers, not just event/event exclusion. |

`event_tags` is filled by an `AFTER INSERT` statement trigger
(`db/schema.rs::ensure_event_tags_index`), inside the inserting transaction.
Separate application writes to an index table are not needed and would risk
breaking atomicity. The trigger does not enforce a DCB read protocol for raw SQL.
Bridge delivery reaches external append operations; changing only HTTP routing
would miss both those writers and direct library users.

## Exact conflict relation

For command A, derive its effective runtime query `Q_A`. Each item ANDs all
its tags and accepts any listed event type; an empty type list means any type.
Items are ORed. An item with neither tags nor types matches nothing. With no
explicit query the default is OR across the command's non-null consistency
tags, over all event types. Empty/all-null default tags read nothing; an
explicit query can match a null tag. Use derived tag values, including blind
index generation, not payload field names.

For actual candidate output events `W_B`, the directional edge is:

```
B invalidates A  iff  exists e in W_B: query_matches(Q_A, e)
A and B are independent iff neither directional edge exists
```

This relation concerns the events the final decisions actually emit. A
redispatch may change the output set, so an initial decision is not a permanent
write-set declaration. `latest` limits the history handed to a decision, not
the invalidation predicate: every new matching event may displace a retained
event. Snapshot items must protect the underlying events/state prefix too.

Examples:

| A reads | B writes | Invalidates A? |
|---|---|---|
| `AccountClosed` with account=A | `Deposited` with account=A | No; same tag, wrong type. |
| account=A AND currency=EUR | account=A only | No; missing conjunct. |
| account=A AND currency=EUR | account=A, currency=EUR, tenant=T | Yes; extra tags do not exclude a match. |
| account=A | one event tagged account=A and account=B | Yes, and it can also invalidate a different reader of B. |
| latest company lifecycle OR ticket=X | new matching lifecycle event | Yes, regardless of the ticket tag. |
| nothing | any event | No incoming edge; the no-history command may still invalidate another reader. |

Read/read overlap is harmless. Write/write tag overlap alone is not a DCB
conflict (though both still need ordered appends and may touch the same sync
projection or idempotency key). Event type identity and tags are scoped to the
bounded context.

## Minimum safe serialization and proof sketch

Under the present transactional counter, a writer acquiring the next range
holds that row lock until commit or rollback. A second writer cannot acquire
the following range during that suffix. This preserves both gaplessness on
rollback and visibility in number order. PostgreSQL sequences do not provide
this contract: allocated numbers are not rolled back
([PostgreSQL sequence documentation](https://www.postgresql.org/docs/18/functions-sequence.html)).

Global gaplessness alone, as a mathematical property of a final event set,
does not prove concurrent transactions impossible. The stronger existing
contract is a durable, visible ordered prefix, with atomic commands/events.
An alternative implementation could prepare unnumbered records concurrently
and publish an ordered prefix through a coordinator. That is another staging,
recovery and reader-visibility protocol, not removal of the ordering gate.
Preallocation followed by independent publication admits rollback gaps or
later numbers becoming visible first.

A sufficient shorter-gate protocol would be:

1. Read a complete committed prefix H, prepare the decision outside the gate.
2. Acquire the same database sequence row on a READ COMMITTED transaction.
3. Validate all matching events after H through the locked high-water mark;
   validate registrations, key generations, idempotency, and other mutable
   dependencies relied on by preparation.
4. If invalidated, abandon/reprepare outside the gate and repeat, or use the
   current ordered under-lock redispatch. Do not append a stale decision.
5. Assign actual sequence numbers, materialize sequence-dependent records,
   append all related rows and synchronous projections, then commit together.

With every writer participating, no foreign append can commit between step 3
and 5. All earlier commits were included in the validated prefix; subsequent
ones occur after this command. Batch-local preceding events must be checked
separately, in order, just as today. No global read/write-set predictor is
needed for this proof. The current algorithm already implements much of this.

Moving step 3 outside the gate without additional exclusion is unsafe:
A reads/validates; B appends a matching event and commits; A takes the gate
and appends its old answer. Disjoint-output optimization does not repair a
missed directional edge. Reading through a stale cache at step 3 is likewise
insufficient. Retry outside the gate can reduce hold time but adds repeated
reads, wasted decisions, and starvation for hot boundaries; it also needs an
explicit rule for batch ordering rather than moving a conflicting batch item
behind later items.

## Designs compared

| Design | Contracts and cross-instance behavior | Costs, failures and rollout |
|---|---|---|
| Keep current | Preserves present atomic ordered prefix, batch semantics and DCB checks through the database row. | Independent work still queues at append. No migration; tune query/history/snapshots, batching, pool and context boundaries first. |
| Shorter validation/append gate | Can preserve contracts with the proof obligations above and unchanged database gate. | Most optimistic work is already outside it. Move only demonstrably expensive, safely revalidated work. Retry outside may starve hot keys. Roll out incrementally with old gate retained and fallbacks; no event sequence migration required. |
| Query-aware locks plus existing gate | Can protect prevalidation across instances if ALL writers obey them. Does not remove ordered commit suffix. | Dynamic output sets, broad reads, blind rotation, upgrades and global lock ordering add complexity. Extra round trips and coarse false conflicts can exceed saved work. Mixed old/new writers cannot justify omitting under-gate validation. |
| PostgreSQL SERIALIZABLE | Protects database reads participating in the same serializable transaction; it is not protection for arbitrary cached/out-of-transaction history. | Predicate locks do not block writes; transactions may abort. Full retry required. Shared sequence-row updates still serialize and old snapshots may produce serialization failures even for disjoint tags. Keep commit validation unless serialization order is proved consistent with event order. |
| Partition order/sequence | Concurrency across independent partitions; retains gaplessness only within each partition. | Explicit semantic/API/schema migration. Cross-partition commands require coordination and atomic commit; common tenant keys still hot. Old/new readers cannot share an implicit scalar sequence contract. |

PostgreSQL documents predicate locking, false positives and whole-transaction
retry requirements under
[Serializable isolation](https://www.postgresql.org/docs/18/transaction-iso.html#XACT-SERIALIZABLE).
Index access shape, predicate lock promotion, and scans over `event_tags` can
affect false conflicts. Merely changing the isolation level of the final write
transaction would exclude the optimistic read from SSI's dependency tracking.
The serializability argument also needs all relevant transaction classes to
participate at the required isolation level; silently leaving bridge or direct
writers at READ COMMITTED is not a complete SSI design.

### A conservative query-lock construction, and why it is not the recommendation

For each tagged read item, choose one canonical tag as an anchor and take a
shared lock; writers take exclusive locks for **every** tag of every event they
will emit. A match necessarily includes the chosen anchor, so it cannot be
missed, including AND items and events with several tags. OR items require all
their anchors. This deliberately overlocks partial matches and wrong event
types. Shared read locks allow read/read concurrency; exclusive write locks
introduce write/write false conflicts.

Type-only reads need type gates. A simple conservative construction uses an
exclusive type gate for a broad reader and a shared gate for every writer of
that type; that serializes broad readers unnecessarily but covers writers.
Any-type tagged reads still use tag anchors. Namespace gates by context and
use one consistent hierarchy/order across both kinds of gate. Normalize null
and blind tag values without collisions that could miss a lock. Hash collisions
may add exclusion but must never permit missed overlap.

The finite model uses an even coarser shared reader/exclusive writer type gate
to check coverage, not to benchmark concurrency. A production hierarchy needs
mode merging before acquisition (no in-place upgrades), canonical acquisition
order, bounded lock-set size, transaction-owned release, and safe retry on
deadlocks/cancellation. Database advisory locks coordinate instances and release
on transaction end; process mutexes do not. See
[PostgreSQL explicit locks](https://www.postgresql.org/docs/18/explicit-locking.html).

Runtime writes are the difficult part: the final decision may differ from the
initial one. Either declare a sound upper bound for all possible outputs or
release/restart when the output lock set changes, then revalidate. Taking an
extra lock opportunistically breaks the deadlock-order proof. Idempotency,
projection state, registration and rotation barriers need their own coverage.
Locks must be acquired in a hierarchy that also respects existing scheduler,
projection and administration row locks. Adding tag locks after the sequence
row could deadlock with a writer holding tags while waiting for that row.

A common anchor (tenant, company, currency) collapses this conservative scheme
back to a hot lock even if full predicates are disjoint. Exact arbitrary
predicate overlap needs a coordinated registry or a richer hierarchical
protocol; checking active readers and registering a writer must itself be
atomic. That central registry adds another serialization point and crash/lease
recovery if it is not transaction-owned. Tables per tag value solve none of
these issues.

### What partitioned ordering changes

`Event.sequence`, command boundary and triggered sequences, read/subscription
cursors, cache windows, projection/snapshot `as_of_sequence` and catch-up
positions all assume one scalar context position. They need partition identity
and per-partition positions, or a separate global publication order. Cross-
partition OR/AND queries need a coherent cut; sorting timestamps or local
numbers does not supply one. `latest` across partitions needs an explicit
tie/order rule. Transfers emitting multi-tag events need a routing rule and
cross-partition consistency, rather than duplicating the event into two
unrelated logs.

Replay of noncommutative folds, synchronous projection atomicity, ordered
subscription delivery and downstream consumers require new semantics. Existing
positions need a migration/translation strategy and versioned APIs. A retained
global publication sequence restores an ordering gate. Explicit separate
bounded contexts already provide separate order domains where the application's
business contract permits them; they are not a transparent replacement for
cross-key operations within one context.

## Specification discrepancy to resolve before changing the protocol

`specs/skilj.allium::DynamicConsistencyBoundaryHonoured` quantifies over every
stored command and event, with no upper bound for the command's commit. As
written, after C1 accepts account=A at sequence 0, a legitimate C2 appending
account=A at sequence 1 violates C1's invariant. Reading null as before the first
event fixes the empty-history loophole but not this missing temporal bound.

The intended property is about the validated committed prefix at C1's
linearization/commit, including earlier same-batch commands, not events from
all future commands. One precise formulation introduces a ghost position
`validated_through(c)` and excludes foreign matching events in
`(boundary(c), validated_through(c)]`; require that cut to include every
preceding committed append and that the decision reflects the query at the
cut. Later batch items are ordered after c even when SQL commits together.
A zero-event command also needs this cut; its own first event cannot supply it.
The cut alone does not prove correct history selection or snapshot folding;
those are separate obligations.

`SequenceIsGaplessPerBoundedContext` similarly states predecessor existence,
not uniqueness or ordered visibility by itself. The primary key and transaction
protocol supply additional guarantees. This report does not silently amend the
spec or claim the literal timeless DCB invariant is preserved by today's code.
There is also stale commentary in
`event_store::consistency_boundary_and_matching_events` calling null-boundary
comparison vacuous; that does not match the newer invariant's explicit null
case. Treat the actual validation logic and the intended temporal property
separately from that comment.

## Reproducible measurement plan and harness changes

`command_throughput` now includes **ind-hot** (one reused key per worker) and
**cross** (one event with adjacent workers' account tags, forming a ring) in
addition to spread/hot/hot-snap/hot-none. Independent keys are reused so this
is not just the existing empty-history benchmark. The ring has overlapping
boundaries without one universal tag. At one worker the pair duplicates its
account; use the multi-worker rows to study overlap. The cap is intentionally
too high to reject normal benchmark traffic: redispatch counts measure
invalidations, not a business contention rejection rate.

These are fixtures, not a new business transaction API. The shared benchmark
module registers the linked event/command on all instances and teaches its
fold/snapshot about that event. Batch rows remain available for the original
and independent workloads; cross uses single commands only. Each row uses new
keys, though previous rows' events remain in the same context and background
snapshot catch-up can affect later measurements. Repeat with fresh databases
and alternate scenario order for a serious comparison.

Use a dedicated server/role with CREATEDB: the test harness drops and recreates
its named benchmark database. Set `DATABASE_URL` to that server locally, then:

```sh
SKILJ_BENCH_REQUIRE_DURABLE=1 SKILJ_BENCH_COMMANDS=8000 \
  cargo test --release -p skilj --test command_throughput -- --ignored --nocapture
SKILJ_BENCH_REQUIRE_DURABLE=1 SKILJ_BENCH_COMMANDS=8000 SKILJ_BENCH_INSTANCES=3 \
  cargo test --release -p skilj --test command_throughput -- --ignored --nocapture
SKILJ_BENCH_REQUIRE_DURABLE=1 SKILJ_BENCH_COMMANDS=8000 SKILJ_BENCH_INSTANCES=3 \
  SKILJ_BENCH_LATENCY_MS=1 \
  cargo test --release -p skilj --test command_throughput -- --ignored --nocapture
```

The harness prints fsync, full_page_writes, synchronous_commit and server
version. Durable mode requires the first three to be `on` and refuses a missing
database. This checks server settings, not hardware durability, replica
acknowledgement configuration or per-connection overrides. Never call tmpfs
results production-like storage evidence. Record actual data/WAL filesystem,
hardware, pool size, instance count, RTT, CPU load and PostgreSQL version.

Collect at least three fresh runs of baseline and any later experimental
candidate; preserve raw output and compare medians/ranges for throughput,
p50/p99, batch size, redecides, and phase costs. Add instrumentation at actual
row acquisition/release for lock hold and use PostgreSQL lock-wait observation
to separate it from pool wait. Count whole-transaction retries and failed
commands separately. These last instruments are **not** supplied by this
change; the existing timing labels must not be relabeled as exact lock timing.

Add mixed single-account and linked-event traffic, bounded-cap rejection tests,
slow decisions and synchronous projections to candidate evaluation. A ring of
linked commands alone does not measure the impact of a transfer stream on an
otherwise independent deposit stream. Compare the same fixtures on both
revisions and control history length/snapshot position. No candidate revision
has been implemented or timed here.

For an observed serial fraction f, the optimistic Amdahl bound is `1/f` even
with unlimited parallelism elsewhere. A 10x target needs f <= 0.1 before
accounting for additional locking, retries, WAL or CPU limits. Group commit
already amortizes durability and set inserts, so the historical gain from those
optimizations cannot be counted again. A workload dominated by expensive
redecisions might gain substantially, but hot-key dependencies make that work
harder to parallelize; representative measurements must decide.

## Model checks and required implementation tests

`node --test scripts/disjoint-boundaries.test.mjs` runs an independent finite
model with three tag bits and two event types: all 32 item shapes, all 1,024
two-item OR combinations, empty/single queries and all 16 event shapes. It
checks conservative anchor coverage, all 20 two-actor read/validate/commit
interleavings (rejecting schedules blocked by the gate) against a serial
create-once reference, type/AND/multi-tag examples, the stale-prevalidation
counterexample, latest invalidation and the timeless-invariant counterexample.
It does not run Rust query helpers, PostgreSQL, payload derivation, snapshots,
failure recovery or a new protocol. Passing it is limited evidence about the
design argument, not proof of production correctness.

Existing integration coverage worth preserving includes:

- `skilj-core/tests/command_batcher.rs`: concurrent same-order accepts exactly
  one; different-order appends have distinct gapless sequences; cancellation,
  idle timeout, batch caps, input order and same-batch visibility.
- `skilj-core/tests/consistency_query.rs`: only matching query events conflict;
  latest-N reselection; batch fallback for queries not covered by the batch
  tag read; null default tags; nothing queries; forgotten/encrypted history.
- `skilj-core/tests/consistency_query_read.rs` and cross-instance/listener tests:
  read/cache and notification behavior. Their existence is not a claim that
  they executed successfully in this investigation.

Before a candidate can ship, add barriers around read completion, lock
acquisition, validation, number assignment and commit. Enumerate A-read,
B-read, A/B-append/rollback/cancel schedules against a sequential decision
reference, on two separate pools/instances. Include:

1. Both directional edges separately; same tag but disjoint type; partial AND;
   OR; latest; explicit null/default all-null/nothing; list-derived tags and
   one multi-tag event bridging two boundaries; no-history writes affecting
   history-reading commands.
2. Empty initial histories (null boundary), zero/multiple output events,
   changed output tags on redispatch, bounded-cap rejection and idempotent
   duplicate submissions across instances and within one batch.
3. Every higher-level writer in the inventory racing a command; blind rotation,
   registration updates, forgotten keys, projection promotion and sync fold
   failures; any database deadlock retry must restart the full preparation.
4. Process death before/after number assignment and commit; tag-index rows,
   command/event atomicity, no gaps/duplicates, prefix visibility, same-batch
   order, retry-safe cached outcomes and no precommit notification.
5. Cache capacity misses, delayed/reordered notification, listener reconnect,
   replay and snapshot/projection positions. Subscribers must recover from
   durable ordered reads rather than assume postcommit callbacks arrive in
   global commit order.

Use noncommutative/rejecting decisions, not only deposits whose final sum can
hide stale reads. Record seeds and minimized schedules. Multiple instances in
one process exercise independent batchers/pools; also kill real separate
processes to test crash behavior. A benchmark is not this correctness suite.

## Completion ledger

| Issue exit criterion | Result |
|---|---|
| Critical section and append paths | Source inventory above, including low-level caller obligations and non-append fencing participants. |
| Query conflict relation | Defined with counterexamples and finite anchor-coverage check. |
| Candidate contracts/costs/rollout | Compared; shortening the existing gate is the least invasive candidate. |
| Durable benchmark comparison | Open: database startup blocked; no candidate implemented. Harness extended and fail-on-skip/durability mode checked. |
| Stress/property tests | Finite model added; concrete database schedule suite described, not executed. |
| Recommendation / 10x | Defer. No measured support for 10x; reject silent partition/preallocation changes under the current contract. |

Resolve the temporal spec wording independently, then obtain the durable
baseline and a real target workload before authorizing an experimental append
protocol. This issue should not be closed as fully measured on this evidence.

Validation performed for these investigation artifacts:

| Command | Outcome |
|---|---|
| `node --test scripts/disjoint-boundaries.test.mjs` | Passed; finite model only. |
| `cargo test --offline --release -p skilj-core --test command_processing` | All 32 pure command tests passed, no skips. |
| `cargo test --offline --release -p skilj --test command_throughput --no-run` | Extended benchmark compiled. |
| `cargo clippy --offline --release -p skilj --test command_throughput --test coresident_pgbench -- -D warnings` | Passed, including both users of the shared benchmark module. |
| `cargo fmt --all -- --check`, `git diff --check` | Passed. |
| Ordinary ignored throughput run, 160 commands configured | Skipped at database setup; no scenario ran. |
| Durable ignored throughput run, 800 commands configured | Failed as intended at missing-database guard; no scenario ran. |

The workspace-wide database/broker suites were not run. Production Rust modules,
SQL schema and formal invariants are unchanged.
