# PostgreSQL, UUIDs and commit batches: issue #158 follow-up

Measured 2026-11-02. This follows the
[initial investigation](disjoint-consistency-boundaries.md). The original
PostgreSQL startup blocker is resolved by using Docker.

The useful compromise is **separate event identity, consistency tokens and
stream positions**. UUIDv7 can identify an event; a tag revision can validate
a decision; `(publication batch, offset)` can identify its immutable place in
a global stream. PostgreSQL WAL can supply actual transaction commit order,
without making every writer wait on a custom sequence row until commit.

There is a working, deliberately restricted PostgreSQL prototype in
[protocol.sql](../scripts/dcb158/protocol.sql) and
[dcb_concurrency_experiment.rs](../skilj/tests/dcb_concurrency_experiment.rs).
It is not a replacement for skilj's production persistence or a new public API.

## Conclusion and the 5× target

**Keep the concurrent writer experimental; do not replace the general append
protocol on these results.** Global ordering need not be abandoned. It can be assigned
after ingest commits, with a different acceptance/publication contract.

The strongest fair comparison in the final independent-key experiment is about
**2.1–2.2× fully published throughput over the grouped serial control**, after
both reuse unchanged decisions. Earlier versions reached about 2.8–3.3×.
The simple serial, one-command-per-commit control is beaten by much more than
5×, but that is not evidence of 5× over skilj's existing group commit.
**A 5× improvement over the tuned general skilj command path is not established.**

Concurrent batching lanes recover hot-key behavior only when the same key is
routed to the same lane. Naive optimistic concurrency has severe hot-key
regressions. The final compromise therefore keeps key-aware batching, database
conflict locks and an ordered commit feed, rather than assuming all commands
benefit from independent transactions.

Changing just numeric identity to UUID does not explain the improvement:
retaining global exclusion with UUID stays slow, while a normal gapped numeric
sequence with concurrent validation performs similarly to UUID. The lever is
**removing the ingest-wide commit gate**, with its semantic and operational costs.

## What “order produced” means

| Meaning | Can preserve it? | How |
|---|---|---|
| Order within one append transaction | Yes | Preserve array/insert order and add an event offset. |
| Order between commands sharing an account | Yes | Hold account conflict locks through commit, maintain revisions, preserve batch-local visibility. |
| One reproducible global stream order | Yes | Persist ordered publication batches; never stream from raw UUID pagination. |
| Actual PostgreSQL transaction commit order | Yes, asynchronously | Decode committed WAL transactions, retain event order within each, then publish in that order. |
| Arbitrary order in which callers started requests or ran business logic | No | Concurrent callers have no unique total “production time”; a store must define a linearization order. |
| Final global stream position in the initial ingest acknowledgement | Changes | Wait for publication, or initially return UUID/request status and obtain its position later. |

A batch counter alone does not solve commit ordering if each concurrent writer
allocates it before committing: it has the same late-commit problem as a
sequence. The counter must belong to the atomic publisher, or the batch must
be identified by a committed WAL position. A publication batch can combine
several complete ingest transactions while retaining their commit order.

PostgreSQL exposes UUIDv7 generation in 18, but its timestamp describes
generation, not transaction commit
([UUID functions](https://www.postgresql.org/docs/18/functions-uuid.html)).
The experiment holds A's older UUID in an uncommitted transaction, commits B's
newer UUID, reads B as the cursor, then commits A. `WHERE id > cursor` returns
nothing and misses A. Logical decoding returns B followed by A. This is tested
against the database, not just a clock simulation.

## Routing by key, value or event type

Use the full **`(bounded context, tag key, tag value)`** for lock identity and
single-key batching affinity. `account=A` and `customer=A` are different tags.
The same tag in another context is also different. Use structured or
length-prefixed encoding before hashing; collisions may add exclusion, never
remove it. This experiment has one context and one implicit `account` tag key,
so its strings represent account values only.

Event type is initially omitted. That is conservative: events on the same tag
but with disjoint types may contend unnecessarily, but matching writes cannot
slip through. Adding event type directly to every lock/routing key is unsafe
for queries that read any type. A future hierarchy can coordinate an all-types
gate with typed tag locks, using shared/intention modes and canonical lock
ordering. Type-only and untagged broad queries need their own covering gates.
Only add that complexity after a measured wrong-type contention workload.

Routing is an optimization, not the consistency mechanism. All commands still
take database locks for their full tag set. Multi-tag events can span lanes;
hashing only one tag cannot establish independence. A batch must acquire the
**union** of its lock set in canonical order before processing its first item.
Per-command lock ordering is insufficient for two batches visiting overlapping
keys in different orders. The two-pool correctness checks exercise that case.

The prototype's FNV hash is a deterministic queue selector. PostgreSQL advisory
locks use separately ordered lock IDs and coordinate independent pools.
No claim is made that local queues coordinate batching across separate service
instances: those instances still need the database conflict protocol.

### Multiple instances and command routing

Start with local key-affine batching and PostgreSQL coordination. This avoids
adding an instance-to-instance command bus merely to obtain the measured local
batching gains. It does mean one hot key's traffic can split across several
instances, so its batches may be smaller and its database lock busier.

If a deployment demonstrates that problem, consistent hashing at an existing
ingress/router can send a single-tag command to a preferred instance. Use the
full context/key/value tuple, not an aggregate identifier invented by the store.
This is affinity, not exclusive correctness ownership: stale routers and
failover targets must still obey database validation and idempotency.

An application forwarding layer is considerably more expensive: membership,
ownership epochs, deadlines, backpressure, forwarding loops, owner crashes,
in-flight retries and idempotent responses become protocol concerns. Commands
reading/writing several tags can name different preferred owners. Choosing one
coordinator does not remove the need for atomic database conflict checks across
all those tags. Requiring exclusive leased ownership would additionally need
fencing so an old instance cannot continue appending after ownership changes.

No distributed ownership protocol is implemented or required by this
experiment. Ingress affinity is a later optimization to measure if local
batching and database coordination prove insufficient. The two-pool tests
exercise cross-instance exclusion, not network partitions or owner failover.

The next lower-risk production experiment is early resolution/caching of exact
interned tag IDs and used-tag discovery while keeping the current ordering
contract. Measure the integrated path before counting its component gains as
command gains. Concurrent ingest with WAL-ordered publication is a separate,
larger opt-in API/design experiment, not a prerequisite for interning.

## Protocols actually implemented

All experiment modes read the same account query, execute the same capped
deposit, and write command/idempotency, event, tag, revision and durable outbox
rows. Events include 512 bytes of padding. Every acknowledged acceptance follows
a durable database commit. The query language is deliberately restricted to OR
over exact account tags, across all event types.

| Mode | Ingest |
|---|---|
| `serial` | Global advisory exclusion, transactional gapless counter, one command per commit. |
| `uuid-global` | Same global exclusion, UUID identity, no numeric allocation. |
| `gapped` | Concurrent tag validation and normal numeric allocation; UUID primary identity retained as a control. |
| `uuid` | One concurrent transaction per command, tag revisions and UUID identity, no global ingest counter. |
| `serial-group` | One queue and synchronous global group commit; preserves input order and reruns invalidated decisions in the batch. |
| `uuid-group` | Eight key-affine queues, concurrent group commits; shared tag locks still exclude overlapping operations. |

The cached-decision variant reads balance and tag revisions in one SQL
snapshot. Under the append locks it checks revisions rather than folding the
same history again. A changed revision forces a fresh decision. Both sides of
the final comparison receive this optimization. This is analogous to work
skilj already does optimistically; it is not a newly discovered production
optimization worth counting a second time.

Idempotency locks precede context/tag locks. Actual lock IDs are sorted, not
merely the unhashed tag strings. Guard initialization, version check, command,
event, index and outbox changes commit or roll back together. The PL/pgSQL
functions execute the lock and subsequent validation as distinct SQL steps;
a single CTE using a snapshot taken before a lock wait is not a substitute.

Two publication strategies are implemented:

- **Dependency publisher:** scans committed outbox entries, publishes only
  events whose per-tag predecessors are published, and chooses a deterministic
  order among independent events. It does not promise wall-clock commit order.
- **WAL publisher:** peeks complete committed changes, retains event order
  within each transaction, persists an immutable batch/offset mapping, and only
  then acknowledges the replication slot. Replayed UUIDs keep their original
  positions. This preserves actual database commit order.

The WAL prototype uses `test_decoding` via SQL for inspectability. Production
should use a structured replication protocol such as `pgoutput`, filter the
publication to relevant tables, and implement reconnect/failover handling. This
experiment decodes other schema writes too, including the publisher's own
writes, and uses a second pass to acknowledge them. That adds overhead.

PostgreSQL documents that logical slots can replay recent changes after a
crash and that unconsumed slots retain WAL. Persist-before-acknowledge and
deduplication are required; so are retention limits and a recovery policy
([logical decoding concepts](https://www.postgresql.org/docs/18/logicaldecoding-explanation.html)).
LSNs also need a database/epoch identity across restoration or failover; they
are not globally portable UUID replacements.

## Tag hashing and interning

Exact interning is a worthwhile separate optimization for reused tags:
`(context, tag key, tag value) -> BIGINT tag_id`, then index event references by
`(tag_id, event type, position)`. Keep original strings in the dictionary.
Hashing for lock/routing selection is different: collisions can conservatively
add contention there, but a hash used as exact identity needs collision
verification against the original tuple.

The read-side tags are known before history loading and `decide()`: derive them
from command/query mappings and the payload, resolve IDs once, and reuse them
through the read and validation phases. Cache mappings and batch misses. Output
tags depend on emitted event payloads, so resolve those after the optimistic
decision and revalidate/re-resolve if a redispatch changes them. A blind-index
generation change can invalidate derived tags; cached IDs must be scoped to
the context identity/epoch and relevant generation. Resolution moves outside
the append critical section but a miss still costs end-to-end time.

An interned ID also gives an empty-history tag a concrete guard row to lock.
That can replace hashed logical locks with exact guard-row locks in a later
protocol. Keep the dictionary itself read-mostly; a separate revision/guard row
avoids making dictionary readers participate in every event write. Multi-tag
batches still need canonical acquisition of the union of those row locks.

### Component measurement

`tag_interning_experiment` compares the existing text layout with an integer-ID
layout: 200,000 references, 36-character values, one tag key and three event
types, three rounds with alternating seed order. It measures heap and indexes,
including the entire dictionary and its indexes. Each read workload does 10,000
server-side index probes for the latest ten events of one type. Checksums assert
that all three lookup paths return the same positions. These are warm component
measurements, not command throughput or concurrent registration tests.

| Distinct tags | Raw total | Interned total, including dictionary | Raw probes | Interned, resolve each probe | Already-resolved IDs |
|---:|---:|---:|---:|---:|---:|
| 1,000 (200 references/tag) | 43.46 MB | 24.10 MB | 233 ms | 192 ms | 137 ms |
| 200,000 (one reference/tag) | 40.99 MB | 62.80 MB | 77 ms | 105 ms | 55 ms |

For reused tags this saves **45%** of total layout storage, with about 1.2× probe
throughput including resolution or 1.7× when IDs are already known in the final
run. Repeated prepared-input runs put the cached-ID gain between approximately
1.27× and 1.70×, so do not treat the last ratio as universal. Dictionary plus
reference seeding took median 1.12 s versus raw 1.13 s. With all-new tags,
total storage rises **53%**, unresolved probes slow down, and dictionary plus
reference seeding takes 1.65 s versus raw 0.83 s. Already-known IDs remain fast,
but they must first be registered; a real cache lookup is not measured by that
path. Both tag strings and IDs are prepared before timing, and all probe
functions receive the same arrays to equalize transfer cost. Registration was
batched, not one extra network call per new tag. The initial exploratory log
constructed text inputs inside the probe and is not the final fair comparison.

This supports an opt-in normalization/cache change, not adding the probe ratios
to the command benchmark ratios. Index residency, tag length/reuse and dictionary
hit rate determine the benefit. The main concurrency prototype is still using
text tags; it has not been benchmarked with interning integrated.

Use **BIGINT/i64 allocated IDs**, not i128, initially. Its positive range exceeds
9 quintillion entries; it points to arbitrary tag contents rather than encoding
their information. PostgreSQL does not have a native fixed-width signed i128
integer ([numeric types](https://www.postgresql.org/docs/18/datatype-numeric.html));
UUID is a native 128-bit option if decentralized ID allocation is a
separate requirement. A hash-derived ID has different collision requirements
from a dictionary-allocated number. Identity allocation gaps are harmless here:
tag IDs are identities, not stream positions.

Registration must not update a hot dictionary row merely to return its ID.
Batch missing registrations and use a cache; on a concurrent insert race, retry
the lookup with a fresh database snapshot. A one-statement `INSERT ... ON
CONFLICT DO NOTHING` plus a SELECT from its original snapshot can miss a row
whose competing insert it waited for. Do not silently reuse dictionary IDs after
deletion. Explicit null-valued tags also need an exact dictionary representation
(for example, [uniqueness with NULLS NOT DISTINCT](https://www.postgresql.org/docs/18/indexes-unique.html)),
not accidental omission.

Fixture: [tags.sql](../scripts/dcb158/tags.sql). Raw output:
[tag-interning-search-indexed.log](benchmarks/dcb158-2026-11-02/tag-interning-search-indexed.log),
with the final summary in `tag-summary.json`.
Run separately, so it cannot interfere with the protocol benchmark:

```sh
cargo test --release -p skilj --test dcb_concurrency_experiment \
  tag_interning_experiment -- --ignored --nocapture
```

### Searching registered and actually used tags

`suggest_tags(key, literal_prefix, limit, only_used)` is included in the SQL
fixture. It reuses the dictionary's exact `(key, value)` B-tree and the
reference index on tag ID. It distinguishes registration from actual use:
an ID resolved for a rejected command can exist without an event reference.
Default `only_used=true` excludes it, without maintaining a hot per-tag usage
counter. The search must remain within the caller's authorized context/scope.

With 200,003 registrations, 200 parameterized prefix searches, twenty results
maximum, and **forced generic prepared plans**, client-observed p50 was
**0.272 ms** and p99 **0.420 ms**. The recorded sample server plan took 0.105 ms
and made twenty index-only reference checks, not a full reference-table scan.
These are warm local measurements; many unused registrations within a prefix
can still require extra candidate checks. A transactional first-used marker
could help that case without introducing a counter update on every event.

Two planner pitfalls were found and corrected in the experiment. A generic
`LIKE` plan can lose useful prefix bounds. Explicit UTF-8 prefix ranges under C
collation retain them, including empty/max-codepoint cases and literal `%`/`_`.
More importantly, PostgreSQL transformed the usage `EXISTS` into a hashed
subplan scanning 200,000 references. Preventing that transformation with a
correlated `OFFSET 0` keeps indexed existence checks. Before that correction,
prepared search p50 was about 33 ms and p99 51 ms despite a literal EXPLAIN
showing a fast plan. The regression check now forces the generic plan and
rejects hashed subplans. Substring/fuzzy search is not implemented; it needs a
separate index and workload decision.

## Measurements

Docker image `postgres:18`, PostgreSQL 18.6, ext4-backed data volume, fsync,
synchronous_commit and full_page_writes all on. Logical runs use
`wal_level=logical`. The Docker image digest, filesystem and settings are
[recorded with the results](benchmarks/dcb158-2026-11-02/environment.txt).
The host is WSL2 on an Intel Core Ultra 9 185H, 22 logical CPUs. Disk flush
latency varied noticeably during the investigation. These are local-machine
results, not a capacity commitment for another deployment.

### Existing application baseline

The extended `command_throughput` harness completed two rounds, 800 commands per
scenario, through the actual in-process REST router. The following ranges are
the measured command rates; they are not prototype comparisons:

| Workload | 8 callers | 32 callers | 80 callers |
|---|---:|---:|---:|
| Fresh independent accounts | 610–700 | 1,328–1,467 | 1,544–1,711 |
| Reused independent accounts | 415–435 | 914–946 | 1,205–1,220 |
| One hot account, full history | 109–110 | 128–132 | 143–144 |
| One hot account, snapshot | 410–434 | 450–462 | 451–524 |
| No-history deposit | 854–890 | 2,330–2,363 | 2,279–2,545 |
| Two-tag ring | 198–212 | 497–577 | 803–911 |

Raw output: [application-baseline.log](benchmarks/dcb158-2026-11-02/application-baseline.log).
This run preceded enabling logical WAL, so it is not a controlled isolation of
that setting's cost. Snapshots/no-history queries remain strong existing options.

### Strict commit-order publication, 1,600 commands, three rounds

Both sides reuse unchanged decisions. The table gives median fully published
events/s and median observed p99 streaming lag after ingest acknowledgement.
The publisher's durable writes and final drain are included in throughput.

| Callers | Mode | Published/s, median (range) | Ratio to grouped control | Stream lag p99 |
|---:|---|---:|---:|---:|
| 32 | grouped serial | 1,593 (1,519–1,625) | 1.00 | 28 ms |
| 32 | concurrent UUID | 3,485 (3,210–3,525) | 2.17 | 54 ms |
| 32 | concurrent UUID groups, worker routing | 2,574 (2,517–2,582) | 1.59 | 42 ms |
| 80 | grouped serial | 2,244 (2,140–2,290) | 1.00 | 33 ms |
| 80 | concurrent UUID | 4,802 (4,313–4,852) | 2.12 | 108 ms |
| 80 | concurrent UUID groups, worker routing | 4,202 (4,170–4,318) | 1.92 | 54 ms |

Ratios are the median of paired-round ratios, not a ratio of independently
selected best runs. Raw output:
[wal-cached-independent.log](benchmarks/dcb158-2026-11-02/wal-cached-independent.log).
The subsequent key-affine run below is the preferred batching policy.

### Final key-affine policy: independent keys and conflicts

Three rounds of 400 commands, 80 callers, same cached decisions and strict
commit-order publication:

| Workload | Grouped serial published/s | Concurrent UUID published/s | Key-affine UUID groups published/s |
|---|---:|---:|---:|
| Independent | 2,126 | 3,486 | 3,629 |
| Same hot key | 1,174 | 54 | 1,200 |
| Overlapping two-tag ring | 1,274 | 2,267 | 1,307 |

At the hot key, naive concurrency incurred tens of thousands of retries per
400-command round and p99 acknowledgement latency around seven seconds.
Key-affine grouping removed the client retry storm in this SQL decision model
and restored approximately the grouped control's throughput. Internal batch
redecisions are not counted in the reported client retry column.

At 80 independent callers, key-affine groups have median acknowledgement p99
24 ms and stream-lag p99 28 ms; the grouped control has 38 ms and 15 ms.
On the ring, batching brings much less throughput gain and can worsen latency.
No single strategy dominates every workload.

Raw output: [wal-key-routing.log](benchmarks/dcb158-2026-11-02/wal-key-routing.log).
Earlier exploratory runs, including UUID-with-global-lock and gapped-number
controls, are retained in the same directory. They precede some prototype
optimizations and must not be pooled as repeated measurements of the final code.

### Linger

The experiment accepts `SKILJ_EXPERIMENT_LINGER_US` and
`SKILJ_EXPERIMENT_BATCH_MAX` (1–256). A deadline starts with the first queued
request. A full batch dispatches immediately; an empty queue may wait only until
that deadline. Zero preserves immediate adaptive batching. This is a scheduling
target rather than a real-time latency guarantee.

The linger comparison and sparse-traffic results are recorded with the final
artifacts; keep zero as the default unless the measured workload justifies a
latency/throughput trade-off.

Measured with eight key-affine lanes, three rounds, maximum batch 256:

| Linger | Independent, 8 callers: published/s | Independent, 80 callers: published/s | Sparse caller: ack p50 / p99 |
|---:|---:|---:|---:|
| 0 | 1,173 | 3,293 | 7.2 / 10.5 ms |
| 100 µs | 992 | 3,187 | 8.4 / 16.0 ms |
| 500 µs | 897 | 2,533 | 8.7 / 11.3 ms |
| 1 ms | 928 | 2,834 | 8.8 / 11.6 ms |

Eight callers across eight lanes often leave one request per lane, so waiting
cannot combine requests from the other lanes. At 80 callers, mean batches were
about five without linger and 6.25 with 1 ms, but that did not improve total
published throughput. Sparse traffic used one caller and 20 ms think time;
every batch remained size one. The waits did not behave as precise submillisecond
latency additions, so tune from measured end-to-end delay rather than the timer
parameter alone. Disk variance was large (especially one 500 µs round); these
results support a **configurable setting defaulting to zero**, not a universal
claim that linger can never help.

Raw files are `linger-*.log` and `sparse-linger-*.log` in the artifact directory.
Linger should also be bounded by bytes, request deadlines and queue limits in a
production implementation; this fixed-payload experiment bounds command count.

## Limits of the evidence

The prototype comparison is between equivalent **restricted storage kernels**.
It is not a 2×, 3× or 5× benchmark of a complete replacement SkilJ application.
It omits REST authorization, general plugin dispatch, schema registration,
encryption, all writer classes and synchronous projections. Conversely, it
writes a 512-byte payload and an outbox that the application baseline does not.
Its grouped SQL loops per command; production skilj already writes batches as
sets. Comparing their absolute rates does not isolate UUIDs or lock design.

The capped deposit's sum is commutative. Its successful stress runs do not prove
arbitrary noncommutative folds or `latest` across several independent keys.
For a general command, its history order must agree with replay order. Possible
solutions are reading a published prefix and retrying/waiting when relevant
unpublished events exist, or restricting an opt-in API to single-key or
commutative decisions. Simply folding raw UUID-sorted history while replaying
in WAL order is not a valid general solution.

The dependency publisher also needs edges from **read dependencies**, not only
write tags, when those sets differ. This experiment uses the same OR tag set
for both and therefore does not solve arbitrary directional query overlap.
Typed predicates, AND items, snapshots, key rotation, forgotten encryption
keys, no-event decisions and projection promotion still need a full protocol.

Database correctness checks cover two independent pools, same-key uniqueness
under a cap, overlapping multi-tag writes, idempotency, rollback, reversed
multi-command batch lock order, publication dependency order, UUID cursor loss,
actual decoded commit order and idempotent publication replay. They do not
simulate power loss or replication failover. An early smoke run exposed a
publisher drain race: an empty read begun before the last ingest commit was
incorrectly treated as final. The corrected barrier samples writer completion
first and verifies there is no unpublished outbox entry before stopping.

## What other DCB stores teach us

| Implementation / inspected version | Mechanism | Lesson for PostgreSQL skilj |
|---|---|---|
| [UmaDB features](https://umadb.io/key-features.html), [benchmarks](https://umadb.io/benchmarks.html) | Copy-on-write MVCC, atomic publication, queued request batching. | High performance need not mean concurrent globally ordered commits. Keep readers cheap and amortize durable writes. Its reported rates use different hardware/workloads and are not our benchmark. |
| [Tephra architecture](https://tephra.tqwewe.com/architecture/), docs at `b5d2d96` | One write coordinator, staged same-batch checks, tag-tip fast rejects, append-only log and commit markers. | Make the serialized part cheap. A recent-tag revision summary can avoid history scans; exact checks remain necessary on uncertain matches. Its same-batch check deliberately permits false conflicts. |
| [Python eventsourcing PostgreSQL](https://github.com/pyeventsourcing/eventsourcing/blob/bf665955d9243e728aaaa5bb62ac63b9cfe5ec19/eventsourcing/dcb/postgres_tt.py) | B-tree tag table, prepared PL/pgSQL operations, EXCLUSIVE table lock for conditional append. | SQL placement and round-trip reduction matter even with serialization. Do not infer lock-free writes from good server-side timings. |
| [Disintegrate](https://github.com/disintegrate-es/disintegrate/blob/33f12a0cd8a0650cfbd1f4336a58415b9bafa6b0/disintegrate-postgres/src/event_store.rs), epoch SQL in same directory | SERIALIZABLE conditional append, numeric positions and in-flight-writer epoch fencing. | Gaps/out-of-order commits need a safe read horizon. A long in-flight writer can hold that horizon back even when other keys progress. |
| [Sliceworkz](https://github.com/sliceworkz/eventstore/blob/eb4222fc9e6ebe2b64dbdf1aa092086a7e50a47c/sliceworkz-eventstore-infra-postgres/src/main/java/org/sliceworkz/eventstore/infra/postgres/PostgresEventStorageImpl.java) | Stream-scoped advisory exclusion for conditional append; comments explicitly reject hashing arbitrary whole predicates. | Different predicates can overlap. Route/group by keys, but retain a covering conflict protocol. Its unconditional path and stream semantics differ; this is not a full correctness audit. |
| [Axoniq PostgreSQL operations](https://docs.axoniq.io/axon-framework-reference/5.4/tuning/rdbms-tuning/) | Normalized tags, monotonic sequence and bounded `consistency_tags` structure. | A compact conflict summary is a relevant direction. The schema documentation alone is not enough to adopt its complete lock protocol or claim a measured speedup. |

The common theme is batching, cheap conflict checks, efficient indexes and a
well-defined read horizon. None supplies evidence that time UUIDs alone make
DCB coordination unnecessary. A different database may reduce storage-engine
overhead, but this investigation does not justify a database migration yet.

## Rollout and remaining decisions

An opt-in production design would need:

1. Opaque UUID event identity, an opaque query read token, and an independently
   versioned stream cursor. An append may return a receipt before publication,
   with an optional wait-for-position operation for read-your-write streaming.
2. A complete query-aware locking/validation protocol covering every writer and
   mutation barrier. Old instances cannot bypass it during a rolling upgrade.
3. General command histories consistent with publication order, especially
   `latest` and noncommutative folds. Synchronous projections cannot quietly
   retain a global caught-up row that recreates the ingest bottleneck.
4. Durable publisher leadership, deduplication, slot retention/backpressure,
   exported-snapshot bootstrap and failover/restore epochs. Normal reads and
   indefinite replay must not depend on WAL being retained forever.
5. Migration of old sequence IDs, boundaries, idempotency results, projection
   positions, snapshot cuts and consumer cursors. Establish a cutover barrier,
   translate the existing log in order, and version the API/spec explicitly.

Revise `SequenceIsGaplessPerBoundedContext` to describe the **published** order,
not accepted raw UUID rows, if choosing this architecture. Fix the temporal
scope of `DynamicConsistencyBoundaryHonoured` identified in the first report.
Neither invariant was edited silently as part of the experiment.

## Reproduction

Use an isolated server: the test harness recreates `skilj_dcb158_experiment`.
Docker setup (the investigation used an ext4-backed Docker volume, not tmpfs):

```sh
docker run -d --name skilj-158-postgres \
  -e POSTGRES_USER=skilj158 -e POSTGRES_PASSWORD=skilj158 -e POSTGRES_DB=postgres \
  -p 127.0.0.1:55458:5432 postgres:18 \
  -c fsync=on -c synchronous_commit=on -c full_page_writes=on \
  -c wal_level=logical -c max_connections=120
```

Set `DATABASE_URL=postgres://skilj158:skilj158@127.0.0.1:55458/postgres`, then:

```sh
SKILJ_EXPERIMENT_COMMANDS=400 SKILJ_EXPERIMENT_ROUNDS=3 \
SKILJ_EXPERIMENT_MODES=serial-group,uuid,uuid-group SKILJ_EXPERIMENT_WORKERS=8,32,80 \
SKILJ_EXPERIMENT_ORDER=wal SKILJ_EXPERIMENT_CACHED_DECISION=1 \
  cargo test --release -p skilj --test dcb_concurrency_experiment dcb_concurrency_experiment -- --ignored --nocapture
```

`SKILJ_EXPERIMENT_WORKLOAD=Independent`, `Hot` or `Cross` selects one workload;
`SKILJ_EXPERIMENT_LANES` selects the number of concurrent batching lanes.
`SKILJ_EXPERIMENT_PAUSE_US` adds think time before each request for sparse-load
measurements; that time is excluded from request latency but included in rate.
Without `SKILJ_EXPERIMENT_ORDER=wal`, use the dependency publisher.

`node scripts/dcb158/summarize.mjs [--json] <logs...>` summarizes complete,
non-skipped runs and paired ratios. All logs must come from sequential runs;
do not launch tests that recreate the same database concurrently.

`node scripts/dcb158/summarize-tags.mjs <tag-log>` summarizes the tag component
and search results. `final-sources.sha256` identifies the final experiment
sources; earlier logs explicitly capture intermediate variants.

## Subsequent production implementation

The implementation following these experiments interns the exact `(key, value)`
identity in a per-context bigint dictionary. Event references carry that ID;
event types remain in the reference index. This uses exact equality, without
hash collisions or a need for 128-bit IDs. Existing event JSON remains the
source of truth. Migration backfills atomically and leaves `event_tags` as a
compatibility view. PostgreSQL 15 or later is required.

Reads resolve dictionary IDs within their SQL snapshot before scanning event
references. There is no persistent ID cache: erasure can remove an identity,
and recreating it must not leave readers using its previous ID. Registration
and references commit together. Production keeps its gapless sequence and
existing batching; UUID ingestion, WAL publication and routing remain isolated
experiments. No production linger setting was added.

Used-tag autocomplete is available through GraphQL `searchUsedTags` (admin
access mapping) and REST `GET /v1/events/tags?key=...&prefix=...&limit=20`
(event-read token). It returns at most 100 distinct, actually referenced tags,
with literal prefix matching and the caller's event-type/owner scope.

The integrated durable application run is preserved in
[application-interned-tags.log](benchmarks/dcb158-2026-11-02/application-interned-tags.log).
It does **not** establish a command-throughput improvement: for example, the
first 80-worker spread round measured 1,481 commands/s versus 1,711 in the
earlier baseline, while reused independent keys measured 1,167 versus 1,205.
These sequential runs are not a controlled repeated A/B trial. Dictionary
registration adds work, especially for fresh tags; smaller repeated-tag indexes
and searchable identities are the demonstrated benefits, not a blanket speedup.
The component storage results above also show the substantial fresh-tag penalty.

### Further blind-tag optimizations to measure

`BlindIndex::blind` currently derives constraint HMAC keys on each call, including
each previous rotation generation. Caching derived keys by context, tag key and
generation could reduce CPU without changing stored hashes. Cache key material,
not normalized plaintext or plaintext-to-hash mappings; keep rotation validation.
This has not been implemented or benchmarked here.

Dictionary IDs could also remain stable during rotation while their hash values
change, avoiding the current reference-ID rewrite. That requires an atomic
collision-safe dictionary update and rotation/erasure concurrency tests. Event
JSON, command tags and snapshot identities still require migration. Finally,
binary 32-byte HMAC values could replace 64-character hex internally, but need a
format migration and an end-to-end measurement. None of these proposed changes
justifies truncating hashes or replacing keyed hashing with a general fast hash.
Prefix discovery over stored hashes does not provide plaintext autocomplete.

## Validation completed

- The full application throughput benchmark completed on Docker PostgreSQL
  without skips. Multiple three-round protocol experiments and the final
  two-pool correctness/smoke run passed with durable settings.
- The final tag-layout/search run passed, including equal result checksums,
  literal prefixes, unused registrations, Unicode prefix bounds and forced
  generic-plan checking. All replication slots were removed after use.
- Targeted release Clippy with warnings denied passed for the experiment,
  throughput benchmark and the other shared-fixture consumer.
- Formatting, diff checks and the finite protocol model passed. All 23 CI/CGA
  script regression tests passed outside the sandbox; the sandbox had suppressed
  child Node CLI output on the first attempt. CI scripts were not changed.
- Those experiment checks preceded the production changes described above.
  The production changes additionally have migration, rollback, scope, null-tag,
  blind-tag erasure/rotation and API conformance coverage. Full workspace service
  suites, power-loss recovery and replication failover were not run. The general
  DCB invariant and append protocol remain unchanged.
- Workspace Clippy and rustdoc with warnings denied passed in the Docker build
  environment. PostgreSQL production suites passed: 14 core consistency/migration
  tests, 13 blind-tag tests, API conformance and four federation tests. Both
  federation composers passed; the spec remains at zero errors and 14 warnings.
  All 30 protocol-model and CI script tests passed. The ignored history-read
  benchmark was not rerun. CGA's feature baseline update is explicitly explained
  in `scripts/cga/README.md`; no whitelist or analysis-gap expansion was added.
