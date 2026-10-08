# Performance and tuning

What limits command throughput in skilj, what has been measured, and the
knobs you can turn. Design background: [docs/architecture.md](architecture.md)
§58-§63.

## The one ceiling that matters

skilj is a DCB event store: every accepted command must be checked against
the events that share its consistency tags, and its events get gapless
sequence numbers. To make that safe, commands for the **same bounded
context** are serialized through one row lock. Different bounded contexts
never contend with each other.

Consequences:

- Throughput is per bounded context, not per process. Splitting unrelated
  tenants or domains into separate bounded contexts is the biggest lever you
  have - a properly multi-tenant deployment mostly sidesteps the lock.
- Concurrent commands to one bounded context are *group-committed*: callers
  queue, one becomes the batch leader, takes the lock once, decides and
  persists every queued command inside it, and commits once. The busier the
  context, the larger the batches, so the lock's cost is amortized.

## Measured numbers

`skilj/tests/command_throughput.rs` is the benchmark (`#[ignore]`d; run it with
`cargo test --release -p skilj --test command_throughput -- --ignored
--nocapture`). It sends 800 commands per scenario through `POST
/v1/commands/trigger` on the in-process router (no network) into one bounded
context, from 1, 8, 32 or 80 concurrent callers, and reads the command
batcher's own `debug` events to report batch size, lock wait and where the
time inside the lock goes. Three workloads: **spread** (every command on its
own account, so `decide()` sees an empty history), **hot** (every command on
one account, whose history grows as the run goes) and **hot-snap** (hot, but
deciding from a snapshot, docs/architecture.md §19).

Measured on a 22-core development machine against the embedded Postgres 18,
shared with other work - the load average moved between 6 and 15 during the
runs, so the absolute rates moved by up to 2x between rounds. Per-command time
inside the lock is the steadier figure. After the fix in docs/architecture.md
§178:

| workload | callers | commands/s | p50 | mean batch | in lock per command |
|---|---|---|---|---|---|
| spread | 1 | 70-230 | 4-15 ms | 1 | ~0.6-1.4 ms (persist 0.4-1 ms) |
| spread | 8 | 410-790 | 9-19 ms | 2-3 | ~0.9-1.7 ms |
| spread | 32 | 580-850 | 36-54 ms | 10-11 | ~0.9-1.4 ms |
| spread | 80 | 480-800 | 95-160 ms | 19-30 | ~0.8-1.7 ms |
| hot | 1 | 36-54 | 15-23 ms | 1 | ~2.8-4.4 ms |
| hot | 8 | ~100 | 70-76 ms | 2 | ~4.8-5 ms |
| hot | 32 | 107-116 | 263-270 ms | 11-12 | ~4.7-5.2 ms |
| hot | 80 | 80-130 | 0.6-1.1 s | 30-32 | ~4.6-8.4 ms |

What the profile says:

- Group commit works: at 80 callers a batch holds 20-40 commands, and the
  final commit drops to a few microseconds per command.
- **Spread** was dominated, before §178, by a re-check query under the lock
  that started at the account's last event instead of where the command's
  read ended - for a new account, at the start of history. It cost 0.8-6.6 ms
  per command and grew with the bounded context's size, even with one caller.
  Now it only runs when something was committed since the read, and covers
  just that: 0 with one caller, 0.2-1 ms under concurrency. What remains is
  `persist` - per-command savepoint, command and event inserts, ~0.5-0.8 ms.
- Before the lock, a bounded context with more events than the event cache
  holds (1000 by default) used to freshen the cache on every command, only
  to miss and read Postgres anyway: 0.5-7 ms per command, growing with
  callers. It now goes straight to Postgres, which raised spread
  throughput by roughly a quarter in one back-to-back run
  (docs/architecture.md §187). The table above predates that.
- **hot-snap**, the hot workload deciding from a `Balance` snapshot
  (§19), reads only the events since the snapshot, usually from the event
  cache (§188): 216-221 commands/s with one caller, 260-340 from 8 callers
  up, against 48-134 for plain hot.
- **Hot** is bounded by the account's history, which every command carries
  into the lock and folds: `decide` takes ~1 ms even with nothing new to
  re-check, and the consistency boundary is computed over the same history
  in `persist`. This grows with the account's event count. The lever is a
  `Snapshot` for such keys (§19), not the batcher.

Older figures, from the `skilj-helpdesk` showcase app (4-minute ramp steps,
commit `0d9c409`): ~45 commands/s at 20 workers, ~58/s at 80, for one shared
bounded context. A different application's commands, a networked client and
another machine - not comparable with the in-process figures above.

## With a network round trip

The figures above talk to Postgres in-process, where a round trip costs
almost nothing, so they hide what each statement under the batch lock
costs a deployment whose database is across a network.
`SKILJ_BENCH_LATENCY_MS` puts a TCP proxy between skilj and Postgres that
adds that round trip, and `SKILJ_BENCH_INSTANCES` runs several instances
on the one bounded context:

```sh
SKILJ_BENCH_LATENCY_MS=1 SKILJ_BENCH_INSTANCES=3 SKILJ_BENCH_COMMANDS=400 \
  cargo test --release -p skilj --test command_throughput -- --ignored --nocapture
```

At +1 ms, 400 commands per scenario, commands/s at 80 callers, two
rounds each, before and after docs/architecture.md §196 (a batch decided
and written as a set):

| workload | before, 1 instance | after, 1 instance | before, 3 instances | after, 3 instances |
|---|---|---|---|---|
| spread | 36-42 | 76-77 | 48 | 166-178 |
| hot | 31 | 77-80 | 31-32 | 105 |
| hot-snap | 35-36 | 64-67 | 33 | 107-112 |

Before, writing a batch cost about 15 ms per command (15 round trips), so
a batch's lock hold grew with its size. After, it is about 11.6 ms per
batch whatever its size. Several instances now add throughput, where
before they mostly split the work into smaller batches queueing for the
same lock. A single caller still takes about 100 ms per command at +1
ms, and most of that is round trips outside the batch lock: the next
thing to measure.

## Async projection catch-up

`skilj-core/tests/projection_catch_up_throughput.rs` (`#[ignore]`d; `cargo test
--release -p skilj-core --test projection_catch_up_throughput -- --ignored
--nocapture`) times how fast async projections catch up on 5000 committed
events. Catch-up folds 100 events per transaction and reads and writes each
projection's keys once per chunk (docs/architecture.md §182). On the same
machine as above:

| projections | events/s (before §182) | events/s |
|---|---|---|
| one, a single key | ~1,340 | ~69,000 |
| one, 500 keys | ~1,270 | ~36,000 |
| four (two of each) | ~470 | ~16,000 |

A projection with `PARTITION_COUNT > 1` uses its own path, which still writes
each key once per event.

## What a co-resident application loses

skilj is meant to share the application's own Postgres (docs/architecture.md
§2.2), so the number for a deployment decision is what the application's own
workload gives up. `skilj/tests/coresident_pgbench.rs` (`#[ignore]`d; `cargo
test --release -p skilj --test coresident_pgbench -- --ignored --nocapture`,
`SKILJ_BENCH_SECONDS` per phase, default 30) runs `pgbench`'s default
TPC-B-like script (`-c 8 -j 4`, scale 10) with its tables in skilj's own
database: twice alone, then next to a running `Skilj` under each load below,
then alone again after `Skilj::shutdown`. Commands are the same `Deposit` as
above, through the in-process router.

Two runs on the same machine and embedded Postgres 18 as above (load average
7-11). Two identical alone runs differed by up to 7% in TPS, so treat
anything smaller as noise:

| next to pgbench | skilj commands/s | pgbench TPS | pgbench p50 | pgbench p99 |
|---|---|---|---|---|
| alone (baseline) | - | ~9,000 | 0.85 ms | 1.4 ms |
| skilj running, no commands | - | 0 to -1% | +0-1% | -0-2% |
| spread, paced at 100/s | 100 | -3 to -5.5% | +1.5-4.5% | +17% |
| spread, paced at 250/s | 250 | -8% | +8-9% | +10-12% |
| spread, 8 callers flat out | ~540 | -18 to -20% | +22-25% | +20-25% |
| spread, 32 callers flat out | ~630 | -29 to -32% | +32-38% | +190-220% |
| hot, 8 callers flat out | ~46 | -13 to -15% | +15-17% | +15-20% |

What it says:

- An idle `Skilj` costs nothing measurable. Its background loops (projection
  catch-up, deadlines, cross-instance listener) don't take a share worth
  noticing from the application.
- A spread command costs the neighbour about as much as 3 pgbench
  transactions, at 100/s and flat out alike: per command, skilj reads the
  command's tag history before the lock, then writes the command, its events
  and their tags. Budget for that: a bounded context taking 250 commands/s
  takes ~8% off an application doing 9,000 TPS on the same server.
- Flat out with 32 callers, the neighbour's p99 roughly triples while
  skilj's own throughput barely moves from 8 callers. More concurrent callers
  add contention, not commands. Size the pool and the callers for the rate
  you need, not for the most the lock can take.
- A hot key is ~10x as expensive per command (~27 pgbench transactions):
  every command reads and folds the key's whole history, which grows. This
  is the case a `Snapshot` (docs/architecture.md §19) is for.

These numbers are for one machine where pgbench and skilj share the CPU as
well as Postgres. On a server where Postgres has its own cores, the CPU part
of the cost goes away and the I/O part (WAL, commits) remains.

## Knobs

All on `SkiljBuilder`:

| method | default | what it does |
|---|---|---|
| `command_batch_max_size(n)` | 256 | Most commands one lock acquisition processes. Larger amortizes more; smaller shortens each lock hold and limits how many commands one failed batch takes down with it. If more than `n` are queued, the same leader keeps processing follow-up batches until the queue is empty. |
| `command_batch_max_concurrent_leaders(n)` | half of the pool's `max_connections` (min 1) | Caps batch leaders running at once across all bounded contexts. Each leader pins one connection for its batch while still needing others for reads; without a cap, many busy bounded contexts can leave the pool full of leaders waiting on each other. |
| `command_batch_idle_in_transaction_timeout(d)` | 30s | Postgres kills a leader whose transaction sits idle *between statements* longer than this, releasing the lock. A backstop for genuinely stuck leaders, not a throughput knob. The lock wait itself is not covered. |
| `pool_options(...)` | sqlx default (10 connections, 30s acquire timeout) | See "Sizing the connection pool" below. `build()` refuses fewer than 2 connections. |
| `statement_cache_capacity(n)` | the database URL's `statement-cache-capacity`, else 100 | Prepared statements each connection keeps. See "Sizing the statement cache" below. |

## Sizing the connection pool

Every `Skilj` instance has one pool. What skilj itself takes from it
(docs/architecture.md §186):

| holder | connections |
|---|---|
| cross-instance listener | 1, for the process's lifetime |
| command batch leaders | one each; at most `command_batch_max_concurrent_leaders`, default half the pool |
| cross-context route ticks | one each; at most half the pool |
| parked-delivery retries | one each; at most half the pool |
| background ticks (projection catch-up, snapshots, deadlines, retention, scheduler) | one per bounded context being worked on, up to 16 per loop |
| GraphQL/REST reads | one per request in flight |

Nothing that holds a lock waits for a second connection, so any pool of 2
or more is correct; a pool of 1 is refused, because the listener takes it
whole. The size decides latency:

- Start from the bounded contexts that take commands at the same time:
  each busy one wants a leader. Give the pool about twice that (leaders
  need their reads too), plus the request concurrency you expect, plus 1
  for the listener. sqlx's default of 10 suits a handful of busy bounded
  contexts.
- Background ticks queue behind requests on the pool. With many bounded
  contexts, a pool well under 16 makes catch-up and deadlines slower,
  not wrong.
- The limit that matters is the database's. Instances times
  `max_connections`, plus everything else connected, must stay under
  Postgres' `max_connections`. Postgres throughput peaks at a small
  multiple of *its* cores, so past that more connections add waiting, not
  work (docs/architecture.md §184: more callers added contention, not
  commands).
  Size from the database server, never from the application host's cores.
- `acquire_timeout` (default 30s) is how long a request waits for a
  connection before failing with "the server's database connections are
  all busy". Lower it to fail fast; it doesn't add capacity.

## Sizing the statement cache

sqlx prepares every statement skilj sends and keeps up to
`statement_cache_capacity` of them per connection, least recently used
out. Each bounded context's SQL names its own schema, so each one brings
its own statements: the command path alone sends 14-20 distinct ones per
bounded context, plus about 15 shared by all. A statement that isn't
cached costs an extra round trip to prepare, and one more to close the
statement it evicts. Measured with `skilj/tests/statement_cache.rs`
(`#[ignore]`d; 1,600 deposits from 8 callers spread over the bounded
contexts, each on its own account, embedded Postgres on the same host,
two rounds; docs/architecture.md §193):

| bounded contexts | capacity 100 (default) | 400 | 1,600 |
|---|---|---|---|
| 1 | 716-883 cmd/s | 732-884 | 760-968 |
| 8 | 977-1,491 | 1,032-1,462 | 1,001-1,474 |
| 32 | 673-994 | 935-1,257 | 897-1,215 |
| 64 | 523-771 | 472-859 | 741-975 |

Up to 8 bounded contexts the default is enough. At 32, 100 cost a quarter
of the throughput; at 64, 400 wasn't enough either. With Postgres on
another host each round trip is longer, so a miss costs more than here.

- **Size it at about 20 per bounded context that takes traffic at the same
  time, plus 20** - more than the command path's 14-20, for the read
  paths (GraphQL queries, projections, subscriptions) that add their own.
- **It costs Postgres memory**: each cached statement took about 18 KB in
  the server process (960 statements, 17.4 MB, after one execution each),
  per connection. 1,000 statements on a 20-connection pool is roughly
  350 MB per skilj instance, on top of Postgres' own `work_mem` budget.
- Set it with `SkiljBuilder::statement_cache_capacity(n)`, or with
  `statement-cache-capacity=n` in the database URL. The builder wins when
  both are set.
- `build()` logs a warning when the capacity is under 10 per active
  bounded context, where the benchmark lost throughput. It can't know
  which bounded contexts are busy together, so a deployment with many
  quiet ones can leave it.

## Deployment settings

Settings outside skilj that change how it performs (docs/architecture.md
§192).

**Postgres.**

- Size the pool from the database server (above). Instances times
  `max_connections`, plus everything else connected, must stay under
  Postgres' own `max_connections`.
- `random_page_cost`: skilj's hot queries don't depend on it. With 500,000
  events, the plans for the tag reads (one tag, several tags, a tag after a
  position, and a tag matching 10% of the events) and for the paged
  per-type read were the same at the default 4 and at 1.1: GIN bitmap
  scans and the primary-key or `events_by_type` index throughout. Lowering
  it on SSD or NVMe storage is the usual advice for other queries, and
  does skilj no harm.

**Kafka producer (`skilj-kafka` outbound).** The producer is yours to
configure; `run_outbound` produces one record at a time and waits for it
before the next (docs/architecture.md §169), so:

- **Set `linger.ms=0`.** librdkafka's default of 5 ms is spent waiting for
  a second record that never comes, on every event. Measured with
  `producer_settings_throughput` (`skilj-kafka/tests/kafka_bridge.rs`,
  `#[ignore]`d; 500 events of ~600 bytes through `produce_once`, local
  broker, two rounds):

  | producer settings | events/s per mapping |
  |---|---|
  | librdkafka defaults (`linger.ms=5`) | 165-168 |
  | `linger.ms=0` | 2,113-2,138 |
  | `enable.idempotence` + `lz4`, `linger.ms=5` | 141-160 |
  | `enable.idempotence` + `lz4` + `linger.ms=0` | 1,802-2,141 |

  To go past one mapping's rate, split the event type across partitioned
  mappings (`OutboundMapping::partition`).
- `enable.idempotence=true` stops librdkafka's own retries from writing a
  record twice. It doesn't cover a record the bridge produces again after
  its acknowledgement to skilj failed, so delivery to Kafka stays
  at-least-once, and consumers must tolerate duplicates.
- `compression.type`: each batch is one record, so compression works one
  payload at a time. `lz4`, `snappy` and `gzip` are built in. `zstd` needs
  rdkafka's `zstd` feature in your own `Cargo.toml`; without it, creating
  the producer fails.

## Observing it

- OpenTelemetry histogram `skilj.command_batch.size` (per bounded context) - the
  lasting instrument. Batches stuck at size 1 under load mean callers are not
  queueing behind the lock; very large ones mean the lock is the bottleneck.
- `RUST_LOG=skilj_core=debug` logs, per batch: `command batch leader lock
  wait`, `command batch committed` (size) and `command batch phase timing`
  (decide / sequence / persist / commit microseconds). Off at the default
  `info` level.

## Failure behavior worth knowing

- A batch whose lock acquisition or final commit fails fails every command in
  it. Followers receive `BatchFailed { code, message }` (HTTP 500) with the
  original error's code; the leader gets the original typed error. Retry is
  safe when you supplied an idempotency key.
- If a client disconnects while its request is the batch leader, commands
  still queued behind it are failed with a retryable `BatchFailed` rather than
  left hanging; the next request elects a fresh leader.
- A single command failing inside a batch (decider error, event-type not
  registered) only fails that command; the rest commit.
