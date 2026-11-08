# SQLx connection-buffer experiment (issue #169)

This isolates client transport buffers without starting skilj or its caches.
It evaluates a possible reclamation point; it does not select a production
policy. The source is
[`skilj-core/examples/connection_buffers.rs`](../../../skilj-core/examples/connection_buffers.rs).
There are no new dependencies and no changes to runtime defaults.

## Reproduce

Requires Linux (`/proc/self/status`) and a reachable disposable Postgres.
The program only issues `SELECT`s; it does not create or drop databases.
Build before measuring, and avoid running other builds/tests/load concurrently.

```sh
cargo build --release -p skilj-core --example connection_buffers
export DATABASE_URL=postgres://postgres@localhost:5432/postgres
for connections in 1 32; do
  for policy in none boundary every-query; do
    target/release/examples/connection_buffers \
      "$policy" "$connections" 2097152 8 5 \
      > "$policy-$connections.jsonl"
  done
done
```

Arguments are policy, connection count, payload bytes, passes per cycle and
idle seconds. There are always two cycles. Each policy/width is a **fresh
process**, and all pool connections are held for both cycles. Idle timeout,
maximum lifetime and acquire pings are disabled. An `after_connect` counter
asserts that only the configured number of sockets opened; backend PIDs are
checked after each load. Holding the sockets intentionally bypasses pool
release hooks: a reduction cannot be attributed to pool eviction/replacement.

Each cycle concurrently runs eight sequential `SELECT $1::text` echoes per
connection with a shared 2 MiB input string. `fetch_all` drains each query,
the full returned content is checked, and rows drop before shrinking:

- `none`: retain buffers throughout.
- `boundary`: shrink on each connection after its eighth large query, a
  known operation boundary after all its results have been consumed.
- `every-query`: shrink after each large query, to expose repeated growth.

After load, every connection echoes a small payload and checks its backend
identity. Then the program waits five seconds with no queries and samples
again. Finally it releases the connections, closes the pool and samples.
These are genuinely idle cycles, unlike foreground-only idleness while
skilj routes and catch-up continue to work. Five seconds is an observation
window, not evidence of a long-term plateau or a bound on memory.

## Metrics and limits

- `live_requested_bytes`: outstanding sizes requested through Rust's global
  allocator, including SQLx, Tokio, shared input and measurement structures.
  Counts successful allocations/reallocations minus frees. It excludes
  allocator metadata, usable-size rounding, free arenas, stacks and allocations
  bypassing Rust's global allocator. This is not a heap-stack attribution or
  an estimate of Postgres backend memory.
- `rss_kib`: Linux process RSS, sampled separately from live bytes. The
  allocator can retain pages after freeing buffers, so neither metric
  substitutes for the other. Sampling/JSON printing allocates small temporary
  objects; live bytes are sampled before reading `/proc`.
- `requested_bytes_during_load`: cumulative requested bytes, including
  reallocation sizes, during that phase. It measures churn, not simultaneous
  memory usage. The counting allocator adds overhead to every policy.
- Query rate includes draining, full-content checks and policy shrinking.
  Latency includes the same work for each query; for `boundary`, only each
  connection's final query includes shrinking. Nearest-rank p95/p99 are
  diagnostic only: with one connection there are just eight samples, so both
  are the maximum; at 32 connections there are 256. These are short bursts,
  not steady-state capacity measurements or HTTP/request latency.
- Every echo must match and all queries must succeed; an error/assertion
  aborts the experiment. There are no accepted commands, writes, transaction
  cancellations, rollback interleavings, pool contention or background routes.
  This cannot validate shrinking in those paths or estimate command throughput.
- The runtime is single-threaded with concurrent I/O; Postgres shares the
  host. Different allocators, topology, payload shapes and production request
  concurrency can produce different RSS and latency results. Payloads exceed
  skilj's default event-payload limit to model large state/batch transport;
  they do not suggest raising that limit.

## Recorded run, 2026-11-08

Linux/WSL x86_64, glibc 2.43, Rust 1.99.1, SQLx 0.9.0 from this
workspace's lockfile, optimized build, System allocator with counting wrapper.
PostgreSQL 17.11 (Alpine container) was on the same machine via loopback TCP.
No builds or tests ran concurrently with the six sequential probe processes.
The checked-in `*-1.jsonl` and `*-32.jsonl` files contain all snapshots and
the server version. Each process completed both cycles, verified every echo,
opened exactly its configured number of sockets and reported zero replacements.

32 connections, 256 successful large echoes per cycle (MiB = 1,048,576 bytes):

| Policy | Cycle | Idle RSS MiB | Idle live MiB | Queries/s | p95 ms | p99 ms | Load requested MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| none | 1 | 143.63 | 130.42 | 294.1 | 207.48 | 236.99 | 1,835.8 |
| none | 2 | 193.52 | 130.42 | 390.5 | 101.04 | 124.56 | 1,567.5 |
| boundary | 1 | 10.06 | 2.67 | 307.2 | 186.41 | 212.36 | 2,025.3 |
| boundary | 2 | 20.03 | 2.67 | 369.5 | 132.00 | 139.44 | 1,945.3 |
| every-query | 1 | 10.10 | 2.67 | 324.9 | 161.96 | 193.71 | 3,219.6 |
| every-query | 2 | 10.10 | 2.67 | 312.6 | 129.51 | 151.83 | 3,146.5 |

Warm 32-connection RSS was 9.28-9.36 MiB, with 2.65 MiB live including
the shared input. At one connection, warm RSS was 8.89-9.10 MiB and live
bytes 2.07 MiB; the smaller-pool control ended its idle periods as follows:

| Policy | Cycle 1 RSS/live MiB | Cycle 2 RSS/live MiB |
|---|---:|---:|
| none | 15.12 / 6.06 | 15.12 / 6.06 |
| boundary | 9.20 / 2.07 | 13.08 / 2.07 |
| every-query | 12.91 / 2.07 | 12.93 / 2.07 |

The 32-connection retained case's excess live bytes scaled roughly with
pool width: about 128 MiB versus 4 MiB at one connection. No skilj event
cache was involved. Boundary shrinking returned live bytes close to warm
levels in both cycles, but RSS differed despite nearly identical live bytes.
Neither result is a bound on future memory.

The subsequent small reads matter. Immediately after boundary shrinking on
32 connections, live allocations were still 130.64/130.63 MiB in cycles
1/2, falling to 2.67 MiB after the small queries. In the retained case,
they fell from 258.39/258.38 to 130.42 MiB even without shrinking. Thus a
post-shrink snapshot alone mischaracterizes what stays referenced by the
receive/protocol machinery. The probe separates these stages explicitly.

Every-query shrinking requested about twice as many allocation bytes as
retention during the second 32-connection cycle and achieved 312.6 versus
390.5 queries/s; boundary shrinking requested 24% more and achieved 369.5.
First-cycle rates and tails varied substantially and did not rank policies
the same way. These short, fixed-order bursts are evidence of a
memory/reallocation tradeoff, not a statistically established performance
ranking. Repeat and lengthen the workload on the deployment hardware before
using rates or tail latencies for a policy decision.

## Decision

Keep existing runtime defaults. The public `Connection::shrink_buffers()`
and pool hooks already permit caller experiments; no new API is needed to
demonstrate this effect. An idle-time `before_acquire` example is in
[`docs/performance.md`](../../performance.md#opt-in-hook-experiment) and
compiled by the builder's rustdoc. It does not fire while an unused pool
sits idle, and background polling can prevent its threshold being reached.

Before introducing a production policy, compare at least two full application
load/idle cycles with and without it, measuring accepted commands/s, p95/p99
request latency, attributed live allocations, RSS and connection replacements.
Include large command batches, projection catch-up and ongoing route traffic;
verify transactions, cancellation, partial streams and rollback. A useful
operation-aware experiment would act on the actual large-operation connection
only after results and transaction cleanup finish, with a measured cadence.
SQLx 0.9 has no public capacity getter, so a byte-threshold pool hook cannot
simply inspect its buffers. Retiring sockets instead also loses prepared
statements and must account for pool churn. Current transport-only evidence
does not justify adding any default cost to every query or release.
