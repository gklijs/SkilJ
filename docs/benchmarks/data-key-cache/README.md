# Data-key cache eviction and concurrent unwraps (#175)

Reproduce the comparison from the workspace root:

```sh
cargo test --release -p skilj-core --lib cache_workload_measurement -- --ignored --nocapture
```

The experiment is in `skilj-core/src/encryption/service/tests.rs`. It compares
an imitation of the previous service path (provider lookup, stored-wrap copy,
SHA-256 digest, mutex-protected cache lookup and full clearing on overflow)
with the production `KeyService`. Both use the same in-memory encryption
provider with a 1 ms delay per unwrap. Tokio timer scheduling can make this
closer to 2 ms. It measures the service only, excluding SQL and command work;
this is not a production throughput measurement. It runs in the optimized release test
profile on a current-thread runtime.

For each policy it preloads the working set, resets the provider call counter,
and measures 2,048 reads chosen by a fixed pseudo-random generator. Retention
is 128 keys, with working sets below, at, just above, and twice capacity.
The 256-request burst starts cold and polls all requests concurrently.
`scalar` uses the previous service path and `shared` uses production. Raw
output is in [run.log](run.log); [build-run.log](build-run.log) records an
additional release run. The timed binary run uses `/usr/bin/time -v` after
compilation; [process-memory.log](process-memory.log) records 9,704 KiB peak
RSS for the whole comparison process, including both policies and the runtime.

## Observations

One timed release run, with latency in microseconds (`clear` → `clock`):

| Working set | Provider calls | Hit rate | p95 | p99 | Shallow retained bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 0 → 0 | 100.00% → 100.00% | 0.410 → 1.017 | 0.760 → 3.485 | 10,112 → 12,024 |
| 128 | 0 → 0 | 100.00% → 100.00% | 0.496 → 0.980 | 1.110 → 1.198 | 20,224 → 23,928 |
| 129 | 375 → 8 | 81.69% → 99.61% | 2,355.103 → 0.704 | 3,511.448 → 2.422 | 19,968 → 23,928 |
| 256 | 1,492 → 1,030 | 27.15% → 49.71% | 2,199.782 → 2,168.681 | 2,375.600 → 2,226.037 | 18,816 → 22,696 |

The identical-miss burst makes 256 → 1 provider calls; measured p95/p99 are
2,380/2,384 → 2,220/2,222 us. Elapsed burst time is 2,532 → 2,316 us. The
provider-call reductions repeat exactly across runs; latency is sensitive to
scheduling and shared-machine load. In the additional release run, burst
elapsed time was 3,172 → 7,181 us, despite the same provider-call counts.
These short samples do not demonstrate a consistent warm-hit or tail-latency
improvement. They demonstrate the removal of full-cache eviction and duplicate
provider calls, with roughly 18% more shallow storage at a full 128-key cache.
At twice capacity, half the reads still miss, so both policies' tails include
provider waits. Real remote providers, concurrent command work, larger working
sets, and cross-process invalidation remain outside this experiment.

## Policy and synchronization costs

Exact LRU would update ordering on every hit, requiring list relinking (and
an indexed node structure or another dependency); an ordered recency map
instead adds logarithmic updates. Clock uses the existing mutex and one
reference-bit write per hit, with a digest queue only touched on insertion.
FIFO has simpler eviction but gives frequently read keys no second chance.
Clock was chosen to keep the hit path small without discarding read recency.
No new dependency is needed.

An insertion evicts at most one live key. If all entries are referenced,
it can scan one full clock revolution plus one slot under the mutex, so
worst-case insertion still costs O(capacity). Reads and ordinary insertions
are O(1) on average. Expiry uses insertion time, not last access; expired
slots are reclaimed when reused or encountered by eviction, without building
up duplicate queue entries. An expired lookup drops the key immediately and
retains only its slot metadata. Zero TTL or capacity disables retention.

Each admitted unwrap owns a semaphore permit until completion or cancellation.
The service admits at most 64 provider unwraps at once; duplicate waiters
join before admission. Callers waiting for admission remain caller-owned
futures, so this is a bound on provider work, not arbitrary application task
memory. A shared future is driven by its waiters without spawning a detached
task. Cancelling all waiters drops the provider future and permit; cancelling
one does not cancel the remaining waiters. Completed successes and errors
are removed from the shared lookup immediately; only successful keys are
retained. Cancelled weak entries are reclaimed on the next admission.

`clear_cache` replaces an identity shared by the retained cache and in-flight
work. Old unwraps and wraps cannot insert after a clear; new callers cannot
join the forgotten work. It does not revoke data keys already returned to
callers or invalidate another process's cache. The database row is still
read before calling the service, preserving erasure and rewrap semantics.
Provider names and versions remain part of the digest and provider lookup.
Custom providers need no changes. `encryption::Error` gains an additive
`Clone` implementation so duplicate waiters receive the same error variant
and fields; the provider trait and service signatures stay compatible.

## Memory accounting

`shallow_bytes` multiplies reported `HashMap::capacity` by element size and
adds clock capacity and stored digest bytes for the previous cache. Reported
map capacity can decrease after removals leave tombstones without releasing
the actual allocation, so this underestimates physical bucket storage. It excludes hash control bytes,
allocator rounding, map headers, provider state, shared-future allocations,
runtime and caller task memory. It is a reproducible storage estimate rather
than a heap profiler result. Clock pays for a queue and reference bits while
using inline 32-byte digests instead of individually allocated digest vectors.
All cache and clock entry counts remain bounded by configured capacity;
provider work and shared lookup metadata have the separate 64-operation bound.

## Regression coverage

Unit tests cover capacity including 10,000 entries, expiry, repeated slot
reuse, second chances, disabled retention, identical misses and errors,
retry while an old waiter stalls, cancelled leaders and abandoned operations,
independent keys, admission saturation, and clear/retirement during unwrap,
wrap and admission. Existing encryption, persistence, subject-erasure and
master-key-rotation suites cover authoritative database reads, reprovisioning,
legacy wraps, multiple providers, rewrap and provider identity behavior.
