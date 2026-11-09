# Authorized event reads — issue #173

The SQL selection/count stage is adopted. It removes full event hydration from
counts, and hydrates only bounded candidate pages for browsing. The public event
and command-origin representations are retained for a separately reviewed
breaking stage. See architecture §242 for validation, owner policy, sequence
bounds, count snapshot semantics and cursor progress.

## Reproduce

```sh
DATABASE_URL=postgres://... cargo test --release -p skilj-core \
  --test authorized_event_bench -- --ignored --nocapture
```

`SKILJ_BENCH_EVENTS` defaults to 1,000,000 and `SKILJ_BENCH_RUNS` to 20. The test
support recreates `skilj_authorized_event_bench` on the supplied server. To rerun
only the final queries against the existing fixture:

```sh
SKILJ_BENCH_REUSE=1 SKILJ_BENCH_ONLY=sql \
DATABASE_URL=postgres://.../skilj_authorized_event_bench \
cargo test --release -p skilj-core --test authorized_event_bench -- --ignored --nocapture
psql postgres://.../skilj_authorized_event_bench -f docs/benchmarks/authorized-events/plans.sql
```

The fixture has one million events, ~280-byte payloads, nine tags each (owner
plus eight filter dimensions), two types, 1,000 owners and 1,000 correlation IDs.
One type is sparse (one event per thousand); the other declares an owner key.
Each broad tag matches half the history; the eight-tag union deliberately has
maximal overlap. Pages serve 50 records. Recent-cache starts 500 events from the
head with a 1,000-record event cache. All origins are direct, so these numbers do
not measure the additional savings from avoiding originating-command hydration.
Types have small schemas; no plaintext decryption or rendering is benchmarked.

## Measurements

Local Postgres 17.11 (Alpine container), `shared_buffers=128MB`, `work_mem=4MB`;
client and database on the same host. Release Rust build with a counting system
allocator. The allocator counts requested bytes across the whole client process,
including SQLx/Tokio and reallocations; it is not retained heap size. No other
benchmark runs concurrently with the final SQL-only run. Initial baseline runs
had other validation/build work on the host, so these are local workload probes,
not controlled production throughput or latency guarantees. Other validation
work ran on the host during the initial and `validation-load.log` probes. The
final run follows completion of the full suite and analyzer; the loaded run is
retained separately to show why these small-sample tails need caution.

`run.log` retains the old implementation measurements and the initial SQL
implementation; `final-sql.log` is the revised implementation. Baseline page
measurements and final SQL measurements have 20 samples. Old counts have **one
sample** each because each hydrates a million or half a million events: their
printed p95/p99 are that single observation and are not tail estimates. The
20-sample final p99 is the maximum observation, also too few samples to estimate
production p99. Both initial and final runs return the same result sizes/counts;
the benchmark now asserts equality across repetitions and implementations.

“Cold” and “reused-cache” describe the **application event cache**, not database
or OS buffers. Reused page samples share one cache, initially empty, and include
its first miss. A full-history count cannot fit in the event cache; its single
reused-cache sample does not establish a cache-hit speedup. The recent-cache
page does exercise repeated complete cache hits. Broad tag baselines bypass the
application cache, as the original surfaces do.

| Workload | Old cold ms | Old reused-cache ms | Final median ms | Final p95 / p99 ms | Old cold / final requested bytes |
|---|---:|---:|---:|---:|---:|
| all page | 14.361 | 3.046 | 2.619 | 4.663 / 19.903 | 3,987,970 / 446,141 |
| all count | 10299.814 | 6979.407 | 81.435 | 92.317 / 105.645 | 3,484,812,687 / 15,973 |
| sparse-type page | 1875.109 | 2028.892 | 3.703 | 15.758 / 30.454 | 265,038,883 / 445,713 |
| sparse-type count | 7875.060 | 8031.635 | 0.864 | 1.200 / 1.287 | 3,485,216,180 / 16,233 |
| sparse-owner page | 2813.372 | 2330.400 | 3.725 | 7.369 / 9.882 | 265,419,817 / 449,817 |
| sparse-owner count | 9312.205 | 8627.752 | 4.079 | 9.125 / 13.120 | 3,485,257,260 / 18,456 |
| sparse-correlation page | 2129.741 | 2042.363 | 4.182 | 5.381 / 7.748 | 265,514,219 / 446,042 |
| sparse-correlation count | 9050.243 | 8217.316 | 1.469 | 2.171 / 2.247 | 3,485,479,204 / 16,233 |
| broad-tag page | 2.358 | 2.354 | 2.886 | 4.212 / 6.678 | 420,040 / 451,320 |
| broad-tag count | 5205.706 | 4053.101 | 564.918 | 928.326 / 1774.533 | 1,715,537,026 / 19,031 |
| wide-union page | 4.358 | 3.771 | 5.750 | 11.001 / 11.490 | 439,603 / 484,332 |
| wide-union count | 6532.234 | 6021.060 | 2127.622 | 2750.760 / 9739.509 | 1,725,327,197 / 39,221 |
| recent-cache page | 9.506 | 0.697 | 2.579 | 4.073 / 4.979 | 3,877,654 / 446,176 |

The old full count requested ~3.48 GB cumulatively while the final aggregate
requested ~16 KB. The client process's peak RSS was 13,620 KB in the initial
combined run and 10,340 KB in the final SQL-only run; these are process-wide,
order-dependent observations, not isolated per-request peaks. Candidate vectors
stay page-bounded. Backend aggregate/union memory is separate: broad unions can
spill to disk at the measured 4 MB `work_mem`, as `plans.txt` records.

The tradeoff is the recent fully cached page: median ~0.70 ms becomes ~2.58 ms,
and requested bytes rise from ~336 KB to ~446 KB. This stage adopts the shared
SQL path for its authoritative selection, predictable hydration and large sparse
read/count gains. Broad-tag and union pages also have modest overhead versus
their already indexed baseline. It does not claim all reads become faster.
Command preparation and other cache consumers keep their existing cache paths. A future cache fast
path would need to preserve current policy, bounded selection and identical
progress/snapshot behavior, and should be measured independently.

## Query plans and index decision

`plans.sql`/`plans.txt` record representative final statement shapes with
`EXPLAIN (ANALYZE, BUFFERS)`, including forced custom and generic plans. They
exclude the bounded-context and related-row hydration queries. Type counts use
`events_by_type`; owner queries resolve `event_tag_values` then use
`event_tag_refs_lookup` and event sequence/type indexes. Correlation pages use
the existing correlation index. Tag pages deduplicate sequences and limit each
type/tag range before unioning and hydrating. Overlapping matches never multiply the aggregate.

The first SQL probe exposed two avoidable scans, fixed without changing an
index: a parameterized “empty types OR matching type” clause could choose a
generic whole-context scan, and JSON owner containment scanned stored tags.
Separate SQL shapes for unrestricted/named types and dictionary/reference owner
selection reduced sparse type counts from ~72 ms to ~0.86 ms and sparse owner
pages from ~134 ms to ~3.7 ms. Unlimited tag candidate arrays also made initial
broad/union pages slow; applying **all** predicates before per-tag page limits
restored bounded index work (final broad/union page medians ~2.9/~5.8 ms).

No index is added or replaced in this stage: the observed large gains come from
query shape and avoiding hydration, using indexes already maintained on append.
There is consequently no schema migration, extra index storage or additional
index-maintenance/WAL cost. This does not establish that another index could
never help. In particular, the remaining broad-union count reads four million
references before deduplicating to half a million events; its recorded plan
spills temporary data. An additional ordered correlation index would also need
to justify its storage and append/WAL cost against the small measured page time.
Neither is assumed beneficial from the query shape alone.
