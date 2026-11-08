# Bounded cache pages (#171)

The cache now applies the type predicate, inclusive upper sequence bound and
page limit to borrowed records before `ContextWindow::serve` copies payloads,
schemas, tags and originating commands. `None` remains a coverage miss;
`Some` is a fully covered window, and a result shorter than the requested
limit means the selected range is exhausted. An exactly full final chunk
still needs the following empty read to establish exhaustion. The returned
window high-water mark is not a completion cursor for a limited result.

The existing freshening, table incarnation/database epoch and current
registration checks run before selection. Every chunk freshens separately;
this preserves live walks rather than introducing a fixed history snapshot.
`up_to` now reaches both cache selection and the two SQL fallback queries,
before hydration. Public signatures and wire responses are unchanged.

## Reproduction

Use a dedicated Postgres database. The example migrates it, creates a uniquely
named bounded context, inserts 10,000 events and removes that context afterward.
It does not include setup/cleanup in measured samples.

```sh
DATABASE_URL=postgres://postgres@localhost:55470/skilj_cache_probe \
  cargo run -p skilj-core --example cache_pages --offline \
  > docs/benchmarks/performance-review-2026-11-08/cache-pages.csv
```

[Source](../../../skilj-core/examples/cache_pages.rs) uses the actual cache,
freshness query and current-registration hydration. The comparison reproduces
the former production order using `try_events_after`, then type selection and
`take`; the new path uses `for_each_event_chunk` and stops after one chunk.
Both discard the result after observation. One event in 100 has the rare type.
Payloads are about 1 KiB and schemas 2 KiB; origins are directly created.

[Recorded results](cache-pages.csv): Rust 1.99.1, normal **debug profile**,
Postgres in the existing local Docker container, single-thread Tokio runtime,
five warmup reads and 100 measured reads per case. A counting system allocator
reports all requested bytes and allocation/reallocation calls during a read,
including SQLx work; these are not retained bytes or peak RSS. p95 uses the
95th sorted sample. Comparisons ran consecutively on a shared development
machine while other checks ran; timings are descriptive, not a stable release
performance target or an HTTP/production throughput claim.

| Window | Page | Types | Requested bytes, previous → bounded | p95 µs, previous → bounded |
| ---: | ---: | --- | ---: | ---: |
| 1,000 | 1 | all | 4,882,428 → 17,300 | 5,022 → 971 |
| 1,000 | 100 | all | 4,882,428 → 535,445 | 6,348 → 1,663 |
| 1,000 | 100 | rare | 4,882,428 → 65,492 | 7,054 → 1,547 |
| 10,000 | 1 | all | 58,387,294 → 17,464 | 44,799 → 716 |
| 10,000 | 100 | all | 58,387,294 → 535,527 | 44,864 → 1,203 |
| 10,000 | 100 | rare | 58,387,294 → 535,032 | 48,093 → 2,005 |

Allocation counts for a one-event page stay at 31 with either window size,
compared with 10,029/100,033 previously. This demonstrates that records beyond
the limit are not deep-copied. The rare 100-of-1,000 request returns just ten
matching events, and allocates for those ten rather than the skipped 990.

## Correctness checks

`skilj-core/tests/event_cache.rs` compares complete events and scan progress
against Postgres with disabled, partially covering and fully covering caches.
It covers empty pages, exact-size final pages, sparse types, after/upper bounds,
multiple chunk sizes, and selection callbacks discarding whole chunks.
Separate regressions cover current owner/private declarations, external writes,
empty/recreated contexts, and an append plus blind-tag erasure committed by a
second task between chunks. Already observed chunks retain their normal live
read semantics; subsequent chunks see erased tags and the newly committed tail.
Existing cache tests cover epoch stamps and contiguous append behavior.

The event-fetch/query, encryption, subject-erasure, blind-tag and REST/GraphQL
conformance suites also passed against real Postgres where needed. Core clippy
and workspace formatting passed. CGA findings are reviewed separately in its
README and baseline. No fixtures or public API changes were needed.
