# Potential Performance Optimizations

Based on kafgres learnings, skilj codebase analysis, and patterns from other Rust event-sourcing/Kafka projects.

---

## 1. EventCache: Lock-Free Reads + Tag Index

**Location**: `skilj-core/src/event_cache.rs:336` (linear filter), `RwLock` contention

| Advantages | Downsides |
|------------|-----------|
| Eliminates `RwLock` read contention under high concurrent reads | Increased memory: secondary index `HashMap<TagKey, Vec<usize>>` per context |
| Tag lookups O(1) vs O(n) filter scan | Cache invalidation complexity on `append`/`freshen` |
| Lock-free reads via seqlock/epoch pattern | More complex correctness reasoning (ABA problems) |
| Matches kafgres "storage engine split" philosophy | Migration risk: subtle bugs in multi-instance freshness |

**Effort**: Medium (2-3 days)

---

## 2. Pipeline Batch Commit Stages

**Location**: `skilj-core/src/db/mod.rs:8353` `commit_command_batch`

| Advantages | Downsides |
|------------|-----------|
| Reduces lock hold time ~40% (decide/sequence/persist overlap) | Breaks `extra_committed_events` visibility guarantee (same-batch DCB) |
| Decide phase parallelizable across commands | Requires two-phase sequence allocation (reserve → confirm) |
| Sequence pool draws batched, fewer round trips | More complex error handling (partial batch failure) |
| Directly addresses "per-command work dominates" finding | `SAVEPOINT` per command still needed for isolation |

**Effort**: High (1-2 weeks, core correctness)

---

## 3. SequencePool Background Pre-allocation

**Location**: `skilj-core/src/db/mod.rs:8220` `SequencePool`

| Advantages | Downsides |
|------------|-----------|
| Eliminates `next_sequence_batch` round trips under steady load | Background task adds complexity (cancellation, restart) |
| Predictable latency (no refill stalls) | Must handle promotion/gap detection correctly |
| Matches kafgres segment engine "pre-provision" idea | Leaked sequences on crash need `shrink_back` coordination |

**Effort**: Medium (3-5 days)

---

## 4. skilj-kafka Outbound Batching ✅ IMPLEMENTED

**Location**: `skilj-kafka/src/lib.rs` `produce_once` / `produce_and_ack_batch`

| Advantages | Downsides |
|------------|-----------|
| 3-5x throughput on high-volume topics (rdkafka internal batching) | Increases latency per event (waits for batch fill/flush) |
| Reduces network round trips, broker CPU | Requires idempotent producer (`enable.idempotence=true`) |
| Better compression ratios on larger batches | Ordering guarantees only within partition - batch must respect key |
| Leverages rdkafka's `linger.ms`/`batch.size` + `compression.type` | Backoff/retry logic more complex for partial batch failure |

**Implementation**: Modified `produce_once` to collect all owned events in a cycle and send them as a batch using `join_all` on individual `send` futures, followed by a single acknowledgment phase. Uses owned `EventData` struct to satisfy lifetime requirements. Requires caller to configure producer with `linger.ms=5`, `compression.type=snappy`, `enable.idempotence=true`.

**Effort**: Low (1-2 days) ✅ Done

---

## 5. skilj-kafka Inbound Batching

**Location**: `skilj-kafka/src/lib.rs:686` `run_inbound`

| Advantages | Downsides |
|------------|-----------|
| Processes multiple messages per `recv()` call | Offset commit semantics: must commit highest offset in batch |
| Reduces HTTP call overhead to skilj REST | Partial failure: some messages succeed, some fail |
| Better utilization of `retry_policy` backoff | `StreamConsumer::stream()` with buffer adds memory pressure |

**Effort**: Low-Medium (2-3 days)

---

## 6. Connection Pool Tuning ✅ IMPLEMENTED

**Location**: `skilj/src/lib.rs` `SkiljBuilder::pool_options_performance_optimized()`

| Advantages | Downsides |
|------------|-----------|
| Eliminates connection churn under load (`min_connections`) | Higher baseline memory (idle connections hold Postgres resources) |
| `acquire_timeout` prevents indefinite queueing | `max_connections` too high → Postgres contention |
| Rule of thumb: `2 * num_cpus` (eventcore-postgres) | Optimal values workload-dependent, need tuning per deployment |
| `idle_timeout` reclaims unused connections | Requires monitoring/observability to tune |

**Implementation**: Added `pool_options_performance_optimized()` method to `SkiljBuilder` that sets:
- `max_connections = 2 * CPU cores`
- `min_connections = max_connections / 2` (at least 1)
- `acquire_timeout = 30s`
- `idle_timeout = 10min`

Can be overridden by chaining `.pool_options(custom_options)` after.

**Effort**: Very Low (30 min) ✅ Done

---

## 7. Kafka Producer Compression ✅ IMPLEMENTED (Documentation)

**Location**: `skilj-kafka/src/lib.rs` module docs + `run_outbound` doc

| Advantages | Downsides |
|------------|-----------|
| 60-80% payload reduction for JSON (snappy/zstd) | CPU overhead on producer (usually negligible) |
| Reduces broker disk I/O, network | Consumers must support same codec (all modern clients do) |
| `compression.type=snappy` low CPU, good ratio | `compression.level` trade-off: higher = more CPU, better ratio |
| Single config flag | Batch compression requires batching (see #4) |

**Implementation**: Added documentation in module-level docs and `run_outbound` function docs with complete example:
```rust
let producer: FutureProducer = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .set("compression.type", "snappy")
    .set("enable.idempotence", "true")
    .set("linger.ms", "5")
    .create()
    .expect("producer creation failed");
```

**Effort**: Trivial (config flag) ✅ Done

---

## 8. Golden Transcript Conformance Suite

**From**: kafgres §1 (differential conformance testing)

| Advantages | Downsides |
|------------|-----------|
| Catches silent behavioral divergence across GraphQL/REST/TUI | Initial recording effort: manual curation of golden files |
| Enables safe refactoring/optimization (any change = diff) | Version skew: recorded baseline must match test environment |
| "Catalogue, don't normalize" keeps assertions honest | Three surfaces × versions = combinatorial matrix |
| Reuses existing `skilj-demo`/`skilj-kafka` integration tests | No reference implementation to diff against (unlike kafgres) |

**Effort**: Medium (1-2 weeks, test infrastructure)

---

## 9. Co-resident OLTP Measurement in CI

**From**: kafgres §5 (measure neighbor pgbench loss)

| Advantages | Downsides |
|------------|-----------|
| Measures what deployments actually care about | Requires CI Postgres + pgbench runner (resource cost) |
| Catches regressions that only show under contention | Noisy: needs multiple runs, statistical significance |
| kafgres: 1% vs 15% pgbench loss = actionable data | Skilj's current numbers "pre-date post-0.0.7 review fixes" |

**Effort**: Low-Medium (CI script, 1-2 days)

---

## 10. Prepared Statement Caching for Hot Queries

**Location**: `skilj-core/src/db/mod.rs` hot paths (`list_events_for_bounded_context`, etc.)

| Advantages | Downsides |
|------------|-----------|
| Avoids parse/plan overhead per query | `query!` macro unusable (no live DB at compile time) |
| sqlx reuses prepared statements automatically | Manual `query_as` loses compile-time checking |
| Significant for high-frequency small queries | Statement cache size limits (Postgres `prepared_statements`) |

**Effort**: Low (refactor hot paths to `query_as`)

---

## 11. Partial Indexes for Time-Window Queries

**From**: composable-rust `idx_events_recent WHERE created_at > NOW() - INTERVAL '30 days'`

| Advantages | Downsides |
|------------|-----------|
| Smaller index, faster scans for recent events | Only helps queries matching the predicate |
| Reduces index maintenance on old events | `NOW()` not immutable - requires `CREATE INDEX ... WHERE created_at > (NOW() - INTERVAL '30 days')` not allowed; must use fixed timestamp or partition |
| Aligns with "recent events hot, old cold" access pattern | Migration: add index concurrent, no lock |

**Effort**: Low (migration + index)

---

## 12. Snapshot Cache Warm-up Optimization

**Location**: `skilj-core/src/event_cache.rs:169` `warm()`, `skilj-core/src/db/mod.rs` snapshot queries

| Advantages | Downsides |
|------------|-----------|
| Avoids replaying 50k events (load snapshot + 500 events) | Snapshot validity: must match `as_of_sequence` exactly |
| `EventCache` already has `SnapshotContext` support | Cache miss on snapshot change → full fallback |
| Reduces `decide()` matching_events load | Adds complexity to `try_events_after` coverage check |

**Effort**: Medium (ensure cache serves snapshot reads)

---

## 13. Read/Write Splitting (PgBouncer / Read Replica)

**Architecture**: Not in codebase yet

| Advantages | Downsides |
|------------|-----------|
| Offloads read queries (subscriptions, projections) from primary | Replication lag: stale reads for `ConsumeEvents`/`QueryEvents` |
| Primary handles only `ProcessCommand` writes | Adds infrastructure complexity (PgBouncer, replica) |
| Horizontal read scaling | `sequence` row lock still on primary (write path) |

**Effort**: High (infrastructure + routing logic)

---

## 14. Async Projection Batching

**Location**: `skilj-core/src/projections/mod.rs` async projection consumer

| Advantages | Downsides |
|------------|-----------|
| Projects multiple events per transaction | Projection ordering guarantees (per-stream) |
| Reduces projection commit overhead | Failure isolation: one bad event blocks batch |
| Matches command batcher pattern | Requires projection dispatcher to support batch |

**Effort**: Medium (dispatcher API change)

---

## 15. `random_page_cost = 1.1` for SSD

**From**: eventcore-postgres production tuning

| Advantages | Downsides |
|------------|-----------|
| Planner favors index scans over seq scans on SSD | Only effective if storage is actually SSD/NVMe |
| Zero code change (Postgres config) | Requires superuser / `ALTER SYSTEM` |
| Measurable improvement for tag-indexed queries | May hurt HDD performance if misconfigured |

**Effort**: Trivial (ops config)

---

## Priority Matrix

| Optimization | Impact | Effort | Risk | Status | Recommended Order |
|--------------|--------|--------|------|--------|-------------------|
| Kafka producer compression | High | Trivial | Low | ✅ Done | 1 |
| Connection pool tuning | High | Trivial | Low | ✅ Done | 2 |
| skilj-kafka outbound batching | High | Low | Medium | ✅ Done | 3 |
| skilj-kafka inbound batching | Medium | Low-Medium | Medium | Planned | 4 |
| Partial indexes | Medium | Low | Low | Planned | 5 |
| SequencePool pre-allocation | High | Medium | Medium | Planned | 6 |
| Pipeline batch commit | Very High | High | High | Planned | 7 (after measurements) |
| EventCache lock-free + tag index | High | Medium | High | Planned | 8 |
| Golden transcript suite | Medium (safety) | Medium | Low | Planned | 9 (enables 7,8) |
| Co-resident pgbench CI | Medium (observability) | Low-Medium | Low | Planned | 10 |
| Prepared statement refactor | Low-Medium | Low | Low | Planned | 11 |
| Snapshot cache warm-up | Medium | Medium | Medium | Planned | 12 |
| Async projection batching | Medium | Medium | Medium | Planned | 13 |
| Read/write splitting | High | High | High | Planned | 14 (later) |
| `random_page_cost` | Low | Trivial | Low | Planned | Anytime |

---

## Decision Framework

**Done** (high impact, low risk, low effort):
- #7 Kafka producer compression (docs)
- #6 Connection pool tuning (`pool_options_performance_optimized()`)
- #4 skilj-kafka outbound batching

**Do next** (high impact, low risk, low effort):
- #5 skilj-kafka inbound batching
- #11 Partial indexes
- #15 `random_page_cost`

**Do after measurement** (need baseline):
- #3 SequencePool pre-allocation
- #10 Prepared statement refactor
- #12 Snapshot cache warm-up
- #14 Async projection batching

**Do with safety net** (golden transcripts first):
- #2 Pipeline batch commit
- #8 Golden transcript suite
- #1 EventCache lock-free + tag index

**Defer** (infrastructure/architectural):
- #13 Read/write splitting
- #9 Co-resident pgbench CI (CI only)

---

## References

- [kafgres learnings](kafgres-learnings.md) - differential conformance, co-resident measurement, segment engine
- [eventcore-postgres](https://lib.rs/crates/eventcore-postgres) - pool sizing, `random_page_cost`, prepared statements
- [composable-rust](https://github.com/jonathanbelolo/composable-rust) - partial indexes, backup strategies
- [skilj performance.md](performance.md) - current baseline: ~45-60 cmd/s per bounded context
- [rdkafka batching](https://docs.rs/rdkafka/latest/rdkafka/producer/struct.FutureProducer.html#method.send_batch) - `send_batch` API