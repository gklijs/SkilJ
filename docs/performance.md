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

From the `skilj-helpdesk` showcase app (single shared bounded context,
one Postgres, local sandbox, 4-minute ramp steps), commit `0d9c409`:

| workers | before batching round-trip cuts | after |
|---|---|---|
| 20 | ~27/s | ~45/s |
| 80 | ~27/s | ~58/s |

Mean batch size at 80 workers was ~35. Treat ~45-60 commands/s for a *single*
bounded context on modest hardware as the working figure; it is a
lower bound for your hardware, not a guarantee. These figures pre-date the
post-0.0.7 review fixes (§63); that change touches the hot path only by
adding a semaphore acquire per batch, but it has not been re-load-tested.

## Knobs

All on `SkiljBuilder`:

| method | default | what it does |
|---|---|---|
| `command_batch_max_size(n)` | 256 | Most commands one lock acquisition processes. Larger amortizes more; smaller shortens each lock hold and limits how many commands one failed batch takes down with it. If more than `n` are queued, the same leader keeps processing follow-up batches until the queue is empty. |
| `command_batch_max_concurrent_leaders(n)` | half of the pool's `max_connections` (min 1) | Caps batch leaders running at once across all bounded contexts. Each leader pins one connection for its batch while still needing others for reads; without a cap, many busy bounded contexts can leave the pool full of leaders waiting on each other. |
| `command_batch_idle_in_transaction_timeout(d)` | 30s | Postgres kills a leader whose transaction sits idle *between statements* longer than this, releasing the lock. A backstop for genuinely stuck leaders, not a throughput knob. The lock wait itself is not covered. |
| `pool_options(...)` | sqlx default (10) | Size the pool for your bounded-context count. A rule of thumb: at least 2x the concurrent leaders you expect, plus headroom for request-time reads. |

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
