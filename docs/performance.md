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
time inside the lock goes. Two workloads: **spread** (every command on its own
account, so `decide()` sees an empty history) and **hot** (every command on
one account, whose history grows as the run goes).

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
- **Hot** is bounded by the account's history, which every command carries
  into the lock and folds: `decide` takes ~1 ms even with nothing new to
  re-check, and the consistency boundary is computed over the same history
  in `persist`. This grows with the account's event count. The lever is a
  `Snapshot` for such keys (§19), not the batcher.

Older figures, from the `skilj-helpdesk` showcase app (4-minute ramp steps,
commit `0d9c409`): ~45 commands/s at 20 workers, ~58/s at 80, for one shared
bounded context. A different application's commands, a networked client and
another machine - not comparable with the in-process figures above.

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
