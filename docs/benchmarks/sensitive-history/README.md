# Sensitive command history investigation (#174)

## Decision

Retain full locked cryptographic validation of unchanged sensitive history.
Adopt only the independent change that skips JSON parsing and serialization
for events whose `sensitive_fields` is empty. Mixed histories preserve those
payloads byte-for-byte. Sensitive events still follow the identical key lookup,
row locking, unwrap, leaf authentication and temporary materialization path.

The experiment shows CPU savings are possible, especially for long histories
with warm keys. It does not establish an end-to-end gain sufficient to justify
adding a second security-sensitive preparation protocol. In particular, the
key-only prototype below has no evidence binding or invalidation implementation;
it is a lower bound on that protocol's cost, not a safe replacement.

## Measurement

Run on 2026-11-09, x86_64 Linux, embedded PostgreSQL 18.6.0, release profile,
with the compiler recorded in [rustc.txt](rustc.txt). The test is
[`benchmark_locked_sensitive_history`](../../../skilj-core/src/db/decision_history/tests.rs).
It requires working Postgres and fails rather than skipping when unavailable:

```sh
cargo test --release -p skilj-core --lib \
  db::decision_history::tests::benchmark_locked_sensitive_history -- --ignored --nocapture
```

`DATABASE_URL` can supply a server with `CREATEDB`; the harness recreates
`skilj_decision_history_test`. Run this probe separately from the regression
tests, which share that database name. On the measurement host, embedded
Postgres also needed `LD_LIBRARY_PATH=/tmp/skilj-173-libs` for its compatibility
libraries, and permission to start the local server.

Each scenario has 25 samples of `decide_command_in_tx` on a real transaction,
starting **after** acquiring the bounded-context sequence lock and ending after
rollback releases the locks. This is critical-section wall time, excluding
lock acquisition/wait and transaction opening. Unchanged cases validate a
prepared acceptance; conflicts add one same-batch event and redispatch to a
rejection. The optimistic decision is actually run before each scenario, but
its latency is not part of the measurement. Histories are synthetic ciphertext
records, 100 or 1,000 events, about 1 KiB padding per payload, sharing one
subject. `sensitive_every=100` means one sensitive event per 100; the rest have
no sensitive registration. Every unwrap still uses the production SQL and
provider paths. The slow provider sleeps for 10 ms per unwrap (Tokio timer
scheduling makes observed delay somewhat larger). Cold samples clear the data
key cache; warm samples retain it. Both paths resolve a repeated subject once
per invocation. The raw logs include unwrap counts: zero per 25 warm samples,
25 per 25 cold samples.

[baseline.log](baseline.log) uses the original production loop;
[skip-unprotected.log](skip-unprotected.log) skips JSON conversion of
unprotected records. The same test-only key validator, which reads/locks the
known subject and calls `unwrap_row`, is measured in both. It never substitutes
for production. Representative medians, in microseconds:

| Events | Sensitive | Provider/cache | Conflict | Original | Skip unprotected | Key-only prototype, final run |
| --- | --- | --- | --- | ---: | ---: | ---: |
| 1,000 | all | local/warm | no | 3,327 | 3,218 | 197 |
| 1,000 | 1% | local/warm | no | 2,263 | 1,102 | 210 |
| 1,000 | 1% | local/warm | yes | 4,026 | 2,521 | ineligible |
| 1,000 | all | slow/cold | no | 14,572 | 14,977 | 11,701 |
| 1,000 | 1% | slow/cold | no | 13,942 | 12,753 | 11,654 |
| 1,000 | 1% | slow/cold | yes | 19,101 | 14,201 | ineligible |

The smaller change reduced the local/warm mixed-history median by 51% without
an evidence protocol. The all-sensitive control does not benefit; differences
between runs include noise. After that change the prototype saves a further
892 us in the local/warm mixed case, while retaining the slow cold unwrap's
roughly 11.7 ms floor. It cannot remove provider work without changing refusal
semantics. More subjects would incur more authoritative lookups and unwraps.
The logs contain p95s, but these short, ordered runs are exploratory, not a
statistical throughput study. They omit durable writes, HTTP, queue contention,
allocations, evidence hashing, and retained memory. No durable throughput or
whole-command latency improvement is claimed.

## Correctness and evidence contract

The existing no-conflict path authenticates the selected sensitive leaves again
and discards the resulting plaintext vector. Each distinct subject is read with
`status = 'active' FOR NO KEY UPDATE` on the command transaction's connection.
These row locks survive dropping the temporary plaintext and stay held until
transaction end. Destruction and rewrap update the same rows, so an erasure
committed before validation is observed; an erasure arriving after validation
waits for the command transaction. Erasure followed by reprovisioning finds a
new data key and fails authentication of old ciphertext. A warm key cache
cannot bypass the authoritative active-row lookup. An unchanged wrap can use a
warm cached data key, exactly as before; provider outages and out-of-band
retirement are not freshly polled on such cache hits. Supported skilj retirement
clears its service's cache; cold unwraps propagate provider/retirement failure.
This investigation does not strengthen those existing distributed-cache limits.

A matching append invalidates the optimistic decision and selects the history
again. The engine decrypts that selected history before redispatch; rejection
history comes from its ciphertext source. Folds, persisted events and caches
never receive the plaintext history. This change introduces no new retained
plaintext, typed state, decrypted cache entries or public API.

A future validation-only path would need **private, unforgeable evidence** minted
only after successful optimistic authentication and consumed with the decision.
The owned `PreparedCommand` from #172 explicitly promises ownership, not proof
that locked validation can be skipped; borrowed submission and public resolved
histories can also be supplied by hand. The evidence must bind at least:

- Database/context identity, exact selected events and ciphertext, subject and
  active row identities, wrapped key bytes/version/provider, registrations and
  decision inputs (including time, snapshots and query boundary).
- A registration/policy generation checked authoritatively under the appropriate
  locks. Privacy, ownership, sensitive paths and subject mappings must invalidate
  evidence when changed. Existing selected `Event` values retain the registration
  used when loaded; `decision_history` itself does not refresh it. A database
  re-registration alone does not update an already prepared slice. The tests
  record this limit and refusal after fresh registration resolution; they do
  not claim registration updates are serialized by the subject-row lock.
- Erasure/reprovisioning: refuse old evidence even for the same subject name.
  Rewrap/provider/version changes: invalidate and fully authenticate again;
  do not infer data-key equality from subject or row ID. Retirement and provider
  failures: retain the real unwrap path and existing cache invalidation rules.
- Missing/changed/malformed ciphertext or changed event selection: refuse or
  rebuild through full validation, never mint evidence from mere row existence.
  Matching concurrent appends and same-batch appends require full redispatch.

Verifying these identities must not trust caller-editable histories or decisions.
Hashing all payloads at commit may eat some CPU savings; retaining identifiers
or shared records changes memory cost and the preparation API. The existing
contiguous `Cow<[Event]>` API also requires cloning the full vector when any
payload changes, so this patch avoids unnecessary JSON conversion but does not
eliminate nonsensitive record clones. Borrowed/shared event views need separate
API work. There is no semver change here.

## Regression coverage

The adjacent test module exercises real Postgres transactions and the actual
locked decision function, with deterministic interleavings:

- Erasure after optimistic dispatch, including cached keys and reprovisioning.
- Provider failure and retirement after dispatch, with cold unwrap refusal.
- An eraser's lock timeout while validation's transaction remains open, then
  successful destruction after commit (no timing-only assertion of blocking).
- A separately committed append read by the delta query, and a same-batch
  append, both redispatched with plaintext while rejection history stays encrypted.
- Changed registrations resolved from Postgres, malformed ciphertext, changed
  stored wraps with an old cached key, and byte preservation in mixed histories.

Existing `submit_command`, `command_batcher`, `encryption`, `subject_erasure`
and `master_key_rotation` integration suites supplement these tests. The
retirement failure test explicitly retires at the provider and clears the
cache to model a cold check; the existing rotation suite verifies skilj's
supported retirement flow and postponement of retirement for outstanding keys.
The key-only prototype is confined to an ignored experiment and cannot be
called by a production command.
