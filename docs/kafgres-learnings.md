# kafgres: what a sibling project does differently

A read-through of [kafgres](https://github.com/RayElg/kafgres) (Elastic
License 2.0) written for SkilJ's benefit. kafgres is a PostgreSQL extension
that embeds a Kafka broker in a background worker; topics, partitions, offsets
and consumer groups are rows in the database, and unmodified Kafka clients
connect to port 9092 and cannot tell the difference.

The resemblance is real but shallow, and worth stating precisely before
anything else: **both projects are "an event log that is a Postgres table,
reached through a library rather than a broker."** Past that, they solve
opposite problems. kafgres's hard part is *wire-protocol compatibility* -
librdkafka, the Java client, Sarama and kafka-python must all interoperate,
and Kafka's own semantics (leader epochs, `read_committed`, aborted
transactions, rebalance protocols) have to be reproduced well enough that
clients' correctness logic still holds. SkilJ's hard part is *DCB correctness* -
a `decide()` must see exactly the events that share its consistency tags, and
the sequence must be gapless, or the boundary is a lie. kafgres has no
equivalent concern; SkilJ has no interest in kafgres's.

So this is not a "steal the design" document. Most of kafgres is not
applicable. What is applicable falls into three groups: **engineering
discipline** worth copying almost verbatim, **capabilities** SkilJ is missing
and could plausibly want, and **one real gap** that reading kafgres made
visible. Each idea below carries a verdict.

| Idea | Verdict |
|---|---|
| Differential conformance suite in CI | Adopt |
| "A deviation gets a catalogue entry, not a test normalizer" | Adopt |
| Document what the suite does *not* cover | Adopt |
| Advertise only what you enforce | Adopt, already half-done |
| Preview/dry-run before enabling | Adopt, cheap |
| Co-resident-OLTP degradation as a measured number | Adopt |
| Capability manifest generated from one source | Adopt, small |
| Storage engine split (table vs segment) | Do not adopt |
| WAL/CDC ingress into the event store | Do not adopt - it breaks the DCB premise |
| Transactional produce via commit markers | Already have it, structurally |
| Retention and compaction | Do not adopt; note the archival lesson |
| Primary-only operation, HA is Postgres HA | Partly relevant - see the gap |

---

## 1. Differential conformance testing

kafgres' strongest idea, and the one most clearly worth taking.

Its test suite drives four real Kafka clients against kafgres **and against a
real `apache/kafka` broker**, running the identical command on both and diffing
the observable output. `13/13 checks identical` in the README is not a claim
about a mock; it is the output of a script. The suite runs in CI on every
commit. The reference half is profile-gated because it needs a second broker,
and *skips rather than fails* when the broker is absent.

Three properties make this work, and all three transfer:

- **Clients are the specification, not the schema.** A response can satisfy
  every schema constraint and still hang a client. librdkafka and the Java
  client disagree about edge cases, and both are correct in the sense that
  matters, which is that users run them.
- **Observable output, not bytes.** Timing, ordering and error text may
  legitimately differ. What must match is what a program using the client would
  *decide*.
- **When a difference is intended, it does not get a normalizer in the test.**
  It gets an entry in a catalogue with the reason. kafgres is explicit that
  weakening the assertion to make a diff go away "removes the only mechanism
  that does that." The exact-equality assertion is what keeps the catalogue
  honest.

**What SkilJ would do.** There is no reference implementation to diff against,
and inventing one is not worth it. The transferable form is a **golden
transcript** suite: a set of scenarios expressed as ordinary GraphQL/REST calls
that print one machine-readable line each, recorded into a checked-in file,
re-run in CI, and diffed. Every deliberate divergence becomes a catalogue entry
rather than a changed expectation. The shape is identical to kafgres'; only the
second column of the comparison changes from "reference broker" to "recorded
observable behaviour".

This lands on top of what already exists rather than replacing it -
`skilj-demo`'s suite exercises real Postgres end to end, and `skilj-kafka` /
`skilj-amqp` / `skilj-nats` run against real containers. The gap is
specifically *cross-surface and cross-version*: the same logical operation
performed over GraphQL and over REST, and the same surface at two versions,
should be observed to agree. SkilJ has three surfaces (GraphQL, REST, TUI) and
a per-caller-scoped schema (§138, §124), which is exactly the shape where a
silent divergence between surfaces is possible and nobody would notice.

The `scripts/check-section-refs.sh` precedent is the same instinct in a
smaller form - a cheap CI check that catches a whole class of silent drift.
This is the same idea applied to behaviour instead of documentation.

## 2. State what the tests do not cover

kafgres' conformance doc has a section titled "What this suite does not cover",
listing throughput, compression end-to-end, TLS/SASL under the matrix, failover,
and "anything a scenario does not do" - with the closing note that the scenario
count is a floor, not a ceiling, and that when a client reports a bug the fix is
a new scenario there first.

Cheap, and it prevents a specific failure mode: a green suite read as broader
than it is. SkilJ's docs are strong on *why* decisions were made
(`docs/architecture.md`) and `docs/performance.md` is honest that its numbers
"pre-date the post-0.0.7 review fixes". The missing piece is the test-side
counterpart - a short statement of which guarantees are verified by
construction, which by test, and which by nothing at all.

## 3. Advertise only what you enforce

kafgres refuses to report `min.insync.replicas` in `kafka-topics.sh --describe`
output, because it does not honour the setting. The stated reason is the one
worth stealing: "reporting an unimplemented setting invites clients to act on
it." Where the feature cannot apply, the answer is a documented error code, not
a success that does not do the thing - `AlterReplicaLogDirs` to the one real
directory is a no-op success, anywhere else is `LOG_DIR_NOT_FOUND`.

SkilJ already runs this discipline in several places and should say so
explicitly: §92 (a scoped admin cannot mint its way out), §140 (REST reads
refuse before loading), §141 (event creation checks `requires` before touching
the database), §128 (REST token resolution reads the token once), §93 (database
error text stays in the log). The pattern is consistent: *the refusal is the
guarantee*.

Two places where the discipline could be tightened:

- **The self-describing surface** (§13's `eventTypes`/`commandTypes`) should
  report only the protections actually enforced for the calling context - the
  same discipline §124 (reads load only the reader's own grants) already applies
  to data.
- **A capability manifest.** kafgres declares its served API surface in
  `codec/implemented.toml`, and *the same declaration generates both the
  dispatch table and the advertised `ApiVersions` payload*, so what is advertised
  and what is implemented cannot drift. SkilJ's analogue: a generated manifest
  of registration, REST routes, and self-described capabilities, plus a CI test
  asserting every advertised capability is implemented and every implemented one
  is advertised. `scripts/check-template.sh` already exists for a neighbouring
  reason (§105) - a consumer artifact nothing else in the workspace exercises.

## 4. Preview before you enable

kafgres ships `kafgres_preview_mapping(mapping, predicate)`: render a mapping
over the source table's current rows, with a predicate spliced into the
mapping's own `WHERE`, before enabling it. It exists because the alternative -
turning on a stream and reading the output to find out what it does - is how
pipelines get their first hour of production traffic.

SkilJ's `decide()` is pure and synchronous by design (§1.1), which makes a
dry-run almost free: assemble the matching events, call `decide()`, return the
decision, persist nothing. That is a genuinely useful operator and debugging
tool - "what would this command do right now?" - and it is strictly a
read-only surface with no new semantics.

The `on_error` knob alongside it is worth noting too: per-mapping `skip`
(default) versus `stall`, with the honest warning that stalling pins WAL and
fills the disk, "so it is the choice for pipelines where every event matters,
not the safe default." SkilJ's parked-delivery mechanism (§47, §64, §144) is
the same decision already made once, globally, with a documented reason. That
is arguably better. The lesson is not "add `on_error`" but "when you give
operators a failure-mode knob, say which default is safe and why."

## 5. Measure what you cost your neighbour

kafgres benchmarks its segment engine as "about 1.5x the table engine's
throughput with a lower p99, **at about 1% degradation of co-resident pgbench
against the table engine's 15%**." The second number is the interesting one. It
is not the project's own throughput; it is what the OLTP workload sharing the
database loses.

SkilJ's stated design is that it sits alongside an application's own tables in
the same Postgres (§2.2, §2.2.2). `docs/performance.md` reports only SkilJ's own
figures - ~45-60 commands/s per bounded context, mean batch size ~35 at 80
workers - with an honest caveat that they are a lower bound. Nothing measures
what the co-resident application loses, which is the number a deployment
decision actually turns on. kafgres' configuration doc goes further still:
putting the log on its own device because "with both on one device the
database's commit flush queues behind the log's writeback," with measured
figures (27% pgbench loss at 500 MB/s produce on a shared device, 11% on a
separate one).

Same lesson, and it generalises past storage: report the neighbour's number.

## 6. Transactional produce - already structural here

kafgres' headline feature is `kafgres_produce()`: append a record inside the
caller's transaction, so `INSERT INTO orders` and the event either both commit
or neither does. "No outbox table, no Debezium, no dual-write inconsistency
window."

The mechanism is worth reading even though SkilJ does not need it. The function
appends bytes to the log *and* writes a small commit-marker row (~40 bytes)
inside the caller's transaction. Visibility to `read_committed` consumers is
computed from committed markers. Three consequences fall out, all documented:

- A rolled-back produce leaves its bytes in the log. That is not a leak, it is
  how Kafka's own aborted transactions work - consumers skip them via the
  `aborted_transactions` list in the Fetch response. Fetch has no way to tell a
  client to advance past a withheld batch, and a client re-requesting the same
  offset forever looks like a hung consumer.
- Each transaction produces under its own producer id, taken from the
  transaction's xid. A shared producer id would let one rollback discard later
  committed records.
- The last stable offset gates in-flight transactions, so `read_committed`
  consumers never see records that may yet commit or abort.

**SkilJ gets this for free.** `process_command` inserts the `Command` row, the
`Event` rows and the sequence increment in one transaction (§1.5's design
note). There is no window because there is no second write. The three
consequences are the part worth keeping in mind: they are the failure modes a
dual-write design *would* have, enumerated by someone who built the hard
version. SkilJ's §39 (external-message dedup on `CreateExternalEvent`) and
§97 (`skilj-kafka` never commits past an unreported message) are the places
where SkilJ *does* have a two-phase boundary, and both are handled the way
kafgres handles its - the marker is written and only then is the external
position advanced.

## 7. The gap reading kafgres exposed

kafgres devotes real engineering to a scenario SkilJ has never examined:
**failing over to an asynchronous standby can lose committed data, and a
consumer holding offset 5000 can reconnect to a primary whose log ends at 4800
and read divergent data.** Its answer is a persisted `leader_epoch` per
partition, raised at every start to the Postgres *timeline id* minus one -
deliberately not `old + 1`, because the timeline ordering stays correct across
promotions in a way that local increment arithmetic may not - stamped into each
response and served via `OffsetForLeaderEpoch` so clients truncate back to what
they can trust. It is verified against a real physical standby, and it is
catalogued as an intended behavioural difference (epochs are not consecutive).

SkilJ's internal invariants survive a promotion for a reason worth stating
plainly, because it is the reason this is a *partial* gap and not a large one:
WAL is a total order. The sequence increment and the `Event` row it names are
in the same transaction, so a standby that lost the tail lost both. A
projection's `caught_up_to` cannot exceed the surviving log for the same
reason. The `sequence` table cannot hand out a duplicate.

What does *not* survive a promotion is any external observer's position:

- `skilj-kafka` already produced to a topic for events the promoted primary no
  longer has. Its committed offset points past the log's end.
- A REST or GraphQL client already read events that are now gone, and holds a
  read cursor or a `manual_ack` position past the end.
- A bridge's `external_message_cursors` watermark (§39) may sit above the
  surviving log end. If it does, a redelivery of messages in the gap is
  silently deduped and *dropped*, which is worse than redelivering them.

kafgres' answer for the consumer case is `OFFSET_OUT_OF_RANGE` plus
`auto.offset.reset` - a visible, recoverable error the client already knows how
to handle. SkilJ has no equivalent signal, and the failure is silent.

Cheapest useful step, in order:

1. **Document it.** One section in `docs/architecture.md` saying what a
   promotion costs a multi-instance deployment, which positions can point past
   the surviving log end, and that SkilJ assumes `synchronous_commit` at least
   as strong as `remote_apply` - or, more plainly, that a deployment using an
   asynchronous standby accepts at-least-once with a possible gap. The cost is
   near zero and it is currently undocumented.
2. **Detect the backwards jump.** Bridges already track a position per
   partition. A bridge that sees its partition's sequence go *backwards* has
   found a promotion, and can log it loudly, refuse to advance, and require an
   operator decision - the kafgres epoch mechanism reduced to the one case that
   can actually occur here.
3. Only then consider anything else.

This is a genuinely good example of what reading a sibling project is for: not
a feature to copy, but a scenario the sibling took seriously enough to build
for, which prompted asking what SkilJ does when it happens. The answer is
currently "nothing, silently."

## 8. Ideas that do not transfer

**A segment-file storage engine alongside the table engine.** kafgres ships
both, selectable by GUC, and measures the segment engine at ~1.5x produce
throughput with much less WAL pressure - a 1 MB batch is ~525 TOAST chunks plus
index entries, WAL for all of it, a dead tuple for autovacuum, and a row lock
per append. That analysis is right and SkilJ is not the workload it describes.
SkilJ reads events by `sequence` range *and* by `tags` GIN containment *and* by
`(event_type_name, sequence)` - the indexes are the query model. A log file has
none of those, and kafgres' own segment engine is admitted not to support
transactional produce for exactly the lock-contention reason. Worth noting one
transferable detail: kafgres refuses to start on a log written by the other
engine unless an explicit override is set, rather than reading a foreign log and
producing nonsense. SkilJ's `allow_engine_mismatch` equivalent already exists in
spirit - §149 handles deleting and recreating a bounded context under the same
name.

**CDC ingress from the WAL.** kafgres' second produce path: a logical
decoding output plugin, mappings written as SQL expressions with `new`, `old`,
`op`, `lsn`, `xid` and `commit_ts` in scope, a drain worker, and a `skip`/`stall`
error policy. It is impressive work, and SkilJ should not have it. A mapping
over a table bypasses `decide()` entirely - no consistency boundary, no tags, no
gapless sequence relationship to the command that caused the write. SkilJ's
entire value proposition is that events are *decisions*, not row diffs.

kafgres is also refreshingly honest about the one caveat, and it is the caveat
that decides it: an enrichment subquery runs when the change is rendered, so it
reads *current* state, not the state the transaction saw. "Postgres cannot
evaluate a query as of an arbitrary past LSN." In a DCB engine that is not an
acceptable trade for a convenience feature - it is the exact property
`decide()`'s `matching_events` exists to provide. SkilJ's
`ExternalEventIngestion` / `CommandTrigger` path (§40-§42) is the right shape
instead: a foreign message becomes a *command*, and `decide()` runs normally.

**Retention, compaction, and archiving.** kafgres implements
`cleanup.policy=delete` by segment and `cleanup.policy=compact` by keeping the
latest record per key. SkilJ deliberately keeps everything - "no retention/TTL,
nothing is ever deleted" (§21's precedent, extended deliberately to
idempotency keys in §87 and resolved deadlines in §163). SkilJ events are
immutable decisions; compaction by key would destroy history a projection
rebuild (§19) depends on. The pressure compaction answers - unbounded growth -
is answered differently here, by snapshots.

One detail does transfer, for whenever event archival is on the table:
kafgres' `segment_archive_command` makes retention *refuse to reclaim a segment
the archive has not taken*, and a failing archive command stops reclamation and
grows the disk rather than losing data. "Deletion must be gated on an
observable acknowledgement from downstream" is the same rule `POST
/v1/parked-deliveries` already implements for bridges (§97, §98, §100) - and it
is worth stating as a general principle rather than three separate
implementations.

## 9. Smaller things worth noting

- **Vendored schemas at a pinned tag, with a generated file checked in and
  marked do-not-edit.** SkilJ's `skilj-codegen` emits at `build.rs` time
  instead, which means generated code never shows up in a review diff. kafgres'
  trade-off is that a spec change's effect on the emitted surface is visible as
  a diff. Worth considering for `skilj-codegen` output specifically, where the
  generated `EventType`/`CommandType` impls are the artefact a reviewer actually
  wants to see.
- **Version skew produces false deviations.** kafgres pins `codec/KAFKA_VERSION`
  and manually matches the reference image tag, because "an unpinned older
  broker turns version skew into false deviations." If the golden-transcript
  suite from §1 is adopted, the same trap applies to the recorded baseline.
- **"One broker per instance; HA is Postgres HA."** No controller, no quorum,
  no leader election, no fencing - "if the HA stack cannot promote two
  primaries, kafgres cannot end up with two leaders." SkilJ's cross-instance
  story (§83, §12, §160, §161) has no equivalent simplification, but the
  principle is the right one: push a hard problem down to infrastructure that
  already solves it rather than solving it twice, worse.
- **The library boundary earns its keep.** `kafgres-codec` is the wire
  protocol with no pgrx and no Postgres, unit-testable alone; the pgrx
  extension is a separate workspace "on purpose." This workspace already has
  the same property in a different form - `skilj-kafka`/`skilj-amqp`/
  `skilj-nats` speak the wire protocol only and depend on no other skilj crate
  (§165's shared client side is the one deliberate exception). Worth noting
  that the discipline is converging from both directions.
- **`cargo check` passes on code that cannot load; only the container build
  proves the link.** The general form of this warning applies to SkilJ: `sqlx`'s
  compile-time query checking is given up entirely, because per-bounded-context
  schemas mean every statement is `sqlx::AssertSqlSafe(format!(...))` built at
  runtime. No SQL in `skilj-core` is verified by the compiler; all of it is
  verified by the integration suite. That makes the CI silent-skip guard (§67)
  load-bearing in a way it may not be fully appreciated as - if a Postgres
  test skips, nothing has checked that SQL at all. kafgres' warning is the
  same shape: the cheap check is not the real check.

## 10. If only three things

1. **A golden-transcript conformance suite** across the three surfaces, with an
   intended-divergences catalogue that exact-equality assertions keep honest
   (§1). Largest gain, smallest conceptual step - it reuses existing tests.
2. **A "what this does not cover" section** for the test suite, next to the one
   `docs/performance.md` already carries for its numbers (§2). Cost: an
   afternoon.
3. **Document and detect the promotion case** (§7). The documentation is free;
   the backwards-jump check in the bridges is a small, well-scoped addition
   that turns a silent data-loss mode into a loud one.

None of these require a new crate, a new dependency, or a change to the Allium
spec. That is the mark of a good fit - the overlap between the two projects is
in engineering discipline, not in architecture.