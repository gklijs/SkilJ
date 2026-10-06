# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
(pre-1.0: breaking changes may land in minor/patch versions until 1.0).

## [Unreleased]

### Added

- Command dry-runs (docs/architecture.md §185, Codeberg issue #47): what
  a command would decide right now, with nothing persisted - no command,
  event, sequence number, encryption key or idempotency record. GraphQL's
  `dryRunCommand` query is Admin-only and returns the would-be events,
  rendered under the read rules, and the matching events. `POST
  /v1/commands/dry-run` takes the command's own `CommandToken` and
  answers only `accepted` and a rejection's reason and kind.

- `skilj/tests/coresident_pgbench.rs`, a benchmark of what an application
  sharing skilj's Postgres loses (docs/architecture.md §184, Codeberg
  issue #46), with results in docs/performance.md.

- A conformance suite across GraphQL and REST (docs/architecture.md
  §179, Codeberg issue #43): `skilj/tests/conformance.rs` runs the same
  command submissions and event reads over both tracks, compares each
  outcome with a checked-in transcript, and requires the two tracks to
  agree unless the scenario is catalogued as a deliberate divergence
  with its reason.

- `build()` warns for every projection that consumes an event type with
  private fields: they reach `project()` unredacted, and whatever of
  them a projection keeps in its state, every reader of that projection
  sees, whatever their private-field grants. Now stated in the spec and
  docs (docs/architecture.md §177, Codeberg issue #48), along with the
  same for snapshots and cross-context routes; nothing is refused.

- Database epochs (docs/architecture.md §176, Codeberg issue #42): event
  reads return the database's `epoch` (system identifier and timeline),
  which changes after a failover to an asynchronous standby or a
  point-in-time restore. `GET /v1/events` (`epoch=` with `after`),
  `POST /v1/events/consume/ack` (`epoch`) and the GraphQL `allEvents`/
  `eventsByType` subscriptions (`epoch` with `fromSequence`) refuse a
  position from another epoch with `epoch_changed` (409 over REST)
  instead of skipping the events that now carry its sequences. GraphQL
  gains a root `epoch` field. A change is logged at `error` and counted
  (`skilj.database.epoch_changes`); migration 0006 adds `skilj_epoch`.
  The bridges and skilj-tui send the epoch back and start over on a
  refusal. `skilj_bridge::ack` takes the epoch (breaking).

### Changed

- A command's DCB pre-read is cheaper in a bounded context with more
  events than the event cache holds (1000 by default): once the cache's
  window no longer reaches back to the first event, the pre-read goes
  straight to Postgres instead of first freshening a window it can't be
  served from - one database round trip less per command. When the
  window does hold the whole history, only the matching events are
  copied out of it, not all of them (docs/architecture.md §187, Codeberg
  issue #50).

- Async projection catch-up folds up to 100 events per transaction
  instead of one, and reads and writes each projection's keys once per
  chunk instead of once per event: 25-50x faster in the new
  `projection_catch_up_throughput` benchmark. An event whose fold fails
  still leaves the events before it committed. A registration or
  promotion of a projection now waits for the chunk being folded into
  it, a few milliseconds (docs/architecture.md §182, Codeberg issue
  #52).
- `skilj-kafka`'s `run_inbound` dispatches the partitions it is assigned
  concurrently, one message in flight per partition, instead of one
  message at a time for the whole consumer. Messages of one partition
  are still dispatched in offset order, and a message that keeps failing
  now holds up only its own partition. A partition with
  `INBOUND_PARTITION_BUFFER` messages waiting is paused until they
  drain. `run_inbound_until`'s `stop` now lets dispatches already in
  flight finish and commit. Messages from different partitions reach
  skilj in a less predictable order than before; the crate docs now
  state that messages whose order matters must share a Kafka key
  (docs/architecture.md §174, Codeberg issue #60).
- `skilj-kafka`, `skilj-amqp` and `skilj-nats` acknowledge outbound
  events to skilj once per contiguous run - one call for a whole page
  in the common case - instead of once per event. Events are still
  delivered one at a time, in order, and never acknowledged past one
  that failed. A failed acknowledgement now re-delivers the whole run
  rather than one event (still at-least-once). The outbound loop the
  three shared by copy now lives in `skilj-bridge`
  (`outbound_cycle`, `OutboundSink`); the bridges' public APIs are
  unchanged (docs/architecture.md §171, Codeberg issue #40).

### Removed

- `SkiljBuilder::pool_options_performance_optimized()`: it sized the pool
  from the application host's cores, which could give fewer connections
  than sqlx's default. docs/performance.md now has pool sizing guidance
  instead (docs/architecture.md §186, Codeberg issue #49). Use
  `pool_options(...)`.

### Fixed

- `build()` refuses a pool of fewer than 2 connections
  (`skilj::MIN_POOL_CONNECTIONS`). The cross-instance listener keeps one
  connection for good, so with a pool of 1 every request failed after
  the acquire timeout (docs/architecture.md §186).

- Registering a sync projection over existing history could deadlock
  against the catch-up folding the same projection: the history fold
  locked state rows before the projection row. It now takes the
  projection row first, the order catch-up and promotion use
  (docs/architecture.md §182).

- Reads of an archived bounded context served no events, over GraphQL
  and REST, whenever the event cache held them; the history is retained
  and is served again. Cached events also stopped matching their own
  type after it was re-registered or, for a scheduled type, after each
  fire, and a live subscription stopped delivering a type's new events
  after a re-registration. Rules now compare bounded contexts and types
  by name, not by every field of a copy loaded at another time
  (docs/architecture.md §179).

- A command's re-check under the bounded context's lock started at the
  last event its consistency tags already had - for a new key, the start
  of history - so every command re-read its tags across the whole history
  while holding the lock, costing more as the bounded context grew. It now
  starts where the command's optimistic read ended. Same DCB guarantee;
  measured in the new `command_throughput` benchmark and refreshed in
  docs/performance.md (docs/architecture.md §178, Codeberg issue #45).
  `db::submit_command` and `CommandBatcher::submit` take the read's
  position as a new last parameter (`None` keeps the old behaviour), and
  the per-command `command decide delta query` log is now `debug`.
- After a failover or restore, the event cache kept serving events the
  database had lost - to queries and to `decide()` - until a restart: a
  promoted standby keeps the table's OID, the cache's only stamp. Windows
  are now stamped with the database epoch too (docs/architecture.md §176).
- `skilj-nats` and `skilj-amqp` could lose inbound `Record` messages.
  JetStream redelivers an unacknowledged message after newer ones, and an
  AMQP broker returns a released message after later deliveries. The
  external-message watermark then took the redelivery for one already
  recorded, the bridge acknowledged it, and it was never created - after
  an ordinary bridge restart, for instance. `POST /v1/events/external`
  now accepts an `Idempotency-Key` header, a per-message key
  (`external_message_keys`, kept for `idempotency_key_retention`, refused
  together with `dedupe`). A keyed redelivery returns the original
  event's `sequence`. NATS sends `{stream}:{stream_sequence}`, and AMQP
  `{group-id}:{group-sequence}` or its `message-id`. Parked external
  events keep their key and are retried under it. Kafka is unchanged. In
  `skilj-core`, `create_and_insert_external_event` takes
  `Option<ExternalEventDedupe>` and `CreateExternalEventOutcome::Redelivered`
  carries `sequence` (docs/architecture.md §175, Codeberg issue #59).

- Retrying a parked inbound external event (`retryParkedDelivery`) could
  silently lose it. When the bridge's request carried a `dedupe` cursor
  and later messages on the same partition had been recorded since, the
  retry was taken for a stale redelivery: nothing was created and the
  parked row was deleted as if it had succeeded. A retry now dedupes
  under a cursor of its own per parked row, so it is created. The one
  case this trades away: an original attempt that committed without the
  bridge hearing back and was parked anyway is created twice on retry
  (docs/architecture.md §173, Codeberg issue #58).
- Two projections whose generated GraphQL type names collide
  (`{a_b}` + `c` against `{a}` + `b_c`, or a nested `{parent}_{field}`
  against another top-level name) are now resolved the same way on every
  schema build, in every process. The winner was decided by
  `HashMap` iteration order over bounded contexts, so it could differ
  between runs - and change between two builds in one run, which meant a
  caller could be served one projection and, after an unrelated
  registration change, be served the other. `list_bounded_contexts` and
  `list_projections_for_bounded_context` now return name order
  (docs/architecture.md §168).
- `skilj-kafka`'s docs said the producer's `enable.idempotence` keeps an
  event that is produced again (after its acknowledgement to skilj
  failed) from landing in Kafka twice. It doesn't: it only covers the
  producer's own internal retries, so outbound delivery is at-least-once
  and Kafka consumers must tolerate duplicates. The crate docs now say
  so, with a "Producer configuration" section (docs/architecture.md
  §169).

## [0.0.9] - 2026-10-01

### Added

- `skilj-bridge`: the skilj side `skilj-kafka`, `skilj-amqp` and
  `skilj-nats` share - consuming and acknowledging events, sending an
  inbound message as an external event or command trigger, reporting a
  parked delivery, partition ownership, the HTTP client and the wire
  DTOs. The bridges, and `skilj-temporal` for consume/ack and its HTTP
  client, now depend on it instead of each carrying its own copy; their
  public APIs are unchanged (the moved types are re-exported
  under the same names). A new crate to publish, after `skilj-retry`
  (docs/architecture.md §165).
- `skilj_retry::MessageRetry`/`RetryDecision`: one message's retry state
  and the park-or-wait decision after each failure, including §161's
  "another instance can" refusals. The Kafka, AMQP and NATS inbound loops
  now share it instead of each carrying its own copy
  (docs/architecture.md §161).
- `SkiljBuilder::deadline_retention(Duration)` (default 30 days) and
  `keep_resolved_deadlines_forever()`: a background task deletes
  deadlines resolved (fired, cancelled, parked or forgotten) longer ago
  than the retention, in bounded batches. Pending and firing deadlines
  are never deleted. Each `deadlines` table used to keep one row per
  deadline ever scheduled (docs/architecture.md §163).
- `skilj_retry::ANOTHER_INSTANCE_CODES`/`another_instance_can_do_it`/
  `ANOTHER_INSTANCE_RETRY_DELAY` and `skilj_core::Error::another_instance_can_do_it`:
  the refusals (`no_decider_registered`, `sync_projection_not_declared`)
  that mean "this instance can't do it, another can"
  (docs/architecture.md §161).
- `skilj_temporal::run_with_retry`/`run_until_with_retry`: `run` with a
  `skilj_retry::RetryPolicy` for failed dispatches. Once a bounded policy
  is exhausted the event is logged, acknowledged and skipped, instead of
  holding up every event behind it - a `Signal` for a workflow that has
  already completed fails on every attempt. `run`/`run_until` keep
  retrying forever, as before (docs/architecture.md §147).
- `skilj-tui --token-command` (`SKILJ_TOKEN_COMMAND`): a shell command
  printing a JWT, run at startup and again whenever the server refuses
  the token as expired - the live feed reconnects with the fresh one,
  and a refused query is retried once - so a session outlives any one
  token. `--token` is no longer required when it's given
  (docs/architecture.md §137).
- `run_outbound_until`/`run_inbound_until` in `skilj-kafka`,
  `skilj-amqp` and `skilj-nats`, and `skilj_temporal::run_until`: the run
  loops, stopping once a given future resolves - after the message or
  cycle in flight, instead of the task being aborted mid-delivery (which
  can duplicate an outbound event). The existing `run_*` functions are
  unchanged (docs/architecture.md §129).
- `Skilj::shutdown(timeout) -> ShutdownReport`: stops the background
  loops (projection/snapshot catch-up, routes, deadlines, scheduled
  events, key retention, the cross-instance listener) after the tick each
  is in, aborts any still busy at the timeout, then closes the connection
  pool. Dropping a `Skilj` still stops nothing. The template's server
  now shuts down gracefully on Ctrl-C/SIGTERM (docs/architecture.md
  §123).
- `SubmitCommandPayload.matchingEventsTruncated: Boolean` - whether a
  rejection's `matchingEvents` left out events for the
  `max_events_per_read` cap (docs/architecture.md §118).
- `SkiljBuilder::application_version(u64)`: an ever-increasing version
  stamped on every `EventType`/`CommandType`/`Projection` registration
  this process makes at startup. A process older than a registration's
  stamp leaves it untouched (reported in `ReconciliationReport::kept_newer`),
  so two versions running side by side - a rolling deploy, a rollback, an
  old instance restarting mid-rollout - no longer undo each other's
  registrations at every startup (flags, a projection's consumed event
  types and so a full rebuild each time). Unset, startup behaves as
  before. A new `registered_by_version` column is added to each bounded
  context's registration tables on first use.

### Changed

- Deadlines, cross-context routes and the Kafka/AMQP/NATS bridges' inbound
  path no longer spend their retry policy on a `no_decider_registered` or
  `sync_projection_not_declared` refusal, and never park for one: the
  work is retried after 10 seconds, when an instance that declares the
  command type or projection can take it. During a rolling deploy these
  used to use up all the attempts and park work that nothing was wrong with
  (docs/architecture.md §161).
- An event that a sync projection consumes is refused with the new
  `sync_projection_not_declared` (REST `503`) on an instance that
  doesn't declare that projection - an older version mid rolling deploy,
  say - instead of being committed with the projection skipping it for
  good. Another instance that declares it can take the write
  (docs/architecture.md §160).
- `db::resolve_data_keys_for_reading`'s accumulator is now
  `HashMap<(String, String), Option<DataKey>>`: a granted subject with no
  active key (forgotten, or never provisioned) is recorded as `None`, so
  a page of events about one forgotten subject looks its key up once
  instead of once per event (docs/architecture.md §155).
- Event subscriptions read the caller's grant, private-field grants and
  decryption keys once per batch of events rather than once per event -
  a resumed subscription replaying a thousand events used to cost
  thousands of queries (docs/architecture.md §154).
- `allEvents`/`eventsByType` refuse a `fromSequence` past the bounded
  context's latest committed sequence with `from_sequence_not_committed`,
  and `GET /v1/events` refuses such an `after` with `400`. Accepted, the
  subscription silently dropped every event up to that value, and the
  REST cursor came back unchanged, skipping them (docs/architecture.md
  §148).
- `registerProjection` (and a Rust `Projection::consumed_event_types()`)
  refuses a consumed event type named more than once, with
  `duplicate_consumed_event_type` - consumed event types are a set
  (docs/architecture.md §145).
- `GraphqlLimits` gains `websocket_init_timeout` and
  `websocket_ping_interval` (breaking for code building it without
  `..Default::default()`) (docs/architecture.md §136).
- `skilj_graphql::auth::resolve_role_from_connection_init` also returns
  the credential's expiry, `Option<(Role, SystemTime)>` (breaking for
  direct callers). New `skilj_core::access_control::verify_jwt` returns
  a `VerifiedJwt { subject, valid_until }` (docs/architecture.md §135).
- `db::fire_due_deadlines` takes the registered cancels as a new
  `cancels` argument (breaking for direct callers) (docs/architecture.md
  §131).
- `CancelDeadlineInfo` gains `deadline_schedule_source_event_type`
  (breaking for code constructing it directly) (docs/architecture.md
  §130).
- `skilj_graphql::resolvers::load_private_field_grants` takes the
  reader's `Role` (docs/architecture.md §124).
- `db::commit_command_batch` no longer takes a `pool` argument, and
  `skilj_graphql::GraphqlState` gains `parked_delivery_retry_permits`
  (breaking for direct callers) (docs/architecture.md §117).
- `db::sync_projections_for_bounded_context` is replaced by
  `db::sync_projection_names`, which runs on the caller's transaction and
  returns names; `insert_event_and_update_sync_projections_in_tx` takes
  `sync_projections: &[String]` (breaking for direct callers)
  (docs/architecture.md §116).
- `db::fire_due_deadlines` takes a `retry_policy` argument, and
  `ParkedDeliveryKind` (GraphQL `ParkedDeliveryKind`) gains `Deadline`
  (`DEADLINE`) - breaking for direct callers and for exhaustive matches
  on the kind (docs/architecture.md §115).
- `CommandBatcher::submit` and `decide_and_submit` take the command
  and projection dispatchers as `&Arc<dyn …>` instead of `&dyn …`, so
  the batch can run on its own task (breaking for direct callers;
  `Skilj`'s REST and GraphQL surfaces already hold them as `Arc`s)
  (docs/architecture.md §114).
- REST reads no longer re-walk events a filter or the token's scope
  excluded. On a page shorter than `max_events_per_read`, `GET
  /v1/events`' `nextCursor`, and an auto-advance `consume` cursor, now
  move to the highest sequence the read examined, not the last event
  served - so a narrow filter or a tenant-scoped token in a busy
  context no longer re-reads everything after its last match on every
  poll. A manual-ack cursor does the same on a call that serves
  nothing. A full page still stops at its last event. `nextCursor` can
  therefore be ahead of the last event a page contains; keep one
  `filter` for a whole walk (docs/architecture.md §112). Breaking for
  direct callers of `event_store::consume_events_page`, which takes a
  new `scanned_through` argument (`db::collect_scanned_event_page`
  supplies it).
- Authenticating a GraphQL request (HTTP, or a WebSocket's
  `connection_init`) no longer loads the whole `roles` table: it looks up
  the one active Role claiming the verified subject, through the unique
  index that already existed for it (`db::active_roles_by_external_subject`).
  The table grows with every user a deployment registers, so this was a
  per-request cost growing with it (docs/architecture.md §110).

- A tag-filtered `queryEvents` or `countEvents` no longer loads every
  event carrying the tag before paging or counting: both now walk the tag
  index a chunk (`max_events_per_read`) at a time, like their untagged
  forms. A broad tag - a customer or tenant touched by most of a bounded
  context's history - used to be loaded whole on every such call.
- **Wire:** `projection(..., waitForSequence:)` with a sequence past the
  bounded context's latest committed event is now refused at once with
  `wait_for_sequence_not_committed`, instead of polling until
  `projection_query_wait_timeout` and failing with
  `projection_caught_up_timed_out`. Any reader could otherwise make each
  query - ten per request, with aliases - cost the whole timeout in
  database polling. The legitimate wait also got cheaper: it polls one
  column, backing off from 20 ms to 250 ms, instead of a three-query
  lookup every 20 ms (~250 lookups for a 5 s wait, now about 25).
- **Behaviour:** idempotency keys now expire. A recorded key
  deduplicates for `SkiljBuilder::idempotency_key_retention` (default
  one hour, `DEFAULT_IDEMPOTENCY_KEY_RETENTION`) and is then deleted by a
  background task (about once a minute, in batches); a submission bearing
  it afterwards is a new command. Keys used to be kept forever, so the
  table grew with every keyed submission - all three bridges key every
  message. `keep_idempotency_keys_forever()` restores the old behaviour.
  Each instance starts sweeping `min(retention, 1 hour)` after it starts,
  so recovery right after an outage longer than the retention (a
  reclaimed deadline, a route re-reading its source, a broker
  redelivering an uncommitted message) still finds its keys.
  Set the retention longer than anything may retry one key: a client or
  Temporal activity retry, a bridge re-reading old broker messages, a
  `CrossContextRoute` retry policy, or a `retryParkedDelivery` redrive of
  an attempt that may have committed. Instances sharing a database are
  governed by the shortest retention among them.
- **Wire:** `parkedDeliveries` returns at most `max_events_per_read`
  rows, newest parked first (`firstFailedAt` descending; it was
  `lastFailedAt`, which moved rows on every failed retry), and pages with
  a new `after` argument taking the last row's new `cursor` field. It
  used to load the bounded context's whole parked backlog - one row per
  message when a bridge hits a poison topic - with full request bodies,
  in one response. It now also counts toward `max_expensive_fields`.
- Live `allEvents`/`eventsByType` subscriptions re-checked the caller's
  grant, resolved decryption keys and read private-field grants - two
  to three database round trips - for *every* event committed in the
  bounded context, before discovering most weren't theirs (another event
  type, a failed filter). They now check the event type, filters and
  sequence first (new pure `event_store::subscription_selects`) and do
  the database work only for events they deliver.
- **Wire:** `allEvents`/`eventsByType` with a `fromSequence` below the
  latest committed event now deliver the events committed in between
  first, in order, then the live feed, with none twice. Previously they
  only filtered live events, so the documented handoff - read back up to
  a sequence, then subscribe from it - lost every event committed
  between the read and the subscribe, and a reconnect after
  `subscription_lagged` lost the span it was meant to recover. The span
  is capped at `max_events_per_read` (type-specific for `eventsByType`);
  a longer one refuses the subscription with `resume_span_too_large`
  (read back with `queryEvents` first). Without `fromSequence` nothing
  changes.
- **Breaking (wire):** `GET /v1/events`, `GET /v1/events/consume` and
  GraphQL `queryEvents` now return at most `max_events_per_read` events
  per call (default 1000), oldest first, instead of everything after the
  cursor - and load history in chunks rather than all at once, so one
  read of a long history can no longer exhaust memory. Page on with
  `after`=`nextCursor` (`GET /v1/events`) or `afterSequence` = the last
  returned `sequence` (`queryEvents`) until a call returns no events;
  consume continues from its cursor as before. New
  `SkiljBuilder::max_events_per_read`; `skilj_rest::router` takes it as
  a new parameter. `skilj-tui`'s query view pages with `n`. The spec
  models the cap as `config.max_events_per_read`.
- **Breaking (wire):** GraphQL `fetchCommands` returns
  `[QueriedCommand!]!` (`id`, `createdAt`, `payload`) instead of
  `[String!]!`, at most `max_events_per_read` per call in the order the
  commands were recorded, and takes `afterCommandId` (the last returned
  `id`) to page on. It used to load and render the bounded context's
  whole command history, in no particular order, with a database round
  trip per command.
- Idempotency keys starting with `skilj-parked-delivery:` are now
  reserved, like `skilj-cross-context-route:` and `skilj-deadline:`, and
  a caller-supplied one is rejected with
  `reserved_idempotency_key_prefix`.
- `skilj-amqp`: `fe2o3-amqp`/`fe2o3-amqp-types` 0.17 -> 0.18 (upstream now
  ships both; verified against the real Artemis broker tests).
- `skilj-core`: `jsonschema` 0.56 -> 0.58 (checked the two intervening
  releases' changelogs against this crate's own narrow usage -
  `validator_for`/`is_valid` only, `default-features = false` - none of
  the breaking changes apply; picks up upstream fixes for a `oneOf`
  canonicalization bug and two numeric-keyword panics).
- OpenTelemetry stack 0.32 -> 0.33 (`opentelemetry`, `opentelemetry-http`,
  `opentelemetry_sdk`, `opentelemetry-otlp`, `opentelemetry-appender-tracing`)
  and `tracing-opentelemetry` 0.33 -> 0.34. No code changes needed. An
  application installing its own tracer/meter provider must move to the
  0.33 line as well: `opentelemetry`'s globals are per crate version, so a
  0.32 provider would silently receive none of skilj's spans or metrics.

### Fixed

- An instance whose code doesn't declare an async projection - an older
  version during a rolling deploy, or one sharing the database - walked
  it in background catch-up anyway, found nothing to fold, and advanced
  its `caught_up_to`. The events in between were never folded by anyone.
  Catch-up now leaves projections it doesn't declare to an instance that
  does (docs/architecture.md §160).
- Startup reconciled declared types before patching bounded contexts'
  schemas, and reconciliation reads the registration tables' current
  columns. An instance registering a type on a bounded context from
  before `private_fields` (or another later column) failed startup with
  `column ... does not exist`. Reconciliation now runs after the patches.
  Upgraded bounded contexts also now get the `CHECK` on
  `access_tokens.start_from` that a fresh one has (docs/architecture.md
  §159).
- Restarting an instance took an `ACCESS EXCLUSIVE` lock on about twenty
  tables of every bounded context (among them `events`), and a `SHARE`
  lock on several more, even when there was nothing to change: Postgres
  takes the lock for `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` and
  `CREATE INDEX IF NOT EXISTS` before checking. Queued behind any
  long-running read, each lock held up every query after it on that
  table. The startup schema patches now check the catalog first and
  run only when something is missing (docs/architecture.md §158).
- Startup warmed a bounded context's event cache before patching its
  schema. The warm-up reads `metadata_correlation_id`/
  `metadata_causation_id`, so a bounded context from before those columns
  existed failed startup before the patch adding them ran. The warm-up
  now runs last (docs/architecture.md §158).
- A bounded context hard-deleted by another instance while this one was
  starting failed `Skilj::build()`. Startup lists every bounded context
  and then warms and patches each, and the deleted one's tables were
  gone. Building the GraphQL schema's projection types had the same
  race: at startup, after a registration change, and for each caller's
  scoped schema, where it failed the request. So did `forgetSubject`'s
  sweep of every bounded context's deadlines. A bounded context that no
  longer exists is now skipped (docs/architecture.md §157).
- Catching up an async projection could deadlock against another
  instance promoting that projection's rebuild: the fold locked a state
  row and then the projection row, promotion the other way round.
  Postgres aborted one of them and the tick was retried. The fold now
  locks the projection row first (docs/architecture.md §156).
- skilj-tui's live feed resumes from now when the server refuses its
  remembered position with `from_sequence_not_committed` (the bounded
  context was recreated), instead of retrying the same refused position
  on every reconnect (docs/architecture.md §148).
- After a `Snapshot::VERSION` bump, the next event for a tag value reset
  its snapshot and folded only that event into it, so decisions using the
  snapshot ran on state missing the tag's whole earlier history. A reset
  (or new) snapshot row is now folded from the tag's full history
  (docs/architecture.md §152).
- A projection rebuild could be promoted with the wrong state when more
  than one instance catches up: an instance working from a stale snapshot
  could delete state another instance had just folded, or - after a
  `rebuildProjection` restarted a running build - keep folding the old
  state and mark the restarted rebuild caught up. Each fold now re-checks
  the rebuild's position under a lock (docs/architecture.md §151).
- A `fire_once` scheduled event type more than 1000 occurrences behind
  (a per-second schedule after ~17 minutes of downtime) never fired
  again: each scheduler tick walked 1000 occurrences from the same
  position without reaching the last. It now raises the latest due
  occurrence directly (docs/architecture.md §150).
- The Kafka, AMQP and NATS bridges park an inbound message that isn't
  JSON on its first failure instead of retrying it through the whole
  retry policy, and the parked request keeps its content (as a JSON
  string) instead of `null`. The AMQP bridge rejects a message whose
  body isn't data sections (e.g. an `amqp-value`) instead of leaving it
  unsettled, where it held link credit for good and could stall the
  receiver (docs/architecture.md §144).
- Caller-supplied idempotency keys had no length bound, though one is
  stored per accepted command; keys over 255 characters are now refused
  as `idempotency_key_too_long` (400 on REST) (docs/architecture.md §134).
- Unreleased regression from the fire-waits-for-cancels change: a
  registered cancel whose source bounded context had been hard-deleted
  failed every fire tick of the bounded context holding its deadlines,
  so none of them fired. A gone cancel source now holds nothing
  (docs/architecture.md §133).
- `forgetSubject` left pending deadlines alone, though they hold their
  target command's payload in plaintext - so one naming the forgotten
  subject still fired later, re-creating its data under a fresh key.
  Such deadlines, in every bounded context that targets the erased one,
  are now resolved with a new `forgotten` status and their payload
  cleared (docs/architecture.md §132).
- A deadline could fire although its cancelling event was committed
  before it came due - when the cancel loop hadn't processed it yet (a
  paid order still cancelled). A due deadline now waits until every
  cancel that can reach it has processed its source events up to the
  deadline's `fire_at` (docs/architecture.md §131).
- A `CancelDeadline` could lose to its own `ScheduleDeadline`: when the
  cancel loop reached the cancelling event before the schedule loop had
  created the deadline, the cancel matched nothing and the deadline fired
  anyway (a paid order still cancelled). Cancels now wait for their
  schedule to have processed every source event that precedes them
  (docs/architecture.md §130).
- Every authenticated REST request read its bearer token twice - once
  for its kind, once for the token. The token is now read once, and the
  kind only looked up to tell 401 from 403 when that read finds nothing
  (docs/architecture.md §128).
- Every event and command write compiled its type's JSON Schema anew
  (about 60 µs for an ordinary schema, against 0.2 µs to validate).
  Compiled validators are now cached by schema text, at most 256
  (docs/architecture.md §127).
- `fetchCommands` and command listings looked up each command's type
  row by row, and every GraphQL resolver returning a bounded context
  listed every role access mapping in the deployment (two lookups each)
  to show one context's grants - `boundedContexts` did that per context.
  Both now batch their lookups, and a context's grants are read for that
  context alone through a new index (migration `0005`)
  (docs/architecture.md §126).
- Reading many events resolved each command-triggered event's
  originating command row by row, several queries each - a few thousand
  queries for a 1000-event page. All multi-event reads (REST reads,
  `queryEvents`/`countEvents` beyond the cache, projection catch-up,
  cache warm-up) now resolve them in one batch (docs/architecture.md
  §125).
- Every event or command read - and every event a subscription
  delivers - loaded all private-field grants in the bounded context, with
  two or more queries per grant. Reads now load only the reader's own
  active grants, through the existing grantee index, in three queries
  however many there are (docs/architecture.md §124).
- Event filters and query tags are bounded: at most 32 filters per read
  or subscription, each value at most 4096 characters (`invalid_filter`),
  and at most 32 tags per `queryEvents`/`countEvents` (new
  `too_many_tags`). Every filter runs against every event examined, and
  every tag was its own condition in the tag-index query
  (docs/architecture.md §122).
- An `IS_LIKE` event filter allocated pattern-length x value-length
  memory for every event it examined - a gigabyte for a 10k-character
  pattern against a 100k-character value - reachable with a REST
  `EventReadToken`. Matching now runs in linear memory (bit-parallel),
  and patterns over 1024 characters are rejected as `invalid_filter`
  (docs/architecture.md §121).
- `inspectEvent`'s `origin.triggeringCommandPayload` returned the
  originating command's payload as stored - its private fields in
  plaintext to any Admin. It is now rendered as `fetchCommands` renders
  it (docs/architecture.md §120).
- `parkedDeliveries`, `retryParkedDelivery` and `discardParkedDelivery`
  returned parked requests raw - never-encrypted sensitive fields and
  private fields in plaintext - and let a scope-restricted Admin see and
  act on every owner's rows. Requests are now masked per caller, and a
  scoped Admin handles only its own owner's rows (docs/architecture.md
  §119).
- A rejected `submitCommand`'s `matchingEvents` returned events as
  stored: private fields in plaintext to any Admin, events owned by
  others to a scope-restricted Admin, and no limit on how many. They are
  now scoped, rendered and capped as `queryEvents` serves them - at most
  `max_events_per_read`, the most recent (docs/architecture.md §118).
- The rest of that stall (docs/architecture.md §117): a command's
  idempotency-key lookup, DCB conflict check and newly needed
  encryption keys, and a new sync projection's registration, now all run
  on the transaction holding the lock. Cross-context route ticks, which
  each hold a lock connection for the whole tick, are capped at half the
  pool - with as many routes as connections, none used to fire.
  Concurrent `retryParkedDelivery` calls wait for one of half the pool's
  permits before taking their lock connection.
- A lock holder no longer waits for a second pooled connection while
  its competitors hold the rest. REST `consume` read events through the
  pool while holding its per-token lock, and every event write read its
  bounded context's sync projections through the pool while holding the
  sequence lock; with as many concurrent consumers of a token, or writers
  to a context, as the pool had connections, all of them stalled until
  the acquire timeout and failed (docs/architecture.md §116).
- A deadline whose command failed with an error - say a payload its
  command no longer accepts after a deploy - was re-fired every five
  minutes forever, never surfaced to an operator, and each failure
  aborted the rest of that tick's due deadlines. Failing deadlines are
  now retried with backoff (`SkiljBuilder::deadline_retry_policy`,
  default as for routes: 5 attempts) and then parked in the target
  bounded context as a new `DEADLINE` parked-delivery kind, which
  `retryParkedDelivery`/`discardParkedDelivery` handle like a parked
  route delivery; a redrive reuses the deadline's reserved idempotency
  key, so it never runs the command twice. A cancel while a retry is
  pending still wins. Other due deadlines keep firing
  (docs/architecture.md §115).
- A client disconnecting while its command led a batch
  (`CommandBatcher`, the group commit behind REST
  `/v1/commands/trigger`, GraphQL `submitCommand` and parked-delivery
  redrive) took its batch-mates down with it. Its dropped request
  rolled back the shared transaction, and up to 255 other callers got
  `BatchFailed` for commands they never got to make. If the drop came
  after the commit, they got `BatchFailed` for commands that *had*
  committed - inviting a duplicate retry - and those events weren't
  broadcast. The batch now runs on its own task and always finishes and
  answers everyone (docs/architecture.md §114).
- Registering a new sync projection while writes were committing could
  silently lose history. The projection was stored as sync and its
  history folded afterwards; a write landing first created a key's
  state at the new event, and the fold then skipped every earlier event
  for that key. A projection with no history yet could likewise miss a
  matching event committed as it was registered. New sync projections
  are now backfilled while hidden from writers and made sync under the
  bounded context's sequence lock, so every event is folded exactly
  once. Affects startup reconciliation, the `registerProjection`
  mutation and template instantiation (`db::create_projection`). Async
  projections' `caught_up_to` also no longer moves backwards when two
  instances catch up at once (docs/architecture.md §113).
- A manual-ack `GET /v1/events/consume` that served nothing still
  claimed the checkout lease, so for the next `read_cursor_checkout_lease`
  (default 5 minutes) every poll on that token returned nothing - new
  events included - unless the client acknowledged a batch it was never
  given. An empty page now claims nothing (docs/architecture.md §112).
- Writing an event or command with a sensitive field for a *new*
  subject could panic if, at that moment, another write provisioned the
  subject's key and a `forgetSubject` destroyed it before this one read
  it back (`get_or_create_encryption_key`'s "vanishingly unlikely"
  `expect`). It now provisions again - a fresh key, as for any data
  written after an erasure.
- `skilj-tui`'s live events tab showed a bogus `null` event whenever the
  server ended its subscription with an error (`subscription_lagged`, a
  cross-instance gap, a revoked grant), then stopped updating while
  still showing itself connected, and never reconnected - after any drop
  the TUI had to be restarted. It now reports the error, marks the feed
  disconnected, and resubscribes with backoff from the last sequence it
  showed, so the server replays what it missed.
- `templates/skilj-template` didn't compile: it pinned `schemars` 0.8
  while skilj uses `schemars` 1 (so no payload satisfied `JsonSchema`),
  called `create_command_token` without its `scope` argument, and still
  named skilj 0.0.7. It now builds against 0.0.8, and CI builds it
  against the workspace (`scripts/check-template.sh`) so it can't
  silently break again.
- `docs/rest-event-reading.md` said a manual-ack batch that wasn't
  acknowledged is served again by the next fetch. For the checkout lease
  (5 minutes by default) the same token is served *nothing*; the guide
  now says so, and to retry a failed batch in memory instead.
- An instance running an **older** version of the application couldn't
  start once a newer version had registered a type with an added
  optional field or tag mapping: its startup reconciliation re-registered
  the older shape and `build()` failed with `schema_incompatible` (or
  `tag_mapping_key_dropped`). That broke rollbacks across any additive
  change, and restarts of not-yet-upgraded instances mid-rollout. When
  the stored registration is itself a valid evolution of the one the
  process declares, startup now keeps the stored (newer) registration,
  logs a warning, and lists the type in the new
  `ReconciliationReport::kept_newer`. A change neither side evolves into
  still fails, and the explicit GraphQL registration mutations are
  unchanged. The same holds for a `Projection` whose state schema a
  newer version extended.
- `skilj-nats`: retrying one inbound message for longer than the
  consumer's `ack_wait` (30 s by default; the default retry policy's
  backoff reaches minutes) had JetStream redeliver it underneath the
  retry loop, and every redelivered copy was dispatched again once the
  original finished - four dispatches for one message in the test. While
  waiting between retries the bridge now sends in-progress acks every
  half `ack_wait`, keeping the message claimed.
- `skilj-temporal`: `run` dropped the events a cycle was served when a
  dispatch failed, and relied on consuming them again - but a manual-ack
  consume claims what it serves for `read_cursor_checkout_lease` (five
  minutes by default), so the same token got nothing back until then. Any
  transient Temporal failure, including the `Signal`-before-`Start` race
  its own docs said "retries next `poll_interval`", stalled that mapping
  for the whole lease. `run` now keeps each mapping's served but
  unacknowledged events and retries them next cycle. `poll_once` is
  unchanged.
- `skilj-amqp`: when a message exhausted its retries and reporting it
  to `POST /v1/parked-deliveries` failed too, the delivery was left
  unsettled - held by the receiver's link until the connection closed, so
  never redelivered while the bridge kept running, and holding a unit of
  link credit (enough of them stalled the receiver). It is now
  *released*, so the broker redelivers it and the next round of retries
  reports it.
- `skilj-kafka`: `run_inbound` could **lose** an inbound message. When a
  message exhausted its retries and reporting it to
  `POST /v1/parked-deliveries` failed too (skilj still unreachable), it
  logged "not committing, will redeliver" and moved on - but Kafka
  offsets are cumulative, so the next message in the partition
  succeeding committed past it: never processed, never parked, never
  redelivered. The report is now retried with backoff until it succeeds
  before the bridge moves on. (`skilj-amqp`/`skilj-nats` settle each
  message individually, so an unsettled one is redelivered and was never
  at risk.)
- Archiving a bounded context stopped commands and events from the
  GraphQL and REST surfaces, but not from the library's own internal
  submissions: a `CrossContextRoute` targeting it, a deadline firing into
  it, and `retryParkedDelivery` of a route delivery all still committed
  commands and events into the archived context. Every command is now
  refused for an archived context where it is decided, whatever path it
  took; a route parks the refused delivery as usual, and a deadline whose
  target is archived is resolved without submitting (as one whose target
  command type is unregistered already was) instead of being reclaimed
  and refused every few minutes forever.
- Live `allEvents`/`eventsByType` subscriptions delivered events in the
  order they were *published* to this instance, not commit order, and
  could skip one entirely: two concurrent commits publish in either
  order, and in a multi-instance deployment another instance's earlier
  event routinely arrives (via `NOTIFY`) after this instance's own later
  one. A subscription now tracks the bounded context's sequence: on a
  jump, it loads the skipped events from Postgres and delivers them first
  (they are already committed, since commits happen in sequence order),
  and drops late arrivals it has already delivered. A jump over more than
  `max_events_per_read` events ends the subscription with
  `subscription_lagged`.
- The in-memory event cache could **silently skip or duplicate events**.
  Each committed event was appended to its bounded context's window
  unconditionally, but commits don't arrive in order: another
  instance's event (which this cache never hears of) or a concurrent
  commit's slower post-commit step can sit in between. Appending event 12
  to a window ending at 10 left a hole the next read trusted as
  complete, since the window's highest sequence matched the database's. So
  `GET /v1/events`, `GET /v1/events/consume` (whose cursor then moved past
  the missing event for good), `queryEvents`, `countEvents` and the
  cross-context-route/deadline catch-ups could all miss it. A read that
  loaded an event from Postgres just before its committing instance's own
  append added it twice. The window now only grows contiguously from
  local commits and skips events it already holds; anything else is
  filled from Postgres on the next read.
- Reading a bounded context that a concurrent `DeleteBoundedContext` had
  just removed panicked (seven `.expect()` sites in `skilj-core::db`)
  instead of returning an error; it now returns `RowNotFound` like any
  other database error.
- Follow-up sweep of the same panic shape one level deeper: reading an
  `EventType`/`CommandType`/`Command` row that a concurrent
  `DeleteBoundedContext` had just taken out from under an in-flight
  `list_events`/`list_events_from`/access-token/command-conversion call
  also panicked instead of erroring (11 more sites, via new
  `require_event_type`/`require_command_type`/`require_command`
  helpers). Two lookalike sites were checked and deliberately left as
  panics: their own batch query already fails loudly on a dropped schema
  before ever reaching the lookup, so a miss there is a genuine data
  inconsistency, not this race.
- Same shape again, in `skilj-graphql`'s `retryParkedDelivery`: a
  `CrossContextRoute`-kind `ParkedDelivery`'s own `target_bounded_context`
  can be hard-deleted independently of (and long after) the context the
  delivery is parked and retried in, and the `ExternalEvent`/
  `CommandTrigger` token lookups have the same gap for their own,
  narrower window. All three used to panic on a stale reference; retrying
  a stranded delivery now fails gracefully and leaves the row parked,
  like any other failed redrive. New `skilj_core::error::Error::
  row_not_found()` gives `skilj-graphql` the same error shape `db`'s own
  `require_*` helpers use, without a new `sqlx` dependency just for it.
- Same shape a third time, in four `skilj-graphql` mutations/queries that
  each write or read a `BoundedContext` and then immediately reload it
  with a nested `.expect("...it must still be there")`:
  `archiveBoundedContext`, `deleteBoundedContext`,
  `resyncBoundedContextFromTemplate`, and - the widest window of the
  four, an admin "list everything" query looping over every listed
  context doing real per-item work - `boundedContexts`. All four can
  have a concurrent `deleteBoundedContext` land in the gap; all four now
  return an ordinary `BoundedContext_not_found` GraphQL error instead of
  panicking. A fifth, identical-looking site in `addBoundedContext` was
  checked and left alone: `deleteBoundedContext` requires `Archived`,
  and a context this call just inserted is `Active`, so that one
  genuinely cannot race - documented inline instead of "fixed" into a
  false positive.
- Same shape a fourth time, in `skilj-rest`'s `resolve_token` - the
  highest-traffic version found yet, since it runs on *every*
  authenticated REST request, not just an occasional admin action.
  `access_token_kind` and the matching `TokenLookup::get` each
  independently re-resolve a bearer token's own bounded context; a
  concurrent `DeleteBoundedContext` landing between the two used to
  panic with "a real bug" instead of the ordinary `401
  UnrecognisedCredential` a caller whose token has genuinely vanished
  should see either way.
- Same shape a fifth time, and the most serious: async `Projection`
  catch-up. `catch_up_bounded_context` re-reads each `building`
  `ProjectionRebuild` after its fold pass and `.expect()`ed it was still
  there, but another instance's overlapping tick promoting it, or a
  `DeleteBoundedContext` landing mid-tick, removes it. Because every
  bounded context's catch-up runs on the one task `SkiljBuilder::build()`
  spawns, that single panic silently stopped async projection catch-up
  for the whole process until restart. The vanished rebuild is now
  skipped. Two lookups of a projection's consumed event types
  (`consumed_event_types`/`rebuild_consumed_event_types`, behind
  `get_projection`/`get_projection_rebuild`) had the same race and now
  return `RowNotFound` via `require_event_type`. New regression test
  uses a Postgres trigger to land the removal in the exact gap every run.
- A bounded context added at runtime (`addBoundedContext`) lacked
  `parked_deliveries`' unique index until the next restart, so every
  `insert_parked_delivery` into it (a `CrossContextRoute` exhausting its
  retries, or `POST /v1/parked-deliveries`) failed on its `ON CONFLICT`.
  Provisioning now creates the index itself.
- `retryParkedDelivery` panicked on a stored `request` body that didn't
  match its kind's wire shape. It now returns an ordinary
  `invalid_parked_delivery_request` error and leaves the row parked.
- JWT authentication rejected concurrent requests with
  `unknown_signing_key` while the IdP's JWKS was being fetched. The first
  request to miss the key cache started the fetch; every other miss
  during it was turned away by the refetch rate limit. So each cold
  start, and each IdP signing-key rotation, 401'd whatever requests
  arrived during that fetch. Concurrent misses now wait for the one
  in-flight fetch and use its result.
- `retryParkedDelivery` could redrive one parked delivery twice: two
  concurrent retries of the same row (a double-click, two operators, a
  client retrying after a timeout) each read it and each resubmitted it.
  Retries of one row are now serialized; the loser gets
  `ParkedDelivery_retry_in_progress` or `ParkedDelivery_not_found`.
  Command-kind redrives also now carry an idempotency key, so a redrive
  whose command committed but whose row delete failed doesn't land a
  second time. A `CrossContextRoute` redrive reuses the route's own
  client id (`cross-context-route`, previously `parked-delivery-retry`)
  and the exact key the route's original attempt used, so an attempt
  that committed despite reporting a failure dedupes too. An
  `ExternalEvent` redrive without a bridge-supplied `dedupe` cursor gets
  one of its own (per parked row), so it can't land twice either.
- A parked `CommandTrigger` delivery was redriven without the
  `Idempotency-Key` its original request carried (a header, so not part
  of the parked body). If that original attempt had committed and only
  the response was lost, `retryParkedDelivery` created the command a
  second time. `POST /v1/parked-deliveries` now accepts `idempotencyKey`
  (the original header) for `command_trigger` reports, and the redrive
  reuses it. `skilj-kafka`, `skilj-amqp` and `skilj-nats` send it.
- `ProjectionQuery`: two projections whose generated GraphQL type names
  collided (`{bounded_context}_{projection}` is ambiguous once either
  contains `_`) were both served, one silently rendered through the
  other's type. A projection or state field whose name isn't a valid
  GraphQL name produced a schema clients couldn't parse. Such a
  projection is now left out of the schema with a logged reason, and
  querying or subscribing to it returns `projection_not_in_schema`; an
  unusable or duplicate field name drops just that field.

- Several paths loaded a bounded context's entire event history into
  memory: `countEvents`, every event subscription (on each subscribe and
  reconnect, only to find the latest sequence), projection registration
  (including every projection's reconciliation at startup, only to check
  whether any consumed event exists), `CrossContextRoute`/deadline
  catch-up ticks, async projection/rebuild and snapshot catch-up, and a
  new sync projection's history fold. Each now reads a chunk at a time,
  uses a single aggregate query, or (catch-up) handles at most 1000
  events per tick and continues on the next.

- `event_cache_warm_up_count(0)` - a natural way to turn the event
  cache off - made every cache-backed read (`GET /v1/events`, consume,
  `queryEvents`, catch-up) silently return no events, after loading the
  whole history on each call. Zero now disables the cache (reads go to
  Postgres). A cold cache window (e.g. a bounded context added at
  runtime) now fills from the recent tail instead of loading the whole
  history. `event_broadcast_capacity(0)` no longer panics in `build()`.
- `skilj-retry`: `next_backoff` panicked for a `max_backoff` of
  `Duration::MAX` or a negative multiplier - inside background retry
  loops, stopping them - and callers storing a due time turned a
  backoff too large for chrono into a zero delay (an immediate, endless
  retry) or overflowed the addition. `next_backoff` is now total, and
  the new `RetryPolicy::next_attempt_at` saturates; the cross-context
  route and the Kafka/AMQP/NATS bridges use it.
- `skilj-codegen` validated nothing beyond the TOML's shape: an invalid
  type or bounded-context name panicked inside `build.rs`, a field named
  `type` was reported as a bug in skilj-codegen, duplicates and name
  collisions surfaced as rustc errors inside generated code, and a tag
  naming an undeclared field only failed at startup registration. Each
  is now a named problem in a new `Error::Invalid`, all reported at
  once; keyword field names (`type`) are supported as raw identifiers.
- Re-registering a scheduled `EventType` (as every instance does at
  startup) wrote back the `schedule_position`/`last_fired_at` it had
  read, so a scheduler firing in between was undone and the occurrence
  fired again. Registration now leaves both to the scheduler, except
  for the position set when scheduling is newly enabled.
- `POST /v1/events/consume/ack` ran outside the per-token lock consume
  uses, so two concurrent acknowledgements could both pass the "no
  regression" check and the lower one land last - moving a manual-ack
  cursor backwards and redelivering events - and an ack could interleave
  with a consume's claim. Acks are now serialized with consume.
- When an instance's cross-instance listener lost its Postgres
  connection, it reconnected silently and every `NOTIFY` sent meanwhile
  was lost unnoticed: a type or bounded context registered on another
  instance stayed missing from this instance's GraphQL schema until some
  later registration change, and live `allEvents`/`eventsByType`
  subscriptions here silently skipped the events other instances
  committed in the gap. The listener now reports the loss
  (`cross_instance::Message::Resync`, after re-listening); the instance
  rebuilds its schema and template cache, and live event subscriptions
  end with `subscription_lagged` - as on a local lag - so clients resume
  from their last sequence. `projectionUpdates` refetches instead. New
  `EventBroadcaster::signal_gap`/`subscribe_gaps`.
- No outbound HTTP request had a timeout. The GraphQL JWKS fetch runs
  under a single-flight lock, so an IdP endpoint that accepted the
  connection and then stalled held that lock forever: every later
  cache miss queued behind it, and after the next key rotation no one
  could log in until restart. The fetch is now bounded
  (`JwksCache::DEFAULT_FETCH_TIMEOUT`, 10 s; `with_fetch_timeout`). Likewise
  a request stuck on a half-open connection stalled a Kafka/AMQP/NATS
  outbound loop or `skilj_temporal::run` forever; each crate now has
  `http_client()` (30 s request, 10 s connect timeout, `HTTP_REQUEST_TIMEOUT`)
  which its own loops use and which `run_inbound`'s documentation asks
  callers to pass.
- A panic in application plugin code run by a background task (a
  `Projection::project` or `Snapshot` fold, a `CrossContextRoute`, a
  deadline, a scheduled event's projections) ended that task for every
  bounded context for the rest of the process's life: one buggy async
  projection in one bounded context silently stopped async projection
  catch-up everywhere. Each bounded context's (or route's) unit of work
  now runs with its panic contained: logged at `error`, counted as
  `skilj.background_task.errors{reason="panicked"}`, rolled back and
  retried next tick, while every other unit carries on.

### Security

- A deadline kept its target command's plaintext payload after it
  resolved (fired, cancelled or parked), for good, and `forgetSubject`
  only cleared pending deadlines. So a forgotten subject's data stayed
  readable in every deadline that had already fired for it. Resolving a
  deadline now clears its payload, and `forgetSubject` also clears the
  payload of an already-resolved deadline naming the subject
  (docs/architecture.md §162).
- `parkedDeliveries` and `forgetSubject` looked up a parked delivery's
  target type or token for every row they examined - several queries per
  row, over every parked row for a scoped grant filling a page. The
  lookups are now memoized per target for the length of a scan
  (docs/architecture.md §153).
- `queryEvents`, `countEvents`, `allEvents` and `fetchCommands` look up
  each distinct name in `eventTypes`/`commandTypes` once. A list
  repeating one name made a query per entry - some 200,000 for a
  request body's worth (docs/architecture.md §143).
- A token id alone no longer tells anyone its token exists. `revokeToken`
  answered `AccessToken_not_found` for a missing id and something else
  for a real one, even with no credential; it now requires a caller and
  refuses both alike. The REST routes' `403 wrong_token_variant` was
  given for another kind's id whatever the secret; a wrong secret is now
  always `401 unrecognised_credential` (docs/architecture.md §142).
- `POST /v1/events/external` and `POST /v1/events/direct` now check the
  token and payload before touching the database. A revoked token, or
  any payload failing its schema, could create an encryption key for
  every subject value it named, and took the bounded context's sequence
  lock; a revoked adapter's redelivery was answered `redelivered`
  instead of refused (docs/architecture.md §141).
- `GET /v1/events/consume` and `GET /v1/events` now refuse a revoked
  token, an invalid filter or a type closed to reads before loading any
  events. A new `Latest`/`AtTime` consumer's start position is found by
  scanning the event type's whole history, and that scan ran first - so
  a revoked credential, or a first call missing `mode`, could make every
  request scan it (docs/architecture.md §140).
- The superadmin-only GraphQL mutations (`deleteBoundedContext`,
  `createBoundedContextFromTemplate`, `resyncBoundedContextFromTemplate`,
  `grantRoleAccessMapping`, and the role ones) now refuse a
  non-superadmin before looking anything up. They answered `not_found`
  for a missing bounded context but `not_superadmin` for an existing one,
  so any authenticated Role could probe which bounded contexts exist;
  `createRole`/`grantRoleAccessMapping`/`addBoundedContext` also read a
  whole table before refusing (docs/architecture.md §139).
- GraphQL introspection no longer lists other tenants' bounded contexts.
  The schema names a type after every bounded context with a projection
  and introspection needed no credential, so anyone could enumerate the
  bounded contexts `boundedContexts` keeps to superadmins, with their
  projections' shapes. Now a superadmin is served the full schema, any
  other Role one with only the bounded contexts it has access to, and a
  caller with no credential one with none and no introspection. Querying
  another bounded context's projection type now fails validation, the
  same as for one that doesn't exist (docs/architecture.md §138).
- A GraphQL websocket that never sends `connection_init` is closed after
  `GraphqlLimits::websocket_init_timeout` (default 10s, close code 4408),
  and the server pings every `websocket_ping_interval` (default 30s),
  dropping a connection that has sent nothing - not even the automatic
  pong - for two intervals. Both kinds used to be held open forever, a
  vanished client's subscriptions with them. Clients that treat any
  non-text frame as an error must skip ping frames; skilj-tui does
  (docs/architecture.md §136).
- A GraphQL websocket now closes (code 4403, "credential expired") when
  the JWT from its `connection_init` expires, `exp` plus the usual
  leeway. The token was only checked when the connection opened, so the
  connection and every subscription started on it later kept serving its
  role indefinitely. Clients reconnect with a fresh token
  (docs/architecture.md §135).
- A JWT whose `nbf` (not before) claim is still in the future is now
  refused. `jsonwebtoken` skips that check unless asked, so a token
  issued to become valid later was accepted immediately. A token with no
  `nbf` is unaffected; the check allows the same clock leeway as `exp`
  (docs/architecture.md §111).
- The superadmin bootstrap secret didn't end at its first claim: each
  process kept it in memory and only refused it while an active
  superadmin existed, so after every superadmin was revoked the
  originally printed secret - possibly days old in shipped logs - worked
  again on every process that hadn't restarted. It is now consumed by the
  first claim (and by any claim once a superadmin exists); a restart while
  no active superadmin exists prints a new one, as the spec intends.
  Separately, two concurrent claims (e.g. on two instances, each with its
  own secret) could both succeed; the "no active superadmin" check now
  runs atomically with the insert. `Skilj::bootstrap_secret()` now returns
  `Option<String>`.
- Startup re-registration could silently **remove** a type's
  protections for every instance: an older version of the application
  (restarting mid-rollout, or rolled back to) that didn't declare a
  sensitive field, private field or owner tag key a newer version had
  added took it out of the shared registration - so the field was stored
  in plaintext from then on (and beyond `forgetSubject`'s reach), shown
  to every reader, or the type's owner scoping switched off. Startup
  now keeps any protection the stored registration has that the process
  lacks (logged, reported in the new
  `ReconciliationReport::kept_protections`); removing one deliberately
  takes the explicit GraphQL registration mutation.
- Hard-deleting a bounded context frees its name for reuse, but the
  in-memory event cache keyed its windows by name and never evicted
  them: a bounded context recreated under the same name (for example a
  new tenant given a deleted one's name) was served its **predecessor's**
  cached events by every cache-backed read - `GET /v1/events`, consume,
  `queryEvents`, `countEvents`, command decisions - on every instance
  that had cached them, and once the new context's sequence passed the
  old window its own events were mixed in. Each window now records the
  identity of the `events` table it was filled from (read together with
  the latest sequence, in the same query) and is refilled when that
  changes.
- REST and GraphQL error responses carried raw database error text in
  `message` - Postgres messages naming schemas (`bc_<bounded context>`,
  i.e. tenant names), constraints and SQL fragments - including to a
  caller whose token lookup failed before authentication completed. A
  `database_error` (or `migration_error`) now answers with a generic
  message (a vanished row and an exhausted connection pool keep their own
  distinguishable wording), and the raw cause is logged at `error` in the
  request's span, findable by the `trace_id`/`traceId` the response
  carries. New `SkiljRejection::internal_detail`.
- A **scoped** admin grant (one owner/tenant's view of a bounded context)
  could escape its scope through REST tokens: it could mint an
  unrestricted or another owner's `EventReadToken`/`ExternalEventToken`/
  `DirectCreationToken`/`CommandToken` and read or write every owner's
  records with it, and revoke other owners' tokens. A scoped admin now
  mints only tokens with its own scope - an omitted scope inherits it,
  another is refused with `token_scope_beyond_grant` - and revokes only
  tokens carrying its own scope. Unscoped (staff) admins are unaffected.
- `forgetSubject` left the forgotten subject's data in plaintext in
  `parked_deliveries`: a parked row holds the request exactly as it was
  submitted (a bridge's event or command body, a route's target command
  payload), never encrypted, so destroying the subject's key didn't touch
  it. Any admin could still read it via `parkedDeliveries`, and
  `retryParkedDelivery` would re-create the subject's data under a fresh
  key. `forgetSubject` now first deletes every parked delivery in the
  bounded context whose request names the subject through its target
  type's sensitive fields (or, when that type can't be resolved, mentions
  the subject value anywhere).
- **Breaking (API):** GraphQL JWT verification ignored the `aud` claim
  entirely (`validate_aud = false`), so a token the configured IdP
  issued to *any other* application - same issuer, same signing keys,
  same `sub` - was accepted as that user, with that user's skilj Role.
  `IdpConfig::new` now takes the expected audience
  (`new(jwks_endpoint, issuer, audience, signing_algorithm)`), more can
  be added with `IdpConfig::with_additional_audience`, and a token
  whose `aud` matches none of them, or that has no `aud`, is rejected
  with `jwt_verification_failed`. Set it to the client id this
  deployment has at the IdP.
- `schema_ident` now escapes `"` in a bounded context name when quoting
  it as a Postgres schema identifier. Not reachable today (every name is
  validated at creation and looked up before use), but the quoting was
  documented as a second line of defence and wasn't one.
- `/graphql` had no per-request limits. The body was read whole, with no
  size cap, before authentication (an unauthenticated multi-megabyte
  request was read and executed; multipart parts were spilled to temp
  files unbounded), queries had no depth or complexity limit, and
  aliasing one history-reading field many times multiplied the per-read
  page cap. New `SkiljBuilder::graphql_limits(GraphqlLimits { .. })`:
  `max_request_body_bytes` (default 2 MiB, `413` beyond it),
  `max_depth` (24), `max_complexity` (2000) and `max_expensive_fields`
  (10 selections of `queryEvents`/`countEvents`/`fetchCommands`/
  `projection` per request, aliases included; `query_too_expensive`
  beyond it). The defaults fit the standard introspection query. The
  body cap also applies to each GraphQL websocket message, which used to
  accept axum's default 64 MiB, unauthenticated. And one websocket
  connection may run at most `max_subscriptions_per_connection`
  subscriptions at once (default 100; `too_many_subscriptions` beyond
  it).
- `POST /v1/parked-deliveries` could be used by one bridge credential to
  overwrite another credential's parked delivery: the dedup key was
  `(source, kind, identifier)` only, so reporting the same (guessable)
  identifier upserted the caller's own `request` body onto the other
  token's row, which `retryParkedDelivery` then redrives under that
  *other* token's authority. The unique key now includes the access
  token; a startup migration moves existing schemas onto it and drops
  the old index. The route also now rejects a revoked token (`403`,
  nothing downstream re-checks it) and a `request` body that isn't its
  kind's route shape (`400 invalid_parked_delivery_request`).

- Lockfile bump of `rustls` 0.23.44 -> 0.23.45 for RUSTSEC-2026-0285
  (TLS 1.3 handshake messages accepted across encryption level
  boundaries). Reached only through optional/dev TLS paths (OTLP export
  over `reqwest`, `fe2o3-amqp`, testcontainers' `bollard`); no source
  change needed.

## [0.0.8] - 2026-09-19

### Added

- Group-commit command batching: concurrent commands to the same bounded
  context now share one lock acquisition and one commit, raising measured
  single-bounded-context throughput from ~27/s to ~45-60/s in the
  `skilj-helpdesk` load test. Includes several rounds of round-trip cuts
  inside the held lock (Codeberg issues #32, #35, #36). See
  [docs/architecture.md §58-§62](docs/architecture.md) and the new
  [docs/performance.md](docs/performance.md).
- New builder options `command_batch_max_size` (default 256) and
  `command_batch_max_concurrent_leaders` (default half the pool), plus
  `CommandBatcher::with_max_batch_size` / `with_max_concurrent_leaders` and
  `command_batcher::DEFAULT_MAX_BATCH_SIZE`.
- `command_batch_idle_in_transaction_timeout` builder option: a backstop
  that lets Postgres kill a stuck batch leader and release the
  bounded-context lock.

### Changed

- **Breaking (pre-1.0):** `Error::BatchFailed` is now
  `BatchFailed { code, message }` instead of `BatchFailed(String)`. Its
  `code()` reports the original error's code rather than a fixed
  `database_error`.
- Per-batch timing log lines are now `debug` instead of `info`.

### Fixed

- A batch leader whose request future was dropped (client disconnect,
  timeout) could leave later commands to that bounded context waiting
  forever; stranded followers now get a retryable `BatchFailed` and the
  next request becomes leader.
- More than the max batch size queued commands could leave the remainder
  unprocessed forever; the leader now keeps draining until the queue is
  empty.
- Batch leaders are capped so they cannot exhaust the connection pool
  waiting on each other.

## [0.0.7] - 2026-09-17

### Added

- `Metadata` on commands and events gains `correlation_id`/`causation_id`:
  a durable, queryable thread from a submitted command through every
  event and cross-context command it triggers, distinct from the
  ephemeral OTel trace id already carried on REST/GraphQL error
  responses. Every stored `Command` ends up with a `correlation_id`
  (generated when the caller omits one); `causation_id` is `null` for a
  root command and set to the causing event's own stable name otherwise.
  See [docs/architecture.md §44](docs/architecture.md#correlation-causation-ids)
  (Codeberg issue #18).
- New `skilj-test-fixture` crate: a given/when/then harness for
  exercising a plugin's own `decide()`/`project()` directly, in memory -
  no Postgres, no license required for a full HTTP round trip just to
  check whether a decision function returns the right event. See
  [docs/architecture.md §45](docs/architecture.md#given-when-then-test-fixture)
  (Codeberg issue #19).
- Native one-shot, per-entity deadlines: `plugin::ScheduleDeadline`/
  `CancelDeadline` schedule a deferred command tied to a specific
  entity's own tags - "cancel this order if it has no time to die within
  30 minutes without payment" - and cancel it if the awaited event
  happens first, without reaching for the much heavier `skilj-temporal`
  for a single deferred command. See
  [docs/architecture.md §46](docs/architecture.md#native-deadlines)
  (Codeberg issue #20).
- Dead-letter/parking for failed event and message handler delivery:
  `CrossContextRoute`'s catch-up loop and the `skilj-kafka`/`skilj-amqp`/
  `skilj-nats` bridges now share one backoff/attempt-cap policy (new
  `skilj-retry` crate) instead of retrying a poison delivery forever on
  every poll tick, blocking every occurrence behind it. Past the attempt
  cap, the occurrence is parked to an operator-visible surface instead
  of looping - a failing delivery finally gets to live and let die,
  rather than take the whole route down with it. See
  [docs/architecture.md §47](docs/architecture.md#parked-deliveries)
  (Codeberg issue #21).
- New GraphQL subscription `projectionUpdates`: the subscription
  counterpart to `ProjectionQuery`, pushing the already-computed
  `Projection` state itself so a client wanting "push me the current
  balance whenever it changes" no longer has to subscribe to raw events
  and reimplement `project()`'s own fold client-side. See
  [docs/architecture.md §49](docs/architecture.md#49-a-graphql-subscription-for-projections-projectionupdates-codeberg-issue-23)
  (Codeberg issue #23).
- Segmented/parallel event processing: async `Projection` and `Snapshot`
  catch-up can now be spread across a fleet of instances, each claiming
  and processing its own segment via `pg_advisory_xact_lock`, for
  workloads where one instance redundantly redoing the same idempotent
  work isn't enough - the world, as it turns out, is not always enough.
  The `skilj-kafka`/`skilj-amqp`/`skilj-nats` bridges gain the same
  per-partition split, on top of a newly-closed double-publish gap
  (`ReadCursor.checked_out_at`) where two concurrent instances of the
  same mapping could otherwise both be served, and both act on, the
  identical unacknowledged batch. See
  [docs/architecture.md §50](docs/architecture.md#50-investigation-segmentedparallel-event-processing-for-horizontal-scale-out-codeberg-issue-25)
  through
  [§54](docs/architecture.md#54-bridge-partitioning-skilj-kafkaskilj-amqpskilj-nats-on-top-of-53)
  (Codeberg issue #25).

### Fixed

- A real cross-instance double-fold bug in async `Projection` catch-up,
  found while checking issue #25's "is the redundant work actually
  idempotent?" premise directly instead of trusting the existing design
  notes: two instances racing the same fold could each apply an
  overlapping batch of events, double-counting some of them. See
  [docs/architecture.md §50](docs/architecture.md#50-investigation-segmentedparallel-event-processing-for-horizontal-scale-out-codeberg-issue-25).
- A fire-vs-cancel race in `CancelDeadline` (§46): `fire_due_deadlines`
  could still exercise a deadline's license to fire its command after a
  concurrent `catch_up_cancel_deadline` tick had already revoked it,
  because the row was only marked `'fired'` after the side effect ran.
  See [docs/architecture.md §55](docs/architecture.md#55-closing-the-canceldeadline-fire-vs-cancel-race).
- A duplicate parked-delivery race in `catch_up_cross_context_route`
  (§47): two instances polling the same route concurrently could both
  read the same near-exhausted retry state and both park a row for the
  identical failure - not quite diamonds are forever, but two rows for
  one real failure was one too many. Retry-state reads and writes are
  now lock-guarded and atomic. See
  [docs/architecture.md §56](docs/architecture.md#56-closing-the-parked-delivery-duplication-race-in-catch_up_cross_context_route).

## [0.0.6] - 2026-09-13

### Added

- `EventReadToken` and `CrossContextRoute` no longer unconditionally start
  at the very beginning of history. `createEventReadToken` gains an
  optional `startFrom` (`BEGINNING` default, unchanged; `LATEST` skips
  straight past current history so only events from then on are delivered),
  plus `startAtSequence`/`startAtTime` for replaying from a chosen
  mid-history point - exactly one of the four may be given. The Rust-only
  `plugin::CrossContextRouteStartFrom` trait const gives a route the same
  four choices (`Beginning`/`Latest`/`AtSequence`/`AtTime`), seeded on its
  first-ever catch-up tick. Wiring a new side effect (e.g.
  `UserRegistered` -> send a welcome email, or a new `EventTypeMapping`
  onto a Temporal workflow) against an event type with existing history no
  longer replays every historical occurrence through it by default. See
  [docs/architecture.md §43](docs/architecture.md#43-stopping-a-new-subscriber-from-replaying-all-of-history).

### Changed

- `docs/architecture.md`'s top-level sections now carry stable,
  number-independent anchors instead of GitHub's auto-generated
  number-dependent slugs, so a `§NN` citation elsewhere in the repo no
  longer breaks the next time a section is added, removed, or
  renumbered. Every existing `§N`/`§N.M` citation - in `docs/architecture.md`
  itself, other `.md` files, and Rust `///`/`//!` doc comments - now links
  against the new anchors. New `scripts/check-section-refs.sh`, wired
  into CI, scans every tracked file for `§N` citations and fails the
  build if a top-level number no longer resolves to a current heading.
  No behavior change.

## [0.0.5] - 2026-09-08

### Added

- Optional `dedupe: { partitionKey, sequence }` on `POST /v1/events/external`
  (`ExternalEventIngestion`): lets an adapter for an at-least-once
  message source (Kafka, Kinesis, Pulsar, and similar partitioned-log
  systems) submit a message's own partition and sequence number so a
  redelivery after a crash-before-commit creates no second event,
  instead of the caller needing to build this themselves. A compact
  per-`(adapter, partition)` watermark, not a row per message - sound
  because these sources already guarantee strictly increasing delivery
  order within one partition. Response gains `redelivered: bool`;
  `sequence` is `null` on a redelivery. Omitting the pair changes
  nothing. See [docs/architecture.md §39](docs/architecture.md#external-message-dedup-create-external-event).
- New `skilj-kafka` crate: a bridge between skilj's own event stream and
  Kafka, both directions. Outbound, a mapped `EventType` is produced to
  a Kafka topic (message key derived from a DCB tag) via the same
  `GET /v1/events/consume`/`POST /v1/events/consume/ack` mechanism
  `skilj-temporal` already uses. Inbound, a Kafka topic maps to either
  `ExternalEventIngestion` (redelivery-safe via the `dedupe` mechanism
  above) or `CommandTrigger` (redelivery-safe via `Idempotency-Key`),
  a per-mapping choice - both derive their own key from the message's
  own `"{topic}:{partition}"`/offset. See [docs/architecture.md §40](docs/architecture.md#skilj-kafka-bridge).
- New `skilj-amqp` crate: `skilj-kafka`'s own sibling for any AMQP 1.0
  broker (Solace PubSub+, Azure Service Bus, ActiveMQ Artemis) via
  `fe2o3-amqp` (pure Rust). Outbound, a mapped `EventType` is sent to an
  AMQP address with a DCB tag as `group-id` and the event's own sequence
  as `group-sequence`. Inbound, an address maps to `ExternalEventIngestion`
  or `CommandTrigger`; both use the message's own `group-id`/
  `group-sequence` or `message-id` properties for redelivery safety when
  the sender populated them (optional in AMQP 1.0, unlike Kafka's own
  broker-guaranteed offsets - omitted gracefully when absent, never an
  error). See [docs/architecture.md §41](docs/architecture.md#skilj-amqp-bridge).
- New `skilj-nats` crate: a third bridge sibling, for NATS JetStream via
  `async-nats`. Outbound, a mapped `EventType` is published to a subject
  with a DCB tag as a `Skilj-Correlation-Key` header and
  `"{bounded_context}:{sequence}"` as `Nats-Msg-Id` (getting JetStream's
  own native, server-side idempotent-publish deduplication for free).
  Inbound, a subject maps to `ExternalEventIngestion` (always
  redelivery-safe - JetStream's own `(stream, stream_sequence)` is
  broker-assigned on every message, never optional) or `CommandTrigger`
  (via `Nats-Msg-Id` when an upstream sender populated one). See
  [docs/architecture.md §42](docs/architecture.md#skilj-nats-bridge).

## [0.0.4] - 2026-09-04

### Security

- `ProjectionQuery` had no whole-projection access gate: a projection
  could declare a `team`-kind private field on the events/commands it
  was built from, but nothing stopped a Role of any name from reading
  the projection itself. New `Projection.TEAM_ONLY` (deployed
  configuration only, not a spec field) rejects a query unless the
  caller's Role carries the required name, composing with the existing
  owner-scope check rather than replacing it (Codeberg issue #17).
- The `projectionSchema` GraphQL field was missed by the fix above: a
  Role off the required team could still read a `TEAM_ONLY` projection's
  declared name/schema/schemaVersion through it even though `projection`
  itself correctly refused the same projection's data. Now gated
  identically to `projection`.
- `CrossContextRoute`'s own background task (added below) derived its
  idempotency key as `"{route.name}:{sequence}"` into the same shared
  `idempotency_keys` table `submitCommand`/command triggering write
  into - which has no caller/client_id column at all. A Write-level
  caller who knew or guessed a route's name and an upcoming sequence
  number could pre-plant that exact key, causing the route's real
  delivery to be silently deduplicated away with no error. Caught and
  fixed before this release: a reserved `skilj-cross-context-route:`
  key prefix for the route's own internal use, rejecting any
  caller-supplied idempotency key that uses it at both the REST trigger
  and `submitCommand` GraphQL wire boundaries. See docs/architecture.md
  [§36](docs/architecture.md#cross-context-route).
- Investigating the fix above surfaced a second, more serious,
  **already-shipped** issue in `idempotency_keys` (live since 0.0.2, not
  new to this release): its key was `(command_type_name,
  idempotency_key)` only, not scoped per caller at all. In a bounded
  context with multiple tenants (owner-tag scoping,
  `RoleAccessMapping`/`CommandToken` `scope`) submitting the same
  `CommandType`, two unrelated tenants choosing the same idempotency-key
  string - plausible with business-derived keys (an order id, an
  invoice number), no attacker needed - would silently collide: the
  second tenant's real command never ran, and they received sequence
  numbers belonging to the first tenant's own event stream. Fixed by
  scoping `idempotency_keys`' key to `(command_type_name, client_id,
  idempotency_key)`, migrated onto every already-provisioned bounded
  context (self-hosted deployments on 0.0.2/0.0.3 should upgrade).
  **Breaking, deliberately, for one edge case**: a genuine retry of a
  request submitted just before this migration runs will no longer be
  recognised as a duplicate (pre-migration rows are retired, not
  reused) - accepted in exchange for fully closing the collision above
  rather than leaving any part of it open. See [docs/architecture.md §37](docs/architecture.md#idempotency-keys-client-id-scoping).

### Added

- `plugin::CrossContextRoute`: a single-hop, stateless reaction - when a
  registered `EventType` commits in its own bounded context, submit a
  `CommandType`'s own payload into a *different* bounded context, going
  through that command's real `decide()`/`submit_command` path like any
  other caller. The answer to "cross bounded contexts without an
  external system like Temporal" for the common single-hop case;
  deliberately not a Saga/process manager - a route that needs more than
  one hop should reach for the `skilj-temporal` pairing instead. Driven
  by a new shared background task (`SkiljBuilder::cross_context_route::<R>()`,
  `.cross_context_route_poll_interval(Duration)`) with its own durable,
  per-route cursor and idempotency-keyed submissions (the Security
  entry above covers a namespace-collision issue in this mechanism,
  found and closed before release), so a redelivered/retried catch-up
  tick is exactly as safe as any other idempotency-keyed submission
  already is. See [docs/architecture.md §36](docs/architecture.md#cross-context-route).
- `skilj_core::db::decide_and_submit_command`: the "optimistic `decide()`,
  then locked `submit_command`" sequence `skilj-rest`'s command-trigger
  route and `skilj-graphql`'s `submitCommand` resolver each ran inline is
  now one shared, real-Postgres-tested function, used by both of them
  and by `CrossContextRoute`'s own background task.
- `plugin::upcast_payload`/`UpcastStep`: sugar for a hand-written
  `BoundedContextEvent::try_from_event` that needs to interpret a
  payload differently depending on which schema_version it was written
  under (`Metadata.version`) - a declared chain of pure JSON transforms
  instead of a repeated `match` per event type. Doesn't relax
  `schema_is_backwards_compatible`'s additive-only contract or touch
  registration in any way; operates entirely on the raw JSON before
  final deserialization (Codeberg issue #14).
- `docs/temporal-integration.md`: documents pairing skilj with Temporal
  via its existing `Idempotency-Key` on `submitCommand`/command
  triggering, keyed by `{run_id}:{activity_id}` per Temporal's own
  documented Activity-idempotency guidance - no new skilj code, phase 1
  of the plan in [`docs/architecture.md` §34](docs/architecture.md#skilj-temporal-plan).
- New `skilj-temporal` crate (phases 2-3 of the same plan): a bridge
  from skilj's own event stream (`GET /v1/events/consume`/`POST
  /v1/events/consume/ack`) to Temporal's client API, starting or
  signaling a workflow execution correlated by a fixed
  `"{bounded_context}:{tag_key}:{tag_value}"` convention over a
  configured DCB tag. Speaks only Temporal's thin `temporalio-client`
  gRPC client, never the full `temporalio-sdk` worker/Activity-authoring
  crate - zero dependency on any other skilj crate, independently
  usable like `skilj-tui`.
- `SkiljBuilder::pool_options`/`db::connect_with`: configurable Postgres
  connection pool sizing/timeouts (`sqlx::postgres::PgPoolOptions` -
  `max_connections`, `min_connections`, `acquire_timeout`, ...),
  previously hardcoded to `sqlx`'s own bare default (10 connections, no
  timeouts) with no way to tune it at all. `db::connect`'s own default
  behaviour is unchanged for every existing caller.

### Changed

- Every direct dependency brought to its latest major version:
  `thiserror` 1→2, `base64` 0.22→0.23, `toml` 0.8→1, `prettyplease`
  0.2→0.3, `syn` 2→3, `tokio-tungstenite` 0.29→0.30, `reqwest` 0.12→0.13,
  `axum-extra` 0.10→0.12, `jsonwebtoken` 9→11, `schemars` 0.8→1, `sqlx`
  0.8→0.9. Most were drop-in; `jsonwebtoken` 11 now requires explicitly
  selecting a crypto backend (`aws_lc_rs`, already used elsewhere in the
  dependency graph); `schemars` 1's JSON Schema output moved `$ref`
  targets from `#/definitions/` to `#/$defs/` (handled for both the new
  and old shape, since a schema stored under 0.8 keeps its old shape
  until next re-registered); `sqlx` 0.9 requires proof that dynamic SQL
  strings aren't injectable - every schema-qualified query (a per-tenant
  Postgres schema name interpolated via `format!`) now wrapped in
  `AssertSqlSafe`, independently audited against the pre-change source
  to confirm no other content moved. No behavior change from any of
  this - verified via the full existing test suite, unchanged.

## [0.0.3] - 2026-09-03

### Security

- Cross-tenant read access: any grant with bounded-context-level access
  (`RoleAccessMapping`/`EventReadToken`) could read another tenant's
  projection instances, raw events, commands, or snapshots, regardless
  of who they actually belonged to. `RoleAccessMapping.scope`/
  `EventReadToken.scope` now gate every read against a per-record
  derived owner (a declared `owner_tag_key`), fail-closed whenever
  ownership can't be affirmatively proven. Applied consistently across
  `ProjectionQuery`, `QueryEvents`/`CountEvents`/`InspectEvent`,
  `FetchEvents`/`ConsumeEvents`, `EventSubscription`, `CommandQuery`/
  `FetchCommands`, `InspectSnapshot`, and the initial grant
  `createBoundedContextFromTemplate` makes for a new tenant.
- Cross-tenant write access: the identical gap existed on the write
  side - even after the read-side fix above, a scope-restricted grant
  or token could still blindly submit a command or create an event for
  another tenant's records. Closed across `submitCommand`, REST command
  triggering, and REST event creation (`ExternalEventToken`/
  `DirectCreationToken`), using the same owner-tag mechanism.
- A REST scope-mismatch rejection returned an HTTP 500 instead of 403 -
  latent until the write-side fix above made it reachable at all; fixed
  alongside it.

### Added

- Private fields: a new field-level visibility mechanism alongside
  `sensitive_fields` - a field visible by default only to its own
  creator (`own`), to any Role in a named team (`team`), or to the
  party its own payload addresses (`addressed`), with self-service
  sharing (`grantPrivateFieldAccessForEvent`/`ForCommand`,
  `revokePrivateFieldAccess`, `listPrivateFieldGrants`) and no
  encryption involved anywhere - a plain read-time redaction, not a
  cryptographic one, since none of the three need to survive an
  erasure request the way `sensitive_fields` does.
- `EventType`/`CommandType.owner_tag_key` and `RoleAccessMapping.scope`/
  `EventReadToken.scope`/`ExternalEventToken.scope`/
  `DirectCreationToken.scope`/`CommandToken.scope`, all exposed on the
  wire (`registerEventType`/`registerCommandType`'s own `ownerTagKey`
  argument, and each token's own `scope` field on the object returned
  by minting it).
- `createBoundedContextFromTemplate` accepts an optional `scope` for the
  tenant's own initial grant.

### Fixed

- A panic (`role_access_mappings row references a bounded_contexts row
  that no longer exists`) when a bounded context was hard-deleted while
  an unrelated concurrent read was in flight - a stale row is now
  treated as gone rather than corrupt.

## [0.0.2] - 2026-08-30

### Added

- Templated bounded-context tenants (Codeberg issue #13): a new
  `createBoundedContextFromTemplate`/`resyncBoundedContextFromTemplate`
  GraphQL surface lets an operator stamp out a new tenant `BoundedContext`
  from an existing one's current `EventType`/`CommandType`/`Projection`
  registrations in a single call - no Rust redeploy per tenant. See
  `specs/skilj.allium`'s `surface BoundedContextTemplating` and
  `docs/architecture.md`.
- An optional idempotency key on command submission (Codeberg issue
  #12), backwards compatible - a key is generated at random whenever
  the caller omits one, so existing callers see no behavioural change.
- Four new filterable scalar `FilterOperator`s: geo, color, IP, and enum.
- `SkiljBuilder::build()`'s startup warm-up and background pollers now
  fan out concurrently across bounded contexts instead of one at a time
  (Codeberg issue #15) - startup latency no longer scales linearly with
  bounded-context count.

### Fixed

- A same-instance dispatch-resolution race, an unbounded orphaned-tenant
  window on a bad role ID, a panic on a legitimate concurrent-delete
  race, and (the most consequential) deleting a template silently and
  permanently breaking command dispatch for every one of its tenants -
  all found by an `ultrareview` pass and fixed with real, decisively
  verified regression tests.
- Six places where documentation had drifted from the `Snapshot`
  concept's own introduction.

### Changed

- Three repeated GraphQL/DB boilerplate shapes (token-creation
  mutations, list-query resolvers, access-token getters) consolidated
  into shared macros - pure refactor, no behavioural change.

## [0.0.1] - 2026-08-28

First published release of the `skilj` workspace: `skilj-macros`,
`skilj-core`, `skilj-graphql`, `skilj-rest`, `skilj`, `skilj-codegen`,
`skilj-tui`, and `skilj-inspector`.

An event-sourced, DDD-style library for building Postgres-backed
applications around a Dynamic Consistency Boundary (DCB), with GraphQL
and REST surfaces, a declarative codegen path for bounded contexts, and
two Ratatui-based operator consoles. See
[`docs/architecture.md`](docs/architecture.md) and
[`specs/skilj.allium`](specs/skilj.allium) for the full design and
behavioural specification.

[Unreleased]: https://codeberg.org/gklijs/SkilJ/compare/v0.0.9...HEAD
[0.0.9]: https://codeberg.org/gklijs/SkilJ/compare/v0.0.8...v0.0.9
[0.0.8]: https://codeberg.org/gklijs/SkilJ/compare/v0.0.7...v0.0.8
[0.0.7]: https://codeberg.org/gklijs/SkilJ/compare/v0.0.6...v0.0.7
[0.0.6]: https://codeberg.org/gklijs/SkilJ/compare/v0.0.5...v0.0.6
[0.0.5]: https://codeberg.org/gklijs/SkilJ/compare/v0.0.4...v0.0.5
[0.0.4]: https://codeberg.org/gklijs/SkilJ/compare/v0.0.3...v0.0.4
[0.0.3]: https://codeberg.org/gklijs/SkilJ/compare/v0.0.2...v0.0.3
[0.0.2]: https://codeberg.org/gklijs/SkilJ/compare/v0.0.1...v0.0.2
[0.0.1]: https://codeberg.org/gklijs/SkilJ/releases/tag/v0.0.1
