# Upgrade preflight and compatibility checks: design

Status: **built** (issue #188), with the decisions at the end.
docs/architecture.md §249 records how it was built and where the build
differs from this text. Read "proposed" below as "built as described".
"Today" describes the code before the change. The fact most likely to
surprise an operator, an older release refusing a migrated database, is
checked against the real 0.3.0 by `scripts/check-upgrade-pair.sh`.

## What the issue asks, and what this document decides

An operator upgrading skilj today can only find out what startup will change
by starting it. This design says:

1. what startup changes in the database, in order, and what each change needs
   to know (the inventory below);
2. which of those changes a read-only preflight can predict, which only the
   application's own registrations can predict, and which nobody can;
3. what a preflight report contains, and how it guarantees that it writes
   nothing;
4. how an explicit migration step and a verify-only startup would fit next to
   today's default, which stays as it is;
5. which upgrades skilj supports, and how an unsupported one is stopped;
6. what the upgrade runbook has to cover.

It ends with the follow-up slices, each small enough for one pull request, and
the decisions that are the user's to make.

Out of scope, as the issue says: down-migrations, and any promise that
arbitrary versions can run side by side.

## Today

`Skilj::build()` (`skilj/src/lib.rs`) is the only upgrade mechanism. It
connects, then in this order:

1. runs the embedded global migrations (`db::migrate`, sqlx);
2. records the database epoch;
3. creates the `admin` bounded context if it is missing;
4. reads roles and generates a bootstrap secret in memory if there's no
   superadmin;
5. patches **every** bounded context's schema, `BACKGROUND_TASK_CONCURRENCY`
   at a time, then warms its event cache;
6. sets up the access trail in the admin context;
7. with a key provider: checks stored data keys and records the key version
   in use;
8. checks stored blind tags against the configured blind-index key;
9. with a `reconciliation_role`: registers blind tags, rotates them, and
   reconciles event types, command types and projections;
10. builds the GraphQL schema and starts the background tasks.

Facts that matter for an upgrade:

- **Global migrations are append-only.** sqlx records each one in
  `_sqlx_migrations` with a checksum. It serializes concurrent runs with an
  advisory lock and refuses an applied migration whose file has changed
  (`VersionMismatch`).
- **An older binary refuses a database a newer one migrated.** sqlx's
  migrator runs with `ignore_missing = false`. When the database records a
  migration the binary doesn't embed, `build()` fails with
  `MigrateError::VersionMissing` before step 2. So after a release that
  adds a global migration (0.0.2, 0.0.3, 0.0.9, 0.0.10, 0.2.0 and 0.3.0
  each did), an instance of the previous release cannot restart against
  that database: not during a rolling deploy, and not as a rollback. An
  instance that is *already running* goes on running. This is the only
  version guard there is, and it says nothing about the per-bounded-context
  patches.
- **Per-bounded-context schemas have no migration record.** Each patch checks
  the catalog and changes only what is missing (§158). Nothing records which
  patches a schema has had or which release applied them. Completeness is
  checked by comparing a v0.0.1 schema, after `build()`, with a freshly
  provisioned one (§159).
- **Startup sets no `lock_timeout`.** A patch with work to do waits as long as
  it takes for its table lock, and every later query on that table waits
  behind it (§158 measured this for the no-op case it fixed).
- **Mixed versions** are documented as untested (CONTRIBUTING.md, "What the
  tests do and don't verify"). Specific mechanisms exist for the cases found
  so far: protections that are never removed (§102), the application version
  on registrations (§104), projections advanced only by an instance that
  declares them (§160), and work that defers to another instance (§161).

## Mutation inventory

"Needs" says what a step must know to decide what to change: **DB** (the
database alone), **config** (builder settings such as the key provider or the
blind-index key), or **registrations** (the application's declared types).
"Preview" says how a read-only preflight could predict it.

### Global (the `public` schema)

| # | Step | Changes | Needs | Locks / cost | Preview |
|---|------|---------|-------|--------------|---------|
| G1 | `db::migrate` (`migrations/0001`-`0009`) | Tables `roles`, `bounded_contexts`, `role_access_mappings`, `access_token_index`, `skilj_epoch`, `encryption_master_keys`, `encryption_key_assignments`, `blob_deletions` and their columns and indexes; `_sqlx_migrations` | DB | sqlx advisory lock; each migration in its own transaction, DDL locks on the global tables | Compare `_sqlx_migrations` with the embedded set: pending, failed (`success = false`), checksum mismatch, unknown (newer) versions |
| G2 | `observe_epoch` | Upserts the single `skilj_epoch` row | DB | One row | Read the row and the server's identifier: "epoch changed since last recorded" |
| G3 | Admin context stamp | Inserts `bounded_contexts('admin')` and provisions `bc_admin` | DB | Only on a new database | Row present or not |
| G4 | `ensure_access_trail` | Widens `bc_admin.events`' `origin_kind` check; registers the access-trail and master-key event types; writes one `AccessBaselined` event per active grant with no history | DB | Constraint swap on `bc_admin.events`; one transaction per grant | Constraint definition, registered types, and a count of grants without an access event |
| G5 | `check_encryption_keys` | Records the provider's key version as in use (`EncryptionMasterKeyRotated`, reason `Adopted`/`External`); may refuse startup (`encryption_key_provider_changed`, `..._material_changed`, `..._legacy_master_key_required`, `..._changed`) | config + provider | Provider calls (unwraps of the newest data keys) | Needs the provider. A preflight given the same key service can run the same checks without the final write. Without it: report "not checked" |
| G6 | `check_stored_blind_tags` | Records the blind-index key fingerprint once every check passes; may refuse (`blind_index_key_changed`, `blind_normalizer_changed`) | config | One row per bounded context | Needs the key and the declared blind tags. Without them: "not checked" |

The bootstrap secret (step 4) is held in memory, never stored, so it is not a
mutation.

### Per bounded context (`bc_<name>`), every context, every startup

In the order `build()` runs them. "Catalog" means the patch reads
`pg_attribute`, `pg_index`, `pg_constraint` or `to_regclass` first and runs
DDL only for what is missing (`add_columns_patch`, `create_index_patch`,
`add_constraint_patch`, §158). `CREATE TABLE IF NOT EXISTS` takes no lock
when the table exists.

| # | Patch | Mechanism | Rewrites data? |
|---|-------|-----------|----------------|
| B1 | `ensure_idempotency_keys_table` | table + catalog index | no |
| B2 | `migrate_idempotency_keys_client_id_scoping` (§37) | advisory lock; `information_schema` check; replaces the primary key | no rows changed; `ACCESS EXCLUSIVE` on `idempotency_keys` while the key is rebuilt |
| B3 | `ensure_projection_state_owner_columns`, `ensure_projection_state_as_of_sequence_columns`, `ensure_event_scoping_columns`, `ensure_private_field_columns`, `ensure_projection_owner_tag_key_column`, `ensure_projection_requires_capability_column` | catalog columns | no |
| B4 | `ensure_private_field_grants_table`, `ensure_command_approval_tables`, `ensure_secret_tables` | catalog columns and indexes | no |
| B5 | `ensure_cross_context_route_cursors_table`, `ensure_deadline_cursors_table`, `ensure_deadlines_table`, `ensure_projection_partition_progress_table`, `ensure_snapshot_partition_progress_table` | tables, catalog columns and indexes | no |
| B6 | `ensure_read_cursors_checkout_column`, `ensure_external_message_cursors_table`, `ensure_external_message_keys_table` | tables, catalog columns and indexes | no |
| B7 | `ensure_event_read_token_start_from_column` (§159) | catalog column, then catalog `CHECK` constraint | validates `access_tokens` once |
| B8 | `ensure_correlation_causation_columns`, `ensure_cross_context_route_retry_columns` | catalog columns and indexes on `events` and the route cursors | no |
| B9 | `ensure_parked_deliveries_table`, then `migrate_parked_deliveries_dedup_and_unique_index` (§56, §64) | advisory lock; `pg_indexes` check | **deletes** duplicate parked deliveries, keeping the newest, then adds a unique index |
| B10 | `ensure_registrations_generation` (§189) | advisory lock; counter table, trigger | inserts the counter row |
| B11 | `make_bounded_context_current` (§200, §204): owner tag key column, `ensure_consistency_query_column`, `ensure_event_tags_index`, `ensure_blind_tags_table`, `ensure_encryption_key_provider_columns`, `ensure_payload_limit_columns`, `ensure_blob_tables`, `ensure_blob_field_columns`, `ensure_private_field_columns`, `ensure_source_content_protection`, `drop_events_by_tags_index` | mixed, below | see below |
| B12 | `EventCache::warm` | none (reads) | no |

The heavy ones in B11:

- `ensure_event_tags_index` (§198, §210, §245): under an advisory lock, and
  `SHARE ROW EXCLUSIVE` on `events` (event writes wait), builds the tag
  dictionary from **every event's tags**, replaces the `event_tags` table
  with a view and installs the insert trigger. It is a no-op once the view and
  the current trigger function are there; replacing an older trigger function
  alone locks nothing. Cost grows with history: this is the 0.2 → 0.3
  "maintenance window" in docs/deployment.md.
- `ensure_source_content_protection` (§240, not yet released): under an
  advisory lock and `SHARE ROW EXCLUSIVE` on `event_types` and `events`,
  installs two triggers and **rewrites** `origin_source_content` to
  `[redacted]` on the external events of protected types. Permanent;
  backups keep the old values.
- `drop_events_by_tags_index` (§210): `DROP INDEX CONCURRENTLY` of the old
  GIN index. Writes go on. An older release still running loses the index it
  read tags through (§210 moved reads to `event_tags`), which only matters
  for releases before §210.

A bounded context created **after** startup is provisioned at the current
shape. One created by an older instance during a rolling deploy is brought up
to date by the first newer instance to touch it (`ensure_bounded_context_current`,
§200), on the command path or in a background tick, not only at startup.

Every per-bounded-context step can be previewed from the catalog, plus row
counts for the data-rewriting ones (duplicate parked deliveries, protected
external events to scrub, events whose tags must be indexed). The patches
already contain the catalog checks; they're inside `DO` blocks and SQL
strings today, so the preview needs them as separate queries (slice 2).

### Registration-dependent (only with a `reconciliation_role`)

All of it needs the application's declared types, so only a preflight run by
the application binary itself, with the same builder, can predict it.

| # | Step | Changes |
|---|------|---------|
| R1 | `reconcile_blind_tags` | registers declared blind tags (`blind_tags` rows) |
| R2 | `rotate_blind_tags` (§209) | **rewrites every stored blind tag value** of a constraint, in events, commands and the tag dictionary, to the current key's hash |
| R3 | `reconcile_event_types`, `reconcile_command_types` | upserts registrations (schema, tags, protections, flags, approval policy); merges stored protections back (§102); leaves a row written by a higher application version alone (§104); stamps the version and adds `registered_by_version` when missing (`ensure_registration_version_columns`); a type gaining protection scrubs its source content (§240); may refuse (`schema_incompatible`, overlap and blind-tag declaration errors) |
| R4 | `reconcile_projections` | creates or upserts projections; **stages a rebuild** when the state schema or consumed types change and history exists (a full replay in the background), or keeps the stored projection when it is newer (§103); sets owner tag key and required capability |

These are decided by pure rules over loaded rows (`register_event_type`,
`register_command_type`, `register_projection`, `keep_stored_protections`,
`schema_is_backwards_compatible`), with the reads and writes around them in
`skilj/src/lib.rs`. That split is what makes a registration preview possible
without copying the rules: load, decide, and report instead of writing.

### After `build()` returns

The background tasks change the database continuously, and some of that is
upgrade work started at startup: staged projection rebuilds replay history,
legacy data keys are rewrapped (`master_key_rotation`), deadlines fire,
retention deletes expired idempotency keys and resolved deadlines, unused
blobs are collected. A preflight doesn't run them. It reports the work they
will have, such as rebuilds that would be staged, with their history size,
and legacy data keys still to rewrap.

## What a preflight can and can't tell

| Question | Answerable from |
|----------|-----------------|
| Which global migrations are pending, failed, or unknown to this binary | DB |
| Which columns, indexes, constraints, tables and triggers each bounded context lacks | DB (catalog) |
| How many rows a data-rewriting patch will touch, and which tables it locks | DB (estimates from `pg_class.reltuples`; exact counts on request) |
| Whether the epoch changed since an instance last ran | DB |
| Whether the configured key provider and blind-index key match what is stored | config, provider reachable |
| Which registrations change, which stay because a newer version wrote them, which are refused, which rebuilds would be staged | registrations + reconciliation role's grants |
| Whether an **older** binary still running can live with the result | **not determinable** from the database today; needs the compatibility declaration below |
| How long a lock will be waited for | **not determinable**: depends on the transactions open at that moment. The report names the locks and their scope; the runbook covers `lock_timeout` |
| Whether a sync projection added by the new version will refuse the old version's writes (§160) | registrations of both versions; reported as "new sync projection consuming X" so an operator can tell |

The report lists every unanswered question as an explicit `unknown` entry,
with the reason (no key provider given, no reconciliation role, provider
unreachable, bounded context deleted mid-walk), never as a pass.

## The preflight report (proposed)

**Entry points.**

- `skilj::preflight::inspect(&pool) -> PreflightReport`: DB only (G1-G4,
  B1-B11). Works from any binary that links skilj, including a deploy job
  that doesn't declare the application's types.
- `SkiljBuilder::preflight(self) -> Result<PreflightReport>`: everything
  above plus config and registration checks (G5, G6, R1-R4), using exactly
  the configuration `build()` would use. Starts no background task, serves
  nothing, generates no bootstrap secret.

**Writes nothing, by construction.** All preflight reads run in one
`BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY` transaction,
which Postgres enforces: any write fails with `25006`, so a bug that slips a
write in fails the preflight instead of changing state. One snapshot also
makes the report consistent across bounded contexts. The provider calls in G5
do read-only unwraps. A database role with `SELECT` on the global tables and
`USAGE`/`SELECT` on the `bc_*` schemas is enough, so a preflight can run with
fewer privileges than the application. Test (acceptance criterion 2): a
preflight against the current schema and against the v0.0.1 fixture leaves
the catalog shape, `_sqlx_migrations`, and the row counts and `xmin` of every
table unchanged.

**Format.** A `serde`-serializable struct; JSON for machines, a `Display` for
humans. A sketch:

```json
{
  "format": 1,
  "skilj_version": "0.4.0",
  "server": { "major": 17, "epoch_changed": false },
  "verdict": "upgrade_needed",
  "global": {
    "applied": [1, 2, 3, 4, 5, 6, 7, 8],
    "pending": [9],
    "unknown": [],
    "checksum_mismatch": [],
    "failed": []
  },
  "bounded_contexts": [
    {
      "name": "banking",
      "missing": [
        { "patch": "ensure_source_content_protection", "object": "trigger events.protect_external_source" }
      ],
      "rewrites": [
        { "patch": "ensure_source_content_protection", "table": "events",
          "rows_estimate": 1200, "lock": "SHARE ROW EXCLUSIVE", "blocks": "event writes" }
      ]
    }
  ],
  "registrations": [
    { "bounded_context": "banking", "kind": "projection", "name": "Balance",
      "outcome": "rebuild_staged", "history_events_estimate": 250000 }
  ],
  "unknown": [
    { "check": "encryption_keys", "reason": "no key provider configured for the preflight" }
  ]
}
```

`verdict` is `up_to_date`, `upgrade_needed` or `refused` (with the codes
`build()` would fail with, such as `schema_incompatible` or
`encryption_key_provider_changed`). The report carries names (of bounded contexts,
types, patches, objects), counts, codes and versions only: no payloads, tag
values, key material, key identifiers beyond the provider's name, connection
strings or role subjects. Bounded-context and type names are metadata an
operator already has. `format` versions the report itself.

## Execution modes (proposed)

The default stays: `build()` migrates, patches and reconciles, as now.

**Explicit migration.** `skilj::preflight::migrate(&pool, &MigrateOptions) ->
MigrationReport` runs G1-G4 and B1-B11 (the DB-only part) and nothing else:
no registrations, no background tasks, no provider. It suits a deploy job or
an init container that owns the schema change, with its own `lock_timeout`
(`MigrateOptions::lock_timeout`, applied with `SET LOCAL` per patch
transaction; on timeout the patch fails, the job fails and is rerun, and the
patches already committed stay done). Registration reconciliation stays at
application startup: it needs the types and is row-level, governed by
§102/§104.

**Verify-only startup.** `SkiljBuilder::schema_changes(SchemaChanges::Refuse)`
makes `build()` run the DB-only preflight first and fail with a new
`schema_upgrade_required` error (listing what is pending) instead of
changing the schema. Registrations still reconcile when a reconciliation
role is configured; that is the existing switch for them. `SchemaChanges::Apply`
is the default and today's behavior. With an explicit migration job in
place, application instances run with `Refuse`, so an instance that would
otherwise take a long lock at startup fails fast instead.

Bounded contexts created at runtime by an older instance still get patched by
a newer one on first touch (§200), in both modes. That is row and column
work on a new, small schema, which is why it stays allowed.

## Supported upgrades (proposed policy)

What can be supported honestly today:

1. **Stop, upgrade, start**, from any released version to the current one.
   The v0.0.1 fixture test covers the schema side. All instances stop
   before the first new one starts. This is the supported path and the
   documentation's default.
2. **Rolling deploy between two releases declared compatible.** A release
   declares, in its CHANGELOG and in code (below), the oldest release whose
   instances may keep running and restarting against a database it has
   upgraded. Until a pair is declared and tested, the answer is no, and the
   documentation says to use 1.
3. **Rollback** only by restoring a backup taken before the upgrade. With a
   new global migration, the older binary refuses the database anyway
   (`VersionMissing`).

**Making compatibility explicit in the database (proposed).** A new global
table, `skilj_schema_compatibility`, with one row: `level` (an integer
skilj bumps with every release that changes the schema, global or per
bounded context), `min_compatible_level` (the oldest level whose binaries may
run against the schema as it is now), `upgraded_by` (the release) and
`upgraded_at`. The migrating binary writes it after G1 and B1-B11 complete for
every bounded context. Each binary embeds its own `LEVEL`; at startup it
refuses with `schema_too_new` (naming `upgraded_by`) when
`min_compatible_level > LEVEL`, and otherwise runs.

That only works if a compatible older binary can get past sqlx. So the
migrator would run with `ignore_missing = true`, and the explicit level check
would replace `VersionMissing` as the guard. This is a deliberate trade: an
older binary would then start against a database with migrations it doesn't
know, *when the newer release has declared that safe*. Releases before the
one introducing the table can't read it, and stay protected by
`VersionMissing`, because adding the table is itself a global migration.
(User decision, below.)

**The test for a declared pair** (acceptance criterion 3, slice 5): the older
release is built from its tag (`git archive v0.X.Y`, outside the workspace, a
target directory on disk), its server started as a separate process against a
database, the newer one started next to it, and both are driven through REST:
commands accepted by each, events read by each, then the older one restarted.
A pair that is not declared gets a test of its own: the older binary refuses
with `schema_too_new` (or `VersionMissing` for releases before the table),
and the documented procedure is stop, upgrade, start.

## Runbook outline (to write into docs/deployment.md)

- **Before:** read the CHANGELOG of every release crossed; run
  `SkiljBuilder::preflight` with production's configuration against a
  restored copy; check the report's rewrites and staged rebuilds against the
  maintenance window; rehearse the restore, including what to do with writes
  made after the backup.
- **Ownership:** one job migrates (explicit mode) or the first instance does
  (default). Concurrent startups are safe: sqlx's advisory lock, the patches'
  advisory locks and catalog checks serialize them. The others wait.
- **Locks:** name the patches that take table locks (B2, B7, B9, B11's tag
  dictionary and source-content scrub), what each blocks, and how
  `lock_timeout` behaves: the patch fails, nothing partial is committed for
  that patch, rerun.
- **Interrupted migration:** each global migration and each patch commits on
  its own; a crash leaves earlier ones done and later ones pending, and the
  next run continues (a preflight shows where). A failed sqlx migration
  (`success = false`) needs inspection before rerunning.
- **Rebuild readiness:** staged rebuilds from the preflight; watch each
  projection's `pendingRebuild`/`buildingRebuild` until promoted before relying on the
  projection; a paused sync projection refuses writes (`live_fold_paused`).
- **Rollback:** restore. There is no down-migration.

## Follow-up slices

All six were built together, in one change, on the user's request to
continue past the design (docs/architecture.md §249). The list stays as the
plan they followed. Two differences: slice 5's separate-process test checks
the pair 0.3.0 → this release, which is declared incompatible, because no
released pair yet records a level; and the concurrency test in slice 3
found a startup race, fixed with it.

1. **Document today's facts** (in this change): the `VersionMissing`
   behavior and the missing `lock_timeout` in docs/deployment.md, with the
   test that pins the former.
2. **DB-only preflight** (`skilj::preflight::inspect`): G1-G4 and
   B1-B11 as read-only checks in one read-only transaction, the report type
   and its JSON. Tests: v0.0.1 fixture reports its missing objects, and
   reports nothing after `build()`; the preflight changes nothing. A new
   patch without a check is caught the way a new object without a patch is
   caught today (§159): after `build()` on the v0.0.1 fixture the
   preflight must report nothing, and before it, every object the catalog
   comparison finds missing. Needs the
   catalog checks in `add_columns_patch` and the others exposed as queries.
3. **Explicit migration and verify-only startup**: `migrate` with
   `lock_timeout`, `SchemaChanges::Refuse`. Tests: concurrent `migrate`
   calls, a `lock_timeout` that fires and a rerun that completes, a crash
   between two patches (simulated by failing the second), `Refuse` refusing
   a v0.0.1 schema and accepting a current one.
4. **Registration preview** (`SkiljBuilder::preflight`): split
   `reconcile_*` into plan and apply, report outcomes, add G5 and G6 without
   their final writes. Tests: each `ReconciliationReport` outcome appears in
   the preview of the same builder, and the preview writes nothing.
5. **Compatibility level and the first declared pair**: the table, the
   `ignore_missing` change, `schema_too_new`, the separate-process test for
   one pair, and its CI job (it compiles an older release: cache its target
   directory).
6. **Runbook** in docs/deployment.md, from the outline above, once 2-3
   exist to reference.

## Decisions (made 2026-11-11)

1. **The compatibility level replaces `VersionMissing` as the guard**, with
   `ignore_missing = true`, together with the separate-process upgrade
   test.
2. **Verify-only startup is opt-in** (`SchemaChanges::Refuse`); the default
   stays "startup upgrades".
3. **Row counts are estimates** (`pg_class.reltuples`) by default; exact
   counts on request.
