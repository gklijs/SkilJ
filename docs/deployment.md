# Operate a SkilJ application

Start from the [wallet tutorial](wallet-tutorial.md) for local development.
The template's server demonstrates wiring and graceful shutdown; adapt
its development bootstrap before exposing an application to other users.

## Database and startup

Use Postgres 15 or later. Set `DATABASE_URL` in the application's environment
and build without the `local` feature when embedded Postgres is unwanted.
The [template Dockerfile](../templates/skilj-template/Dockerfile) builds
that mode. Its scratch image contains the binary and CA bundle; inspect
it through logs rather than an interactive shell.

SkilJ owns global tables and a schema per bounded context. Startup performs
forward-only database upgrades. Registered Rust types also drive
reconciliation of context schemas and registrations; inspect the
`ReconciliationReport` returned by `build()`, including `skipped_no_access`.
Configure `reconciliation_role` with an authorized external subject for
registration changes. A process starting successfully does not mean all
requested registrations had permission to reconcile.

## Identity and bootstrap

Configure `SkiljBuilder::identity_provider` with the trusted issuer, JWKS
endpoint, signing algorithm and subject claim. The [authorization guide](authorization.md)
shows how verified subjects map to Roles and grants and how REST tokens
scope individual capabilities. See the [builder API](https://docs.rs/skilj/latest/skilj/struct.SkiljBuilder.html)
for current configuration signatures.

On a new database with no active superadmin, `build()` prints a bootstrap
secret. Claim it through GraphQL's `createSuperadmin` mutation to establish
the first superadmin; then configure contexts, Roles and access through the
administrative API. The secret is consumed by its first successful claim.
Protect the startup logs and perform this step before granting public access.
The template's `skilj::dev::seed_admin` bypasses that flow for development.

## Upgrade with a restore plan

Read the [changelog](../CHANGELOG.md) for every release crossed by an upgrade.
Its "Upgrading from" section says what the first startup does and whether the
previous release may keep running. Rehearse on a restored backup with the
intended application registrations, database version and representative data
volume. Verify recovery from that backup before updating the production
database; there is no down-migration tool. Restoring also means deciding how
to handle writes made after the backup.

### Which upgrades are supported

- **Stop, upgrade, start** from any release. Stop every instance, start the new
  release (or run its migration step), then start the rest. The suite upgrades
  a bounded context from v0.0.1's schema, and CI runs the previous release and
  this one against one database (`scripts/check-upgrade-pair.sh`).
- **Rolling deploys** only between two releases whose changelog says the older
  one may keep running. A release records a schema level and the oldest level
  allowed to run against its schema. An instance of an older release then
  refuses to start with `schema_too_new`, naming the release that upgraded the
  database. Instances already running keep running, but don't restart them.
  Releases up to 0.3.0 record no level and refuse with sqlx's `VersionMissing`
  instead.
- **Rollback** only by restoring the backup.

### Before: preflight

Run the preflight with production's configuration against the restored copy,
and against production just before the window:

```rust
let report = my_app::register(Skilj::builder(database_url))
    .reconciliation_role(subject)
    .preflight()
    .await?;
println!("{report}"); // or serde_json::to_string(&report)
```

It changes nothing: its connections are read-only, and Postgres refuses any
write. The report lists:

- pending global migrations;
- each bounded context's missing objects;
- the patches that rewrite rows or block writes (`rewrites`), and the table
  locks of the rest (`locks`), with row counts. The counts are planner
  estimates; `InspectOptions::exact_counts` counts them instead;
- what reconciliation would do with each type, such as `rebuild_staged`, which
  replays history in the background, `kept_newer` and `refused`;
- `unknown`: checks it couldn't answer, such as registrations on a schema that
  must be upgraded before they can be read.

A `refused` verdict means startup would fail. `skilj::preflight::inspect(&pool,
..)` gives the database part from any binary.

### During: who migrates, and locks

By default the first instance to start upgrades the schema. Several starting
together is safe: each step serializes on a lock and skips what another
already did. To make the change a step of its own, run
`skilj::preflight::migrate(database_url, &MigrateOptions::new().lock_timeout(..))`
from a deploy job or init container. Then start the application with
`.schema_changes(SchemaChanges::Refuse)`, so an instance that would otherwise
upgrade fails with `schema_upgrade_required` instead. A `Refuse` instance changes
no schema at startup. Its `concurrent_indexes` background task still builds and
drops indexes concurrently, which blocks no writes (below), so the preflight
lists that work as `lazy` and doesn't make `Refuse` refuse over it.

A schema change that has real work to do waits for every open transaction on
its table, and queries on that table queue behind it. Startup itself sets no
lock timeout. Run the upgrade with no long-running transactions open: no
reporting queries, no idle `psql` sessions. `migrate`'s `lock_timeout` turns a
long wait into a failure. The step that waited is not committed, and the steps
before it stay done.

Indexes on tables that grow with traffic (from 0.5, `deadlines_claimable`) are
built with `CREATE INDEX CONCURRENTLY` instead, and the indexes they replace
dropped with `DROP INDEX CONCURRENTLY` (docs/architecture.md §256). Writes go on
while they work, unless a conflicting schema change such as a bounded context's
hard delete queues behind them. They still wait for transactions already
writing the table, and a build also for every older transaction anywhere in the
database. The first instance on the new version does them one bounded context
at a time, so with many bounded contexts that adds up: run `migrate` as a
deploy job then. `lock_timeout` bounds these waits too, with the same `55P03`
error. A build cut short leaves an invalid index; the preflight lists it, and
the rerun, the next default start or the background task rebuilds it. If the
instances connect as a role that doesn't own the tables, the background task
logs that once per bounded context; run `migrate` as the owner.

### If it's interrupted

Each global migration and each patch commits on its own. After a crash, a
timeout or a killed deploy, run the upgrade again: it carries on where it
stopped, and a preflight shows where that is. A global migration recorded as
failed (`migration_failed` in the preflight) needs inspecting before a rerun.
An interrupted concurrent index build is the one step that leaves something
behind: an invalid index, which the rerun drops and builds again.

### After: rebuilds

Projections the preflight reported as `rebuild_staged` replay their history in
the background. Watch each projection's `pendingRebuild`/`buildingRebuild`
until promoted before relying on it. A staged or building rebuild can pause
live folding unless `LIVE_FOLD_COMPATIBLE` is declared, and a paused
synchronous projection refuses writes with `live_fold_paused`.

### Upgrading from 0.4 to 0.5

The first startup (or `migrate`) builds `deadlines_claimable` concurrently on
every bounded context and then drops `deadlines_due`; writes go on. 0.4 may keep
running against the upgraded database (schema level 2, compatible with 1), so a
rolling deploy works. Two things to know:

- A 0.4 instance that starts in default mode, an old replica restarting during
  the rollout or a rollback, builds `deadlines_due` again with its
  write-blocking `CREATE INDEX`. The next 0.5 start, or within a minute a 0.5
  instance's background task, drops it again.
- A bounded context a 0.4 instance creates during the rollout has only
  `deadlines_due`. The 0.5 instances' background task builds its index within
  about a minute, in either mode.

To roll back to 0.4, stop every 0.5 instance first; a running one drops
`deadlines_due` again. Check that no `DROP INDEX CONCURRENTLY` is still running
(`pg_stat_activity`), then start 0.4. With `SchemaChanges::Refuse`, run 0.4's
`migrate` before starting it: 0.4 refuses a database without `deadlines_due`
until its own `migrate` has rebuilt it.

### Upgrading from 0.2 to 0.3

Requires Postgres 15 or later. The first startup interns historical tags
while locking event writes, so large histories need a maintenance window.
Sensitive projections need `Projection::SUBJECT_KEY` for decryption.

Mixed application versions beyond a declared pair are untested; see
[coverage limits](../CONTRIBUTING.md#what-the-tests-do-and-dont-verify).
Local use of Postgres 15+ is not a multi-major CI guarantee either; the full
suite runs against Postgres 17. The design behind the preflight is in
[upgrade-preflight-design.md](upgrade-preflight-design.md).

## Shutdown and capacity

Stop accepting requests, drain in-flight HTTP work, then call
`Skilj::shutdown(timeout)` and inspect its report for aborted work. The
[template server](../templates/skilj-template/src/bin/server.rs) handles
Ctrl-C and SIGTERM in this order. Give the process supervisor enough time
for HTTP draining and the configured background-work shutdown timeout.

Use the [performance guide](performance.md) to size workloads and tune
batching; its results are measurements of stated workloads, not capacity
guarantees. Check [environment troubleshooting](troubleshooting.md) for
database startup, downloads, Docker and disk-space failures. Keep
encryption-provider configuration and keys available according to your
backup and recovery procedure when using sensitive fields.
