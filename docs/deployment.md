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
drops optional indexes concurrently. Preflight calls that work `lazy`. The
open-proposal index is required for submission and must be ready before
`Refuse` starts.

A schema change that has real work to do waits for every open transaction on
its table, and queries on that table queue behind it. Startup itself sets no
lock timeout. Run the upgrade with no long-running transactions open: no
reporting queries, no idle `psql` sessions. `migrate`'s `lock_timeout` turns a
long wait into a failure. The step that waited is not committed, and the steps
before it stay done.

Indexes on tables that grow with traffic (from 0.5, `deadlines_claimable`
and `command_proposals_open_idempotency`) are
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

0.5 builds the open-proposal uniqueness index and the deadline polling
index concurrently. It also installs replica identity and database checks for
deadline statuses, blob states and blob pin kinds. These changes ship together
at schema level 2, which requires 0.5.
Stop every 0.4 instance before upgrading. Keep a backup from the
quiesced database for a rollback to 0.4.

1. Run 0.5 preflight while 0.4 still serves its Admin operations. If it
   reports duplicate open proposals, record all IDs in each group before
   changing any row. Settle unwanted proposals with an authorized Admin.
2. Stop submission traffic and all application processes, including route,
   deadline, approval, and retention workers. Take the backup and run
   `skilj::preflight::migrate`. Start 0.5 after migration succeeds.
3. If a duplicate is found during the offline upgrade, use the release's
   worker-free maintenance command below. Keep writers and retention
   stopped. If a late concurrent build left a confirmed invalid but ready
   `command_proposals_open_idempotency` and its uniqueness enforcement
   blocks settlement, the schema owner can drop that invalid index
   concurrently. Keep the old unique index in place and retry migration
   after settling the groups.

If an existing row violates a new deadline or blob constraint, PostgreSQL
reports the constraint and migration stops. Correct the affected values using
the application data and retry migration. PostgreSQL enforces an installed
check on new writes even when validation of existing rows is still pending.
If a manually added `sequence` primary key conflicts with the built-in
singleton key, migration reports its schema and constraint name. Review
the conflicting constraint and any dependencies before retrying.

### Logical replication and blue-green upgrades

Bounded-context tables can be included in a `FOR ALL TABLES` publication.
The `sequence` row has a singleton primary key, and the derived
`event_tag_refs` table uses `REPLICA IDENTITY FULL` because one event can have
duplicate tag references. Command submission, subject erasure and blind-key
rotation can update or delete those rows while the publication is active.
This applies to all-table publishers used by systems such as RDS and Aurora
blue-green upgrades. The publisher still needs `wal_level=logical` for actual
replication.

### Settling an open-proposal group offline

Use the SkilJ 0.5 source tree with the deployment's database credentials
and an existing scoped Admin's current JWT. The maintenance command reads
the JWT from stdin. It verifies that token against the trusted deployment
configuration and uses the existing claim and rejection operation. It
starts no application workers. The host needs access to the trusted IdP's
JWKS endpoint. The database credential and selected IdP settings are
trusted operator inputs. Supply the same settings that the application
uses, including all accepted audiences and any custom subject claim.

Set `DATABASE_URL` in the operator environment and point
`SKILJ_RECOVERY_IDP_CONFIG` to a protected JSON file with these fields:

```json
{
  "jwks_endpoint": "https://issuer.example/jwks",
  "issuer": "https://issuer.example/",
  "audiences": ["skilj"],
  "signing_algorithm": "RS256",
  "subject_claim": "sub"
}
```

Pipe the Admin's token from your credential provider into this command:

```sh
cargo run -p skilj-core --example reject_duplicate_proposal -- \
  banking proposal-id "legacy duplicate during the 0.5 upgrade"
```

Repeat for each ID that needs settlement. An approving claim becomes
eligible for takeover after ten minutes. If a retained key proves the
command committed, settle every ID from the original group, including the
would-be survivor, while the key still exists. The command can report
`approved` because the key proves acceptance, or `expired` if a pending
proposal already reached its deadline. This recovery command never executes
the command. If there is no retained key and the previous result is
ambiguous, leave the proposals unapproved and reject the remaining
siblings. A missing key or a missing match in history is insufficient
evidence that no command ran. Leave a survivor open for later approval only
when there is affirmative evidence the operation never executed. Confirm
the final status for every recorded ID before rerunning migration.

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
