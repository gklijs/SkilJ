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
Rehearse on a restored backup with the intended application registrations,
database version and representative data volume. Verify recovery from that
backup before updating the production database; there is no down-migration
tool. Restoring also means deciding how to handle writes made after the backup.

For example, upgrading from 0.2 to 0.3 requires Postgres 15 or later.
The first startup interns historical tags while locking event writes;
large histories require a maintenance window. Sensitive projections need
`Projection::SUBJECT_KEY` for decryption, and a staged or building rebuild
can pause live folding unless `LIVE_FOLD_COMPATIBLE` is declared. A paused
synchronous projection refuses writes with `live_fold_paused`. Follow the
release's instructions and verify rebuild readiness before resuming traffic.

Mixed application versions on one database are untested. Use a stop,
upgrade and restart procedure instead of assuming a rolling deployment is
compatible. The historical-schema fixture tests upgrade from v0.0.1;
it does not establish arbitrary mixed-version compatibility. Likewise,
local use of Postgres 15+ is not a multi-major CI guarantee; the full suite
currently runs against Postgres 17. See [coverage limits](../CONTRIBUTING.md#what-the-tests-do-and-dont-verify).

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
