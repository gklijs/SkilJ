# PostgreSQL compatibility

Skilj supports PostgreSQL 15 through 18, using the latest minor release of
each major. PostgreSQL 15 is the minimum because the schema uses
`UNIQUE NULLS NOT DISTINCT`. New majors are added explicitly after testing;
prereleases are not supported. Review this range when PostgreSQL releases a
new major or an included major reaches end of life, following
[PostgreSQL's version policy](https://www.postgresql.org/support/versioning/).
Raising the minimum follows the project's minor-release policy. A supported
major does not mean every optional extension or managed-service configuration
has been tested.

The required `test` check includes the existing complete test suite on
`postgres:17-alpine`, plus focused compatibility jobs on `postgres:15-alpine`
and `postgres:18-alpine` on pull requests, pushes to main and manual dispatch.
Major image tags receive current minor releases. Version 16 is within the
supported range but has no dedicated job. Each compatibility job uses a
separate disposable service, checks `SHOW server_version_num` before database
writes, and fails if the service is unavailable or reports the wrong major.
There is no local-server fallback in these jobs.

The focused suite reuses whole integration-test binaries:

| Target in `skilj/tests/` | Scenarios |
| --- | --- |
| `startup_bounded_contexts` | Fresh startup, unchanged schema, missing columns/indexes, reconciliation, upgrade of the checked-in `v0_0_1_bounded_context.sql` historical fixture, and deletion during startup |
| `conformance` | Accepted/rejected commands, idempotent retry and key misuse, dry runs, event reads, tag search, credentials and archived contexts through REST and GraphQL, checked against the recorded transcript |
| `sync_projections` | A REST command updates a synchronous projection persisted in PostgreSQL |
| `async_projections` | A REST command eventually updates a background asynchronous projection |
| `decrypt_on_read` | Sensitive event reads with grants and subject ownership, and reads with no encryption needed |
| `blind_tags` | Blind-tag uniqueness, concurrent conflicts, erasure, retention, key rotation, startup validation and authorized plaintext lookup |

`scripts/ci-test.sh` runs these with `--nocapture` and the existing no-skip
guard. Compilation, setup, version mismatch, failed tests and any `skipping: `
message fail the job. The required `test` aggregator fails if either matrix
job fails or is skipped. Existing full-suite shards, doctests, ignored
benchmark defaults, broker tests and Temporal tests retain their coverage.

## Reproduce locally

Install `psql` and `pg_isready` on PATH and use a disposable PostgreSQL server.
Every binary drops and recreates its own named database; the role must have
`CREATEDB`. For example, from the repository root:

```sh
docker run --rm -d --name skilj-pg15 -e POSTGRES_PASSWORD=postgres \
  -p 127.0.0.1:15415:5432 postgres:15-alpine
export CI_TEST_GROUP=postgres CI_POSTGRES_MAJOR=15
export CI_POSTGRES_HOST=127.0.0.1 CI_POSTGRES_PORT=15415
export CI_POSTGRES_USER=postgres CI_POSTGRES_PASSWORD=postgres CI_POSTGRES_DB=postgres
bash scripts/ci-test.sh --setup-only
bash scripts/ci-test.sh --build-only
time bash scripts/ci-test.sh
docker stop skilj-pg15
```

Repeat with image `postgres:18-alpine`, `CI_POSTGRES_MAJOR=18` and a free port.
These scripts construct `DATABASE_URL` from `CI_POSTGRES_*`; setting
`DATABASE_URL` alone does not select their server. `--build-only` needs no
database, but still requires the explicit expected major for this group.
Do not run two copies against the same server: their named databases collide.

## Cost and limits

Each additional CI job has a 30-minute timeout and uploads its test output and
Cargo timing report, including on failure. Compile and test steps are separate
so their costs can be compared. Only one boundary job saves the shared Rust
cache on main. Measure these reports before expanding to a scheduled full
matrix; the focused jobs do not multiply broker or Temporal containers.

Local validation on 2026-11-09 passed all 26 tests on both PostgreSQL 15.19
and 18.6 with no skips. Building the selected binaries with an existing
dependency cache took 31.83 seconds; the two test runs took 54.38 and 40.58
seconds respectively. They ran concurrently on a shared development machine,
so these are a scope/cost check, not GitHub runner estimates or a performance
comparison between PostgreSQL versions. Cold-build and runner costs remain
to be measured from CI artifacts.

This tests selected SQL paths across majors, not every SQL statement on every
version. It does not test PostgreSQL in-place major upgrades, arbitrary
historical Skilj databases, mixed Skilj releases during rolling deployment,
or failover across PostgreSQL majors. The historical fixture checks Skilj's
schema migration on each selected server, not `pg_upgrade`.
