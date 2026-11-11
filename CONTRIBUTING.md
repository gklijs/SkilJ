# Contributing

Thanks for considering a contribution to `skilj`. This is a
solo-maintainer project, so the process is kept light.

For building an application, start with the [documentation guide](docs/index.md).
For local or CI setup failures, see [environment troubleshooting](docs/troubleshooting.md).

## Choosing a feedback loop

| Changed component | Start with | Broader validation |
| --- | --- | --- |
| Application decision or projection fold | `cargo test -p skilj-demo --test banking_fixture`; generated apps use `cargo test --no-default-features --test wallet_fixture` | Application integration tests for tags, persistence, authorization and conflicts |
| Application template | `bash scripts/check-template.sh --no-run --cache-dir target/template-check` (domain tests and both build modes) | Omit `--no-run` for startup, deposit and overdraft smoke checks |
| GraphQL or REST | Relevant `skilj` integration target and `cargo test -p skilj --test conformance` | Workspace tests against Postgres |
| Code generator | `cargo test -p skilj-codegen` and `cargo test -p skilj-demo --test generated_code` | Build demo and template |
| Federation description | `cargo test -p skilj --test graphql_federation` | `scripts/check-federation-composition.sh` |
| Agent guides (`skilj/agent-guides`), or a trait they list | `node scripts/agent-guides/check.mjs` | `bash scripts/agent-guides/eval/check-solutions.sh --no-run --cache-dir target/template-check` (reference solutions of the evaluation tasks); [agent guidance](docs/agent-guidance.md) |
| CI scripts | `node --test scripts/ci.test.mjs scripts/cga/check.test.mjs scripts/agent-guides/check.test.mjs` | Run the changed script's real path |

For intended fixture changes, prefix the corresponding test command with
`SKILJ_RECORD_CONFORMANCE=1`, `SKILJ_RECORD_GENERATED=1`, or
`SKILJ_RECORD_FEDERATION=1`, then review the recorded diff. Domain fixtures
verify handwritten rules and folds; they do not verify tag selection,
persistence, authorization or concurrency. Before review, run the build,
test, lint and docs checks below appropriate to the change; database-backed
changes need real Postgres coverage. Time repeat runs separately from the
initial dependency build rather than assuming a universal fast-loop duration.

## Building and testing

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo fmt --check
cargo deny check   # advisories and licenses, per deny.toml
```

The minimum supported Rust version is 1.95 (`rust-version` in
`Cargo.toml`), and CI builds the libraries with it: code using a newer
standard-library API needs that raised, which is a minor-release change
(README.md, "Status and stability"). Every public enum of the library
crates is `#[non_exhaustive]`, and a new one must be too (docs/architecture.md
§219). A match in another crate needs a `_` arm answering
`skilj_core::Error::Unsupported`, so adding a variant compiles without
complaint: search for the enum's matches in the other crates and handle
it there.

Integration tests use `DATABASE_URL` when set, otherwise an embedded Postgres.
Use a disposable server: each test binary drops and recreates its own named
database, and the role needs `CREATEDB`. Broker and S3Mock tests need Docker;
Temporal tests download their own test server. A local run can pass with
service-dependent tests skipped. `bash scripts/ci-test.sh` reproduces CI's
setup and no-skip guard; it requires the service prerequisites.

For embedded-server libraries, Docker connectivity, Temporal downloads,
CI database diagnostics and disk-space problems, see
[environment troubleshooting](docs/troubleshooting.md).

CI runs on GitHub Actions (docs/architecture.md §222). The Rust jobs run
in a `rust:1-bookworm` container, as root, because the CI database scripts
were written against that; a job that runs on the bare runner instead
would need `sudo` for the in-job Postgres fallback and the failover
test. The `actionlint` job checks `.github/workflows/ci.yml` itself.

The required `test` check aggregates three whole-binary shards: `business`
(`graphql_business_surfaces`), `app` (the other `skilj` targets and its
doctests), and `libraries` (every other workspace crate). Each shard has
its own Postgres server and the same no-skip guard. Both app shards enable
`skilj/local`, as the full workspace does through `skilj-demo`. To reproduce
a shard, set `CI_TEST_GROUP=business`, `app`, or `libraries` before running
`bash scripts/ci-test.sh`; omit it for the full workspace. `--build-only`
compiles the selected group without a database and writes Cargo timings.
The shard jobs upload their timing reports and test logs, including on failure.

It also requires a focused PostgreSQL compatibility matrix on 15 and 18
(the minimum and newest supported majors). The full suite stays on 17;
brokers and Temporal are not repeated in the matrix. See
[PostgreSQL compatibility](docs/postgres-compatibility.md) for the support
policy, exact scenarios, local command and limits. Compatibility jobs verify
the actual server major before creating test databases and refuse a fallback
server. Run `node --test scripts/ci.test.mjs scripts/cga/check.test.mjs` after
changing the CI scripts.

Coverage is informational and runs on pushes to `main` and manual workflow
dispatch, rather than repeating the instrumented full run on every PR.
Clippy and rustdoc stay together; MSRV and CGA keep their own toolchains.
Most dependency caches are saved on `main`; the PR-only semver job saves a
PR-scoped cache for subsequent updates. The native template uses a stable
generated project and cached target directory. Its Docker check imports
and exports BuildKit layers; the template's pinned `cargo-chef` dependency
stage survives source-only changes, while the final image is still smoke-tested.

The blocking `check-cga` job compares potential panic paths in `skilj-core`
with a checked-in per-finding baseline. Run `node scripts/cga/check.mjs`;
after a deliberate reduction, use `--record` and review the baseline diff.
The tool is pinned and patched around an upstream generic-dispatch crash.
Skipped targets are tracked too. Coverage is partial and excludes the
GraphQL/REST server; see [the CGA guide](scripts/cga/README.md).

## What the tests do and don't verify

A green `cargo test --workspace` doesn't vouch for every guarantee
equally. Some guarantees hold whatever the tests do, some hold only as
far as a test exercises them, and some aren't checked at all
(docs/architecture.md §183).

**By construction.** These hold even on code paths no test reaches.
- A bounded context's sequence is gapless, and its events commit in
  sequence order. Allocation is an `UPDATE` of the context's `sequence`
  row inside the writing transaction. A rollback undoes it, and the row
  lock is held until commit (§57, §89).
- A command's DCB check, its command row, the events it triggers and its
  idempotency key commit or roll back together. So does every position
  skilj stores next to the events it describes: dedupe watermarks, read
  cursors, `caught_up_to` (§10, §176).
- The spec's uniqueness invariants (`Unique*NamePerContext`,
  `UniqueActiveExternalSubject`, `UniqueActiveAccessPerRoleAndContext`,
  `UniqueActiveEncryptionKeyPerSubject`, `UniqueReadCursorPerToken`) are
  primary keys or unique indexes.

**By tests.**
- **All SQL**, through the Postgres-backed suites in `skilj-core`,
  `skilj`, `skilj-demo` and `skilj-inspector`. In CI they run against a
  real Postgres, and `scripts/ci-test.sh` fails the build if any of them
  skipped (§67, §166).
- **The GraphQL and REST surfaces**: `skilj/tests/`, and both checked
  against one transcript in `skilj/tests/conformance.rs` (§179).
- **Several instances on one database**: `skilj/tests/cross_instance*.rs`.
  These run two `Skilj` instances in one test process, with real
  `LISTEN`/`NOTIFY` between them.
- **Failover**: `skilj-test-support`'s `FailoverServer`, a real primary
  and standby, used by `skilj/tests/event_fetch_rest.rs` (§176).
- **Upgrading from v0.0.1**: a checked-in schema fixture in
  `skilj/tests/startup_bounded_contexts.rs` (§159).
- **Upgrade preflight and migration** (§249): the same file checks that the
  preflight reports a v0.0.1 bounded context's missing objects and changes
  nothing, that `migrate` stops at its lock timeout and a rerun completes,
  that two migrations at once both finish, and that the recorded schema
  shape matches what provisioning creates. `scripts/check-upgrade-pair.sh`
  (CI) upgrades a real database written by the previous release, as
  separate processes.
- **Known races**: a targeted test each, often using a trigger to force
  the interleaving. There is one property test,
  `UniqueReadCursorPerToken` in `skilj-core/tests/event_fetch_surface.rs`.
- **Blind tags** (§205): `skilj/tests/blind_tags.rs` checks that a blind
  value appears in no stored tag, query or payload, that racing claims
  accept one, and that erasure removes the hash from the database and
  from the event cache.
- **Blobs** (§239): `skilj/tests/blob_lifecycle.rs` checks who may upload and
  download, that the store holds no plaintext of a blob with a subject and
  that `forgetSubject` removes it, that a cut-off or wrong blob ends a
  download in an error, and that collection spares what a parked delivery
  or pending approval holds. The race between collection and a new pin has
  no test of its own; a failed pin storing nothing does. Real AWS S3 is
  verified by nothing: `skilj-blob-s3` runs against S3Mock.
- **The template**: `scripts/check-template.sh` generates it, builds it
  with and without its `local` feature, and runs the README's quickstart:
  the server starts its own embedded Postgres, a deposit is accepted, a
  larger withdrawal is rejected, and Ctrl-C stops it cleanly. CI runs it
  on the bare runner, so the embedded Postgres is downloaded as a reader's
  would be; `--no-run` skips the run on a machine that can't start it.
- **Federation composition**: `skilj/tests/graphql_federation.rs` checks
  what `/graphql` publishes as a subgraph and records it, and
  `scripts/check-federation-composition.sh` composes that with Apollo's
  and Hive's composition (§194).

**By nothing.**
- **SQL at compile time.** No `sqlx::query!`. Every statement is
  `AssertSqlSafe(format!(...))` (over 300 of them), because table names
  depend on each bounded context's schema and the macros need fixed
  ones. A wrong column name or type compiles, and fails only when the
  statement runs. The Postgres suites, and the skip guard that makes
  sure they actually ran, are the only check on SQL. Nothing measures
  coverage either, so no one knows which statements no test reaches.
- **Real routers in CI.** `scripts/federation-smoke.sh` runs skilj-demo
  behind Apollo Router and Hive Router, but downloads them, so it runs by
  hand only. Apollo Router's subscriptions need GraphOS, so only Hive
  Router's are exercised (§194).
- **Full-suite coverage on every Postgres major.** The full suite runs on
  17, and a focused compatibility suite runs on 15 and 18. Version 16 has
  no dedicated CI job. The matrix does not establish compatibility for SQL
  paths outside its scenarios; see the [coverage inventory](docs/postgres-compatibility.md).
- **Performance.** The benchmarks (`command_throughput`,
  `inbound_throughput`, `projection_catch_up_throughput`) are
  `#[ignore]`d. A throughput regression fails nothing.
- **Mixed versions on one database.** Upgrading is tested. Two skilj
  versions running side by side during a rolling deploy are not; §164
  records one known gap, and §205 another: a version from before blind
  tags would derive a blind tag's value in the clear. What *is* tested:
  the previous release and this one run one after the other against one
  database, and the previous one then refuses it
  (`scripts/check-upgrade-pair.sh`); a release refuses a schema level it is
  too old for, and starts at one declared compatible (§249). No pair has
  been declared compatible yet, so no two versions have run side by side.
- **Fallbacks for a variant a crate predates** (§219): the `_` arms
  that answer `Unsupported`, halt a bridge's partition or consumer, or
  treat a cross-instance notification as a resync. No such variant
  exists to send them, so they run only with crates of different
  versions in one build.
- **Spec against code.** No test is generated from `specs/skilj.allium`.
  Their alignment is checked by periodic drift audits.
- **Interleavings in general.** Races someone thought of have tests.
  Nothing explores interleavings systematically (no `loom`, no model
  checking), and nothing tests separate processes or a network partition
  between an instance and its database.

## Where things live

- [`specs/skilj.allium`](specs/skilj.allium) is the behavioural
  specification, written in Allium (a spec language/tooling used to
  describe system behaviour independent of any implementation). It
  describes *what* the system guarantees, independent of the Rust
  implementation.
- [`docs/architecture.md`](docs/architecture.md) explains *how* the
  spec is realised in Rust - crate boundaries, design decisions, and
  the reasoning behind them.

If your change affects observable system behaviour (a new rule, a
changed guarantee, a new surface), please update `specs/skilj.allium`
alongside the code, and run `allium check` against it if you have the
Allium CLI installed. Changes that are pure implementation detail
(refactors, performance work, doc fixes) don't need a spec change.

## Pull requests

- Keep PRs focused - one change, one PR.
- Add or update tests for behavioural changes. This codebase favours
  real integration tests (real Postgres, real GraphQL/REST calls) over
  mocks wherever practical.
- Update `CHANGELOG.md` under an `## [Unreleased]` heading if the
  change is user-visible.
- Open an issue first for anything large or architectural, so we can
  agree on direction before you invest the time.

## Reporting bugs

Open an issue with steps to reproduce. For security issues, see
[`SECURITY.md`](SECURITY.md) instead - please don't open a public issue
for those.
