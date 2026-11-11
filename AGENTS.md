# AGENTS.md

Build, test, and lint instructions for the skilj workspace.

## Build

```sh
cargo build --workspace
cargo build --workspace --all-targets
```

## Test

```sh
cargo test --workspace
```

The CI pipeline (`scripts/ci-test.sh`, which runs `cargo test --workspace -- --nocapture`) additionally checks that no test silently skips. It runs against a `postgres` service container via `DATABASE_URL`, falling back to a Postgres inside the CI step's own container when that service doesn't answer (docs/architecture.md §166). `bash scripts/ci-test.sh` reproduces that setup and run by hand.

CONTRIBUTING.md's "What the tests do and don't verify" lists which guarantees hold by construction, which the tests verify, and which nothing verifies (docs/architecture.md §183). In particular, no SQL is checked at compile time, so the Postgres suites are its only check.

`skilj/tests/conformance.rs` compares what GraphQL and REST answer against the checked-in `skilj/tests/fixtures/conformance.transcript`, and against each other (docs/architecture.md §179). When a change to either surface is intended, re-record it with `SKILJ_RECORD_CONFORMANCE=1 cargo test -p skilj --test conformance` and review the diff; a deliberate difference between the surfaces goes in the test's `DIVERGENCES` catalogue with its reason.

`skilj-demo/tests/generated_code.rs` compares what `skilj-codegen` generates for `skilj-demo/src/banking.skilj.toml` against the checked-in `skilj-demo/tests/fixtures/banking_generated.rs` (docs/architecture.md §191). After an intended change to either, re-record it with `SKILJ_RECORD_GENERATED=1 cargo test -p skilj-demo --test generated_code` and review the diff.

`skilj-core/src/db/sql/bounded_context_shape.txt` is the schema a bounded context has once startup patched it, which `skilj::preflight::inspect` compares against (docs/architecture.md §249). After a change to provisioning or a startup patch, re-record it with `SKILJ_RECORD_SHAPE=1 cargo test -p skilj --test startup_bounded_contexts`, review the diff, and raise `skilj_core::preflight::SCHEMA_LEVEL` if the change will be released (RELEASING.md).

`skilj/tests/graphql_federation.rs` compares the federation description skilj publishes, under two prefixes, against the checked-in `skilj/tests/fixtures/federation/` (docs/architecture.md §194). After an intended change, re-record it with `SKILJ_RECORD_FEDERATION=1 cargo test -p skilj --test graphql_federation` and review the diff; `scripts/check-federation-composition.sh` (Node.js) then composes the fixtures with Apollo's and Hive's composition, as CI does. `scripts/federation-smoke.sh` runs skilj-demo behind real Apollo and Hive routers; it downloads them, so it is local only.

Some integration tests require external services:
- **Postgres** — `skilj-core`, `skilj`, `skilj-inspector` and `skilj-demo` integration tests use `DATABASE_URL` when it is set, and otherwise fall back to `postgresql_embedded`. Set `DATABASE_URL` to a real Postgres to avoid embedded-server issues. `DATABASE_URL` supplies the server, not the database: every test binary drops and recreates a named database of its own on it (docs/architecture.md §167), so the role needs `CREATEDB`.
- **Kafka** (`skilj-kafka`), **AMQP** (`skilj-amqp`), **NATS** (`skilj-nats`) — these tests use `testcontainers-modules` and need a reachable Docker daemon. They skip gracefully if Docker is unavailable, except in CI, where any skip fails the build (docs/architecture.md §223).
- **S3** (`skilj-blob-s3`) — one test runs against an S3Mock container and needs a reachable Docker daemon, like the brokers; the other uses `object_store`'s in-memory backend.
- **Temporal** (`skilj-temporal`) — downloads Temporal's test server from temporal.download on first run and skips if it can't. Where that host is blocked, set `SKILJ_TEMPORAL_TEST_SERVER` to a `temporal-test-server` binary; the Java SDK's GitHub releases publish it (`temporal-test-server_<version>_linux_amd64.tar.gz`).

## Lint

```sh
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

## Dependencies

```sh
cargo deny check
```

`deny.toml` allows only licenses compatible with distributing skilj under MIT OR Apache-2.0; an advisory that does not apply goes in its `ignore` list with the reason.

## Docs

```sh
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

`skilj`, `skilj-core`, `skilj-rest`, `skilj-retry`, `skilj-test-fixture`, `skilj-macros` and `skilj-blob-s3` have `#![warn(missing_docs)]`, which clippy's `-D warnings` makes an error: a new public item there needs a doc comment.

Every public enum of the library crates is `#[non_exhaustive]` (docs/architecture.md §217, §219), and a new one must be too: match one from another crate with a `_` arm that answers `skilj_core::Error::Unsupported` (or the bridge's `BridgeError::Unsupported`). Adding a variant therefore compiles everywhere - search for the enum's matches in the other crates and handle it there.

## Guides for application authors

This file is for work on skilj itself. What an agent needs to build an application with skilj is in `skilj/agent-guides/`: six skills, shipped inside the `skilj` crate, and linked from `.claude/skills/` (Claude Code) and `.agents/skills/` (Codex). Edit them there, never through a copy (docs/architecture.md §248, docs/agent-guidance.md).

```sh
node scripts/agent-guides/check.mjs
node --test scripts/agent-guides/check.test.mjs
```

The check fails when a guide shows a trait other than as it is, names something the workspace's code doesn't contain, or links to a file that is gone. A change to `EventType`, `CommandType`, `Projection` or another trait a guide lists therefore needs the guide updated in the same diff. A name that only a guide's own example uses goes in `scripts/agent-guides/illustrative-names.txt`. `scripts/agent-guides/eval/check-solutions.sh` applies the reference solutions of the three evaluation tasks in `scripts/agent-guides/eval/` to a generated project (`--no-run` skips starting its server); when one stops applying or passing, fix the guide or the solution, and record the solution again as docs/agent-guidance.md describes.

## Panic analysis (CGA)

The blocking CI check analyzes `skilj-core` with a pinned [Call Graph Analyzer](https://github.com/OpenDevicePartnership/cga), pinned nightly, and a checked-in compatibility patch. Run:

```sh
node scripts/cga/check.mjs
node --test scripts/cga/check.test.mjs
```

The baseline compares finding identities and occurrence counts, so removing one finding cannot offset an unrelated new one. After an intended reduction, run `node scripts/cga/check.mjs --record` and review `scripts/cga/baseline.json`'s diff. Compiler failures and newly skipped generic calls fail the check. Tool/config changes require an explicit baseline update.

Coverage is partial: unresolved generic vtable calls are skipped with their names tracked ([CGA issue #6](https://github.com/OpenDevicePartnership/cga/issues/6)); GraphQL, REST, and other crates are outside the initial baseline. This does not establish that requests cannot panic. See `scripts/cga/README.md` for coverage, setup, artifacts, and the workaround. `[profile.cga]` is release with opt-level 2; normal builds stay on stable.

## CI checks

The full CI pipeline (see `.github/workflows/ci.yml`; GitHub Actions, docs/architecture.md §222) runs these as parallel jobs. Steps 8-9 run per whole-binary test shard (`business`, `app`, `libraries`), aggregated by the required `test` check; 10-11 stay together. Coverage (15) runs on `main` and manual dispatch; 16 is a weekly workflow:
1. actionlint — checks the workflow file itself, including shellcheck on its `run:` blocks
2. `scripts/check-section-refs.sh` — validates all "§N" citations in `docs/architecture.md`; the same job runs `node scripts/agent-guides/check.mjs`, which holds `skilj/agent-guides` to the workspace's traits, names and paths
3. `scripts/check-template.sh` — generates and builds `templates/skilj-template` against this workspace, then runs its quickstart (embedded Postgres, a deposit accepted, an overdraft rejected); `--no-run` skips the run. The same job runs `scripts/check-upgrade-pair.sh` (the previous release's template on an empty database, this workspace upgrading it with its data intact, the previous release then refusing the upgraded database; it fetches the previous release's tag, docs/architecture.md §249), then `scripts/agent-guides/eval/check-solutions.sh`: the generated project finds the agent guides, and the reference solution of each evaluation task applies, passes and answers over REST
4. `scripts/check-federation-composition.sh` — composes the recorded federation descriptions with Apollo's and Hive's composition
5. `cargo fmt --all -- --check`
6. `cargo deny check` — RustSec advisories, licenses, banned crates and sources, per `deny.toml`
7. `cargo +1.95 check --workspace --lib --bins` — the declared minimum supported Rust version (`rust-version` in `Cargo.toml`) still builds
8. `scripts/ci-test.sh --setup-only` — settles the CI job's Postgres before the long build, preferring the `services:` container and falling back to one inside the job's own container
9. `CI_TEST_GROUP=<group> bash scripts/ci-test.sh --build-only`, then `scripts/ci-test.sh` with the same group — `cargo test --workspace -- --nocapture` against that database, plus the guard that fails the build if any test skipped (Postgres, brokers or Temporal)
10. `cargo clippy --workspace --all-targets -- -D warnings`
11. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
12. `scripts/check-spec.sh` — `allium check` on `specs/skilj.allium`: fails on an error, or on more warnings than the script's `MAX_WARNINGS` (a ratchet; lower it when a warning is fixed)
13. `cargo machete` — no unused dependency; one kept on purpose goes in the crate's `[package.metadata.cargo-machete] ignored` with its reason
14. `cargo semver-checks` on the library crates against their latest release, pull requests only. Blocking: a breaking change fails it until the minor version is bumped in the same diff. A crate not yet on crates.io has no baseline and fails the whole job, so it goes in the step's `--exclude` until its first release
15. `cargo llvm-cov` — on pushes to `main` and manual workflow dispatch, line coverage of the full test run, in the job summary and as an lcov artifact. Informational (`continue-on-error`), no threshold
16. `cargo mutants` — weekly (`.github/workflows/mutants.yml`), on the access trail and the `*_with_trail` writers against the `access_trail` tests. Surviving mutants are listed in the job summary. Informational
17. `scripts/check-template-docker.sh` — builds `templates/skilj-template`'s Dockerfile against this workspace and starts the image (its own job, needs Docker)
18. `node scripts/cga/check.mjs` — pinned partial `skilj-core` panic analysis; fails on new findings, new analysis gaps, or analyzer failures. Its Node.js regression tests run in the same job.

Run `node --test scripts/ci.test.mjs scripts/cga/check.test.mjs scripts/agent-guides/check.test.mjs` after changing CI scripts.
The CI regression suite checks that sharding covers every integration target exactly once,
preserves doctests and `skilj/local`, and still fails when a service-dependent test skips.
The required `test` check also includes focused PostgreSQL 15/18 compatibility jobs;
the full suite stays on 17. `CI_TEST_GROUP=postgres` selects their six integration
binaries and requires `CI_POSTGRES_MAJOR`. These runs verify the server major and
refuse fallback servers; see docs/postgres-compatibility.md for local reproduction.
`CI_TEST_GROUP` defaults to `all` locally. `--build-only` needs no database and records timings.
Most Rust dependency caches save on `main`; semver saves within the PR because it is PR-only.
Template builds and the CGA tool binary have separate caches; see docs/architecture.md §222.
