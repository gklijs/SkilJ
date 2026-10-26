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

`skilj/tests/graphql_federation.rs` compares the federation description skilj publishes, under two prefixes, against the checked-in `skilj/tests/fixtures/federation/` (docs/architecture.md §194). After an intended change, re-record it with `SKILJ_RECORD_FEDERATION=1 cargo test -p skilj --test graphql_federation` and review the diff; `scripts/check-federation-composition.sh` (Node.js) then composes the fixtures with Apollo's and Hive's composition, as CI does. `scripts/federation-smoke.sh` runs skilj-demo behind real Apollo and Hive routers; it downloads them, so it is local only.

Some integration tests require external services:
- **Postgres** — `skilj-core`, `skilj`, `skilj-inspector` and `skilj-demo` integration tests use `DATABASE_URL` when it is set, and otherwise fall back to `postgresql_embedded`. Set `DATABASE_URL` to a real Postgres to avoid embedded-server issues. `DATABASE_URL` supplies the server, not the database: every test binary drops and recreates a named database of its own on it (docs/architecture.md §167), so the role needs `CREATEDB`.
- **Kafka** (`skilj-kafka`), **AMQP** (`skilj-amqp`), **NATS** (`skilj-nats`) — these tests use `testcontainers-modules` and need a reachable Docker daemon. They skip gracefully if Docker is unavailable, except in CI, where any skip fails the build (docs/architecture.md §223).
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

`skilj`, `skilj-core`, `skilj-rest`, `skilj-retry`, `skilj-test-fixture` and `skilj-macros` have `#![warn(missing_docs)]`, which clippy's `-D warnings` makes an error: a new public item there needs a doc comment.

Every public enum of the library crates is `#[non_exhaustive]` (docs/architecture.md §217, §219), and a new one must be too: match one from another crate with a `_` arm that answers `skilj_core::Error::Unsupported` (or the bridge's `BridgeError::Unsupported`). Adding a variant therefore compiles everywhere - search for the enum's matches in the other crates and handle it there.

## CI checks

The full CI pipeline (see `.github/workflows/ci.yml`; GitHub Actions, docs/architecture.md §222) runs these as parallel jobs, steps 8-9 and 10-11 in one each (12-15 and 17 are their own jobs; 16 is a weekly workflow):
1. actionlint — checks the workflow file itself, including shellcheck on its `run:` blocks
2. `scripts/check-section-refs.sh` — validates all "§N" citations in `docs/architecture.md`
3. `scripts/check-template.sh` — generates and builds `templates/skilj-template` against this workspace, then runs its quickstart (embedded Postgres, a deposit accepted, an overdraft rejected); `--no-run` skips the run
4. `scripts/check-federation-composition.sh` — composes the recorded federation descriptions with Apollo's and Hive's composition
5. `cargo fmt --all -- --check`
6. `cargo deny check` — RustSec advisories, licenses, banned crates and sources, per `deny.toml`
7. `cargo +1.95 check --workspace --lib --bins` — the declared minimum supported Rust version (`rust-version` in `Cargo.toml`) still builds
8. `scripts/ci-test.sh --setup-only` — settles the CI job's Postgres before the long build, preferring the `services:` container and falling back to one inside the job's own container
9. `cargo test --workspace --no-run`, then `scripts/ci-test.sh` — `cargo test --workspace -- --nocapture` against that database, plus the guard that fails the build if any test skipped (Postgres, brokers or Temporal)
10. `cargo clippy --workspace --all-targets -- -D warnings`
11. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
12. `scripts/check-spec.sh` — `allium check` on `specs/skilj.allium`: fails on an error, or on more warnings than the script's `MAX_WARNINGS` (a ratchet; lower it when a warning is fixed)
13. `cargo machete` — no unused dependency; one kept on purpose goes in the crate's `[package.metadata.cargo-machete] ignored` with its reason
14. `cargo semver-checks` on the library crates against their latest release, pull requests only. Blocking: a breaking change fails it until the minor version is bumped in the same diff
15. `cargo llvm-cov` — line coverage of the full test run, in the job summary and as an lcov artifact. Informational (`continue-on-error`), no threshold
16. `cargo mutants` — weekly (`.github/workflows/mutants.yml`), on the access trail and the `*_with_trail` writers against the `access_trail` tests. Surviving mutants are listed in the job summary. Informational
17. `scripts/check-template-docker.sh` — builds `templates/skilj-template`'s Dockerfile against this workspace and starts the image (its own job, needs Docker)
