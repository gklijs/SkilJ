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

The CI pipeline (`scripts/ci-test.sh`, which runs `cargo test --workspace -- --nocapture`) additionally checks that no Postgres-backed tests silently skip. It runs against a `postgres` service container via `DATABASE_URL`, falling back to a Postgres inside the CI step's own container when that service doesn't answer (docs/architecture.md §166). `bash scripts/ci-test.sh` reproduces that setup and run by hand.

CONTRIBUTING.md's "What the tests do and don't verify" lists which guarantees hold by construction, which the tests verify, and which nothing verifies (docs/architecture.md §183). In particular, no SQL is checked at compile time, so the Postgres suites are its only check.

`skilj/tests/conformance.rs` compares what GraphQL and REST answer against the checked-in `skilj/tests/fixtures/conformance.transcript`, and against each other (docs/architecture.md §179). When a change to either surface is intended, re-record it with `SKILJ_RECORD_CONFORMANCE=1 cargo test -p skilj --test conformance` and review the diff; a deliberate difference between the surfaces goes in the test's `DIVERGENCES` catalogue with its reason.

`skilj-demo/tests/generated_code.rs` compares what `skilj-codegen` generates for `skilj-demo/src/banking.skilj.toml` against the checked-in `skilj-demo/tests/fixtures/banking_generated.rs` (docs/architecture.md §191). After an intended change to either, re-record it with `SKILJ_RECORD_GENERATED=1 cargo test -p skilj-demo --test generated_code` and review the diff.

`skilj/tests/graphql_federation.rs` compares the federation description skilj publishes, under two prefixes, against the checked-in `skilj/tests/fixtures/federation/` (docs/architecture.md §194). After an intended change, re-record it with `SKILJ_RECORD_FEDERATION=1 cargo test -p skilj --test graphql_federation` and review the diff; `scripts/check-federation-composition.sh` (Node.js) then composes the fixtures with Apollo's and Hive's composition, as CI does. `scripts/federation-smoke.sh` runs skilj-demo behind real Apollo and Hive routers; it downloads them, so it is local only.

Some integration tests require external services:
- **Postgres** — `skilj-core`, `skilj`, `skilj-inspector` and `skilj-demo` integration tests use `DATABASE_URL` when it is set, and otherwise fall back to `postgresql_embedded`. Set `DATABASE_URL` to a real Postgres to avoid embedded-server issues. `DATABASE_URL` supplies the server, not the database: every test binary drops and recreates a named database of its own on it (docs/architecture.md §167), so the role needs `CREATEDB`.
- **Kafka** (`skilj-kafka`), **AMQP** (`skilj-amqp`), **NATS** (`skilj-nats`) — these tests use `testcontainers-modules` and need a reachable Docker daemon. They skip gracefully if Docker is unavailable.
- **Temporal** (`skilj-temporal`) — downloads Temporal's test server from temporal.download on first run and skips if it can't. Where that host is blocked, set `SKILJ_TEMPORAL_TEST_SERVER` to a `temporal-test-server` binary; the Java SDK's GitHub releases publish it (`temporal-test-server_<version>_linux_amd64.tar.gz`).

## Lint

```sh
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

## Docs

```sh
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
```

## CI checks

The full CI pipeline (see `.woodpecker.yml`) runs:
1. `scripts/check-woodpecker-config.sh` — validates `.woodpecker.yml` against Woodpecker's own JSON schema, so a key Woodpecker would only warn about fails the build instead
2. `scripts/check-section-refs.sh` — validates all "§N" citations in `docs/architecture.md`
3. `scripts/check-template.sh` — generates and builds `templates/skilj-template` against this workspace
4. `scripts/check-federation-composition.sh` — composes the recorded federation descriptions with Apollo's and Hive's composition
5. `cargo fmt --all -- --check`
6. `scripts/ci-test.sh --setup-only` — settles the CI step's Postgres before the long build, preferring the `services:` container and falling back to one inside the step
7. `cargo build --workspace --all-targets`
8. `cargo clippy --workspace --all-targets -- -D warnings`
9. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
10. `scripts/ci-test.sh` — `cargo test --workspace -- --nocapture` against that database, plus the silent-skip guard for Postgres tests
