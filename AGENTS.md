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

Some integration tests require external services:
- **Postgres** — `skilj-core`, `skilj`, `skilj-inspector` and `skilj-demo` integration tests use `DATABASE_URL` when it is set, and otherwise fall back to `postgresql_embedded`. Set `DATABASE_URL` to a real Postgres to avoid embedded-server issues. `DATABASE_URL` supplies the server, not the database: every test binary drops and recreates a named database of its own on it (docs/architecture.md §167), so the role needs `CREATEDB`.
- **Kafka** (`skilj-kafka`), **AMQP** (`skilj-amqp`), **NATS** (`skilj-nats`), **Temporal** (`skilj-temporal`) — these tests use `testcontainers-modules` and need a reachable Docker daemon. They skip gracefully if Docker is unavailable.

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
4. `cargo fmt --all -- --check`
5. `scripts/ci-test.sh --setup-only` — settles the CI step's Postgres before the long build, preferring the `services:` container and falling back to one inside the step
6. `cargo build --workspace --all-targets`
7. `cargo clippy --workspace --all-targets -- -D warnings`
8. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
9. `scripts/ci-test.sh` — `cargo test --workspace -- --nocapture` against that database, plus the silent-skip guard for Postgres tests
