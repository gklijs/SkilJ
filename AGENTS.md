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

The CI pipeline (`cargo test --workspace -- --nocapture`) additionally checks that no Postgres-backed tests silently skip. See `.woodpecker.yml` for the exact CI test step.

Some integration tests require external services:
- **Postgres** — `skilj-core` and `skilj` integration tests use `postgresql_embedded` as a fallback when `DATABASE_URL` is not set. Set `DATABASE_URL` to a real Postgres to avoid embedded-server issues.
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
1. `scripts/check-section-refs.sh` — validates all "§N" citations in `docs/architecture.md`
2. `scripts/check-template.sh` — generates and builds the `templates/skilj-template` against this workspace
3. `cargo fmt --all -- --check`
4. `cargo build --workspace --all-targets`
5. `cargo clippy --workspace --all-targets -- -D warnings`
6. `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
7. `cargo test --workspace -- --nocapture` (with a silent-skip guard for Postgres tests)
