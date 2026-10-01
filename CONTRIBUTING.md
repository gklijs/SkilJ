# Contributing

Thanks for considering a contribution to `skilj`. This is a
solo-maintainer project, so the process is kept light.

## Building and testing

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo fmt --check
```

Without `DATABASE_URL` set, integration tests spin up a real, embedded
Postgres via `postgresql_embedded`. On some Linux setups (observed on WSL after a
distro package upgrade) the cached `postgres` binary was built against
an older libxml2 ABI (`libxml2.so.2`) than the one the system now
provides (e.g. `libxml2.so.16`), and fails to start with `error while
loading shared libraries: libxml2.so.2: cannot open shared object
file`. Affected tests catch this and skip gracefully rather than fail,
so the symptom is quietly-passing tests that never actually touched a
real database - not a hard failure. To get real coverage back, point
`LD_LIBRARY_PATH` at any directory that still has a `libxml2.so.2` on
it (a JetBrains IDE's bundled LLDB often ships one, e.g. under
`~/.cache/JetBrains/*/bin/lldb/linux/x64/lib`; `find / -iname
'libxml2.so.2*' 2>/dev/null` locates one) - this is a known environment
quirk, not a code problem, and not specific to this project.

Setting `DATABASE_URL` to a Postgres you run yourself sidesteps all of
it: no embedded server is started, so nothing depends on that binary or
its libraries. What `DATABASE_URL` then supplies is the *server* - host,
port, credentials - not the database: each test binary drops and
recreates a named database of its own on that server (docs/architecture.md
§167), so the role behind `DATABASE_URL` needs `CREATEDB` and, if a
previous run was killed rather than exited and left sessions attached,
enough privilege to terminate them. Locally a real Postgres is
preferable anyway: an embedded server is downloaded and `initdb`'d on
first use, where a local Postgres is already there. CI sets it for the
same reason, to a `services:` container in `.woodpecker.yml`
(docs/architecture.md §166).

CI's database setup and test run live in `scripts/ci-test.sh` rather
than inline in `.woodpecker.yml`, and `bash scripts/ci-test.sh`
reproduces them by hand. It prefers the pipeline's `services:`
container, and falls back to a Postgres started inside the step's own
container (`scripts/ci-postgres-local.sh`) when the service doesn't
answer - so a database that isn't there is a loud `[ci-test] WARNING` in
the step's output rather than a red build, and the tests still run
against something real. `scripts/ci-wait-for-postgres.sh` is the
poller, and on failure prints what the step can see (name resolution,
hosts file, routes, a raw `/dev/tcp` connect, psql's own error); the
service's logs are not in the step's output, so that is the whole of
what there is to go on.

Two CI-config facts worth knowing before editing `.woodpecker.yml`:

- **`services:` is a workflow-level key, not a step-level one.** Drone's
  shape put it under the step; Woodpecker's schema has it at the top
  level, and a step object has no such property. Getting that wrong is
  what made five CI runs chase a Postgres that was never started - see
  docs/architecture.md §166.
- **Woodpecker's own linter reports schema problems without failing the
  pipeline.** `scripts/check-woodpecker-config.sh` (a CI step) validates
  the file against that same schema and *does* fail the build, so a key
  that would only warn cannot cost a run. It needs `python3-yaml` and
  `python3-jsonschema`, and network access for the schema fetch;
  `WOODPECKER_SCHEMA_URL` pins a specific version.

Building `skilj-temporal` ([docs/architecture.md §34](docs/architecture.md#skilj-temporal-plan)) no longer needs a
system `protoc` binary: `temporalio-client`/`temporalio-common` 1.0.0
(bumped from 0.8.0 as part of 0.0.5) added a `vendored-protox` feature,
which this crate now enables - it forwards down to `temporalio-protos`'s
own `vendored-protox` (pulling in the pure-Rust `protox` compiler
instead of shelling out to a system `protoc`) via
[sdk-rust#1590](https://github.com/temporalio/sdk-rust/pull/1590).
Confirmed by testing, not assumed: a full `cargo build`/`cargo
test -p skilj-temporal` (including the real-ephemeral-Temporal-service
integration tests below) passes clean with no `protoc` anywhere on
`PATH` and no `PROTOC` env var set. The 1.0.0 bump did need one small
source change - `WorkflowIdConflictPolicy`/`WorkflowIdReusePolicy` moved
from a `temporalio_common::protos::...` re-export to being owned
directly by `temporalio_client` - see `skilj-temporal/src/lib.rs`'s
imports. `skilj-temporal`'s own real-Temporal-service tests
(`skilj-temporal/tests/temporal_bridge.rs`) separately download a small
ephemeral test-server binary on first run, cached under
`~/.cache/skilj-temporal-test-server` (deliberately never `/tmp` - see
the note on `/tmp` filling up below) for 15 days; they skip gracefully,
the same tolerance the embedded-Postgres tests above already have, if
that download can't reach the network.

Building `skilj-kafka` ([docs/architecture.md §39](docs/architecture.md#external-message-dedup-create-external-event)/[§40](docs/architecture.md#skilj-kafka-bridge)) needs a C compiler
that can see `curl/curl.h` at build time - `rdkafka-sys`'s own vendored
`librdkafka` (`cmake-build` feature, which compiles it from source
rather than needing a system `libcurl4-openssl-dev` package broadly
installed) includes that header unconditionally in `rdkafka_conf.c`
even with CURL support itself compiled out (`WITH_CURL=0`) - a real
upstream quirk in this librdkafka version, not something a Cargo
feature avoids. With no root available: `apt-get download
libcurl4-openssl-dev` (no root needed, downloads the `.deb` to the
current directory without installing it) then `dpkg-deb -x
libcurl4-openssl-dev_*.deb extracted` and point `CPATH` at
`extracted/usr/include` (and, on a multiarch system,
`extracted/usr/include/x86_64-linux-gnu` too, where the actual
`curl/curl.h` lives) - `CPATH` is a plain GCC/Clang env var honoured by
angle-bracket `#include`s regardless of the build system driving the
compiler, so no `cmake`/`rdkafka-sys` configuration is needed beyond
setting it. Confirmed by testing, not assumed: a full real-Kafka
integration test (produce, consume, assert the payload round-trips)
compiled and ran clean with only this env var set.

`skilj-kafka`, `skilj-amqp`, `skilj-nats`, and `skilj-temporal`'s real-
broker tests (`skilj-kafka/tests/`, `skilj-amqp/tests/`,
`skilj-nats/tests/`, `skilj-temporal/tests/`) each need a reachable Docker
daemon (`testcontainers-modules` or `testcontainers` spinning up real,
ephemeral Kafka/Artemis/NATS containers, and `skilj-temporal` downloads a
small ephemeral Temporal server binary on first run, cached under
`~/.cache/skilj-temporal-test-server` for 15 days). On WSL with
Docker Desktop, the `docker` CLI on `PATH` may be a wrapper script that
hardcodes a stale `DOCKER_HOST` (observed pointing at a
`docker-desktop-bind-mounts` socket path that no longer exists) -
`unset DOCKER_HOST` before running these tests lets the client fall
back to the real, working socket at `/var/run/docker.sock` (confirmed
present and reachable via `docker version`/`docker run hello-world`
once unset). A known environment quirk, not a code problem - these
tests skip gracefully (the same `skipping:` tolerance every other
real-dependency test in this codebase already has) if Docker still
can't be reached after that.

To run just the broker bridge test suites locally (with Docker
available), after applying the `unset DOCKER_HOST` workaround on WSL:

```sh
cargo test -p skilj-kafka -p skilj-amqp -p skilj-nats -p skilj-temporal
```

CI does not run these (see `.woodpecker.yml`'s `build-clippy-test`
step: the `rust:1-bookworm` image has no Docker daemon), so they
always skip in the pipeline - run them by hand before merging changes
that touch any bridge crate. `skilj-temporal`'s own ephemeral-server
download also needs outbound network access on first run.

The embedded Postgres a test binary starts (via `skilj-test-support`),
and the Kafka/Artemis/NATS containers the bridge crates' tests start,
are cleaned up by a small watchdog process once that test binary exits
- normally, on a panic, on Ctrl-C, or when killed. Before that existed,
each test binary left its server running and its data dir under `/tmp`
behind; on a RAM-backed `/tmp` (a tmpfs, as on WSL) a few full
workspace runs filled it, observed as `ENOSPC` failures and slow or
timing-out tests, and very likely behind several WSL crashes
(docs/architecture.md §66). If you still see leftovers - from runs
before this fix, or from a VM crash that took the watchdog down too -
`df -h /tmp`, `ps -C postgres`, and `docker ps` show them; stop the
servers (`kill` their `postgres -D /tmp/.tmp...` postmaster), then
`rm -rf /tmp/.tmp*` and `docker rm -f` the containers. Setting
`DATABASE_URL` to one Postgres you run yourself avoids embedded servers
entirely, so does CI.

A separate, easier-to-miss disk-space issue: `target/` lives on the
*real* root filesystem, not `/tmp`, and a long session doing many full
workspace rebuilds (a dependency bump, a version bump touching every
crate's own `Cargo.toml`, several `cargo clean`s) can grow it far
larger than expected - observed once at over 800GB, filling the actual
disk to 100% and turning an otherwise-ordinary `cargo build` into a
confusing "No space left on device" failure with no relation to the
code being built. `df -h` (not just `df -h /tmp`) to check; `rm -rf
target` reclaims it unconditionally - it is pure build cache, never
source or data, and the next `cargo build` regenerates whatever it
needs, just slower for that one run.

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
