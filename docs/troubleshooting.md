# Environment troubleshooting

Start with the normal commands in [CONTRIBUTING](../CONTRIBUTING.md).
These notes describe observed environment failures; a skipped test does not
verify the behavior it would have exercised. For application deployment,
see [operating an application](deployment.md).

## Postgres setup and CI diagnostics

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
same reason, to a `services:` container in `.github/workflows/ci.yml`
(docs/architecture.md §166).

CI's database setup and test run live in `scripts/ci-test.sh` rather
than inline in `.github/workflows/ci.yml`, and `bash scripts/ci-test.sh`
reproduces them by hand. It prefers the pipeline's `services:`
container, and falls back to a Postgres started inside the step's own
container (`scripts/ci-postgres-local.sh`) when the service doesn't
answer - so a database that isn't there is a loud `[ci-test] WARNING` in
the step's output rather than a red build, and the tests still run
against something real. `scripts/ci-wait-for-postgres.sh` is the
poller, and on failure prints what the step can see (name resolution,
hosts file, routes, a raw `/dev/tcp` connect, psql's own error). The
service's own logs are in the job's "Stop containers" step.

## Temporal compiler and test-server downloads

Building `skilj-temporal` ([docs/architecture.md §34](architecture.md#skilj-temporal-plan)) no longer needs a
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
the note on `/tmp` filling up below) for 15 days. If temporal.download is blocked, set
`SKILJ_TEMPORAL_TEST_SERVER` to a `temporal-test-server` binary (the Java
SDK's GitHub releases distribute one). They skip gracefully,
the same tolerance the embedded-Postgres tests above already have, if
that download can't reach the network.

## Kafka build dependencies

Building `skilj-kafka` ([docs/architecture.md §39](architecture.md#external-message-dedup-create-external-event)/[§40](architecture.md#skilj-kafka-bridge)) needs `cmake` and a C compiler, not a system libcurl: its TLS features build curl
from source for the `curl/curl.h` that `librdkafka`'s source includes
([§234](architecture.md)). Only a build with `default-features =
false` still needs that header on the system, from
`libcurl4-openssl-dev` or, without root, from `apt-get download
libcurl4-openssl-dev`, `dpkg-deb -x libcurl4-openssl-dev_*.deb extracted`
and `CPATH` pointing at `extracted/usr/include` and, on a multiarch
system, `extracted/usr/include/x86_64-linux-gnu`.

## Docker connectivity

`skilj-kafka`, `skilj-amqp`, `skilj-nats`, and `skilj-blob-s3`'s S3Mock
test need a reachable Docker daemon to start ephemeral containers.
Temporal uses its own downloaded test server rather than Docker. On WSL with
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

CI runs these in `.github/workflows/ci.yml`'s `test` job, and fails if
any of them skip (docs/architecture.md §223). The brokers start next to
the job's container, on the runner's Docker, so a test connects to
`get_host()` rather than `127.0.0.1` - keep it that way in a new broker
test. `skilj-temporal`'s own ephemeral-server download also needs
outbound network access on first run.

## Leftover services and disk space

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
remove only the data directories and containers you identified as belonging
to those stopped tests. Other temporary directories can belong to live programs. Setting
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

