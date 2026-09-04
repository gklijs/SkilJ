# Contributing

Thanks for considering a contribution to `skilj`. This is a
solo-maintainer project, so the process is kept light.

## Building and testing

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace -- -D warnings
cargo fmt --check
```

Some integration tests spin up a real, embedded Postgres via
`postgresql_embedded`. On some Linux setups (observed on WSL after a
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

Building `skilj-temporal` (docs/architecture.md §34) needs a `protoc`
binary at compile time - `temporalio-protos` (a `temporalio-client`
dependency) compiles Temporal's own `.proto` files in its own build
script and doesn't vendor `protoc` itself, nor expose a feature to
(tried unifying in a `vendored-protox` feature via a feature-only
`prost-wkt-types` dependency, the trick that crate itself supports -
that only fixes `prost-wkt-types`'s own smaller build step, not
`temporalio-protos`'s, which has no such escape hatch and `links =
"temporalio_protos"`, so nothing downstream can inject one either;
confirmed by testing, not assumed). Install it via your system package
manager (`apt-get install protobuf-compiler` on Debian/Ubuntu, `apk add
protoc` on Alpine) or, with no root available, download a prebuilt
release directly (e.g.
`https://github.com/protocolbuffers/protobuf/releases`, a
`protoc-*-linux-x86_64.zip`) and point `PROTOC` at the extracted
`bin/protoc`. This is purely a build-time compiler - the compiled
binary has no runtime dependency on `protoc`/`libprotobuf` at all
(confirmed with `ldd`), so a multi-stage Docker build only needs this
in the *builder* stage; a genuinely `FROM scratch` final image (see
`templates/skilj-template/Dockerfile`) is completely unaffected either
way. `skilj-temporal`'s own real-Temporal-service tests
(`skilj-temporal/tests/temporal_bridge.rs`) separately download a small
ephemeral test-server binary on first run, cached under
`~/.cache/skilj-temporal-test-server` (deliberately never `/tmp` - see
the note on `/tmp` filling up below) for 15 days; they skip gracefully,
the same tolerance the embedded-Postgres tests above already have, if
that download can't reach the network.

A `postgresql_embedded`-backed test process's own data directory lives
under `/tmp` by default. On a small `/tmp` (a tmpfs capped well under
the host's real disk, common in a container or a WSL distro) a long
testing session can fill it entirely - observed as `cargo test` runs
slowing down or a test's own temp-file writes failing with `ENOSPC`,
not as a clean `skipping:` message the way the libxml2 issue above is.
`df -h /tmp` to check, and `rm -rf /tmp/.tmp*` plus killing any
still-running `postgres`/embedded-server child processes
(`pkill -9 -f postgresql` or similar) to clear it - safe, since nothing
under `/tmp` from these tests is meant to survive past the run that
created it.

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
