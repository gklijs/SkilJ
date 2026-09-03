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
