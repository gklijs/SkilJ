# CGA regression check

Run from the workspace root:

```sh
node scripts/cga/check.mjs
node --test scripts/cga/check.test.mjs
```

The first command installs the exact CGA revision in `check.mjs`, using its
pinned nightly (including `rustc-dev`, `rust-src`, and `llvm-tools`), into
`target/cga-tool` (or `CGA_TOOL_ROOT` when set). The LLVM component supplies the library needed to link
CGA's compiler driver; it must also be installed on cache hits. The command
applies `compat.patch`, then analyzes `skilj-core` on x86_64 Linux with `cga.toml`. Because the library
workspace does not track `Cargo.lock`, `workspace.lock` is a dedicated
analysis dependency snapshot. A missing root lock is initialized from it;
an existing different lock is refused unless you explicitly re-record.
Successful `--record` runs update both the snapshot and finding baseline.
It needs Git, Node.js 22 or newer, Rustup, network access for toolchain setup, and the usual
native build dependencies (including OpenSSL headers). It does not need
Postgres or Docker. The blocking `check-cga` CI job runs both commands.

CI sets `CGA_TOOL_ROOT` to `.ci-cga-tool`, outside Rust-cache's target
cleanup, and restores that directory with a dedicated Actions cache.
`node scripts/cga/check.mjs --tool-cache-key` prints the key without setting
up Rust or touching the workspace lockfile. It includes the CGA revision,
pinned nightly, target, and compatibility-patch hash. A tool cache hit skips
building the driver, but never skips installing its nightly components or
running the core analysis. Workspace dependencies use their existing Rust cache.

## What the check covers

This is a **partial core-library regression check**, not proof that the
server cannot panic. CGA traverses public library roots as well as binary
entry points. GraphQL, REST, demo, and other workspace crates are outside
this initial baseline. It reports possible call paths without proving
that their branch conditions can occur. Most current diagnostics are in
SQLx calls, formatting, or cleanup; they require review before being
classified as bugs.

Unmodified CGA crashes on unresolved generic vtable candidates involving
`jsonschema` ([upstream issue #6](https://github.com/OpenDevicePartnership/cga/issues/6)).
The compatibility patch skips those candidates and emits their names.
The baseline records the skipped names and occurrence counts, so a newly
skipped path fails CI rather than silently reducing coverage. This still
leaves known gaps: resolving concrete generic dispatch needs an upstream
fix. The patch also requests Cargo JSON diagnostics, sets `cga::panics`
to warnings so analysis can finish, and isolates the tool checkout's
workspace. It does not change SkilJ's normal build or suppress whole crates.

The pinned nightly cleans the primary crate before each analysis, avoiding
reliance on Cargo's replay of diagnostics and skipped-target output.
Dependencies remain cached. A compiler/analyzer failure, invalid JSON, or a missing
successful `skilj_core` artifact/build completion fails the check, even
with `--record`. Logs and the current findings are in `target/cga-report`
and uploaded by CI even when comparison fails.

## Reducing findings

The issue #158 tag-dictionary/search change explicitly re-records the baseline
after reviewing the analyzer output. New paths include SQLx query construction,
binding and cleanup, formatting/replacement of the migration SQL, and allocation
and cleanup while building Unicode prefix bounds. Sampled backtraces reach
standard-library capacity/unsafe-precondition assertions and SQLx error/drop
paths; they are retained as potential panics, not suppressed or declared
impossible. Embedded closure line numbers also change some existing identities.
The skipped generic target multiset, tool, dependencies and configuration remain
unchanged. This baseline update accepts additional analyzed code, not a reduction
in potential panics. Database and Unicode edge tests complement the analysis;
they do not establish panic freedom.

`baseline.json` records a multiset of `(file, diagnostic, normalized source
span)`, rather than just a maximum count. Line numbers and indentation
are excluded: moving code does not create findings. A new finding cannot
be offset by removing an unrelated one. Additional occurrences of an
existing finding also fail. Identical diagnostics on identical source
spans in the same file share an identity and are counted together.

After fixing a finding, re-record:

```sh
node scripts/cga/check.mjs --record
```

Review the baseline diff with the code change. Removed findings must be
removed from the baseline; the check refuses stale allowances. Baseline
increases require explicit review of the new paths. Do not whitelist a
whole runtime or dependency crate: a whitelist match on any frame hides
the entire path. No additional whitelist is used by this baseline.

A dependency/tool/configuration update can change diagnostics. The tool
revision, target, nightly, patch hash, and config hash are recorded to
make such baseline changes explicit. Update pins and review a fresh run
together. Remove the generic-dispatch workaround once upstream resolves
it; do not interpret fewer findings caused by skipped code as a fix.
