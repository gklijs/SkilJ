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

The issue #175 data-key cache change explicitly re-records the baseline after
review. The new paths include clock queue cleanup, inline SHA-256 digest
copying, cache and flight map operations, generation `Arc`s, shared futures
and semaphore permit cleanup. The digest copy uses SHA-256's fixed 32-byte
output; the analyzer still retains its standard-library length assertion.
The shared unwrap path adds synchronization, reference-count and allocation
assertions and propagates provider-future drop behavior; the baseline keeps
these as potential panic paths, without adding suppressions or claiming they
cannot occur. Adding `Clone` to the encryption error also changes two derived
finding identities. Tool, configuration, dependencies and the skipped generic
target multiset remain unchanged. Tests cover cancellation, admission, expiry,
retirement and invalidation races, but do not establish panic freedom.

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

The issue #132 large-payload change also re-records it after review: 407 new
identities, 86 gone at first record, mostly in the new `db/blobs.rs` and `blob.rs` and in the
checks added to the append paths. They are SQLx calls, formatting, cleanup,
allocation and serde derives, plus the poison-tolerant lock of the payload
warning throttle; the change adds no `unwrap`, `expect` or indexing outside
tests. Moved code and closure line numbers account for the ones gone. The
skipped generic target multiset, tool and configuration are unchanged; the
dependency snapshot grows with `object_store` in `skilj-blob-s3`. This accepts
additional analyzed code, not a reduction in potential panics.

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


The 2026-11-08 security fixes review and re-record the baseline after adding
bounded JWKS expiry and database guards for protected external provenance.
The reviewed delta has 51 added finding identities (64 occurrences) and 19
removed identities (22 occurrences). The additions are in
`access_control/mod.rs`, `db/schema.rs` and `event_store/mod.rs`: clock access,
HTTP status/error handling, key-cache lock/map operations, SQLx query creation,
binding, transaction and cleanup paths, formatting, and source-content string
cleanup/iteration. Tracing closure locations and changed cache guard types also
change existing identities. These remain recorded as potential panics; this
update does not claim the dependency paths cannot panic. The source changes add
no production `unwrap`, `expect`, unchecked indexing or unsafe block. All skipped
generic targets, analyzer pins/configuration and dependency snapshot remain
unchanged. Postgres regressions verify the actual guards, concurrent upgrade
cleanup, stale writers and metadata retention; JWT regressions verify removed
keys and outage behavior. This is an explicit review of the analysis growth,
not a panic-count reduction.

Issue #171's bounded cache pages review and re-record the baseline with 20
added identities (20 occurrences) and 18 removed identities (18 occurrences);
one existing bounded-context drop identity falls from four occurrences to three.
The two SQL queries now bind an optional upper sequence bound; their changed
query strings replace existing query/bind identities and add the extra bind.
The cache's existing `partition_point`/`range` operations move into a selection
helper, with their bounds still computed from the same guarded deque. The
bounded iterator changes allocation/collection and event-vector drop paths;
tracing's embedded async-block line numbers also change identities. Sampled
backtraces lead to standard-library allocation/unsafe-precondition assertions
and SQLx/drop paths. These remain potential panics; this is not a claim of
panic freedom. No production `unwrap`, `expect`, unchecked indexing or unsafe
code is added. Tool/configuration, dependency snapshot and the skipped generic
target multiset are unchanged. Postgres parity tests and an append/erasure
interleaving exercise the changed read path.

Issue #173's authorized SQL reads review and re-record the baseline with nine
new finding identities (12 occurrences), plus one additional occurrence of the
existing formatting-macro string-drop identity. No identities are removed.
The new paths are the count query's SQLx construction, generic binding,
fetch/cancellation and cleanup, string formatting/drop, and the query value's
`Debug` derive. Reviewed sample backtraces reach standard-library formatting,
allocation/alignment precondition checks, and SQLx argument/future/error cleanup;
these remain potential panics, not a claim that the paths cannot panic. The
production change adds no `unwrap`, `expect`, unchecked indexing or unsafe code.
The tool/configuration, dependency snapshot and skipped generic target multiset
are unchanged. Differential Postgres tests and page/erasure/failover conformance
complement this partial analysis; generic page collectors remain subject to its
existing coverage limits. The benchmark-only counting allocator forwards to
`System` and is outside the core-library analysis.
