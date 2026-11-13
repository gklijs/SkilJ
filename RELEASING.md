# Releasing

The workspace has 18 crates. 16 are published to crates.io:
`skilj-macros`, `skilj-core`, `skilj-graphql`, `skilj-rest`, `skilj`,
`skilj-codegen`, `skilj-tui`, `skilj-inspector`, `skilj-temporal`,
`skilj-kafka`, `skilj-amqp`, `skilj-nats`, `skilj-test-fixture`,
`skilj-retry`, `skilj-bridge`, `skilj-blob-s3`. `skilj-demo` is not (`publish = false` in its
`Cargo.toml` - it's a worked example, not a library anyone should
depend on), and neither is `skilj-test-support` (the workspace's own
test harness). Other crates reference it only as a path-only
dev-dependency with no `version`, which `cargo publish` strips from the
published manifest - so it never needs publishing or a version bump.

## Version sync

Every crate uses `version.workspace = true`, pulling its version from
`[workspace.package] version` in the root `Cargo.toml`. A release is
therefore:

1. Bump `[workspace.package] version`.
2. Update the matching `version = "..."` in each of the 10 internal
   `[workspace.dependencies]` path entries (`skilj-core`,
   `skilj-graphql`, `skilj-macros`, `skilj-rest`, `skilj`,
   `skilj-codegen`, `skilj-test-fixture`, `skilj-retry`, `skilj-bridge`,
   `skilj-blob-s3`). The first nine are the only crates any other
   workspace member depends on via a `workspace = true` reference (the
   `[dev-dependencies]` of `skilj-demo` and `skilj` are
   `skilj-test-fixture`'s dependents); `skilj-blob-s3` has an entry but no
   dependent yet, so its version string still has to match.
   `skilj-tui`/`skilj-inspector` have no internal dependents and need no
   such entry.

Not 16 separate per-crate edits - one field plus 10 matching version
strings (`skilj-temporal`/`skilj-kafka`/`skilj-amqp`/`skilj-nats` have
no internal dependents either - `skilj-retry` is a dependency of
`skilj-core`/`skilj-bridge`/`skilj-kafka`/`skilj-amqp`/`skilj-nats`, and
`skilj-bridge` of the three broker bridges and `skilj-temporal`, not the
other way round -
the same reason `skilj-tui`/`skilj-inspector` need no entry).

3. Re-record the GraphQL/REST conformance transcript, which pins the
   version it was recorded against (docs/architecture.md §179):
   `SKILJ_RECORD_CONFORMANCE=1 cargo test -p skilj --test conformance`.
   Review its diff: only the first line should change, unless the
   release changes what either surface answers.
4. Point `templates/skilj-template/Cargo.toml`'s `skilj`/`skilj-core`
   requirements at the new version, in the same commit as the bump.
   CI's `scripts/check-template.sh` builds the template against the
   workspace through a `[patch.crates-io]`, which Cargo only applies
   when the workspace version satisfies the requirement - so the two
   change together, and the template already matches the moment the
   crates are on crates.io.

5. The schema level (docs/architecture.md §249,
   docs/upgrade-preflight-design.md). When the release changes the schema
   since the previous one - a new global migration under
   `skilj-core/migrations/`, a new or changed startup patch, a re-recorded
   `skilj-core/src/db/sql/bounded_context_shape.txt` - raise
   `skilj_core::preflight::SCHEMA_LEVEL` by one. Leave
   `MIN_COMPATIBLE_SCHEMA_LEVEL` equal to it, unless the previous release
   is declared able to keep running against the upgraded database: then
   set it to the previous release's level and say so in the CHANGELOG.
   `scripts/check-upgrade-pair.sh` reads both levels - the previous
   release's `SCHEMA_LEVEL` from its tag, this one's
   `MIN_COMPATIBLE_SCHEMA_LEVEL` - and expects the previous release to
   refuse the upgraded database when the first is below the second, and to
   keep running otherwise. A release that leaves the schema alone keeps
   both levels, so CI checks that the previous release still runs.
6. The CGA baseline (AGENTS.md, "Panic analysis"). The check analyzes
   against `scripts/cga/workspace.lock`, which names the workspace crates'
   versions, and the version also enters the symbol hashes, which moves
   some findings to another instance of the same call. With the bumped
   versions, after setting your own `Cargo.lock` aside: `cp
   scripts/cga/workspace.lock Cargo.lock && cargo update
   --workspace` (only the workspace members' versions may change), then
   `node scripts/cga/check.mjs --record`. The totals of findings and
   skipped generic targets should stay as they were; 0.4.0 moved findings
   between ten files and kept 6449 and 186.

### Doing this with `cargo-release`

The manual version above is correct but tedious and easy to get wrong
by hand across every workspace crate. [`cargo-release`](https://github.com/crate-ci/cargo-release)
(`cargo install cargo-release`) automates it: it bumps every
workspace-member version together, updates the internal path
dependencies' `version` fields to match, publishes each crate in true
dependency order, and tags the release - one command:

```sh
cargo release <patch|minor|major> --workspace --execute
```

Not wired into CI and not run automatically - install and run it
yourself when cutting a release. Dry-run first (omit `--execute`) to
see exactly what it would do.

## Compatibility check before publishing

From 0.1.0 on, a patch release (`0.1.0` → `0.1.1`) must be compatible
with the previous one - README.md's "Status and stability" promise, and
what Cargo assumes when it hands a user `0.1.1` for a `"0.1"`
requirement. That covers every published crate, the internal ones
(`skilj-macros`, `skilj-retry`, `skilj-bridge`) included: an older
`skilj-kafka` built against `skilj-bridge` `0.1.0` gets `0.1.1` too.
[`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks)
(`cargo install cargo-semver-checks --locked`) compares each crate's
public API against the version crates.io serves:

```sh
cargo semver-checks --workspace
```

- **Patch release:** it must report no breaking change. One it finds
  goes into the next minor release instead, or is reworked to stay
  compatible (a new function next to the old one, a defaulted trait
  method, a `#[non_exhaustive]` enum's new variant).
- **Minor release:** breaking changes are allowed; check that each one
  it reports is in CHANGELOG.md.

It reads the Rust API only. The GraphQL/REST contract is the
conformance transcript's job (step 3 above): a patch release's
re-recorded transcript should differ in its version line alone, or only
add fields and types. A raised `rust-version` is a minor-release change
too.

## Publish order (if publishing by hand)

Derived from the real internal dependency graph:

1. `skilj-macros`, `skilj-retry` (both zero internal deps)
2. `skilj-core` (depends on `skilj-macros` and `skilj-retry`),
   `skilj-bridge` (depends on `skilj-retry` only)
3. `skilj-graphql`, `skilj-rest`, `skilj-codegen`, `skilj-test-fixture`
   (each depends on `skilj-core` at most; independent of each other -
   `skilj-codegen` has zero internal deps, `skilj-rest`/
   `skilj-test-fixture` each depend only on `skilj-core`)
4. `skilj` (depends on `skilj-core`, `skilj-graphql`, `skilj-rest`,
   `skilj-macros`, and for its tests `skilj-test-fixture`)
5. `skilj-tui` (zero internal deps), `skilj-inspector` (depends on
   `skilj-core` only), `skilj-temporal` (depends on `skilj-retry` and
   `skilj-bridge` - it speaks only skilj's wire protocol, the same posture
   as `skilj-tui`),
   `skilj-kafka`/`skilj-amqp`/`skilj-nats` (depend on `skilj-retry` and
   `skilj-bridge`, already published, plus skilj's wire protocol - the same
   posture as `skilj-temporal` otherwise) - independent of each other
   and of `skilj`, `skilj-blob-s3` (depends on `skilj-core` only)

Wait for each crate to finish indexing on crates.io
(`cargo search <crate>` or the crates.io page) before publishing
anything that depends on it - a `path + version` dependency needs the
version to actually resolve from the registry.

```sh
cargo publish -p skilj-macros
cargo publish -p skilj-retry
# wait for indexing
cargo publish -p skilj-core
cargo publish -p skilj-bridge
# wait for indexing
cargo publish -p skilj-graphql
cargo publish -p skilj-rest
cargo publish -p skilj-codegen
cargo publish -p skilj-test-fixture
# wait for indexing
cargo publish -p skilj
cargo publish -p skilj-tui
cargo publish -p skilj-inspector
cargo publish -p skilj-temporal
cargo publish -p skilj-kafka
cargo publish -p skilj-amqp
cargo publish -p skilj-nats
cargo publish -p skilj-blob-s3
```

`skilj-temporal` no longer needs a system `protoc` binary as of 0.0.5 -
it builds via `temporalio-client`'s own `vendored-protox` feature
instead (pure-Rust `protox`, no external compiler). See
CONTRIBUTING.md's own note.

`skilj-temporal`'s own dependencies - `temporalio-client`/
`temporalio-common` - are "Public
Preview" per Temporal's own docs as of when this crate was built: "the
API can and will continue to evolve." Treat a routine `cargo update`
touching either as something to actually review, not the
same "trust semver, move on" confidence the rest of this workspace's
dependencies warrant - a patch-level bump there is more likely than
usual to need a real code change here, not just a version bump.

`skilj-kafka` needs a C compiler that can see `curl/curl.h` at build
time - `rdkafka`'s vendored `librdkafka` (the `cmake-build` feature)
includes that header unconditionally regardless of CURL support being
compiled out. See CONTRIBUTING.md's own note for the workaround with no
root available; wherever the publish actually runs needs this too,
`cargo publish` builds the crate as part of its own verification step.

## After publishing

- Tag the release: `git tag vX.Y.Z && git push origin vX.Y.Z`.
- Point `DEFAULT_FROM` in `scripts/check-upgrade-pair.sh` at the new tag,
  so the next release's CI upgrades from this one.
- The first release of a crate (`skilj-blob-s3` in 0.3.0) gives
  `cargo semver-checks` a baseline: drop it from the `--exclude` list of
  the semver step in `.github/workflows/ci.yml`.
- Run `scripts/check-template.sh --published` - it generates the
  template and builds it against what crates.io now serves (step 4 of
  "Version sync" already pointed it at the new version). Then
  `scripts/agent-guides/eval/check-solutions.sh --published`: the agent
  guides are in the published `skilj` crate, and the evaluation tasks'
  reference solutions pass against it (docs/agent-guidance.md). 0.0.8 shipped
  with the template still on 0.0.7, where it didn't compile at all.
