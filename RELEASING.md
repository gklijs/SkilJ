# Releasing

The workspace has 17 crates. 15 are published to crates.io:
`skilj-macros`, `skilj-core`, `skilj-graphql`, `skilj-rest`, `skilj`,
`skilj-codegen`, `skilj-tui`, `skilj-inspector`, `skilj-temporal`,
`skilj-kafka`, `skilj-amqp`, `skilj-nats`, `skilj-test-fixture`,
`skilj-retry`, `skilj-bridge`. `skilj-demo` is not (`publish = false` in its
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
2. Update the matching `version = "..."` in each of the 9 internal
   `[workspace.dependencies]` path entries (`skilj-core`,
   `skilj-graphql`, `skilj-macros`, `skilj-rest`, `skilj`,
   `skilj-codegen`, `skilj-test-fixture`, `skilj-retry`, `skilj-bridge` - these are the
   only crates any other workspace member depends on via a
   `workspace = true` reference (`skilj-demo`'s own `[dev-dependencies]`
   is `skilj-test-fixture`'s one dependent); `skilj-tui`/`skilj-inspector`
   have no internal dependents and need no such entry).

Not 15 separate per-crate edits - one field plus 9 matching version
strings (`skilj-temporal`/`skilj-kafka`/`skilj-amqp`/`skilj-nats` have
no internal dependents either - `skilj-retry` is a dependency of
`skilj-core`/`skilj-bridge`/`skilj-kafka`/`skilj-amqp`/`skilj-nats`, and
`skilj-bridge` of the three broker bridges and `skilj-temporal`, not the
other way round -
the same reason `skilj-tui`/`skilj-inspector` need no entry).

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
   `skilj-macros`)
5. `skilj-tui` (zero internal deps), `skilj-inspector` (depends on
   `skilj-core` only), `skilj-temporal` (depends on `skilj-retry` and
   `skilj-bridge` - it speaks only skilj's wire protocol, the same posture
   as `skilj-tui`),
   `skilj-kafka`/`skilj-amqp`/`skilj-nats` (depend on `skilj-retry` and
   `skilj-bridge`, already published, plus skilj's wire protocol - the same
   posture as `skilj-temporal` otherwise) - independent of each other
   and of `skilj`

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
```

`skilj-temporal` no longer needs a system `protoc` binary as of 0.0.5 -
it builds via `temporalio-client`'s own `vendored-protox` feature
instead (pure-Rust `protox`, no external compiler). See
CONTRIBUTING.md's own note.

`skilj-temporal`'s own dependencies - `temporalio-client`/
`temporalio-common`/`temporalio-sdk-core` (dev-only) - are "Public
Preview" per Temporal's own docs as of when this crate was built: "the
API can and will continue to evolve." Treat a routine `cargo update`
touching any of the three as something to actually review, not the
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
- Point `templates/skilj-template/Cargo.toml`'s `skilj`/`skilj-core`
  requirements at the new version, then run
  `scripts/check-template.sh --published` - it generates the template and
  builds it against what crates.io now serves. (CI's own
  `scripts/check-template.sh` builds it against the workspace instead, so
  an API change that breaks the template shows up before a release.) 0.0.8
  shipped with the template still on 0.0.7, where it didn't compile at all.
