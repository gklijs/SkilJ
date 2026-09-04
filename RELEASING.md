# Releasing

The workspace has 10 crates. 9 are published to crates.io:
`skilj-macros`, `skilj-core`, `skilj-graphql`, `skilj-rest`, `skilj`,
`skilj-codegen`, `skilj-tui`, `skilj-inspector`, `skilj-temporal`.
`skilj-demo` is not (`publish = false` in its `Cargo.toml` - it's a
worked example, not a library anyone should depend on).

## Version sync

Every crate uses `version.workspace = true`, pulling its version from
`[workspace.package] version` in the root `Cargo.toml`. A release is
therefore:

1. Bump `[workspace.package] version`.
2. Update the matching `version = "..."` in each of the 6 internal
   `[workspace.dependencies]` path entries (`skilj-core`,
   `skilj-graphql`, `skilj-macros`, `skilj-rest`, `skilj`,
   `skilj-codegen` - these are the only crates any other workspace
   member depends on; `skilj-tui`/`skilj-inspector` have no internal
   dependents and need no such entry).

Not 10 separate per-crate edits - one field plus 6 matching version
strings (`skilj-temporal` has no internal dependents either, the same
reason `skilj-tui`/`skilj-inspector` need no entry).

### Doing this with `cargo-release`

The manual version above is correct but tedious and easy to get wrong
by hand across 9 crates. [`cargo-release`](https://github.com/crate-ci/cargo-release)
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

1. `skilj-macros` (zero internal deps)
2. `skilj-core` (depends on `skilj-macros`)
3. `skilj-graphql`, `skilj-rest`, `skilj-codegen` (each depends on
   `skilj-core` at most; independent of each other - `skilj-codegen`
   has zero internal deps, `skilj-rest` depends only on `skilj-core`)
4. `skilj` (depends on `skilj-core`, `skilj-graphql`, `skilj-rest`,
   `skilj-macros`)
5. `skilj-tui` (zero internal deps), `skilj-inspector` (depends on
   `skilj-core` only), `skilj-temporal` (zero internal deps - it speaks
   only skilj's wire protocol, the same posture as `skilj-tui`) -
   independent of each other and of `skilj`

Wait for each crate to finish indexing on crates.io
(`cargo search <crate>` or the crates.io page) before publishing
anything that depends on it - a `path + version` dependency needs the
version to actually resolve from the registry.

```sh
cargo publish -p skilj-macros
# wait for indexing
cargo publish -p skilj-core
# wait for indexing
cargo publish -p skilj-graphql
cargo publish -p skilj-rest
cargo publish -p skilj-codegen
# wait for indexing
cargo publish -p skilj
cargo publish -p skilj-tui
cargo publish -p skilj-inspector
cargo publish -p skilj-temporal
```

`skilj-temporal` needs a `protoc` binary on `PATH` (or `PROTOC` set) to
build at all - one of its dependencies generates Rust from `.proto`
files in its own build script. `cargo publish` builds the crate as part
of its own verification step, so this has to be true wherever the
publish actually runs, not just wherever it was developed. See
CONTRIBUTING.md's own note for how to get one without root.

`skilj-temporal`'s own dependencies - `temporalio-client`/
`temporalio-common`/`temporalio-sdk-core` (dev-only) - are "Public
Preview" per Temporal's own docs as of when this crate was built: "the
API can and will continue to evolve." Treat a routine `cargo update`
touching any of the three as something to actually review, not the
same "trust semver, move on" confidence the rest of this workspace's
dependencies warrant - a patch-level bump there is more likely than
usual to need a real code change here, not just a version bump.

## After publishing

- Tag the release: `git tag vX.Y.Z && git push origin vX.Y.Z`.
- Move the relevant item(s) in `docs/open-source-todo.md` to its "Done"
  section once the crates are live - in particular, switching
  `templates/skilj-template/Cargo.toml` off its git dependency onto a
  real version requirement only makes sense after this point.
