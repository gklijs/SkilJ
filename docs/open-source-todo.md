# Once SklilJ is open source

A running list of things that are only worth doing (or only *make sense*
to do) once this repository is public - so they don't get lost between
now and whenever that happens. Not a roadmap or a set of commitments,
just a place to park "we should do X, but not yet" items instead of
letting them drop.

When one of these gets done, move it to a "Done" section at the bottom
(with the date and a link to what was actually done) rather than
deleting it - keeps a record of what this list already caught.

## Pending

- **List `skilj-temporal` on Temporal's own integrations page.** Mirrors
  the already-done DCB community listing below - Temporal maintains a
  browsable integrations page (<https://docs.temporal.io/integrations>)
  and a community org for third-party projects
  (<https://github.com/temporal-community>) to submit to. Worth doing
  once `skilj-temporal` is actually published to crates.io (see
  `RELEASING.md`), not before.

## Done

- **List skilj on the DCB community's implementations page.** 2026-08-28:
  opened [dcb-events/dcb-events.github.io#81](https://github.com/dcb-events/dcb-events.github.io/pull/81),
  adding `skilj` to the Rust section of `docs/resources/libraries.md`
  alongside Disintegrate, linking the Codeberg repo and the crates.io
  page. Merged by the DCB community maintainers as of 2026-08-30.

- **Publish `skilj`/`skilj-core` (and the rest of the workspace) to
  crates.io, and switch `templates/skilj-template/Cargo.toml` off its
  git dependency.** 2026-08-28: all 8 publishable crates (`skilj-macros`,
  `skilj-core`, `skilj-graphql`, `skilj-rest`, `skilj-codegen`, `skilj`,
  `skilj-tui`, `skilj-inspector`) published at `0.0.1`. The template's
  `Cargo.toml` now depends on `skilj = "0.0.1"`/`skilj-core = "0.0.1"`
  instead of the private git dependency, and its `Dockerfile`/`README.md`
  had the `openssh-client`/`ssh-keyscan`/`CARGO_NET_GIT_FETCH_WITH_CLI`/
  `--mount=type=ssh`/`--ssh default` machinery dropped, since a plain
  `cargo build`/`docker build` needs none of it once the dependency
  resolves from crates.io. Verified for real: a fresh `cargo generate`
  from the template built cleanly against the published crates. See
  `RELEASING.md` for the publish process itself.
