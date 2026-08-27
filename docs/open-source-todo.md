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

- **List skilj on the DCB community's implementations page.**
  [dcb-events/dcb-events.github.io](https://github.com/dcb-events/dcb-events.github.io)'s
  own `docs/resources/libraries.md` explicitly invites a PR from anyone
  building a DCB-compatible event store, and skilj's own consistency
  mechanism is one (see Codeberg issue #11, closed, and
  `docs/architecture.md` §10 for the actual terminology mapping). Not
  worth doing while the repo is private - the whole point of that list
  is a link a reader can actually follow, and a private repo makes it
  dead for everyone but the owner. Once public: fork the repo, add one
  line to that file (name, language, link, short description),
  `gh pr create` - `gh` is already authenticated as the right GitHub
  account with `repo` scope, so this is genuinely a five-minute task
  once it's not pointless.

- **Publish `skilj`/`skilj-core` (and the rest of the workspace) to
  crates.io, and switch `templates/skilj-template/Cargo.toml` off its
  git dependency.** The template (Codeberg issue #10) depends on
  `skilj = { git = "https://codeberg.org/gklijs/SklilJ" }` for now,
  which only resolves for whoever already has repo access - fine while
  that's just the owner, not fine for anyone `cargo generate`-ing the
  template once it's meant to be generally usable. Once published:
  update both `skilj`/`skilj-core` lines in the template's `Cargo.toml`
  to real version requirements (`skilj = "0.1"`), drop the "not on
  crates.io yet" comment above them, and update the same note in the
  template's own `README.md`. Also simplify `templates/skilj-template/Dockerfile`
  at that point - the `openssh-client`/`ssh-keyscan`/
  `CARGO_NET_GIT_FETCH_WITH_CLI`/`--mount=type=ssh` machinery exists only
  to fetch the private git dependency; a plain `cargo build` needs none
  of it once the dependency is a crates.io version, and the README's
  `docker build` line can drop `--ssh default` too.

## Done

*(nothing yet)*
