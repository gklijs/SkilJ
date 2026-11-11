# Guides for coding agents

Task guides for an agent (Claude Code, Codex, or any other that reads
files) working on an application built with skilj. They ship inside the
`skilj` crate, so the copy next to the sources you compile against always
describes that version.

Read the one guide your task needs. Each is a `SKILL.md` with a
description of when it applies, and `references/` it sends you to.

| The task | Guide |
|---|---|
| Add or change an event type, a command type or a projection; a schema change; a rejected registration or command | [skilj/SKILL.md](skilj/SKILL.md) |
| Design the events and commands of a domain before writing them | [skilj-event-modeling/SKILL.md](skilj-event-modeling/SKILL.md) |
| Create a bounded context, the first superadmin, a grant, the encryption keys; types that don't register | [skilj-bootstrap/SKILL.md](skilj-bootstrap/SKILL.md) |
| A value that must be unique: an email address, a username, an order number | [skilj-uniqueness/SKILL.md](skilj-uniqueness/SKILL.md) |
| A file, document or image; a payload over the limit | [skilj-large-payloads/SKILL.md](skilj-large-payloads/SKILL.md) |
| A long-running process across systems, with Temporal | [skilj-temporal/SKILL.md](skilj-temporal/SKILL.md) |

## Finding this directory from an application

It is `agent-guides/` next to the `skilj` crate's `Cargo.toml`, wherever
Cargo put the version the project builds against:

```sh
cargo metadata --format-version 1 |
    grep -oE '"manifest_path":"[^"]*/skilj(-[0-9][^/"]*)?/Cargo\.toml"' | head -n 1
```

A project generated from skilj-template has `scripts/skilj-guides.sh`,
which prints the directory, and its `AGENTS.md` points here.

## Loading them as skills

Both Claude Code and Codex load a directory holding a `SKILL.md` as a
skill, and both follow a symlink to one:

| Agent | Looks in |
|---|---|
| Claude Code | `.claude/skills/<name>/` |
| Codex | `.agents/skills/<name>/` |

`scripts/skilj-guides.sh --link` in a generated project links every guide
into both. The links name a version's directory, so run it again after
changing the skilj version. Without the links, an agent that reads
`AGENTS.md` still finds the guides through the table above.

## What the guides refer to

- **Paths such as `skilj-demo/src/banking.rs`, `skilj/tests/blind_tags.rs`
  or `docs/authorization.md`** are files of the skilj repository, not of
  your project. Read them at the tag of your version:
  `https://github.com/gklijs/SkilJ/blob/v<version>/<path>`
  (`https://raw.githubusercontent.com/gklijs/SkilJ/v<version>/<path>` for
  the file alone).
- **`docs/architecture.md` §N** is where a decision is reasoned out. No
  guide needs it to be read: it says why, and the guide says what to do.
  The file is over a megabyte, so never load it whole. One section is

  ```sh
  curl -s https://raw.githubusercontent.com/gklijs/SkilJ/v<version>/docs/architecture.md |
      awk -v n=205 '/^## [0-9]+[a-z]?\. /{p = ($2 == n".")} p'
  ```

- **`src/wallet.rs` and `tests/wallet_fixture.rs`** are the worked example
  in a project generated from skilj-template: an event, a command and a
  projection, and tests for the last two.

## Checking a change

`decide()` and `project()` are pure, so test them with
`skilj-test-fixture`, which needs no database:

```sh
cargo test --no-default-features
```

What that doesn't cover (tag selection, projection keys, authorization,
concurrency) needs the application running against Postgres.
