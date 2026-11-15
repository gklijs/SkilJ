---
name: skilj-new-project
description: >
  Start a new skilj project from nothing: generate it from skilj's template,
  link the skilj guides of that version as skills, run its quickstart (a
  deposit accepted, an overdraft rejected), and say what to ask for next.
  Triggers on "start a skilj project", "create a new skilj app", "try
  skilj", "scaffold an event-sourced Rust app with skilj", or "set up skilj
  here". Do NOT use inside an existing skilj project (one whose Cargo.toml
  depends on skilj): its own AGENTS.md and the linked guides cover adding
  to it.
---

# Start a skilj project

skilj is a Rust library for event-sourced applications on Postgres: commands
decide against the events their tags select (Dynamic Consistency
Boundaries), and GraphQL and REST come built in. This skill only gets a
project to the point where it runs and its own guides take over. It carries
no copy of those guides: they ship inside the `skilj` crate, so the ones a
project links follow the skilj version in its `Cargo.lock`.

Run the steps in order. Each block is what to run; the check after it says
what must hold before you go on. Don't skip the quickstart: it is what shows
the user that the project works on their machine.

## 0. Before you start

- If the current directory has a `Cargo.toml` that depends on `skilj`, stop:
  this is an existing project, and its `AGENTS.md` says what to do.
- `cargo --version` must work, and report 1.95 or newer. Without Rust, point
  the user to <https://rustup.rs> and stop; installing a toolchain is theirs
  to decide.
- `cargo generate --version` must work. If it doesn't, `cargo install
  cargo-generate` installs it (a few minutes of compiling); ask before you
  run it.
- Nothing else is needed: no Docker, no Postgres, no account. The server
  starts an embedded Postgres of its own and downloads it on the first run.

Pick the project's name: the one the user gave, or else ask for one. It
becomes a directory under the current one and the crate's name, so use
lowercase letters, digits and `-` (`my-wallet`). Below it is `$NAME`.

## 1. Generate

```sh
# step: generate
cargo generate --git https://github.com/gklijs/SkilJ.git templates/skilj-template --name "$NAME"
```

Check: `$NAME/Cargo.toml`, `$NAME/AGENTS.md` and `$NAME/src/wallet.rs`
exist. Every following step runs inside `$NAME`.

## 2. Link the guides

```sh
# step: guides
cd "$NAME"
bash scripts/skilj-guides.sh --link
```

It resolves the project's dependencies, so the first run downloads the
crates. Check: it printed a directory ending in `agent-guides`, and
`.claude/skills/` and `.agents/skills/` each have six links whose names start
with `skilj`. The links are in `.gitignore`: they point into the Cargo
registry of this machine.

## 3. Run the quickstart

The domain tests first. They need no database:

```sh
# step: test
cargo test --no-default-features --test wallet_fixture
```

Check: every test passes.

Then the server. Run it in the background, or in a second terminal the user
can see, because it keeps running:

```sh
# step: run
cargo run --bin server
```

The first run compiles for a few minutes and downloads Postgres. When it is
ready it prints an `export DEPOSIT=... WITHDRAW=...` line and two `curl`
commands. Run that `export` line and then both `curl` commands exactly as
printed, from another shell. Check: the first answers `"accepted":true`, the
second `"rejectionKind":"insufficient_funds"` (it withdraws more than the
wallet holds). Then stop the server with Ctrl-C (SIGINT), which shuts it down
cleanly.

If the server can't start the embedded Postgres (a sandbox without network,
or a CI container running as root), say so and give the user the
alternative from the project's `README.md`: set `DATABASE_URL` to a Postgres
of their own. Don't report the quickstart as passed.

## 4. Say what to ask for next

Tell the user, briefly:

- what they have: one bounded context, `wallet` (`src/wallet.rs`), its tests
  in `tests/wallet_fixture.rs`, and a server with REST and GraphQL;
- that the guides load as skills in a new agent session started inside
  `$NAME` (Claude Code reads `.claude/skills/`, Codex `.agents/skills/`),
  and that `AGENTS.md` points to them either way;
- three good next requests, in their own words:
  - "Help me design the events and commands for <their domain>": the
    `skilj-event-modeling` guide. If they already model in an Event Modeling
    tool (Event Modeling HCL, Nebulit's Miro app, prooph board, an `.evml`
    file), that guide imports the model into a `.skilj.toml`.
  - "Add a command that <does something> to the wallet, with tests": the
    `skilj` guide.
  - "Make the email address of a customer unique": the `skilj-uniqueness`
    guide.

If the user keeps an [Allium](https://juxt.github.io/allium) specification,
the `skilj-event-modeling` guide also says how to keep one next to the
model. It is optional; nothing in the project needs it.
