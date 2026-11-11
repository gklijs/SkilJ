# AGENTS.md

For coding agents working on this application. It is built with
[skilj](https://github.com/gklijs/SkilJ): event sourcing on Postgres, where
each command decides against the events its tags select. Add your own
project's instructions anywhere in this file; nothing regenerates it.

## Where things are

- `src/wallet.rs`: one bounded context. Its event types, its command
  types with their `decide()`, its projection, and the `WalletEvent` enum
  they share. A new bounded context is a new module like it, with its own
  `pub const BOUNDED_CONTEXT`.
- `src/lib.rs`: `register()`. It finds every `#[auto_register]` type by
  itself: don't list types there.
- `src/bin/server.rs`: the server and its development bootstrap. A new
  bounded context goes in the list `seed_admin` is given, a command to
  call over REST in `COMMAND_TYPES`.
- `tests/wallet_fixture.rs`: tests of `decide()` and `project()`.

## Rules

- `decide()` and `project()` are pure: no I/O, no clock, no randomness, no
  async. skilj may call `decide()` twice for one command. Time comes from
  `decision.now()` in `decide_with`.
- A command decides only on the events its `tag_mappings()` select. A fact
  `decide()` depends on has to carry one of the command's tags, or a
  concurrent command can change it unnoticed. Map the same tag key on the
  event types and on the command.
- A new event type needs four things: its payload struct, its
  `impl EventType`, a variant in the bounded context's event enum, and an
  arm in `try_from_event`. Its name in `EventSpec` and
  `consumed_event_types()` is a string the compiler doesn't check.
- A stored payload's schema only grows: add a field as `Option<T>`. Never
  rename, remove or retype one, and never make an optional field
  required. Old events are read with the new struct.
- Personal data goes in `sensitive_fields()`, on the event type and on
  the command type that carries it, before the first such event is
  written. A sensitive field can't be a tag; a value that must be unique
  uses a blind tag.
- A rejection is `CommandDecision::Rejected` with a `kind` callers branch
  on (`snake_case`) and a `reason` for people. Don't panic and don't
  return an error.

## Checks

```sh
cargo test --no-default-features    # decide() and project(), no database
cargo check --all-targets           # with the embedded Postgres of local mode
cargo run --bin server              # prints the commands to try it
```

Write a test in `tests/` for every rule you add to a `decide()`, with
`skilj_test_fixture::command::GivenEvents`, and for every fold, with
`skilj_test_fixture::projection::GivenEvents`. They get the history you
hand them, so they don't test tag selection, projection keys, access or
concurrency: for those, run the server.

## Guides, for the skilj version this project uses

skilj ships task guides inside its crate. Print where they are:

```sh
bash scripts/skilj-guides.sh
```

Read `README.md` there first, then only the guide the task needs:

| The task | Guide |
|---|---|
| Add or change an event type, command type or projection; a schema change; a rejected registration or command | `skilj/SKILL.md` |
| Design the events and commands of a domain | `skilj-event-modeling/SKILL.md` |
| A new bounded context, roles and grants, encryption keys, types that don't register | `skilj-bootstrap/SKILL.md` |
| A value that must be unique | `skilj-uniqueness/SKILL.md` |
| A file, a document, a payload over the limit | `skilj-large-payloads/SKILL.md` |
| A long-running process, with Temporal | `skilj-temporal/SKILL.md` |

`bash scripts/skilj-guides.sh --link` also makes them skills that Claude
Code and Codex load by themselves. Run it again after changing the skilj
version in `Cargo.toml`.

If the script finds no guides, this project's skilj is older than they
are: read them at
<https://github.com/gklijs/SkilJ/tree/main/skilj/agent-guides>, and trust
the crate's own documentation (`cargo doc --open -p skilj`) where the two
differ.
