# Guidance for coding agents

skilj comes with instructions for an agent that writes an application with
it: Claude Code, Codex, or any other that reads files. This page says where
they are, how an agent finds them, how they follow your skilj version, and
how they are checked. The decisions behind it are in
[architecture.md §248](architecture.md).

Guidance for work on skilj itself is a different thing: that is the
repository's own [AGENTS.md](../AGENTS.md).

## What there is

| What | Where | For |
|---|---|---|
| Task guides | `agent-guides/` inside the `skilj` crate ([in this repository](../skilj/agent-guides/README.md)) | How to add a type, bootstrap a bounded context, make a value unique, store a large value, model a domain, pair with Temporal |
| Project instructions | `AGENTS.md` in a project generated from [skilj-template](../templates/skilj-template) | Where the domain code is, the rules that hold everywhere, the checks to run, and which guide a task needs |
| `scripts/skilj-guides.sh` | The same generated project | Prints where the guides of the project's skilj version are; `--link` makes them skills |

There is one copy of the guides. Each is a directory with a `SKILL.md`,
the format Claude Code and Codex both load, and a `references/` directory
it sends the agent to. Nothing in a guide requires reading
`docs/architecture.md`: a `§N` there says where a decision is reasoned
out, and the guides' `README.md` shows how to read one section of it.

## How an agent finds them

**Through `AGENTS.md`.** Both agents read it at the start of a session:
Codex from the repository root down to the working directory, Claude Code
(2.1.277 or later) when the project has no `CLAUDE.md`. A project with a
`CLAUDE.md` of its own imports it with a line `@AGENTS.md`. The file has a
table from task to guide, and the command that prints the guides'
directory. This works for any agent that reads `AGENTS.md`, without setup.

**As skills.** `bash scripts/skilj-guides.sh --link` links each guide into
`.claude/skills/` (Claude Code) and `.agents/skills/` (Codex). Both follow
a symlink to a skill directory, and then load a guide by its description
when a task calls for it. The links are in the project's `.gitignore`:
they point into the machine's Cargo registry.

In this repository the same six guides are linked from `.claude/skills/`
and `.agents/skills/`, so a session here has them as before.

A project that wasn't generated from the template can copy `AGENTS.md` and
`scripts/skilj-guides.sh` from it; neither names the project.

## Your skilj version, and your own instructions

The guides are part of the `skilj` package, so Cargo downloads them with
the version in your `Cargo.lock`, and `scripts/skilj-guides.sh` finds them
through `cargo metadata`. After `cargo update -p skilj`, the script prints
the new version's directory. Links made with `--link` name the old one:
run it again.

Nothing writes to your `AGENTS.md` after the project is generated. The
script only replaces links it made itself, and leaves a skill of your own
with the same name alone. Upgrading skilj therefore changes the guides and
none of your instructions.

A version of skilj from before the guides shipped has no `agent-guides/`
directory; the script says so, and `AGENTS.md` points at the guides on
`main` instead, which may describe a newer API than yours.

## How they are kept right

`node scripts/agent-guides/check.mjs`, in CI, compares the guides and the
template's `AGENTS.md` with the workspace:

- A `pub trait X { .. }` block in a guide lists exactly the items the
  trait has, with their signatures. Adding a method to `CommandType`
  fails the check until the guide has it.
- Every Rust path, type, function, error code or GraphQL name in
  backticks occurs in the workspace's code, and every `.method(` a guide
  or one of its Rust examples calls is a function some crate declares. A
  name that only an example of the guide's own uses is listed in
  `scripts/agent-guides/illustrative-names.txt`.
- Every repository path and every relative link resolves, and every
  `§N` names a section (`scripts/check-section-refs.sh`).
- Every guide is linked from both skill directories and listed in the
  guides' `README.md` and in the template's `AGENTS.md`.

That catches a guide that names something gone. It doesn't catch one that
describes behavior wrongly. For that there are three tasks.

## The evaluation tasks

Each is a request as a user would write it, in
`scripts/agent-guides/eval/tasks/`, for a project generated from the
template:

| Task | Asks for | What it takes |
|---|---|---|
| `add-command` | Closing a wallet, with tests | A command, its event, the enum variant and its decoding, the tag, and two existing commands that now read the new event |
| `evolve-event` | An optional reference on a deposit, in production | An additive schema change on the event and the command, and old events and old clients that keep working |
| `sensitive-field` | An owner with an email address that can be forgotten | A sensitive field on the event and on the command, a subject, no tag on the value, and a key provider in the server |

`scripts/agent-guides/eval/check.sh <task> <project> [--run]` decides
whether a project has done one:

1. The project's own tests pass, and there are more than the template has.
2. `acceptance/<task>.rs` passes: decisions for given histories, the tags
   and sensitive fields the types declare, and for `evolve-event` that the
   new schemas are revisions of the stored ones by the rule startup
   applies (`schema_is_backwards_compatible`).
3. It builds with and without local mode.
4. With `--run`, the server starts on an embedded Postgres and answers the
   task's requests over REST. A command that is rejected for an event
   written by an earlier request shows that the event is stored, tagged,
   decoded and, for the email address, decrypted.

**Reference solutions.** `solutions/<task>.patch` is a solution of each
task. `scripts/agent-guides/eval/check-solutions.sh`, in CI after the
template check, applies each to the generated project and runs the check
on it, with `--run` unless it was given `--no-run`. So what the guides teach is compiled and run against every
change to the crates and to the template. When the template changes, a
patch may stop applying: apply it by hand in a generated project, fix it,
and record it again with

```sh
(diff -ruN pristine/src src; diff -ruN pristine/tests tests) |
    sed -E 's#^(---|\+\+\+) pristine/#\1 a/#; s#^\+\+\+ (src|tests)/#+++ b/\1/#'
```

where `pristine` is a copy of the generated project's `src` and `tests`
from before the change.

**With an agent.** `scripts/agent-guides/eval/run.sh <claude|codex> <task>
[--link]` generates a project outside the repository, hands the agent the
task's text and nothing else, and runs the check with `--run` on the
result. Without `--link` the agent has `AGENTS.md`; with it, the guides
are skills as well. It needs the agent's CLI signed in and costs a
session, so it is run by hand: after a change to a guide or to `AGENTS.md`,
and before a release.

**Review by hand.** The check can't see everything. After a run, read
`git diff` in the project for:

- `decide()` and `project()` stay pure: no clock, no I/O, no state outside
  the arguments.
- The new event has its variant in `WalletEvent` and its arm in
  `try_from_event`, and every `match` on the enum handles it on purpose
  rather than through a `_` arm.
- Rejections have a `kind` in `snake_case` and a `reason` a person can read.
- The tests added exercise the new rules, including the rejections.
- `add-command`: the closing is found through the wallet's tag, not
  through a second lookup; `Balance` is unchanged.
- `evolve-event`: the field is an `Option`, nothing was renamed, removed or
  retyped, and no new event type was made for it.
- `sensitive-field`: the email address is in `sensitive_fields()` of both
  types, is not a tag and is not compared in `decide()`; the key provider's
  file is somewhere that survives a restart, and nothing logs the address.
- Nothing outside the task was changed, and the project's `AGENTS.md` is
  as it was unless the task gave a reason.

## Results

RESULTS_PLACEHOLDER
