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
| The `skilj` plugin | This repository's marketplace ([plugins/skilj](../plugins/skilj)), for Claude Code and Codex | Starting a project from inside the agent, before one exists |

There is one copy of the guides. Each is a directory with a `SKILL.md`,
the format Claude Code and Codex both load, and a `references/` directory
it sends the agent to. Nothing in a guide requires reading
`docs/architecture.md`: a `§N` there says where a decision is reasoned
out, and the guides' `README.md` shows how to read one section of it.

## Before a project exists: the plugin

Nothing above reaches an agent until a project exists. The `skilj` plugin
fills that gap. It has one skill, `skilj-new-project`: asked to "start a
skilj project", the agent generates one from the template, runs
`scripts/skilj-guides.sh --link`, runs the quickstart (the domain tests, then
the server and the two commands it prints), and says what to ask for next.

```sh
# Claude Code
claude plugin marketplace add gklijs/SkilJ
claude plugin install skilj@skilj
# Codex
codex plugin marketplace add gklijs/SkilJ
codex plugin add skilj@skilj
```

The plugin carries no copy of the guides: those keep coming from the crate,
so they follow the project's skilj version and not the plugin's. Start a new
session inside the generated project and they load as skills.
`scripts/check-new-project-skill.sh` runs what the skill tells the agent to
run against the generated template, in CI, so the two can't drift
(architecture.md §252).

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
describes behavior wrongly. For that there are ten tasks.

## The evaluation tasks

Each is a request as a user would write it, in
`scripts/agent-guides/eval/tasks/`, for a project generated from the
template, with the files it needs (`fixtures/<task>/`) put in the project
before the agent starts:

| Task | Asks for | What it takes | Guides |
|---|---|---|---|
| `add-command` | Closing a wallet, with tests | A command, its event, the enum variant and its decoding, the tag, and two existing commands that now read the new event | `skilj` |
| `evolve-event` | An optional reference on a deposit, in production | An additive schema change on the event and the command, and old events and old clients that keep working | `skilj` |
| `sensitive-field` | An owner with an email address that can be forgotten | A sensitive field on the event and on the command, a subject, no tag on the value, and a key provider in the server | `skilj` |
| `unique-email` | An email address that belongs to one owner across wallets | A blind tag erased with the owner, a consistency query on it, normalization as an email address, a blind index key and a key provider in the server | `skilj-uniqueness` |
| `attach-document` | A monthly statement PDF on a wallet, erased with its owner | A blob field with a subject on the event and the command, upload first and append second, a key provider | `skilj-large-payloads` |
| `wallet-statement` | Each wallet's transactions in order, with a running balance | A synchronous projection with keys per wallet | `skilj` (projection.md) |
| `import-model` | The bounded context an Event Modeling export describes | `skilj-codegen import --from em-json`, a `.skilj.toml` that keeps what the model says, the decisions after its specifications, a second bounded context registered | `skilj-event-modeling` (its import half) |
| `retry-idempotent` | A client that retries a deposit without booking it twice | An `Idempotency-Key` per deposit, retries on transport errors and 5xx only, the retention window | `skilj-temporal` (its first half: a command submitted idempotently) |
| `second-context` | A rewards bounded context next to wallet | A bounded context created and registered at startup in local development, with tokens per context | `skilj-bootstrap` |
| `temporal-activity` | A Temporal workflow started for every opened wallet | The `skilj-temporal` bridge, an event read token that starts when the bridge is enabled (`DevAdmin::create_event_read_token` with `ReadFrom::Now`), `event_read_allowed` | `skilj-temporal` (its second half: the bridge) |

So every guide is exercised by a task: `skilj` by four,
`skilj-uniqueness` by `unique-email`, `skilj-large-payloads` by
`attach-document`, `skilj-event-modeling` by `import-model`,
`skilj-temporal` by `retry-idempotent` and `temporal-activity`, and
`skilj-bootstrap` by `second-context` (architecture.md §257).

Some of it isn't checked over the wire, because a project from the
template can't: skilj has no REST route that reads a projection, and a
GraphQL read, a download of a blob with a subject and `forgetSubject` need
a Role authenticated by an identity provider, which the template doesn't
configure. `wallet-statement` is checked by folding a history through the
projection as skilj's writer does, `attach-document` by an upload, an
append and the refusals, and the erasure in `unique-email` and
`attach-document` by what the types declare.

`scripts/agent-guides/eval/check.sh <task> <project> [--run]` decides
whether a project has done one. Each step passes, fails or is skipped, every
step that can runs, and it ends with a summary and exits 1 when any failed:

1. `own-tests`: the project's own tests pass.
2. `tests-added`: more of them passed than the template has tests. A test
   gated on local mode or ignored doesn't count.
3. `build-local` and 4. `build-no-local`: it builds with and without local
   mode.
5. `static`: the task's checks of the source, where it has any: a key
   provider and a blind index key are configured, and for `import-model`
   that `src/savings.skilj.toml` passes `skilj-codegen check` and keeps
   every type, field, tag, REST trigger and rejection the import of the
   original model has (`compare_import.py`).
6. `acceptance`: `acceptance/<task>.rs`, copied into the project as
   `tests/skilj_eval_acceptance.rs` (a project file of that name fails the
   step and is left alone), passes: decisions for given histories, the tags,
   sensitive fields and blob fields the types declare, and for
   `evolve-event` that the new schemas are revisions of the stored ones by
   the rule startup applies (`schema_is_backwards_compatible`).
7. `run`, with `--run`: the server starts on an embedded Postgres and
   answers the task's requests over REST. A command that is rejected for an
   event written by an earlier request shows that the event is stored,
   tagged, decoded and, for the email address, decrypted.
   `temporal-activity` runs Temporal's development server in Docker
   (`temporalio/temporal:1.5.0`, so it needs Docker, and fails without it):
   a wallet opened while the server ran without `TEMPORAL_ADDRESS`, then on
   the same data with it a wallet that starts its `WalletOnboarding`
   workflow, while the first one doesn't.

A step that needs a build is skipped when that build failed.

**Reference solutions.** `solutions/<task>.patch` is a solution of each
task. `scripts/agent-guides/eval/check-solutions.sh`, in CI in a job of its
own (`agent-guide-solutions`, with Docker for Temporal), applies each to the
generated project and runs the check on it, with `--run` unless it was given
`--no-run`, and puts the project back exactly as it was. So what the guides
teach is compiled and run against every change to the crates and to the
template. When the template changes, a patch may stop applying: apply it by
hand in a generated project, fix it, and record it again with

```sh
scripts/agent-guides/eval/record-solution.sh <task> <project>
```

which diffs the project's `src/`, `tests/`, `build.rs` and `Cargo.toml`
against a freshly generated project with the task's fixtures, and leaves out
key files, the guides' links and `Cargo.lock`.

**With an agent.** `scripts/agent-guides/eval/run.sh <agent> <task>
[--link] [--model <model>]` generates a project outside the repository,
hands the agent the task's text and nothing else, runs the check with
`--run` on the result, and writes a result record. Without `--link` the
agent has `AGENTS.md`; with it, the guides are skills as well. This
workspace's `skilj-codegen` is first on the agent's `PATH`, so
`import-model` exercises this workspace's importer. The agents:

- `claude` and `codex`: the CLI, signed in, with `--model` (required, and
  recorded). `SKILJ_EVAL_CLAUDE` and `SKILJ_EVAL_CODEX` name another binary.
- `skilj-agent`: `skilj-agent task` with a local model, with `--model`
  (required, and recorded). It writes a change only when its verifier
  passed it, builds under the shared build directory, and its log ends with
  one JSON record of its settings, executable and outcome.
  `SKILJ_EVAL_SKILJ_AGENT` names another binary; `SKILJ_EVAL_DEADLINE`
  stops it after that many seconds.
- `none`: does nothing. Its run fails, and shows which steps a task's
  checks fail on an untouched project.
- `reference`: applies the task's reference solution. Its run passes, and
  tests the whole pipeline without a paid session.

`scripts/agent-guides/eval/suite.sh <agent> [--link] [--model <model>]
[--only <task>,...]` runs every task for one agent, goes on past a failed
one, and lists the records written.

Each run has a directory of its own under `SKILJ_EVAL_DIR`
(`~/.cache/skilj-agent-eval`) with the project, the agent's log and the
check's logs. The build directory is shared and used by one run at a time:
a run holds its lock from before the agent starts until its check is done,
and another run on the same build directory waits, saying so (a hung agent
holds it until it is stopped). `SKILJ_EVAL_BUILD_DIR` gives a run a build
directory of its own. The lock is taken with `flock`, so the scripts need
Linux; `check.sh` on its own, and so `check-solutions.sh`, takes the same
lock.

**Result records.** Every run writes
`scripts/agent-guides/eval/results/<run id>.json` (or
`SKILJ_EVAL_RESULTS`), a failed run too:

| Field | Holds |
|---|---|
| `format` | 1 |
| `runId`, `agent`, `agentVersion`, `model`, `task`, `linked` | What ran |
| `skiljCommit`, `dirty`, `guidesHash` | What it ran against: the commit, whether the checkout had changes, and a hash of the guides, the template and the tasks, so a record made before a commit still says what it measured |
| `startedAt`, `durationSeconds`, `lockWaitSeconds` | When, and how long, not counting the wait for the build directory |
| `steps`, `passed`, `failure` | Each check step's `pass`, `fail`, `skipped` or `unrun`, and the phase and message of the first failure |
| `costUsd`, `tokens` | What the agent's CLI reported, or `null` |

A third agent is a case in `run.sh` and, if its CLI reports usage, a parser
in `record.mjs`; the format stays the same. The records are committed with
the scoreboard they make.

The runs with `claude` and `codex` are manual and cost money: an agent
session per task. Run the suite after a change to a guide or to `AGENTS.md`,
and before a release.

**Review by hand.** The check can't see everything. After a run, read
`git diff --cached` in the project for:

- `decide()` and `project()` stay pure: no clock, no I/O, no state outside
  the arguments.
- A new event has its variant in the event enum and its arm in
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
- `unique-email`: `decide()` decides on which events the blind tag
  matched, not on the address; the blind index key survives a restart.
- `attach-document`: the PDF is uploaded, not put in the payload as text.
- `import-model`: the decisions are hand-written after the model's
  specifications, and the `.skilj.toml` is the import's, not retyped.
- `retry-idempotent`: the key comes from the deposit, not from the
  attempt, and the documentation names the retention window.
- `temporal-activity`: the read token is minted before the server takes
  requests.
- Nothing outside the task was changed, and the project's `AGENTS.md` is
  as it was unless the task gave a reason.

## Scoreboard

The records in `scripts/agent-guides/eval/results/`, per task, agent, model
and whether the guides were linked as skills: how many runs passed, and the
latest run's date, outcome, skilj commit and guides hash.
`node scripts/agent-guides/eval/summarize.mjs --write` regenerates it from
the records, and CI fails when it differs from them (`--check`). The harness
agents `none` and `reference` are left out (`--baselines` shows them).

<!-- scoreboard:start -->
No runs recorded yet. Run the suite for an agent and model, then regenerate this
section:

```sh
scripts/agent-guides/eval/suite.sh claude --model <model> [--link]
node scripts/agent-guides/eval/summarize.mjs --write
```
<!-- scoreboard:end -->
