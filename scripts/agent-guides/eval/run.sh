#!/usr/bin/env bash
# Gives one evaluation task of the agent guides to a coding agent, in a
# project freshly generated from the template, checks what it made
# (check.sh --run) and writes a result record (docs/agent-guidance.md,
# docs/architecture.md §257). The agent gets the task's text, and the
# task's fixtures in the project, and nothing else: what it knows about
# skilj is what the project's AGENTS.md and the guides tell it.
#
# The project builds against this workspace's crates, so the guides it
# reads are this workspace's too, and this workspace's skilj-codegen is
# first on its PATH (lib.sh setup_tools, before the agent starts). It
# lives outside the repository, where no AGENTS.md or skill of skilj's own
# is in reach of the agent.
#
# Agents:
#   claude, codex  the CLI, signed in; --model is required and recorded
#   none           does nothing: a baseline, and the check failing on the
#                  untouched project shows the task's checks test the task
#   reference      applies solutions/<task>.patch: the pipeline end to end
#                  without a paid session
# A third agent is a case in run_agent and, if its CLI reports usage, a
# parser in record.mjs; the record's format stays the same.
#
# Each run has a directory of its own under SKILJ_EVAL_DIR, with the
# project, the agent's log, the check's logs and its result. The build
# directory is shared, and used by one run at a time: the run holds its
# lock (lib.sh) from before the agent starts until the check is done, and
# passes it on to check.sh; a second run waits, saying so. The record's
# startedAt and durationSeconds start after the wait, which
# lockWaitSeconds records. A hung agent holds the lock until it is stopped.
#
# The record goes to results/<run id>.json (or SKILJ_EVAL_RESULTS), written
# for a failed run too, with the phase that failed in `failure` and the
# steps it didn't reach `unrun`. The exit code is 0 when the run passed.
#
# Usage: scripts/agent-guides/eval/run.sh <claude|codex|none|reference> <task> [--link] [--model MODEL]
#        <task>: a name in tasks/ (ls scripts/agent-guides/eval/tasks)
#        --link: run scripts/skilj-guides.sh --link first, so the guides
#                are skills the agent loads by itself
#        SKILJ_EVAL_DIR: where the runs go (~/.cache/skilj-agent-eval)
#        SKILJ_EVAL_BUILD_DIR: the build directory ($SKILJ_EVAL_DIR/build);
#                one of its own lets a run go in parallel with another
#        SKILJ_EVAL_RESULTS: where the record goes (results/)
#        SKILJ_EVAL_CLAUDE, SKILJ_EVAL_CODEX: the CLI to run (claude, codex)
#
# claude and codex cost what a session of that agent costs, so nothing
# runs them automatically.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"
root="$eval_root"

usage='eval-run: usage: run.sh <claude|codex|none|reference> <task> [--link] [--model MODEL]'
agent="${1:-}"
task="${2:-}"
link=false
model=
shift 2 || { echo "$usage" >&2; exit 2; }
while [[ $# -gt 0 ]]; do
    case "$1" in
        --link) link=true ;;
        --model)
            [[ $# -ge 2 && -n "$2" ]] || { echo "$usage" >&2; exit 2; }
            model="$2"; shift ;;
        *) echo "eval-run: unknown argument $1" >&2; echo "$usage" >&2; exit 2 ;;
    esac
    shift
done
case "$agent" in
    claude | codex)
        [[ -n "$model" ]] || { echo "eval-run: $agent needs --model, so the record says which model ran" >&2; exit 2; } ;;
    none | reference) ;;
    *) echo "$usage" >&2; exit 2 ;;
esac
[[ -n "$task" && -f "$here/tasks/$task.md" ]] || { echo "eval-run: no task '$task'" >&2; echo "$usage" >&2; exit 2; }

# What is measured, captured before anything else: records made before a
# commit still say what they measured.
skilj_commit="$(git -C "$root" rev-parse HEAD)"
dirty=false
[[ -n "$(git -C "$root" status --porcelain)" ]] && dirty=true
guides_hash="$(cd "$root" && git ls-files -co --exclude-standard -- skilj/agent-guides templates/skilj-template \
    scripts/agent-guides/eval/tasks scripts/agent-guides/eval/fixtures scripts/agent-guides/eval/acceptance |
    LC_ALL=C sort | while IFS= read -r file; do printf '%s\0' "$file"; cat "$file"; done | sha256sum | cut -c1-64)"

dir="${SKILJ_EVAL_DIR:-$HOME/.cache/skilj-agent-eval}"
mkdir -p "$dir"
dir="$(cd "$dir" && pwd)"
variant=plain
if $link; then variant=linked; fi
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$agent-$task-$variant-$(od -An -N2 -tx1 /dev/urandom | tr -d ' \n')"
run_dir="$dir/$run_id"
results="${SKILJ_EVAL_RESULTS:-$here/results}"
mkdir -p "$run_dir" "$results"
results="$(cd "$results" && pwd)"
app="$run_dir/app"
log="$run_dir/agent.log"
export CARGO_TARGET_DIR="${SKILJ_EVAL_BUILD_DIR:-$dir/build}"
mkdir -p "$CARGO_TARGET_DIR"

failure=
started_at=
duration=0
lock_wait=0
agent_version=
guides=

setup() {
    "$root/scripts/check-template.sh" --prepare-only --cache-dir "$run_dir" || return 1
    if [[ -d "$here/fixtures/$task" ]]; then cp -a "$here/fixtures/$task/." "$app/" || return 1; fi
    cd "$app" || return 1
    # Its own repository: an agent looks for instructions up to the root of
    # one. Staged and not committed, so `git diff --cached` after the agent
    # is what it changed.
    { git init -q && git add -A; } || return 1
    guides="$(bash scripts/skilj-guides.sh)" || return 1
    if $link; then bash scripts/skilj-guides.sh --link >/dev/null || return 1; fi
    # This workspace's importer, first on PATH for the agent and the check.
    setup_tools
}

run_agent() {
    local tool
    case "$agent" in
        claude)
            tool="${SKILJ_EVAL_CLAUDE:-claude}"
            agent_version="$("$tool" --version 2>/dev/null | head -n 1)"
            without_lock "$tool" --print --model "$model" --output-format stream-json --verbose \
                --permission-mode acceptEdits --add-dir "$guides" --add-dir "$CARGO_TARGET_DIR" \
                --allowedTools 'Bash(cargo:*)' 'Bash(skilj-codegen:*)' 'Bash(bash scripts/skilj-guides.sh:*)' \
                'Bash(git diff:*)' 'Bash(git status:*)' 'Bash(ls:*)' 'Bash(cat:*)' 'Bash(head:*)' 'Bash(tail:*)' \
                'Bash(grep:*)' Read Edit Write Glob Grep Skill \
                < "$here/tasks/$task.md" > "$log" 2>&1
            ;;
        codex)
            tool="${SKILJ_EVAL_CODEX:-codex}"
            agent_version="$("$tool" --version 2>/dev/null | head -n 1)"
            without_lock "$tool" exec -m "$model" --json --sandbox workspace-write --add-dir "$CARGO_TARGET_DIR" - \
                < "$here/tasks/$task.md" > "$log" 2>&1
            ;;
        none)
            echo "the none agent changes nothing" > "$log"
            ;;
        reference)
            patch -p1 < "$here/solutions/$task.patch" > "$log" 2>&1
            ;;
    esac
}

echo "eval-run: $agent${model:+ ($model)} on $task in $app"
if ! setup; then
    failure="setup: generating the project or building the tools failed (see above)"
    echo "eval-run: $failure" >&2
else
    wait_from="$(date +%s)"
    if ! eval_lock "$CARGO_TARGET_DIR"; then
        failure="setup: couldn't take the build directory's lock"
        echo "eval-run: $failure" >&2
    else
        lock_wait=$(($(date +%s) - wait_from))
        started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        start="$(date +%s)"
        echo "eval-run: agent log: $log"
        run_agent
        agent_rc=$?
        if [[ $agent_rc -ne 0 ]]; then
            failure="agent: $agent exited with $agent_rc"
            echo "eval-run: $failure" >&2
        else
            git -C "$app" add -A >/dev/null 2>&1
            git -C "$app" diff --cached --stat 2>/dev/null | tail -n 1
            # The lock passes on to the check, which would otherwise wait for it.
            SKILJ_EVAL_LOCK_FD="$eval_lock_fd" "$here/check.sh" "$task" "$app" --run \
                --result "$run_dir/check.json" --logs "$run_dir"
        fi
        duration=$(($(date +%s) - start))
    fi
fi
[[ -n "$started_at" ]] || started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

record="$results/$run_id.json"
RUN_ID="$run_id" AGENT="$agent" AGENT_VERSION="$agent_version" MODEL="$model" COMMIT="$skilj_commit" \
    DIRTY="$dirty" GUIDES="$guides_hash" TASK="$task" LINKED="$link" STARTED="$started_at" \
    DURATION="$duration" LOCK_WAIT="$lock_wait" FAILURE="$failure" node -e '
const e = process.env;
process.stdout.write(JSON.stringify({
  runId: e.RUN_ID, agent: e.AGENT, agentVersion: e.AGENT_VERSION || null, model: e.MODEL || null,
  skiljCommit: e.COMMIT, dirty: e.DIRTY === "true", guidesHash: e.GUIDES, task: e.TASK,
  linked: e.LINKED === "true", startedAt: e.STARTED, durationSeconds: Number(e.DURATION),
  lockWaitSeconds: Number(e.LOCK_WAIT), failure: e.FAILURE || null,
}));' > "$run_dir/meta.json"
node "$here/record.mjs" --out "$record" --meta "$run_dir/meta.json" --check "$run_dir/check.json" \
    --agent-log "$log" || { echo "eval-run: writing the record failed" >&2; exit 1; }
echo "eval-run: record $record"
node -e 'process.exit(JSON.parse(require("fs").readFileSync(process.argv[1], "utf8")).passed ? 0 : 1)' "$record"
