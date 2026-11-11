#!/usr/bin/env bash
# Gives one evaluation task of the agent guides to a coding agent, in a
# project freshly generated from the template, and checks what it made
# (docs/agent-guidance.md). The agent gets the task's text and nothing
# else: what it knows about skilj is what the project's AGENTS.md and the
# guides tell it.
#
# The project builds against this workspace's crates, so the guides it
# reads are this workspace's too. It lives outside the repository, where no
# AGENTS.md or skill of skilj's own is in reach of the agent.
#
# Usage: scripts/agent-guides/eval/run.sh <claude|codex> <task> [--link]
#        <task>: add-command | evolve-event | sensitive-field
#        --link: run scripts/skilj-guides.sh --link first, so the guides
#                are skills the agent loads by itself
#        SKILJ_EVAL_DIR: where the projects go (~/.cache/skilj-agent-eval)
#
# Needs the agent's CLI installed and signed in. It costs what a session of
# that agent costs, so nothing runs it automatically.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"

agent="${1:-}"
task="${2:-}"
link=false
[[ "${3:-}" == "--link" ]] && link=true
[[ "$agent" == claude || "$agent" == codex ]] && [[ -f "$here/tasks/$task.md" ]] || {
    echo 'eval-run: usage: run.sh <claude|codex> <add-command|evolve-event|sensitive-field> [--link]' >&2
    exit 2
}

dir="${SKILJ_EVAL_DIR:-$HOME/.cache/skilj-agent-eval}"
name="$agent-$task"
if $link; then name+="-linked"; fi
"$root/scripts/check-template.sh" --prepare-only --cache-dir "$dir/$name"
app="$dir/$name/app"
log="$dir/$name/agent.log"
# One build directory for every run: only the project itself is rebuilt.
export CARGO_TARGET_DIR="$dir/build"
mkdir -p "$CARGO_TARGET_DIR"

cd "$app"
# Its own repository: an agent looks for instructions up to the root of one.
# Staged and not committed, so `git diff` is what the agent changed.
git init -q
git add -A
guides="$(bash scripts/skilj-guides.sh)"
if $link; then bash scripts/skilj-guides.sh --link >/dev/null; fi

echo "eval-run: $agent on $task in $app (log: $log)"
case "$agent" in
    claude)
        claude --print --permission-mode acceptEdits --add-dir "$guides" --add-dir "$CARGO_TARGET_DIR" \
            --allowedTools 'Bash(cargo:*)' 'Bash(bash scripts/skilj-guides.sh:*)' 'Bash(git diff:*)' \
            'Bash(git status:*)' 'Bash(ls:*)' 'Bash(cat:*)' 'Bash(head:*)' 'Bash(tail:*)' 'Bash(grep:*)' \
            Read Edit Write Glob Grep Skill \
            < "$here/tasks/$task.md" > "$log" 2>&1 || echo "eval-run: claude exited with $?" >&2
        ;;
    codex)
        codex exec --sandbox workspace-write --add-dir "$CARGO_TARGET_DIR" - \
            < "$here/tasks/$task.md" > "$log" 2>&1 || echo "eval-run: codex exited with $?" >&2
        ;;
esac

git diff --stat
"$here/check.sh" "$task" "$app" --run
