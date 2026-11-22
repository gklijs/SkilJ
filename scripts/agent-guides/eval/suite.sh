#!/usr/bin/env bash
# Runs every evaluation task for one agent (run.sh), one after another,
# going on past a failed one, and lists the result records written
# (docs/agent-guidance.md). Exits 1 when any task failed.
#
# Then `node scripts/agent-guides/eval/summarize.mjs --write` puts the
# records in docs/agent-guidance.md's scoreboard.
#
# Usage: scripts/agent-guides/eval/suite.sh <claude|codex|none|reference> [--link] [--model MODEL]
#        [--only TASK,TASK...]
#        The environment run.sh reads applies (SKILJ_EVAL_DIR, SKILJ_EVAL_RESULTS, ...).
#
# With claude or codex it costs an agent session per task.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"

usage='eval-suite: usage: suite.sh <claude|codex|none|reference> [--link] [--model MODEL] [--only TASK,TASK...]'
agent="${1:-}"
shift || { echo "$usage" >&2; exit 2; }
args=()
only=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --link) args+=(--link) ;;
        --model)
            [[ $# -ge 2 && -n "$2" ]] || { echo "$usage" >&2; exit 2; }
            args+=(--model "$2"); shift ;;
        --only)
            [[ $# -ge 2 && -n "$2" ]] || { echo "$usage" >&2; exit 2; }
            only="$2"; shift ;;
        *) echo "eval-suite: unknown argument $1" >&2; echo "$usage" >&2; exit 2 ;;
    esac
    shift
done

mapfile -t tasks < <(eval_tasks)
if [[ -n "$only" ]]; then
    IFS=',' read -r -a tasks <<<"$only"
    for task in "${tasks[@]}"; do
        [[ -f "$here/tasks/$task.md" ]] || { echo "eval-suite: no task '$task'" >&2; exit 2; }
    done
fi

failed=()
records=()
for task in "${tasks[@]}"; do
    echo "eval-suite: $agent on $task"
    out="$(mktemp)"
    "$here/run.sh" "$agent" "$task" "${args[@]}" 2>&1 | tee "$out"
    rc="${PIPESTATUS[0]}"
    # A usage error is the suite's: stop rather than repeat it per task.
    [[ "$rc" -eq 2 ]] && { rm -f "$out"; exit 2; }
    record="$(sed -n 's/^eval-run: record //p' "$out" | tail -n 1)"
    rm -f "$out"
    [[ -n "$record" ]] && records+=("$record")
    [[ "$rc" -eq 0 ]] || failed+=("$task")
done

echo "eval-suite: records written:"
printf '  %s\n' "${records[@]}"
if [[ ${#failed[@]} -gt 0 ]]; then
    echo "eval-suite: failed: ${failed[*]}"
    exit 1
fi
echo "eval-suite: every task passed"
