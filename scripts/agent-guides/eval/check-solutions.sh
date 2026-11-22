#!/usr/bin/env bash
# What a project generated from the template gives a coding agent, checked
# against this workspace's crates (docs/architecture.md §248):
#
#   - scripts/skilj-guides.sh finds the guides the skilj crate ships, and
#     links them where Claude Code and Codex load skills from
#   - the reference solution of each evaluation task (every one in tasks/)
#     still applies to the template, builds, passes its acceptance tests
#     and, unless --no-run, answers over REST as its task says (check.sh;
#     temporal-activity runs Temporal in Docker)
#
# So a change to a trait, to registration or to the template that breaks
# what the guides teach fails here.
#
# It generates the project the way scripts/check-template.sh does, and
# shares its --cache-dir: after that script, in CI, the project is there
# and built. With --published it builds against crates.io instead: the
# check to run after publishing. A release without
# skilj::dev::DevAdmin::create_event_read_token can't build
# temporal-activity's solution, so --published skips it, saying so, until
# one has it.
#
# check.sh takes the build directory's lock (lib.sh), so this waits while
# an evaluation run (run.sh) uses the same one.
#
# Usage: scripts/agent-guides/eval/check-solutions.sh [--no-run] [--published] [--cache-dir DIR]
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "$here/lib.sh"
root="$eval_root"

run=true
published=false
prepare=(--prepare-only)
cache_dir=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-run) run=false ;;
        --published) prepare+=(--published); published=true ;;
        --cache-dir)
            [[ $# -ge 2 && -n "$2" ]] || { echo 'check-solutions: --cache-dir needs a directory' >&2; exit 2; }
            cache_dir="$2"; shift ;;
        *) echo "check-solutions: unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done

# On disk: the build is large, and /tmp may be memory.
[[ -n "$cache_dir" ]] || cache_dir="$root/target/agent-eval"
"$root/scripts/check-template.sh" "${prepare[@]}" --cache-dir "$cache_dir"
cache_dir="$(cd "$cache_dir" && pwd)"
app="$cache_dir/app"
export CARGO_TARGET_DIR="$cache_dir/build"

cd "$app"
guides="$(bash scripts/skilj-guides.sh --link)"
for entry in "$guides/README.md" .claude/skills/skilj/SKILL.md .agents/skills/skilj/SKILL.md; do
    [[ -f "$entry" ]] || { echo "check-solutions: scripts/skilj-guides.sh left no $entry" >&2; exit 1; }
done

args=(--solution)
if $run; then args+=(--run); fi
failed=()
for task in $(eval_tasks); do
    if [[ "$task" == temporal-activity ]] && $published; then
        skilj_src="$(cargo metadata --format-version 1 | python3 -c '
import json, sys
packages = json.load(sys.stdin)["packages"]
print(next(p["manifest_path"] for p in packages if p["name"] == "skilj").rsplit("/", 1)[0])')"
        if ! grep -q 'fn create_event_read_token' "$skilj_src/src/dev.rs"; then
            echo "check-solutions: temporal-activity skipped: the published skilj ($skilj_src) has no DevAdmin::create_event_read_token yet"
            continue
        fi
    fi
    "$here/check.sh" "$task" "$app" "${args[@]}" || failed+=("$task")
done
if [[ ${#failed[@]} -gt 0 ]]; then
    echo "check-solutions: FAILED: ${failed[*]}" >&2
    exit 1
fi
echo "check-solutions: OK"
