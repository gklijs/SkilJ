#!/usr/bin/env bash
# What a project generated from the template gives a coding agent, checked
# against this workspace's crates (docs/architecture.md §248):
#
#   - scripts/skilj-guides.sh finds the guides the skilj crate ships, and
#     links them where Claude Code and Codex load skills from
#   - the reference solution of each evaluation task still applies to the
#     template, builds, passes its acceptance tests and, unless --no-run,
#     answers over REST as its task says (check.sh)
#
# So a change to a trait, to registration or to the template that breaks
# what the guides teach fails here.
#
# It generates the project the way scripts/check-template.sh does, and
# shares its --cache-dir: after that script, in CI, the project is there
# and built. With --published it builds against crates.io instead: the
# check to run after publishing.
#
# Usage: scripts/agent-guides/eval/check-solutions.sh [--no-run] [--published] [--cache-dir DIR]
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"

run=true
prepare=(--prepare-only)
cache_dir=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-run) run=false ;;
        --published) prepare+=(--published) ;;
        --cache-dir)
            [[ $# -ge 2 && -n "$2" ]] || { echo 'check-solutions: --cache-dir needs a directory' >&2; exit 2; }
            cache_dir="$2"; shift ;;
        *) echo "check-solutions: unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done

work=
if [[ -z "$cache_dir" ]]; then
    work="$(mktemp -d)"
    trap 'rm -rf "$work"' EXIT
    cache_dir="$work"
fi
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
for task in add-command evolve-event sensitive-field; do
    "$here/check.sh" "$task" "$app" "${args[@]}"
done
echo "check-solutions: OK"
