#!/usr/bin/env bash
# Records the reference solution of an evaluation task (docs/agent-guidance.md):
# diffs a project in which the task is done against a freshly generated one
# and writes solutions/<task>.patch.
#
# Only what a solution owns is diffed: src/, tests/, build.rs and
# Cargo.toml, new files included. Never key files (*.keys.json), the
# guides' skill links (.claude/, .agents/), the task's fixtures, Cargo.lock
# or target/. check-solutions.sh then shows the patch applies to a fresh
# project and passes.
#
# The pristine project is generated into target/agent-eval-record (on disk),
# with the task's fixtures, the way check.sh --solution starts from it.
#
# Usage: scripts/agent-guides/eval/record-solution.sh <task> <project-dir>
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"

task="${1:-}"
project="${2:-}"
[[ -n "$task" && -f "$here/tasks/$task.md" ]] || {
    echo 'record-solution: usage: record-solution.sh <task> <project-dir>' >&2
    exit 2
}
[[ -f "$project/Cargo.toml" ]] || { echo "record-solution: $project is not a project" >&2; exit 2; }
project="$(cd "$project" && pwd)"

cache="$root/target/agent-eval-record"
"$root/scripts/check-template.sh" --prepare-only --cache-dir "$cache" >/dev/null
pristine="$cache/app"
if [[ -d "$here/fixtures/$task" ]]; then cp -a "$here/fixtures/$task/." "$pristine/"; fi

out="$here/solutions/$task.patch"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
for path in Cargo.toml build.rs src tests; do
    [[ -e "$pristine/$path" || -e "$project/$path" ]] || continue
    # diff exits 1 when the two differ.
    (cd "$cache" && diff -ruN -x '*.keys.json' -x '*.blind-index.key' "app/$path" "$project/$path") >>"$tmp" || [[ $? -eq 1 ]]
done
# Paths as patch -p1 takes them from the project's root, without dates.
sed -E \
    -e "s#^diff -ruN -x [^ ]+ -x [^ ]+ app/([^ ]+) .*#diff -ruN a/\\1 b/\\1#" \
    -e "s#^--- app/([^\t]+)\t.*#--- a/\\1#" \
    -e "s#^\\+\\+\\+ $project/([^\t]+)\t.*#+++ b/\\1#" \
    "$tmp" > "$out"
if grep -qE "$project|$pristine" "$out"; then
    echo "record-solution: $out still names a local path" >&2
    exit 1
fi
echo "record-solution: wrote $out ($(grep -c '^+++ ' "$out") files)"
