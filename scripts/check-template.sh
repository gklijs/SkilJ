#!/usr/bin/env bash
# Generates templates/skilj-template the way cargo-generate would (its two
# built-in placeholders filled in) and builds it - against this workspace's
# own crates by default, so a change that breaks the template fails here,
# before a release rather than for the first user who generates it
# (docs/architecture.md §105). With --published it builds against the
# versions the template's Cargo.toml names on crates.io instead: the check
# to run after publishing.
#
# Usage: scripts/check-template.sh [--published]
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cp -r "$root/templates/skilj-template" "$work/app"
cd "$work/app"
grep -rl '{{' . | xargs sed -i 's/{{project-name}}/template-check/g; s/{{crate_name}}/template_check/g'
if grep -rn '{{' . ; then
    echo "check-template: unfilled placeholders left (see above)" >&2
    exit 1
fi

if [[ "${1:-}" != "--published" ]]; then
    cat >> Cargo.toml <<EOF

[patch.crates-io]
skilj = { path = "$root/skilj" }
skilj-core = { path = "$root/skilj-core" }
EOF
fi

# The template isn't a workspace member; keep its build out of the
# workspace's own target directory.
CARGO_TARGET_DIR="$work/target" cargo check --all-targets
echo "check-template: OK"
