#!/usr/bin/env bash
# Checks a change against the architecture rules in .config/architecture.toml
# with Atlas (https://github.com/flowerornament/atlas): the crate graph,
# file sizes, module names, public surface and module-header citations.
#
#   scripts/check-architecture.sh [BASELINE]
#
# BASELINE is the commit the change is judged against (default HEAD~1; on a
# pull request, the merge commit's first parent, the base branch). `atlas
# delta` reads the policy from both commits and refuses a change that breaks
# a rule or makes a ratchet worse. While the baseline has no policy yet (the
# change that adds it), there is nothing to compare, so it checks the policy
# against the code instead.
#
# Needs `cargo atlas` on PATH:
#   cargo install --locked --git https://github.com/flowerornament/atlas.git --rev <rev> cargo-atlas
# and a clean tree: delta refuses uncommitted changes to tracked files.
set -euo pipefail

POLICY=.config/architecture.toml
# What counts as a citation in a module's //! header.
CITATIONS='docs/,§'

cd "$(dirname "$0")/.."
baseline="${1:-HEAD~1}"

if ! git cat-file -e "$baseline:$POLICY" 2>/dev/null; then
  echo "check-architecture: $baseline has no $POLICY; checking the policy alone"
  exec cargo atlas policy --policy "$POLICY"
fi
exec cargo atlas delta "$baseline" --policy "$POLICY" --citation-markers "$CITATIONS"
