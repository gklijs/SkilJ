#!/usr/bin/env bash
# Validates every "§N" style citation of docs/architecture.md's top-level
# sections against the headings that actually exist there today.
#
# docs/architecture.md's "## N. Title" sections have been renumbered before
# (a deletion shifted every later section down by one, silently breaking
# ~600 "§NN" citations scattered across the repo). This script exists so a
# future renumbering fails CI instead of leaving stale citations behind.
#
# What it checks, for every "§..." citation found anywhere in the repo
# (docs, README/CHANGELOG/CONTRIBUTING, .claude skills, Rust doc comments,
# etc.):
#   - bare top-level refs, e.g. "§41", "§10b"
#   - slash-separated lists of the above, e.g. "§39/§40"
#   - the top-level number in a dotted subsection ref, e.g. "§1.3.1" -> "1"
#   - the top-level number in a free-text "item N" ref, e.g.
#     "§8 item 4" -> "8"
# In every case the check is the same: does that top-level number still
# name a "## N. Title" heading in docs/architecture.md right now? Dotted
# subsections and "item N" text aren't machine-checkable headings, so this
# script doesn't try to validate those parts - only the base number.
#
# One known false positive is explicitly skipped: "(§0/spec" in
# docs/architecture.md refers to a section of the *Allium spec*
# (specs/skilj.allium), not to this document, and there is no "## 0."
# heading here to check it against.
#
# Usage: scripts/check-section-refs.sh
# Exit status: 0 if every citation resolves, non-zero otherwise (with a
# file:line report of everything that didn't).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARCH_DOC="$REPO_ROOT/docs/architecture.md"

if [[ ! -f "$ARCH_DOC" ]]; then
  echo "error: $ARCH_DOC not found" >&2
  exit 1
fi

# Current top-level section numbers, e.g. 1 2 3 ... 10 10b ... 42.
mapfile -t HEADING_NUMS < <(grep -oP '^## \K[0-9]+[a-z]?(?=\. )' "$ARCH_DOC" | sort -u)

if [[ ${#HEADING_NUMS[@]} -eq 0 ]]; then
  echo "error: found no '## N. Title' headings in $ARCH_DOC - regex or file broken?" >&2
  exit 1
fi

is_valid_heading() {
  local n="$1"
  local h
  for h in "${HEADING_NUMS[@]}"; do
    [[ "$h" == "$n" ]] && return 0
  done
  return 1
}

# Lines in docs/architecture.md that carry the one known, allowed false
# positive: a reference to the Allium spec's own "§0", not a section here.
declare -A ALLOWED_FALSE_POSITIVE=()
while IFS=: read -r fline; do
  ALLOWED_FALSE_POSITIVE["$ARCH_DOC:$fline"]=1
done < <(grep -noP '(?=.*§0/spec).*' "$ARCH_DOC" | cut -d: -f1)

problems=()

# Scan only git-tracked files (this naturally skips target/, .idea/,
# .git/, and anything else .gitignore already excludes), and skip this
# script's own file - its comments above use "§NN" literally to document
# the pattern being checked, not as citations to validate.
SELF_REL="scripts/check-section-refs.sh"

while IFS=: read -r rel lineno match; do
  [[ -z "${match:-}" ]] && continue
  [[ "$rel" == "$SELF_REL" ]] && continue

  if [[ "$match" == "§0" && "$rel" == "docs/architecture.md" && -n "${ALLOWED_FALSE_POSITIVE["$ARCH_DOC:$lineno"]:-}" ]]; then
    continue
  fi

  base="${match#§}"
  if ! is_valid_heading "$base"; then
    problems+=("$rel:$lineno: stale reference '$match' - no '## $base. ...' heading in docs/architecture.md")
  fi
done < <(cd "$REPO_ROOT" && git ls-files -z \
  | xargs -0 grep -HnoP '§[0-9]+[a-z]?(\.[0-9]+)*' --binary-files=without-match \
  | sed -E 's/^(.*):([0-9]+):(§[0-9]+[a-z]?)(\.[0-9]+)*$/\1:\2:\3/')

if [[ ${#problems[@]} -gt 0 ]]; then
  echo "check-section-refs: found ${#problems[@]} stale section reference(s):" >&2
  echo >&2
  for p in "${problems[@]}"; do
    echo "  $p" >&2
  done
  echo >&2
  echo "Current docs/architecture.md top-level sections:" >&2
  printf '  %s\n' "${HEADING_NUMS[@]}" >&2
  exit 1
fi

echo "check-section-refs: OK - all §-references resolve to a current docs/architecture.md heading."
