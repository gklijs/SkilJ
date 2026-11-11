#!/usr/bin/env bash
# Finds skilj's guides for coding agents: they ship inside the skilj crate,
# so these are the ones written for the version this project builds
# against. AGENTS.md says when to read which.
#
# Usage: bash scripts/skilj-guides.sh          print the directory
#        bash scripts/skilj-guides.sh --link   also link each guide as a
#                                              skill, for Claude Code
#                                              (.claude/skills) and Codex
#                                              (.agents/skills)
#
# The links name that version's directory: run --link again after changing
# the skilj version. They are replaced; a skill of your own is left alone.
set -euo pipefail

cd "$(dirname "$0")/.."

# The skilj package's manifest: skilj-<version>/Cargo.toml from crates.io,
# skilj/Cargo.toml from a path or git dependency.
manifest="$(cargo metadata --format-version 1 |
    grep -oE '"manifest_path":"[^"]*/skilj(-[0-9][^/"]*)?/Cargo\.toml"' | head -n 1 |
    sed -E 's/^"manifest_path":"//; s/"$//')"
guides="${manifest%/Cargo.toml}/agent-guides"
if [[ -z "$manifest" || ! -f "$guides/README.md" ]]; then
    echo "skilj-guides: this project's skilj ships no agent-guides directory; AGENTS.md says where to read them instead" >&2
    exit 1
fi

if [[ "${1:-}" == "--link" ]]; then
    for skills in .claude/skills .agents/skills; do
        mkdir -p "$skills"
        for guide in "$guides"/*/; do
            link="$skills/$(basename "$guide")"
            if [[ -e "$link" && ! -L "$link" ]]; then
                echo "skilj-guides: left $link alone, it is not a link" >&2
                continue
            fi
            ln -sfn "${guide%/}" "$link"
        done
    done
    echo "skilj-guides: linked into .claude/skills and .agents/skills" >&2
elif [[ $# -gt 0 ]]; then
    echo "skilj-guides: unknown argument $1" >&2
    exit 2
fi

echo "$guides"
