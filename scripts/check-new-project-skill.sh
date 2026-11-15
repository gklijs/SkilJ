#!/usr/bin/env bash
# Runs what the plugin's skilj-new-project skill tells an agent to run, so
# the skill can't drift from the template (docs/architecture.md §252).
#
# The skill's commands are its ```sh blocks that start with `# step: NAME`:
#
# - generate: must be the README's `cargo generate` line for
#   templates/skilj-template, with --name. It isn't run here: cargo-generate
#   isn't installed in CI, and scripts/check-template.sh already generates
#   the project the way cargo-generate does (its two placeholders filled).
# - guides, test: run as written in that generated project. The guides step
#   must leave a link to each of the six guides in .claude/skills and
#   .agents/skills.
# - run: started as written by scripts/template-quickstart.sh, which runs the
#   printed commands; --no-run skips it.
#
# It also checks that the two marketplaces and the two plugin manifests agree
# with each other and with the workspace version.
#
# Usage: scripts/check-new-project-skill.sh [--no-run] [--cache-dir DIR]
#   --cache-dir DIR  use the project scripts/check-template.sh --cache-dir DIR
#                    prepared (and its build directory), instead of
#                    preparing one in a temporary directory
set -euo pipefail

run=true
cache_dir=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-run) run=false ;;
        --cache-dir)
            [[ $# -ge 2 && -n "$2" ]] || { echo 'check-new-project-skill: --cache-dir needs a directory' >&2; exit 2; }
            cache_dir="$2"; shift ;;
        *) echo "check-new-project-skill: unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done

root="$(cd "$(dirname "$0")/.." && pwd)"
skill="$root/plugins/skilj/skills/skilj-new-project/SKILL.md"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fail() {
    echo "check-new-project-skill: $*" >&2
    exit 1
}

# The lines of the ```sh block that starts with `# step: $1`, without that line.
step() {
    awk -v name="$1" '
        /^```/ { if (inside) { inside = 0; found = 0 } else if ($0 == "```sh") { inside = 1; first = 1 }; next }
        inside && first { first = 0; if ($0 == "# step: " name) { found = 1; seen = 1 }; next }
        inside && found { print }
        END { if (!seen) exit 1 }
    ' "$skill" || fail "SKILL.md has no \`# step: $1\` block"
}

# --- Manifests ---------------------------------------------------------------
version="$(sed -n 's/^version = "\(.*\)"/\1/p' "$root/Cargo.toml" | head -n 1)"
python3 - "$root" "$version" <<'PY' || fail "the marketplaces or plugin manifests don't agree (above)"
import json, os, sys
root, version = sys.argv[1], sys.argv[2]
def load(path):
    with open(os.path.join(root, path)) as f:
        return json.load(f)
problems = []
claude = load(".claude-plugin/marketplace.json")
codex = load(".agents/plugins/marketplace.json")
claude_plugin = load("plugins/skilj/.claude-plugin/plugin.json")
codex_plugin = load("plugins/skilj/plugin.json")
if claude["name"] != codex["name"]:
    problems.append("the two marketplaces have different names")
for entry, path in [(e, e["source"]) for e in claude["plugins"]] + \
                   [(e, e["source"]["path"]) for e in codex["plugins"]]:
    if not path.startswith("./") or ".." in path:
        problems.append(f"{entry['name']}: source {path} isn't a ./ path inside the repository")
    elif not os.path.isdir(os.path.join(root, path)):
        problems.append(f"{entry['name']}: source {path} doesn't exist")
    for key in ("installation", "authentication"):
        if "policy" in entry and key not in entry["policy"]:
            problems.append(f"{entry['name']}: policy has no {key}")
names = {e["name"] for e in claude["plugins"]} | {e["name"] for e in codex["plugins"]}
if names != {claude_plugin["name"]} or codex_plugin["name"] != claude_plugin["name"]:
    problems.append(f"entry names {sorted(names)} and manifest names differ")
for manifest, label in [(claude_plugin, "Claude Code"), (codex_plugin, "Codex")]:
    if manifest.get("version") != version:
        problems.append(f"the {label} manifest's version {manifest.get('version')} isn't the workspace's {version}")
if claude_plugin["description"] != codex_plugin["description"]:
    problems.append("the two manifests have different descriptions")
for p in problems:
    print(f"  - {p}", file=sys.stderr)
sys.exit(1 if problems else 0)
PY

# --- generate: the README's line --------------------------------------------
generate="$(step generate)"
readme_line="$(grep -o 'cargo generate --git https://github.com/gklijs/SkilJ.git templates/skilj-template' "$root/README.md" | head -n 1)"
[[ -n "$readme_line" ]] || fail "README.md has no cargo generate line for templates/skilj-template"
[[ "$generate" == "$readme_line --name \"\$NAME\"" ]] ||
    fail "the generate step isn't README.md's line with --name \"\$NAME\": $generate"
[[ -f "$root/templates/skilj-template/cargo-generate.toml" ]] || fail "templates/skilj-template is gone"

# --- The generated project --------------------------------------------------
if [[ -n "$cache_dir" ]]; then
    [[ -f "$cache_dir/app/Cargo.toml" ]] || fail "prepare $cache_dir with scripts/check-template.sh --prepare-only first"
    cache_dir="$(cd "$cache_dir" && pwd)"
    export CARGO_TARGET_DIR="$cache_dir/build"
    app="$work/app"
    cp -r "$cache_dir/app" "$app"
else
    "$root/scripts/check-template.sh" --prepare-only --cache-dir "$work/cache"
    export CARGO_TARGET_DIR="$work/cache/build"
    app="$work/cache/app"
fi
cd "$app"
rm -rf .claude/skills .agents/skills

# --- guides: as written, apart from the `cd` into the project --------------
NAME="$(basename "$app")"
export NAME
(cd .. && eval "$(step guides)") >"$work/guides.out" || { cat "$work/guides.out" >&2; fail "the guides step failed"; }
grep -q '/agent-guides$' "$work/guides.out" || fail "the guides step didn't print the guides directory"
for skills in .claude/skills .agents/skills; do
    for guide in skilj skilj-bootstrap skilj-event-modeling skilj-large-payloads skilj-temporal skilj-uniqueness; do
        [[ -L "$skills/$guide" && -f "$skills/$guide/SKILL.md" ]] || fail "the guides step left no link $skills/$guide"
    done
done

# --- test: as written -------------------------------------------------------
eval "$(step test)"

# --- run: as written, with the printed commands ----------------------------
if ! $run; then
    echo "check-new-project-skill: OK (not run)"
    exit 0
fi
read -r -a server <<<"$(step run)"
"$root/scripts/template-quickstart.sh" "$work/quickstart" "${server[@]}"
echo "check-new-project-skill: OK (generate is the README's, guides linked, tests passed, quickstart ran)"
