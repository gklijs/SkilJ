#!/usr/bin/env bash
# Builds templates/skilj-template's Dockerfile the way a user would, and
# checks what comes out: a static binary in a `FROM scratch` image that
# starts (GitHub issue #123). scripts/check-template.sh builds and runs the
# template with cargo; this is the one that builds the image, so a change
# that breaks the Dockerfile - a build dependency a crate gained, an
# architecture assumption - fails here instead of for the first user.
#
# The generated project is built against this workspace's own crates, as
# check-template.sh does: they are copied into the build context and
# patched in. Pass --published to build against crates.io instead, for
# the check after a release. Needs a Docker daemon.
#
# Usage: scripts/check-template-docker.sh [--published]
#        [--cache-from SPEC] [--cache-to SPEC]
set -euo pipefail

published=false
cache_args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --published) published=true ;;
        --cache-from|--cache-to)
            [[ $# -ge 2 && -n "$2" ]] || { echo "check-template-docker: $1 needs a cache specification" >&2; exit 2; }
            cache_args+=("$1" "$2"); shift ;;
        *) echo "check-template-docker: unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
image="skilj-template-check:$$"
cleanup() {
    docker rmi -f "$image" >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT

cp -r "$root/templates/skilj-template" "$work/app"
cd "$work/app"
grep -rl '{{' . | xargs sed -i 's/{{project-name}}/template-check/g; s/{{crate_name}}/template_check/g'
if grep -rn '{{' . ; then
    echo "check-template-docker: unfilled placeholders left (see above)" >&2
    exit 1
fi

if ! $published; then
    # The Dockerfile's `COPY . .` carries the workspace into the build.
    mkdir skilj-workspace
    (cd "$root" && git ls-files -z | tar --null -T - -cf -) | tar -xf - -C skilj-workspace
    # cargo-chef records workspace members, not crates reached only through
    # [patch]. Make the generated app and the copied libraries one workspace
    # for this check, retaining the library manifests' inherited settings.
    # The published-template path needs no such adaptation.
    python3 - <<'PYTHON'
from pathlib import Path
import re
manifest = Path("skilj-workspace/Cargo.toml")
workspace = manifest.read_text()
start = workspace.index("members = [")
end = workspace.index("]", start) + 1
# Cargo automatically adds their local path dependencies as members. Avoid
# pulling unrelated demo/broker packages into dependency resolution.
members = 'members = ["skilj-workspace/skilj", "skilj-workspace/skilj-core"]'
workspace = workspace[:start] + members + workspace[end:]
workspace = re.sub(r'path = "([^"\n]+)"', r'path = "skilj-workspace/\1"', workspace)
app = Path("Cargo.toml")
# Direct path dependencies let cargo-chef mask local package versions along
# with their consumers; registry patches alone retain incompatible versions.
contents = re.sub(r'(?m)^(skilj(?:-core)?) = "[^"\n]+"$',
                  r'\1 = { path = "skilj-workspace/\1" }', app.read_text())
app.write_text(contents + "\n" + workspace)
manifest.unlink()
PYTHON
fi

if [[ ${#cache_args[@]} -gt 0 ]]; then
    # --load keeps the existing smoke test exercising the locally built image.
    docker buildx build --load "${cache_args[@]}" --tag "$image" .
else
    docker build --tag "$image" .
fi

# scratch has no shell, so the binary can't be inspected from inside the
# image; the builder stage's `ldd` is the static-linking check, and
# starting the server proves the copied binary runs without a libc.
# With no DATABASE_URL and no `local` feature it must refuse and say why,
# not fail to exec.
set +e
output="$(docker run --rm "$image" 2>&1)"
status=$?
set -e
if [[ "$output" == *"exec format error"* || "$output" == *"no such file"* || "$output" == *"not found"* ]]; then
    echo "check-template-docker: the image's binary can't execute:" >&2
    echo "$output" >&2
    exit 1
fi
if [[ $status -eq 0 ]]; then
    echo "check-template-docker: the server exited 0 with no DATABASE_URL" >&2
    exit 1
fi
echo "check-template-docker: OK (image built; the server started and refused to run without DATABASE_URL: ${output##*$'\n'})"
