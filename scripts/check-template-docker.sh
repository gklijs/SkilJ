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
set -euo pipefail

published=false
for arg in "$@"; do
    case "$arg" in
        --published) published=true ;;
        *) echo "check-template-docker: unknown argument $arg" >&2; exit 2 ;;
    esac
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
    cat >> Cargo.toml <<EOF2

[patch.crates-io]
skilj = { path = "skilj-workspace/skilj" }
skilj-core = { path = "skilj-workspace/skilj-core" }
EOF2
fi

docker build --tag "$image" .

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
