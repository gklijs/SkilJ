#!/usr/bin/env bash
# Generates templates/skilj-template the way cargo-generate would (its two
# built-in placeholders filled in), builds it, and runs the README's
# quickstart against it - against this workspace's own crates by default,
# so a change that breaks the template fails here, before a release rather
# than for the first user who generates it (docs/architecture.md §105).
# With --published it builds against the versions the template's
# Cargo.toml names on crates.io instead: the check to run after publishing.
#
# The run is the quickstart as a reader does it (scripts/template-quickstart.sh):
# the server started with no DATABASE_URL, so it starts an embedded Postgres
# of its own (downloaded on first use), then the curl commands it prints - a
# deposit accepted, a larger withdrawal rejected - and a clean shutdown on
# Ctrl-C. Its data directory is a temporary one, never the user's own.
# --no-run skips the run, for a machine that can't start the embedded Postgres.
#
# Usage: scripts/check-template.sh [--published] [--no-run]
#        [--cache-dir DIR [--prepare-only | --prepared]]
set -euo pipefail

published=false
run=true
prepare_only=false
prepared=false
cache_dir=
while [[ $# -gt 0 ]]; do
    case "$1" in
        --published) published=true ;;
        --no-run) run=false ;;
        --prepare-only) prepare_only=true ;;
        --prepared) prepared=true ;;
        --cache-dir)
            [[ $# -ge 2 && -n "$2" ]] || { echo 'check-template: --cache-dir needs a directory' >&2; exit 2; }
            cache_dir="$2"; shift ;;
        *) echo "check-template: unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done
if { $prepare_only || $prepared; } && [[ -z "$cache_dir" ]]; then
    echo 'check-template: --prepare-only/--prepared requires --cache-dir' >&2
    exit 2
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d)"
cleanup() {
    rm -rf "$work"
}
trap cleanup EXIT

# CI uses a stable generated-project path and target directory so Cargo's
# fingerprints survive between runs. Logs and Postgres data stay temporary.
if [[ -n "$cache_dir" ]]; then
    mkdir -p "$cache_dir"
    cache_dir="$(cd "$cache_dir" && pwd)"
    app="$cache_dir/app"
    export CARGO_TARGET_DIR="$cache_dir/build"
else
    app="$work/app"
    export CARGO_TARGET_DIR="$work/target"
fi

if ! $prepared; then
    # Only the generated app is replaced; cached build artifacts are its sibling.
    rm -rf "$app"
    cp -r "$root/templates/skilj-template" "$app"
    cd "$app"
    grep -rl '{{' . | xargs sed -i 's/{{project-name}}/template-check/g; s/{{crate_name}}/template_check/g'
    if grep -rn '{{' . ; then
        echo "check-template: unfilled placeholders left (see above)" >&2
        exit 1
    fi
    # The persistent generated app may live below the main workspace's target/.
    # Declare a standalone workspace so Cargo does not adopt its ancestor.
    printf '\n[workspace]\n' >> Cargo.toml
    if ! $published; then
        cat >> Cargo.toml <<EOF

[patch.crates-io]
skilj = { path = "$root/skilj" }
skilj-core = { path = "$root/skilj-core" }
skilj-test-fixture = { path = "$root/skilj-test-fixture" }
# What the agent guides' evaluation tasks add (scripts/agent-guides/eval/):
# patched too, so they build against this workspace. Unused, they only warn.
skilj-codegen = { path = "$root/skilj-codegen" }
skilj-temporal = { path = "$root/skilj-temporal" }
skilj-bridge = { path = "$root/skilj-bridge" }
skilj-retry = { path = "$root/skilj-retry" }
EOF
    fi
else
    [[ -f "$app/Cargo.toml" ]] || { echo 'check-template: prepare the cached app first' >&2; exit 2; }
    cd "$app"
fi
if $prepare_only; then
    echo "check-template: prepared $app"
    exit 0
fi

# Domain tests also run with --no-run: only the server smoke test is skipped.
# Disable local mode and remove DATABASE_URL to exercise the database-free loop.
env -u DATABASE_URL cargo test --no-default-features --test wallet_fixture

cargo check --all-targets
# What the Dockerfile builds: without the embedded-Postgres `local` feature.
cargo check --all-targets --no-default-features

if ! $run; then
    echo "check-template: OK (not run)"
    exit 0
fi

cargo build --bin server
"$root/scripts/template-quickstart.sh" "$work/quickstart" "$CARGO_TARGET_DIR/debug/server"

echo "check-template: OK (quickstart ran: deposit accepted, overdraft rejected)"
