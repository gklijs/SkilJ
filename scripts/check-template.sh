#!/usr/bin/env bash
# Generates templates/skilj-template the way cargo-generate would (its two
# built-in placeholders filled in), builds it, and runs the README's
# quickstart against it - against this workspace's own crates by default,
# so a change that breaks the template fails here, before a release rather
# than for the first user who generates it (docs/architecture.md §105).
# With --published it builds against the versions the template's
# Cargo.toml names on crates.io instead: the check to run after publishing.
#
# The run is the quickstart as a reader does it: the server started with
# no DATABASE_URL, so it starts an embedded Postgres of its own (downloaded
# on first use), then the curl commands it prints - a deposit accepted, a
# larger withdrawal rejected - and a clean shutdown on Ctrl-C. Its data
# directory is a temporary one, never the user's own. --no-run skips the
# run, for a machine that can't start the embedded Postgres.
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
server_pid=
cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill -INT "$server_pid" 2>/dev/null || true
        sleep 5
        kill -KILL "$server_pid" 2>/dev/null || true
    fi
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

cargo check --all-targets
# What the Dockerfile builds: without the embedded-Postgres `local` feature.
cargo check --all-targets --no-default-features

if ! $run; then
    echo "check-template: OK (not run)"
    exit 0
fi

cargo build --bin server
port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
log="$work/server.log"
env -u DATABASE_URL PORT="$port" SKILJ_LOCAL_DATA_DIR="$work/postgres" \
    "$CARGO_TARGET_DIR/debug/server" >"$log" 2>&1 &
server_pid=$!

fail() {
    echo "check-template: $*" >&2
    echo "--- server output ---" >&2
    cat "$log" >&2
    exit 1
}

# Long enough for the first run's Postgres download and initdb.
for _ in $(seq 1 300); do
    # shellcheck disable=SC2016 # Match the literal variable in printed commands.
    grep -q '^  curl .*\$WITHDRAW' "$log" && break
    kill -0 "$server_pid" 2>/dev/null || fail "the server exited before it was ready"
    sleep 1
done
# shellcheck disable=SC2016 # Match the literal variable in printed commands.
grep -q '^  curl .*\$WITHDRAW' "$log" || fail "the server didn't print its try-it commands within 300s"

# The commands exactly as the server prints them, as a reader pastes them.
eval "$(grep -o 'export DEPOSIT=.*' "$log")"
mapfile -t commands < <(grep '^  curl ' "$log")
[[ ${#commands[@]} -eq 2 ]] || fail "expected two printed curl commands, got ${#commands[@]}"
deposit="$(eval "${commands[0]} -sS")"
withdraw="$(eval "${commands[1]} -sS")"
[[ "$deposit" == *'"accepted":true'* ]] || fail "the deposit wasn't accepted: $deposit"
[[ "$withdraw" == *'"rejectionKind":"insufficient_funds"'* ]] ||
    fail "the withdrawal of more than the balance wasn't rejected as insufficient_funds: $withdraw"

kill -INT "$server_pid"
status=0
for _ in $(seq 1 30); do
    kill -0 "$server_pid" 2>/dev/null || break
    sleep 1
done
kill -0 "$server_pid" 2>/dev/null && fail "the server didn't stop within 30s of Ctrl-C"
wait "$server_pid" || status=$?
server_pid=
[[ $status -eq 0 ]] || fail "the server exited with status $status on Ctrl-C"

echo "check-template: OK (quickstart ran: deposit accepted, overdraft rejected)"
