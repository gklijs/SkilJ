#!/usr/bin/env bash
# Runs an older release and this workspace against one database, as separate
# processes, the way an upgrade does it (docs/architecture.md §249,
# docs/upgrade-preflight-design.md "Supported upgrades"):
#
#   1. the older release's generated template starts on an empty database and
#      takes four deposits of 100 into wallet "w1";
#   2. it stops, and this workspace's template starts on the same database:
#      the upgrade. A withdrawal of 350 is accepted - it can only be, if the
#      older release's deposits came through - and one of 500 is rejected as
#      insufficient_funds;
#   3. it stops, and the older release starts again. For a pair not declared
#      compatible (the default), it must refuse the upgraded database; with
#      --expect-compatible it must start and take a deposit;
#   4. this workspace starts once more.
#
# Both templates use local mode's embedded Postgres, with one data directory:
# the older release must pin the same Postgres major as this one
# (skilj::local::postgres_version). The older release is built from its git
# tag's template against the crates it names on crates.io, so the tag has to
# be present locally (CI fetches it).
#
# Usage: scripts/check-upgrade-pair.sh [--from TAG] [--expect-compatible]
#        [--cache-dir DIR [--prepare-only | --prepared]] [--new-cache DIR]
#   --from TAG           the older release (default: $DEFAULT_FROM below; raise
#                        it with each release, see RELEASING.md)
#   --cache-dir DIR      keep the generated apps and their builds in DIR
#   --new-cache DIR      reuse this workspace's template as scripts/check-template.sh
#                        --cache-dir DIR prepared and built it
#   --prepare-only       only generate the older release's app under --cache-dir,
#                        so CI can restore its build cache before building it
#   --prepared           use the app --prepare-only generated
set -euo pipefail

DEFAULT_FROM=v0.4.0

from="$DEFAULT_FROM"
expect_compatible=false
cache_dir=
new_cache=
prepare_only=false
prepared=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --from)
            [[ $# -ge 2 && -n "$2" ]] || { echo 'check-upgrade-pair: --from needs a tag' >&2; exit 2; }
            from="$2"; shift ;;
        --expect-compatible) expect_compatible=true ;;
        --prepare-only) prepare_only=true ;;
        --prepared) prepared=true ;;
        --cache-dir)
            [[ $# -ge 2 && -n "$2" ]] || { echo 'check-upgrade-pair: --cache-dir needs a directory' >&2; exit 2; }
            cache_dir="$2"; shift ;;
        --new-cache)
            [[ $# -ge 2 && -n "$2" ]] || { echo 'check-upgrade-pair: --new-cache needs a directory' >&2; exit 2; }
            new_cache="$2"; shift ;;
        *) echo "check-upgrade-pair: unknown argument $1" >&2; exit 2 ;;
    esac
    shift
done

if { $prepare_only || $prepared; } && [[ -z "$cache_dir" ]]; then
    echo 'check-upgrade-pair: --prepare-only/--prepared requires --cache-dir' >&2
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

if ! $prepared && ! git -C "$root" rev-parse -q --verify "refs/tags/$from" >/dev/null; then
    echo "check-upgrade-pair: tag $from is not present locally; fetch it first:" >&2
    echo "  git fetch --no-tags --depth=1 origin refs/tags/$from:refs/tags/$from" >&2
    exit 2
fi

if [[ -n "$cache_dir" ]]; then
    mkdir -p "$cache_dir"
    cache_dir="$(cd "$cache_dir" && pwd)"
else
    cache_dir="$work/cache"
    mkdir -p "$cache_dir"
fi

# Fills cargo-generate's two built-in placeholders and declares a standalone
# workspace, as scripts/check-template.sh does.
generate() {
    local app="$1"
    (
        cd "$app"
        grep -rl '{{' . | xargs sed -i 's/{{project-name}}/upgrade-pair/g; s/{{crate_name}}/upgrade_pair/g'
        printf '\n[workspace]\n' >> Cargo.toml
    )
}

# The older release: its tag's template, against crates.io.
old_app="$cache_dir/old"
old_target="$cache_dir/old-build"
if ! $prepared; then
    rm -rf "$old_app"
    mkdir -p "$old_app"
    git -C "$root" archive "$from" templates/skilj-template | tar -x -C "$work"
    cp -r "$work/templates/skilj-template/." "$old_app/"
    rm -rf "$work/templates"
    generate "$old_app"
    # Resolve now, so a build cache keyed on the lock file can be restored.
    (cd "$old_app" && cargo generate-lockfile)
elif [[ ! -f "$old_app/Cargo.toml" ]]; then
    echo 'check-upgrade-pair: prepare the older release first (--prepare-only)' >&2
    exit 2
fi
if $prepare_only; then
    echo "check-upgrade-pair: prepared $old_app ($from)"
    exit 0
fi
(cd "$old_app" && CARGO_TARGET_DIR="$old_target" cargo build --bin server)
old_server="$old_target/debug/server"

# This workspace's template.
if [[ -n "$new_cache" ]]; then
    new_server="$(cd "$new_cache" && pwd)/build/debug/server"
    [[ -x "$new_server" ]] || { echo "check-upgrade-pair: no built server under $new_cache" >&2; exit 2; }
else
    new_app="$cache_dir/new"
    new_target="$cache_dir/new-build"
    rm -rf "$new_app"
    cp -r "$root/templates/skilj-template" "$new_app"
    generate "$new_app"
    cat >> "$new_app/Cargo.toml" <<EOF

[patch.crates-io]
skilj = { path = "$root/skilj" }
skilj-core = { path = "$root/skilj-core" }
skilj-test-fixture = { path = "$root/skilj-test-fixture" }
EOF
    (cd "$new_app" && CARGO_TARGET_DIR="$new_target" cargo build --bin server)
    new_server="$new_target/debug/server"
fi

data="$work/postgres"
log=
fail() {
    echo "check-upgrade-pair: $*" >&2
    if [[ -n "$log" && -f "$log" ]]; then
        echo "--- server output ($log) ---" >&2
        cat "$log" >&2
    fi
    exit 1
}

# Starts a server; answers 0 once it printed its try-it commands, 1 when it
# exited first.
start() {
    local server="$1" name="$2"
    log="$work/$name.log"
    port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
    env -u DATABASE_URL PORT="$port" SKILJ_LOCAL_DATA_DIR="$data" "$server" >"$log" 2>&1 &
    server_pid=$!
    # Long enough for the first run's Postgres download and initdb.
    for _ in $(seq 1 300); do
        # shellcheck disable=SC2016 # Match the literal variable in printed commands.
        grep -q '^  curl .*\$WITHDRAW' "$log" && return 0
        if ! kill -0 "$server_pid" 2>/dev/null; then
            wait "$server_pid" || true
            server_pid=
            return 1
        fi
        sleep 1
    done
    fail "$name didn't print its try-it commands within 300s"
}

stop() {
    kill -INT "$server_pid"
    for _ in $(seq 1 30); do
        kill -0 "$server_pid" 2>/dev/null || break
        sleep 1
    done
    kill -0 "$server_pid" 2>/dev/null && fail "the server didn't stop within 30s of Ctrl-C"
    local status=0
    wait "$server_pid" || status=$?
    server_pid=
    [[ $status -eq 0 ]] || fail "the server exited with status $status on Ctrl-C"
}

# Runs the printed command for $1 (DEPOSIT or WITHDRAW) with amount $2.
send() {
    local variable="$1" amount="$2" command
    eval "$(grep -o 'export DEPOSIT=.*' "$log")"
    command="$(grep "^  curl .*\\\$$variable" "$log")"
    command="${command//\"amount\":100/\"amount\":$amount}"
    command="${command//\"amount\":500/\"amount\":$amount}"
    eval "$command -sS"
}

echo "check-upgrade-pair: $from on an empty database"
start "$old_server" old || fail "$from didn't start on an empty database"
for _ in 1 2 3 4; do
    reply="$(send DEPOSIT 100)"
    [[ "$reply" == *'"accepted":true'* ]] || fail "$from rejected a deposit: $reply"
done
stop

echo "check-upgrade-pair: this workspace upgrades it"
start "$new_server" new || fail "this workspace didn't start on $from's database"
reply="$(send WITHDRAW 350)"
[[ "$reply" == *'"accepted":true'* ]] ||
    fail "withdrawing 350 of the 400 deposited by $from wasn't accepted: $reply"
reply="$(send WITHDRAW 500)"
[[ "$reply" == *'"rejectionKind":"insufficient_funds"'* ]] ||
    fail "withdrawing more than the balance wasn't rejected: $reply"
stop

if $expect_compatible; then
    echo "check-upgrade-pair: $from starts again (declared compatible)"
    start "$old_server" old-again || fail "$from didn't start on the upgraded database"
    reply="$(send DEPOSIT 100)"
    [[ "$reply" == *'"accepted":true'* ]] || fail "$from rejected a deposit after the upgrade: $reply"
    stop
else
    echo "check-upgrade-pair: $from refuses the upgraded database"
    if start "$old_server" old-again; then
        fail "$from started on a database this workspace upgraded; the pair isn't declared compatible"
    fi
    # An older release recording no compatibility level fails on the global
    # migration it doesn't know (sqlx's VersionMissing); a later one with
    # schema_too_new.
    grep -Eq 'previously applied but is missing|schema level' "$log" ||
        fail "$from failed, but not by refusing the upgraded schema"
fi

echo "check-upgrade-pair: this workspace starts again"
start "$new_server" new-again || fail "this workspace didn't start again"
stop

if $expect_compatible; then
    echo "check-upgrade-pair: OK ($from and this workspace share a database)"
else
    echo "check-upgrade-pair: OK ($from upgraded; $from then refused the upgraded database)"
fi
