#!/usr/bin/env bash
# Runs a generated skilj-template project's quickstart the way its README
# tells a reader to: the server started with no DATABASE_URL, so it starts an
# embedded Postgres of its own (downloaded on first use), then the export
# line and the two curl commands it prints - a deposit accepted, a larger
# withdrawal rejected - and a clean shutdown on Ctrl-C. Run it from the
# generated project. The Postgres data directory and the log go in WORK,
# never the user's own data directory.
#
# Shared by scripts/check-template.sh, which starts the built binary, and
# scripts/check-new-project-skill.sh, which starts the server with the
# command the plugin's skilj-new-project skill tells an agent to run.
#
# Usage: scripts/template-quickstart.sh WORK SERVER_COMMAND...
set -euo pipefail

[[ $# -ge 2 ]] || { echo 'template-quickstart: usage: WORK SERVER_COMMAND...' >&2; exit 2; }
work="$1"
shift
mkdir -p "$work"
server_pid=
cleanup() {
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill -INT "$server_pid" 2>/dev/null || true
        sleep 5
        kill -KILL "$server_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
log="$work/server.log"

fail() {
    echo "template-quickstart: $*" >&2
    echo "--- server output ---" >&2
    cat "$log" >&2
    exit 1
}

# Retry only a failed embedded-Postgres download's HTTP 5xx response.
# Application, initialization and quickstart failures must still fail immediately.
for attempt in 1 2 3; do
    env -u DATABASE_URL PORT="$port" SKILJ_LOCAL_DATA_DIR="$work/postgres" \
        "$@" >"$log" 2>&1 &
    server_pid=$!
    # Long enough for a first `cargo run`'s build, the Postgres download and initdb.
    for _ in $(seq 1 600); do
        # shellcheck disable=SC2016 # Match the literal variable in printed commands.
        grep -q '^  curl .*\$WITHDRAW' "$log" && break
        if ! kill -0 "$server_pid" 2>/dev/null; then
            wait "$server_pid" || true
            server_pid=
            break
        fi
        sleep 1
    done
    [[ -n "$server_pid" ]] && break
    if [[ $attempt -eq 3 ]] || ! grep -Eq '^error: local Postgres: HTTP status server error \(5[0-9]{2} ' "$log"; then
        fail "the server exited before it was ready"
    fi
    cat "$log" >&2
    echo "template-quickstart: retrying Postgres download (attempt $((attempt + 1))/3)" >&2
    sleep 2
done
# shellcheck disable=SC2016 # Match the literal variable in printed commands.
grep -q '^  curl .*\$WITHDRAW' "$log" || fail "the server didn't print its try-it commands within 600s"

# The commands exactly as the server prints them, as a reader pastes them.
eval "$(grep -o 'export DEPOSIT=.*' "$log")"
mapfile -t commands < <(grep '^  curl ' "$log")
[[ ${#commands[@]} -eq 2 ]] || fail "expected two printed curl commands, got ${#commands[@]}"
deposit="$(eval "${commands[0]} -sS")"
withdraw="$(eval "${commands[1]} -sS")"
[[ "$deposit" == *'"accepted":true'* ]] || fail "the deposit wasn't accepted: $deposit"
[[ "$withdraw" == *'"rejectionKind":"insufficient_funds"'* ]] ||
    fail "the withdrawal of more than the balance wasn't rejected as insufficient_funds: $withdraw"

# `cargo run` passes SIGINT on to the server it started.
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

echo "template-quickstart: OK (deposit accepted, overdraft rejected)"
