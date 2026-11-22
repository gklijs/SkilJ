# shellcheck shell=bash
# What run.sh, check.sh, suite.sh and check-solutions.sh share
# (docs/agent-guidance.md, docs/architecture.md §257). Sourced, not run.

eval_here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
eval_root="$(cd "$eval_here/../../.." && pwd)"

# The tasks, one list for every script: the names of tasks/*.md.
eval_tasks() {
    local file
    for file in "$eval_here"/tasks/*.md; do
        basename "$file" .md
    done
}

# Builds this workspace's skilj-codegen and puts it first on PATH, so the
# agent and the check use the importer of the workspace under test, not a
# published one an agent might `cargo install`. It builds into a target
# directory of its own, whatever CARGO_TARGET_DIR the caller exported.
setup_tools() {
    local tools="$eval_root/target/agent-eval-tools"
    (cd "$eval_root" && cargo build --quiet -p skilj-codegen --target-dir "$tools") || {
        echo "eval: building this workspace's skilj-codegen failed" >&2
        return 1
    }
    PATH="$tools/debug:$PATH"
    export PATH
    hash -r
    if [[ "$(command -v skilj-codegen)" != "$tools/debug/skilj-codegen" ]]; then
        echo "eval: skilj-codegen resolves to $(command -v skilj-codegen), not $tools/debug/skilj-codegen" >&2
        return 1
    fi
}

# The lock file for a build directory: outside it, so `cargo clean` can't
# remove it while it is held, and named after the build directory's
# canonical path, so every spelling of one directory takes the same lock.
eval_lock_file() {
    local dir base
    mkdir -p "$1" || return 1
    dir="$(cd "$1" && pwd -P)" || return 1
    base="${XDG_RUNTIME_DIR:-${TMPDIR:-/tmp}}/skilj-agent-eval-locks"
    mkdir -p "$base" || return 1
    printf '%s/%s.lock\n' "$base" "$(printf '%s' "$dir" | sha256sum | cut -c1-32)"
}

# eval_lock <build dir>: takes the exclusive lock on a build directory and
# leaves its file descriptor in eval_lock_fd, held until the script exits.
# One run at a time uses a build directory: an agent's or a check's build
# would otherwise overwrite the other's executables mid-run.
#
# A script that already holds it (run.sh, around the agent and the check)
# passes the descriptor to the one it runs as SKILJ_EVAL_LOCK_FD. That is
# honoured only when the descriptor is open on this very lock file, so a
# stale or made-up value can't skip the lock: the lock is then taken here.
# It needs flock(1) from util-linux, so Linux.
eval_lock() {
    local file
    command -v flock >/dev/null || {
        echo 'eval: needs flock (util-linux), to run one evaluation at a time per build directory' >&2
        return 1
    }
    file="$(eval_lock_file "$1")" || return 1
    if [[ "${SKILJ_EVAL_LOCK_FD:-}" =~ ^[0-9]+$ ]] &&
        [[ "$(readlink "/proc/self/fd/$SKILJ_EVAL_LOCK_FD" 2>/dev/null)" == "$file" ]]; then
        eval_lock_fd="$SKILJ_EVAL_LOCK_FD"
    else
        exec {eval_lock_fd}>>"$file"
    fi
    unset SKILJ_EVAL_LOCK_FD
    if ! flock -n "$eval_lock_fd"; then
        echo "eval: waiting for the lock on $1: another evaluation run uses this build directory" \
            "until its check is done (SKILJ_EVAL_BUILD_DIR gives a run a build directory of its own)" >&2
        flock "$eval_lock_fd" || return 1
    fi
}

# Runs a command without the lock's descriptor, so nothing it leaves
# running (a server, a process an agent backgrounded) keeps the lock after
# the script is gone.
without_lock() {
    if [[ -n "${eval_lock_fd:-}" ]]; then
        "$@" {eval_lock_fd}>&-
    else
        "$@"
    fi
}
