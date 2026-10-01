#!/usr/bin/env bash
#
# Starts a Postgres *inside* the CI step's own container, as a fallback
# for when the step's `services:` container isn't reachable.
#
# Why this exists: Woodpecker (as Codeberg runs it) gives a step's
# services no readiness signal and, so far, no way for the step to find
# out whether a service is merely slow, exited, or was never reachable
# at all. `scripts/ci-wait-for-postgres.sh` now answers that question,
# but a red CI run is still a wasted run - so the step prefers the
# service and falls back to this, and only stays red if both fail.
#
# The trade-offs, stated rather than discovered later:
#
#   * One Postgres version per step image (Debian bookworm's 15) rather
#     than whatever the service tag tracks. The suite only uses ordinary
#     DDL, so this costs nothing.
#   * No `initdb` on first boot, hence no slow start to poll for - the
#     server is up when `pg_ctl -w start` returns.
#   * Durability is off (`fsync=off`, `synchronous_commit=off`,
#     `full_page_writes=off`). Nothing in a test run needs to survive a
#     crash, and these are also applied to the service container, where
#     they cut a full-workspace test run substantially.
#   * Trust auth on the loopback socket, so DATABASE_URL keeps its usual
#     shape; a password is accepted and ignored.
#   * The server runs as the unprivileged `postgres` user that the
#     Debian package creates, because the server refuses to run as root
#     and the step container's default user is root.

set -euo pipefail

PGDATA="${CI_LOCAL_PGDATA:-/tmp/ci-postgres/data}"
PGSOCK="${CI_LOCAL_PGSOCK:-/tmp/ci-postgres/sock}"
PGPORT="${CI_POSTGRES_PORT:-5432}"
PGUSER="${CI_POSTGRES_USER:-postgres}"
PGLOG="${CI_LOCAL_PGLOG:-/tmp/ci-postgres/postgres.log}"

log() { printf '[local-postgres] %s\n' "$*"; }

# Debian installs the *client* on PATH (/usr/bin/psql, via pg_wrapper) but
# the server only under /usr/lib/postgresql/<major>/bin - so `command -v
# pg_ctl` finds nothing even straight after installing, and
# `dirname "$(command -v pg_ctl)"` is "." rather than a directory, which
# is exactly how the first CI run of this script died with
# "./initdb: No such file or directory". Search the versioned
# directories instead, newest first, and only then PATH.
#
# The glob roots are overridable so this function is testable without
# installing a server (see the ordering claim below: the last match of
# /usr/lib/postgresql/*/bin is the newest major).
PG_SEARCH_ROOTS="${CI_PG_SEARCH_ROOTS:-/usr/lib/postgresql/*/bin /usr/local/pgsql/bin}"
find_pgbin() {
  local root candidate found=""
  for root in $PG_SEARCH_ROOTS; do
    for candidate in $root/pg_ctl; do
      [ -x "$candidate" ] && found="${candidate%/pg_ctl}"
    done
  done
  if [ -n "$found" ]; then
    printf '%s\n' "$found"
    return 0
  fi
  candidate="$(command -v pg_ctl 2>/dev/null || true)"
  [ -n "$candidate" ] && printf '%s\n' "${candidate%/pg_ctl}" && return 0
  return 1
}

PGBIN="${CI_PGBIN:-}"
if [ -z "$PGBIN" ]; then
  if ! PGBIN="$(find_pgbin)"; then
    log "no server binaries yet, installing postgresql from apt"
    apt-get update -qq
    # --no-install-recommends keeps the server out of the way of the
    # client's default: an apt-created cluster that nothing starts is
    # exactly the sort of half-initialised state this script exists to
    # avoid, so initdb below does the one cluster that is actually used.
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends postgresql
    if ! PGBIN="$(find_pgbin)"; then
      log "FATAL: postgresql installed but no pg_ctl found under /usr/lib/postgresql/*/bin"
      exit 1
    fi
  fi
fi
log "server binaries in ${PGBIN}"

id postgres >/dev/null 2>&1 || {
  log "FATAL: the image has no 'postgres' user to run the server as"
  exit 1
}

# Everything after `su ... -c` has to survive one round of re-quoting by
# `su`, so the server's options live in a variable of their own (no
# single quotes in it) rather than being spliced into a command string.
PGOPTS="-p ${PGPORT} -k ${PGSOCK} -c listen_addresses=127.0.0.1"
# Durability off: nothing in a test run needs to survive a crash.
# max_connections raised from the default 100, because `cargo test
# --workspace` runs dozens of binaries at once and each holds a pool -
# which is the other way a run like this fails, and one that also fails
# on the service container, which cannot be given a command at all.
PGOPTS="${PGOPTS} -c fsync=off -c synchronous_commit=off -c full_page_writes=off"
PGOPTS="${PGOPTS} -c max_connections=500"

as_postgres() { su postgres -s /bin/bash -c "exec $*"; }

# Idempotent, because the CI step calls this once for a fast "is the
# database there at all" check before the long build and again in
# scripts/ci-test.sh. Re-running initdb over a live data directory would
# throw away everything the first server accumulated.
if [ -s "${PGDATA}/PG_VERSION" ]; then
  if as_postgres "'${PGBIN}/pg_ctl' -D '${PGDATA}' status" >/dev/null 2>&1; then
    log "already running from ${PGDATA}, leaving it alone"
    exit 0
  fi
  log "data directory ${PGDATA} exists but no server is running; restarting it"
  as_postgres "'${PGBIN}/pg_ctl' -D '${PGDATA}' -l '${PGLOG}' -w -t 60 -o '${PGOPTS}' start"
  exit 0
fi

rm -rf "$PGDATA" "$PGSOCK"
mkdir -p "$PGDATA" "$PGSOCK" "$(dirname "$PGLOG")"
chown -R postgres:postgres "$PGDATA" "$PGSOCK" "$(dirname "$PGLOG")"

log "initdb into ${PGDATA} (this takes a few seconds)"
as_postgres "'${PGBIN}/initdb' -D '${PGDATA}' -U '${PGUSER}' --auth-local=trust --auth-host=trust"

log "starting on 127.0.0.1:${PGPORT}"
as_postgres "'${PGBIN}/pg_ctl' -D '${PGDATA}' -l '${PGLOG}' -w -t 60 -o '${PGOPTS}' start"

log "started; server log tail:"
tail -n 5 "$PGLOG" 2>/dev/null | sed 's/^/[local-postgres]   /' || true