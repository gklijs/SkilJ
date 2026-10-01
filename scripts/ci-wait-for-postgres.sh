#!/usr/bin/env bash
#
# Waits for the CI step's Postgres to accept authenticated connections,
# and says *why* it can't if it never does.
#
# Woodpecker gives a `services:` container no readiness signal, so a step
# has to poll for one. The polling half is trivial; the part that has
# repeatedly cost a CI cycle here is the failure half. An earlier inline
# version of this loop in .woodpecker.yml redirected everything to
# /dev/null and, when the service never came up, printed exactly one
# line - "didn't accept authenticated connections within 120s" - which
# is the same message for a slow first-boot initdb, a container that
# exited, an image that failed to pull, a hostname that doesn't resolve,
# a wrong password, and a step image with no psql on it at all. This
# script prints the resolver's answer, the hosts file, the route, and
# the *actual* psql error, so the next failure names its own cause.
#
# Exits 0 when the server answers, 1 otherwise (with the diagnostics
# already printed). `scripts/ci-test.sh` uses that exit to decide
# whether to fall back to a Postgres inside the step's own container.
#
# Configuration is by environment variable, so the same script serves
# the `services:` container (default: host `postgres`) and the in-step
# fallback (host `127.0.0.1`).

set -euo pipefail

HOST="${CI_POSTGRES_HOST:-postgres}"
PORT="${CI_POSTGRES_PORT:-5432}"
USER="${CI_POSTGRES_USER:-postgres}"
PASSWORD="${CI_POSTGRES_PASSWORD:-postgres}"
DB="${CI_POSTGRES_DB:-postgres}"
TIMEOUT="${CI_POSTGRES_TIMEOUT:-120}"

log() { printf '[postgres-wait] %s\n' "$*"; }

# A missing client tool is a different failure from an unreachable
# server, and silently looping for the full timeout hides it: both psql
# and pg_isready would be "not ready" forever.
if ! command -v psql >/dev/null 2>&1 || ! command -v pg_isready >/dev/null 2>&1; then
  log "FATAL: this image has no psql/pg_isready, so 'not ready' would"
  log "       mean 'cannot even try'. Install postgresql-client first."
  exit 1
fi

log "target ${USER}@${HOST}:${PORT}/${DB}, timeout ${TIMEOUT}s"

diagnostics() {
  log "--- name resolution ---"
  getent hosts "$HOST" || log "getent hosts ${HOST}: no answer"
  log "--- /etc/hosts ---"
  cat /etc/hosts 2>/dev/null || log "(unreadable)"
  log "--- /etc/resolv.conf ---"
  cat /etc/resolv.conf 2>/dev/null || log "(unreadable)"
  log "--- routes ---"
  ip route 2>/dev/null || log "(no ip command)"
  log "--- raw TCP probe ---"
  if timeout 5 bash -c "exec 3<>/dev/tcp/${HOST}/${PORT}" 2>/dev/null; then
    log "/dev/tcp ${HOST}:${PORT}: connected"
  else
    log "/dev/tcp ${HOST}:${PORT}: no connection"
  fi
  log "--- psql ---"
  local out
  out=$(PGPASSWORD="$PASSWORD" PGCONNECT_TIMEOUT=5 \
    psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -tAq -c 'SELECT 1' 2>&1) || true
  printf '%s\n' "$out" | sed 's/^/[postgres-wait]   /'
  case "$out" in
    *"password authentication failed"*)
      log "The server answered and rejected the credentials, so this is not a"
      log "startup problem: no amount of waiting will change it. Two ways that"
      log "happens - the client's password and the server's disagree (this step"
      log "sends CI_POSTGRES_PASSWORD, currently '${PASSWORD}'), or the name"
      log "'${HOST}' is not this step's service at all but something else on"
      log "the same network, in which case no credential this step sends will"
      log "ever be accepted. The service is named distinctly for that reason;"
      log "the line above shows which address answered, and a successful"
      log "connect would print the server's own data_directory."
      ;;
  esac
}

# One probe, printing the server's own complaint on failure rather than
# swallowing it.
probe() {
  local out
  if ! out=$(PGPASSWORD="$PASSWORD" PGCONNECT_TIMEOUT=5 \
    psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -tAq -c 'SELECT 1' 2>&1); then
    printf '%s' "$out"
    return 1
  fi
  [ "$(printf '%s' "$out" | tr -d '[:space:]')" = "1" ]
}

# First failure is always logged, later ones only when the message
# changes: a slow initdb stays visible, and a server stuck on one error
# doesn't print 120 copies of it.
last=""
for i in $(seq 1 "$TIMEOUT"); do
  if out=$(probe); then
    log "ready after ${i}s"
    # What the server says about itself. A `services:` container's own
    # identity is otherwise invisible from here - the step can't see its
    # environment, its logs or its container id - so this is the only
    # way to tell our Postgres from any other one this network may be
    # answering to under the same name.
    identity=$(PGPASSWORD="$PASSWORD" PGCONNECT_TIMEOUT=5 \
      psql -h "$HOST" -p "$PORT" -U "$USER" -d "$DB" -tAq \
      -c "SELECT current_setting('data_directory') || ' | ' || version()" 2>&1) || true
    log "server says: ${identity}"
    exit 0
  fi
  if [ -z "$last" ] || [ "$out" != "$last" ]; then
    log "[${i}s] not ready: $(printf '%s' "$out" | tr '\n' ' ')"
    last="$out"
  fi
  sleep 1
done

log "ERROR: ${HOST}:${PORT} did not accept authenticated connections within ${TIMEOUT}s"
diagnostics
exit 1