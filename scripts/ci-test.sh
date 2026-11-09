#!/usr/bin/env bash
#
# CI's database setup and test run, as a script rather than a wall of
# YAML.
#
# Three reasons it isn't inline in .github/workflows/ci.yml:
#
#   * The Postgres setup needs to export DATABASE_URL, and a job whose
#     `run:` steps are each their own process can't rely on that
#     surviving from one line to the next. A single script makes the
#     environment a fact rather than a hope.
#   * The service-reachable-or-fallback decision is a branch, and a
#     branch in YAML is a multi-line `|` block that has already broken
#     once.
#   * It's runnable by hand: `bash scripts/ci-test.sh` reproduces the
#     step's database setup and the run, without a CI run.
#
# `--setup-only` settles the database and stops there. The CI step runs
# that first and this in full at the end: a database that isn't there
# should be visible in the first seconds of the log, not after twenty
# minutes of compiling librdkafka. `--build-only` compiles one whole-binary
# shard without needing a database. Both database calls do the same setup, and
# scripts/ci-postgres-local.sh is idempotent, so the second one is
# cheap.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CI_POSTGRES_HOST="${CI_POSTGRES_HOST:-postgres}"
CI_POSTGRES_PORT="${CI_POSTGRES_PORT:-5432}"
CI_POSTGRES_USER="${CI_POSTGRES_USER:-postgres}"
CI_POSTGRES_PASSWORD="${CI_POSTGRES_PASSWORD:-postgres}"
CI_POSTGRES_DB="${CI_POSTGRES_DB:-postgres}"
export CI_POSTGRES_HOST CI_POSTGRES_PORT CI_POSTGRES_USER CI_POSTGRES_PASSWORD CI_POSTGRES_DB

log() { printf '[ci-test] %s\n' "$*"; }

setup_database() {
  # 1. The step's own `services:` container, if it is there.
  if CI_POSTGRES_TIMEOUT="${CI_POSTGRES_TIMEOUT:-30}" bash "$HERE/ci-wait-for-postgres.sh"; then
    database_host="$CI_POSTGRES_HOST"
    log "using the postgres service container at ${CI_POSTGRES_HOST}:${CI_POSTGRES_PORT}"
  else
    if [[ -n "${CI_POSTGRES_MAJOR:-}" ]]; then
      log "FATAL: required Postgres ${CI_POSTGRES_MAJOR} service is unavailable; refusing fallback"
      exit 1
    fi
    # 2. Otherwise, a server in this container, so a broken service is a
    #    loud note in the log rather than a red pipeline.
    log "WARNING: the postgres service container was not usable; falling back to a"
    log "WARNING: Postgres inside this job. The service block in .github/workflows/ci.yml is"
    log "WARNING: not doing what it says it does - worth fixing, but not worth a red"
    log "WARNING: build over."
    bash "$HERE/ci-postgres-local.sh"
    database_host=127.0.0.1
    log "using the in-step Postgres at 127.0.0.1:${CI_POSTGRES_PORT}"
  fi

  DATABASE_URL="postgres://${CI_POSTGRES_USER}:${CI_POSTGRES_PASSWORD}@${database_host}:${CI_POSTGRES_PORT}/${CI_POSTGRES_DB}"
  export DATABASE_URL
  log "DATABASE_URL=${DATABASE_URL}"

  # A compatibility job must prove which server it exercised, rather than
  # silently passing against a different installed major. Check before writes.
  if [[ -n "${CI_POSTGRES_MAJOR:-}" ]]; then
    local server_version
    server_version=$(PGPASSWORD="$CI_POSTGRES_PASSWORD" PGCONNECT_TIMEOUT=5 \
      psql -h "$database_host" -p "$CI_POSTGRES_PORT" \
      -U "$CI_POSTGRES_USER" -d "$CI_POSTGRES_DB" -v ON_ERROR_STOP=1 -tAq \
      -c 'SHOW server_version_num') || {
        log "FATAL: cannot read the required server version"
        exit 1
      }
    if [[ ! "$server_version" =~ ^[1-9][0-9]{5}$ ]] || \
        [[ "$((10#$server_version / 10000))" != "$CI_POSTGRES_MAJOR" ]]; then
      log "FATAL: expected Postgres ${CI_POSTGRES_MAJOR}, got server_version_num=${server_version}"
      exit 1
    fi
    log "verified Postgres ${CI_POSTGRES_MAJOR} (server_version_num=${server_version})"
  fi

  # 3. Prove the tests' own path into that server works before starting a
  #    run that would otherwise report fifty-nine passing tests that never
  #    touched it: connect, and create a database, which is what every test
  #    binary's first `database_url` call does
  #    (skilj-test-support's `fresh_named_database`).
  #
  #    Two `-c` flags rather than one with two statements, because DROP
  #    and CREATE DATABASE cannot run in a transaction block and psql
  #    sends one `-c` as a single query.
  PGPASSWORD="$CI_POSTGRES_PASSWORD" PGCONNECT_TIMEOUT=5 psql -h "$database_host" -p "$CI_POSTGRES_PORT" \
    -U "$CI_POSTGRES_USER" -d "$CI_POSTGRES_DB" -v ON_ERROR_STOP=1 -tAq \
    -c "DROP DATABASE IF EXISTS ci_preflight" -c "CREATE DATABASE ci_preflight" \
    >/dev/null ||
    {
      log "FATAL: cannot create a database on the server the tests were just pointed at"
      exit 1
    }
  log "preflight: created a database on ${database_host}"
}

# Validate the group even for build-only runs, before any database work.
# shellcheck source=scripts/ci-test-targets.sh
source "$HERE/ci-test-targets.sh"
ci_test_targets "${CI_TEST_GROUP:-all}"
if [[ "${CI_TEST_GROUP:-all}" == postgres && -z "${CI_POSTGRES_MAJOR:-}" ]]; then
  log "FATAL: CI_TEST_GROUP=postgres requires CI_POSTGRES_MAJOR"
  exit 2
fi
if [[ -n "${CI_POSTGRES_MAJOR:-}" && ! "$CI_POSTGRES_MAJOR" =~ ^[1-9][0-9]$ ]]; then
  log "FATAL: CI_POSTGRES_MAJOR must be a two-digit major version"
  exit 2
fi
case "${1:-}" in
    --build-only)
        cargo test "${test_args[@]}" --no-run --timings
        exit 0
        ;;
    ''|--setup-only) ;;
    *) log "unknown argument: $1"; exit 2 ;;
esac

setup_database

if [ "${1:-}" = "--setup-only" ]; then
  log "database settled; stopping before the test run as asked"
  exit 0
fi

# 4. The run itself, and the guard that makes a silently skipped test a
#    red build rather than a green one. Every test that needs something
#    external - Postgres, a Docker daemon for the Kafka/AMQP/NATS
#    brokers, Temporal's test server - prints a `skipping: ` line and
#    passes when it can't get it. CI provides all of them, so any such
#    line means a suite didn't run. Until the move to GitHub Actions
#    (docs/architecture.md §222) only the Postgres-flavoured lines were
#    checked, because Woodpecker's container had no Docker daemon.
#
# `--nocapture` is what makes this check work at all: the skip note is
# printed from inside a test that then passes, and libtest swallows a
# passing test's output - without it, a run where every Postgres test
# skipped logged no "skipping:" line and this grep could never match
# (reproduced with an unreachable DATABASE_URL: 59 "passing" persistence
# tests, zero matches). Not anchored with `^` for the same reason: with
# tests running in parallel, a note can land mid-line after another
# test's own `test ... ` prefix.
#
# `--no-fail-fast` so one failing suite doesn't hide what the others
# would have said, or that they would have skipped.
log_output="${CI_TEST_LOG:-/tmp/test-output.log}"
test_exit=0
# CI_CARGO_TEST swaps the runner for the coverage job (`cargo llvm-cov
# --no-report`), which then reuses this skip guard.
# shellcheck disable=SC2086 # the command is meant to word-split
${CI_CARGO_TEST:-cargo test} "${test_args[@]}" --no-fail-fast -- --nocapture >"$log_output" 2>&1 || test_exit=$?
if $test_app_docs; then
  # Preserve doctests for skilj, which the app shard's explicit targets omit.
  # shellcheck disable=SC2086
  ${CI_CARGO_TEST:-cargo test} -p skilj --features local --doc --no-fail-fast -- --nocapture >>"$log_output" 2>&1 || test_exit=$?
fi
cat "$log_output"

if grep -qE 'skipping: ' "$log_output"; then
  log "ERROR: one or more tests silently skipped (see the 'skipping: ...' lines"
  log "ERROR: above) instead of running - CI must never report green without"
  log "ERROR: actually exercising Postgres, the brokers and Temporal."
  exit 1
fi

if [ "$test_exit" -ne 0 ]; then
  log "ERROR: cargo test (${CI_TEST_GROUP:-all}) exited ${test_exit}"
  exit "$test_exit"
fi

log "all tests ran against ${DATABASE_URL}"
