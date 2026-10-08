#!/usr/bin/env bash
# Composes the federation descriptions skilj publishes - recorded under
# two prefixes in skilj/tests/fixtures/federation/, as two skilj services
# would publish them - next to another subgraph using skilj's unprefixed
# names, with both Apollo's composition (Rover, GraphOS) and Hive's (Hive
# Console, Hive Router), and checks the API schema serves exactly the
# published root fields (docs/architecture.md §194).
#
# The fixtures are kept current by skilj/tests/graphql_federation.rs; this
# checks they still compose. Needs Node.js and npm.
#
# Usage: scripts/check-federation-composition.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
fixtures="$root/skilj/tests/fixtures/federation"
cd "$root/scripts/federation"
npm ci --silent --no-audit --no-fund
node compose.mjs \
    "ledger=$fixtures/ledger.graphql" \
    "bank=$fixtures/bank.graphql" \
    "other=other-subgraph.graphql"
