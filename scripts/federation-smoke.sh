#!/usr/bin/env bash
# Runs skilj-demo as a federation subgraph behind real routers and drives
# them (docs/architecture.md §194): Apollo Router 2 and the 3.0 preview,
# composed with Rover, and Hive Router. Each gets a mutation, a projection
# read, a refusal without a credential, and an Account.balance join that
# the router resolves through skilj's `_entities`; Hive Router also a
# subscription. Apollo Router only routes subscriptions when connected to
# GraphOS (APOLLO_KEY/APOLLO_GRAPH_REF), so they're skipped there.
#
# Local only, not part of CI: it downloads Rover (and Rover's supergraph
# plugin) and three router binaries into target/federation-smoke/, which
# means accepting the Elastic License 2.0 Apollo's tools are under. When
# rover.apollo.dev can't be reached, it composes with @apollo/composition
# from npm instead (scripts/federation/supergraph.mjs).
#
# Needs DATABASE_URL naming a Postgres database skilj-demo may use (its
# server is safe to run again against the same one), Node.js with npm,
# and curl.
#
# Usage: DATABASE_URL=postgres://... scripts/federation-smoke.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$root/target/federation-smoke"
bin="$work/bin"
apollo_routers=(v2.18.0 v3.0.0-preview.0)
hive_router=v0.3.2
mkdir -p "$bin"
export APOLLO_ELV2_LICENSE=accept APOLLO_TELEMETRY_DISABLED=1

pids=()
cleanup() { for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

wait_for() { # url
    for _ in $(seq 1 60); do
        curl -s -o /dev/null "$1" && return 0
        sleep 1
    done
    echo "federation-smoke: nothing answered at $1" >&2
    return 1
}

# --- tools ---
# Rover comes from rover.apollo.dev, which some networks block. Without
# it, a subgraph's SDL is read with a plain `_service` query and the
# supergraph composed by scripts/federation/supergraph.mjs, with the same
# composition library from npm.
if [[ ! -x "$bin/rover" ]]; then
    if curl -sSfL https://rover.apollo.dev/tar/rover/x86_64-unknown-linux-gnu/latest | tar xz -C "$work" 2>/dev/null \
        && [[ -x "$work/dist/rover" ]]; then
        mv "$work/dist/rover" "$bin/rover"
    else
        echo "federation-smoke: no Rover (rover.apollo.dev unreachable) - composing with @apollo/composition" >&2
    fi
fi
for v in "${apollo_routers[@]}"; do
    if [[ ! -x "$bin/router-$v" ]]; then
        curl -sSfL "https://github.com/apollographql/router/releases/download/$v/router-$v-x86_64-unknown-linux-gnu.tar.gz" | tar xz -C "$work"
        mv "$work/dist/router" "$bin/router-$v"
    fi
done
if [[ ! -x "$bin/hive_router-$hive_router" ]]; then
    curl -sSfL "https://github.com/graphql-hive/router/releases/download/hive-router/$hive_router/hive_router_linux_amd64" -o "$bin/hive_router-$hive_router"
    chmod +x "$bin/hive_router-$hive_router"
fi
(cd "$root/scripts/federation" && npm ci --silent --no-audit --no-fund)

# --- the subgraphs ---
cargo build -q -p skilj-demo --bin server
PORT=18080 SKILJ_FEDERATION_PREFIX=demo \
    "$root/target/debug/server" > "$work/demo.log" 2>&1 &
pids+=($!)
wait_for http://127.0.0.1:18080/graphql
for _ in $(seq 1 30); do grep -q "GraphQL Role credential" "$work/demo.log" && break; sleep 1; done
jwt="$(grep -A1 "GraphQL Role credential" "$work/demo.log" | tail -1 | tr -d ' ')"

accounts="smoke-$(date +%s)-a,smoke-$(date +%s)-b"
ACCOUNT_IDS="$accounts" PORT=18081 node "$root/scripts/federation/accounts-subgraph.mjs" &
pids+=($!)
wait_for http://127.0.0.1:18081
for account in ${accounts//,/ }; do
    curl -sf http://127.0.0.1:18080/graphql -H "authorization: Bearer $jwt" \
        -H 'content-type: application/json' \
        -d "{\"query\":\"mutation(\$p: String!) { demoSubmitCommand(boundedContext: \\\"banking\\\", commandTypeName: \\\"DepositMoney\\\", payload: \$p) { accepted } }\",\"variables\":{\"p\":\"{\\\"account_id\\\":\\\"$account\\\",\\\"amount\\\":11}\"}}" > /dev/null
done

# --- composition, through skilj's own `_service` ---
introspect() { # url
    if [[ -x "$bin/rover" ]]; then
        "$bin/rover" subgraph introspect "$1"
    else
        curl -sSf "$1" -H 'content-type: application/json' \
            -d '{"query":"query SubgraphIntrospectQuery { _service { sdl } }"}' |
            node -e 'let b="";process.stdin.on("data",d=>b+=d).on("end",()=>process.stdout.write(JSON.parse(b).data._service.sdl))'
    fi
}
introspect http://127.0.0.1:18080/graphql > "$work/demo.graphql"
introspect http://127.0.0.1:18081 > "$work/accounts.graphql"
cat > "$work/supergraph.yaml" <<YAML
federation_version: =2.11.2
subgraphs:
  demo:
    routing_url: http://127.0.0.1:18080/graphql
    schema:
      file: $work/demo.graphql
  accounts:
    routing_url: http://127.0.0.1:18081
    schema:
      file: $work/accounts.graphql
YAML
if [[ -x "$bin/rover" ]]; then
    APOLLO_ROVER_ALLOW_AUTOMATIC_DOWNLOAD=true "$bin/rover" supergraph compose \
        --config "$work/supergraph.yaml" --elv2-license accept > "$work/supergraph.graphql"
else
    (cd "$root/scripts/federation" && node supergraph.mjs \
        "demo=$work/demo.graphql=http://127.0.0.1:18080/graphql" \
        "accounts=$work/accounts.graphql=http://127.0.0.1:18081") > "$work/supergraph.graphql"
fi

smoke() { # http-url ws-url
    (cd "$root/scripts/federation" && ACCOUNT_IDS="$accounts" node smoke.mjs "$1" "$2" "$jwt")
}

# --- Apollo Router ---
cat > "$work/apollo-router.yaml" <<YAML
supergraph:
  listen: 127.0.0.1:4000
  path: /graphql
headers:
  all:
    request:
      operations:
        - propagate:
            named: authorization
health_check:
  enabled: false
YAML
failed=0
for v in "${apollo_routers[@]}"; do
    echo "== Apollo Router $v"
    "$bin/router-$v" --supergraph "$work/supergraph.graphql" --config "$work/apollo-router.yaml" \
        > "$work/apollo-router-$v.log" 2>&1 &
    pid=$!
    wait_for http://127.0.0.1:4000/graphql
    smoke http://127.0.0.1:4000/graphql - || failed=1
    kill "$pid"; wait "$pid" 2>/dev/null || true
done

# --- Hive Router: reads router.config.yaml from its working directory ---
echo "== Hive Router $hive_router"
mkdir -p "$work/hive"
cat > "$work/hive/router.config.yaml" <<YAML
supergraph:
  source: file
  path: $work/supergraph.graphql
http:
  host: 127.0.0.1
  port: 4001
headers:
  all:
    request:
      - propagate:
          named: authorization
subscriptions:
  enabled: true
  websocket:
    all: {}
websocket:
  enabled: true
YAML
(cd "$work/hive" && exec "$bin/hive_router-$hive_router") > "$work/hive-router.log" 2>&1 &
pids+=($!)
wait_for http://127.0.0.1:4001/graphql
smoke http://127.0.0.1:4001/graphql ws://127.0.0.1:4001/graphql || failed=1

exit "$failed"
