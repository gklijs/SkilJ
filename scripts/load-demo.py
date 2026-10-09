#!/usr/bin/env python3
"""End-to-end load run against a running skilj-demo server.

Drives `POST /v1/commands/trigger` with `banking/DepositMoney` from N
concurrent clients for a fixed number of commands, then (with
--database-url) checks what Postgres holds afterwards:

- the banking event log has no gap and no duplicate in its sequence,
- every accepted command produced exactly one event, with the amounts
  summing up,
- the sync `AccountBalance` projection agrees with those events.

  --mode spread  every command deposits into its own account (no contention)
  --mode hot     every command deposits into the same account

Start the server with `cargo run --release -p skilj-demo --bin server`
(DATABASE_URL set) and pass the `banking/DepositMoney:` token it prints.
Python standard library only; the checks shell out to `psql`.
"""

import argparse
import http.client
import json
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid


def run_clients(url, token, clients, commands, mode, run_id):
    parsed = urllib.parse.urlparse(url)
    next_index = [0]
    lock = threading.Lock()
    latencies, statuses, errors = [], {}, []

    def take():
        with lock:
            if next_index[0] >= commands:
                return None
            next_index[0] += 1
            return next_index[0]

    def client():
        conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=60)
        while (i := take()) is not None:
            account = f"{run_id}-hot" if mode == "hot" else f"{run_id}-{i}"
            body = json.dumps({"payload": {"account_id": account, "amount": i}})
            started = time.perf_counter()
            try:
                conn.request(
                    "POST",
                    "/v1/commands/trigger",
                    body=body,
                    headers={"content-type": "application/json", "authorization": f"Bearer {token}"},
                )
                response = conn.getresponse()
                text = response.read()
                status = response.status
            except Exception as e:  # noqa: BLE001 - every failure is counted, not fatal
                conn.close()
                conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80, timeout=60)
                status, text = f"error: {type(e).__name__}", str(e).encode()
            elapsed = time.perf_counter() - started
            with lock:
                statuses[status] = statuses.get(status, 0) + 1
                if isinstance(status, int) and 200 <= status < 300:
                    latencies.append((elapsed, i))
                elif len(errors) < 5:
                    errors.append((status, text[:300]))
        conn.close()

    threads = [threading.Thread(target=client) for _ in range(clients)]
    started = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return time.perf_counter() - started, latencies, statuses, errors


def psql(database_url, sql):
    out = subprocess.run(
        ["psql", database_url, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", sql],
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


def check(database_url, run_id, accepted_amounts):
    failures = []
    total, low, high, distinct = psql(
        database_url,
        "SELECT count(*), min(sequence), max(sequence), count(DISTINCT sequence) FROM bc_banking.events",
    ).split("|")
    if int(total) != int(high) - int(low) + 1 or int(total) != int(distinct):
        failures.append(f"event sequence has gaps or duplicates: {total} events over {low}..{high}")

    count, amount_sum = psql(
        database_url,
        "SELECT count(*), coalesce(sum((payload::jsonb->>'amount')::bigint), 0) FROM bc_banking.events "
        f"WHERE event_type_name = 'MoneyDeposited' AND payload::jsonb->>'account_id' LIKE '{run_id}-%'",
    ).split("|")
    if int(count) != len(accepted_amounts) or int(amount_sum) != sum(accepted_amounts):
        failures.append(
            f"{len(accepted_amounts)} accepted commands (sum {sum(accepted_amounts)}) "
            f"but {count} events (sum {amount_sum})"
        )

    balance_sum = psql(
        database_url,
        "SELECT coalesce(sum((state::jsonb->>'balance')::bigint), 0) FROM bc_banking.projection_state "
        f"WHERE projection_name = 'AccountBalance' AND key LIKE '{run_id}-%'",
    )
    if int(balance_sum) != sum(accepted_amounts):
        failures.append(f"AccountBalance sums to {balance_sum}, events to {sum(accepted_amounts)}")
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://127.0.0.1:8080")
    parser.add_argument("--token", required=True, help="the banking/DepositMoney token the server printed")
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--commands", type=int, default=2000)
    parser.add_argument("--mode", choices=["spread", "hot"], default="spread")
    parser.add_argument("--database-url", help="the server's DATABASE_URL, to check what was written")
    args = parser.parse_args()

    run_id = f"load-{uuid.uuid4().hex[:12]}"
    elapsed, latencies, statuses, errors = run_clients(
        args.url, args.token, args.clients, args.commands, args.mode, run_id
    )
    times = sorted(t for t, _ in latencies)

    def pct(p):
        return times[min(len(times) - 1, int(len(times) * p))] * 1000 if times else float("nan")

    print(
        f"{args.mode:6} clients={args.clients:3} commands={args.commands} "
        f"accepted={len(latencies)} {len(latencies) / elapsed:8.1f}/s "
        f"p50={pct(0.50):7.1f}ms p95={pct(0.95):7.1f}ms p99={pct(0.99):7.1f}ms "
        f"statuses={statuses}"
    )
    for status, text in errors:
        print(f"  {status}: {text!r}")

    failed = len(latencies) != args.commands
    if args.database_url:
        failures = check(args.database_url, run_id, [i for _, i in latencies])
        for f in failures:
            print(f"  CHECK FAILED: {f}")
        failed = failed or bool(failures)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
