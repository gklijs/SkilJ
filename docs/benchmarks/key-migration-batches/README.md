# Moving data keys to another key in batches (#176)

Reproduce from the workspace root:

```sh
cargo test --release -p skilj --test master_key_rotation move_measurement -- --ignored --nocapture
```

The experiment is `move_measurement` in `skilj/tests/master_key_rotation.rs`.
It moves 1,000 data keys, five rewrap batches of 200, from the default Vault
key to a bounded context's own key with `db::rewrap_data_keys`, the function
the background task runs after `assignMasterKey`. The provider is
`VaultTransitProvider` against the test file's in-process imitation of
Vault's Transit API, which waits a set time before answering each `encrypt`
or `decrypt` request. The database is the test's own Postgres, on the same
machine.

Each latency is run three ways:

- **row by row (before)**: the batch operations call the single `unwrap` and
  `wrap` one after the other, which is what the rewrap did before this change.
- **trait defaults**: the batch operations as `KeyProvider`'s default
  implementations have them, single calls with at most eight in flight. This
  is what a provider without batch operations of its own gets.
- **Vault batches**: `VaultTransitProvider`'s own, one Transit `decrypt` and
  one `encrypt` request per rewrap batch.

A second pool samples, every 200 µs or so, how many connections the
rewrap's pool has out and the age of the oldest open transaction in the
database. RSS is the whole test process, read from `/proc/self/status`
after each run; the runs share one process, so it only grows.

## Observations

One release run ([run.log](run.log), compiler in [rustc.txt](rustc.txt)):

| Request latency | Asked | Requests | Elapsed | Per data key | Connections in use, most | Longest transaction seen | RSS after |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 ms | row by row (before) | 2,000 | 2,742 ms | 2.742 ms | 1 | 5.6 ms | 35,216 KiB |
| 0 ms | trait defaults | 2,000 | 407 ms | 0.407 ms | 1 | 5.8 ms | 38,008 KiB |
| 0 ms | Vault batches | 10 | 43 ms | 0.043 ms | 1 | 4.4 ms | 41,428 KiB |
| 1 ms | row by row (before) | 2,000 | 5,188 ms | 5.188 ms | 1 | 5.2 ms | 42,640 KiB |
| 1 ms | trait defaults | 2,000 | 719 ms | 0.719 ms | 1 | 4.9 ms | 43,168 KiB |
| 1 ms | Vault batches | 10 | 55 ms | 0.055 ms | 1 | 5.0 ms | 44,832 KiB |
| 5 ms | row by row (before) | 2,000 | 13,140 ms | 13.140 ms | 1 | 4.5 ms | 44,844 KiB |
| 5 ms | trait defaults | 2,000 | 1,693 ms | 1.693 ms | 1 | 5.6 ms | 45,176 KiB |
| 5 ms | Vault batches | 10 | 119 ms | 0.119 ms | 1 | 8.4 ms | 45,828 KiB |
| 20 ms | row by row (before) | 2,000 | 43,032 ms | 43.032 ms | 1 | 7.8 ms | 46,660 KiB |
| 20 ms | trait defaults | 2,000 | 5,443 ms | 5.443 ms | 1 | 5.0 ms | 47,580 KiB |
| 20 ms | Vault batches | 10 | 250 ms | 0.250 ms | 1 | 4.9 ms | 47,604 KiB |

- **Requests.** Two per data key before; two per rewrap batch with Vault's
  batch operations, 200 times fewer. The trait defaults make the same
  requests as before and only overlap them.
- **Elapsed.** With Vault's batches the move takes 63 to 172 times less time
  than row by row, more the slower the provider. The trait defaults take about
  7 to 8 times less, which is their eight calls in flight. The 0 ms rows show
  that a local HTTP round trip already costs over a millisecond here, so the
  set latency comes on top of that.
- **Connections and transactions.** One connection at a time, and no
  transaction older than 9 ms, however the provider is asked and however
  slow it is: the provider is asked between a batch's read and its
  transaction, as before, and holds neither.
- **Memory.** A batch holds its 200 rows, their unwrapped data keys (32
  bytes each) and the new wraps until its transaction commits, whichever way
  the provider is asked. The process's RSS grows by about 12 MiB over the
  twelve runs, which include the imitation's request log and 12,000 seeded
  rows; the three ways don't differ by more than the order they ran in.

## Limits

One run on one machine, against an imitation that waits instead of doing
Vault's work: it shows what the request count does to the wait, and no
real Vault's throughput. The container suite
(`skilj/tests/vault_transit_container.rs`) checks the batch operations'
answers against real Vault and OpenBao, without timing them. A rotation's
rewrap of a key's own data keys already used Transit's batch `rewrap` and
is not changed or measured here.
