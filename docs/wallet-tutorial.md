# Build and test a wallet

This tutorial follows the [wallet template](../templates/skilj-template).
The complete domain and server are supplied, so you can run the application
first and then change a rule. You need Rust 1.95 or later. Local mode
downloads Postgres on first use; you can also supply your own server.

## Generate and test

```sh
cargo install cargo-generate
cargo generate --git https://github.com/gklijs/SkilJ.git templates/skilj-template --name my-wallet
cd my-wallet
cargo test --no-default-features --test wallet_fixture
```

These commands generate from the repository's default branch. For a pinned
release, add `--tag <release-tag>` to `cargo generate` and read the guides
at the same tag. Generated dependencies are declared in `Cargo.toml`.

The tests need no running Postgres or Docker. The first run compiles
dependencies. Subsequent runs are the starting point for changes to wallet
rules; the [test source](../templates/skilj-template/tests/wallet_fixture.rs)
includes positive and nonpositive amounts, exact-balance withdrawals,
overdrafts and projection folds.

## Events and tags select the decision's history

Open `src/wallet.rs` ([source](../templates/skilj-template/src/wallet.rs)).
`DepositedPayload` and `WithdrawnPayload` each have a `wallet_id: String`
and `amount: i64`. Their `EventType` implementations derive JSON schemas
from the payloads and use this shared tag mapping, extracted from that file:

```rust
fn wallet_tag() -> Vec<TagMapping> {
    vec![TagMapping::new("wallet", "wallet_id")]
}
```

`wallet` is the tag key; `wallet_id` is the payload field supplying its
value. Both commands use the same mapping. A withdrawal for `w1` therefore
sees the deposits and withdrawals tagged `wallet: w1`; activity for `w2`
does not enter its decision. The context's `WalletEvent` enum and
`BoundedContextEvent` implementation convert stored events into the typed
history passed to the domain functions.

`#[auto_register(BOUNDED_CONTEXT)]` marks the types for registration.
`src/lib.rs` exposes the module and calls `builder.auto_register()`;
the server calls that library's `register()` function. Follow the same
pattern when adding a type or another context module.

## Decide without I/O

`Deposit::decide` rejects amounts at or below zero with `invalid_amount`;
otherwise it returns a `Deposited` event. `Withdraw::decide` also checks
the amount, then folds the supplied history with `balance_of`. If the
requested amount exceeds that balance, it returns `insufficient_funds`;
otherwise it emits `Withdrawn`.

A new wallet has no history and balance zero. Its first deposit creates
its balance; no separate open command is required. Keep `decide()`
synchronous and free of external calls: it consumes a payload and typed
history, then returns `CommandDecision`. SkilJ persists the decision and
events and checks for conflicting changes to the selected history.

## Project a read model

`Balance` consumes both event types. Its `keys()` returns the wallet ID,
and `project()` adds a deposit or subtracts a withdrawal from `BalanceState`.
It declares `sync() == true`, so the stored projection reflects an accepted
command when the command finishes. The decision still checks its selected
event history; it does not query this read model for authorization or funds.

The projection fixture supplies typed events and asserts the final balance.
It exercises the fold, while key routing and storage need integration tests.

## Start the server and authorize a command

```sh
cargo run --bin server
```

Without `DATABASE_URL`, the default `local` feature starts embedded Postgres
and keeps its data between runs. To use a disposable database of your own:

```sh
export DATABASE_URL=postgres://user:pass@localhost:5432/my_wallet
cargo run --bin server
```

The [server source](../templates/skilj-template/src/bin/server.rs) seeds a
development admin and creates a separate `CommandToken` for `Deposit` and
`Withdraw`. Each command explicitly enables `rest_trigger_allowed()`.
The token selects and authorizes that command; the request supplies only
its payload. Copy the server's printed `export DEPOSIT=... WITHDRAW=...`
line into another terminal, then run:

```sh
curl localhost:8080/v1/commands/trigger -H "authorization: Bearer $DEPOSIT" \
     -H 'content-type: application/json' -d '{"payload":{"wallet_id":"w1","amount":100}}'
curl localhost:8080/v1/commands/trigger -H "authorization: Bearer $WITHDRAW" \
     -H 'content-type: application/json' -d '{"payload":{"wallet_id":"w1","amount":500}}'
```

For a fresh `w1`, the first response has `accepted: true`; the second has
`rejectionKind: "insufficient_funds"`. Persistent data affects later runs,
so use a new wallet ID when repeating this scenario. Ctrl-C drains HTTP
requests and shuts down SkilJ's background work and local database.

The scaffold's direct admin seeding is a development shortcut. GraphQL is
mounted at `/graphql`, but Role authentication needs an identity provider;
the printed REST tokens do not set that up. See [authorization](authorization.md)
and [deployment](deployment.md) before adapting the server for production.

## Verify the right guarantee

Pure fixtures call `decide()` and `project()` with histories you supply.
They verify domain rules without a database; they assume the history was
selected correctly. They do not verify tags, projection key routing,
persistence, authorization or concurrent conflict detection.

In the SkilJ repository, `bash scripts/check-template.sh` generates this
template, runs its pure tests, builds both feature modes and checks the
HTTP deposit/overdraft scenario with real Postgres. The
[banking integration tests](../skilj-demo/tests/banking.rs) demonstrate
database-backed isolation and persistence checks for a similar domain.
Follow [the courses guide](courses-dcb.md) for a boundary covering two
entities and a concurrent conflict test. The complete coverage limits are
in [CONTRIBUTING](../CONTRIBUTING.md#what-the-tests-do-and-dont-verify).
