Every new wallet is onboarded by a Temporal workflow.

Wallets are opened now: add an `OpenWallet` command (payload `wallet_id`,
`owner_id`) that records a `WalletOpened` event with the same fields (the
`WalletEvent` variant is `WalletOpened`, the payload structs are
`OpenWalletPayload` and `WalletOpenedPayload`). Opening a wallet that is
open already is rejected with kind `wallet_already_open`. `OpenWallet` can
be triggered over REST, and the server prints a token for it.

Each opened wallet starts a Temporal workflow of type `WalletOnboarding`
on the task queue `wallet-onboarding`, correlated by the wallet's tag:

- In `src/onboarding.rs`, add
  `pub fn onboarding_mapping(credential: String) -> skilj_temporal::EventTypeMapping`
  that says so for `WalletOpened`.
- When the environment variable `TEMPORAL_ADDRESS` is set (`host:port`
  of the Temporal frontend, e.g. `127.0.0.1:7233`, namespace `default`),
  the server starts the workflows while it runs; without it, it runs as
  before. Wallets opened while it ran without Temporal are not onboarded
  later.
- The workers that run `WalletOnboarding` are another team's; nothing in
  this project executes the workflow.

Add tests, and make sure the project's checks pass.
