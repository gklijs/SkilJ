Wallets can be closed.

Add a `CloseWallet` command (payload: `wallet_id`) that records a
`WalletClosed` event (payload: `wallet_id`; the `WalletEvent` variant is
`WalletClosed`, its payload struct `WalletClosedPayload`, the command's
`CloseWalletPayload`).

- A wallet whose balance is zero can be closed, also one that never had a
  deposit.
- Closing a wallet that still holds money is rejected with kind
  `wallet_not_empty`.
- Closing a closed wallet is rejected with kind `wallet_closed`, and so
  are a `Deposit` into and a `Withdraw` from a closed wallet.
- `CloseWallet` can be triggered over REST, like the other two, and the
  server prints a token for it.

Add tests for the new rules, and make sure the project's checks pass.
