A wallet gets an owner.

Add a `RegisterOwner` command with the payload `wallet_id`, `owner_id` and
`email`, recording an `OwnerRegistered` event with the same three fields
(the `WalletEvent` variant is `OwnerRegistered`, the payload structs are
`RegisterOwnerPayload` and `OwnerRegisteredPayload`).

- A wallet has one owner: registering an owner for a wallet that has one
  is rejected with kind `owner_already_registered`.
- The email address is personal data. It must not be stored readable, and
  when an owner asks to be forgotten it must become unreadable everywhere,
  without touching the wallet's deposits and withdrawals.
- `RegisterOwner` can be triggered over REST, like the other commands, the
  server prints a token for it, and registering an owner works against the
  running server.

Add tests, and make sure the project's checks pass.
