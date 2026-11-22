An email address belongs to one owner.

Add a `RegisterOwner` command with the payload `wallet_id`, `owner_id` and
`email`, recording an `OwnerRegistered` event with the same three fields
(the `WalletEvent` variant is `OwnerRegistered`, the payload structs are
`RegisterOwnerPayload` and `OwnerRegisteredPayload`).

- An email address belongs to one owner across all wallets: registering
  an owner with an address another registration already has, in any
  wallet, is rejected with kind `email_taken`. Addresses are compared as
  email addresses, so `Ada@Example.com` and `ada@example.com` are the
  same. The type that declares this rule is `OwnerEmail`.
- The email address is personal data. It must not be stored readable, and
  when an owner asks to be forgotten it must become unreadable everywhere,
  and the address free to register again, without touching the wallet's
  deposits and withdrawals.
- `RegisterOwner` can be triggered over REST, like the other commands, the
  server prints a token for it, and registering owners works against the
  running server.

Add tests, and make sure the project's checks pass.
