A wallet gets a statement.

Add a projection `WalletStatement`, keyed by `wallet_id`, whose state
(`WalletStatementState`, with a field `entries: Vec<StatementEntry>`)
lists each deposit and withdrawal of the wallet in the order they
happened. A `StatementEntry` has `kind` (`"deposit"` or `"withdrawal"`),
`amount` (positive for both) and `balance`, the wallet's balance after it.

- Each wallet's statement holds only its own deposits and withdrawals.
- It is up to date as soon as a command is accepted, like `Balance`.
- `Balance` stays as it is.

Add tests, and make sure the project's checks pass.
