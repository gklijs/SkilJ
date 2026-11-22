A wallet gets its monthly statements.

Add an `AttachStatement` command with the payload `wallet_id`, `owner_id`,
`month` (text such as `2026-10`) and `document`, the statement as a PDF,
recording a `StatementAttached` event with the same four fields (the
`WalletEvent` variant is `StatementAttached`, the payload structs are
`AttachStatementPayload` and `StatementAttachedPayload`).

- A statement is a PDF of up to a few megabytes; it is uploaded first and
  then attached with the command.
- A wallet has one statement per month: attaching a second one for the
  same month is rejected with kind `statement_already_attached`.
- The statement is the owner's personal data. It must not be stored
  readable, and when the owner (subject key `owner`, identified by
  `owner_id`) asks to be forgotten it must become unreadable everywhere,
  without touching the wallet's deposits and withdrawals.
- `AttachStatement` can be triggered over REST, like the other commands,
  the server prints a token for it, and that token can upload a statement
  to the running server.

Add tests, and make sure the project's checks pass.
