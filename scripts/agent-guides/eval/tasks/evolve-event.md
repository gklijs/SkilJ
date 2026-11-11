A deposit can say what it was for.

Add an optional free-text `reference` to the `Deposit` command, and record
it on the `Deposited` event.

This application is in production: wallets have deposits stored without a
reference, and clients send `Deposit` commands without one. Both have to
keep working, and the new version has to start against the existing
database.

Add tests, and make sure the project's checks pass.
