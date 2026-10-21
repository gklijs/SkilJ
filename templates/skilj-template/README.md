# {{project-name}}

A working [skilj](https://github.com/gklijs/SkilJ) app, scaffolded from
[skilj-template](https://github.com/gklijs/SkilJ/blob/main/templates/skilj-template).
`skilj` is a Rust library for building event-sourced applications backed by Postgres, with
GraphQL and REST surfaces built in - see its own README for what that means and why.

This project has one worked bounded context, `wallet` (deposit/withdraw, one `Balance`
projection - see `src/wallet.rs`).

## Run it

```sh
cargo run --bin server
```

With no `DATABASE_URL` set, this starts an embedded Postgres of its own
(skilj's single-user local mode), downloading it on the first run and
keeping its data in your user data directory between runs. To use your
own Postgres instead:

```sh
export DATABASE_URL=postgres://user:pass@localhost:5432/{{crate_name}}
cargo run --bin server
```

It prints a `CommandToken` for each of `Deposit`/`Withdraw`, and
ready-to-paste commands to try them from another terminal:

```sh
export DEPOSIT=<id>.<secret> WITHDRAW=<id>.<secret>
curl localhost:8080/v1/commands/trigger -H "authorization: Bearer $DEPOSIT" \
     -H 'content-type: application/json' -d '{"payload":{"wallet_id":"w1","amount":100}}'
curl localhost:8080/v1/commands/trigger -H "authorization: Bearer $WITHDRAW" \
     -H 'content-type: application/json' -d '{"payload":{"wallet_id":"w1","amount":500}}'
```

The second one is rejected: the wallet holds 100, and `Withdraw` decides
against every `Deposited`/`Withdrawn` event of that wallet.

## Docker

```sh
docker build -t {{project-name}} .
docker run --env DATABASE_URL=postgres://user:pass@host:5432/{{crate_name}} -p 8080:8080 {{project-name}}
```

The image is built without the embedded-Postgres `local` feature, so it
always needs `DATABASE_URL`.

The image is `FROM scratch` - no shell, no package manager, nothing
besides the binary and a CA bundle. That means `docker exec ... sh` (or
any interactive debugging inside the container) won't work; use `docker
logs` instead.

## Where to go from here

- `src/wallet.rs` - the `EventType`/`CommandType`/`Projection` trio.
  Rename/replace it with your own domain, or add more bounded contexts
  as their own modules next to it (see `src/lib.rs`'s own doc comment).
- `src/bin/server.rs` - the bootstrap shown here is a shortcut (seeds a
  `Role` directly), not the intended production flow. See
  [`docs/architecture.md` §5/§6](https://github.com/gklijs/SkilJ/blob/main/docs/architecture.md)
  in the skilj repository for the real bootstrap secret / superadmin /
  GraphQL admin console flow, and for wiring a real `identity_provider`
  so GraphQL's Role-based auth works (this scaffold only sets up REST
  command tokens).
