# {{project-name}}

A working [skilj](https://codeberg.org/gklijs/SkilJ) app, scaffolded from
[skilj-template](https://codeberg.org/gklijs/SkilJ/src/branch/main/templates/skilj-template).
`skilj` is a Rust library for building event-sourced applications backed by Postgres, with
GraphQL and REST surfaces built in - see its own README for what that means and why.

This project has one worked bounded context, `wallet` (deposit/withdraw, one `Balance`
projection - see `src/wallet.rs`).

## Run it

```sh
export DATABASE_URL=postgres://user:pass@localhost:5432/{{crate_name}}
cargo run --bin server
```

Prints a `CommandToken` for each of `Deposit`/`Withdraw` - use one to
trigger a command:

```sh
curl -H 'authorization: Bearer <id>.<secret>' -H 'content-type: application/json' \
     -d '{"payload":{"wallet_id":"w1","amount":100}}' \
     http://localhost:8080/v1/commands/trigger
```

## Docker

```sh
docker build -t {{project-name}} .
docker run --env DATABASE_URL=postgres://user:pass@host:5432/{{crate_name}} -p 8080:8080 {{project-name}}
```

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
  [`docs/architecture.md` §5/§6](https://codeberg.org/gklijs/SkilJ/src/branch/main/docs/architecture.md)
  in the skilj repository for the real bootstrap secret / superadmin /
  GraphQL admin console flow, and for wiring a real `identity_provider`
  so GraphQL's Role-based auth works (this scaffold only sets up REST
  command tokens).
