# {{project-name}}

Scaffolded from [skilj-template](https://codeberg.org/gklijs/SklilJ/src/branch/main/templates/skilj-template) -
a minimal, working [skilj](https://codeberg.org/gklijs/SklilJ) app with
one worked bounded context, `wallet` (deposit/withdraw, one `Balance`
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
DOCKER_BUILDKIT=1 docker build --ssh default -t {{project-name}} .
docker run --env DATABASE_URL=postgres://user:pass@host:5432/{{crate_name}} -p 8080:8080 {{project-name}}
```

`--ssh default` forwards your own running ssh-agent into the build - the
image still needs to fetch `skilj`/`skilj-core` from their private git
repository (see the note below), and BuildKit's SSH forwarding is how it
does that without a key ever landing in a layer. Requires an ssh-agent
with access to that repository actually running and reachable via
`SSH_AUTH_SOCK` on the host doing the build.

The image is `FROM scratch` - no shell, no package manager, nothing
besides the binary and a CA bundle. That means `docker exec ... sh` (or
any interactive debugging inside the container) won't work; use `docker
logs` instead.

## Where to go from here

- `src/wallet.rs` - the `EventType`/`CommandType`/`Projection` trio.
  Rename/replace it with your own domain, or add more bounded contexts
  as their own modules next to it (see `src/lib.rs`'s own doc comment).
- `src/bin/server.rs` - the bootstrap shown here is a shortcut (seeds a
  `Role` directly), not the intended production flow. See the skilj
  repository's own `docs/architecture.md` §5/§6 for the real bootstrap
  secret / superadmin / GraphQL admin console flow, and for wiring a
  real `identity_provider` so GraphQL's Role-based auth works (this
  scaffold only sets up REST command tokens).
- This project currently depends on `skilj`/`skilj-core` via git, since
  neither is published to crates.io yet - check the skilj repository's
  own `docs/open-source-todo.md` for when that changes.
