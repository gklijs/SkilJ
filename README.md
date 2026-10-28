# SkilJ - clean-room rebuild from the specification

This branch (`spec-rebuild`) deliberately throws away the implementation and
regenerates one from `specs/skilj.allium` alone, to learn what the
specification does and doesn't pin down. Nothing outside `specs/` was read
while building it (the existing docs and code were consulted only
afterwards, for the comparison).

| crate          | what                                                                 |
|----------------|----------------------------------------------------------------------|
| `skilj-core`   | the model, the black boxes (schema, tags, filters, crypto) and every rule as a method on `Skilj` |
| `skilj-server` | GraphQL generated from registered state (JWT identity) and REST (AccessTokens) |
| `skilj-demo`   | a banking context served over both                                   |

Not rebuilt: anything the spec doesn't describe (deadlines, cross-context
routes, broker bridges, Temporal, codegen, TUI, inspector, federation
subgraph wire shape, master-key rotation, blind-key rotation).

## Running the tests

They need a Postgres server whose role can `CREATE DATABASE`; each test
creates its own database.

```sh
DATABASE_URL=postgres://postgres:postgres@localhost:5432 cargo test --workspace
```

## Choices the spec left open

Recorded where they are made, in doc comments; the main ones:

* decide() sees matching history with sensitive leaves decrypted (where the
  key still exists); project() sees them as stored.
* Private-field entitlement is decided per field, against that field's kind.
* A sensitive field whose subject field is absent is refused, not stored.
* decide()'s and scheduled payloads are validated before storage.
* Templated tenants are served by their template's compiled declaration.
* The event cache checks the sequence counter before answering from memory.
* Summaries (snapshot items) are folded on demand rather than persisted.
