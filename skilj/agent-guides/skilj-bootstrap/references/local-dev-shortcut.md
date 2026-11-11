# The dev/test shortcut

`skilj::dev::seed_admin` creates the bounded contexts that don't exist
yet, and a new `Role` with Admin access to each of them, straight in the
database. It is what `skilj-demo/src/bin/server.rs` does, and
`src/bin/server.rs` of a project generated from skilj-template.

```rust
use skilj::Skilj;

// 1. The bounded contexts, and a Role with Admin access to them. Each
//    call makes a new Role with a random external subject, so it is safe
//    on every start against the same database.
let admin = skilj::dev::seed_admin(&database_url, "banking admin", &["banking"]).await?;

// 2. Reconcile as that Role: it has the grant, so the types register.
let (skilj, report) = Skilj::builder(database_url)
    .auto_register()
    .reconciliation_role(admin.external_subject())
    .build()
    .await?;
assert!(report.skipped_no_access.is_empty());

// 3. Optional: a REST token per command type. After build(), since the
//    command type has to be registered.
let token = admin.create_command_token("banking", "DepositMoney").await?;
println!("send as `authorization: Bearer {token}`");
admin.close().await;
```

A bounded context added later goes in the list `seed_admin` is given:
one it wasn't given has no grant, and shows up in
`report.skipped_no_access`.

`create_command_token` answers `Error::Database(sqlx::Error::RowNotFound)`
for a command type that isn't registered, or a bounded context that
wasn't in the list.

Don't write the `roles` and `role_access_mappings` rows yourself:
`skilj_core::db` is hidden from the documentation and not a stable API.
skilj's own tests do (`skilj/tests/command_trigger.rs`'s `setup()`), and
change with it.

## Why this is never the production answer

A real deployment doesn't ship code that creates a Role and its grant -
`entity AccessManagement`'s whole point is that every grant traces back
to a superadmin's own deliberate action over GraphQL, auditable and
revocable the same way. A grant written this way is also not in the
access trail until the next start baselines it. The shortcut is fine for
a demo binary, a local run or a test's own `setup()` precisely *because*
nothing there needs that audit trail to mean anything - see
[production-flow.md](production-flow.md) for the real sequence.

## No `identity_provider` needed for this path alone

If the only thing exercising `skilj` is REST (`CommandToken`/
`EventReadToken`/etc. - own `id.secret` credentials, no JWT involved),
skipping `.identity_provider(...)` entirely is fine. The moment
anything needs to authenticate as the Role over GraphQL, a real IdP - or
a local JWKS stand-in signing with a well-known test keypair, the way
`skilj-demo/src/bin/server.rs`'s own `serve_local_jwks`/`sign_jwt` do
- is needed, with `admin.external_subject()` as the `sub` claim the
signed JWT carries.
