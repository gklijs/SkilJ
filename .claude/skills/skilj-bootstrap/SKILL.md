---
name: skilj-bootstrap
description: >
  Create a brand-new skilj bounded context and get it to the point
  where a process's own reconciliation loop can actually register
  EventType/CommandType/Projection types onto it - superadmin bootstrap,
  addBoundedContext, granting the reconciliation Role Admin access, and
  the #1 gotcha (a bounded context with no grant is silently skipped,
  not an error). Triggers on "create a new bounded context", "set up
  skilj from scratch", "bootstrap the superadmin", "why is my
  bounded context/type not being registered", or "ReconciliationReport
  shows skipped_no_access". Do NOT use for adding a type to a bounded
  context that already has admin access sorted out - that's the `skilj`
  skill. Do NOT use for the GraphQL/REST wire contract itself, IdP login
  UI, or skilj-tui - those are skilj's own implementation, not this
  bootstrapping sequence.
---

# skilj-bootstrap

A freshly created `BoundedContext` is inert on purpose - `AddBoundedContext`
grants nobody access to it, deliberately, not an oversight (see
`docs/architecture.md` §1.5). Getting from "nothing exists yet" to "my
process's own `.reconciliation_role(...)` can register types onto it"
needs a handful of steps in a specific order, over a surface the
`skilj` skill doesn't cover at all.

## The one thing every mistake here comes back to

**Reconciliation never bypasses access control.** `.reconciliation_role(external_subject)`
names a real, pre-existing `Role`, looked up directly at startup (no
JWT - this is an internal process, not a request). It then calls
`RegisterEventType`/`RegisterCommandType`/`RegisterProjection` exactly
as if that Role's own `RoleAccessMapping` were being used by a human
admin over GraphQL - because it is the same rule, the same grant, just
automated. **A bounded context that Role has no `Admin`-level grant on
is silently skipped for that pass, not an error** - it shows up in
`ReconciliationReport.skipped_no_access`, and `.build()` still succeeds.
If your types "aren't registering" and nothing looks wrong in your
`EventType`/`CommandType` code, check that list first, before anything
else.

## Two paths - pick based on what you're building

- **A real deployment** → [references/production-flow.md](references/production-flow.md):
  claim the once-only bootstrap secret to create a superadmin, then
  everything else happens over the GraphQL admin console
  (`addBoundedContext`, `createRole`, `grantRoleAccessMapping`) - a
  human or an automated setup script acting as that superadmin, not
  application code.
- **A demo, a test, or local dev** → [references/local-dev-shortcut.md](references/local-dev-shortcut.md):
  write the `BoundedContext`/`Role`/`RoleAccessMapping` rows directly
  via `skilj_core::db`, the same shortcut `skilj-demo/src/bin/server.rs`
  and every end-to-end test in this workspace already use. Convenient,
  not the production flow - a real deployment doesn't have code that
  writes to `roles`/`role_access_mappings` directly.

Both paths end at the same place: a `Role` with an `Admin`-level
`RoleAccessMapping` on the bounded context, ready to be named in
`.reconciliation_role(that_role.external_subject)`.

## After the grant exists

```rust
let (skilj, report) = Skilj::builder(database_url)
    .bounded_context("banking")
    .auto_register() // or .event_type::<T>()/.command_type::<T>()/.projection::<T>() by hand
    .reconciliation_role(external_subject)
    .identity_provider(idp_config) // only if GraphQL Role-auth is needed - see below
    .build()
    .await?;

assert!(report.skipped_no_access.is_empty(), "check the grant actually landed");
```

This is where the `skilj` skill picks up - which trait to implement for
a new `EventType`/`CommandType`/`Projection`, `#[auto_register]`, and
what a rejected registration's error actually means.

## `.identity_provider(...)` is a separate axis, easy to conflate with the above

Granting a Role `Admin` access via `RoleAccessMapping` is *authorization*
- what that Role can do once identified. `.identity_provider(IdpConfig::new(jwks_url, issuer, algorithm))`
is *authentication* - how a GraphQL caller's bearer JWT gets verified
into a `Role` at all. Omitting it entirely still lets `.build()` and
`createSuperadmin` work (that one mutation has no Role-based actor
behind it), but no other GraphQL resolver can ever authenticate anyone
- there's no bearer JWT that would resolve to a verified subject
without a configured IdP to verify it against. REST tokens
(`CommandToken`/`EventReadToken`/etc.) are unaffected either way - they
carry their own `id.secret` credential, not a JWT.
