# The real production flow

Everything past the very first step happens over GraphQL, as a
superadmin - never as application code writing to the database
directly (that's [the dev/test shortcut](local-dev-shortcut.md)
instead).

## 1. Claim the once-only bootstrap secret

`Skilj::builder(database_url).build()` generates and prints a
bootstrap secret the first time it ever runs against a fresh database
(`bootstrap_secret`, `ClosesPermanentlyOnFirstClaim`) - it's gone for
good the moment a superadmin is created, not reprintable, not
recoverable. Whoever is going to administer this deployment claims it:

```graphql
mutation {
  createSuperadmin(
    bootstrapSecret: "<the printed secret>"
    name: "platform admin"
    externalSubject: "admin@example.com" # must match this caller's own IdP-verified subject
  ) {
    id
  }
}
```

`createSuperadmin` is the one mutation with no Role-based actor behind
it at all - it's how the very first Role ever gets created. Every
mutation from here on needs a bearer JWT that verifies (via
`.identity_provider(...)`) to a Role with `superadmin: true`.

## 2. Create the bounded context

```graphql
mutation {
  addBoundedContext(name: "banking") {
    name
    status
  }
}
```

`Superadmin`-gated. The row now exists, `status: ACTIVE` - and nothing
can read or write to it yet. This is deliberate, not a bug to work
around: a freshly created bounded context grants nobody access on
purpose (`docs/architecture.md` §1.5).

## 3. Create the Role your process will reconcile as

```graphql
mutation {
  createRole(
    name: "banking-service"
    superadmin: false
    externalSubject: "banking-service@example.com"
  ) {
    id
  }
}
```

`externalSubject` is what `.reconciliation_role(...)` names later -
pick something stable and identifiable (a service account identifier,
not a human's own subject, if this Role's only job is registering
types on startup).

## 4. Grant that Role `Admin` access on the bounded context

```graphql
mutation {
  grantRoleAccessMapping(
    roleId: "<the Role id from step 3>"
    boundedContext: "banking"
    level: ADMIN
    canReadSensitive: false
    scope: null
  ) {
    role { id }
    boundedContext { name }
    level
  }
}
```

`level: ADMIN` specifically - `RegisterEventType`/`RegisterCommandType`/
`RegisterProjection` all require it; `READ`/`WRITE` are not enough,
and reconciliation will silently skip this bounded context
(`ReconciliationReport.skipped_no_access`) rather than error if this
step is missed or the wrong level was granted.

## 5. Point your process at that Role

```rust
let (skilj, report) = Skilj::builder(database_url)
    .bounded_context("banking")
    .auto_register()
    .reconciliation_role("banking-service@example.com") // step 3's externalSubject
    .identity_provider(IdpConfig::new(jwks_url, issuer, audience, SigningAlgorithm::Rs256))
    .build()
    .await?;

assert!(report.skipped_no_access.is_empty());
```

If `skipped_no_access` isn't empty, re-check step 4 before anything
else - see this skill's own top-level "one thing every mistake comes
back to" note.

## Granting other Roles read/write access

The same `grantRoleAccessMapping` mutation, with `level: READ` or
`level: WRITE` instead, is also how every other Role - a human admin,
a service, a customer-facing backend - gets its own access to a
bounded context once it exists. `scope` (optional) restricts a grant
to one tenant's own data, for a bounded context that declares an
`owner_tag_key` - leave it `null` for an unscoped grant.
