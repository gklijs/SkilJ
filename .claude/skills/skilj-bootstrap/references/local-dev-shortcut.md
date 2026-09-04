# The dev/test shortcut

Real example: `skilj-demo/src/bin/server.rs` (`cargo run -p skilj-demo
--bin server`) - its own module doc comment is explicit that this is
"a shortcut, not the intended production flow." Every end-to-end test
in this workspace uses the identical pattern (e.g.
`skilj/tests/command_trigger.rs`'s own `setup()`).

Write the rows directly via `skilj_core::db`, skipping the GraphQL
admin-console dance entirely:

```rust
use chrono::Utc;
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::generate_token_id;

let pool = db::connect(&database_url).await?;
db::migrate(&pool).await?;

// 1. The bounded context, only if it doesn't already exist - safe to
//    run this on every startup against the same database.
if db::get_bounded_context(&pool, "banking").await?.is_none() {
    db::insert_bounded_context(&pool, &BoundedContext {
        name: "banking".to_string(),
        status: BoundedContextStatus::Active,
        created_at: Utc::now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    }).await?;
}

// 2. A Role for the reconciliation loop to authenticate as.
let external_subject = format!("demo-admin-{}", generate_token_id());
let role = Role {
    id: generate_token_id(),
    external_subject: external_subject.clone(),
    name: "demo admin".into(),
    superadmin: false,
    status: RoleStatus::Active,
    created_at: Utc::now(),
    revoked_at: None,
};
db::insert_role(&pool, &role).await?;

// 3. Admin access on the bounded context - the same grant step 4 of
//    the production flow performs over GraphQL, done here as a plain
//    insert instead.
let bc = db::get_bounded_context(&pool, "banking").await?.unwrap();
let mapping = RoleAccessMapping {
    role: role.clone(),
    bounded_context: bc,
    level: AccessLevel::Admin,
    can_read_sensitive: false,
    scope: None,
    status: RoleStatus::Active,
    created_at: Utc::now(),
    revoked_at: None,
};
db::insert_role_access_mapping(&pool, &mapping).await?;

// 4. Now .reconciliation_role(external_subject) will actually find
//    Admin access waiting for it.
let (skilj, report) = Skilj::builder(database_url)
    .bounded_context("banking")
    .auto_register()
    .reconciliation_role(external_subject)
    .build()
    .await?;
```

## Why this is never the production answer

A real deployment doesn't ship code that writes to `roles`/
`role_access_mappings` directly - `entity AccessManagement`'s whole
point is that every grant traces back to a superadmin's own deliberate
action over GraphQL, auditable and revocable the same way. This
shortcut is fine for a demo binary or a test's own `setup()` precisely
*because* nothing there needs that audit trail to mean anything - see
[production-flow.md](production-flow.md) for the real sequence.

## No `identity_provider` needed for this path alone

If the only thing exercising `skilj` is REST (`CommandToken`/
`EventReadToken`/etc. - own `id.secret` credentials, no JWT involved),
skipping `.identity_provider(...)` entirely is fine. The moment
anything needs to authenticate as `role` over GraphQL, a real IdP - or
a local JWKS stand-in signing with a well-known test keypair, the way
`skilj-demo/src/bin/server.rs`'s own `serve_local_jwks`/`sign_jwt` do
- is needed, matching `external_subject` above to whatever `sub` claim
the signed JWT carries.
