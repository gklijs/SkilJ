//! A real, runnable server for the `wallet` bounded context scaffolded
//! by skilj-template - `cargo run --bin server`. Boots an actual `axum`
//! process serving both REST and GraphQL, prints `CommandToken`
//! credentials for `Deposit`/`Withdraw`, and then serves until killed.
//!
//! Needs `DATABASE_URL` pointing at a real Postgres (`PORT` optionally
//! overrides the default `8080`). Safe to re-run against the same
//! database: the bounded context is only created if it doesn't exist
//! yet, and each run mints its own fresh admin `Role` and
//! `CommandToken`s rather than reusing a previous run's.
//!
//! **The bootstrap below is a shortcut, not the intended production
//! flow** - it seeds a `Role`/`RoleAccessMapping` directly, the way
//! skilj's own test suite and `skilj-demo` do. A real deployment
//! doesn't write to `roles`/`role_access_mappings` directly; instead a
//! human claims the once-only bootstrap secret `Skilj::builder(...).build()`
//! prints, to create the first superadmin, and everything past that
//! happens over the GraphQL admin console. See [docs/architecture.md §5](../../../../docs/architecture.md#graphql-wire-contract)/[§6](../../../../docs/architecture.md#idp-trust-configuration)
//! in the skilj repository for the full picture, including how to wire
//! a real `identity_provider` so GraphQL's Role-based auth (not just
//! the REST command tokens below) actually works.

use chrono::Utc;
use skilj::Skilj;
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};

const COMMAND_TYPES: &[&str] = &["Deposit", "Withdraw"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set, e.g. postgres://user:pass@localhost:5432/wallet_demo");
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let pool = db::connect(&database_url).await?;
    db::migrate(&pool).await?;

    let bounded_context = {{crate_name}}::wallet::BOUNDED_CONTEXT;
    if db::get_bounded_context(&pool, bounded_context).await?.is_none() {
        db::insert_bounded_context(
            &pool,
            &BoundedContext {
                name: bounded_context.to_string(),
                status: BoundedContextStatus::Active,
                created_at: Utc::now(),
                created_by: ContextCreator::SystemCreator,
                template: None,
            },
        )
        .await?;
        println!("created bounded context {bounded_context:?}");
    }

    let external_subject = format!("{{crate_name}}-admin-{}", generate_token_id());
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.clone(),
        name: "{{crate_name}} admin".into(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: Utc::now(),
        revoked_at: None,
    };
    db::insert_role(&pool, &role).await?;

    let bc = db::get_bounded_context(&pool, bounded_context)
        .await?
        .expect("just ensured it exists above");
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

    let (skilj, report) = {{crate_name}}::register(Skilj::builder(database_url))
        .reconciliation_role(external_subject)
        .build()
        .await?;
    println!("reconciliation: registered {:?}", report.registered);
    if !report.skipped_no_access.is_empty() {
        println!("reconciliation: skipped, no access yet: {:?}", report.skipped_no_access);
    }

    println!("\ncommand tokens (send as `authorization: Bearer <id>.<secret>`):");
    for command_type_name in COMMAND_TYPES {
        let command_type = db::get_command_type(&pool, bounded_context, command_type_name)
            .await?
            .unwrap_or_else(|| {
                panic!("{bounded_context}/{command_type_name} should have just been registered")
            });
        let token = access_control::create_command_token(
            &mapping,
            &command_type,
            generate_token_id(),
            generate_token_secret(),
            Utc::now(),
        )?;
        db::insert_command_token(&pool, &token).await?;
        println!("  {bounded_context}/{command_type_name}: {}.{}", token.id, token.secret);
    }

    let rest = skilj.rest_router();
    let graphql = skilj.graphql_router().await?;
    let app = rest.merge(graphql);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    println!("\nlistening on http://localhost:{port} (REST under /v1/..., GraphQL at /graphql)");

    let example_payload = serde_json::json!({ "payload": { "wallet_id": "w1", "amount": 100 } });
    println!("\nexample - deposit into wallet \"w1\":");
    println!("  curl -H 'authorization: Bearer <Deposit token>' -H 'content-type: application/json' \\");
    println!("       -d '{}' \\", example_payload);
    println!("       http://localhost:{port}/v1/commands/trigger");

    axum::serve(listener, app).await?;
    Ok(())
}
