//! A real, runnable server for the demo bounded contexts in
//! `skilj_demo::{banking, courses}` - `cargo run -p skilj-demo --bin
//! server`. Not a test: this boots an actual `axum` process serving both
//! REST and GraphQL, prints `CommandToken` credentials for every
//! `rest_trigger_allowed` command, and then serves until killed. Point
//! `curl` at the printed examples, or a GraphQL client at `/graphql`.
//!
//! Needs `DATABASE_URL` pointing at a real Postgres (`PORT` optionally
//! overrides the default `8080`). Every run is safe to repeat against the
//! same database: bounded contexts are only created if they don't exist
//! yet, and each run mints its own fresh admin `Role` and `CommandToken`s
//! rather than reusing a previous run's.
//!
//! **The bootstrap below is a shortcut, not the intended production
//! flow.** It seeds a `Role`/`RoleAccessMapping` directly via
//! `skilj_core::db`, the same way every end-to-end test in this
//! repository does (see e.g. `skilj/tests/command_trigger.rs`'s own
//! `setup()`) - convenient for a self-contained demo binary, but a real
//! deployment doesn't have code that writes to `roles`/
//! `role_access_mappings` directly. There, a human claims the
//! once-only bootstrap secret `Skilj::builder(...).build()` prints
//! (`ClosesPermanentlyOnFirstClaim`) to create the first superadmin, and
//! everything past that - creating bounded contexts, granting access -
//! happens over the GraphQL admin console (docs/architecture.md §6,
//! `entity AccessManagement`).

use chrono::Utc;
use skilj::Skilj;
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};

/// `(bounded_context, command_type_name)` for every command this demo
/// wants a REST `CommandToken` printed for - kept as one list so main()
/// doesn't have to hand-enumerate both bounded contexts' commands twice.
const COMMAND_TYPES: &[(&str, &str)] = &[
    (skilj_demo::banking::BOUNDED_CONTEXT, "DepositMoney"),
    (skilj_demo::banking::BOUNDED_CONTEXT, "WithdrawMoney"),
    (skilj_demo::courses::BOUNDED_CONTEXT, "OpenCourse"),
    (skilj_demo::courses::BOUNDED_CONTEXT, "EnrollStudentInCourse"),
    (skilj_demo::courses::BOUNDED_CONTEXT, "DropCourse"),
];

const BOUNDED_CONTEXTS: &[&str] = &[
    skilj_demo::banking::BOUNDED_CONTEXT,
    skilj_demo::courses::BOUNDED_CONTEXT,
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL").expect(
        "DATABASE_URL must be set, e.g. postgres://user:pass@localhost:5432/skilj_demo",
    );
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let pool = db::connect(&database_url).await?;
    db::migrate(&pool).await?;

    for name in BOUNDED_CONTEXTS {
        if db::get_bounded_context(&pool, name).await?.is_none() {
            db::insert_bounded_context(
                &pool,
                &BoundedContext {
                    name: (*name).to_string(),
                    status: BoundedContextStatus::Active,
                    created_at: Utc::now(),
                    created_by: ContextCreator::SystemCreator,
                },
            )
            .await?;
            println!("created bounded context {name:?}");
        }
    }

    let external_subject = format!("skilj-demo-admin-{}", generate_token_id());
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.clone(),
        name: "skilj-demo admin".into(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: Utc::now(),
        revoked_at: None,
    };
    db::insert_role(&pool, &role).await?;

    let mut mappings = Vec::with_capacity(BOUNDED_CONTEXTS.len());
    for name in BOUNDED_CONTEXTS {
        let bc = db::get_bounded_context(&pool, name)
            .await?
            .expect("just ensured it exists above");
        let mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: bc,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            status: RoleStatus::Active,
            created_at: Utc::now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping).await?;
        mappings.push(mapping);
    }

    let (skilj, report) = skilj_demo::register(Skilj::builder(database_url))
        .reconciliation_role(external_subject)
        .build()
        .await?;
    println!("reconciliation: registered {:?}", report.registered);
    if !report.skipped_no_access.is_empty() {
        println!(
            "reconciliation: skipped, no access yet: {:?}",
            report.skipped_no_access
        );
    }

    println!("\ncommand tokens (send as `authorization: Bearer <id>.<secret>`):");
    for (bounded_context, command_type_name) in COMMAND_TYPES {
        let mapping = mappings
            .iter()
            .find(|m| m.bounded_context.name == *bounded_context)
            .expect("a mapping was inserted above for every bounded context in BOUNDED_CONTEXTS");
        let command_type = db::get_command_type(&pool, bounded_context, command_type_name)
            .await?
            .unwrap_or_else(|| {
                panic!(
                    "{bounded_context}/{command_type_name} should have just been registered by \
                     skilj_demo::register()"
                )
            });
        let token = access_control::create_command_token(
            mapping,
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
    println!("\nskilj-demo listening on http://localhost:{port} (REST under /v1/..., GraphQL at /graphql)");
    println!("example - deposit into account \"a1\" (banking):");
    println!(
        "  curl -H 'authorization: Bearer <DepositMoney token>' -H 'content-type: application/json' \\\n\
         \x20      -d '{{\"payload\":{{\"account_id\":\"a1\",\"amount\":100}}}}' \\\n\
         \x20      http://localhost:{port}/v1/commands/trigger"
    );
    axum::serve(listener, app).await?;
    Ok(())
}
