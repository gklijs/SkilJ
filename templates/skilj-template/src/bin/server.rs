//! A real, runnable server for the `wallet` bounded context scaffolded
//! by skilj-template - `cargo run --bin server`. Boots an actual `axum`
//! process serving both REST and GraphQL, prints `CommandToken`
//! credentials for `Deposit`/`Withdraw`, and then serves until killed.
//!
//! Uses the Postgres `DATABASE_URL` points at. Without it, the default
//! `local` feature starts an embedded Postgres owned by this process, with
//! its data kept between runs (see `skilj::local` for where, and the
//! `SKILJ_LOCAL_*` settings). `PORT` optionally overrides the default
//! `8080`. Safe to re-run against the same
//! database: the bounded context is only created if it doesn't exist
//! yet, and each run mints its own fresh admin `Role` and
//! `CommandToken`s rather than reusing a previous run's.
//!
//! **The bootstrap below is a shortcut, not the intended production
//! flow** - `skilj::dev::seed_admin` writes an admin `Role` and its grant
//! straight into the database. A real deployment instead has a human
//! claim the once-only bootstrap secret `Skilj::builder(...).build()`
//! prints, to create the first superadmin, and everything past that
//! happens over the GraphQL admin console. See [docs/architecture.md §5](../../../../docs/architecture.md#graphql-wire-contract)/[§6](../../../../docs/architecture.md#idp-trust-configuration)
//! in the skilj repository for the full picture, including how to wire
//! a real `identity_provider` so GraphQL's Role-based auth (not just
//! the REST command tokens below) actually works.

use skilj::Skilj;

const COMMAND_TYPES: &[&str] = &["Deposit", "Withdraw"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "local")]
    let local_postgres = match std::env::var("DATABASE_URL") {
        Ok(_) => None,
        Err(_) => {
            let config = skilj::local::LocalConfig::from_env()?;
            let postgres = skilj::local::LocalPostgres::start(config).await?;
            println!(
                "no DATABASE_URL: started an embedded Postgres at {} (data in {})",
                postgres.display_url(),
                postgres.data_dir().display()
            );
            Some(postgres)
        }
    };
    #[cfg(feature = "local")]
    let database_url = match &local_postgres {
        Some(postgres) => postgres.database_url().to_string(),
        None => std::env::var("DATABASE_URL")?,
    };
    #[cfg(not(feature = "local"))]
    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set, e.g. postgres://user:pass@localhost:5432/wallet_demo");
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let bounded_context = {{crate_name}}::wallet::BOUNDED_CONTEXT;
    let admin =
        skilj::dev::seed_admin(&database_url, "{{crate_name}} admin", &[bounded_context]).await?;

    let (skilj, report) = {{crate_name}}::register(Skilj::builder(database_url))
        .reconciliation_role(admin.external_subject())
        .build()
        .await?;
    println!("reconciliation: registered {:?}", report.registered);
    if !report.skipped_no_access.is_empty() {
        println!("reconciliation: skipped, no access yet: {:?}", report.skipped_no_access);
    }

    println!("\ncommand tokens (send as `authorization: Bearer <id>.<secret>`):");
    let mut tokens = Vec::new();
    for command_type_name in COMMAND_TYPES {
        let token = admin
            .create_command_token(bounded_context, command_type_name)
            .await?;
        println!("  {bounded_context}/{command_type_name}: {token}");
        tokens.push(token);
    }
    admin.close().await;

    let rest = skilj.rest_router();
    let graphql = skilj.graphql_router().await?;
    let app = rest.merge(graphql);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    println!("\nlistening on http://localhost:{port} (REST under /v1/..., GraphQL at /graphql)");

    println!("\ntry it from another terminal - deposit 100 into wallet \"w1\", then withdraw more:");
    println!("  export DEPOSIT={} WITHDRAW={}", tokens[0], tokens[1]);
    for (variable, amount) in [("DEPOSIT", 100), ("WITHDRAW", 500)] {
        let payload = serde_json::json!({ "payload": { "wallet_id": "w1", "amount": amount } });
        println!(
            "  curl localhost:{port}/v1/commands/trigger -H \"authorization: Bearer ${variable}\" \
             -H 'content-type: application/json' -d '{payload}'"
        );
    }

    // On Ctrl-C or SIGTERM (what `docker stop` sends): stop taking
    // requests and finish the ones in flight, then let skilj's background
    // work finish what it's doing and close its connections.
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    let report = skilj.shutdown(std::time::Duration::from_secs(10)).await;
    if !report.aborted.is_empty() {
        println!("shutdown: aborted after the timeout: {:?}", report.aborted);
    }
    #[cfg(feature = "local")]
    if let Some(postgres) = local_postgres {
        postgres.shutdown().await?;
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to listen for Ctrl-C");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to listen for SIGTERM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
