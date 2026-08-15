//! sqlx queries and migrations - persistence for every module in this
//! crate. See docs/architecture.md §2.2 for why `sqlx`, and §2.2.1 for
//! why every `Integer`-typed column is `BIGINT`, not `INT`.

pub async fn connect(database_url: &str) -> Result<sqlx::PgPool, sqlx::Error> {
    sqlx::PgPool::connect(database_url).await
}

// TODO: `sqlx::migrate!` embedding this crate's own migrations (so a
// consuming application's migration runner picks them up - the same
// "library owns its own bootstrap" pattern as the superadmin/admin-context
// work), and per-entity persistence functions as each domain module's
// entities are implemented.
