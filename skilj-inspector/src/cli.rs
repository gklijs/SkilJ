//! v1 config: one required arg, the database to connect to - no IdP
//! config, no bearer token, since raw Postgres access has no
//! `Role`/`RoleAccessMapping` layer to check against at all. That's the
//! point of this tool, not an oversight (see this crate's own doc
//! comment).

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "skilj-inspector",
    about = "A read-only Postgres console for a skilj deployment - for when skilj-graphql itself isn't running"
)]
pub struct Args {
    /// The Postgres connection string skilj's own server is configured
    /// with, e.g. postgres://user:pass@host/db.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,
}
