//! v1 config: endpoint + token + bounded context, all supplied up front
//! (flag or env var), no in-app login flow - see this crate's own doc
//! comment on why it never talks to an IdP itself.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "skilj-tui",
    about = "A Ratatui operator console for a skilj deployment"
)]
pub struct Args {
    /// The skilj-graphql endpoint, e.g. http://localhost:8080/graphql.
    #[arg(long, env = "SKILJ_GRAPHQL_URL")]
    pub endpoint: reqwest::Url,

    /// A bearer JWT for a Role with (at least) Admin access to
    /// `--bounded-context` - obtained however this deployment's own IdP
    /// issues one. `skilj-demo`'s own server prints one ready to use.
    #[arg(long, env = "SKILJ_TOKEN")]
    pub token: String,

    /// The bounded context to operate on. Superadmin-only operations
    /// (the cross-context directory, admin console) aren't in v1 - see
    /// [docs/architecture.md §11](../../docs/architecture.md#skilj-tui-console).
    #[arg(long, env = "SKILJ_BOUNDED_CONTEXT")]
    pub bounded_context: String,
}
