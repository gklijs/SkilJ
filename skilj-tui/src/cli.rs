//! v1 config: endpoint + token (or a command producing one) + bounded
//! context, all supplied up front (flag or env var), no in-app login
//! flow - see this crate's own doc comment on why it never talks to an
//! IdP itself.

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
    /// Used until it expires; for longer sessions use `--token-command`.
    #[arg(long, env = "SKILJ_TOKEN", required_unless_present = "token_command")]
    pub token: Option<String>,

    /// A shell command printing such a JWT on stdout, e.g. an IdP's own
    /// CLI (`gcloud auth print-identity-token`, `oidc-token <account>`).
    /// Run at startup, and again whenever the server says the token has
    /// expired, so a session outlives any one token. Takes precedence
    /// over `--token`.
    #[arg(long, env = "SKILJ_TOKEN_COMMAND")]
    pub token_command: Option<String>,

    /// The bounded context to operate on. Superadmin-only operations
    /// (the cross-context directory, admin console) aren't in v1 - see
    /// [docs/architecture.md §11](../../docs/architecture.md#skilj-tui-console).
    #[arg(long, env = "SKILJ_BOUNDED_CONTEXT")]
    pub bounded_context: String,

    /// The name prefix of a skilj whose `/graphql` is a federation
    /// subgraph (`FederationOptions::prefix`, docs/architecture.md §194).
    /// Point this console at skilj itself, not at a router: the admin
    /// operations it uses are kept out of the supergraph.
    #[arg(long, env = "SKILJ_GRAPHQL_PREFIX", default_value = "")]
    pub graphql_prefix: String,
}
