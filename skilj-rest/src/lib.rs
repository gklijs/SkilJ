//! SkilJ's REST surface. Depends on `skilj-core`, never the other way
//! round - see docs/architecture.md §3.1.
//!
//! Exists for narrowly-scoped `AccessToken`-holding callers - AI agents,
//! remote workflows, adapters - never for general application access,
//! which is what `skilj-graphql` is for. See §7.1.

pub mod auth;
pub mod routes;
