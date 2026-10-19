//! SkilJ's REST surface. Depends on `skilj-core`, never the other way
//! round - see docs/architecture.md §3.1.
//!
//! Exists for narrowly-scoped `AccessToken`-holding callers - AI agents,
//! remote workflows, adapters - never for general application access,
//! which is what `skilj-graphql` is for. See §7.1.

// Every public item is documented; CI fails on a gap (`-D warnings`).
#![warn(missing_docs)]

pub mod auth;
pub mod error;
pub mod routes;

pub use routes::router;
