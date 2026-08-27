//! SkilJ's domain engine.
//!
//! This crate implements the behaviour specified in `specs/skilj.allium` -
//! entities, rules, invariants, and the plugin API a consuming application
//! writes its own bounded-context logic against. It has no web-framework
//! dependency; `skilj-graphql` and `skilj-rest` each depend on this crate,
//! not the other way round.
//!
//! Modules are grouped by domain concern, not by the spec's own section
//! order (Value Types / Entities / Rules / ...) - see
//! `docs/architecture.md` §3.2 for the reasoning and the full module map.

pub mod access_control;
pub mod bootstrap;
pub mod cross_instance;
pub mod db;
pub mod encryption;
pub mod error;
pub mod event_cache;
pub mod event_store;
pub mod plugin;
pub mod projections;
pub mod shared;

pub use error::Error;
