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
//!
//! `db`, `command_batcher`, `cross_instance`, `event_cache`,
//! `template_cache` and `slow_command` are `#[doc(hidden)]`: public only
//! because `skilj`, `skilj-graphql`, `skilj-rest` and the tools call
//! them, and not covered by the patch-release compatibility promise
//! (docs/architecture.md §220). Go through `skilj` instead.

// Every public item outside the hidden modules is documented; CI fails on
// a gap (`-D warnings`).
#![warn(missing_docs)]

pub mod access_control;
#[doc(hidden)]
pub mod access_trail;
pub mod approvals;
mod base64;
pub mod blind;
pub mod bootstrap;
#[doc(hidden)]
pub mod command_batcher;
#[doc(hidden)]
pub mod cross_instance;
#[doc(hidden)]
pub mod db;
pub mod encryption;
pub mod error;
#[doc(hidden)]
pub mod event_cache;
pub mod event_store;
pub mod plugin;
pub mod projections;
pub mod secrets;
pub mod shared;
#[doc(hidden)]
pub mod slow_command;
#[doc(hidden)]
pub mod template_cache;

pub use error::Error;
