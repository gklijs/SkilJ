//! SkilJ core, rebuilt from `specs/skilj.allium` alone.
//!
//! Event sourcing on one shared Postgres database, organised in bounded
//! contexts, with commands decided under Dynamic Consistency Boundaries.

pub mod crypto;
pub mod decl;
pub mod engine;
pub mod error;
pub mod filters;
pub mod model;
pub mod schema;
pub mod tags;
pub mod validate;

pub use engine::{Skilj, SkiljBuilder};
pub use error::{Error, Result};
