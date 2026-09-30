//! A Ratatui operator console for any `skilj-graphql` endpoint - see
//! `src/main.rs` for the entry point and [docs/architecture.md §11](../../docs/architecture.md#skilj-tui-console) for
//! why this crate exists and what it deliberately doesn't do yet.
//! Split into a library (this crate root) plus a thin `main.rs` purely
//! so `tests/*.rs` can exercise `graphql`/`projection_query` directly -
//! a `[[bin]]`-only crate has nothing `tests/` can import.

pub mod app;
pub mod cli;
pub mod form;
pub mod graphql;
pub mod json_style;
pub mod projection_query;
pub mod token;
pub mod ui;
