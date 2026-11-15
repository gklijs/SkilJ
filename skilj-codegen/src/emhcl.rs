//! Imports a model made with [Event Modeling HCL](https://event-modeling-hcl.github.io)
//! into a `.skilj.toml` (docs/architecture.md §243,
//! `docs/event-modeling-hcl.md`).
//!
//! The input is what `emhcl export model.em.hcl -o model.json` writes: the
//! slice-based Event Modeling JSON. That format carries everything this
//! import needs - element ids, fields with their types and key flags,
//! dependencies, API endpoints and scenarios - and none of what it drops
//! (lifecycles, aggregate identities, scenario cases) matters here. Reading
//! the JSON rather than the HCL keeps an HCL parser and the language's own
//! validation out of this crate: validate with `emhcl` first.
//!
//! The naming convention, the whole of it:
//!
//! - The bounded context's emhcl label is the skilj bounded context name;
//!   both are lowercase `snake_case`.
//! - An event's or command's emhcl label is its `NAME` in PascalCase:
//!   `money_deposited` becomes `MoneyDeposited`.
//! - A field with `id_attribute = true` is a DCB tag with the field name as
//!   its key: `account_id` becomes `account_id = "account_id"`.
//!   An aggregate isn't needed and isn't read.
//! - A command belongs to the bounded context of the events it emits.
//! - A command issued by a screen whose `api_endpoint` is
//!   `POST /v1/commands/trigger` gets `rest_trigger_allowed = true`.
//! - A scenario's `then { error = "<kind>: <reason>" }` is a rejection with
//!   that `kind` and `reason`. An error without a kind in front is a
//!   rejection of the generic kind [`GENERIC_REJECTION_KIND`]. The kinds are
//!   written as a comment above the command, to implement in its `decide_*`
//!   function.
//!
//! A type with a field this format can't express (a list, nested fields, an
//! optional field, a `Double`, `Decimal` or `Custom`) is left out with a note
//! saying why, to hand-write instead - the same rule `.skilj.toml` itself
//! follows. Personal data (`pii = true`) is kept, with a note: choosing its
//! subject is a decision the model doesn't record.
//!
//! This is [`crate::import::import`] with [`Source::Emhcl`], which
//! `skilj-codegen import --from emhcl` runs; the other sources share
//! everything after reading (docs/modeling-tools.md).

use crate::import::Source;
use crate::Error;

pub use crate::import::{Import, GENERIC_REJECTION_KIND, REST_TRIGGER_ENDPOINT};

/// Imports the events and commands of `bounded_context` from `emhcl export`
/// JSON into a `.skilj.toml`.
///
/// Refused as [`Error::Json`] when `json` isn't the export format, and as
/// [`Error::Invalid`] when `bounded_context` isn't a skilj bounded context
/// name, has no events in the model, or the result wouldn't generate (two
/// commands with one label in different workflows, say).
pub fn import(json: &str, bounded_context: &str) -> Result<Import, Error> {
    crate::import::import(Source::Emhcl, json, bounded_context)
}
