//! Scaffolded by skilj-template - one worked example bounded context,
//! `wallet`, showing the shape every skilj bounded context takes: an
//! `EventType`/`CommandType`/`Projection` trio, each
//! `#[skilj::auto_register(BOUNDED_CONTEXT)]`-tagged so `register()`
//! below needs no per-type wiring.
//!
//! Rename/replace `wallet` with your own domain, or add more bounded
//! contexts alongside it as their own modules - each just needs its own
//! `pub const BOUNDED_CONTEXT` and the same `#[auto_register(BOUNDED_CONTEXT)]`
//! pattern; `register()` picks all of them up automatically, the same
//! way it already picks up `wallet`.

pub mod wallet;

/// Registers every `#[auto_register]`-tagged type in this crate onto
/// `builder` - see `src/bin/server.rs` for the one real caller.
pub fn register(builder: skilj::SkiljBuilder) -> skilj::SkiljBuilder {
    builder.auto_register()
}
