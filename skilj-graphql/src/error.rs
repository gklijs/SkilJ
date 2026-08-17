//! Renders `skilj_core::error::SkiljRejection` (both error tiers - see
//! docs/architecture.md §4) into `async_graphql`'s own `Error`, carrying
//! `code()` as the `code` extension - §5.4's shared error shape, the
//! same `code()`/`message()` trait `skilj-rest::error` renders through
//! for REST (§4.2, §7.5). `CommandRejected` isn't specially handled here,
//! since that split (rejections as typed data, not an error) is
//! `CommandSubmission`-specific, Phase 3, not built yet; nothing in
//! Phase 1 can ever produce one.

use async_graphql::ErrorExtensions;
use skilj_core::error::SkiljRejection;

/// The one conversion every resolver in this crate uses for a
/// `skilj_core` (or auth-layer) rejection - `.map_err(to_graphql_error)?`,
/// not `?` alone: `async_graphql::Error` has a blanket `From<T: Display>`
/// impl that would compile without this, but it wouldn't carry `code()`
/// as an extension, silently dropping half of §5.4's error shape.
pub fn to_graphql_error(rejection: impl SkiljRejection) -> async_graphql::Error {
    let code = rejection.code().to_string();
    async_graphql::Error::new(rejection.message()).extend_with(|_, ext| ext.set("code", code))
}
