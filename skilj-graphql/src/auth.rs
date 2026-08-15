//! JWT extraction from the incoming GraphQL request. Identity resolution
//! itself (signature verification against the IdP's JWKS, subject-claim
//! trust, Role lookup) is `skilj-core::access_control`'s job, not this
//! crate's - this module only extracts the bearer JWT from the request
//! and hands it off. See docs/architecture.md §6.

// TODO: axum extractor pulling the JWT out of the request, delegating
// verification and Role resolution to skilj_core::access_control.
