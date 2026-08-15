//! Bearer-token extraction: `Authorization: Bearer <id>.<secret>` - see
//! the REST token presentation note in `specs/skilj.allium`. Verification
//! itself (looking the token up by id, comparing the secret in constant
//! time) is `skilj-core::access_control`'s job, not this crate's.

// TODO: axum extractor parsing the "<id>.<secret>" bearer credential and
// delegating verification to skilj_core::access_control.
