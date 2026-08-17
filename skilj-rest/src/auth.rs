//! Bearer-token extraction: `Authorization: Bearer <id>.<secret>` - see
//! the REST token presentation note in `specs/skilj.allium`. Verification
//! itself (looking the token up by id, comparing the secret in constant
//! time) is `skilj-core::access_control`'s job, not this crate's - this
//! module only pulls the two halves of the credential apart, structurally
//! (§7.5's 401 tier: missing or malformed, before any lookup happens at
//! all). Each route handler does its own kind-specific lookup afterwards
//! (see `crate::routes`), since which `AccessToken` variant a given route
//! needs varies per route.

use crate::error::RestError;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;

/// The two halves of a presented bearer credential, split apart but not
/// yet looked up or verified against anything - see this module's own
/// doc comment for why that's each route handler's job, not this
/// extractor's.
pub struct BearerCredential {
    pub id: String,
    pub secret: String,
}

impl<S> FromRequestParts<S> for BearerCredential
where
    S: Send + Sync,
{
    type Rejection = RestError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .ok_or(RestError::MissingCredential)?
            .to_str()
            .map_err(|_| RestError::MalformedCredential)?;
        let credential = header
            .strip_prefix("Bearer ")
            .ok_or(RestError::MalformedCredential)?;
        let (id, secret) = credential
            .split_once('.')
            .ok_or(RestError::MalformedCredential)?;
        if id.is_empty() || secret.is_empty() {
            return Err(RestError::MalformedCredential);
        }
        Ok(BearerCredential {
            id: id.to_string(),
            secret: secret.to_string(),
        })
    }
}
