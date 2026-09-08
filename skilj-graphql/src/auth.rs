//! JWT extraction and identity resolution for incoming GraphQL requests -
//! see [docs/architecture.md §6](../../docs/architecture.md#idp-trust-configuration). Verification itself (JWKS fetch/cache,
//! JWT signature check) is `skilj_core::access_control`'s job, not this
//! crate's; this module only reads the bearer header, calls into that,
//! and resolves the verified subject to a `Role`.

use async_graphql::ErrorExtensions;
use skilj_core::access_control::{IdpConfig, JwksCache, Role};
use skilj_core::db::Pool;
use std::sync::Arc;

/// `skilj`'s own trusted-IdP bundle, handed to this crate by
/// `Skilj::graphql_router()` - mirrors `skilj::Skilj`'s private
/// `IdentityProvider` on this side of the crate boundary. `Clone`-cheap:
/// `IdpConfig` is a small owned struct, `JwksCache` is `Arc`-wrapped so
/// its cached keys and refetch timer are shared, not duplicated, across
/// every request (see `JwksCache`'s own doc comment for why that
/// matters).
#[derive(Clone)]
pub struct Identity {
    pub config: IdpConfig,
    pub cache: Arc<JwksCache>,
}

/// Resolves the caller's `Role` from an incoming request's
/// `Authorization` header - `Ok(None)` when no header is present at all,
/// valid only for `createSuperadmin` (see `surface SuperadminBootstrap`,
/// the one surface with no Role-based actor). Every other resolver in
/// this crate checks for `Some` itself and rejects `None` with its own
/// "unauthenticated" error - the same "each rule re-checks its own
/// requires" discipline every pure function in `skilj-core` already has,
/// rather than this function deciding on any resolver's behalf which
/// ones need a caller.
///
/// `Err` when a header *is* presented but doesn't resolve to a Role - a
/// malformed bearer credential, no `identity_provider` configured at
/// all, a JWT that doesn't verify, or a verified JWT whose subject claim
/// matches no active Role. The caller (`graphql_handler`) rejects the
/// whole request outright in that case, never executing any resolver -
/// the same "a wrong credential is a hard stop" treatment
/// `skilj-rest`'s own bearer extractor gives a malformed one.
pub async fn resolve_role(
    headers: &axum::http::HeaderMap,
    identity: Option<&Identity>,
    pool: &Pool,
) -> Result<Option<Role>, async_graphql::Error> {
    let Some(header_value) = headers.get(axum::http::header::AUTHORIZATION) else {
        return Ok(None);
    };
    let header_value = header_value
        .to_str()
        .map_err(|_| malformed_credential("Authorization header is not valid UTF-8"))?;
    let Some(jwt) = header_value.strip_prefix("Bearer ") else {
        return Err(malformed_credential(
            "Authorization header is not a well-formed \"Bearer <jwt>\" credential",
        ));
    };
    verify_jwt_to_role(jwt, identity, pool).await.map(Some)
}

/// `resolve_role`'s own counterpart for a GraphQL-over-websocket
/// connection - reads the bearer credential from the `connection_init`
/// message's own JSON payload (there is no per-message header on an
/// already-established websocket, so this is the graphql-ws protocol's
/// own place for it), the same `{"Authorization": "Bearer <jwt>"}` shape
/// as the header case, just JSON instead of an HTTP header. `Ok(None)`
/// when the payload has no recognisable credential at all - valid, the
/// same "some callers need none" case `resolve_role` itself allows;
/// `Err` (rejecting the whole connection before any subscription starts)
/// for anything present but invalid, mirroring `resolve_role` exactly.
pub async fn resolve_role_from_connection_init(
    payload: &serde_json::Value,
    identity: Option<&Identity>,
    pool: &Pool,
) -> Result<Option<Role>, async_graphql::Error> {
    let Some(header_value) = payload
        .get("Authorization")
        .or_else(|| payload.get("authorization"))
        .and_then(|v| v.as_str())
    else {
        return Ok(None);
    };
    let Some(jwt) = header_value.strip_prefix("Bearer ") else {
        return Err(malformed_credential(
            "connection_init's Authorization payload is not a well-formed \"Bearer <jwt>\" \
             credential",
        ));
    };
    verify_jwt_to_role(jwt, identity, pool).await.map(Some)
}

/// The shared core `resolve_role`/`resolve_role_from_connection_init`
/// both need once they have a bare JWT string in hand: verify it, then
/// resolve its subject to an active `Role`. Always `Err` on failure,
/// never `Ok(None)` - a bearer credential was *presented* by the time
/// either caller reaches this, so "doesn't resolve" is always a real
/// rejection here, unlike the "absent entirely" case each caller checks
/// for itself first.
async fn verify_jwt_to_role(
    jwt: &str,
    identity: Option<&Identity>,
    pool: &Pool,
) -> Result<Role, async_graphql::Error> {
    let Some(identity) = identity else {
        return Err(async_graphql::Error::new(
            "this deployment has no identity_provider configured - no bearer JWT can ever verify",
        )
        .extend_with(|_, ext| ext.set("code", "no_identity_provider_configured")));
    };

    let verified_subject = skilj_core::access_control::verify_and_extract_subject(
        jwt,
        &identity.config,
        &identity.cache,
    )
    .await
    .map_err(crate::error::to_graphql_error)?;

    let roles = skilj_core::db::list_roles(pool)
        .await
        .map_err(crate::error::to_graphql_error)?;
    let role =
        skilj_core::access_control::resolve_role_by_external_subject(&verified_subject, &roles)
            .map_err(crate::error::to_graphql_error)?
            .clone();
    Ok(role)
}

fn malformed_credential(message: &str) -> async_graphql::Error {
    async_graphql::Error::new(message).extend_with(|_, ext| ext.set("code", "malformed_credential"))
}
