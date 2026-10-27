//! The encryption master key's admin surface (docs/architecture.md §230):
//! `encryptionMasterKey`, where its rotation stands, and
//! `rotateEncryptionMasterKey`, which rotates it now. Superadmin only, like
//! the rest of the admin console; both answer `encryption_master_key_not_configured`
//! when this deployment has no key provider.

use super::require_superadmin;
use crate::error::to_graphql_error;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::db::Rotation;
use skilj_core::encryption::trail::RotationReason;

fn key_service(state: &GraphqlState) -> async_graphql::Result<&skilj_core::encryption::KeyService> {
    state
        .key_service
        .as_ref()
        .ok_or_else(|| to_graphql_error(skilj_core::encryption::Error::MasterKeyNotConfigured))
}

/// `encryptionMasterKey: EncryptionMasterKey` - the version in use and
/// where its rotation stands; `null` before any version was recorded.
pub fn encryption_master_key_field(n: &Naming) -> Field {
    Field::new(
        n.root("encryptionMasterKey"),
        TypeRef::named(n.ty("EncryptionMasterKey")),
        |ctx| {
            FieldFuture::new(async move {
                require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                key_service(state)?;
                let current = skilj_core::db::master_key_state(&state.pool)
                    .await
                    .map_err(to_graphql_error)?;
                Ok(current.map(FieldValue::owned_any))
            })
        },
    )
}

/// `rotateEncryptionMasterKey(retireImmediately: Boolean = false): EncryptionMasterKey!` -
/// a new version at the provider, recorded as `EncryptionMasterKeyRotated`
/// with the caller as actor. The rewrap follows in the background, and the
/// old versions are retired once it is done: after the grace period, or
/// at once with `retireImmediately` - what a leaked key calls for.
pub fn rotate_encryption_master_key_field(n: &Naming) -> Field {
    Field::new(
        n.root("rotateEncryptionMasterKey"),
        TypeRef::named_nn(n.ty("EncryptionMasterKey")),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let key_service = key_service(state)?;
                let retire_immediately = match ctx
                    .args
                    .get("retireImmediately")
                    .filter(|value| !value.is_null())
                {
                    Some(value) => value.boolean()?,
                    None => false,
                };
                skilj_core::db::rotate_master_key(
                    &state.pool,
                    key_service,
                    Rotation::Rotate(RotationReason::Requested),
                    retire_immediately,
                    Some(&caller.id),
                    None,
                )
                .await
                .map_err(to_graphql_error)?;
                let current = skilj_core::db::master_key_state(&state.pool)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| async_graphql::Error::new("the rotation was not recorded"))?;
                Ok(Some(FieldValue::owned_any(current)))
            })
        },
    )
    .argument(InputValue::new(
        "retireImmediately",
        TypeRef::named(TypeRef::BOOLEAN),
    ))
}
