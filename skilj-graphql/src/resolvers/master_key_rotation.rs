//! The encryption master keys' admin surface (docs/architecture.md §230):
//! `masterKeys`, every key and where its rotation stands;
//! `rotateMasterKey`, which rotates a bounded context's key (or the
//! default key) now; and `assignMasterKey`, which gives a bounded
//! context a key of its own, or the default key back. Superadmin only,
//! like the rest of the admin console; each answers
//! `encryption_master_key_not_configured` when this deployment has no key
//! provider.

use super::require_superadmin;
use crate::error::to_graphql_error;
use crate::gql_types::MasterKeyOverview;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::db::Rotation;
use skilj_core::encryption::trail::RotationReason;
use skilj_core::encryption::KeyService;

fn key_service(state: &GraphqlState) -> async_graphql::Result<&KeyService> {
    state
        .key_service
        .as_ref()
        .ok_or_else(|| to_graphql_error(skilj_core::encryption::Error::MasterKeyNotConfigured))
}

/// Every key's overview: its state and the bounded contexts using it.
async fn overviews(
    state: &GraphqlState,
    key_service: &KeyService,
) -> skilj_core::error::Result<Vec<MasterKeyOverview>> {
    let assignments = skilj_core::db::encryption_key_assignments(&state.pool).await?;
    let contexts = skilj_core::db::list_bounded_contexts(&state.pool).await?;
    let default_name = key_service.provider_name().to_string();
    Ok(skilj_core::db::master_key_states(&state.pool)
        .await?
        .into_iter()
        .map(|key| {
            let bounded_contexts = contexts
                .iter()
                .filter(|bc| match (&key.key, assignments.get(&bc.name)) {
                    (None, None) => key.provider == default_name,
                    (Some(own), Some(assigned)) => own == assigned,
                    _ => false,
                })
                .map(|bc| bc.name.clone())
                .collect();
            MasterKeyOverview {
                state: key,
                bounded_contexts,
            }
        })
        .collect())
}

async fn overview_of(
    state: &GraphqlState,
    key_service: &KeyService,
    provider: &str,
) -> async_graphql::Result<FieldValue<'static>> {
    overviews(state, key_service)
        .await
        .map_err(to_graphql_error)?
        .into_iter()
        .find(|o| o.state.provider == provider)
        .map(FieldValue::owned_any)
        .ok_or_else(|| async_graphql::Error::new("the key's state was not recorded"))
}

/// `masterKeys: [MasterKey!]!` - the default key and every
/// bounded context's own, with where each one's rotation stands and the
/// bounded contexts using it.
pub fn master_keys_field(n: &Naming) -> Field {
    Field::new(
        n.root("masterKeys"),
        TypeRef::named_nn_list_nn(n.ty("MasterKey")),
        |ctx| {
            FieldFuture::new(async move {
                require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let key_service = key_service(state)?;
                let all = overviews(state, key_service)
                    .await
                    .map_err(to_graphql_error)?;
                Ok(Some(FieldValue::list(
                    all.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
}

/// `rotateMasterKey(boundedContext: String, retireImmediately: Boolean = false): MasterKey!` -
/// a new version of the bounded context's key at the provider - its own,
/// or the default key every bounded context without one shares; without
/// `boundedContext`, the default key. A name that is no bounded context is
/// refused, rather than read as one without a key of its own. Recorded as `EncryptionMasterKeyRotated` with the
/// caller as actor. The rewrap follows in the background, and the old
/// versions are retired once it is done: after the grace period, or at
/// once with `retireImmediately` - what a leaked key calls for.
pub fn rotate_master_key_field(n: &Naming) -> Field {
    Field::new(
        n.root("rotateMasterKey"),
        TypeRef::named_nn(n.ty("MasterKey")),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let key_service = key_service(state)?;
                let retire_immediately = optional_bool(&ctx, "retireImmediately")?;
                let key = match optional_string(&ctx, "boundedContext")? {
                    // A name given must name a bounded context: one that
                    // doesn't would otherwise read as "no key of its own"
                    // and rotate the default key everyone shares.
                    Some(bounded_context) => {
                        let exists = !bounded_context.is_empty()
                            && skilj_core::db::get_bounded_context(&state.pool, &bounded_context)
                                .await
                                .map_err(to_graphql_error)?
                                .is_some();
                        if !exists {
                            return Err(
                                to_graphql_error(skilj_core::error::Error::row_not_found()),
                            );
                        }
                        skilj_core::db::bounded_context_key(&state.pool, &bounded_context)
                            .await
                            .map_err(to_graphql_error)?
                    }
                    None => None,
                };
                let provider = key_service
                    .provider_for(key.as_deref())
                    .map_err(to_graphql_error)?;
                skilj_core::db::rotate_master_key(
                    &state.pool,
                    provider.as_ref(),
                    key.as_deref(),
                    Rotation::Rotate(RotationReason::Requested),
                    retire_immediately,
                    Some(&caller.id),
                    None,
                )
                .await
                .map_err(to_graphql_error)?;
                Ok(Some(
                    overview_of(state, key_service, provider.name()).await?,
                ))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "retireImmediately",
        TypeRef::named(TypeRef::BOOLEAN),
    ))
}

/// `assignMasterKey(boundedContext: String!, key: String, create: Boolean = false): MasterKey!` -
/// gives the bounded context the provider's key named `key`, or the
/// default key back when `key` is null, recorded as
/// `EncryptionKeyAssigned`. With `create`, skilj creates the key first,
/// and deletes it once no bounded context uses it any more and the grace
/// period has passed; without, it must exist already and is only ever let
/// go of. The bounded context's data keys move to it in the background.
pub fn assign_master_key_field(n: &Naming) -> Field {
    Field::new(
        n.root("assignMasterKey"),
        TypeRef::named_nn(n.ty("MasterKey")),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let key_service = key_service(state)?;
                let bounded_context = ctx.args.try_get("boundedContext")?.string()?.to_string();
                let key = optional_string(&ctx, "key")?;
                let create = optional_bool(&ctx, "create")?;
                let assigned = skilj_core::db::assign_encryption_key(
                    &state.pool,
                    key_service,
                    &bounded_context,
                    key.as_deref(),
                    create,
                    Some(&caller.id),
                )
                .await
                .map_err(to_graphql_error)?;
                Ok(Some(
                    overview_of(state, key_service, &assigned.provider).await?,
                ))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("key", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new("create", TypeRef::named(TypeRef::BOOLEAN)))
}

fn optional_string(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    name: &str,
) -> async_graphql::Result<Option<String>> {
    match ctx.args.get(name).filter(|value| !value.is_null()) {
        Some(value) => Ok(Some(value.string()?.to_string())),
        None => Ok(None),
    }
}

fn optional_bool(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    name: &str,
) -> async_graphql::Result<bool> {
    match ctx.args.get(name).filter(|value| !value.is_null()) {
        Some(value) => Ok(value.boolean()?),
        None => Ok(false),
    }
}
