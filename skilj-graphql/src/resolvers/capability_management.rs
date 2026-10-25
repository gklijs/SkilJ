//! Capabilities (docs/architecture.md §228): `grantCapability`,
//! `revokeCapability`, `setDelegableCapabilities`, and the two reads that
//! go with them - `capabilityDelegation` (`surface CapabilityDelegation`,
//! what a context admin sees of its own bounded context) and
//! `capabilityOverview` (`surface CapabilityOverview`, a superadmin's
//! view of what is required against what is held).
//!
//! The first two take either actor the core rules know
//! (`access_control::CapabilityActor`): a superadmin, or an admin grant
//! on the named bounded context. A caller who is neither is refused with
//! `grant_not_active`, the same answer a caller with no grant there gets
//! anywhere else, so nothing here says whether the bounded context or the
//! target exists. `access_control::grant_capability`/`revoke_capability`
//! make every decision; `db::add_mapping_capability`/
//! `remove_mapping_capability` apply it atomically.

use super::{not_found, parse_rfc3339, require_admin_mapping, require_caller, require_superadmin};
use crate::error::to_graphql_error;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, ResolverContext, TypeRef,
};
use skilj_core::access_control::{CapabilityActor, Role, RoleAccessMapping};
use std::collections::BTreeSet;

/// The caller as the core's actor: a superadmin acts as one; anyone else
/// must hold an admin grant on `bounded_context_name`.
async fn actor_for(
    ctx: &ResolverContext<'_>,
    state: &GraphqlState,
    caller: &Role,
    bounded_context_name: &str,
) -> async_graphql::Result<Option<RoleAccessMapping>> {
    if caller.superadmin {
        return Ok(None);
    }
    require_admin_mapping(ctx, &state.pool, bounded_context_name)
        .await
        .map(Some)
}

async fn target_mapping(
    state: &GraphqlState,
    role_id: &str,
    bounded_context_name: &str,
) -> async_graphql::Result<RoleAccessMapping> {
    skilj_core::db::get_active_role_access_mapping(&state.pool, role_id, bounded_context_name)
        .await
        .map_err(to_graphql_error)?
        .ok_or_else(|| {
            not_found(
                "RoleAccessMapping",
                &format!("{role_id}/{bounded_context_name}"),
            )
        })
}

fn change_field(n: &Naming, name: &'static str, grant: bool) -> Field {
    Field::new(
        n.root(name),
        TypeRef::named_nn(n.ty("RoleAccessMapping")),
        move |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let role_id = ctx.args.try_get("roleId")?.string()?.to_string();
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let capability = ctx.args.try_get("capability")?.string()?.to_string();

                let admin_mapping = actor_for(&ctx, state, &caller, &bounded_context_name).await?;
                let actor = match &admin_mapping {
                    None => CapabilityActor::Superadmin(&caller),
                    Some(mapping) => CapabilityActor::Admin(mapping),
                };
                let target = target_mapping(state, &role_id, &bounded_context_name).await?;

                let updated = if grant {
                    skilj_core::access_control::grant_capability(actor, &target, &capability)
                } else {
                    skilj_core::access_control::revoke_capability(actor, &target, &capability)
                }
                .map_err(to_graphql_error)?;

                let applied = skilj_core::db::change_mapping_capability_with_trail(
                    &state.pool,
                    &role_id,
                    &bounded_context_name,
                    &capability,
                    grant,
                    &caller.id,
                    admin_mapping.is_some(),
                )
                .await
                .map_err(to_graphql_error)?;
                if !applied {
                    // A concurrent change got there first: the decision
                    // above was made on a mapping that has since moved.
                    return Err(to_graphql_error(if grant {
                        skilj_core::access_control::Error::CapabilityNotAdded
                    } else {
                        skilj_core::access_control::Error::CapabilityNotRevoked
                    }));
                }
                tracing::info!(
                    actor = %caller.id,
                    target_role = %role_id,
                    bounded_context = %bounded_context_name,
                    capability = %capability,
                    granted = grant,
                    "capability changed"
                );
                Ok(Some(FieldValue::owned_any(updated)))
            })
        },
    )
    .argument(InputValue::new("roleId", TypeRef::named_nn(TypeRef::ID)))
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "capability",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}

/// `grantCapability(roleId: ID!, boundedContext: String!, capability: String!): RoleAccessMapping!`
pub fn grant_capability_field(n: &Naming) -> Field {
    change_field(n, "grantCapability", true)
}

/// `revokeCapability(roleId: ID!, boundedContext: String!, capability: String!): RoleAccessMapping!`
pub fn revoke_capability_field(n: &Naming) -> Field {
    change_field(n, "revokeCapability", false)
}

/// `setDelegableCapabilities(roleId: ID!, boundedContext: String!, delegable: [String!]!): RoleAccessMapping!`
///
/// Superadmin only. Replaces the set; capabilities the admin already
/// granted stay where they are.
pub fn set_delegable_capabilities_field(n: &Naming) -> Field {
    Field::new(
        n.root("setDelegableCapabilities"),
        TypeRef::named_nn(n.ty("RoleAccessMapping")),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let role_id = ctx.args.try_get("roleId")?.string()?.to_string();
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let delegable = ctx
                    .args
                    .try_get("delegable")?
                    .list()?
                    .iter()
                    .map(|item| item.string().map(str::to_string))
                    .collect::<Result<BTreeSet<_>, _>>()?;

                let target = target_mapping(state, &role_id, &bounded_context_name).await?;
                let updated = skilj_core::access_control::set_delegable_capabilities(
                    &caller, &target, delegable,
                )
                .map_err(to_graphql_error)?;
                // `false` is a grant gone or revoked, or (only in a race with
                // the pure size check above) an oversized set; both read as
                // not active.
                if !skilj_core::db::set_mapping_delegable_with_trail(
                    &state.pool,
                    &role_id,
                    &bounded_context_name,
                    &updated.delegable,
                    &caller.id,
                )
                .await
                .map_err(to_graphql_error)?
                {
                    return Err(to_graphql_error(
                        skilj_core::access_control::Error::GrantNotActive,
                    ));
                }
                tracing::info!(
                    actor = %caller.id,
                    target_role = %role_id,
                    bounded_context = %bounded_context_name,
                    "delegable capabilities set"
                );
                Ok(Some(FieldValue::owned_any(updated)))
            })
        },
    )
    .argument(InputValue::new("roleId", TypeRef::named_nn(TypeRef::ID)))
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "delegable",
        TypeRef::named_nn_list_nn(TypeRef::STRING),
    ))
}

/// One grant in a bounded context, as an admin sees it.
#[derive(Clone)]
pub struct DelegationGrant {
    /// The Role's id.
    pub role_id: String,
    /// The Role's name.
    pub role_name: String,
    /// What the grant holds.
    pub capabilities: BTreeSet<String>,
}

/// `surface CapabilityDelegation`'s `exposes`.
pub struct CapabilityDelegation {
    /// What the admin grant may hand out.
    pub delegable: BTreeSet<String>,
    /// Every active grant in the bounded context.
    pub grants: Vec<DelegationGrant>,
}

/// `capabilityDelegation(boundedContext: String!): CapabilityDelegation!` -
/// an admin grant's own `delegable` set, and who holds what in its
/// bounded context.
pub fn capability_delegation_field(n: &Naming) -> Field {
    Field::new(
        n.root("capabilityDelegation"),
        TypeRef::named_nn(n.ty("CapabilityDelegation")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let admin = require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let mappings =
                    skilj_core::db::list_active_role_access_mappings_for_bounded_context(
                        &state.pool,
                        &admin.bounded_context,
                    )
                    .await
                    .map_err(to_graphql_error)?;
                let grants = mappings
                    .into_iter()
                    .map(|m| DelegationGrant {
                        role_id: m.role.id,
                        role_name: m.role.name,
                        capabilities: m.capabilities,
                    })
                    .collect();
                Ok(Some(FieldValue::owned_any(CapabilityDelegation {
                    delegable: admin.delegable,
                    grants,
                })))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}

/// `surface CapabilityOverview`'s row for one bounded context.
#[derive(Clone)]
pub struct CapabilityOverviewEntry {
    /// The bounded context.
    pub bounded_context: String,
    /// Required by a registered projection or private field.
    pub required: BTreeSet<String>,
    /// Held by an active grant.
    pub held: BTreeSet<String>,
}

/// `capabilityOverview: [CapabilityOverviewEntry!]!` - superadmin only,
/// read-only. The two differences are the lists to read for a typo.
pub fn capability_overview_field(n: &Naming) -> Field {
    Field::new(
        n.root("capabilityOverview"),
        TypeRef::named_nn_list_nn(n.ty("CapabilityOverviewEntry")),
        |ctx| {
            FieldFuture::new(async move {
                require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let mut entries = Vec::new();
                for bc in skilj_core::db::list_bounded_contexts(&state.pool)
                    .await
                    .map_err(to_graphql_error)?
                {
                    let mut required = BTreeSet::new();
                    for p in
                        skilj_core::db::list_projections_for_bounded_context(&state.pool, &bc.name)
                            .await
                            .map_err(to_graphql_error)?
                    {
                        required.extend(p.requires_capability);
                    }
                    for et in
                        skilj_core::db::list_event_types_for_bounded_context(&state.pool, &bc.name)
                            .await
                            .map_err(to_graphql_error)?
                    {
                        required.extend(et.private_fields.into_iter().filter_map(|f| f.capability));
                    }
                    for ct in skilj_core::db::list_command_types_for_bounded_context(
                        &state.pool,
                        &bc.name,
                    )
                    .await
                    .map_err(to_graphql_error)?
                    {
                        required.extend(ct.private_fields.into_iter().filter_map(|f| f.capability));
                    }
                    let held =
                        skilj_core::db::list_active_role_access_mappings_for_bounded_context(
                            &state.pool,
                            &bc,
                        )
                        .await
                        .map_err(to_graphql_error)?
                        .into_iter()
                        .flat_map(|m| m.capabilities)
                        .collect();
                    entries.push(FieldValue::owned_any(CapabilityOverviewEntry {
                        bounded_context: bc.name,
                        required,
                        held,
                    }));
                }
                Ok(Some(FieldValue::list(entries)))
            })
        },
    )
}

/// `capabilityHistory(roleId: ID!, boundedContext: String!, asOf: String): CapabilityHistory!` -
/// `rule CapabilityHistory`, superadmin only: what the grant held at
/// `asOf` (RFC 3339, default now), folded from the access trail
/// (docs/architecture.md §229). `known: false` when the trail had not
/// begun for that grant by then.
pub fn capability_history_field(n: &Naming) -> Field {
    Field::new(
        n.root("capabilityHistory"),
        TypeRef::named_nn(n.ty("CapabilityHistory")),
        |ctx| {
            FieldFuture::new(async move {
                require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let role_id = ctx.args.try_get("roleId")?.string()?.to_string();
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let as_of = match ctx.args.get("asOf").filter(|value| !value.is_null()) {
                    Some(value) => parse_rfc3339(value.string()?)?,
                    None => chrono::Utc::now(),
                };
                let records = skilj_core::db::list_access_event_records(
                    &state.pool,
                    &role_id,
                    &bounded_context_name,
                )
                .await
                .map_err(to_graphql_error)?;
                Ok(Some(FieldValue::owned_any(
                    skilj_core::access_trail::fold_access_events(&records, as_of),
                )))
            })
        },
    )
    .argument(InputValue::new("roleId", TypeRef::named_nn(TypeRef::ID)))
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("asOf", TypeRef::named(TypeRef::STRING)))
}
