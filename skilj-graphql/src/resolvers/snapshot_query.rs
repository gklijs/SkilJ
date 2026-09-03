//! `inspectSnapshot` - docs/architecture.md §19's own inspection
//! endpoint for `Snapshot`. `AdminAccess`-gated, via the same shared
//! `require_admin_mapping` helper `EventQuery`'s own `inspectEvent`
//! uses - a snapshot's own `state` is full derived business data, the
//! same sensitivity class as raw event content
//! (`EventQuery::SensitiveFieldsStayProtected`'s own reasoning), not
//! folded into the `ReadAccess`-gated `ProjectionQuery` the way the
//! shapes might otherwise suggest - see `crate::plugin::Snapshot`'s own
//! doc comment for why `Snapshot` stays structurally separate from
//! `Projection` throughout, this endpoint included.

use super::{not_found, require_admin_mapping};
use crate::error::to_graphql_error;
use crate::gql_types::InspectedSnapshotData;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};

/// `inspectSnapshot(boundedContext: String!, snapshotName: String!, tagValue: String!): InspectedSnapshot`
///
/// `snapshotName` unrecognised in this process (`SnapshotDispatcher::tag_key`
/// returns `None`) is a real `not_found` error - the caller named
/// something that doesn't exist. `null` is reserved for the other,
/// legitimate case: a real, registered snapshot that's simply never had
/// a row written for this `tagValue` yet (cold - see
/// `CommandType::decide_from_snapshot`'s own doc comment on why a
/// missing snapshot is never an error there either) - unless the reject
/// below fires first (see the next paragraph): those two "nothing to
/// show" outcomes stay distinguishable from each other, but a
/// `scope`-restricted caller can't tell "cold" from "not mine" apart -
/// deliberately, the identical "invisible, not merely unreadable"
/// framing every sibling in this pass already gives an owner mismatch.
///
/// Cross-tenant read fix (docs/architecture.md's own write-up of these
/// passes): `Snapshot::OWNER_TAG_KEY`, when declared, names which of a
/// folding event's own tags becomes a stored row's derived `owner` (see
/// `db::catch_up_snapshots`). A `scope`-restricted `RoleAccessMapping`
/// whose `scope` doesn't match a found row's own `owner` is rejected
/// with `GrantScopeMismatch` - see `access_control::scope_matches_owner`'s
/// own doc comment for the full fail-closed contract, identical to
/// `projections::query_projection`'s.
///
/// Deliberately shows the *raw stored row*, including its own recorded
/// `version`, even when that no longer matches the currently-registered
/// `Snapshot::VERSION` - unlike `decide_from_snapshot`'s own path, which
/// treats a version mismatch as if the row were absent. An operator
/// inspecting a snapshot wants to see what's really there, including a
/// stale one still waiting for the next catch-up tick to overwrite it;
/// only the decision path needs to distrust it.
pub fn inspect_snapshot_field() -> Field {
    Field::new(
        "inspectSnapshot",
        TypeRef::named("InspectedSnapshot"),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let snapshot_name = ctx.args.try_get("snapshotName")?.string()?.to_string();
                let tag_value = ctx.args.try_get("tagValue")?.string()?.to_string();

                let Some(tag_key) = state
                    .snapshot_dispatcher
                    .tag_key(&bounded_context_name, &snapshot_name)
                else {
                    return Err(not_found("Snapshot", &snapshot_name));
                };
                let owner_tag_key = state
                    .snapshot_dispatcher
                    .owner_tag_key(&bounded_context_name, &snapshot_name)
                    .flatten();

                let stored = skilj_core::db::get_snapshot_state_and_owner(
                    &state.pool,
                    &bounded_context_name,
                    &snapshot_name,
                    tag_key,
                    &tag_value,
                )
                .await
                .map_err(to_graphql_error)?;

                // Cross-tenant read fix (docs/architecture.md's own
                // write-up of these passes): a `scope`-restricted grant
                // querying an owner-declaring snapshot can't tell "cold"
                // from "not mine" apart - both fail closed alike, rather
                // than the ordinary "nothing recorded yet" `null`
                // `stored.is_none()` otherwise answers with. See
                // `surface SnapshotInspection`'s own
                // `GrantScopedToOwnerWhenDeclared` guarantee.
                let scope_restricted = owner_tag_key.is_some() && access_mapping.scope.is_some();

                let Some(((version, as_of_sequence, state_json, updated_at), owner)) = stored
                else {
                    if scope_restricted {
                        return Err(to_graphql_error(
                            skilj_core::access_control::Error::GrantScopeMismatch,
                        ));
                    }
                    return Ok(None);
                };
                if scope_restricted
                    && !skilj_core::access_control::scope_matches_owner(
                        owner.as_deref(),
                        access_mapping.scope.as_deref(),
                    )
                {
                    return Err(to_graphql_error(
                        skilj_core::access_control::Error::GrantScopeMismatch,
                    ));
                }

                Ok(Some(FieldValue::owned_any(InspectedSnapshotData {
                    tag_key: tag_key.to_string(),
                    tag_value,
                    version: version as i64,
                    as_of_sequence,
                    state: state_json,
                    updated_at,
                })))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "snapshotName",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "tagValue",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}
