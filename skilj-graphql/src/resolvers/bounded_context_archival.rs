//! `surface BoundedContextArchival` - `archiveBoundedContext`. Gated by
//! an admin-level `RoleAccessMapping` on the target context (not
//! `Superadmin` - a deliberately lower bar than `BoundedContextCreation`/
//! `BoundedContextDeletion`, see `bootstrap::archive_bounded_context`'s
//! own doc comment), so this resolver resolves that mapping itself
//! before calling in - the pure function takes it as a required
//! parameter, not an `Option`, so a caller with no mapping at all for
//! this context needs a resolver-level rejection, not a library one.

use super::{load_bounded_context_with_mappings, not_found, require_caller};
use crate::error::to_graphql_error;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};

pub fn field(n: &Naming) -> Field {
    Field::new(
        n.root("archiveBoundedContext"),
        TypeRef::named_nn(n.ty("BoundedContext")),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let name = ctx.args.try_get("name")?.string()?.to_string();

                // No mapping at all for this context collapses into the
                // same rejection a revoked one gets - `archive_bounded_context`
                // can't tell "missing" from "revoked" either, since both
                // arrive as `GrantNotActive` from `require_active_admin`
                // once a real (revoked) row exists; this is the "no row
                // at all" case that has none to pass in.
                let access_mapping =
                    skilj_core::db::get_active_role_access_mapping(&state.pool, &caller.id, &name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| {
                            to_graphql_error(skilj_core::access_control::Error::GrantNotActive)
                        })?;
                let bounded_context = skilj_core::db::get_bounded_context(&state.pool, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| super::not_found("BoundedContext", &name))?;

                let archived = skilj_core::bootstrap::archive_bounded_context(
                    &access_mapping,
                    &bounded_context,
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::update_bounded_context_status(&state.pool, &name, archived.status)
                    .await
                    .map_err(to_graphql_error)?;

                // A concurrent `deleteBoundedContext` (which requires exactly
                // the `Archived` status this request's own write above just
                // gave it) could in principle land in the gap between that
                // write and this reload - narrow (two sequential awaits,
                // nothing else in between), but the same "ordinary error,
                // not a panic" treatment every other version of this race
                // gets elsewhere in this codebase.
                let with_mappings = load_bounded_context_with_mappings(&state.pool, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("BoundedContext", &name))?;
                Ok(Some(FieldValue::owned_any(with_mappings)))
            })
        },
    )
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
}
