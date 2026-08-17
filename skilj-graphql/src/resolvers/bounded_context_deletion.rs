//! `surface BoundedContextDeletion` - `deleteBoundedContext`.
//! `Superadmin`-gated (a deliberately higher bar than
//! `BoundedContextArchival`'s per-context admin grant - see
//! `bootstrap::delete_bounded_context`'s own doc comment).

use super::{load_bounded_context_with_mappings, require_caller};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};

/// `deleteBoundedContext(name: String!): BoundedContext!` - returns the
/// pre-deletion snapshot, loaded before `hard_delete_bounded_context`
/// runs: there is nothing left to query afterward (`DROP SCHEMA ...
/// CASCADE` removes the schema, and the registry row itself is gone
/// too), so the response is built from what was there right before the
/// irreversible act, not a fresh read after it.
pub fn field() -> Field {
    Field::new(
        "deleteBoundedContext",
        TypeRef::named_nn("BoundedContext"),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let name = ctx.args.try_get("name")?.string()?.to_string();

                let bounded_context = skilj_core::db::get_bounded_context(&state.pool, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| super::not_found("BoundedContext", &name))?;
                let with_mappings = load_bounded_context_with_mappings(&state.pool, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .expect("just loaded this bounded context above - it must still be there");

                skilj_core::bootstrap::delete_bounded_context(&caller, &bounded_context)
                    .map_err(to_graphql_error)?;
                skilj_core::db::hard_delete_bounded_context(&state.pool, &name)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(with_mappings)))
            })
        },
    )
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
}
