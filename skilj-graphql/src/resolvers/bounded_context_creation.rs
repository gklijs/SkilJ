//! `surface BoundedContextCreation` - `addBoundedContext`. `Superadmin`-
//! gated; `bootstrap::add_bounded_context` re-checks `caller.superadmin`
//! for real.

use super::{load_bounded_context_with_mappings, require_superadmin};
use crate::error::to_graphql_error;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};

/// `addBoundedContext(name: String!): BoundedContext!`
pub fn field(n: &Naming) -> Field {
    Field::new(
        n.root("addBoundedContext"),
        TypeRef::named_nn(n.ty("BoundedContext")),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let name = ctx.args.try_get("name")?.string()?.to_string();

                let existing_contexts = skilj_core::db::list_bounded_contexts(&state.pool)
                    .await
                    .map_err(to_graphql_error)?;
                let bounded_context = skilj_core::bootstrap::add_bounded_context(
                    &caller,
                    name.clone(),
                    &existing_contexts,
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::insert_bounded_context(&state.pool, &bounded_context)
                    .await
                    .map_err(to_graphql_error)?;

                // Unlike the identical-looking reload in
                // `bounded_context_archival`/`bounded_context_deletion`/
                // `bounded_context_directory`/`bounded_context_templating`,
                // this one really can't race a concurrent
                // `deleteBoundedContext`: `bootstrap::delete_bounded_context`
                // requires `Archived`, and a context this call just inserted
                // is `Active` - it isn't yet eligible for deletion by
                // anyone, so a missing row here would be a real bug in this
                // function itself, not a race. Stays a panic.
                let with_mappings = load_bounded_context_with_mappings(&state.pool, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .expect("just inserted this bounded context - it must be readable back");
                Ok(Some(FieldValue::owned_any(with_mappings)))
            })
        },
    )
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
}
