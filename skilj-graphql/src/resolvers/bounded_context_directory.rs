//! `surface BoundedContextDirectory` - `boundedContexts`. `Superadmin`-
//! gated; `bootstrap::list_bounded_contexts` re-checks `caller.superadmin`
//! for real, and passes every context straight through unfiltered (see
//! its own doc comment).

use super::{load_bounded_context_with_mappings, require_caller};
use crate::error::to_graphql_error;
use crate::gql_types::BoundedContextWithMappings;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, TypeRef};

/// `boundedContexts: [BoundedContext!]!`
pub fn field() -> Field {
    Field::new(
        "boundedContexts",
        TypeRef::named_nn_list_nn("BoundedContext"),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_caller(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;

                let all_contexts = skilj_core::db::list_bounded_contexts(&state.pool)
                    .await
                    .map_err(to_graphql_error)?;
                let listed = skilj_core::bootstrap::list_bounded_contexts(&caller, &all_contexts)
                    .map_err(to_graphql_error)?;

                let mut with_mappings: Vec<BoundedContextWithMappings> =
                    Vec::with_capacity(listed.len());
                for bc in listed {
                    let loaded = load_bounded_context_with_mappings(&state.pool, &bc.name)
                        .await
                        .map_err(to_graphql_error)?
                        .expect("just listed this bounded context - it must be readable back");
                    with_mappings.push(loaded);
                }

                Ok(Some(FieldValue::list(
                    with_mappings.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
}
