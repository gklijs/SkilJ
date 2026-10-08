//! `surface BoundedContextDirectory` - `boundedContexts`. `Superadmin`-
//! gated; `bootstrap::list_bounded_contexts` re-checks `caller.superadmin`
//! for real, and passes every context straight through unfiltered (see
//! its own doc comment).

use super::{load_bounded_context_with_mappings, not_found, require_superadmin};
use crate::error::to_graphql_error;
use crate::gql_types::BoundedContextWithMappings;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, TypeRef};

/// `boundedContexts: [BoundedContext!]!`
pub fn field(n: &Naming) -> Field {
    Field::new(
        n.root("boundedContexts"),
        TypeRef::named_nn_list_nn(n.ty("BoundedContext")),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;

                let all_contexts = skilj_core::db::list_bounded_contexts(&state.pool)
                    .await
                    .map_err(to_graphql_error)?;
                let listed = skilj_core::bootstrap::list_bounded_contexts(&caller, &all_contexts)
                    .map_err(to_graphql_error)?;

                let mut with_mappings: Vec<BoundedContextWithMappings> =
                    Vec::with_capacity(listed.len());
                // The widest window of this race in the codebase: an
                // admin-facing "list everything" request that loops over
                // every listed context doing real per-item work, while any
                // one of them (an already-archived context is eligible)
                // can be hard-deleted by a *different* concurrent request
                // at any point during that loop - an ordinary error for
                // that one item, not a panic that takes the whole listing
                // (and the request serving it) down.
                for bc in listed {
                    let loaded = load_bounded_context_with_mappings(&state.pool, &bc.name)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| not_found("BoundedContext", &bc.name))?;
                    with_mappings.push(loaded);
                }

                Ok(Some(FieldValue::list(
                    with_mappings.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
}
