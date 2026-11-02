//! `setPayloadLimits` (issue #132, docs/architecture.md §238): a
//! superadmin overrides how large a stored payload may be in one bounded
//! context. Superadmin-only, like adding and deleting a bounded context -
//! see `bootstrap::set_payload_limits`'s doc comment.

use super::{not_found, require_superadmin};
use crate::error::to_graphql_error;
use crate::gql_types::PayloadLimitsOverview;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};

/// `setPayloadLimits(boundedContext: String!, maxPayloadBytes: Int,
/// warnPayloadBytes: Int): PayloadLimits!` - sets both halves of the
/// override as given: an argument left out or null clears that half, back
/// to the builder's value. A write already holding the bounded context's
/// lock finishes under the old limits; the next one sees the new.
pub fn set_payload_limits_field(n: &Naming) -> Field {
    Field::new(
        n.root("setPayloadLimits"),
        TypeRef::named_nn(n.ty("PayloadLimits")),
        |ctx| {
            FieldFuture::new(async move {
                let caller = require_superadmin(&ctx)?;
                let state = ctx.data::<GraphqlState>()?;
                let name = ctx.args.try_get("boundedContext")?.string()?.to_string();
                let bytes = |arg: &str| -> async_graphql::Result<Option<i64>> {
                    match ctx.args.get(arg) {
                        Some(value) if !value.is_null() => Ok(Some(value.i64()?)),
                        _ => Ok(None),
                    }
                };
                let stored = skilj_core::db::PayloadLimitsOverride {
                    max_payload_bytes: bytes("maxPayloadBytes")?,
                    warn_payload_bytes: bytes("warnPayloadBytes")?,
                };

                skilj_core::bootstrap::set_payload_limits(&caller, &stored)
                    .map_err(to_graphql_error)?;
                skilj_core::db::get_bounded_context(&state.pool, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("BoundedContext", &name))?;
                skilj_core::db::set_payload_limits(&state.pool, &name, stored)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(PayloadLimitsOverview {
                    effective: stored.apply(state.projection_dispatcher.payload_limits()),
                    bounded_context: name,
                    stored,
                })))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "maxPayloadBytes",
        TypeRef::named(TypeRef::INT),
    ))
    .argument(InputValue::new(
        "warnPayloadBytes",
        TypeRef::named(TypeRef::INT),
    ))
}
