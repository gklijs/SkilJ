//! `surface SubjectErasure` - the one mutation, `forgetSubject`.
//! `AdminAccess`-gated (`require_admin_mapping`), matching
//! `ForgetSubject`'s own `requires: access_mapping.level = admin` and
//! every other Phase 2 mutation. No `exposes` list on this surface at
//! all - there is no query to list `EncryptionKey`s, only this one
//! write, so `EncryptionKey` itself is only ever reachable as this
//! mutation's own return value.

use super::{not_found, require_admin_mapping};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};

/// `forgetSubject(boundedContext: String!, subjectKey: String!, subjectValue: String!): EncryptionKey!`
///
/// "There is no un-forget: destroying an `EncryptionKey` is irreversible
/// by design" (the surface's own `@guidance`) - `db::destroy_encryption_key`
/// nulls the wrapped data key in the same statement that flips
/// `status`/`destroyed_at`, real crypto-shredding rather than a status
/// flag alone (see `skilj_core::encryption`'s own doc comment).
pub fn field() -> Field {
    Field::new("forgetSubject", TypeRef::named_nn("EncryptionKey"), |ctx| {
        FieldFuture::new(async move {
            let state = ctx.data::<GraphqlState>()?;
            let bounded_context_name = ctx.args.try_get("boundedContext")?.string()?.to_string();
            let access_mapping =
                require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
            let subject_key = ctx.args.try_get("subjectKey")?.string()?.to_string();
            let subject_value = ctx.args.try_get("subjectValue")?.string()?.to_string();

            let key = skilj_core::db::get_active_encryption_key(
                &state.pool,
                &bounded_context_name,
                &subject_key,
                &subject_value,
            )
            .await
            .map_err(to_graphql_error)?
            .ok_or_else(|| not_found("EncryptionKey", &format!("{subject_key}/{subject_value}")))?;

            let now = chrono::Utc::now();
            let destroyed = skilj_core::event_store::forget_subject(&access_mapping, &key, now)
                .map_err(to_graphql_error)?;
            skilj_core::db::destroy_encryption_key(
                &state.pool,
                &bounded_context_name,
                &subject_key,
                &subject_value,
                now,
            )
            .await
            .map_err(to_graphql_error)?;

            Ok(Some(FieldValue::owned_any(destroyed)))
        })
    })
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "subjectKey",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "subjectValue",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}
