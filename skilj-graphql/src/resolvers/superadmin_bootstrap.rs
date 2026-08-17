//! `surface SuperadminBootstrap` - `createSuperadmin`, the one mutation
//! with no Role-based actor behind it at all (see `auth::resolve_role`'s
//! own `Ok(None)` case, and `surface SuperadminBootstrap`'s own
//! `@guidance`: "the one surface with no Role-based actor behind it").

use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::shared::generate_token_id;

/// `createSuperadmin(bootstrapSecret: String!, name: String!, externalSubject: String!): CreatedSuperadmin!`,
/// see `gql_types::created_superadmin_object`'s own doc comment for why
/// the return type is deliberately narrower than the full `Role`.
pub fn field() -> Field {
    Field::new(
        "createSuperadmin",
        TypeRef::named_nn("CreatedSuperadmin"),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let presented_secret = ctx.args.try_get("bootstrapSecret")?.string()?.to_string();
                let name = ctx.args.try_get("name")?.string()?.to_string();
                let external_subject = ctx.args.try_get("externalSubject")?.string()?.to_string();

                let Some(bootstrap_secret) = &state.bootstrap_secret else {
                    return Err(async_graphql::Error::new(
                        "an active superadmin Role already exists; the bootstrap secret can \
                         never be used again",
                    ));
                };
                let existing_roles = skilj_core::db::list_roles(&state.pool)
                    .await
                    .map_err(to_graphql_error)?;

                let role = skilj_core::bootstrap::create_superadmin(
                    bootstrap_secret,
                    &presented_secret,
                    name,
                    external_subject,
                    &existing_roles,
                    generate_token_id(),
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::insert_role(&state.pool, &role)
                    .await
                    .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(role)))
            })
        },
    )
    .argument(InputValue::new(
        "bootstrapSecret",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
    .argument(InputValue::new(
        "externalSubject",
        TypeRef::named_nn(TypeRef::STRING),
    ))
}
