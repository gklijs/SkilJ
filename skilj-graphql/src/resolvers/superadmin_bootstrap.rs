//! `surface SuperadminBootstrap` - `createSuperadmin`, the one mutation
//! with no Role-based actor behind it at all (see `auth::resolve_role`'s
//! own `Ok(None)` case, and `surface SuperadminBootstrap`'s own
//! `@guidance`: "the one surface with no Role-based actor behind it").

use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use async_graphql::ErrorExtensions;
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

                // docs/architecture.md §109: held for the whole claim, and
                // the secret consumed once it succeeds.
                let mut gate = state.bootstrap.lock().await;
                let Some(bootstrap_secret) = gate.as_ref() else {
                    return Err(async_graphql::Error::new(
                        "this process holds no bootstrap secret: it was already used, or an \
                         active superadmin existed when the process started. If every superadmin \
                         has since been revoked, restart to generate a new one.",
                    )
                    .extend_with(|_, ext| ext.set("code", "bootstrap_secret_unavailable")));
                };
                let existing_roles = skilj_core::db::list_roles(&state.pool)
                    .await
                    .map_err(to_graphql_error)?;

                let role = match skilj_core::bootstrap::create_superadmin(
                    bootstrap_secret,
                    &presented_secret,
                    name,
                    external_subject,
                    &existing_roles,
                    generate_token_id(),
                    chrono::Utc::now(),
                ) {
                    Ok(role) => role,
                    Err(e) => {
                        // A superadmin exists (made through another
                        // instance's secret): this one has ended too.
                        if skilj_core::error::SkiljRejection::code(&e)
                            == "superadmin_already_exists"
                        {
                            *gate = None;
                        }
                        return Err(to_graphql_error(e));
                    }
                };
                // The rule's "no active superadmin" guard again, atomically
                // with the insert - another instance may have claimed with
                // its own secret since `existing_roles` was read.
                if !skilj_core::db::insert_superadmin_if_none_active(&state.pool, &role)
                    .await
                    .map_err(to_graphql_error)?
                {
                    *gate = None;
                    return Err(to_graphql_error(
                        skilj_core::bootstrap::Error::SuperadminAlreadyExists,
                    ));
                }
                *gate = None;

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
