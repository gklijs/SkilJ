//! `surface TokenRevocation` - `revokeToken`, covering every `AccessToken`
//! variant with one mutation. Unlike every other Phase 2 surface, the
//! bounded context the caller needs an admin grant on isn't a caller-
//! supplied argument - it's `token_scope` (`rule RevokeToken`'s own
//! `let`), derived from the token itself (`event_type.bounded_context`
//! for three variants, `command_type.bounded_context` for `CommandToken`)
//! once the token is resolved by id.

use super::{require_admin_mapping, require_caller};
use crate::error::to_graphql_error;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::access_control::AccessToken;
use skilj_core::db::AccessTokenKind;

/// Resolves `id` to its full, typed `AccessToken` plus the bounded
/// context name that scopes it (`token_scope`) - one lookup per variant,
/// since which `db::get_*_token` function to call depends on
/// `access_token_kind`'s own answer. A missing token is refused exactly
/// like one on a bounded context the caller has no admin grant on
/// (`grant_not_active`): answered `not_found`, any caller - with no
/// credential at all, even - could tell which token ids exist
/// (docs/architecture.md §142, the §138/§139 principle).
async fn resolve_token(
    pool: &skilj_core::db::Pool,
    id: &str,
) -> async_graphql::Result<(AccessToken, String)> {
    let missing = || to_graphql_error(skilj_core::access_control::Error::GrantNotActive);
    let kind = skilj_core::db::access_token_kind(pool, id)
        .await
        .map_err(to_graphql_error)?
        .ok_or_else(missing)?;

    Ok(match kind {
        AccessTokenKind::ExternalEvent => {
            let token = skilj_core::db::get_external_event_token(pool, id)
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(missing)?;
            let scope = token.event_type.bounded_context.name.clone();
            (AccessToken::ExternalEventToken(token), scope)
        }
        AccessTokenKind::DirectCreation => {
            let token = skilj_core::db::get_direct_creation_token(pool, id)
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(missing)?;
            let scope = token.event_type.bounded_context.name.clone();
            (AccessToken::DirectCreationToken(token), scope)
        }
        AccessTokenKind::EventRead => {
            let token = skilj_core::db::get_event_read_token(pool, id)
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(missing)?;
            let scope = token.event_type.bounded_context.name.clone();
            (AccessToken::EventReadToken(token), scope)
        }
        AccessTokenKind::Command => {
            let token = skilj_core::db::get_command_token(pool, id)
                .await
                .map_err(to_graphql_error)?
                .ok_or_else(missing)?;
            let scope = token.command_type.bounded_context.name.clone();
            (AccessToken::CommandToken(token), scope)
        }
    })
}

/// `revokeToken(tokenId: ID!): AccessToken!` - the return type is the
/// `AccessToken` union (`gql_types::access_token_union`), tagged via
/// `FieldValue::with_type` per variant, the same pattern
/// `async_graphql::dynamic::Field`'s own doc example uses.
pub fn revoke_token_field(n: &Naming) -> Field {
    Field::new(
        n.root("revokeToken"),
        TypeRef::named_nn(n.ty("AccessToken")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let token_id = ctx.args.try_get("tokenId")?.string()?.to_string();

                // Authenticated before the token is looked up (§142).
                require_caller(&ctx)?;
                let (token, bounded_context_name) = resolve_token(&state.pool, &token_id).await?;
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let revoked = skilj_core::access_control::revoke_token(
                    &access_mapping,
                    &token,
                    chrono::Utc::now(),
                )
                .map_err(to_graphql_error)?;
                skilj_core::db::revoke_access_token(&state.pool, &token_id, chrono::Utc::now())
                    .await
                    .map_err(to_graphql_error)?;

                let field_value =
                    match revoked {
                        AccessToken::ExternalEventToken(t) => FieldValue::owned_any(t)
                            .with_type(state.naming.ty("ExternalEventToken")),
                        AccessToken::DirectCreationToken(t) => FieldValue::owned_any(t)
                            .with_type(state.naming.ty("DirectCreationToken")),
                        AccessToken::EventReadToken(t) => {
                            FieldValue::owned_any(t).with_type(state.naming.ty("EventReadToken"))
                        }
                        AccessToken::CommandToken(t) => {
                            FieldValue::owned_any(t).with_type(state.naming.ty("CommandToken"))
                        }
                    };
                Ok(Some(field_value))
            })
        },
    )
    .argument(InputValue::new("tokenId", TypeRef::named_nn(TypeRef::ID)))
}
