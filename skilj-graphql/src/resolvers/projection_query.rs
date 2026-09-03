//! `surface ProjectionQuery` - `projection` (the data) and
//! `projectionSchema` (the declared shape, drift audit finding #9 -
//! 2026-08-20, see project memory `skilj-drift-audit-2026-08-20`). Both
//! `ReadAccess`-gated (any active level - `require_read_mapping`, not
//! `require_admin_mapping`). See `crate::projection_types` for how the
//! `ProjectionResult` union and its per-projection member types are
//! generated (`projection`'s own concern - `projectionSchema` doesn't
//! touch it at all, see that field's own doc comment).

use super::{not_found, require_read_mapping};
use crate::error::to_graphql_error;
use crate::gql_types::ProjectionWithRebuild;
use crate::projection_types::graphql_type_name;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::db::Pool;

/// `await_projection_caught_up(projection, wait_for_sequence)`'s real
/// implementation (the black box `skilj_core::projections::query_projection`
/// leaves to its caller - see that function's own doc comment): polls
/// `Projection.caught_up_to` until it reaches `wait_for_sequence` or
/// `timeout` elapses. Trivially fast for a sync projection - its
/// `caught_up_to` is already current the moment the write transaction
/// that produced `wait_for_sequence` committed (see
/// `db::insert_event_and_update_sync_projections`), so the very first
/// poll already succeeds; for an async one it may need to wait out one
/// or more of the background consumer's own poll ticks
/// (`db::catch_up_bounded_context`).
async fn wait_until_caught_up(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    wait_for_sequence: i64,
    timeout: std::time::Duration,
) -> skilj_core::error::Result<bool> {
    const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let projection =
            skilj_core::db::get_projection(pool, bounded_context, projection_name).await?;
        if let Some(projection) = &projection {
            if projection.caught_up_to.unwrap_or(-1) >= wait_for_sequence {
                return Ok(true);
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// `projection(boundedContext: String!, name: String!, key: String, waitForSequence: Int): ProjectionResult!`
///
/// `read_projection` is real now: `sensitive_field_is_granted` (the
/// identical two-grant test `render_event` already has, reused unchanged)
/// decides, once per query, whether this caller is entitled to see
/// anything decrypted for this instance's own `key` at all - if so,
/// `db::list_active_data_keys_for_subject_value` resolves every active
/// `EncryptionKey` for it, and `skilj_core::projections::read_projection`
/// tries them against every string leaf in the stored state, at any
/// depth. No `Projection.sensitive_fields` declaration anywhere - see
/// `skilj_core::encryption::decrypt_ciphertext_leaves`'s own doc comment
/// for why automatic detection was chosen over a declared one (a
/// forgotten declaration would silently leak real content to every
/// caller, regardless of grant).
///
/// `key` omitted defaults to `""` (the rule's own `instance_key = key ?? ""`),
/// the single implicit instance a projection that never overrides
/// `Projection::keys()` always has, so an existing single-value
/// projection's own callers see no change at all from this argument
/// existing. A key nothing has touched yet answers with
/// `ProjectionDispatcher::default_state` (the same value a fresh instance
/// lazily starts from) rather than a "not found" error - a customer with
/// no purchase history yet is a legitimate, common case (§9's "keyed /
/// multi-row Projections" pass) - unless `query_projection` below
/// rejects it first: for an owner-declaring projection, an untouched key
/// is exactly the "unestablished owner" case a `scope`-restricted grant
/// fails closed on (cross-tenant projection read fix,
/// docs/architecture.md's own write-up of this pass).
pub fn field() -> Field {
    Field::new("projection", TypeRef::named_nn("ProjectionResult"), |ctx| {
        FieldFuture::new(async move {
            let state = ctx.data::<GraphqlState>()?;
            let bounded_context_name = ctx.args.try_get("boundedContext")?.string()?.to_string();
            let access_mapping =
                require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
            let name = ctx.args.try_get("name")?.string()?.to_string();
            let key = ctx
                .args
                .get("key")
                .filter(|v| !v.is_null())
                .map(|v| v.string().map(str::to_string))
                .transpose()?
                .unwrap_or_default();
            let wait_for_sequence = ctx
                .args
                .get("waitForSequence")
                .filter(|v| !v.is_null())
                .map(|v| v.i64())
                .transpose()?;

            let projection =
                skilj_core::db::get_projection(&state.pool, &bounded_context_name, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("Projection", &name))?;

            let caught_up = match wait_for_sequence {
                None => true,
                Some(seq) => wait_until_caught_up(
                    &state.pool,
                    &bounded_context_name,
                    &name,
                    seq,
                    state.projection_query_wait_timeout,
                )
                .await
                .map_err(to_graphql_error)?,
            };

            let stored = skilj_core::db::get_projection_state_and_owner(
                &state.pool,
                &bounded_context_name,
                &name,
                &key,
            )
            .await
            .map_err(to_graphql_error)?;
            // Cross-tenant projection read fix (docs/architecture.md's
            // own write-up of this pass): `instance_owner` is this row's
            // own `owner` column, `None` for a row that doesn't exist yet
            // - the same "no proven owner" treatment either way, decided
            // by `query_projection` below, not here.
            let instance_owner = stored.as_ref().and_then(|(_, owner)| owner.clone());
            let state_json = stored
                .map(|(state_json, _)| state_json)
                .or_else(|| {
                    state
                        .projection_dispatcher
                        .default_state(&bounded_context_name, &name)
                })
                .unwrap_or_else(|| "{}".to_string());
            let owner_tag_key = state
                .projection_dispatcher
                .owner_tag_key(&bounded_context_name, &name)
                .flatten();

            // Real decrypt-on-read - automatic, no `Projection.sensitive_fields`
            // declaration anywhere (see this field's own doc comment).
            // Grant checked once, up front: an ungranted caller's query
            // never needs a master key, or even a DB round trip for one,
            // at all.
            let data_keys =
                if skilj_core::event_store::sensitive_field_is_granted(&access_mapping, &key) {
                    skilj_core::db::list_active_data_keys_for_subject_value(
                        &state.pool,
                        &bounded_context_name,
                        &key,
                        state.encryption_master_key.as_ref(),
                    )
                    .await
                    .map_err(to_graphql_error)?
                } else {
                    Vec::new()
                };
            let state_json = skilj_core::projections::read_projection(&state_json, &data_keys);

            let result = skilj_core::projections::query_projection(
                &access_mapping,
                &projection,
                &key,
                wait_for_sequence,
                caught_up,
                owner_tag_key.is_some(),
                instance_owner.as_deref(),
                state_json,
            )
            .map_err(to_graphql_error)?;

            let value: serde_json::Value = serde_json::from_str(&result)
                .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));

            Ok(Some(FieldValue::owned_any(value).with_type(
                graphql_type_name(&bounded_context_name, &name),
            )))
        })
    })
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
    .argument(InputValue::new("key", TypeRef::named(TypeRef::STRING)))
    .argument(InputValue::new(
        "waitForSequence",
        TypeRef::named(TypeRef::INT),
    ))
}

/// `projectionSchema(boundedContext: String!, name: String!): Projection` -
/// `ProjectionQuery`'s own `exposes: projection.name/schema/schema_version`
/// (drift audit finding #9, 2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): before this field existed, the only
/// way to see a `Projection`'s own declared schema was `TypeRegistration`'s
/// `projections` query, `AdminAccess`-gated - a strictly stronger grant
/// than `ProjectionQuery`'s own `facing access_mapping: ReadAccess`
/// promises, so a plain read-level caller genuinely could not reach this
/// data through any surface at all, contradicting the spec's own contract.
///
/// Reuses the already-registered `"Projection"` GraphQL object type
/// unchanged (`gql_types::projection_object`) rather than inventing a
/// second one - the type itself is generic over any caller's own grant
/// level; only which resolver a query goes through decides what's
/// reachable. `pendingRebuild`/`buildingRebuild` are always `null` here,
/// deliberately: rebuild status is `TypeRegistration`'s own concern, not
/// named in `ProjectionQuery`'s own `exposes` clause, so a `ReadAccess`
/// caller querying those two fields through this field gets nothing
/// rather than a second, narrower gate response readers would have to
/// reason about differently from `projections`' own.
pub fn schema_field() -> Field {
    Field::new("projectionSchema", TypeRef::named("Projection"), |ctx| {
        FieldFuture::new(async move {
            let state = ctx.data::<GraphqlState>()?;
            let bounded_context_name = ctx.args.try_get("boundedContext")?.string()?.to_string();
            require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
            let name = ctx.args.try_get("name")?.string()?.to_string();

            let projection =
                skilj_core::db::get_projection(&state.pool, &bounded_context_name, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("Projection", &name))?;

            Ok(Some(FieldValue::owned_any(ProjectionWithRebuild {
                projection,
                pending_rebuild: None,
                building_rebuild: None,
            })))
        })
    })
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
}
