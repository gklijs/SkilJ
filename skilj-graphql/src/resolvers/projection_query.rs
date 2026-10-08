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
use crate::naming::Naming;
use crate::projection_types::graphql_type_name;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use async_graphql::ErrorExtensions;
use skilj_core::access_control::RoleAccessMapping;
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
pub(crate) async fn wait_until_caught_up(
    pool: &Pool,
    bounded_context: &str,
    projection_name: &str,
    wait_for_sequence: i64,
    timeout: std::time::Duration,
) -> skilj_core::error::Result<bool> {
    // docs/architecture.md §88: one column per poll, and a backoff - the
    // first checks come quickly (an async projection's next tick is often
    // imminent), later ones less often, so a query that waits out the
    // whole timeout costs a few dozen reads rather than hundreds.
    const FIRST_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);
    const MAX_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut interval = FIRST_POLL_INTERVAL;
    loop {
        let caught_up_to =
            skilj_core::db::projection_caught_up_to(pool, bounded_context, projection_name).await?;
        if caught_up_to.flatten().unwrap_or(-1) >= wait_for_sequence {
            return Ok(true);
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(interval.min(deadline - now)).await;
        interval = (interval * 2).min(MAX_POLL_INTERVAL);
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
/// no purchase history yet is a legitimate, common case ([§9](../../../docs/architecture.md#next-steps)'s "keyed /
/// multi-row Projections" pass) - unless `query_projection` below
/// rejects it first: for an owner-declaring projection, an untouched key
/// is exactly the "unestablished owner" case a `scope`-restricted grant
/// fails closed on (cross-tenant projection read fix,
/// docs/architecture.md's own write-up of this pass).
/// `field()`/`resolvers::projection_subscription`'s shared read path -
/// `team_only` gate, `get_projection`, `wait_until_caught_up`,
/// `get_projection_state_and_owner`, decrypt-on-read, and
/// `projections::query_projection`, all as `field()` used to have them
/// inlined. Pulled out so the new `projectionUpdates` subscription
/// (Codeberg issue #23) can push through the *identical* authorization/
/// decrypt path the plain query already has coverage for, instead of a
/// second hand-rolled copy that could silently drift from it - both the
/// subscription's initial snapshot and every one of its per-event
/// refetches call this, never anything else.
///
/// `access_mapping` is the caller's: `field()` fetches it once via
/// `require_read_mapping`, while the subscription re-fetches a fresh one
/// on every push (never trusting the mapping captured at subscribe time,
/// the same rule `event_subscription.rs`'s own per-delivery re-check
/// already follows) - so this function takes it as a parameter rather
/// than fetching it itself.
pub(crate) async fn fetch_projection_result(
    state: &GraphqlState,
    access_mapping: &RoleAccessMapping,
    bounded_context_name: &str,
    name: &str,
    key: &str,
    wait_for_sequence: Option<i64>,
) -> async_graphql::Result<(crate::projection_types::ProjectionInstance, String)> {
    // `team_only` is a plain in-memory `ProjectionDispatcher` lookup, no
    // DB round trip - checked first and rejected outright before any of
    // the DB work below runs, rather than waiting for `query_projection`'s
    // own copy of this same check at the very end. A caller who was never
    // going to be authorized shouldn't pay for `get_projection`, the
    // `waitForSequence` poll loop (up to `state.
    // projection_query_wait_timeout`), the state fetch, or a
    // sensitive-field decrypt just to be told no.
    let team_only = state
        .projection_dispatcher
        .team_only(bounded_context_name, name)
        .flatten();
    if !skilj_core::access_control::role_matches_required_team(&access_mapping.role, team_only) {
        return Err(to_graphql_error(
            skilj_core::access_control::Error::NotOnRequiredTeam,
        ));
    }

    let projection = skilj_core::db::get_projection(&state.pool, bounded_context_name, name)
        .await
        .map_err(to_graphql_error)?
        .ok_or_else(|| not_found("Projection", name))?;

    // docs/architecture.md §88: `waitForSequence` names a sequence the
    // caller already knows, i.e. one already committed. One past the
    // latest committed sequence can't be, and would only ever wait out
    // the whole timeout - refused at once instead.
    if let Some(seq) = wait_for_sequence {
        let latest = skilj_core::db::latest_sequence(&state.pool, bounded_context_name)
            .await
            .map_err(to_graphql_error)?
            .unwrap_or(-1);
        if seq > latest {
            return Err(async_graphql::Error::new(format!(
                "waitForSequence {seq} is past the latest committed sequence {latest} of \
                 {bounded_context_name:?} - pass a sequence already committed, such as one \
                 a submitted command triggered"
            ))
            .extend_with(|_, ext| ext.set("code", "wait_for_sequence_not_committed")));
        }
    }
    let caught_up = match wait_for_sequence {
        None => true,
        Some(seq) => wait_until_caught_up(
            &state.pool,
            bounded_context_name,
            name,
            seq,
            state.projection_query_wait_timeout,
        )
        .await
        .map_err(to_graphql_error)?,
    };

    let stored = skilj_core::db::get_projection_state_and_owner(
        &state.pool,
        bounded_context_name,
        name,
        key,
    )
    .await
    .map_err(to_graphql_error)?;
    // Cross-tenant projection read fix (docs/architecture.md's own
    // write-up of this pass): `instance_owner` is this row's own `owner`
    // column, `None` for a row that doesn't exist yet - the same "no
    // proven owner" treatment either way, decided by `query_projection`
    // below, not here.
    let instance_owner = stored.as_ref().and_then(|(_, owner)| owner.clone());
    let state_json = stored
        .map(|(state_json, _)| state_json)
        .or_else(|| {
            state
                .projection_dispatcher
                .default_state(bounded_context_name, name)
        })
        .unwrap_or_else(|| "{}".to_string());
    let owner_tag_key = state
        .projection_dispatcher
        .owner_tag_key(bounded_context_name, name)
        .flatten();

    // Real decrypt-on-read - automatic, no `Projection.sensitive_fields`
    // declaration anywhere (see `field()`'s own doc comment). Grant
    // checked once, up front: an ungranted caller's query never needs a
    // master key, or even a DB round trip for one, at all.
    let data_keys = if skilj_core::event_store::sensitive_field_is_granted(access_mapping, key) {
        skilj_core::db::list_active_data_keys_for_subject_value(
            &state.pool,
            bounded_context_name,
            key,
            state.encryption_master_key.as_ref(),
        )
        .await
        .map_err(to_graphql_error)?
    } else {
        Vec::new()
    };
    let state_json = skilj_core::projections::read_projection(&state_json, &data_keys);

    let result = skilj_core::projections::query_projection(
        access_mapping,
        &projection,
        key,
        wait_for_sequence,
        caught_up,
        skilj_core::projections::ProjectionAccessScope {
            declares_owner: owner_tag_key.is_some(),
            instance_owner: instance_owner.as_deref(),
            team_only,
        },
        state_json,
    )
    .map_err(to_graphql_error)?;

    let value: serde_json::Value = serde_json::from_str(&result)
        .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));

    Ok((
        crate::projection_types::ProjectionInstance {
            key: key.to_string(),
            state: value,
        },
        state
            .naming
            .ty(&graphql_type_name(bounded_context_name, name)),
    ))
}

/// [`crate::projection_types::AdmittedProjections::require`], except that
/// a projection that doesn't exist at all is still `Projection_not_found`
/// rather than `projection_not_in_schema` - only the refusal path pays
/// for the lookup that tells the two apart.
pub(crate) async fn require_admitted(
    ctx: &async_graphql::dynamic::ResolverContext<'_>,
    state: &GraphqlState,
    bounded_context_name: &str,
    name: &str,
) -> async_graphql::Result<()> {
    let Err(refused) = ctx
        .data::<crate::projection_types::AdmittedProjections>()?
        .require(bounded_context_name, name)
    else {
        return Ok(());
    };
    skilj_core::db::get_projection(&state.pool, bounded_context_name, name)
        .await
        .map_err(to_graphql_error)?
        .ok_or_else(|| not_found("Projection", name))?;
    Err(refused)
}

pub fn field(n: &Naming) -> Field {
    Field::new(
        n.root("projection"),
        TypeRef::named_nn(n.ty("ProjectionResult")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let name = ctx.args.try_get("name")?.string()?.to_string();
                require_admitted(&ctx, state, &bounded_context_name, &name).await?;
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

                let (value, type_name) = fetch_projection_result(
                    state,
                    &access_mapping,
                    &bounded_context_name,
                    &name,
                    &key,
                    wait_for_sequence,
                )
                .await?;

                Ok(Some(FieldValue::owned_any(value).with_type(type_name)))
            })
        },
    )
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

/// `_entities` (docs/architecture.md §194): the projection instances a
/// router names by type and `projectionKey`, each looked up as
/// `projection(boundedContext, name, key)` would be for the same caller,
/// with every check that field makes (`@guarantee PublishingGrantsNothing`)
/// and no `waitForSequence`. One representation that is refused, or names
/// a type that isn't a projection, fails the whole answer with its error:
/// async-graphql's dynamic schema has no `null` for one item of a list of
/// union values.
pub fn entity_resolver(ctx: async_graphql::dynamic::ResolverContext<'_>) -> FieldFuture<'_> {
    FieldFuture::new(async move {
        let state = ctx.data::<GraphqlState>()?;
        let representations = ctx.args.try_get("representations")?.list()?;
        // A router sends many instances of one type at once: the caller's
        // grant on each bounded context is looked up once, not per
        // instance.
        let mut mappings: std::collections::HashMap<String, RoleAccessMapping> =
            std::collections::HashMap::new();
        let mut values = Vec::with_capacity(representations.len());
        for representation in representations.iter() {
            let representation = representation.object()?;
            let type_name = representation.try_get("__typename")?.string()?;
            let key = representation
                .try_get(crate::federation::PROJECTION_KEY_FIELD)?
                .string()?
                .to_string();
            let (bounded_context_name, name) = ctx
                .data::<crate::projection_types::AdmittedProjections>()
                .ok()
                .and_then(|admitted| admitted.by_type_name(type_name))
                .cloned()
                .ok_or_else(|| not_found("Projection type", type_name))?;
            let access_mapping = match mappings.get(&bounded_context_name) {
                Some(mapping) => mapping.clone(),
                None => {
                    let mapping =
                        require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                    mappings.insert(bounded_context_name.clone(), mapping.clone());
                    mapping
                }
            };
            let (instance, type_name) = fetch_projection_result(
                state,
                &access_mapping,
                &bounded_context_name,
                &name,
                &key,
                None,
            )
            .await?;
            values.push(FieldValue::owned_any(instance).with_type(type_name));
        }
        Ok(Some(FieldValue::list(values)))
    })
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
///
/// `TEAM_ONLY`-gated exactly like the `projection` field above (Codeberg
/// issue #17): a projection's own declared schema is whole-projection
/// data, the same thing the gate protects there, so a Role not on the
/// required team gets the identical rejection here rather than being
/// able to learn the projection's name/schema/version through this
/// field while `projection` itself refuses it - `TeamGatedWhenDeclared`'s
/// own "invisible, not merely unreadable" promise would otherwise hold
/// for one field and not the other on the same surface.
pub fn schema_field(n: &Naming) -> Field {
    Field::new(
        n.root("projectionSchema"),
        TypeRef::named(n.ty("Projection")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let name = ctx.args.try_get("name")?.string()?.to_string();

                let team_only = state
                    .projection_dispatcher
                    .team_only(&bounded_context_name, &name)
                    .flatten();
                if !skilj_core::access_control::role_matches_required_team(
                    &access_mapping.role,
                    team_only,
                ) {
                    return Err(to_graphql_error(
                        skilj_core::access_control::Error::NotOnRequiredTeam,
                    ));
                }

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
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
}
