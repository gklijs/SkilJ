//! `surface EventQuery` - `queryEvents`, `countEvents`, `inspectEvent`.
//! `AdminAccess`-gated, via the shared `require_admin_mapping` helper.

use super::{not_found, require_admin_mapping, resolve_read_data_keys};
use crate::error::to_graphql_error;
use crate::gql_types::InspectedEventData;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::shared::Tag;

/// Parses a `[TagInput!]` argument (nullable - absent means "no tag
/// restriction", matching `query_events`'/`count_events`'s own
/// `Option<&[Tag]>`) into `Vec<Tag>`.
fn parse_tags(
    value: Option<async_graphql::dynamic::ValueAccessor>,
) -> async_graphql::Result<Option<Vec<Tag>>> {
    let Some(value) = value.filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let mut tags = Vec::new();
    for item in value.list()?.iter() {
        let obj = item.object()?;
        let key = obj.try_get("key")?.string()?.to_string();
        let value = match obj.get("value").filter(|v| !v.is_null()) {
            Some(v) => Some(v.string()?.to_string()),
            None => None,
        };
        tags.push(Tag { key, value });
    }
    Ok(Some(tags))
}

/// `queryEvents(boundedContext: String!, eventTypes: [String!]!, tags: [TagInput!], afterSequence: Int, correlationId: String): [QueriedEvent!]!`
pub fn query_events_field() -> Field {
    Field::new(
        "queryEvents",
        TypeRef::named_nn_list_nn("QueriedEvent"),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let mut event_types = Vec::new();
                for item in ctx.args.try_get("eventTypes")?.list()?.iter() {
                    let name = item.string()?.to_string();
                    let et =
                        skilj_core::db::get_event_type(&state.pool, &bounded_context_name, &name)
                            .await
                            .map_err(to_graphql_error)?
                            .ok_or_else(|| not_found("EventType", &name))?;
                    event_types.push(et);
                }
                let tags = parse_tags(ctx.args.get("tags"))?;
                let after_sequence = ctx
                    .args
                    .get("afterSequence")
                    .filter(|v| !v.is_null())
                    .map(|v| v.i64())
                    .transpose()?;
                // Codeberg issue #18 - "show me everything in this
                // transaction".
                let correlation_id = ctx
                    .args
                    .get("correlationId")
                    .filter(|v| !v.is_null())
                    .and_then(|v| v.string().ok())
                    .map(|s| s.to_string());

                // docs/architecture.md §19's "Problem 1" fix: a non-empty
                // `tags` filter can go straight to the tag-indexed query
                // instead of pulling the whole bounded context and
                // filtering in memory - `query_events`'s own
                // `event_types`/`tags` filters below still run
                // unchanged, just over an already-narrowed candidate set
                // rather than everything. No `tags` filter (the common
                // "browse everything" case, or an `event_types`-only
                // filter) keeps the existing full fetch - narrowing that
                // case is a separate, unaddressed optimisation.
                let bounded_context_events = match tags.as_deref() {
                    Some(wanted) if !wanted.is_empty() => {
                        skilj_core::db::list_events_for_bounded_context_matching_tags_cached(
                            &state.pool,
                            &state.event_cache,
                            &bounded_context_name,
                            wanted,
                            after_sequence,
                        )
                        .await
                        .map_err(to_graphql_error)?
                    }
                    _ => skilj_core::db::list_events_for_bounded_context_cached(
                        &state.pool,
                        &state.event_cache,
                        &bounded_context_name,
                        after_sequence.unwrap_or(-1),
                    )
                    .await
                    .map_err(to_graphql_error)?,
                };

                // Decrypt-on-read's own pre-resolution step - scoped to
                // events matching the caller's own `eventTypes` argument
                // (the cheapest part of query_events' own filter to
                // replicate here without duplicating the whole thing);
                // `tags`/`afterSequence` may narrow the actual results
                // further, so this may resolve a few more keys than
                // strictly needed, never fewer - no correctness impact.
                let mut data_keys = std::collections::HashMap::new();
                for e in bounded_context_events.iter().filter(|e| {
                    e.bounded_context == access_mapping.bounded_context
                        && (event_types.is_empty() || event_types.contains(&e.event_type))
                }) {
                    resolve_read_data_keys(
                        &state.pool,
                        &bounded_context_name,
                        &e.event_type.sensitive_fields,
                        &e.payload,
                        &access_mapping,
                        state.encryption_master_key.as_ref(),
                        &mut data_keys,
                    )
                    .await?;
                }

                let private_field_grants =
                    super::load_private_field_grants(&state.pool, &bounded_context_name).await?;
                let results = skilj_core::event_store::query_events(
                    &access_mapping,
                    &event_types,
                    tags.as_deref(),
                    after_sequence,
                    correlation_id.as_deref(),
                    &bounded_context_events,
                    |sk, sv| data_keys.get(&(sk.to_string(), sv.to_string())).cloned(),
                    &private_field_grants,
                )
                .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::list(
                    results.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "eventTypes",
        TypeRef::named_nn_list_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("tags", TypeRef::named_nn_list("TagInput")))
    .argument(InputValue::new(
        "afterSequence",
        TypeRef::named(TypeRef::INT),
    ))
    .argument(InputValue::new(
        "correlationId",
        TypeRef::named(TypeRef::STRING),
    ))
}

/// `countEvents(boundedContext: String!, eventTypes: [String!]!, tags: [TagInput!], correlationId: String): Int!`
pub fn count_events_field() -> Field {
    Field::new("countEvents", TypeRef::named_nn(TypeRef::INT), |ctx| {
        FieldFuture::new(async move {
            let state = ctx.data::<GraphqlState>()?;
            let bounded_context_name = ctx.args.try_get("boundedContext")?.string()?.to_string();
            let access_mapping =
                require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

            let mut event_types = Vec::new();
            for item in ctx.args.try_get("eventTypes")?.list()?.iter() {
                let name = item.string()?.to_string();
                let et = skilj_core::db::get_event_type(&state.pool, &bounded_context_name, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("EventType", &name))?;
                event_types.push(et);
            }
            let tags = parse_tags(ctx.args.get("tags"))?;
            let correlation_id = ctx
                .args
                .get("correlationId")
                .filter(|v| !v.is_null())
                .and_then(|v| v.string().ok())
                .map(|s| s.to_string());

            // See `query_events_field`'s own identical comment - same
            // §19 "Problem 1" fix, same tags-supplied-or-not branch.
            let bounded_context_events = match tags.as_deref() {
                Some(wanted) if !wanted.is_empty() => {
                    skilj_core::db::list_events_for_bounded_context_matching_tags_cached(
                        &state.pool,
                        &state.event_cache,
                        &bounded_context_name,
                        wanted,
                        None,
                    )
                    .await
                    .map_err(to_graphql_error)?
                }
                _ => skilj_core::db::list_events_for_bounded_context_cached(
                    &state.pool,
                    &state.event_cache,
                    &bounded_context_name,
                    -1,
                )
                .await
                .map_err(to_graphql_error)?,
            };

            let count = skilj_core::event_store::count_events(
                &access_mapping,
                &event_types,
                tags.as_deref(),
                correlation_id.as_deref(),
                &bounded_context_events,
            )
            .map_err(to_graphql_error)?;

            Ok(Some(async_graphql::Value::from(count)))
        })
    })
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "eventTypes",
        TypeRef::named_nn_list_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("tags", TypeRef::named_nn_list("TagInput")))
    .argument(InputValue::new(
        "correlationId",
        TypeRef::named(TypeRef::STRING),
    ))
}

/// `inspectEvent(boundedContext: String!, sequence: Int!): InspectedEvent!`
pub fn inspect_event_field() -> Field {
    Field::new("inspectEvent", TypeRef::named_nn("InspectedEvent"), |ctx| {
        FieldFuture::new(async move {
            let state = ctx.data::<GraphqlState>()?;
            let bounded_context_name = ctx.args.try_get("boundedContext")?.string()?.to_string();
            let access_mapping =
                require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
            let sequence = ctx.args.try_get("sequence")?.i64()?;

            let event = skilj_core::db::get_event_by_sequence_cached(
                &state.pool,
                &state.event_cache,
                &bounded_context_name,
                sequence,
            )
            .await
            .map_err(to_graphql_error)?
            .ok_or_else(|| not_found("Event", &sequence.to_string()))?;

            let mut data_keys = std::collections::HashMap::new();
            resolve_read_data_keys(
                &state.pool,
                &bounded_context_name,
                &event.event_type.sensitive_fields,
                &event.payload,
                &access_mapping,
                state.encryption_master_key.as_ref(),
                &mut data_keys,
            )
            .await?;

            let private_field_grants =
                super::load_private_field_grants(&state.pool, &bounded_context_name).await?;
            let inspected = skilj_core::event_store::inspect_event(
                &access_mapping,
                &event,
                |sk, sv| data_keys.get(&(sk.to_string(), sv.to_string())).cloned(),
                &private_field_grants,
            )
            .map_err(to_graphql_error)?;

            Ok(Some(FieldValue::owned_any(InspectedEventData {
                event: inspected.event,
                rendered_payload: inspected.rendered_payload,
            })))
        })
    })
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("sequence", TypeRef::named_nn(TypeRef::INT)))
}
