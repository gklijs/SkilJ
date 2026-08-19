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

/// `queryEvents(boundedContext: String!, eventTypes: [String!]!, tags: [TagInput!], afterSequence: Int): [QueriedEvent!]!`
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

                let bounded_context_events =
                    skilj_core::db::list_events_for_bounded_context_cached(
                        &state.pool,
                        &state.event_cache,
                        &bounded_context_name,
                        after_sequence.unwrap_or(-1),
                    )
                    .await
                    .map_err(to_graphql_error)?;

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

                let results = skilj_core::event_store::query_events(
                    &access_mapping,
                    &event_types,
                    tags.as_deref(),
                    after_sequence,
                    &bounded_context_events,
                    |sk, sv| data_keys.get(&(sk.to_string(), sv.to_string())).cloned(),
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
}

/// `countEvents(boundedContext: String!, eventTypes: [String!]!, tags: [TagInput!]): Int!`
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

            let bounded_context_events = skilj_core::db::list_events_for_bounded_context_cached(
                &state.pool,
                &state.event_cache,
                &bounded_context_name,
                -1,
            )
            .await
            .map_err(to_graphql_error)?;

            let count = skilj_core::event_store::count_events(
                &access_mapping,
                &event_types,
                tags.as_deref(),
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

            let inspected =
                skilj_core::event_store::inspect_event(&access_mapping, &event, |sk, sv| {
                    data_keys.get(&(sk.to_string(), sv.to_string())).cloned()
                })
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
