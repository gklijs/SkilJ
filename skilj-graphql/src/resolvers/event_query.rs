//! `surface EventQuery` - `queryEvents`, `countEvents`, `inspectEvent`.
//! `AdminAccess`-gated, via the shared `require_admin_mapping` helper.

use super::{
    distinct_names, not_found, require_admin_mapping, require_caller, resolve_read_data_keys,
};
use crate::error::to_graphql_error;
use crate::gql_types::InspectedEventData;
use crate::naming::Naming;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use skilj_core::event_store::QueryTag;
use skilj_core::shared::Tag;

/// Parses a `[TagInput!]` argument (nullable - absent means "no tag
/// restriction", matching `query_events`'/`count_events`'s own
/// `Option<&[Tag]>`) into `Vec<Tag>`, a `plaintext` value of a blind key
/// hashed as stored (docs/architecture.md §207). The bounded context's
/// blind keys are read only when some tag asks for it.
async fn parse_tags(
    value: Option<async_graphql::dynamic::ValueAccessor<'_>>,
    state: &GraphqlState,
    bounded_context: &str,
    access_mapping: &skilj_core::access_control::RoleAccessMapping,
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
        let plaintext = match obj.get("plaintext").filter(|v| !v.is_null()) {
            Some(v) => v.boolean()?,
            None => false,
        };
        tags.push(QueryTag {
            tag: Tag { key, value },
            plaintext,
        });
    }
    let blind = state.dispatcher.blind_index();
    let blind_keys = if tags.iter().any(|t| t.plaintext) {
        let stored = skilj_core::db::list_blind_tags(&state.pool, bounded_context)
            .await
            .map_err(to_graphql_error)?;
        // Hashed at the generation stored now, should a key rotation
        // (docs/architecture.md §209) have landed since this process last
        // looked.
        for declaration in &stored {
            blind.observe_generation(
                bounded_context,
                &declaration.tag_key,
                declaration.key_generation,
            );
        }
        stored.into_iter().map(|b| b.tag_key).collect()
    } else {
        Vec::new()
    };
    skilj_core::event_store::resolve_query_tags(
        tags,
        &blind_keys,
        &blind,
        bounded_context,
        access_mapping,
    )
    .map(Some)
    .map_err(to_graphql_error)
}

/// `queryEvents(boundedContext: String!, eventTypes: [String!]!, tags: [TagInput!], afterSequence: Int, correlationId: String): [QueriedEvent!]!`
pub fn query_events_field(n: &Naming) -> Field {
    Field::new(
        n.root("queryEvents"),
        TypeRef::named_nn_list_nn(n.ty("QueriedEvent")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let mut event_types = Vec::new();
                for name in distinct_names(&ctx.args.try_get("eventTypes")?)? {
                    let et =
                        skilj_core::db::get_event_type(&state.pool, &bounded_context_name, &name)
                            .await
                            .map_err(to_graphql_error)?
                            .ok_or_else(|| not_found("EventType", &name))?;
                    event_types.push(et);
                }
                let tags = parse_tags(
                    ctx.args.get("tags"),
                    state,
                    &bounded_context_name,
                    &access_mapping,
                )
                .await?;
                skilj_core::event_store::valid_query_tags(tags.as_deref())
                    .map_err(to_graphql_error)?;
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

                // At most `max_events_per_read` events are served - the
                // caller pages on with `afterSequence` = the last one's
                // `sequence`. docs/architecture.md §19's "Problem 1" fix:
                // a non-empty `tags` filter goes straight to the
                // tag-indexed query (already narrowed, so loaded whole,
                // then paged); otherwise the bounded context's history is
                // walked a chunk at a time until the page is full.
                let select = |events: &[skilj_core::event_store::Event], max: usize| {
                    skilj_core::event_store::query_events_select(
                        &access_mapping,
                        &event_types,
                        tags.as_deref(),
                        after_sequence,
                        correlation_id.as_deref(),
                        events,
                        max,
                    )
                };
                let page = match tags.as_deref() {
                    // docs/architecture.md §108: through the tag index a
                    // chunk at a time, like the untagged path below -
                    // never every match of a widely shared tag at once.
                    Some(wanted) if !wanted.is_empty() => {
                        skilj_core::db::collect_tagged_event_page(
                            &state.pool,
                            &bounded_context_name,
                            wanted,
                            after_sequence.unwrap_or(-1),
                            state.max_events_per_read,
                            select,
                        )
                        .await
                    }
                    _ => {
                        skilj_core::db::collect_event_page(
                            &state.pool,
                            &state.event_cache,
                            &bounded_context_name,
                            None,
                            after_sequence.unwrap_or(-1),
                            state.max_events_per_read,
                            select,
                        )
                        .await
                    }
                }
                .map_err(to_graphql_error)?;

                // Decrypt-on-read's own pre-resolution step - only for
                // the events actually served.
                let mut data_keys = std::collections::HashMap::new();
                for e in &page {
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

                let private_field_grants = super::load_private_field_grants(
                    &state.pool,
                    &bounded_context_name,
                    &access_mapping.role,
                )
                .await?;
                let results = skilj_core::event_store::query_events_page(
                    &access_mapping,
                    &event_types,
                    tags.as_deref(),
                    after_sequence,
                    correlation_id.as_deref(),
                    &page,
                    |sk, sv| {
                        data_keys
                            .get(&(sk.to_string(), sv.to_string()))
                            .cloned()
                            .flatten()
                    },
                    &private_field_grants,
                    state.max_events_per_read,
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
    .argument(InputValue::new(
        "tags",
        TypeRef::named_nn_list(n.ty("TagInput")),
    ))
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
pub fn count_events_field(n: &Naming) -> Field {
    Field::new(
        n.root("countEvents"),
        TypeRef::named_nn(TypeRef::INT),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let mut event_types = Vec::new();
                for name in distinct_names(&ctx.args.try_get("eventTypes")?)? {
                    let et =
                        skilj_core::db::get_event_type(&state.pool, &bounded_context_name, &name)
                            .await
                            .map_err(to_graphql_error)?
                            .ok_or_else(|| not_found("EventType", &name))?;
                    event_types.push(et);
                }
                let tags = parse_tags(
                    ctx.args.get("tags"),
                    state,
                    &bounded_context_name,
                    &access_mapping,
                )
                .await?;
                skilj_core::event_store::valid_query_tags(tags.as_deref())
                    .map_err(to_graphql_error)?;
                let correlation_id = ctx
                    .args
                    .get("correlationId")
                    .filter(|v| !v.is_null())
                    .and_then(|v| v.string().ok())
                    .map(|s| s.to_string());

                // See `query_events_field`'s own identical comment - same
                // §19 "Problem 1" fix, same tags-supplied-or-not branch.
                // An aggregate over everything matching (rule `CountEvents`
                // is deliberately not paged), but never loaded whole: a
                // non-empty `tags` filter uses the tag index (already
                // narrowed); otherwise history is walked a chunk at a time and
                // the rule's own pure count summed per chunk - it runs at
                // least once, so its validation errors still surface.
                let count_in = |events: &[skilj_core::event_store::Event]| {
                    skilj_core::event_store::count_events(
                        &access_mapping,
                        &event_types,
                        tags.as_deref(),
                        correlation_id.as_deref(),
                        events,
                    )
                };
                let count = match tags.as_deref() {
                    Some(wanted) if !wanted.is_empty() => {
                        let mut total = 0;
                        skilj_core::db::for_each_tagged_event_chunk(
                            &state.pool,
                            &bounded_context_name,
                            wanted,
                            -1,
                            state.max_events_per_read,
                            |chunk| {
                                total += count_in(chunk)?;
                                Ok(true)
                            },
                        )
                        .await
                        .map_err(to_graphql_error)?;
                        total
                    }
                    _ => {
                        let mut total = 0;
                        skilj_core::db::for_each_event_chunk(
                            &state.pool,
                            &state.event_cache,
                            &bounded_context_name,
                            None,
                            -1,
                            state.max_events_per_read,
                            |chunk| {
                                total += count_in(chunk)?;
                                Ok(true)
                            },
                        )
                        .await
                        .map_err(to_graphql_error)?;
                        total
                    }
                };

                Ok(Some(async_graphql::Value::from(count)))
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
    .argument(InputValue::new(
        "tags",
        TypeRef::named_nn_list(n.ty("TagInput")),
    ))
    .argument(InputValue::new(
        "correlationId",
        TypeRef::named(TypeRef::STRING),
    ))
}

/// `inspectEvent(boundedContext: String!, sequence: Int!): InspectedEvent!`
pub fn inspect_event_field(n: &Naming) -> Field {
    Field::new(
        n.root("inspectEvent"),
        TypeRef::named_nn(n.ty("InspectedEvent")),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
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
                // And for the originating command's payload, rendered too.
                if let skilj_core::event_store::EventOrigin::CommandTriggered { command } =
                    &event.origin
                {
                    resolve_read_data_keys(
                        &state.pool,
                        &bounded_context_name,
                        &command.command_type.sensitive_fields,
                        &command.payload,
                        &access_mapping,
                        state.encryption_master_key.as_ref(),
                        &mut data_keys,
                    )
                    .await?;
                }

                let private_field_grants = super::load_private_field_grants(
                    &state.pool,
                    &bounded_context_name,
                    &access_mapping.role,
                )
                .await?;
                let inspected = skilj_core::event_store::inspect_event(
                    &access_mapping,
                    &event,
                    |sk, sv| {
                        data_keys
                            .get(&(sk.to_string(), sv.to_string()))
                            .cloned()
                            .flatten()
                    },
                    &private_field_grants,
                )
                .map_err(to_graphql_error)?;

                Ok(Some(FieldValue::owned_any(InspectedEventData {
                    event: inspected.event,
                    rendered_payload: inspected.rendered_payload,
                })))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("sequence", TypeRef::named_nn(TypeRef::INT)))
}

/// `epoch: String!` - the database's current epoch (docs/architecture.md
/// §176), for a client to keep next to the sequences it reads and pass
/// back as `allEvents`/`eventsByType`'s `epoch` when it resubscribes from
/// one. After a failover or restore it differs, and such a resubscription
/// is refused with `epoch_changed` instead of silently skipping events
/// whose sequences the new history reused. Any authenticated caller.
pub fn epoch_field(n: &Naming) -> Field {
    Field::new(n.root("epoch"), TypeRef::named_nn(TypeRef::STRING), |ctx| {
        FieldFuture::new(async move {
            require_caller(&ctx)?;
            let state = ctx.data::<GraphqlState>()?;
            let epoch = skilj_core::db::current_epoch(&state.pool)
                .await
                .map_err(to_graphql_error)?;
            Ok(Some(FieldValue::value(epoch.to_string())))
        })
    })
}
