//! `surface EventSubscription` - `allEvents`, `eventsByType`. `ReadAccess`-
//! gated like `ProjectionQuery` (`require_read_mapping`, not
//! `require_admin_mapping` - see the surface's own `GrantScopedToBoundedContext`
//! guarantee). The last surface out of §8/§9's backlog - see
//! `docs/architecture.md`'s own write-up of this pass, and
//! `skilj_core::event_store::EventBroadcaster`'s own doc comment, for why
//! a single-process, in-memory broadcast is the architecturally correct
//! delivery mechanism here, not a simplification (`specs/skilj.allium`'s
//! own Excludes list rules out multi-instance deployment entirely).
//!
//! Both fields share one shape: resolve the caller/access mapping/target
//! `EventType`(s) up front, so a genuinely unauthorised subscribe attempt
//! fails immediately rather than opening a stream that never delivers;
//! build the initial `Subscription` value via
//! `event_store::create_all_events_subscription`/`create_event_type_subscription`
//! (the pure rule, unchanged); then hand back an `asynk_strim::try_stream_fn`
//! stream that pulls from `state.event_broadcaster.subscribe()` for as
//! long as the connection lives, re-running `deliver_to_subscriptions`
//! (also unchanged) per delivered event against a *freshly refetched*
//! `access_mapping` - never the snapshot captured at subscribe time (see
//! below). `filters` is real wire-shape (matching
//! `CreateEventTypeSubscription`'s own signature faithfully - not
//! silently dropped), but a non-empty list is rejected eagerly, the
//! identical precedent REST's own `GET /v1/events` route already set for
//! the same underlying reason: `matches_filters`/`valid_filters` are
//! still `todo!()` for their non-empty case, an existing,
//! separately-tracked gap this pass doesn't newly touch.
//!
//! **Ending the stream, never silently continuing** - both cases below
//! yield exactly one final `Err` then return, the "stop polling the
//! instant an `Err` is yielded" mechanism `async_graphql::dynamic`'s own
//! `Subscription::collect_streams` already provides (confirmed against
//! the installed crate source, not improvised):
//! - `RecvError::Lagged` - `DeliveryIsAtMostOnce`'s own guarantee ("no
//!   duplicates, no gaps... a transport-level disconnect is... the only
//!   signal" one was missed): falling behind ends the stream rather than
//!   silently skipping ahead.
//! - a live-refetched `access_mapping` no longer active -
//!   `RevocationClosesTheConnection`: checked fresh, per delivered event,
//!   against the database, never against the mapping snapshot captured
//!   at subscribe time, so a mid-stream revocation stops delivery at the
//!   next matching event, not at the next reconnect.

use super::{not_found, parse_filters, require_read_mapping, resolve_read_data_keys};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{
    FieldValue, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::ErrorExtensions;
use skilj_core::event_store::{self, Subscription};
use tokio::sync::broadcast::error::RecvError;

fn subscription_lagged_error(skipped: u64) -> async_graphql::Error {
    async_graphql::Error::new(format!(
        "this subscription fell {skipped} event(s) behind and cannot resume without a gap - \
         DeliveryIsAtMostOnce forbids silently skipping ahead, so the connection is closed; \
         reconnect and, if needed, catch up via queryEvents/countEvents first"
    ))
    .extend_with(|_, ext| ext.set("code", "subscription_lagged"))
}

/// Matches `RestError::FiltersNotSupported`'s own code/message exactly
/// (`skilj-rest/src/error.rs`) - the same "non-empty filter sets aren't
/// supported yet" rejection, at the same point in the flow (eagerly,
/// before ever reaching `create_event_type_subscription`).
fn filters_not_supported_error() -> async_graphql::Error {
    async_graphql::Error::new("non-empty filter sets aren't supported yet")
        .extend_with(|_, ext| ext.set("code", "filters_not_supported"))
}

/// `allEvents(boundedContext: String!, eventTypes: [String!], fromSequence: Int): QueriedEvent!`
pub fn all_events_field() -> SubscriptionField {
    SubscriptionField::new("allEvents", TypeRef::named_nn("QueriedEvent"), |ctx| {
        SubscriptionFieldFuture::new(async move {
            let state = ctx.data::<GraphqlState>()?.clone();
            let bounded_context_name = ctx.args.try_get("boundedContext")?.string()?.to_string();
            let access_mapping =
                require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;

            let mut event_types = Vec::new();
            if let Some(list) = ctx.args.get("eventTypes").filter(|v| !v.is_null()) {
                for item in list.list()?.iter() {
                    let name = item.string()?.to_string();
                    let et =
                        skilj_core::db::get_event_type(&state.pool, &bounded_context_name, &name)
                            .await
                            .map_err(to_graphql_error)?
                            .ok_or_else(|| not_found("EventType", &name))?;
                    event_types.push(et);
                }
            }
            let from_sequence = ctx
                .args
                .get("fromSequence")
                .filter(|v| !v.is_null())
                .map(|v| v.i64())
                .transpose()?;

            let bounded_context_events =
                skilj_core::db::list_events_for_bounded_context(&state.pool, &bounded_context_name)
                    .await
                    .map_err(to_graphql_error)?;

            let initial = event_store::create_all_events_subscription(
                &access_mapping,
                event_types,
                from_sequence,
                &bounded_context_events,
                chrono::Utc::now(),
            )
            .map_err(to_graphql_error)?;

            let role_id = access_mapping.role.id.clone();
            let bounded_context_name = bounded_context_name.clone();

            Ok(asynk_strim::try_stream_fn(move |mut yielder| async move {
                let mut current = Subscription::AllEventsSubscription(Box::new(initial));
                let mut rx = state.event_broadcaster.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            if event.bounded_context.name != bounded_context_name {
                                continue;
                            }
                            let fresh_mapping =
                                match skilj_core::db::get_active_role_access_mapping(
                                    &state.pool,
                                    &role_id,
                                    &bounded_context_name,
                                )
                                .await
                                {
                                    Ok(mapping) => mapping,
                                    Err(err) => {
                                        yielder.yield_error(to_graphql_error(err)).await;
                                        return Ok(());
                                    }
                                };
                            let Some(fresh_mapping) = fresh_mapping else {
                                yielder
                                    .yield_error(to_graphql_error(
                                        skilj_core::access_control::Error::GrantNotActive,
                                    ))
                                    .await;
                                return Ok(());
                            };

                            // Decrypt-on-read's own pre-resolution step,
                            // against the freshly-refetched mapping - the
                            // same live, never-snapshot-at-subscribe-time
                            // grant `RevocationClosesTheConnection` above
                            // already relies on.
                            let mut data_keys = std::collections::HashMap::new();
                            if let Err(err) = resolve_read_data_keys(
                                &state.pool,
                                &bounded_context_name,
                                &event.event_type.sensitive_fields,
                                &event.payload,
                                &fresh_mapping,
                                state.encryption_master_key.as_ref(),
                                &mut data_keys,
                            )
                            .await
                            {
                                yielder.yield_error(err).await;
                                return Ok(());
                            }

                            if let Subscription::AllEventsSubscription(s) = &mut current {
                                s.access_mapping = fresh_mapping;
                            }
                            for delivered in event_store::deliver_to_subscriptions(
                                &event,
                                std::slice::from_ref(&current),
                                |sk, sv| data_keys.get(&(sk.to_string(), sv.to_string())).cloned(),
                            ) {
                                yielder
                                    .yield_ok(FieldValue::owned_any((
                                        delivered.event.sequence,
                                        delivered.rendered_payload,
                                    )))
                                    .await;
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            yielder.yield_error(subscription_lagged_error(n)).await;
                            return Ok(());
                        }
                        Err(RecvError::Closed) => return Ok(()),
                    }
                }
            }))
        })
    })
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "eventTypes",
        TypeRef::named_nn_list(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "fromSequence",
        TypeRef::named(TypeRef::INT),
    ))
}

/// `eventsByType(boundedContext: String!, eventType: String!, filters: [FilterInput!], fromSequence: Int): QueriedEvent!`
pub fn events_by_type_field() -> SubscriptionField {
    SubscriptionField::new("eventsByType", TypeRef::named_nn("QueriedEvent"), |ctx| {
        SubscriptionFieldFuture::new(async move {
            let state = ctx.data::<GraphqlState>()?.clone();
            let bounded_context_name = ctx.args.try_get("boundedContext")?.string()?.to_string();
            let access_mapping =
                require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
            let event_type_name = ctx.args.try_get("eventType")?.string()?.to_string();
            let event_type = skilj_core::db::get_event_type(
                &state.pool,
                &bounded_context_name,
                &event_type_name,
            )
            .await
            .map_err(to_graphql_error)?
            .ok_or_else(|| not_found("EventType", &event_type_name))?;

            let filters = match ctx.args.get("filters").filter(|v| !v.is_null()) {
                Some(value) => parse_filters(&value)?,
                None => Vec::new(),
            };
            if !filters.is_empty() {
                return Err(filters_not_supported_error());
            }

            let from_sequence = ctx
                .args
                .get("fromSequence")
                .filter(|v| !v.is_null())
                .map(|v| v.i64())
                .transpose()?;

            let bounded_context_events =
                skilj_core::db::list_events_for_bounded_context(&state.pool, &bounded_context_name)
                    .await
                    .map_err(to_graphql_error)?;

            let initial = event_store::create_event_type_subscription(
                &access_mapping,
                &event_type,
                filters,
                from_sequence,
                &bounded_context_events,
                chrono::Utc::now(),
            )
            .map_err(to_graphql_error)?;

            let role_id = access_mapping.role.id.clone();
            let bounded_context_name = bounded_context_name.clone();

            Ok(asynk_strim::try_stream_fn(move |mut yielder| async move {
                let mut current = Subscription::EventTypeSubscription(Box::new(initial));
                let mut rx = state.event_broadcaster.subscribe();
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            if event.bounded_context.name != bounded_context_name {
                                continue;
                            }
                            let fresh_mapping =
                                match skilj_core::db::get_active_role_access_mapping(
                                    &state.pool,
                                    &role_id,
                                    &bounded_context_name,
                                )
                                .await
                                {
                                    Ok(mapping) => mapping,
                                    Err(err) => {
                                        yielder.yield_error(to_graphql_error(err)).await;
                                        return Ok(());
                                    }
                                };
                            let Some(fresh_mapping) = fresh_mapping else {
                                yielder
                                    .yield_error(to_graphql_error(
                                        skilj_core::access_control::Error::GrantNotActive,
                                    ))
                                    .await;
                                return Ok(());
                            };

                            // Decrypt-on-read's own pre-resolution step,
                            // against the freshly-refetched mapping - see
                            // `all_events_field`'s own identical comment.
                            let mut data_keys = std::collections::HashMap::new();
                            if let Err(err) = resolve_read_data_keys(
                                &state.pool,
                                &bounded_context_name,
                                &event.event_type.sensitive_fields,
                                &event.payload,
                                &fresh_mapping,
                                state.encryption_master_key.as_ref(),
                                &mut data_keys,
                            )
                            .await
                            {
                                yielder.yield_error(err).await;
                                return Ok(());
                            }

                            if let Subscription::EventTypeSubscription(s) = &mut current {
                                s.access_mapping = fresh_mapping;
                            }
                            for delivered in event_store::deliver_to_subscriptions(
                                &event,
                                std::slice::from_ref(&current),
                                |sk, sv| data_keys.get(&(sk.to_string(), sv.to_string())).cloned(),
                            ) {
                                yielder
                                    .yield_ok(FieldValue::owned_any((
                                        delivered.event.sequence,
                                        delivered.rendered_payload,
                                    )))
                                    .await;
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            yielder.yield_error(subscription_lagged_error(n)).await;
                            return Ok(());
                        }
                        Err(RecvError::Closed) => return Ok(()),
                    }
                }
            }))
        })
    })
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "eventType",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new(
        "filters",
        TypeRef::named_nn_list("FilterInput"),
    ))
    .argument(InputValue::new(
        "fromSequence",
        TypeRef::named(TypeRef::INT),
    ))
}
