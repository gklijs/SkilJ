//! `surface EventSubscription` - `allEvents`, `eventsByType`. `ReadAccess`-
//! gated like `ProjectionQuery` (`require_read_mapping`, not
//! `require_admin_mapping` - see the surface's own `GrantScopedToBoundedContext`
//! guarantee). The last surface out of [§8](../../../docs/architecture.md#open-for-a-future-pass)/[§9](../../../docs/architecture.md#next-steps)'s backlog - see
//! `docs/architecture.md`'s own write-up of this pass, and
//! `skilj_core::event_store::EventBroadcaster`'s own doc comment, for why
//! a single-process, in-memory broadcast is the architecturally correct
//! delivery mechanism here, not a simplification (`specs/skilj.allium`'s
//! own Excludes list rules out multi-instance deployment entirely).
//!
//! Both fields share one shape: resolve the caller/access mapping/target
//! `EventType`(s) up front; `state.event_broadcaster.subscribe()` next -
//! see the comment at that exact call site (drift audit finding #4,
//! 2026-08-20) for why it comes *before* the `bounded_context_events`
//! snapshot read just after it, not the reverse; build the initial
//! `Subscription` value from that snapshot via
//! `event_store::create_all_events_subscription`/`create_event_type_subscription`
//! (the pure rule, unchanged); then hand back an `asynk_strim::try_stream_fn`
//! stream that pulls from the already-subscribed receiver for as long as
//! the connection lives, re-running `deliver_to_subscriptions` (also
//! unchanged) per delivered event against a *freshly refetched*
//! `access_mapping` - never the snapshot captured at subscribe time (see
//! below). `filters` is real wire-shape (matching
//! `CreateEventTypeSubscription`'s own signature faithfully) and real
//! behaviour now that `matches_filters`/`valid_filters` are real -
//! `create_event_type_subscription`'s own `valid_filters` call rejects an
//! invalid filter the normal way, via `to_graphql_error`.
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
//! - `RevocationClosesTheConnection`, via two independent paths (drift
//!   audit finding #4, 2026-08-20 - the first of these used to be the
//!   only one, which meant a revoked subscription in an otherwise-quiet
//!   bounded context stayed open indefinitely; see
//!   `revocation_closes_connection`'s own doc comment): a live-refetched
//!   `access_mapping` no longer active on the next delivered event -
//!   checked fresh against the database, never against the mapping
//!   snapshot captured at subscribe time - and, independently of any
//!   event ever arriving at all, a matching notification from
//!   `state.revocation_broadcaster`, published the instant
//!   `resolvers::access_management` actually revokes the mapping.

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

/// `RevocationClosesTheConnection`'s push half (drift audit finding #4,
/// see project memory `skilj-drift-audit-2026-08-20`) - both subscription
/// loops below `tokio::select!` against `state.revocation_broadcaster`
/// alongside their own `EventBroadcaster` receiver, and route whatever
/// they get from it through here. Doesn't replace the pre-existing
/// per-delivered-event re-check just below each call site - that one
/// still does real, separate work (a fresh `access_mapping` for
/// decrypt-on-read, e.g. a `can_read_sensitive` flip that isn't a
/// revocation at all) - this only adds the orthogonal "nothing has to
/// arrive for a revocation to close the connection" path that check
/// alone couldn't cover.
///
/// `Ok(true)`: close now, this notification named our own
/// `(role_id, bounded_context)`. `Ok(false)`: not ours, or nothing to act
/// on yet - keep looping. `Err`: a real I/O failure while re-checking a
/// lagged receiver, ready to `yield_error` as-is.
pub(crate) async fn revocation_closes_connection(
    pool: &skilj_core::db::Pool,
    role_id: &str,
    bounded_context_name: &str,
    revoked: Result<skilj_core::access_control::RevokedMapping, RecvError>,
) -> Result<bool, async_graphql::Error> {
    match revoked {
        Ok(revoked) => {
            Ok(revoked.role_id == role_id && revoked.bounded_context == bounded_context_name)
        }
        Err(RecvError::Lagged(_)) => {
            // May have missed a notification meant for us - a lagged
            // broadcast receiver gives no way to tell which ones, so the
            // only safe response is a direct, authoritative re-check
            // against the database rather than assuming either way.
            let still_active =
                skilj_core::db::get_active_role_access_mapping(pool, role_id, bounded_context_name)
                    .await
                    .map_err(to_graphql_error)?
                    .is_some();
            Ok(!still_active)
        }
        // No more revocation notifications will ever arrive on this
        // receiver - not fatal on its own, the per-delivered-event
        // re-check below still catches a revocation whenever the next
        // matching event happens to arrive, the same guarantee this
        // connection had before this push mechanism existed at all.
        Err(RecvError::Closed) => Ok(false),
    }
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

            // Subscribed *before* the snapshot read just below, not after
            // (drift audit finding #7, 2026-08-20 - see project memory
            // `skilj-drift-audit-2026-08-20`) - this receiver's own doc
            // comment on `EventBroadcaster::subscribe` says it plainly:
            // "each see every event published after they were created".
            // An event committed in the gap between this call and the
            // snapshot read is now guaranteed to be either already in
            // that snapshot (and so already reflected in
            // `bounded_context_events`) or delivered live through `rx`
            // instead - not both: `deliver_to_subscriptions`'s own
            // `s.starting_sequence() < event.sequence` filter already
            // excludes anything at or below `from_sequence`, and
            // `from_sequence` here is computed as the snapshot's own
            // highest sequence, so an event the snapshot already
            // includes can never also be delivered live as a duplicate.
            // Subscribing *after* the snapshot, the old order, left a
            // real gap the other way: an event landing in that same
            // window postdated the snapshot (so `from_sequence` didn't
            // cover it) but predated this receiver's own creation (so
            // the broadcaster's fan-out, which only reaches receivers
            // that already existed at publish time, never delivered it
            // either) - silently missed, on both sides at once.
            let mut rx = state.event_broadcaster.subscribe();

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
                let mut revocation_rx = state.revocation_broadcaster.subscribe();
                loop {
                  let event = tokio::select! {
                    revoked = revocation_rx.recv() => {
                        match revocation_closes_connection(&state.pool, &role_id, &bounded_context_name, revoked).await {
                            Ok(true) => {
                                yielder
                                    .yield_error(to_graphql_error(
                                        skilj_core::access_control::Error::GrantNotActive,
                                    ))
                                    .await;
                                return Ok(());
                            }
                            Ok(false) => continue,
                            Err(err) => {
                                yielder.yield_error(err).await;
                                return Ok(());
                            }
                        }
                    }
                    event = rx.recv() => event,
                  };
                    match event {
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
                            // Same "live, never snapshot-at-subscribe-time"
                            // treatment as `fresh_mapping` above - a grant
                            // made or revoked after this connection opened
                            // still takes effect on the very next delivery.
                            let private_field_grants = match skilj_core::db::list_private_field_grants_for_context(
                                &state.pool,
                                &bounded_context_name,
                            )
                            .await
                            {
                                Ok(grants) => grants,
                                Err(err) => {
                                    yielder.yield_error(to_graphql_error(err)).await;
                                    return Ok(());
                                }
                            };
                            for delivered in event_store::deliver_to_subscriptions(
                                &event,
                                std::slice::from_ref(&current),
                                |sk, sv| data_keys.get(&(sk.to_string(), sv.to_string())).cloned(),
                                &private_field_grants,
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

            let from_sequence = ctx
                .args
                .get("fromSequence")
                .filter(|v| !v.is_null())
                .map(|v| v.i64())
                .transpose()?;

            // Subscribed before the snapshot read just below - see
            // `all_events_field`'s own identical comment for why (drift
            // audit finding #7).
            let mut rx = state.event_broadcaster.subscribe();

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
                let mut revocation_rx = state.revocation_broadcaster.subscribe();
                loop {
                  let event = tokio::select! {
                    revoked = revocation_rx.recv() => {
                        match revocation_closes_connection(&state.pool, &role_id, &bounded_context_name, revoked).await {
                            Ok(true) => {
                                yielder
                                    .yield_error(to_graphql_error(
                                        skilj_core::access_control::Error::GrantNotActive,
                                    ))
                                    .await;
                                return Ok(());
                            }
                            Ok(false) => continue,
                            Err(err) => {
                                yielder.yield_error(err).await;
                                return Ok(());
                            }
                        }
                    }
                    event = rx.recv() => event,
                  };
                    match event {
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
                            // See `all_events_field`'s own identical comment.
                            let private_field_grants = match skilj_core::db::list_private_field_grants_for_context(
                                &state.pool,
                                &bounded_context_name,
                            )
                            .await
                            {
                                Ok(grants) => grants,
                                Err(err) => {
                                    yielder.yield_error(to_graphql_error(err)).await;
                                    return Ok(());
                                }
                            };
                            for delivered in event_store::deliver_to_subscriptions(
                                &event,
                                std::slice::from_ref(&current),
                                |sk, sv| data_keys.get(&(sk.to_string(), sv.to_string())).cloned(),
                                &private_field_grants,
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
