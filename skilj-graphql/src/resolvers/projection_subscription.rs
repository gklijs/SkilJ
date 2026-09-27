//! `surface ProjectionQuery`'s own subscription counterpart -
//! `projectionUpdates` (Codeberg issue #23). `ReadAccess`-gated exactly
//! like `projection`/`projectionSchema` (`require_read_mapping`, not
//! `require_admin_mapping`), and `team_only`-gated the same way too
//! (Codeberg issue #17's fix - see `projection_query::fetch_projection_result`'s
//! own doc comment, which this field's every push runs through).
//!
//! Shaped after `event_subscription.rs`'s own two fields: resolve the
//! caller/access mapping/target `Projection` up front;
//! `state.event_broadcaster.subscribe()` next, *before* the initial state
//! read (identical drift-audit-finding-#7 ordering, same reasoning - see
//! that module's own comment at its `subscribe()` call site); then hand
//! back an `asynk_strim::try_stream_fn` stream that pulls from the
//! already-subscribed receiver for as long as the connection lives.
//!
//! Where this genuinely differs from `EventSubscription`, deliberately:
//!
//! - **What triggers a push.** Not every event - only one whose
//!   `ProjectionDispatcher::keys()` names this subscription's own `key`.
//!   Everything else is a cheap in-memory `continue`, no DB round trip,
//!   the same "position always advances, state only changes when
//!   consumed" distinction `ProjectionDispatcher::keys`'s own doc comment
//!   already draws.
//! - **What gets pushed.** Never the triggering event itself - always a
//!   fresh `projection_query::fetch_projection_result` refetch (the exact
//!   read path `projection` the query field already uses), passing the
//!   triggering event's own `sequence` as `wait_for_sequence` so an
//!   *async* projection's `caught_up_to` poll (inside that shared
//!   function) is waited out before reading `projection_state` - not
//!   assumed current the way a sync projection's is. No separate
//!   sync/async branch exists in this file at all; `wait_until_caught_up`
//!   already resolves instantly for a sync projection and waits out a
//!   real poll tick for an async one.
//! - **`RecvError::Lagged` handling.** `EventSubscription` closes the
//!   connection - it promises every individual event, so falling behind
//!   is real data loss (`DeliveryIsAtMostOnce`). `projectionUpdates`
//!   promises no such thing - its contract is "current state whenever it
//!   changes", not "every event that changed it" - so a lagged receiver
//!   loses nothing a plain refetch can't recover: self-heal by refetching
//!   this key's current state (no specific `wait_for_sequence` - just
//!   whatever is current *now*) and keep the connection open, rather than
//!   erroring out.

use super::projection_query::fetch_projection_result;
use super::{not_found, require_read_mapping};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{
    FieldValue, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use tokio::sync::broadcast::error::RecvError;

/// `projectionUpdates(boundedContext: String!, name: String!, key: String): ProjectionResult!`
pub fn projection_updates_field() -> SubscriptionField {
    SubscriptionField::new(
        "projectionUpdates",
        TypeRef::named_nn("ProjectionResult"),
        |ctx| {
            SubscriptionFieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?.clone();
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_read_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let name = ctx.args.try_get("name")?.string()?.to_string();
                // Checked once: a running subscription stays on the schema
                // (and admitted set) it started under.
                super::projection_query::require_admitted(
                    &ctx,
                    &state,
                    &bounded_context_name,
                    &name,
                )
                .await?;
                let key = ctx
                    .args
                    .get("key")
                    .filter(|v| !v.is_null())
                    .map(|v| v.string().map(str::to_string))
                    .transpose()?
                    .unwrap_or_default();

                // Confirms the projection actually exists (a typo'd name
                // fails the subscription attempt itself, not silently on
                // the first push) - not otherwise used here, `keys()`
                // below takes bounded_context/name as plain strings, not
                // this struct.
                skilj_core::db::get_projection(&state.pool, &bounded_context_name, &name)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("Projection", &name))?;

                // Subscribed *before* the initial state read just below -
                // see this module's own doc comment (mirrors
                // `event_subscription.rs`'s drift-audit-finding-#7
                // ordering): an event landing in the gap must be either
                // already reflected in the snapshot read or delivered
                // live, never neither.
                let mut rx = state.event_broadcaster.subscribe();

                let (initial_value, type_name) = fetch_projection_result(
                    &state,
                    &access_mapping,
                    &bounded_context_name,
                    &name,
                    &key,
                    None,
                )
                .await?;

                let role_id = access_mapping.role.id.clone();

                Ok(asynk_strim::try_stream_fn(move |mut yielder| async move {
                    yielder
                        .yield_ok(FieldValue::owned_any(initial_value).with_type(type_name.clone()))
                        .await;

                    let mut revocation_rx = state.revocation_broadcaster.subscribe();
                    loop {
                        let event = tokio::select! {
                            revoked = revocation_rx.recv() => {
                                match super::event_subscription::revocation_closes_connection(
                                    &state.pool,
                                    &role_id,
                                    &bounded_context_name,
                                    revoked,
                                )
                                .await
                                {
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

                        // `wait_for_sequence`: `None` on the self-heal
                        // (lag) path - there's no specific triggering
                        // event to wait for, just whatever is current
                        // right now; `Some(event.sequence)` on a real
                        // matching event, so an async projection's
                        // catch-up is actually waited out rather than
                        // raced (see this module's own doc comment).
                        let wait_for_sequence = match &event {
                            Ok(event) => {
                                if event.bounded_context.name != bounded_context_name {
                                    continue;
                                }
                                match state.projection_dispatcher.keys(
                                    &bounded_context_name,
                                    &name,
                                    event,
                                ) {
                                    None => {
                                        yielder
                                            .yield_error(async_graphql::Error::new(
                                                "projection is no longer registered",
                                            ))
                                            .await;
                                        return Ok(());
                                    }
                                    Some(keys) if !keys.iter().any(|k| k == &key) => continue,
                                    Some(_) => Some(event.sequence),
                                }
                            }
                            Err(RecvError::Lagged(_)) => None,
                            Err(RecvError::Closed) => return Ok(()),
                        };

                        let fresh_mapping = match skilj_core::db::get_active_role_access_mapping(
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

                        match fetch_projection_result(
                            &state,
                            &fresh_mapping,
                            &bounded_context_name,
                            &name,
                            &key,
                            wait_for_sequence,
                        )
                        .await
                        {
                            Ok((value, type_name)) => {
                                yielder
                                    .yield_ok(FieldValue::owned_any(value).with_type(type_name))
                                    .await;
                            }
                            Err(err) => {
                                yielder.yield_error(err).await;
                                return Ok(());
                            }
                        }
                    }
                }))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("name", TypeRef::named_nn(TypeRef::STRING)))
    .argument(InputValue::new("key", TypeRef::named(TypeRef::STRING)))
}
