//! `parkedDeliveries(boundedContext:)`, `retryParkedDelivery`,
//! `discardParkedDelivery` (Codeberg issue #21) - the admin surface over
//! `skilj_core::db::ParkedDelivery`. `AdminAccess`-gated, same
//! `require_admin_mapping` treatment every other admin surface in this
//! module gets. **No spec entity** - see `ParkedDelivery`'s own doc
//! comment; this module exists purely to make an otherwise Rust-only
//! mechanism operator-visible and operator-actionable.
//!
//! `retryParkedDelivery` redrives a delivery's own stored `request_json`
//! straight through the same `skilj_core::db`/`skilj_core::event_store`
//! functions the original REST route (or `catch_up_cross_context_route`)
//! would have called, bypassing the REST/token-secret layer entirely -
//! this mutation is already admin-gated, so re-presenting the original
//! bridge's own secret would add nothing. A `CrossContextRoute` redrive
//! deliberately doesn't reconstruct the original source event's own
//! `correlation_id`/`causation_id` (not stored on the parked row) - the
//! retried command gets a fresh, server-generated `correlation_id`
//! instead, the same "omitted means generated server-side" register
//! Codeberg issue #18 already established for every other caller that
//! doesn't have one to hand.

use super::{not_found, require_admin_mapping};
use crate::error::to_graphql_error;
use crate::GraphqlState;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use async_graphql::ErrorExtensions;
use chrono::Utc;
use serde::Deserialize;
use skilj_core::db::{self, ParkedDelivery, ParkedDeliveryKind};

/// Mirrors `skilj-rest::routes::ExternalEventRequest`'s own wire shape -
/// duplicated, not shared, the same "no crate both sides already depend
/// on that this small a struct would justify adding" reasoning
/// `skilj-graphql::error::current_trace_id`'s own doc comment gives for
/// its identical `skilj-rest` copy. What a bridge originally sent is
/// exactly what a retry resubmits.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExternalEventRedrive {
    payload: serde_json::Value,
    source_content: String,
    source_context: Option<String>,
    dedupe: Option<DedupeRedrive>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DedupeRedrive {
    partition_key: String,
    sequence: i64,
}

/// Mirrors `skilj-rest::routes::CommandTriggerRequest`'s own wire shape -
/// see `ExternalEventRedrive`'s own doc comment.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommandTriggerRedrive {
    payload: serde_json::Value,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    /// Not part of `CommandTriggerRequest` itself: the original request's
    /// `Idempotency-Key` header, merged in by `POST /v1/parked-deliveries`.
    idempotency_key: Option<String>,
}

fn decode_request<T: serde::de::DeserializeOwned>(
    request_json: &serde_json::Value,
) -> skilj_core::error::Result<T> {
    serde_json::from_value(request_json.clone()).map_err(|e| {
        skilj_core::event_store::Error::InvalidParkedDeliveryRequest(e.to_string()).into()
    })
}

/// Redrives one [`ParkedDelivery`] through the same core function its
/// original attempt would have called. `Ok(())` covers every outcome
/// that means "this delivery is no longer stuck" - a business rejection
/// included, the same "a rejection is a legitimate outcome, not an
/// error" register every other submission path in this codebase already
/// uses - so the caller deletes the parked row on any `Ok`. Only a real
/// `Err` (the identical failure class that parked it in the first place)
/// leaves it parked.
async fn redrive_parked_delivery(
    state: &GraphqlState,
    delivery: &ParkedDelivery,
) -> skilj_core::error::Result<()> {
    match delivery.kind {
        // docs/architecture.md §115: a parked deadline redrives exactly
        // like a parked route delivery - its command, under the identity
        // `parked_delivery_redrive_identity` gives it.
        ParkedDeliveryKind::CrossContextRoute | ParkedDeliveryKind::Deadline => {
            let target_bounded_context = delivery.target_bounded_context.as_deref().expect(
                "CrossContextRoute/Deadline-kind ParkedDelivery always carries \
                 target_bounded_context",
            );
            let target_command_type_name = delivery.target_command_type.as_deref().expect(
                "CrossContextRoute/Deadline-kind ParkedDelivery always carries \
                 target_command_type",
            );
            // `CommandType` rows are never hard-deleted individually
            // (only ever added to - see docs/architecture.md's own
            // additive-only-schema-evolution write-up) - but the *whole*
            // target bounded context can be, via `DeleteBoundedContext`,
            // entirely independently of the (still-alive) context this
            // delivery is parked in and retried from. A stranded delivery
            // pointed at an already-gone target is a real, retry-worthy
            // outcome an operator can hit by ordinary use of both
            // surfaces, not a programming error - an ordinary `Err` here
            // (leaving the row parked, same as any other failed redrive),
            // not a panic.
            let target_command_type = db::get_command_type(
                &state.pool,
                target_bounded_context,
                target_command_type_name,
            )
            .await?
            .ok_or_else(skilj_core::error::Error::row_not_found)?;
            let payload = serde_json::to_string(&delivery.request_json)
                .expect("serde_json::Value serialization is infallible");
            let (client_id, idempotency_key) = db::parked_delivery_redrive_identity(
                delivery,
                db::CROSS_CONTEXT_ROUTE_CLIENT_ID,
                None,
            )
            .expect("CrossContextRoute/Deadline-kind redrives always carry an idempotency key");
            // Codeberg issue #32 (round two): routed through
            // `state.command_batcher` rather than calling
            // `db::decide_and_submit_command` directly - a redrive is
            // just as much real, externally-triggerable submission
            // volume as the original delivery was.
            state
                .command_batcher
                .decide_and_submit(
                    &state.pool,
                    &state.dispatcher,
                    &state.projection_dispatcher,
                    state.snapshot_dispatcher.as_ref(),
                    &state.event_broadcaster,
                    &state.event_cache,
                    &target_command_type,
                    &payload,
                    &client_id,
                    None,
                    None,
                    state.encryption_master_key.as_ref(),
                    Utc::now(),
                    Some(&idempotency_key),
                )
                .await?;
        }
        ParkedDeliveryKind::ExternalEvent => {
            let access_token_id = delivery
                .access_token_id
                .as_deref()
                .expect("ExternalEvent-kind ParkedDelivery always carries access_token_id");
            // Access tokens themselves are only ever revoked (a status
            // flip `create_and_insert_external_event`'s own
            // `TokenNotActive` check below catches, once we have the row
            // in hand), never deleted individually - a `None` here can
            // only mean the token's own bounded context was hard-deleted
            // out from under this call, between the row fetch above and
            // this lookup. A much tighter window than the
            // `target_bounded_context` race above (it'd have to be this
            // delivery's *own* bounded context, mid-request), but the
            // same principle applies: an ordinary `Err`, not a panic.
            let token = db::get_external_event_token(&state.pool, access_token_id)
                .await?
                .ok_or_else(skilj_core::error::Error::row_not_found)?;
            // `request_json` is bridge-supplied JSON. `POST
            // /v1/parked-deliveries` now rejects one that doesn't have
            // this shape, but a row stored before that check existed may
            // not - an ordinary `Err` (the row stays parked for an
            // operator to discard), not a panic.
            let redrive: ExternalEventRedrive = decode_request(&delivery.request_json)?;
            let payload = serde_json::to_string(&redrive.payload)
                .expect("serde_json::Value serialization is infallible");
            let fallback_partition_key = db::parked_delivery_redrive_dedupe_partition_key(delivery);
            let dedupe = match &redrive.dedupe {
                Some(d) => db::DedupeCursor {
                    partition_key: &d.partition_key,
                    sequence: d.sequence,
                },
                None => db::DedupeCursor {
                    partition_key: &fallback_partition_key,
                    sequence: 1,
                },
            };
            db::create_and_insert_external_event(
                &state.pool,
                state.projection_dispatcher.as_ref(),
                &state.event_broadcaster,
                &state.event_cache,
                &token,
                payload,
                redrive.source_content,
                redrive.source_context,
                redrive.correlation_id,
                redrive.causation_id,
                Some(dedupe),
                Utc::now(),
                state.encryption_master_key.as_ref(),
            )
            .await?;
        }
        ParkedDeliveryKind::CommandTrigger => {
            let access_token_id = delivery
                .access_token_id
                .as_deref()
                .expect("CommandTrigger-kind ParkedDelivery always carries access_token_id");
            // See the identical `ExternalEvent` branch above for why this
            // is an ordinary `Err`, not a panic.
            let token = db::get_command_token(&state.pool, access_token_id)
                .await?
                .ok_or_else(skilj_core::error::Error::row_not_found)?;
            // See the identical `ExternalEvent` branch above.
            let redrive: CommandTriggerRedrive = decode_request(&delivery.request_json)?;
            let payload = serde_json::to_string(&redrive.payload)
                .expect("serde_json::Value serialization is infallible");
            let authorised = skilj_core::event_store::authorise_command_trigger(
                &token,
                payload,
                redrive.correlation_id,
                redrive.causation_id,
            )?;
            // Checked at report time too; again here for a row that
            // predates that check.
            skilj_core::event_store::reject_reserved_idempotency_key(
                redrive.idempotency_key.as_deref(),
            )?;
            let (client_id, idempotency_key) = db::parked_delivery_redrive_identity(
                delivery,
                &authorised.client_id,
                redrive.idempotency_key.as_deref(),
            )
            .expect("CommandTrigger-kind redrives always carry an idempotency key");
            // Codeberg issue #32 (round two) - see the identical comment
            // on the `CrossContextRoute` branch above.
            state
                .command_batcher
                .decide_and_submit(
                    &state.pool,
                    &state.dispatcher,
                    &state.projection_dispatcher,
                    state.snapshot_dispatcher.as_ref(),
                    &state.event_broadcaster,
                    &state.event_cache,
                    &authorised.command_type,
                    &authorised.payload,
                    &client_id,
                    authorised.correlation_id.as_deref(),
                    authorised.causation_id.as_deref(),
                    state.encryption_master_key.as_ref(),
                    Utc::now(),
                    Some(&idempotency_key),
                )
                .await?;
        }
    }
    Ok(())
}

/// `parkedDeliveries(boundedContext: String!, after: String): [ParkedDelivery!]!`
pub fn parked_deliveries_field() -> Field {
    Field::new(
        "parkedDeliveries",
        TypeRef::named_nn_list_nn("ParkedDelivery"),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                // Bounded like every other list read (docs/architecture.md
                // §86): at most `max_events_per_read` rows, continued with
                // the last row's `cursor` as `after`.
                let after = match ctx.args.get("after").filter(|v| !v.is_null()) {
                    Some(value) => {
                        let raw = value.string()?;
                        Some(db::ParkedDeliveryCursor::decode(raw).ok_or_else(|| {
                            async_graphql::Error::new(format!(
                                "{raw:?} is not a parkedDeliveries cursor"
                            ))
                            .extend_with(|_, ext| ext.set("code", "invalid_cursor"))
                        })?)
                    }
                    None => None,
                };
                let limit = i64::try_from(state.max_events_per_read.max(1)).unwrap_or(i64::MAX);
                // Only the rows this grant may see, each masked for it
                // (docs/architecture.md §119). Rows a scoped grant can't
                // see are skipped, so the page is filled from as many
                // stored pages as that takes - a short page still means
                // the end.
                let mut deliveries = Vec::new();
                let mut after = after;
                // A scoped grant can walk many rows to fill one page; each
                // row's rules come from its target type or token, shared
                // by the rows one bridge or route parks (§153).
                let mut rules_cache = db::ParkedPayloadRulesCache::default();
                'pages: loop {
                    let page = db::list_parked_deliveries_page(
                        &state.pool,
                        &bounded_context_name,
                        after.as_ref(),
                        limit,
                    )
                    .await
                    .map_err(to_graphql_error)?;
                    let exhausted = (page.len() as i64) < limit;
                    for delivery in &page {
                        after = Some(db::ParkedDeliveryCursor::of(delivery));
                        let rules = rules_cache
                            .get(&state.pool, delivery)
                            .await
                            .map_err(to_graphql_error)?;
                        if db::parked_delivery_visible_to(delivery, rules.as_ref(), &access_mapping)
                        {
                            deliveries.push(db::render_parked_delivery(
                                delivery,
                                rules.as_ref(),
                                &access_mapping,
                            ));
                            if deliveries.len() as i64 == limit {
                                break 'pages;
                            }
                        }
                    }
                    if exhausted {
                        break;
                    }
                }

                Ok(Some(FieldValue::list(
                    deliveries.into_iter().map(FieldValue::owned_any),
                )))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("after", TypeRef::named(TypeRef::STRING)))
}

/// `retryParkedDelivery(boundedContext: String!, id: String!): ParkedDelivery!` -
/// see `redrive_parked_delivery`'s own doc comment for what "retry"
/// actually does. Returns the (now-deleted) row on success; on a repeat
/// failure, the row stays parked with its `error`/`attemptCount`/
/// `lastFailedAt` updated, and this mutation returns a GraphQL error
/// carrying that same failure.
pub fn retry_parked_delivery_field() -> Field {
    Field::new(
        "retryParkedDelivery",
        TypeRef::named_nn("ParkedDelivery"),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let id = ctx.args.try_get("id")?.string()?.to_string();

                // Taken before the lock's connection, and held until this
                // resolver returns - see `parked_delivery_retry_permits`.
                let _permit = state
                    .parked_delivery_retry_permits
                    .acquire()
                    .await
                    .expect("parked_delivery_retry_permits is never closed");
                // Held until this resolver returns - see
                // `try_lock_parked_delivery_for_retry`. The row is read
                // only once the lock is ours, so a retry that lost a race
                // with one that already succeeded sees it gone.
                let _retry_lock =
                    db::try_lock_parked_delivery_for_retry(&state.pool, &bounded_context_name, &id)
                        .await
                        .map_err(to_graphql_error)?
                        .ok_or_else(|| {
                            async_graphql::Error::new(format!(
                                "ParkedDelivery {id:?} is already being retried"
                            ))
                            .extend_with(|_, ext| {
                                ext.set("code", "ParkedDelivery_retry_in_progress")
                            })
                        })?;
                let delivery = db::get_parked_delivery(&state.pool, &bounded_context_name, &id)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("ParkedDelivery", &id))?;
                // A row outside a scoped grant's owner is as absent to it
                // as a missing one (docs/architecture.md §119).
                let rules = db::parked_delivery_payload_rules(&state.pool, &delivery)
                    .await
                    .map_err(to_graphql_error)?;
                if !db::parked_delivery_visible_to(&delivery, rules.as_ref(), &access_mapping) {
                    return Err(not_found("ParkedDelivery", &id));
                }

                match redrive_parked_delivery(state, &delivery).await {
                    Ok(()) => {
                        let removed =
                            db::delete_parked_delivery(&state.pool, &bounded_context_name, &id)
                                .await
                                .map_err(to_graphql_error)?
                                .unwrap_or(delivery);
                        Ok(Some(FieldValue::owned_any(db::render_parked_delivery(
                            &removed,
                            rules.as_ref(),
                            &access_mapping,
                        ))))
                    }
                    Err(e) => {
                        let message = skilj_core::error::SkiljRejection::message(&e);
                        db::record_parked_delivery_retry_failure(
                            &state.pool,
                            &bounded_context_name,
                            &id,
                            &message,
                            Utc::now(),
                        )
                        .await
                        .map_err(to_graphql_error)?;
                        Err(to_graphql_error(e))
                    }
                }
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::STRING)))
}

/// `discardParkedDelivery(boundedContext: String!, id: String!): ParkedDelivery!`,
/// returning the just-discarded row - same "hand back what's gone"
/// treatment `discardProjectionRebuild` already uses.
pub fn discard_parked_delivery_field() -> Field {
    Field::new(
        "discardParkedDelivery",
        TypeRef::named_nn("ParkedDelivery"),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                let access_mapping =
                    require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let id = ctx.args.try_get("id")?.string()?.to_string();

                // Checked against the row before it's deleted - see
                // `retryParkedDelivery`.
                let delivery = db::get_parked_delivery(&state.pool, &bounded_context_name, &id)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("ParkedDelivery", &id))?;
                let rules = db::parked_delivery_payload_rules(&state.pool, &delivery)
                    .await
                    .map_err(to_graphql_error)?;
                if !db::parked_delivery_visible_to(&delivery, rules.as_ref(), &access_mapping) {
                    return Err(not_found("ParkedDelivery", &id));
                }
                let discarded = db::delete_parked_delivery(&state.pool, &bounded_context_name, &id)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("ParkedDelivery", &id))?;

                Ok(Some(FieldValue::owned_any(db::render_parked_delivery(
                    &discarded,
                    rules.as_ref(),
                    &access_mapping,
                ))))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::STRING)))
}
