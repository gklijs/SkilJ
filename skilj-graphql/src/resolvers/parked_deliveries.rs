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
        ParkedDeliveryKind::CrossContextRoute => {
            let target_bounded_context = delivery.target_bounded_context.as_deref().expect(
                "CrossContextRoute-kind ParkedDelivery always carries target_bounded_context",
            );
            let target_command_type_name = delivery
                .target_command_type
                .as_deref()
                .expect("CrossContextRoute-kind ParkedDelivery always carries target_command_type");
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
            // Codeberg issue #32 (round two): routed through
            // `state.command_batcher` rather than calling
            // `db::decide_and_submit_command` directly - a redrive is
            // just as much real, externally-triggerable submission
            // volume as the original delivery was.
            state
                .command_batcher
                .decide_and_submit(
                    &state.pool,
                    state.dispatcher.as_ref(),
                    state.projection_dispatcher.as_ref(),
                    state.snapshot_dispatcher.as_ref(),
                    &state.event_broadcaster,
                    &state.event_cache,
                    &target_command_type,
                    &payload,
                    "parked-delivery-retry",
                    None,
                    None,
                    state.encryption_master_key.as_ref(),
                    Utc::now(),
                    None,
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
            // `request_json` is always this exact shape - written
            // verbatim from a real `ExternalEventRequest` body by
            // `skilj-rest`'s own `POST /v1/parked-deliveries` handler,
            // never caller-supplied free-form JSON.
            let redrive: ExternalEventRedrive = serde_json::from_value(
                delivery.request_json.clone(),
            )
            .expect(
                "ExternalEvent-kind ParkedDelivery's own request_json matches ExternalEventRedrive",
            );
            let payload = serde_json::to_string(&redrive.payload)
                .expect("serde_json::Value serialization is infallible");
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
                redrive.dedupe.as_ref().map(|d| db::DedupeCursor {
                    partition_key: &d.partition_key,
                    sequence: d.sequence,
                }),
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
            let redrive: CommandTriggerRedrive = serde_json::from_value(delivery.request_json.clone())
                .expect("CommandTrigger-kind ParkedDelivery's own request_json matches CommandTriggerRedrive");
            let payload = serde_json::to_string(&redrive.payload)
                .expect("serde_json::Value serialization is infallible");
            let authorised = skilj_core::event_store::authorise_command_trigger(
                &token,
                payload,
                redrive.correlation_id,
                redrive.causation_id,
            )?;
            // Codeberg issue #32 (round two) - see the identical comment
            // on the `CrossContextRoute` branch above.
            state
                .command_batcher
                .decide_and_submit(
                    &state.pool,
                    state.dispatcher.as_ref(),
                    state.projection_dispatcher.as_ref(),
                    state.snapshot_dispatcher.as_ref(),
                    &state.event_broadcaster,
                    &state.event_cache,
                    &authorised.command_type,
                    &authorised.payload,
                    &authorised.client_id,
                    authorised.correlation_id.as_deref(),
                    authorised.causation_id.as_deref(),
                    state.encryption_master_key.as_ref(),
                    Utc::now(),
                    None,
                )
                .await?;
        }
    }
    Ok(())
}

/// `parkedDeliveries(boundedContext: String!): [ParkedDelivery!]!`
pub fn parked_deliveries_field() -> Field {
    Field::new(
        "parkedDeliveries",
        TypeRef::named_nn_list_nn("ParkedDelivery"),
        |ctx| {
            FieldFuture::new(async move {
                let state = ctx.data::<GraphqlState>()?;
                let bounded_context_name =
                    ctx.args.try_get("boundedContext")?.string()?.to_string();
                require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;

                let deliveries = db::list_parked_deliveries(&state.pool, &bounded_context_name)
                    .await
                    .map_err(to_graphql_error)?;

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
                require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let id = ctx.args.try_get("id")?.string()?.to_string();

                let delivery = db::get_parked_delivery(&state.pool, &bounded_context_name, &id)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("ParkedDelivery", &id))?;

                match redrive_parked_delivery(state, &delivery).await {
                    Ok(()) => {
                        let removed =
                            db::delete_parked_delivery(&state.pool, &bounded_context_name, &id)
                                .await
                                .map_err(to_graphql_error)?
                                .unwrap_or(delivery);
                        Ok(Some(FieldValue::owned_any(removed)))
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
                require_admin_mapping(&ctx, &state.pool, &bounded_context_name).await?;
                let id = ctx.args.try_get("id")?.string()?.to_string();

                let discarded = db::delete_parked_delivery(&state.pool, &bounded_context_name, &id)
                    .await
                    .map_err(to_graphql_error)?
                    .ok_or_else(|| not_found("ParkedDelivery", &id))?;

                Ok(Some(FieldValue::owned_any(discarded)))
            })
        },
    )
    .argument(InputValue::new(
        "boundedContext",
        TypeRef::named_nn(TypeRef::STRING),
    ))
    .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::STRING)))
}
