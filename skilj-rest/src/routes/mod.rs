//! Capability-based routes, not type-or-context-in-path - the presented
//! `AccessToken` alone determines what's being read or written. See
//! docs/architecture.md §7.2 for the full route table. All six routes
//! are wired here:
//!
//! - `POST /v1/events/external`    - ExternalEventIngestion
//! - `POST /v1/events/direct`      - DirectEventCreation
//! - `GET  /v1/events`             - EventFetch::FetchEvents (client-tracked)
//! - `GET  /v1/events/consume`     - EventFetch::ConsumeEvents (server-tracked)
//! - `POST /v1/events/consume/ack` - EventFetch::AcknowledgeEvents (manual_ack only)
//! - `POST /v1/commands/trigger`   - CommandTrigger
//!
//! `CommandTrigger` needs an `Arc<dyn CommandDispatcher>` - `router()`'s
//! second parameter - to actually reach a bounded context's typed
//! `decide()`; see `CommandDispatcher`'s own doc comment
//! (`skilj-core::plugin`) and docs/architecture.md §1.7/§8 item 4 for why
//! that's a trait `skilj-core` owns rather than something this crate
//! reaches into `skilj` (the facade crate that actually builds one) for
//! directly - keeps `skilj-rest` usable standalone, per §3.1. `router()`'s
//! third parameter, `Arc<dyn ProjectionDispatcher>`, is the identical
//! reasoning applied to `project()` (§8 item 6) - every route that writes
//! an event calls `db::insert_event_and_update_sync_projections` rather
//! than the plain `insert_event`, threading this through.
//!
//! Presenting the wrong token variant at a route is a 403, not a 404 -
//! the route exists, the credential just doesn't authorize that action
//! (see `crate::error::RestError::WrongTokenVariant`).

use crate::auth::BearerCredential;
use crate::error::RestError;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
// `axum::extract::Query` doesn't support a repeated `filter=`/`filter=`
// query param deserializing into `Vec<String>` (`serde_urlencoded`, which
// it's built on, has no sequence support for query strings) - found via
// this pass's own real end-to-end REST test, not assumed.
// `axum_extra::extract::Query` (built on `serde_html_form`) does, and is
// otherwise a drop-in-compatible replacement for the single-value fields
// (`after`/`mode`) both query structs also carry.
use axum_extra::extract::Query;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use skilj_core::access_control::{
    CommandToken, DirectCreationToken, EventReadToken, ExternalEventToken,
};
use skilj_core::db::{self, AccessTokenKind, Pool};
use skilj_core::encryption::EncryptionMasterKey;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{self, AckMode, Event, EventBroadcaster};
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher};
use skilj_core::shared::{hash_secret, secret_matches, Filter, FilterOperator};
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    pool: Pool,
    dispatcher: Arc<dyn CommandDispatcher>,
    projection_dispatcher: Arc<dyn ProjectionDispatcher>,
    encryption_master_key: Option<EncryptionMasterKey>,
    event_broadcaster: EventBroadcaster,
    event_cache: EventCache,
}

pub fn router(
    pool: Pool,
    dispatcher: Arc<dyn CommandDispatcher>,
    projection_dispatcher: Arc<dyn ProjectionDispatcher>,
    encryption_master_key: Option<EncryptionMasterKey>,
    event_broadcaster: EventBroadcaster,
    event_cache: EventCache,
) -> Router {
    Router::new()
        .route("/v1/events/external", post(post_events_external))
        .route("/v1/events/direct", post(post_events_direct))
        .route("/v1/events", get(get_events))
        .route("/v1/events/consume", get(get_events_consume))
        .route("/v1/events/consume/ack", post(post_events_consume_ack))
        .route("/v1/commands/trigger", post(post_commands_trigger))
        .with_state(AppState {
            pool,
            dispatcher,
            projection_dispatcher,
            encryption_master_key,
            event_broadcaster,
            event_cache,
        })
}

// --- token resolution: BearerCredential -> the concrete AccessToken variant a route needs ---
//
// One generic `resolve_token::<T>` replaces what were four near-identical
// functions (one per `AccessToken` variant) differing only in which
// `AccessTokenKind`/`db::get_*_token` getter/struct type they closed
// over - real duplication, but a plain-Rust-generics fit, not a macro
// one: `TokenLookup` names the one axis each variant actually differs on.
//
// Each call tells three outcomes apart: no AccessToken has this id at
// all, one does but of a different AccessTokenKind (403 - wrong variant),
// or one does, the kind matches, but the presented secret doesn't
// (folded into the same "unrecognised credential" 401 as "no such id" -
// see RestError::UnrecognisedCredential's own doc comment for why that
// pair doesn't get told apart on the wire). Everything else about
// whether this token is actually allowed to do what the route is asking
// (active, opted in, right bounded context) is left to the skilj-core
// rule the handler calls next - not this layer's job.
//
// `token.secret()` as loaded from `T::get` is `hash_secret`'s output (see
// AccessToken.secret), never the plaintext - so the presented half of the
// credential is hashed here too before `secret_matches` ever compares the
// two, always comparing hash against hash.

/// One `AccessToken` variant's own `AccessTokenKind` tag, `db::get_*_token`
/// getter, and `secret` accessor - the whole of what `resolve_token`
/// needs to be generic over.
trait TokenLookup: Sized {
    const KIND: AccessTokenKind;

    async fn get(pool: &Pool, id: &str) -> skilj_core::error::Result<Option<Self>>;

    fn secret(&self) -> &str;
}

impl TokenLookup for ExternalEventToken {
    const KIND: AccessTokenKind = AccessTokenKind::ExternalEvent;

    async fn get(pool: &Pool, id: &str) -> skilj_core::error::Result<Option<Self>> {
        db::get_external_event_token(pool, id).await
    }

    fn secret(&self) -> &str {
        &self.secret
    }
}

impl TokenLookup for DirectCreationToken {
    const KIND: AccessTokenKind = AccessTokenKind::DirectCreation;

    async fn get(pool: &Pool, id: &str) -> skilj_core::error::Result<Option<Self>> {
        db::get_direct_creation_token(pool, id).await
    }

    fn secret(&self) -> &str {
        &self.secret
    }
}

impl TokenLookup for EventReadToken {
    const KIND: AccessTokenKind = AccessTokenKind::EventRead;

    async fn get(pool: &Pool, id: &str) -> skilj_core::error::Result<Option<Self>> {
        db::get_event_read_token(pool, id).await
    }

    fn secret(&self) -> &str {
        &self.secret
    }
}

impl TokenLookup for CommandToken {
    const KIND: AccessTokenKind = AccessTokenKind::Command;

    async fn get(pool: &Pool, id: &str) -> skilj_core::error::Result<Option<Self>> {
        db::get_command_token(pool, id).await
    }

    fn secret(&self) -> &str {
        &self.secret
    }
}

async fn resolve_token<T: TokenLookup>(
    state: &AppState,
    credential: &BearerCredential,
) -> Result<T, RestError> {
    match db::access_token_kind(&state.pool, &credential.id).await? {
        None => Err(RestError::UnrecognisedCredential),
        Some(kind) if kind == T::KIND => {
            let token = T::get(&state.pool, &credential.id)
                .await?
                .expect("access_token_kind matched T::KIND, so T::get finding none is a real bug");
            if secret_matches(&hash_secret(&credential.secret), token.secret()) {
                Ok(token)
            } else {
                Err(RestError::UnrecognisedCredential)
            }
        }
        Some(_) => Err(RestError::WrongTokenVariant),
    }
}

// --- wire DTOs - §7.3's request/response bodies ---

#[derive(Serialize)]
struct SequenceResponse {
    sequence: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExternalEventRequest {
    payload: serde_json::Value,
    source_content: String,
    source_context: Option<String>,
}

#[derive(Deserialize)]
struct DirectEventRequest {
    payload: serde_json::Value,
}

#[derive(Serialize)]
struct TagDto {
    key: String,
    value: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataDto {
    r#type: String,
    version: i64,
    client_id: String,
    created_at: chrono::DateTime<Utc>,
}

/// `payload` is re-parsed as native JSON (not left as the stored string)
/// so a client sees `{"amount": 5}`, not `"{\"amount\": 5}"` - the same
/// "deliberately coarse on the wire contract" per-event shape
/// `event_store::query_events`'s own doc comment describes, made
/// concrete here for REST's own response body.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventDto {
    sequence: i64,
    event_type: String,
    payload: serde_json::Value,
    tags: Vec<TagDto>,
    metadata: MetadataDto,
}

impl From<&Event> for EventDto {
    fn from(e: &Event) -> Self {
        EventDto {
            sequence: e.sequence,
            event_type: e.event_type.name.clone(),
            payload: serde_json::from_str(&e.payload)
                .unwrap_or_else(|_| serde_json::Value::String(e.payload.clone())),
            tags: e
                .tags
                .iter()
                .map(|t| TagDto {
                    key: t.key.clone(),
                    value: t.value.clone(),
                })
                .collect(),
            metadata: MetadataDto {
                r#type: e.metadata.r#type.clone(),
                version: e.metadata.version,
                client_id: e.metadata.client_id.clone(),
                created_at: e.metadata.created_at,
            },
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventsResponse {
    events: Vec<EventDto>,
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct ConsumeResponse {
    events: Vec<EventDto>,
}

#[derive(Deserialize)]
struct EventsQuery {
    #[serde(default)]
    filter: Vec<String>,
    after: Option<i64>,
}

#[derive(Deserialize)]
struct ConsumeQuery {
    mode: Option<String>,
    #[serde(default)]
    filter: Vec<String>,
}

/// §7.3's `filter=field:op:value` wire shape, repeatable
/// (`?filter=a:equals:1&filter=b:contains:x`) - `value` may itself
/// contain `:`, so only the first two colons are structural
/// (`splitn(3, ':')`). Operator tokens match `FilterOperator`'s own
/// `#[serde(rename_all = "snake_case")]` spelling exactly, the same
/// tokens the spec text itself uses. Malformed syntax or an unrecognised
/// operator is this crate's own routing-layer rejection
/// (`RestError::InvalidRequest`) - a real schema/operator mismatch (the
/// field doesn't exist, or `greater_than` on a string) is deliberately
/// *not* caught here; it flows through to `valid_filters` and surfaces
/// as the normal `Core(EventStoreError::InvalidFilter)` → 400 path.
fn parse_filter_param(raw: &str) -> Result<Filter, RestError> {
    let mut parts = raw.splitn(3, ':');
    let (Some(field), Some(op), Some(value)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(RestError::InvalidRequest(format!(
            "malformed filter {raw:?} - expected field:op:value"
        )));
    };
    let operator = match op {
        "equals" => FilterOperator::Equals,
        "contains" => FilterOperator::Contains,
        "is_like" => FilterOperator::IsLike,
        "greater_than" => FilterOperator::GreaterThan,
        "less_than" => FilterOperator::LessThan,
        other => {
            return Err(RestError::InvalidRequest(format!(
                "unknown filter operator {other:?} - expected one of equals, contains, \
                 is_like, greater_than, less_than"
            )))
        }
    };
    Ok(Filter {
        field: field.to_string(),
        operator,
        value: value.to_string(),
    })
}

fn parse_filter_params(raw: &[String]) -> Result<Vec<Filter>, RestError> {
    raw.iter().map(|s| parse_filter_param(s)).collect()
}

#[derive(Deserialize)]
struct AckRequest {
    sequence: i64,
}

#[derive(Serialize)]
struct EmptyResponse {}

#[derive(Deserialize)]
struct CommandTriggerRequest {
    payload: serde_json::Value,
}

/// §7.3's `-> 200 { accepted: true, triggeredEventSequences: [...] }` /
/// `-> 200 { accepted: false, rejectionReason: ..., rejectionKind: ... }`
/// pair as one struct, matching §5.4/§7.3: a rejection is a legitimate
/// outcome, not an HTTP-level error, so this always renders as `200` -
/// `accepted` is what a client branches on, not the status code.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CommandTriggerResponse {
    accepted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    triggered_event_sequences: Option<Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rejection_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rejection_kind: Option<String>,
}

// --- handlers ---

async fn post_events_external(
    State(state): State<AppState>,
    credential: BearerCredential,
    Json(body): Json<ExternalEventRequest>,
) -> Result<impl IntoResponse, RestError> {
    let token = resolve_token::<ExternalEventToken>(&state, &credential).await?;
    let payload = serde_json::to_string(&body.payload)
        .expect("serde_json::Value serialization is infallible");

    // `next_sequence`'s own lock, `create_external_event`'s pure
    // construction, and the insert all happen inside one transaction
    // now - see `db::create_and_insert_external_event`'s own doc
    // comment for why a rejection here no longer burns a sequence
    // number the way it used to.
    let event = db::create_and_insert_external_event(
        &state.pool,
        state.projection_dispatcher.as_ref(),
        &state.event_broadcaster,
        &state.event_cache,
        &token,
        payload,
        body.source_content,
        body.source_context,
        Utc::now(),
        state.encryption_master_key.as_ref(),
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(SequenceResponse {
            sequence: event.sequence,
        }),
    ))
}

async fn post_events_direct(
    State(state): State<AppState>,
    credential: BearerCredential,
    Json(body): Json<DirectEventRequest>,
) -> Result<impl IntoResponse, RestError> {
    let token = resolve_token::<DirectCreationToken>(&state, &credential).await?;
    let payload = serde_json::to_string(&body.payload)
        .expect("serde_json::Value serialization is infallible");

    // See `post_events_external`'s own comment -
    // `db::create_and_insert_direct_event`'s identical fix.
    let event = db::create_and_insert_direct_event(
        &state.pool,
        state.projection_dispatcher.as_ref(),
        &state.event_broadcaster,
        &state.event_cache,
        &token,
        payload,
        Utc::now(),
        state.encryption_master_key.as_ref(),
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(SequenceResponse {
            sequence: event.sequence,
        }),
    ))
}

async fn get_events(
    State(state): State<AppState>,
    credential: BearerCredential,
    Query(query): Query<EventsQuery>,
) -> Result<impl IntoResponse, RestError> {
    let filters = parse_filter_params(&query.filter)?;
    let token = resolve_token::<EventReadToken>(&state, &credential).await?;
    let events = db::list_events_cached(
        &state.pool,
        &state.event_cache,
        &token.event_type.bounded_context.name,
        &token.event_type.name,
        query.after.unwrap_or(-1),
    )
    .await?;

    let matched = event_store::fetch_events(&token, &events, &filters, query.after)?;
    let next_cursor = matched
        .last()
        .map(|e| e.sequence.to_string())
        .or_else(|| query.after.map(|a| a.to_string()));

    Ok(Json(EventsResponse {
        events: matched.iter().map(EventDto::from).collect(),
        next_cursor,
    }))
}

async fn get_events_consume(
    State(state): State<AppState>,
    credential: BearerCredential,
    Query(query): Query<ConsumeQuery>,
) -> Result<impl IntoResponse, RestError> {
    let filters = parse_filter_params(&query.filter)?;
    let token = resolve_token::<EventReadToken>(&state, &credential).await?;
    let ack_mode = match query.mode.as_deref() {
        None => None,
        Some("auto") => Some(AckMode::AutoAdvance),
        Some("manual") => Some(AckMode::ManualAck),
        Some(_) => {
            return Err(RestError::InvalidRequest(
                "mode must be \"auto\" or \"manual\"".to_string(),
            ))
        }
    };

    let existing_cursor = db::get_read_cursor(&state.pool, &token).await?;
    let events = db::list_events_cached(
        &state.pool,
        &state.event_cache,
        &token.event_type.bounded_context.name,
        &token.event_type.name,
        existing_cursor.as_ref().map(|c| c.sequence).unwrap_or(-1),
    )
    .await?;

    let result = event_store::consume_events(
        &token,
        existing_cursor.as_ref(),
        ack_mode,
        &events,
        &filters,
        Utc::now(),
    )?;
    db::apply_cursor_update(&state.pool, &token, &result.cursor_update).await?;

    Ok(Json(ConsumeResponse {
        events: result.served.iter().map(EventDto::from).collect(),
    }))
}

async fn post_events_consume_ack(
    State(state): State<AppState>,
    credential: BearerCredential,
    Json(body): Json<AckRequest>,
) -> Result<impl IntoResponse, RestError> {
    let token = resolve_token::<EventReadToken>(&state, &credential).await?;
    let cursor = db::get_read_cursor(&state.pool, &token).await?;

    let (sequence, updated_at) =
        event_store::acknowledge_events(&token, cursor.as_ref(), body.sequence, Utc::now())?;
    db::record_acknowledgement(&state.pool, &token, sequence, updated_at).await?;

    Ok(Json(EmptyResponse {}))
}

async fn post_commands_trigger(
    State(state): State<AppState>,
    credential: BearerCredential,
    Json(body): Json<CommandTriggerRequest>,
) -> Result<impl IntoResponse, RestError> {
    let token = resolve_token::<CommandToken>(&state, &credential).await?;
    let payload = serde_json::to_string(&body.payload)
        .expect("serde_json::Value serialization is infallible");

    let authorised = event_store::authorise_command_trigger(&token, payload)?;
    let bounded_context_name = authorised.command_type.bounded_context.name.clone();

    // The optimistic, unlocked half: read matching events and call
    // dispatch() once, same as always - see the note above the rules in
    // specs/skilj.allium ("reading matching events and running decide()
    // beforehand does not [need the lock]"). `db::submit_command` below
    // is what re-checks this under `next_sequence`'s own lock and
    // redispatches if a DCB conflict actually happened in between.
    let bounded_context_events = db::list_events_for_bounded_context_cached(
        &state.pool,
        &state.event_cache,
        &bounded_context_name,
        -1,
    )
    .await?;
    let consistency_tags =
        event_store::derive_tags(&authorised.command_type.tag_mappings, &authorised.payload);
    let (_boundary, matching_events) = event_store::consistency_boundary_and_matching_events(
        &bounded_context_events,
        &consistency_tags,
    );

    let decision = match state.dispatcher.dispatch(
        &bounded_context_name,
        &authorised.command_type.name,
        &authorised.payload,
        &matching_events,
    ) {
        None => return Err(RestError::NoDeciderRegistered),
        Some(Err(e)) => return Err(e.into()),
        Some(Ok(decision)) => decision,
    };

    let outcome = db::submit_command(
        &state.pool,
        state.dispatcher.as_ref(),
        state.projection_dispatcher.as_ref(),
        &state.event_broadcaster,
        &state.event_cache,
        &authorised.command_type,
        &authorised.payload,
        &authorised.client_id,
        &bounded_context_events,
        &consistency_tags,
        decision,
        state.encryption_master_key.as_ref(),
        Utc::now(),
    )
    .await?;

    Ok(Json(match outcome {
        // §5.4/§7.3: a legitimate business outcome, not an HTTP error -
        // whether this was the first decision or a DCB-conflict retry
        // inside submit_command, a rejection renders identically either
        // way.
        db::SubmitCommandOutcome::Rejected { reason, kind } => CommandTriggerResponse {
            accepted: false,
            triggered_event_sequences: None,
            rejection_reason: Some(reason),
            rejection_kind: Some(kind),
        },
        db::SubmitCommandOutcome::Accepted { events, .. } => CommandTriggerResponse {
            accepted: true,
            triggered_event_sequences: Some(events.iter().map(|e| e.sequence).collect()),
            rejection_reason: None,
            rejection_kind: None,
        },
    }))
}
