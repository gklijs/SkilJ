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
use skilj_core::event_store::{self, AckMode, Event, EventBroadcaster, EventType};
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher};
use skilj_core::shared::{secret_matches, CommandDecision, Filter, FilterOperator};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
struct AppState {
    pool: Pool,
    dispatcher: Arc<dyn CommandDispatcher>,
    projection_dispatcher: Arc<dyn ProjectionDispatcher>,
    encryption_master_key: Option<EncryptionMasterKey>,
    event_broadcaster: EventBroadcaster,
}

pub fn router(
    pool: Pool,
    dispatcher: Arc<dyn CommandDispatcher>,
    projection_dispatcher: Arc<dyn ProjectionDispatcher>,
    encryption_master_key: Option<EncryptionMasterKey>,
    event_broadcaster: EventBroadcaster,
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
        })
}

// --- token resolution: BearerCredential -> the concrete AccessToken variant a route needs ---
//
// Each of these tells three outcomes apart: no AccessToken has this id at
// all, one does but of a different AccessTokenKind (403 - wrong variant),
// or one does, the kind matches, but the presented secret doesn't
// (folded into the same "unrecognised credential" 401 as "no such id" -
// see RestError::UnrecognisedCredential's own doc comment for why that
// pair doesn't get told apart on the wire). Everything else about
// whether this token is actually allowed to do what the route is asking
// (active, opted in, right bounded context) is left to the skilj-core
// rule the handler calls next - not this layer's job.

async fn resolve_external_event_token(
    state: &AppState,
    credential: &BearerCredential,
) -> Result<ExternalEventToken, RestError> {
    match db::access_token_kind(&state.pool, &credential.id).await? {
        None => Err(RestError::UnrecognisedCredential),
        Some(AccessTokenKind::ExternalEvent) => {
            let token = db::get_external_event_token(&state.pool, &credential.id)
                .await?
                .expect(
                    "access_token_kind said ExternalEvent, get_external_event_token found none",
                );
            if secret_matches(&credential.secret, &token.secret) {
                Ok(token)
            } else {
                Err(RestError::UnrecognisedCredential)
            }
        }
        Some(_) => Err(RestError::WrongTokenVariant),
    }
}

async fn resolve_direct_creation_token(
    state: &AppState,
    credential: &BearerCredential,
) -> Result<DirectCreationToken, RestError> {
    match db::access_token_kind(&state.pool, &credential.id).await? {
        None => Err(RestError::UnrecognisedCredential),
        Some(AccessTokenKind::DirectCreation) => {
            let token = db::get_direct_creation_token(&state.pool, &credential.id)
                .await?
                .expect(
                    "access_token_kind said DirectCreation, get_direct_creation_token found none",
                );
            if secret_matches(&credential.secret, &token.secret) {
                Ok(token)
            } else {
                Err(RestError::UnrecognisedCredential)
            }
        }
        Some(_) => Err(RestError::WrongTokenVariant),
    }
}

async fn resolve_event_read_token(
    state: &AppState,
    credential: &BearerCredential,
) -> Result<EventReadToken, RestError> {
    match db::access_token_kind(&state.pool, &credential.id).await? {
        None => Err(RestError::UnrecognisedCredential),
        Some(AccessTokenKind::EventRead) => {
            let token = db::get_event_read_token(&state.pool, &credential.id)
                .await?
                .expect("access_token_kind said EventRead, get_event_read_token found none");
            if secret_matches(&credential.secret, &token.secret) {
                Ok(token)
            } else {
                Err(RestError::UnrecognisedCredential)
            }
        }
        Some(_) => Err(RestError::WrongTokenVariant),
    }
}

async fn resolve_command_token(
    state: &AppState,
    credential: &BearerCredential,
) -> Result<CommandToken, RestError> {
    match db::access_token_kind(&state.pool, &credential.id).await? {
        None => Err(RestError::UnrecognisedCredential),
        Some(AccessTokenKind::Command) => {
            let token = db::get_command_token(&state.pool, &credential.id)
                .await?
                .expect("access_token_kind said Command, get_command_token found none");
            if secret_matches(&credential.secret, &token.secret) {
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
    let token = resolve_external_event_token(&state, &credential).await?;
    let next_seq = db::next_sequence(&state.pool, &token.event_type.bounded_context.name).await?;
    let payload = serde_json::to_string(&body.payload)
        .expect("serde_json::Value serialization is infallible");

    // `protect_sensitive_fields`'s own pre-resolution step - see
    // `db::resolve_encryption_keys`'s own doc comment. A no-op unless
    // `token.event_type.sensitive_fields` actually names a subject this
    // payload carries.
    let bounded_context_name = token.event_type.bounded_context.name.clone();
    let mut resolved = HashMap::new();
    db::resolve_encryption_keys(
        &state.pool,
        &bounded_context_name,
        &token.event_type.sensitive_fields,
        &payload,
        state.encryption_master_key.as_ref(),
        &mut resolved,
    )
    .await?;

    let event = event_store::create_external_event(
        &token,
        payload,
        body.source_content,
        body.source_context,
        next_seq,
        Utc::now(),
        |subject_key, subject_value| {
            let (key, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect("resolve_encryption_keys pre-resolved every subject sensitive_field_subjects named");
            (key.clone(), data_key.clone())
        },
    )?;
    let encryption_key_ids = db::encryption_key_ids(&event.encryption_keys, &resolved);
    db::insert_event_and_update_sync_projections(
        &state.pool,
        &event,
        None,
        state.projection_dispatcher.as_ref(),
        &encryption_key_ids,
        &state.event_broadcaster,
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
    let token = resolve_direct_creation_token(&state, &credential).await?;
    let next_seq = db::next_sequence(&state.pool, &token.event_type.bounded_context.name).await?;
    let payload = serde_json::to_string(&body.payload)
        .expect("serde_json::Value serialization is infallible");

    let bounded_context_name = token.event_type.bounded_context.name.clone();
    let mut resolved = HashMap::new();
    db::resolve_encryption_keys(
        &state.pool,
        &bounded_context_name,
        &token.event_type.sensitive_fields,
        &payload,
        state.encryption_master_key.as_ref(),
        &mut resolved,
    )
    .await?;

    let event = event_store::create_direct_event(
        &token,
        payload,
        next_seq,
        Utc::now(),
        |subject_key, subject_value| {
            let (key, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect("resolve_encryption_keys pre-resolved every subject sensitive_field_subjects named");
            (key.clone(), data_key.clone())
        },
    )?;
    let encryption_key_ids = db::encryption_key_ids(&event.encryption_keys, &resolved);
    db::insert_event_and_update_sync_projections(
        &state.pool,
        &event,
        None,
        state.projection_dispatcher.as_ref(),
        &encryption_key_ids,
        &state.event_broadcaster,
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
    let token = resolve_event_read_token(&state, &credential).await?;
    let events = db::list_events(
        &state.pool,
        &token.event_type.bounded_context.name,
        &token.event_type.name,
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
    let token = resolve_event_read_token(&state, &credential).await?;
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
    let events = db::list_events(
        &state.pool,
        &token.event_type.bounded_context.name,
        &token.event_type.name,
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
    let token = resolve_event_read_token(&state, &credential).await?;
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
    let token = resolve_command_token(&state, &credential).await?;
    let payload = serde_json::to_string(&body.payload)
        .expect("serde_json::Value serialization is infallible");

    let authorised = event_store::authorise_command_trigger(&token, payload)?;
    let bounded_context_name = authorised.command_type.bounded_context.name.clone();

    // consistency_boundary_and_matching_events' own `matching_events` is
    // what decide() itself needs - process_command below recomputes the
    // identical thing internally for the command it stores, per its own
    // doc comment (decide() may run more than once per submission under
    // the optimistic-then-locked retry pattern, so this isn't wasted
    // work, it's the two calls' own separate concerns).
    let bounded_context_events =
        db::list_events_for_bounded_context(&state.pool, &bounded_context_name).await?;
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

    let event_specs = match decision {
        CommandDecision::Rejected { reason, kind } => {
            // §5.4/§7.3: a legitimate business outcome, not an HTTP
            // error - process_command itself is never called on this
            // branch (it would turn this into an Err(CommandRejected)).
            return Ok(Json(CommandTriggerResponse {
                accepted: false,
                triggered_event_sequences: None,
                rejection_reason: Some(reason),
                rejection_kind: Some(kind),
            }));
        }
        CommandDecision::Accepted { events } => events,
    };

    // process_command's own `resolve_event_type`/`next_sequence` are
    // plain sync closures (see its own doc comment: decide() and
    // everything downstream of it stays I/O-free) - so every EventType
    // lookup and sequence allocation this call will need happens first,
    // here, and the closures below just index into what's already
    // in hand.
    let mut event_types_by_name: HashMap<String, EventType> = HashMap::new();
    for spec in &event_specs {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            event_types_by_name.entry(spec.event_type.clone())
        {
            if let Some(et) =
                db::get_event_type(&state.pool, &bounded_context_name, &spec.event_type).await?
            {
                entry.insert(et);
            }
        }
    }
    let mut sequences = Vec::with_capacity(event_specs.len());
    for _ in 0..event_specs.len() {
        sequences.push(db::next_sequence(&state.pool, &bounded_context_name).await?);
    }
    let mut sequences = sequences.into_iter();

    // `protect_sensitive_fields`'s own pre-resolution step, for the
    // command's own payload *and* every accepted event spec's - see
    // `db::resolve_encryption_keys`'s own doc comment on why this
    // accumulates into one shared map rather than resolving each
    // separately: a subject the command and one of its own events both
    // name must resolve to the identical `EncryptionKey`.
    let mut resolved = HashMap::new();
    db::resolve_encryption_keys(
        &state.pool,
        &bounded_context_name,
        &authorised.command_type.sensitive_fields,
        &authorised.payload,
        state.encryption_master_key.as_ref(),
        &mut resolved,
    )
    .await?;
    for spec in &event_specs {
        if let Some(event_type) = event_types_by_name.get(&spec.event_type) {
            let spec_payload = spec.payload.to_string();
            db::resolve_encryption_keys(
                &state.pool,
                &bounded_context_name,
                &event_type.sensitive_fields,
                &spec_payload,
                state.encryption_master_key.as_ref(),
                &mut resolved,
            )
            .await?;
        }
    }

    let result = event_store::process_command(
        &authorised.command_type,
        &authorised.payload,
        &authorised.client_id,
        &bounded_context_events,
        CommandDecision::Accepted {
            events: event_specs,
        },
        |name| event_types_by_name.get(name).cloned(),
        || {
            sequences.next().expect(
                "process_command called next_sequence more times than there are accepted events",
            )
        },
        Utc::now(),
        |subject_key, subject_value| {
            let (key, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect("resolve_encryption_keys pre-resolved every subject sensitive_field_subjects named");
            (key.clone(), data_key.clone())
        },
    )?;

    let command_key_ids = db::encryption_key_ids(&result.command.encryption_keys, &resolved);
    let command_id = db::insert_command(&state.pool, &result.command, &command_key_ids).await?;
    let mut triggered_event_sequences = Vec::with_capacity(result.events.len());
    for event in &result.events {
        let event_key_ids = db::encryption_key_ids(&event.encryption_keys, &resolved);
        db::insert_event_and_update_sync_projections(
            &state.pool,
            event,
            Some(command_id),
            state.projection_dispatcher.as_ref(),
            &event_key_ids,
            &state.event_broadcaster,
        )
        .await?;
        triggered_event_sequences.push(event.sequence);
    }

    Ok(Json(CommandTriggerResponse {
        accepted: true,
        triggered_event_sequences: Some(triggered_event_sequences),
        rejection_reason: None,
        rejection_kind: None,
    }))
}
