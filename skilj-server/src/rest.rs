//! The REST track: AccessToken bearer credentials, one capability each.
//!
//! ```text
//! POST /rest/v1/events/{type}/external         SubmitExternalEvent
//! POST /rest/v1/events/{type}/external/batch   SubmitExternalEvents
//! POST /rest/v1/events/{type}/direct           SubmitDirectEvent
//! GET  /rest/v1/events/{type}?after=&filter=   FetchEvents
//! POST /rest/v1/events/{type}/consume          ConsumeEvents
//! POST /rest/v1/events/{type}/ack              AcknowledgeEvents
//! POST /rest/v1/commands/{type}                TriggerCommand
//! POST /rest/v1/commands/{type}/dry-run        DryRunTriggerCommand
//! POST /rest/v1/commands/{type}/batch          TriggerCommands
//! ```
//!
//! A fetch hands out `cursor = "<epoch>@<sequence>"`; passing it back as
//! `after` sends the epoch along without the caller asking.

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};
use skilj_core::engine::{
    CommandOutcome, CommandRequest, EventPage, ExternalEventItem, ExternalOutcome,
};
use skilj_core::model::{AccessToken, AckMode, Filter, FilterOperator};
use skilj_core::{Error, Skilj};

pub fn router(skilj: Skilj) -> Router {
    Router::new()
        .route("/rest/v1/events/{ty}/external", post(external))
        .route("/rest/v1/events/{ty}/external/batch", post(external_batch))
        .route("/rest/v1/events/{ty}/direct", post(direct))
        .route("/rest/v1/events/{ty}", get(fetch))
        .route("/rest/v1/events/{ty}/consume", post(consume))
        .route("/rest/v1/events/{ty}/ack", post(ack))
        .route("/rest/v1/commands/{ty}", post(trigger))
        .route("/rest/v1/commands/{ty}/dry-run", post(dry_run))
        .route("/rest/v1/commands/{ty}/batch", post(trigger_batch))
        .with_state(skilj)
}

pub struct ApiError(pub Error);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        ApiError(e)
    }
}

pub fn status_of(e: &Error) -> StatusCode {
    match e {
        Error::NotFound(_) => StatusCode::NOT_FOUND,
        Error::Forbidden(_) => StatusCode::FORBIDDEN,
        Error::Invalid(_) | Error::Json(_) => StatusCode::BAD_REQUEST,
        Error::Conflict(_) | Error::Archived(_) | Error::EpochMismatch => StatusCode::CONFLICT,
        Error::Rejected { .. } => StatusCode::UNPROCESSABLE_ENTITY,
        Error::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
        Error::NotDeclared(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let msg = match &self.0 {
            Error::Database(_) => "internal error".to_string(),
            e => e.to_string(),
        };
        if matches!(self.0, Error::Database(_)) {
            tracing::error!("{}", self.0);
        }
        (
            status_of(&self.0),
            Json(json!({"error": {"code": self.0.code(), "message": msg}})),
        )
            .into_response()
    }
}

async fn token(skilj: &Skilj, headers: &HeaderMap) -> Result<AccessToken, ApiError> {
    let h = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or_else(|| ApiError(Error::forbidden("missing bearer token")))?;
    Ok(skilj.authenticate_token(h.trim()).await?)
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ExternalBody {
    payload: Value,
    source_content: Option<Value>,
    source_context: Option<String>,
    dedupe_partition_key: Option<String>,
    dedupe_sequence: Option<i64>,
    idempotency_key: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

impl ExternalBody {
    fn item(self, header_key: Option<String>) -> ExternalEventItem {
        // The header is ignored when the body names a dedupe cursor.
        let key = if self.dedupe_partition_key.is_some() {
            self.idempotency_key
        } else {
            self.idempotency_key.or(header_key)
        };
        ExternalEventItem {
            payload: self.payload,
            source_content: match self.source_content {
                Some(Value::String(s)) => s,
                Some(v) => v.to_string(),
                None => String::new(),
            },
            source_context: self.source_context,
            dedupe_partition_key: self.dedupe_partition_key,
            dedupe_sequence: self.dedupe_sequence,
            idempotency_key: key,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
        }
    }
}

fn header_key(h: &HeaderMap) -> Option<String> {
    h.get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

fn external_json(o: &ExternalOutcome) -> (StatusCode, Value) {
    match o {
        ExternalOutcome::Created(e) => (
            StatusCode::CREATED,
            json!({"status": "created", "sequence": e.sequence, "correlationId": e.metadata.correlation_id}),
        ),
        ExternalOutcome::Redelivered { sequence } => (
            StatusCode::OK,
            json!({"status": "redelivered", "sequence": sequence}),
        ),
        ExternalOutcome::Failed(m, c) => (
            StatusCode::BAD_REQUEST,
            json!({"status": "failed", "error": {"code": c, "message": m}}),
        ),
        ExternalOutcome::NotProcessed => (StatusCode::OK, json!({"status": "not_processed"})),
    }
}

async fn external(
    State(s): State<Skilj>,
    Path(ty): Path<String>,
    h: HeaderMap,
    Json(b): Json<ExternalBody>,
) -> Result<Response, ApiError> {
    let t = token(&s, &h).await?;
    let o = s
        .submit_external_event(&t, &ty, b.item(header_key(&h)))
        .await?;
    let (code, body) = external_json(&o);
    Ok((code, Json(body)).into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExternalBatchBody {
    events: Vec<ExternalBody>,
    #[serde(default)]
    stop_at_first_failure: bool,
}

async fn external_batch(
    State(s): State<Skilj>,
    Path(ty): Path<String>,
    h: HeaderMap,
    Json(b): Json<ExternalBatchBody>,
) -> Result<Response, ApiError> {
    let t = token(&s, &h).await?;
    let items = b.events.into_iter().map(|e| e.item(None)).collect();
    let out = s
        .submit_external_events(&t, &ty, items, b.stop_at_first_failure)
        .await?;
    Ok(
        Json(json!({"outcomes": out.iter().map(|o| external_json(o).1).collect::<Vec<_>>()}))
            .into_response(),
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DirectBody {
    payload: Value,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

async fn direct(
    State(s): State<Skilj>,
    Path(ty): Path<String>,
    h: HeaderMap,
    Json(b): Json<DirectBody>,
) -> Result<Response, ApiError> {
    let t = token(&s, &h).await?;
    let e = s
        .submit_direct_event(&t, &ty, b.payload, b.correlation_id, b.causation_id)
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"sequence": e.sequence, "correlationId": e.metadata.correlation_id})),
    )
        .into_response())
}

/// `field:operator:value`, operator in snake_case.
fn parse_filter(s: &str) -> Result<Filter, ApiError> {
    let mut parts = s.splitn(3, ':');
    let (Some(field), Some(op), Some(value)) = (parts.next(), parts.next(), parts.next()) else {
        return Err(ApiError(Error::invalid(format!(
            "filter '{s}' is not field:operator:value"
        ))));
    };
    let operator: FilterOperator = serde_json::from_value(Value::String(op.into()))
        .map_err(|_| ApiError(Error::invalid(format!("unknown operator '{op}'"))))?;
    Ok(Filter {
        field: field.into(),
        operator,
        value: value.into(),
    })
}

fn page_json(p: &EventPage) -> Value {
    json!({
        "events": p.events,
        "epoch": p.epoch,
        "cursor": p.last_sequence.map(|s| format!("{}@{s}", p.epoch)),
    })
}

/// "N" or "epoch@N".
fn parse_after(s: &str) -> Result<(Option<String>, i64), ApiError> {
    let (epoch, n) = match s.rsplit_once('@') {
        Some((e, n)) => (Some(e.to_string()), n),
        None => (None, s),
    };
    let n = n
        .parse()
        .map_err(|_| ApiError(Error::invalid("after must be a sequence or a cursor")))?;
    Ok((epoch, n))
}

async fn fetch(
    State(s): State<Skilj>,
    Path(ty): Path<String>,
    h: HeaderMap,
    Query(q): Query<Vec<(String, String)>>,
) -> Result<Response, ApiError> {
    let t = token(&s, &h).await?;
    let mut filters = vec![];
    let mut after = None;
    let mut epoch = None;
    for (k, v) in q {
        match k.as_str() {
            "filter" => filters.push(parse_filter(&v)?),
            "after" | "afterSequence" => {
                let (e, n) = parse_after(&v)?;
                after = Some(n);
                if e.is_some() {
                    epoch = e;
                }
            }
            "epoch" => epoch = Some(v),
            other => {
                return Err(ApiError(Error::invalid(format!(
                    "unknown parameter '{other}'"
                ))))
            }
        }
    }
    let page = s
        .fetch_events(&t, &ty, &filters, after, epoch.as_deref())
        .await?;
    Ok(Json(page_json(&page)).into_response())
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct ConsumeBody {
    ack_mode: Option<AckMode>,
    filters: Vec<String>,
}

async fn consume(
    State(s): State<Skilj>,
    Path(ty): Path<String>,
    h: HeaderMap,
    body: Option<Json<ConsumeBody>>,
) -> Result<Response, ApiError> {
    let t = token(&s, &h).await?;
    if t.type_name != ty {
        return Err(ApiError(Error::forbidden(
            "token is scoped to another event type",
        )));
    }
    let b = body.map(|Json(b)| b).unwrap_or_default();
    let filters = b
        .filters
        .iter()
        .map(|f| parse_filter(f))
        .collect::<Result<Vec<_>, _>>()?;
    let page = s.consume_events(&t, b.ack_mode, &filters).await?;
    Ok(Json(page_json(&page)).into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AckBody {
    sequence: Value,
    epoch: Option<String>,
}

async fn ack(
    State(s): State<Skilj>,
    Path(ty): Path<String>,
    h: HeaderMap,
    Json(b): Json<AckBody>,
) -> Result<Response, ApiError> {
    let t = token(&s, &h).await?;
    if t.type_name != ty {
        return Err(ApiError(Error::forbidden(
            "token is scoped to another event type",
        )));
    }
    let (epoch, seq) = match &b.sequence {
        Value::Number(n) => (None, n.as_i64().unwrap_or(-1)),
        Value::String(s) => parse_after(s)?,
        _ => {
            return Err(ApiError(Error::invalid(
                "sequence must be a number or a cursor",
            )))
        }
    };
    s.acknowledge_events(&t, seq, b.epoch.or(epoch).as_deref())
        .await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommandBody {
    payload: Value,
    idempotency_key: Option<String>,
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

pub fn outcome_json(o: &CommandOutcome) -> Value {
    match o {
        CommandOutcome::Accepted { command, events } => json!({
            "status": "accepted", "commandId": command.id,
            "events": events.iter().map(|e| json!({"sequence": e.sequence, "type": e.event_type})).collect::<Vec<_>>()
        }),
        CommandOutcome::Duplicate { command_id, events } => json!({
            "status": "duplicate", "commandId": command_id,
            "events": events.iter().map(|e| json!({"sequence": e.sequence, "type": e.event_type})).collect::<Vec<_>>()
        }),
        CommandOutcome::Rejected { reason, kind, .. } => {
            json!({"status": "rejected", "rejectionReason": reason, "rejectionKind": kind})
        }
        CommandOutcome::Refused { code, message } => {
            json!({"status": "refused", "error": {"code": code, "message": message}})
        }
        CommandOutcome::NotProcessed => json!({"status": "not_processed"}),
    }
}

async fn trigger(
    State(s): State<Skilj>,
    Path(ty): Path<String>,
    h: HeaderMap,
    Json(b): Json<CommandBody>,
) -> Result<Response, ApiError> {
    let t = token(&s, &h).await?;
    let req = CommandRequest {
        command_type: ty.clone(),
        payload: b.payload,
        idempotency_key: b.idempotency_key.or_else(|| header_key(&h)),
        correlation_id: b.correlation_id,
        causation_id: b.causation_id,
    };
    let o = s.trigger_command(&t, &ty, req).await?;
    let code = match &o {
        CommandOutcome::Accepted { .. } => StatusCode::CREATED,
        CommandOutcome::Rejected { .. } => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::OK,
    };
    Ok((code, Json(outcome_json(&o))).into_response())
}

async fn dry_run(
    State(s): State<Skilj>,
    Path(ty): Path<String>,
    h: HeaderMap,
    Json(b): Json<CommandBody>,
) -> Result<Response, ApiError> {
    let t = token(&s, &h).await?;
    let d = s.dry_run_trigger(&t, &ty, b.payload).await?;
    Ok(Json(json!({"accepted": d.accepted, "rejectionReason": d.rejection_reason, "rejectionKind": d.rejection_kind})).into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchBody {
    commands: Vec<CommandBody>,
    #[serde(default)]
    stop_at_first_failure: bool,
}

async fn trigger_batch(
    State(s): State<Skilj>,
    Path(ty): Path<String>,
    h: HeaderMap,
    Json(b): Json<BatchBody>,
) -> Result<Response, ApiError> {
    let t = token(&s, &h).await?;
    let reqs = b
        .commands
        .into_iter()
        .map(|c| CommandRequest {
            command_type: ty.clone(),
            payload: c.payload,
            idempotency_key: c.idempotency_key,
            correlation_id: c.correlation_id,
            causation_id: c.causation_id,
        })
        .collect();
    let out = s
        .trigger_commands(&t, &ty, reqs, b.stop_at_first_failure)
        .await?;
    Ok(Json(json!({"outcomes": out.iter().map(outcome_json).collect::<Vec<_>>()})).into_response())
}
