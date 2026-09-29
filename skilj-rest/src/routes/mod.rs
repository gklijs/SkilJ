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
//! - `POST /v1/parked-deliveries`  - ParkedDeliveryReport (Codeberg issue
//!   #21) - a message-broker bridge's own "I gave up retrying this
//!   inbound delivery" report, authenticated with the same
//!   `ExternalEventToken`/`CommandToken` credential the bridge already
//!   uses for the delivery itself (`kind` in the body says which),
//!   rather than a new credential kind of its own.
//!
//! `CommandTrigger` needs an `Arc<dyn CommandDispatcher>` - `router()`'s
//! second parameter - to actually reach a bounded context's typed
//! `decide()`; see `CommandDispatcher`'s own doc comment
//! (`skilj-core::plugin`) and docs/architecture.md §1.7/[§8](../../../docs/architecture.md#open-for-a-future-pass) item 4 for why
//! that's a trait `skilj-core` owns rather than something this crate
//! reaches into `skilj` (the facade crate that actually builds one) for
//! directly - keeps `skilj-rest` usable standalone, per §3.1. `router()`'s
//! third parameter, `Arc<dyn ProjectionDispatcher>`, is the identical
//! reasoning applied to `project()` ([§8](../../../docs/architecture.md#open-for-a-future-pass) item 6) - every route that writes
//! an event calls `db::insert_event_and_update_sync_projections` rather
//! than the plain `insert_event`, threading this through.
//!
//! Presenting the wrong token variant at a route is a 403, not a 404 -
//! the route exists, the credential just doesn't authorize that action
//! (see `crate::error::RestError::WrongTokenVariant`).

use crate::auth::BearerCredential;
use crate::error::RestError;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use opentelemetry::global;
use opentelemetry::metrics::Histogram;
use opentelemetry::KeyValue;
use opentelemetry_http::HeaderExtractor;
use std::sync::LazyLock;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
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
    CommandToken, DirectCreationToken, Error as AccessControlError, EventReadStartPosition,
    EventReadToken, ExternalEventToken, TokenStatus,
};
use skilj_core::command_batcher::CommandBatcher;
use skilj_core::db::{self, AccessTokenKind, Pool};
use skilj_core::encryption::EncryptionMasterKey;
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{self, AckMode, Event, EventBroadcaster};
use skilj_core::plugin::{CommandDispatcher, ProjectionDispatcher, SnapshotDispatcher};
use skilj_core::shared::{hash_secret, secret_matches, Filter, FilterOperator};
use std::sync::Arc;

/// See `skilj-core::db`'s own `meter()`/`LazyLock` doc comment for the
/// `global::meter()` snapshot-binding caveat this is subject to too -
/// only ever touched from inside `trace_request` below, strictly after a
/// real consuming app's `init_telemetry` has already run.
static REQUEST_DURATION: LazyLock<Histogram<f64>> = LazyLock::new(|| {
    opentelemetry::global::meter("skilj-rest")
        .f64_histogram("http.server.request.duration")
        .with_unit("s")
        .with_description("Duration of HTTP requests served by skilj-rest.")
        .build()
});

#[derive(Clone)]
struct AppState {
    pool: Pool,
    dispatcher: Arc<dyn CommandDispatcher>,
    projection_dispatcher: Arc<dyn ProjectionDispatcher>,
    snapshot_dispatcher: Arc<dyn SnapshotDispatcher>,
    encryption_master_key: Option<EncryptionMasterKey>,
    event_broadcaster: EventBroadcaster,
    event_cache: EventCache,
    /// `ConsumeEvents`' own `manual_ack` claim lease - see
    /// `skilj::SkiljBuilder::read_cursor_checkout_lease`'s own doc
    /// comment (Codeberg issue #25, docs/architecture.md §53).
    read_cursor_checkout_lease: chrono::Duration,
    /// `config.max_events_per_read` - the most events one `GET
    /// /v1/events` or `GET /v1/events/consume` serves. See
    /// `skilj::SkiljBuilder::max_events_per_read`.
    max_events_per_read: usize,
    /// Codeberg issue #32 (round two) - `post_commands_trigger`'s own
    /// real, externally-triggered submission volume is exactly what
    /// `CommandBatcher` exists to coalesce; see its own module doc
    /// comment.
    command_batcher: CommandBatcher,
}

/// One request-level span per REST call, its parent set from an incoming
/// W3C `traceparent`/`tracestate` header pair if present - so a trace
/// that starts in an upstream caller continues through this request
/// rather than restarting here. A safe no-op when the consuming
/// application never registers a real propagator (`opentelemetry::global`'s
/// default is a no-op propagator) - this crate never registers one
/// itself; only the consuming application does (see docs/architecture.md's
/// tracing section for the crate boundary this keeps).
///
/// Deliberately hand-rolled rather than `tower_http::trace::TraceLayer`:
/// `OpenTelemetrySpanExt::set_parent` only works *before* a span is ever
/// entered (it errors with `AlreadyStarted` afterwards, per its own doc
/// comment), and `TraceLayer` creates *and enters* its own span
/// internally with no hook to set the parent first - the two don't
/// compose safely. Here, the span is built and its parent set before
/// `.instrument()` ever polls (thus enters) it for the first time.
async fn trace_request(request: Request, next: Next) -> Response {
    let parent_cx = global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(request.headers()))
    });
    // Captured as owned values before `request` moves into `next.run(...)`
    // below - `http.route` deliberately the bare path, not the full URI
    // with query string: every route in this crate is a fixed string
    // (§7.2), so the path alone is already low-cardinality, which a raw
    // query string wouldn't be.
    let method = request.method().to_string();
    let route = request.uri().path().to_string();
    let span = tracing::info_span!(
        "request",
        method = %request.method(),
        uri = %request.uri(),
        status = tracing::field::Empty,
        // `tracing-opentelemetry`'s own well-known field name
        // (`SPAN_STATUS_DESCRIPTION_FIELD`) - recording it is what sets
        // this span's OTel status to `Error` with this description, via
        // `on_record`'s `SpanAttributeVisitor`. Verified against a real
        // `SdkTracerProvider` in `tests/tracing_middleware.rs` - not
        // `OpenTelemetrySpanExt::set_status` directly (which should also
        // work, but this field-based route is what the crate itself
        // documents for a status set well after span creation, and is
        // the one actually exercised by that test).
        otel.status_description = tracing::field::Empty,
    );
    // `Err` here just means no `tracing-opentelemetry` layer is installed
    // in this process (the common case for `skilj-rest` used standalone,
    // or `skilj-demo` run without `OTEL_EXPORTER_OTLP_ENDPOINT` set) -
    // nothing to propagate into, so nothing to do.
    let _ = span.set_parent(parent_cx);

    async move {
        let start = std::time::Instant::now();
        let response = next.run(request).await;
        let status = response.status();
        let span = tracing::Span::current();
        span.record("status", status.as_u16());
        // A 4xx is an expected, well-modelled outcome here - business
        // rejections render as 200 (§5.4/§7.5), auth/validation failures
        // as 4xx, so a 5xx is the one status class that's always a
        // genuine, unexpected server-side failure. Marked on the span
        // (so a collector's "show me errored traces" query finds it) and
        // logged at `error!` (so it's both console-visible regardless of
        // an operator's `RUST_LOG` filter for lower levels, and exported
        // as an error-severity OTel log record via the same bridge every
        // other `tracing` event already goes through).
        if status.is_server_error() {
            span.record("otel.status_description", status.to_string());
            tracing::error!(
                status = status.as_u16(),
                "request failed with a server error"
            );
        }
        REQUEST_DURATION.record(
            start.elapsed().as_secs_f64(),
            &[
                KeyValue::new("http.request.method", method),
                KeyValue::new("http.route", route),
                KeyValue::new("http.response.status_code", status.as_u16() as i64),
            ],
        );
        response
    }
    .instrument(span)
    .await
}

#[allow(clippy::too_many_arguments)]
pub fn router(
    pool: Pool,
    dispatcher: Arc<dyn CommandDispatcher>,
    projection_dispatcher: Arc<dyn ProjectionDispatcher>,
    snapshot_dispatcher: Arc<dyn SnapshotDispatcher>,
    encryption_master_key: Option<EncryptionMasterKey>,
    event_broadcaster: EventBroadcaster,
    event_cache: EventCache,
    read_cursor_checkout_lease: chrono::Duration,
    max_events_per_read: usize,
    command_batcher: CommandBatcher,
) -> Router {
    Router::new()
        .route("/v1/events/external", post(post_events_external))
        .route("/v1/events/direct", post(post_events_direct))
        .route("/v1/events", get(get_events))
        .route("/v1/events/consume", get(get_events_consume))
        .route("/v1/events/consume/ack", post(post_events_consume_ack))
        .route("/v1/commands/trigger", post(post_commands_trigger))
        .route("/v1/parked-deliveries", post(post_parked_deliveries))
        .layer(middleware::from_fn(trace_request))
        .with_state(AppState {
            pool,
            dispatcher,
            projection_dispatcher,
            snapshot_dispatcher,
            encryption_master_key,
            event_broadcaster,
            event_cache,
            read_cursor_checkout_lease,
            max_events_per_read: max_events_per_read.max(1),
            command_batcher,
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
            // `access_token_kind` and `T::get` each independently re-resolve
            // this token's own bounded context (`fetch_access_token_row`'s
            // own `access_token_index` lookup, then a per-schema row fetch) -
            // two full round trips, not one shared read. A concurrent
            // `DeleteBoundedContext` landing in the gap between them makes
            // this `None`, the identical race `skilj_core::db`'s own
            // `require_event_type`/`require_command_type` and
            // `skilj-graphql`'s `redrive_parked_delivery` already treat as
            // an ordinary outcome rather than "a real bug" - and on this
            // code path, every authenticated REST request runs it, not just
            // an occasional admin retry. From the caller's own point of
            // view a token that's vanished out from under it should look
            // exactly like one that was never recognised, not a 500.
            let Some(token) = T::get(&state.pool, &credential.id).await? else {
                return Err(RestError::UnrecognisedCredential);
            };
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
    // docs/architecture.md §39, specs/skilj.allium's own rule
    // CreateExternalEvent - both-or-neither by construction, not by a
    // runtime check: `DedupeRequest`'s own two fields are non-optional,
    // so a request supplying exactly one of them fails ordinary JSON
    // deserialization (a 400, the same as any other malformed body)
    // before this handler ever sees it - see db::DedupeCursor's own doc
    // comment for the full reasoning.
    dedupe: Option<DedupeRequest>,
    // Codeberg issue #18 - an adapter's own attachment point for an
    // upstream trace id (a message-broker bridge forwarding the id its
    // own broker already carried). `correlation_id` omitted means
    // generated server-side, never that the mechanism is skipped -
    // unlike `dedupe`/`idempotency_key`, every stored record ends up
    // with one regardless. Neither field takes any part in the dedupe
    // mechanism above - a redelivery with a fresh correlation_id is
    // still the same redelivery.
    correlation_id: Option<String>,
    causation_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DedupeRequest {
    partition_key: String,
    sequence: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExternalEventResponse {
    // `None` only for a `redelivered` response - nothing was created, so
    // there is no sequence to report, the same "no outcome to describe"
    // register CreateExternalEventOutcome::Redelivered itself uses.
    sequence: Option<i64>,
    redelivered: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DirectEventRequest {
    payload: serde_json::Value,
    // See `ExternalEventRequest`'s own identical pair (Codeberg issue #18).
    correlation_id: Option<String>,
    causation_id: Option<String>,
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
    // Codeberg issue #18. `correlation_id` is `None` only for a record
    // written before this field existed - see `Metadata`'s own doc
    // comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    causation_id: Option<String>,
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
                correlation_id: e.metadata.correlation_id.clone(),
                causation_id: e.metadata.causation_id.clone(),
            },
        }
    }
}

/// `event_type_*` - drift audit finding #8 (2026-08-20, see project
/// memory `skilj-drift-audit-2026-08-20`): `surface EventFetch`'s own
/// `exposes: event_type.name/schema/schema_version` had no REST route
/// returning `schema`/`schema_version` at all, only the bare name
/// (`EventDto.event_type`, per delivered event). Top-level here, not
/// repeated per event: `TokenScopedToEventType` already means every
/// event in one response shares the same `EventType`, since a token is
/// scoped to exactly one - the schema can't vary within a response, so
/// naming it once matches that invariant instead of fighting it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EventsResponse {
    events: Vec<EventDto>,
    next_cursor: Option<String>,
    event_type_name: String,
    event_type_schema: String,
    event_type_schema_version: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConsumeResponse {
    events: Vec<EventDto>,
    event_type_name: String,
    event_type_schema: String,
    event_type_schema_version: i64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventsQuery {
    #[serde(default)]
    filter: Vec<String>,
    after: Option<i64>,
    // Codeberg issue #18 - "show me everything in this transaction",
    // the REST-side counterpart to `queryEvents`'s own `correlationId`
    // GraphQL argument.
    correlation_id: Option<String>,
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
        "near" => FilterOperator::Near,
        "similar_color" => FilterOperator::SimilarColor,
        "in_subnet" => FilterOperator::InSubnet,
        "in" => FilterOperator::In,
        other => {
            return Err(RestError::InvalidRequest(format!(
                "unknown filter operator {other:?} - expected one of equals, contains, \
                 is_like, greater_than, less_than, near, similar_color, in_subnet, in"
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
#[serde(rename_all = "camelCase")]
struct CommandTriggerRequest {
    payload: serde_json::Value,
    // See `ExternalEventRequest`'s own identical pair (Codeberg issue
    // #18) - unlike that surface, a plain command trigger has no
    // upstream event of its own to attach a `causation_id` from, but a
    // bridge fronting this route may still know one (an upstream
    // broker's own message id), so it's accepted here too rather than
    // special-cased away.
    correlation_id: Option<String>,
    causation_id: Option<String>,
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
    // Codeberg issue #12: `true` only when an `Idempotency-Key` header
    // was given and it matched a prior `Accepted` outcome -
    // `triggered_event_sequences` is that prior outcome's, not a fresh
    // decision. Always `false` when no header was given, matching
    // today's behaviour exactly.
    deduplicated: bool,
    // Codeberg issue #18: echoes back the correlation_id the resulting
    // Command actually ended up with - the caller's own, if it supplied
    // one, or the one skilj generated on its behalf otherwise. `None`
    // for a rejection (`process_command` never runs, so nothing was ever
    // stored to have one) and for a deduplicated outcome (a cached prior
    // answer carries only `triggered_event_sequences` - the original
    // Command itself isn't re-fetched to answer this).
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<String>,
}

/// Codeberg issue #21 - which credential kind (and therefore which
/// original REST route) this parked delivery came from. Deliberately
/// not `CrossContextRoute` here - that kind is only ever written by
/// `db::catch_up_cross_context_route` directly (same process, real DB
/// access, no REST hop needed), never reported over the wire by an
/// external bridge.
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ParkedDeliveryKindRequest {
    ExternalEvent,
    CommandTrigger,
}

/// A message-broker bridge's own "I gave up retrying this inbound
/// delivery" report - see docs/architecture.md's parked-deliveries
/// section. `request` is the exact original `ExternalEventRequest`/
/// `CommandTriggerRequest` body the bridge already tried to submit
/// (verbatim, whichever shape `kind` says), stored as-is so
/// `retryParkedDelivery` can redrive it later without the bridge's own
/// involvement.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ParkedDeliveryRequest {
    /// The bridge's own name for itself (e.g. `"kafka-inbound"`,
    /// `"amqp-inbound"`, `"nats-inbound"`) - purely informational, not
    /// validated against any fixed set, since this crate has no notion
    /// of which bridges exist.
    source: String,
    kind: ParkedDeliveryKindRequest,
    /// The broker-native identifier for this message (e.g.
    /// `"{topic}:{partition}:{offset}"` for Kafka) - an operator's own
    /// way of finding the original message in the broker's own tooling,
    /// not used by skilj itself for anything.
    identifier: String,
    error: String,
    attempt_count: i32,
    first_failed_at: chrono::DateTime<Utc>,
    request: serde_json::Value,
    /// `command_trigger` only: the `Idempotency-Key` header the original
    /// `POST /v1/commands/trigger` carried, which isn't part of `request`
    /// (the body). Stored with it so `retryParkedDelivery` redrives
    /// under the same key - which dedupes against the original attempt
    /// if that attempt committed after all (a lost response, say) -
    /// rather than landing the command a second time.
    idempotency_key: Option<String>,
}

#[derive(Serialize)]
struct ParkedDeliveryResponse {
    id: String,
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
    let outcome = db::create_and_insert_external_event(
        &state.pool,
        state.projection_dispatcher.as_ref(),
        &state.event_broadcaster,
        &state.event_cache,
        &token,
        payload,
        body.source_content,
        body.source_context,
        body.correlation_id,
        body.causation_id,
        body.dedupe.as_ref().map(|d| db::DedupeCursor {
            partition_key: &d.partition_key,
            sequence: d.sequence,
        }),
        Utc::now(),
        state.encryption_master_key.as_ref(),
    )
    .await?;

    // A redelivery is still a 201 - the submission was accepted, exactly
    // as specs/skilj.allium's own ARedeliveryProducesNoEventAndNoOutcome
    // guarantee describes; it's just that nothing was created. Neither
    // outcome is an error, so both share this one success response shape,
    // distinguished by `redelivered` rather than by status code.
    let (sequence, redelivered) = match outcome {
        db::CreateExternalEventOutcome::Created(event) => (Some(event.sequence), false),
        db::CreateExternalEventOutcome::Redelivered => (None, true),
    };

    Ok((
        StatusCode::CREATED,
        Json(ExternalEventResponse {
            sequence,
            redelivered,
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
        body.correlation_id,
        body.causation_id,
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
    // At most `max_events_per_read`, loaded a chunk at a time - a caller
    // pages on with `after` = this response's `nextCursor`.
    let page = db::collect_scanned_event_page(
        &state.pool,
        &state.event_cache,
        &token.event_type.bounded_context.name,
        Some(&token.event_type.name),
        query.after.unwrap_or(-1),
        state.max_events_per_read,
        |chunk, remaining| {
            event_store::fetch_events_page(
                &token,
                chunk,
                &filters,
                query.after,
                query.correlation_id.as_deref(),
                remaining,
            )
        },
    )
    .await?;
    let matched = page.events;
    // A short page walked to the end of history, and nothing it passed
    // over matched - so the cursor moves past all of it, not just to the
    // last event served, and the next poll starts after what this one
    // already examined (docs/architecture.md §112). A full page stops at
    // its last event.
    let last_served = matched.last().map(|e| e.sequence);
    let next_position = if matched.len() < state.max_events_per_read {
        last_served.max(page.scanned_through)
    } else {
        last_served
    };
    let next_cursor = next_position
        .or(query.after)
        .map(|sequence| sequence.to_string());

    // `redact_private_fields` - unconditional, no `Role`/`access_mapping`
    // to condition it on (see that function's own doc comment, and
    // `EventFetch`'s `PrivateFieldsStayRedacted` guarantee): a private
    // field is stored in plaintext, so this REST track - which has no
    // path to decrypt a *sensitive* field either - must actively redact
    // one rather than rely on it already being ciphertext at rest.
    let matched: Vec<_> = matched
        .iter()
        .map(event_store::redact_private_fields)
        .collect();
    Ok(Json(EventsResponse {
        events: matched.iter().map(EventDto::from).collect(),
        next_cursor,
        event_type_name: token.event_type.name.clone(),
        event_type_schema: token.event_type.schema.clone(),
        event_type_schema_version: token.event_type.schema_version,
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

    // Codeberg issue #25's investigation (docs/architecture.md §53) - the
    // read and the write below share one transaction, guarded end to end
    // by `lock_read_cursor_for_consume`'s own advisory lock, so a second
    // concurrent request for the same token genuinely blocks here until
    // the first commits, rather than both reading the same pre-claim
    // snapshot the way a separate read-then-write each against a fresh
    // pooled connection would let them - see that function's own doc
    // comment for why a row lock alone wouldn't have closed this (a
    // token's very first-ever call has no row yet to lock).
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(skilj_core::error::Error::from)?;
    db::lock_read_cursor_for_consume(&mut tx, &token).await?;
    let existing_cursor = db::get_read_cursor(&mut *tx, &token).await?;
    let bounded_context_name = &token.event_type.bounded_context.name;
    // Where serving starts: the cursor, or for a new one its seed
    // (`initial_consume_position`) - which for `Latest`/`AtTime` is the
    // highest qualifying sequence in the whole history, found a chunk at
    // a time rather than by loading all of it.
    let position = match &existing_cursor {
        Some(cursor) => cursor.sequence,
        None => match token.start_from {
            EventReadStartPosition::Latest | EventReadStartPosition::AtTime => {
                let mut position = -1;
                db::for_each_event_chunk(
                    &state.pool,
                    &state.event_cache,
                    bounded_context_name,
                    Some(&token.event_type.name),
                    -1,
                    state.max_events_per_read,
                    |chunk| {
                        position =
                            position.max(event_store::initial_consume_position(&token, chunk));
                        Ok(true)
                    },
                )
                .await?;
                position
            }
            _ => event_store::initial_consume_position(&token, std::iter::empty()),
        },
    };
    // The page after `position`: `fetch_events_page` applies exactly the
    // filters `ConsumeEvents` serves by (type, `filters`, owner scope),
    // and `consume_events_page` below re-applies them over this already
    // bounded set along with the lease and cursor logic.
    let candidates = db::collect_scanned_event_page(
        &state.pool,
        &state.event_cache,
        bounded_context_name,
        Some(&token.event_type.name),
        position,
        state.max_events_per_read,
        |chunk, remaining| {
            event_store::fetch_events_page(&token, chunk, &filters, Some(position), None, remaining)
        },
    )
    .await?;

    let result = event_store::consume_events_page(
        &token,
        existing_cursor.as_ref(),
        ack_mode,
        position,
        &candidates.events,
        candidates.scanned_through,
        &filters,
        Utc::now(),
        state.read_cursor_checkout_lease,
        state.max_events_per_read,
    )?;
    db::apply_cursor_update(&mut *tx, &token, &result.cursor_update).await?;
    tx.commit().await.map_err(skilj_core::error::Error::from)?;

    // See `get_events`' own identical comment above.
    let served: Vec<_> = result
        .served
        .iter()
        .map(event_store::redact_private_fields)
        .collect();
    Ok(Json(ConsumeResponse {
        events: served.iter().map(EventDto::from).collect(),
        event_type_name: token.event_type.name.clone(),
        event_type_schema: token.event_type.schema.clone(),
        event_type_schema_version: token.event_type.schema_version,
    }))
}

async fn post_events_consume_ack(
    State(state): State<AppState>,
    credential: BearerCredential,
    Json(body): Json<AckRequest>,
) -> Result<impl IntoResponse, RestError> {
    let token = resolve_token::<EventReadToken>(&state, &credential).await?;
    // Same transaction-plus-per-token-lock shape as `get_events_consume`
    // (docs/architecture.md §53, §75): the check (`sequence >=
    // cursor.sequence`) and the write must see the same cursor. Unlocked,
    // two acks could both pass against the same old cursor and the lower
    // one land last - the cursor moving backwards - and an ack could
    // interleave with a consume's claim.
    let mut tx = state
        .pool
        .begin()
        .await
        .map_err(skilj_core::error::Error::from)?;
    db::lock_read_cursor_for_consume(&mut tx, &token).await?;
    let cursor = db::get_read_cursor(&mut *tx, &token).await?;

    let (sequence, updated_at) =
        event_store::acknowledge_events(&token, cursor.as_ref(), body.sequence, Utc::now())?;
    db::record_acknowledgement(&mut *tx, &token, sequence, updated_at).await?;
    tx.commit().await.map_err(skilj_core::error::Error::from)?;

    Ok(Json(EmptyResponse {}))
}

#[tracing::instrument(
    skip_all,
    fields(bounded_context = tracing::field::Empty, command_type = tracing::field::Empty)
)]
async fn post_commands_trigger(
    State(state): State<AppState>,
    credential: BearerCredential,
    headers: HeaderMap,
    Json(body): Json<CommandTriggerRequest>,
) -> Result<impl IntoResponse, RestError> {
    let token = resolve_token::<CommandToken>(&state, &credential).await?;
    let payload = serde_json::to_string(&body.payload)
        .expect("serde_json::Value serialization is infallible");
    // Codeberg issue #12: optional, backward compatible - omitted (the
    // existing default for every caller) means skip the idempotency
    // mechanism entirely, not "generate a key anyway" - see
    // skilj_core::db::submit_command's own doc comment for why that's
    // the right realisation of "unchanged behaviour when absent". A
    // present but non-UTF-8 header value is treated the same as absent
    // rather than a hard error - this is a caller convenience, not a
    // load-bearing part of the request.
    let idempotency_key = headers.get("Idempotency-Key").and_then(|v| v.to_str().ok());
    // A security-review finding on CrossContextRoute (docs/architecture.md
    // §36): its own background task's internally-derived idempotency keys
    // share this same table's namespace, unscoped by caller - reject a
    // caller-supplied key that impersonates one before it ever reaches
    // the shared idempotency lookup, rather than letting an ordinary
    // Write-level caller pre-plant one and silently swallow a real route
    // delivery. See `reject_reserved_idempotency_key`'s own doc comment.
    event_store::reject_reserved_idempotency_key(idempotency_key)?;

    let authorised = event_store::authorise_command_trigger(
        &token,
        payload,
        body.correlation_id,
        body.causation_id,
    )?;
    let bounded_context_name = authorised.command_type.bounded_context.name.clone();
    let span = tracing::Span::current();
    span.record("bounded_context", bounded_context_name.as_str());
    span.record("command_type", authorised.command_type.name.as_str());

    // The full "optimistic decide, then locked submit" sequence -
    // docs/architecture.md §19's own "Problem 1"/"Problem 2" fixes
    // (tag-indexed fetch, snapshot-context resolution), now shared with
    // `skilj-graphql`'s identical `submitCommand` resolver (and the
    // cross-context event router) via `skilj_core::db::
    // decide_and_submit_command` rather than each duplicating the dance.
    // Routed through `state.command_batcher` rather than calling that
    // function directly (Codeberg issue #32, round two) - this route's
    // own real-world concurrent traffic is exactly what
    // `CommandBatcher` exists to coalesce into fewer bounded-context
    // lock acquisitions; see its own module doc comment.
    let outcome = state
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
            idempotency_key,
        )
        .await?;

    Ok(Json(match outcome {
        // §5.4/§7.3: a legitimate business outcome, not an HTTP error -
        // whether this was the first decision or a DCB-conflict retry
        // inside submit_command, a rejection renders identically either
        // way.
        // `matching_events` (Codeberg issue #7's DCB conflict visualizer)
        // stays a `skilj-tui`/GraphQL-only debugging affordance - out of
        // scope for REST's narrower, machine-caller-facing wire shape
        // (§7.1) - so it's computed and immediately dropped here.
        db::SubmitCommandOutcome::Rejected { reason, kind, .. } => CommandTriggerResponse {
            accepted: false,
            triggered_event_sequences: None,
            rejection_reason: Some(reason),
            rejection_kind: Some(kind),
            deduplicated: false,
            correlation_id: None,
        },
        db::SubmitCommandOutcome::Accepted { command, events } => CommandTriggerResponse {
            accepted: true,
            triggered_event_sequences: Some(events.iter().map(|e| e.sequence).collect()),
            rejection_reason: None,
            rejection_kind: None,
            deduplicated: false,
            correlation_id: command.metadata.correlation_id,
        },
        // Codeberg issue #12: a cached prior answer, not a fresh
        // decision.
        db::SubmitCommandOutcome::Deduplicated {
            triggered_event_sequences,
        } => CommandTriggerResponse {
            accepted: true,
            triggered_event_sequences: Some(triggered_event_sequences),
            rejection_reason: None,
            rejection_kind: None,
            deduplicated: true,
            correlation_id: None,
        },
    }))
}

/// `ParkedDeliveryReport` (Codeberg issue #21) - authenticated with the
/// same `ExternalEventToken`/`CommandToken` credential the bridge
/// already presents for the delivery itself, `kind` saying which -
/// resolving it here (rather than accepting a bare `boundedContext`
/// argument) is this route's own instance of the capability-based
/// design this whole crate already follows (see this module's own
/// top-of-file doc comment): the *credential* says what's being
/// written and where, not a caller-supplied path/argument. The bounded
/// context a delivery is parked under, and which `access_token_id` a
/// later `retryParkedDelivery` re-resolves, both come from that same
/// token.
async fn post_parked_deliveries(
    State(state): State<AppState>,
    credential: BearerCredential,
    Json(body): Json<ParkedDeliveryRequest>,
) -> Result<impl IntoResponse, RestError> {
    // Unlike every other route here, nothing downstream re-checks the
    // token's own status - `insert_parked_delivery` is a plain write, not
    // a skilj-core rule - so a revoked credential is refused here rather
    // than left able to keep writing rows. `request` is checked against
    // the exact body shape its kind's original route takes (the same
    // DTOs, not a copy), since `retryParkedDelivery` later decodes it as
    // exactly that.
    let (bounded_context_name, access_token_id, status, kind, shape_check) = match body.kind {
        ParkedDeliveryKindRequest::ExternalEvent => {
            let token = resolve_token::<ExternalEventToken>(&state, &credential).await?;
            (
                token.event_type.bounded_context.name,
                token.id,
                token.status,
                db::ParkedDeliveryKind::ExternalEvent,
                serde_json::from_value::<ExternalEventRequest>(body.request.clone()).map(|_| ()),
            )
        }
        ParkedDeliveryKindRequest::CommandTrigger => {
            let token = resolve_token::<CommandToken>(&state, &credential).await?;
            (
                token.command_type.bounded_context.name,
                token.id,
                token.status,
                db::ParkedDeliveryKind::CommandTrigger,
                serde_json::from_value::<CommandTriggerRequest>(body.request.clone()).map(|_| ()),
            )
        }
    };
    if status != TokenStatus::Active {
        return Err(skilj_core::error::Error::from(AccessControlError::TokenNotActive).into());
    }
    shape_check.map_err(|e| {
        skilj_core::error::Error::from(event_store::Error::InvalidParkedDeliveryRequest(
            e.to_string(),
        ))
    })?;
    let mut request = body.request;
    if let Some(idempotency_key) = body.idempotency_key {
        if !matches!(kind, db::ParkedDeliveryKind::CommandTrigger) {
            return Err(skilj_core::error::Error::from(
                event_store::Error::InvalidParkedDeliveryRequest(
                    "idempotencyKey only applies to kind command_trigger".to_string(),
                ),
            )
            .into());
        }
        event_store::reject_reserved_idempotency_key(Some(&idempotency_key))?;
        // An object: it just parsed as `CommandTriggerRequest`.
        request["idempotencyKey"] = serde_json::Value::String(idempotency_key);
    }

    let delivery = db::insert_parked_delivery(
        &state.pool,
        &bounded_context_name,
        &body.source,
        kind,
        &body.identifier,
        Some(&access_token_id),
        None,
        None,
        &request,
        &body.error,
        body.attempt_count,
        body.first_failed_at,
        Utc::now(),
    )
    .await?;

    Ok((
        StatusCode::CREATED,
        Json(ParkedDeliveryResponse { id: delivery.id }),
    ))
}
