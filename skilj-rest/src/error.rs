//! Maps `skilj_core::Error` to the REST wire contract's error shape - see
//! docs/architecture.md §7.5. `{ "code": ..., "message": ... }`, built
//! from the same `code()`/`message()` trait `skilj-graphql` renders
//! through for its own `errors` array (§4.2), so a client library sees
//! the identical body shape on both tracks. `CommandRejected` isn't
//! mapped here - §7.3/§5.4 make that 200-with-typed-data, not an error
//! status; `CommandTrigger`'s own handler renders it that way directly,
//! never routing a `Rejected` decision through this type at all.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use skilj_core::access_control::Error as AccessControlError;
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::Error as EventStoreError;
use skilj_core::Error as CoreError;

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    /// The current request's OTel trace id, hex-encoded, for a caller to
    /// quote back when escalating - `None` when no real
    /// `tracing-opentelemetry` layer is installed (this crate used
    /// standalone, or `skilj-demo` run without
    /// `OTEL_EXPORTER_OTLP_ENDPOINT` set), in which case there's no real
    /// trace to point at. `#[serde(skip_serializing_if)]` rather than
    /// always emitting `"trace_id":null` - the field simply isn't there
    /// when it isn't meaningful.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

/// See `skilj-graphql::error`'s identical copy for the full reasoning -
/// duplicated rather than shared, same as `trace_request`, since there's
/// no crate both `skilj-rest` and `skilj-graphql` already depend on that
/// this small a helper would justify adding.
fn current_trace_id() -> Option<String> {
    use opentelemetry::trace::TraceContextExt;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let trace_id = tracing::Span::current()
        .context()
        .span()
        .span_context()
        .trace_id();
    (trace_id != opentelemetry::trace::TraceId::INVALID).then(|| trace_id.to_string())
}

/// A REST-facing error: either a `skilj_core::Error` on its way through
/// the mapping below, or one of this crate's own auth-layer rejections
/// (§7.5's 401 tier - a missing/malformed bearer credential, or one
/// that's well-formed but names no token at all) that never reaches
/// `skilj-core` in the first place, so there's no `Error` variant for it
/// to map from.
#[derive(Debug)]
pub enum RestError {
    Core(CoreError),
    MissingCredential,
    MalformedCredential,
    /// No `AccessToken` matches the presented `id`, or one does but the
    /// presented `secret` doesn't match it - both read as "this isn't a
    /// credential we recognise" rather than leaking which half was wrong.
    UnrecognisedCredential,
    /// An `AccessToken` matches the presented `id`, but of a different
    /// `AccessTokenKind` than the route needs - §7.2's "presenting the
    /// wrong token variant at a route is a 403, not a 404".
    WrongTokenVariant,
    /// A request-shape problem this crate's own routing layer catches
    /// before calling into skilj-core at all (e.g. `mode` isn't `"auto"`
    /// or `"manual"`) - distinct from a library-level `Error`, which
    /// always comes from a rule that *did* run.
    InvalidRequest(String),
    /// `CommandTrigger`'s own `Arc<dyn CommandDispatcher>` had nothing
    /// registered for this `(bounded_context, command_type)` pair - see
    /// `CommandDispatcher::dispatch`'s own doc comment for why that's
    /// `None`, not a `skilj_core::Error`, and so this route's own job to
    /// turn into a rejection. Reachable only when the compiled binary's
    /// registrations have drifted from what's stored (the token itself
    /// already proves the `CommandType` exists), so 500 - a server-side
    /// configuration problem, not anything the caller did wrong.
    NoDeciderRegistered,
}

impl From<CoreError> for RestError {
    fn from(e: CoreError) -> Self {
        RestError::Core(e)
    }
}

/// §7.5's table, one rule per line: library-level errors map to standard
/// HTTP semantics by what they actually mean, not by guessing from the
/// string `code()` returns - matching each `Error` variant directly is
/// more robust than string-matching `code()` would be, and `code()`
/// still goes on the wire unchanged as the body's own machine-readable
/// field.
fn status_for(err: &CoreError) -> StatusCode {
    match err {
        CoreError::AccessControl(e) => match e {
            AccessControlError::TokenNotActive => StatusCode::FORBIDDEN,
            AccessControlError::GrantNotActive => StatusCode::FORBIDDEN,
            AccessControlError::InsufficientAccessLevel => StatusCode::FORBIDDEN,
            AccessControlError::GrantBoundedContextMismatch => StatusCode::FORBIDDEN,
            // Cross-tenant write fix (docs/architecture.md's own write-up
            // of these passes): newly reachable over REST by this pass -
            // authorise_command_trigger/create_external_event/
            // create_direct_event are the first Error::GrantScopeMismatch
            // sources on the REST track that actually reject rather than
            // filter (EventFetch's own fetch_events/consume_events only
            // ever filter - see event_owner_scope_satisfied's own doc
            // comment - so this variant was unreachable over REST before
            // the write side existed). Same meaning as
            // GrantBoundedContextMismatch above: a caller a token no
            // longer covers.
            AccessControlError::GrantScopeMismatch => StatusCode::FORBIDDEN,
            AccessControlError::NotSuperadmin => StatusCode::FORBIDDEN,
            AccessControlError::RoleNotActive => StatusCode::FORBIDDEN,
            AccessControlError::UnrecognisedSubject => StatusCode::UNAUTHORIZED,
            AccessControlError::ExternalSubjectAlreadyClaimed => StatusCode::CONFLICT,
            AccessControlError::DuplicateActiveMapping => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
        CoreError::EventStore(e) => match e {
            EventStoreError::BoundedContextArchived => StatusCode::CONFLICT,
            EventStoreError::NotManualAckCursor => StatusCode::CONFLICT,
            EventStoreError::NoReadCursor => StatusCode::CONFLICT,
            EventStoreError::AcknowledgementRegresses => StatusCode::CONFLICT,
            EventStoreError::InvalidFilter => StatusCode::BAD_REQUEST,
            EventStoreError::CursorAckModeMismatch => StatusCode::BAD_REQUEST,
            EventStoreError::ExternalCreationNotAllowed => StatusCode::FORBIDDEN,
            EventStoreError::DirectCreationNotAllowed => StatusCode::FORBIDDEN,
            EventStoreError::EventReadNotAllowed => StatusCode::FORBIDDEN,
            EventStoreError::RestTriggerNotAllowed => StatusCode::FORBIDDEN,
            EventStoreError::EncryptionKeyNotActive => StatusCode::FORBIDDEN,
            EventStoreError::EventTypeNotInBoundedContext => StatusCode::FORBIDDEN,
            EventStoreError::CommandTypeNotInBoundedContext => StatusCode::FORBIDDEN,
            EventStoreError::TriggeredEventNotInBoundedContext => StatusCode::FORBIDDEN,
            EventStoreError::AfterCommandNotInBoundedContext => StatusCode::FORBIDDEN,
            EventStoreError::InvalidTagMapping => StatusCode::BAD_REQUEST,
            EventStoreError::InvalidSensitiveField => StatusCode::BAD_REQUEST,
            EventStoreError::PayloadDoesNotMatchSchema => StatusCode::BAD_REQUEST,
            EventStoreError::ReservedIdempotencyKeyPrefix => StatusCode::BAD_REQUEST,
            EventStoreError::InvalidParkedDeliveryRequest(_) => StatusCode::BAD_REQUEST,
            // Not reachable over REST today - RegisterEventType is
            // GraphQL-only (TypeRegistration) - but matched explicitly
            // rather than left to the wildcard below, the same "each
            // variant mapped by what it actually means" reasoning this
            // whole match already follows.
            EventStoreError::MissingScheduleOrPolicy => StatusCode::BAD_REQUEST,
            EventStoreError::SensitiveFieldTagOverlap => StatusCode::BAD_REQUEST,
            EventStoreError::SchemaIncompatible => StatusCode::CONFLICT,
            EventStoreError::TagMappingKeyDropped => StatusCode::CONFLICT,
            EventStoreError::UnregisteredEventType(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
        // Neither module has a route in front of it yet (projections'
        // QueryProjection and bootstrap's superadmin/context-management
        // rules are both GraphQL-only surfaces, still unwired) - no REST
        // route reachable today can actually produce one of these.
        CoreError::Projections(_) => StatusCode::INTERNAL_SERVER_ERROR,
        CoreError::Bootstrap(_) => StatusCode::INTERNAL_SERVER_ERROR,
        // A misconfigured/missing encryption_master_key or a corrupted
        // wrapped key are both operator-facing configuration/data-integrity
        // problems, never a caller-facing 4xx.
        CoreError::Encryption(_) => StatusCode::INTERNAL_SERVER_ERROR,
        // §7.3/§5.4: a rejection is a legitimate outcome of a successful
        // request, not an HTTP-level error - CommandTrigger's own handler
        // renders this as 200-with-typed-data instead of ever routing a
        // `Rejected` decision through this function at all. Kept as a
        // defensive fallback (`process_command` itself still returns this
        // variant if handed one directly), never reached in practice.
        CoreError::CommandRejected { .. } => StatusCode::OK,
        // `db::submit_command`'s own DCB-conflict retry path only - see
        // that variant's own doc comment. The same status
        // `RestError::NoDeciderRegistered` already gives the first,
        // optimistic `dispatch` call's identical `None` outcome.
        CoreError::NoDeciderRegistered => StatusCode::INTERNAL_SERVER_ERROR,
        CoreError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        // A migration failure is a startup-time concern (Skilj::builder().build())
        // - no request ever reaches a route handler while it's unresolved,
        // so this arm is unreachable in practice, not merely unlikely.
        CoreError::Migration(_) => StatusCode::INTERNAL_SERVER_ERROR,
        // `command_batcher::CommandBatcher`'s own follower-facing error -
        // a batch this request's own command happened to share failed at
        // the shared-transaction level (lock acquisition or the final
        // commit), or its own batch leader's task ended without replying
        // at all. Neither is this caller's fault, and neither maps to
        // any more specific real database error it could itself recover
        // from - the identical `INTERNAL_SERVER_ERROR` `CoreError::Database`
        // already gets.
        CoreError::BatchFailed { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            RestError::Core(e) => {
                // docs/architecture.md §93: the raw cause stays server-side,
                // in this request's span, findable by the returned trace id.
                if let Some(detail) = e.internal_detail() {
                    tracing::error!(code = %e.code(), error = %detail, "internal error answering a REST request");
                }
                (status_for(&e), e.code().to_string(), e.message())
            }
            RestError::MissingCredential => (
                StatusCode::UNAUTHORIZED,
                "missing_credential".to_string(),
                "missing Authorization: Bearer <id>.<secret> header".to_string(),
            ),
            RestError::MalformedCredential => (
                StatusCode::UNAUTHORIZED,
                "malformed_credential".to_string(),
                "Authorization header is not a well-formed \"<id>.<secret>\" bearer credential"
                    .to_string(),
            ),
            RestError::UnrecognisedCredential => (
                StatusCode::UNAUTHORIZED,
                "unrecognised_credential".to_string(),
                "no AccessToken matches this credential".to_string(),
            ),
            RestError::WrongTokenVariant => (
                StatusCode::FORBIDDEN,
                "wrong_token_variant".to_string(),
                "this AccessToken's kind doesn't authorize this route".to_string(),
            ),
            RestError::InvalidRequest(message) => (
                StatusCode::BAD_REQUEST,
                "invalid_request".to_string(),
                message,
            ),
            RestError::NoDeciderRegistered => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "no_decider_registered".to_string(),
                "this CommandType has no decide() registered in the running process".to_string(),
            ),
        };
        (
            status,
            Json(ErrorBody {
                code,
                message,
                trace_id: current_trace_id(),
            }),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::RestError;
    use axum::response::IntoResponse;

    /// docs/architecture.md §93: a database error answers with its code
    /// and a generic message - never the raw text, which names schemas
    /// (bounded contexts, i.e. tenants), constraints and SQL.
    #[tokio::test]
    async fn a_database_error_does_not_reach_the_caller_verbatim() {
        let raw = r#"relation "bc_acme_corp.events" does not exist"#;
        let error = skilj_core::Error::Database(sqlx::Error::Protocol(raw.to_string()));
        let response = RestError::Core(error).into_response();
        assert_eq!(
            response.status(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "database_error");
        assert_eq!(body["message"], "an internal database error occurred");
        assert!(!body.to_string().contains("acme_corp"), "{body}");
    }

    #[test]
    fn a_vanished_row_and_pool_exhaustion_stay_distinguishable() {
        use skilj_core::error::SkiljRejection;
        assert_eq!(
            skilj_core::Error::Database(sqlx::Error::RowNotFound).message(),
            "a record this request needed no longer exists"
        );
        assert!(skilj_core::Error::Database(sqlx::Error::PoolTimedOut)
            .message()
            .contains("retry"));
        // The detail is still there for the server log.
        let error = skilj_core::Error::Database(sqlx::Error::Protocol("boom".to_string()));
        assert!(error.internal_detail().unwrap().contains("boom"));
    }
}
