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
    /// §7.4's `filter=field:op:value` param is present, but
    /// `valid_filters`/`matches_filters` are still `todo!()` for any
    /// non-empty filter set (see `event_store`'s own doc comment) - a
    /// route can't call into skilj-core with one without panicking the
    /// process, so this is rejected here instead, before that call ever
    /// happens.
    FiltersNotSupported,
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
            EventStoreError::InvalidTagMapping => StatusCode::BAD_REQUEST,
            EventStoreError::InvalidSensitiveField => StatusCode::BAD_REQUEST,
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
        // §7.3/§5.4: a rejection is a legitimate outcome of a successful
        // request, not an HTTP-level error - CommandTrigger's own handler
        // renders this as 200-with-typed-data instead of ever routing a
        // `Rejected` decision through this function at all. Kept as a
        // defensive fallback (`process_command` itself still returns this
        // variant if handed one directly), never reached in practice.
        CoreError::CommandRejected { .. } => StatusCode::OK,
        CoreError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        // A migration failure is a startup-time concern (Skilj::builder().build())
        // - no request ever reaches a route handler while it's unresolved,
        // so this arm is unreachable in practice, not merely unlikely.
        CoreError::Migration(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            RestError::Core(e) => (status_for(&e), e.code().to_string(), e.message()),
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
            RestError::FiltersNotSupported => (
                StatusCode::BAD_REQUEST,
                "filters_not_supported".to_string(),
                "non-empty filter sets aren't supported yet".to_string(),
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
        (status, Json(ErrorBody { code, message })).into_response()
    }
}
