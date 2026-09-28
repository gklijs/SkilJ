//! Renders `skilj_core::error::SkiljRejection` (both error tiers - see
//! [docs/architecture.md §4](../../docs/architecture.md#error-handling)) into `async_graphql`'s own `Error`, carrying
//! `code()` as the `code` extension - §5.4's shared error shape, the
//! same `code()`/`message()` trait `skilj-rest::error` renders through
//! for REST (§4.2, §7.5). `CommandRejected` isn't specially handled here
//! at all, and never will be: `resolvers::command_submission` matches
//! `db::SubmitCommandOutcome::{Accepted, Rejected}` directly, as plain
//! typed data on the success path, before this conversion ever gets a
//! chance to run - a business rejection is `SubmitCommandPayload`'s own
//! `accepted: false` field, not a GraphQL error at all.

use async_graphql::ErrorExtensions;
use skilj_core::error::SkiljRejection;

/// The one conversion every resolver in this crate uses for a
/// `skilj_core` (or auth-layer) rejection - `.map_err(to_graphql_error)?`,
/// not `?` alone: `async_graphql::Error` has a blanket `From<T: Display>`
/// impl that would compile without this, but it wouldn't carry `code()`
/// as an extension, silently dropping half of §5.4's error shape.
pub fn to_graphql_error(rejection: impl SkiljRejection) -> async_graphql::Error {
    let code = rejection.code().to_string();
    let trace_id = current_trace_id();
    // docs/architecture.md §93: the raw cause stays server-side, in this
    // request's span, findable by the trace id the caller is given.
    if let Some(detail) = rejection.internal_detail() {
        tracing::error!(code = %code, error = %detail, "internal error answering a GraphQL request");
    }
    async_graphql::Error::new(rejection.message()).extend_with(|_, ext| {
        ext.set("code", code);
        if let Some(trace_id) = &trace_id {
            ext.set("traceId", trace_id.as_str());
        }
    })
}

/// See `skilj-rest::error`'s identical copy for the full reasoning -
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

#[cfg(test)]
mod tests {
    /// docs/architecture.md §93 - see `skilj-rest`'s identical test.
    #[test]
    fn a_database_error_does_not_reach_the_caller_verbatim() {
        let raw = r#"duplicate key value violates unique constraint "bc_acme_corp_pkey""#;
        let error = skilj_core::Error::Database(sqlx::Error::Protocol(raw.to_string()));
        let rendered = super::to_graphql_error(error);
        assert_eq!(rendered.message, "an internal database error occurred");
        assert!(!format!("{rendered:?}").contains("acme_corp"));
    }
}
