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
///
/// The rejection's own `hint()`/`guide()` go in `extensions.hint` and
/// `extensions.guide` when it has them (docs/architecture.md §258), so a
/// business rejection never gets skilj's advice.
pub fn to_graphql_error(rejection: impl SkiljRejection) -> async_graphql::Error {
    let code = rejection.code().to_string();
    let (hint, guide) = (rejection.hint(), rejection.guide());
    let trace_id = current_trace_id();
    // docs/architecture.md §93: the raw cause stays server-side, in this
    // request's span, findable by the trace id the caller is given.
    if let Some(detail) = rejection.internal_detail() {
        tracing::error!(code = %code, error = %detail, "internal error answering a GraphQL request");
    }
    async_graphql::Error::new(rejection.message()).extend_with(|_, ext| {
        set_code(ext, &code, hint, guide);
        if let Some(trace_id) = &trace_id {
            ext.set("traceId", trace_id.as_str());
        }
    })
}

/// The one way this crate sets an error's `code`: with `hint` and `guide`
/// next to it, each only when there is one (docs/architecture.md §258).
pub(crate) fn set_code(
    ext: &mut async_graphql::ErrorExtensionValues,
    code: &str,
    hint: Option<&str>,
    guide: Option<&str>,
) {
    ext.set("code", code);
    if let Some(hint) = hint {
        ext.set("hint", hint);
    }
    if let Some(guide) = guide {
        ext.set("guide", guide);
    }
}

/// An error with one of this crate's own codes, and the hint and guide
/// `skilj_core::error::GUIDANCE` has for it. The code comes first, as in
/// `limits::budget_error`.
pub(crate) fn coded(code: &'static str, message: impl Into<String>) -> async_graphql::Error {
    let guidance = skilj_core::error::guidance(code);
    async_graphql::Error::new(message).extend_with(|_, ext| {
        set_code(
            ext,
            code,
            guidance.map(|guidance| guidance.hint),
            guidance.map(|guidance| guidance.guide),
        )
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
    use async_graphql::Value;

    fn extension(error: &async_graphql::Error, name: &str) -> Option<Value> {
        error
            .extensions
            .as_ref()
            .and_then(|ext| ext.get(name))
            .cloned()
    }

    #[test]
    fn a_guided_code_carries_hint_and_guide_and_an_unguided_one_neither() {
        let guided = super::to_graphql_error(skilj_core::Error::from(
            skilj_core::event_store::Error::PayloadDoesNotMatchSchema,
        ));
        assert_eq!(
            extension(&guided, "code"),
            Some(Value::from("payload_does_not_match_schema"))
        );
        assert!(extension(&guided, "hint").is_some());
        assert_eq!(
            extension(&guided, "guide"),
            Some(Value::from(
                "skilj#if-a-registration-or-a-submission-gets-rejected"
            ))
        );
        let unguided =
            super::to_graphql_error(skilj_core::Error::Database(sqlx::Error::RowNotFound));
        assert_eq!(
            extension(&unguided, "code"),
            Some(Value::from("database_error"))
        );
        assert_eq!(extension(&unguided, "hint"), None);
        assert_eq!(extension(&unguided, "guide"), None);
        for code in ["unregistered_event_type", "direct_creation_not_allowed"] {
            let error = super::coded(code, "refused");
            assert!(extension(&error, "hint").is_some(), "{code}");
            assert!(extension(&error, "guide").is_some(), "{code}");
        }
        let error = super::coded("no_identity_provider_configured", "no IdP");
        assert!(extension(&error, "guide").is_some());
        let error = super::coded("unauthenticated", "who are you");
        assert_eq!(
            extension(&error, "code"),
            Some(Value::from("unauthenticated"))
        );
        assert_eq!(extension(&error, "hint"), None);
    }

    /// A `decide()`'s own kind is the application's, never skilj's advice.
    #[test]
    fn a_business_rejection_spelled_like_a_skilj_code_gets_no_advice() {
        let rejected = super::to_graphql_error(skilj_core::Error::CommandRejected {
            reason: "over this account's limit".to_string(),
            kind: "payload_too_large".to_string(),
        });
        assert_eq!(
            extension(&rejected, "code"),
            Some(Value::from("payload_too_large"))
        );
        assert_eq!(rejected.message, "over this account's limit");
        assert_eq!(extension(&rejected, "hint"), None);
        assert_eq!(extension(&rejected, "guide"), None);
    }

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
